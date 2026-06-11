use std::pin::Pin;
use std::sync::Arc;

use prost_types::Timestamp;
use tokio::sync::broadcast;
use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

#[cfg(feature = "cluster")]
use crate::cluster::ClusterNode;
#[cfg(not(feature = "cluster"))]
use crate::cluster_stub::ClusterNode;
use crate::conflict::{ConflictDetector, ConflictResolver};
use crate::metrics;
use crate::proto;
use crate::ratelimit::ScopeRateLimiter;
use crate::scope::ScopeResolver;
use crate::search::VectorSearch;
use crate::storage::StorageBackend;

/// CVE-16: Log full error internally, return opaque message to client.
/// Prevents leaking internal paths, stack traces, or implementation details.
fn internal_error(context: &str, err: impl std::fmt::Display) -> Status {
    // Log full error for operators
    tracing::error!(%err, context, "Internal error");
    // Return opaque message to client (no internal details)
    Status::internal(format!("{context}: internal server error"))
}

/// gRPC implementation of MemoryService.
#[allow(dead_code)]
pub struct MemoryServiceImpl {
    store: Arc<dyn StorageBackend>,
    search: Arc<VectorSearch>,
    conflict_detector: Arc<ConflictDetector>,
    cluster: Arc<ClusterNode>,
    events_tx: broadcast::Sender<proto::MemoryEvent>,
    rate_limiter: Arc<ScopeRateLimiter>,
}

impl MemoryServiceImpl {
    pub fn new(
        store: Arc<dyn StorageBackend>,
        search: Arc<VectorSearch>,
        conflict_detector: Arc<ConflictDetector>,
        cluster: Arc<ClusterNode>,
        events_tx: broadcast::Sender<proto::MemoryEvent>,
        rate_limiter: Arc<ScopeRateLimiter>,
    ) -> Self {
        Self {
            store,
            search,
            conflict_detector,
            cluster,
            events_tx,
            rate_limiter,
        }
    }

    fn now_timestamp() -> Timestamp {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Timestamp {
            seconds: now.as_secs() as i64,
            nanos: now.subsec_nanos() as i32,
        }
    }

    fn emit_event(&self, event_type: proto::EventType, record: &proto::MemoryRecord) {
        let event = proto::MemoryEvent {
            r#type: event_type as i32,
            record: Some(record.clone()),
            conflict_id: String::new(),
            timestamp: Some(Self::now_timestamp()),
            source_node: self.cluster.node_id.clone(),
        };
        // Best-effort broadcast; ignore if no subscribers
        let _ = self.events_tx.send(event);
    }
}

#[tonic::async_trait]
impl proto::memory_service_server::MemoryService for MemoryServiceImpl {
    async fn add(
        &self,
        request: Request<proto::AddRequest>,
    ) -> Result<Response<proto::AddResponse>, Status> {
        let req = request.into_inner();

        // === Input validation (CVE-17: prevent oversized payloads) ===
        const MAX_CONTENT_BYTES: usize = 1_048_576; // 1MB
        #[allow(dead_code)]
        const MAX_EMBEDDING_DIMS: usize = 4096;
        const MAX_TAGS: usize = 100;
        const MAX_TAG_LEN: usize = 256;

        if req.content.is_empty() {
            return Err(Status::invalid_argument("content must not be empty"));
        }
        if req.content.len() > MAX_CONTENT_BYTES {
            return Err(Status::invalid_argument(format!(
                "content exceeds maximum size ({} bytes > {} limit)",
                req.content.len(),
                MAX_CONTENT_BYTES
            )));
        }
        if req.tags.len() > MAX_TAGS {
            return Err(Status::invalid_argument(format!(
                "too many tags ({} > {} limit)",
                req.tags.len(),
                MAX_TAGS
            )));
        }
        for tag in &req.tags {
            if tag.len() > MAX_TAG_LEN {
                return Err(Status::invalid_argument(format!(
                    "tag exceeds maximum length ({} > {})",
                    tag.len(),
                    MAX_TAG_LEN
                )));
            }
        }
        if req.confidence < 0.0 || req.confidence > 1.0 {
            return Err(Status::invalid_argument(
                "confidence must be between 0.0 and 1.0",
            ));
        }

        // === Per-scope rate limiting ===
        let scope_key = req
            .scope
            .as_ref()
            .map(ScopeRateLimiter::scope_key)
            .unwrap_or_else(|| "_:_:_:_".to_string());
        if let Err(retry_after_ms) = self.rate_limiter.check(&scope_key).await {
            return Err(Status::resource_exhausted(format!(
                "Rate limit exceeded for scope. Retry after {}ms",
                retry_after_ms
            )));
        }

        // Generate ID and timestamps
        let id = ulid::Ulid::new().to_string().to_lowercase();
        let now = Self::now_timestamp();
        let clock = self.cluster.tick().await;

        let record = proto::MemoryRecord {
            id: id.clone(),
            content: req.content,
            embedding: vec![], // Embeddings computed externally or via future embedding service
            scope: req.scope,
            tags: req.tags,
            kind: req.kind,
            confidence: if req.confidence > 0.0 {
                req.confidence
            } else {
                1.0
            },
            source: None,
            created_at: Some(now),
            updated_at: Some(now),
            accessed_at: Some(now),
            expires_at: req.expires_at,
            version: 1,
            vector_clock: Some(proto::VectorClock { clock }),
            lineage: vec![],
        };

        // Store
        let stored = self
            .store
            .add(record)
            .await
            .map_err(|e| internal_error("storage", e))?;

        // Update cluster record count
        let count = self.store.count().await.unwrap_or(0);
        self.cluster.set_record_count(count).await;

        // Update metrics
        metrics::MEMORIES_TOTAL.set(count as i64);
        metrics::REQUESTS_TOTAL
            .with_label_values(&["add", "ok"])
            .inc();

        // Emit event
        self.emit_event(proto::EventType::MemoryAdded, &stored);

        info!(id = %id, "Memory record added");

        Ok(Response::new(proto::AddResponse {
            record: Some(stored),
            deduplicated: false,
            merged_with: String::new(),
        }))
    }

    async fn update(
        &self,
        request: Request<proto::UpdateRequest>,
    ) -> Result<Response<proto::UpdateResponse>, Status> {
        let req = request.into_inner();

        // Get existing record
        let existing = self
            .store
            .get(&req.id)
            .await
            .map_err(|e| internal_error("storage", e))?
            .ok_or_else(|| Status::not_found(format!("Record '{}' not found", req.id)))?;

        // Store previous version in history
        let _ = self.store.store_version(&existing).await;

        let now = Self::now_timestamp();
        let clock = self.cluster.tick().await;

        let updated = proto::MemoryRecord {
            id: req.id.clone(),
            content: if req.content.is_empty() {
                existing.content
            } else {
                req.content
            },
            embedding: existing.embedding,
            scope: existing.scope,
            tags: if req.tags.is_empty() {
                existing.tags
            } else {
                req.tags
            },
            kind: if req.kind == 0 {
                existing.kind
            } else {
                req.kind
            },
            confidence: if req.confidence > 0.0 {
                req.confidence
            } else {
                existing.confidence
            },
            source: existing.source,
            created_at: existing.created_at,
            updated_at: Some(now),
            accessed_at: existing.accessed_at,
            expires_at: req.expires_at.or(existing.expires_at),
            version: existing.version + 1,
            vector_clock: Some(proto::VectorClock { clock }),
            lineage: existing.lineage,
        };

        let stored = self
            .store
            .update(updated)
            .await
            .map_err(|e| internal_error("storage", e))?;

        metrics::REQUESTS_TOTAL
            .with_label_values(&["update", "ok"])
            .inc();

        self.emit_event(proto::EventType::MemoryUpdated, &stored);
        info!(id = %req.id, "Memory record updated");

        Ok(Response::new(proto::UpdateResponse {
            record: Some(stored),
        }))
    }

    async fn forget(
        &self,
        request: Request<proto::ForgetRequest>,
    ) -> Result<Response<proto::ForgetResponse>, Status> {
        let req = request.into_inner();

        let count = if !req.id.is_empty() {
            // Delete by ID
            let existed = self
                .store
                .delete(&req.id)
                .await
                .map_err(|e| internal_error("storage", e))?;
            if existed {
                1u64
            } else {
                0u64
            }
        } else if let Some(scope) = req.scope {
            // Delete by scope
            self.store
                .delete_by_scope(&scope, req.before)
                .await
                .map_err(|e| internal_error("storage", e))?
        } else {
            return Err(Status::invalid_argument(
                "Either 'id' or 'scope' must be provided",
            ));
        };

        // Update cluster record count and metrics
        let total = self.store.count().await.unwrap_or(0);
        self.cluster.set_record_count(total).await;
        metrics::MEMORIES_TOTAL.set(total as i64);
        metrics::REQUESTS_TOTAL
            .with_label_values(&["forget", "ok"])
            .inc();

        info!(count, "Memory records forgotten");

        Ok(Response::new(proto::ForgetResponse {
            forgotten_count: count,
        }))
    }

    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> Result<Response<proto::GetResponse>, Status> {
        let req = request.into_inner();

        let record = self
            .store
            .get(&req.id)
            .await
            .map_err(|e| internal_error("storage", e))?
            .ok_or_else(|| Status::not_found(format!("Record '{}' not found", req.id)))?;

        metrics::REQUESTS_TOTAL
            .with_label_values(&["get", "ok"])
            .inc();

        Ok(Response::new(proto::GetResponse {
            record: Some(record),
        }))
    }

    async fn search(
        &self,
        request: Request<proto::SearchRequest>,
    ) -> Result<Response<proto::SearchResponse>, Status> {
        let req = request.into_inner();
        // CVE-6: Cap top_k to prevent excessive memory allocation
        let top_k = (if req.top_k == 0 { 10 } else { req.top_k }).min(1000) as usize;
        let min_score = req.min_score;

        // If query_embedding is provided, use vector search
        let results: Vec<proto::SearchResult> = if !req.query_embedding.is_empty() {
            let timer = metrics::SEARCH_DURATION_SECONDS.start_timer();
            let scored = self
                .search
                .search(&req.query_embedding, top_k, min_score)
                .await
                .map_err(|e| internal_error("search", e))?;
            timer.observe_duration();

            let ids: Vec<String> = scored.iter().map(|(id, _)| id.clone()).collect();
            let records = self
                .store
                .get_many(&ids)
                .await
                .map_err(|e| internal_error("storage", e))?;

            // Apply scope visibility filter
            // CVE-4 fix: scope=None means only PUBLIC records are visible
            let visible = if let Some(ref scope) = req.scope {
                ScopeResolver::filter_visible(&records, scope)
            } else {
                // No scope provided: only return PUBLIC records
                records
                    .into_iter()
                    .filter(|r| {
                        r.scope
                            .as_ref()
                            .map(|s| s.visibility == proto::Visibility::Public as i32)
                            .unwrap_or(false)
                    })
                    .collect()
            };

            // Apply additional filters and build SearchResults
            visible
                .into_iter()
                .filter(|r| {
                    // Kind filter
                    if !req.kinds.is_empty() && !req.kinds.contains(&r.kind) {
                        return false;
                    }
                    // Tag filter
                    if !req.tags.is_empty() {
                        for tag in &req.tags {
                            if !r.tags.contains(tag) {
                                return false;
                            }
                        }
                    }
                    true
                })
                .map(|r| {
                    let score = scored
                        .iter()
                        .find(|(id, _)| *id == r.id)
                        .map(|(_, s)| *s)
                        .unwrap_or(0.0);
                    proto::SearchResult {
                        record: Some(r),
                        score,
                        explanation: String::new(),
                    }
                })
                .take(top_k)
                .collect()
        } else {
            // Without embedding, fall back to listing with filters
            let (records, _) = self
                .store
                .list(req.scope.as_ref(), &req.kinds, &req.tags, top_k as u32, 0)
                .await
                .map_err(|e| internal_error("storage", e))?;

            // Apply scope visibility
            // CVE-4 fix: scope=None means only PUBLIC records are visible
            let visible = if let Some(ref scope) = req.scope {
                ScopeResolver::filter_visible(&records, scope)
            } else {
                records
                    .into_iter()
                    .filter(|r| {
                        r.scope
                            .as_ref()
                            .map(|s| s.visibility == proto::Visibility::Public as i32)
                            .unwrap_or(false)
                    })
                    .collect()
            };

            visible
                .into_iter()
                .map(|r| proto::SearchResult {
                    record: Some(r),
                    score: 1.0, // No semantic score available
                    explanation: "Listed (no embedding search)".to_string(),
                })
                .collect()
        };

        metrics::REQUESTS_TOTAL
            .with_label_values(&["search", "ok"])
            .inc();

        let total = results.len() as u32;
        Ok(Response::new(proto::SearchResponse { results, total }))
    }

    async fn list(
        &self,
        request: Request<proto::ListRequest>,
    ) -> Result<Response<proto::ListResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 50 } else { req.limit };

        // Use cursor-based pagination when cursor is provided, otherwise offset
        let (records, total, next_cursor) = if !req.cursor.is_empty() {
            let (recs, cursor) = self
                .store
                .list_with_cursor(
                    req.scope.as_ref(),
                    &req.kinds,
                    &req.tags,
                    limit,
                    Some(&req.cursor),
                )
                .await
                .map_err(|e| internal_error("storage", e))?;
            let total = recs.len() as u32; // cursor mode doesn't compute total
            (recs, total, cursor.unwrap_or_default())
        } else {
            let (recs, total) = self
                .store
                .list(req.scope.as_ref(), &req.kinds, &req.tags, limit, req.offset)
                .await
                .map_err(|e| internal_error("storage", e))?;
            (recs, total, String::new())
        };

        // Apply scope visibility
        // CVE-4 fix: scope=None means only PUBLIC records are visible
        let visible = if let Some(ref scope) = req.scope {
            ScopeResolver::filter_visible(&records, scope)
        } else {
            records
                .into_iter()
                .filter(|r| {
                    r.scope
                        .as_ref()
                        .map(|s| s.visibility == proto::Visibility::Public as i32)
                        .unwrap_or(false)
                })
                .collect()
        };

        metrics::REQUESTS_TOTAL
            .with_label_values(&["list", "ok"])
            .inc();

        Ok(Response::new(proto::ListResponse {
            records: visible,
            total,
            next_cursor,
        }))
    }

    async fn history(
        &self,
        request: Request<proto::HistoryRequest>,
    ) -> Result<Response<proto::HistoryResponse>, Status> {
        let req = request.into_inner();

        let versions = self
            .store
            .history(&req.id)
            .await
            .map_err(|e| internal_error("storage", e))?;

        metrics::REQUESTS_TOTAL
            .with_label_values(&["history", "ok"])
            .inc();

        Ok(Response::new(proto::HistoryResponse { versions }))
    }

    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<proto::MemoryEvent, Status>> + Send + 'static>>;

    async fn subscribe(
        &self,
        request: Request<proto::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let rx = self.events_tx.subscribe();

        let scope_filter = req.scope.clone();
        let event_types: Vec<i32> = req.event_types.clone();

        let stream = BroadcastStream::new(rx).filter_map(move |result| {
            match result {
                Ok(event) => {
                    // Filter by event type
                    if !event_types.is_empty()
                        && !event_types.contains(&0) // EVENT_ALL = 0
                        && !event_types.contains(&event.r#type)
                    {
                        return None;
                    }

                    // Filter by scope (if specified)
                    if let Some(ref filter_scope) = scope_filter {
                        if let Some(ref record) = event.record {
                            if let Some(ref record_scope) = record.scope {
                                if !ScopeResolver::is_visible(record_scope, filter_scope) {
                                    return None;
                                }
                            }
                        }
                    }

                    Some(Ok(event))
                }
                Err(_) => None, // Skip lagged messages
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }

    async fn batch_add(
        &self,
        request: Request<proto::BatchAddRequest>,
    ) -> Result<Response<proto::BatchAddResponse>, Status> {
        let req = request.into_inner();
        let mut results = Vec::with_capacity(req.records.len());

        for add_req in req.records {
            let inner_request = Request::new(add_req);
            match self.add(inner_request).await {
                Ok(resp) => results.push(resp.into_inner()),
                Err(e) => {
                    warn!("Batch add item failed: {}", e);
                    results.push(proto::AddResponse {
                        record: None,
                        deduplicated: false,
                        merged_with: format!("Error: {}", e),
                    });
                }
            }
        }

        Ok(Response::new(proto::BatchAddResponse { results }))
    }

    type ExportStream =
        Pin<Box<dyn Stream<Item = Result<proto::MemoryRecord, Status>> + Send + 'static>>;

    async fn export(
        &self,
        request: Request<proto::ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        let req = request.into_inner();

        let (records, _) = self
            .store
            .list(req.scope.as_ref(), &[], &[], u32::MAX, 0)
            .await
            .map_err(|e| internal_error("export", e))?;

        let stream = tokio_stream::iter(records.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn import(
        &self,
        request: Request<tonic::Streaming<proto::MemoryRecord>>,
    ) -> Result<Response<proto::ImportResponse>, Status> {
        let mut stream = request.into_inner();
        let mut imported = 0u64;
        let mut skipped = 0u64;

        while let Some(record) = stream.message().await? {
            match self.store.add(record).await {
                Ok(_) => imported += 1,
                Err(_) => skipped += 1,
            }
        }

        // Update cluster record count and metrics
        let count = self.store.count().await.unwrap_or(0);
        self.cluster.set_record_count(count).await;
        metrics::MEMORIES_TOTAL.set(count as i64);

        Ok(Response::new(proto::ImportResponse {
            imported_count: imported,
            skipped_count: skipped,
        }))
    }

    /// Attach or update an embedding for an existing memory record.
    ///
    /// This is part of the embedding lifecycle:
    /// 1. Client stores content via `Add()` (no embedding yet)
    /// 2. Client computes embedding externally (e.g., via OpenAI, sentence-transformers)
    /// 3. Client calls `Embed()` to attach the embedding vector to the record
    /// 4. Record becomes searchable via vector similarity in `Search()`
    ///
    /// Currently returns UNIMPLEMENTED — full implementation will:
    /// - Validate embedding dimensions against configured model
    /// - Store the embedding in the record
    /// - Update the HNSW index for ANN search
    /// - Emit a MEMORY_UPDATED event
    async fn embed(
        &self,
        _request: Request<proto::EmbedRequest>,
    ) -> Result<Response<proto::EmbedResponse>, Status> {
        Err(Status::unimplemented(
            "Embed RPC is not yet implemented. Embeddings should be provided via Update or computed by an external embedding service.",
        ))
    }
}

/// gRPC implementation of ConflictService.
pub struct ConflictServiceImpl {
    conflict_detector: Arc<ConflictDetector>,
    store: Arc<dyn StorageBackend>,
}

impl ConflictServiceImpl {
    pub fn new(conflict_detector: Arc<ConflictDetector>, store: Arc<dyn StorageBackend>) -> Self {
        Self {
            conflict_detector,
            store,
        }
    }
}

#[tonic::async_trait]
impl proto::conflict_service_server::ConflictService for ConflictServiceImpl {
    async fn list_conflicts(
        &self,
        request: Request<proto::ListConflictsRequest>,
    ) -> Result<Response<proto::ListConflictsResponse>, Status> {
        let req = request.into_inner();

        let status_filter = if req.status == 0 {
            None
        } else {
            Some(req.status)
        };
        let limit = if req.limit == 0 { 50 } else { req.limit };

        let (conflicts, total) = self
            .conflict_detector
            .list_conflicts(status_filter, limit, req.offset)
            .await;

        Ok(Response::new(proto::ListConflictsResponse {
            conflicts,
            total,
        }))
    }

    async fn resolve_conflict(
        &self,
        request: Request<proto::ResolveConflictRequest>,
    ) -> Result<Response<proto::ResolveConflictResponse>, Status> {
        let req = request.into_inner();

        let conflict = self
            .conflict_detector
            .get_conflict(&req.conflict_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!("Conflict '{}' not found", req.conflict_id))
            })?;

        let strategy = proto::ResolutionStrategy::try_from(req.strategy)
            .unwrap_or(proto::ResolutionStrategy::LastWriterWins);

        // Resolve
        let (resolved_record, reasoning) = ConflictResolver::resolve(&conflict.records, strategy);

        let resolution = ConflictResolver::make_resolution(
            strategy,
            resolved_record.clone(),
            reasoning,
            "system".to_string(),
        );

        // Update conflict status
        self.conflict_detector
            .resolve_conflict(
                &req.conflict_id,
                proto::ConflictStatus::AutoResolved as i32,
                Some(resolution),
            )
            .await;

        // Store the resolved record
        let _ = self.store.update(resolved_record.clone()).await;

        let updated_conflict = self.conflict_detector.get_conflict(&req.conflict_id).await;

        metrics::CONFLICTS_TOTAL.inc();

        Ok(Response::new(proto::ResolveConflictResponse {
            conflict: updated_conflict,
            resolved_record: Some(resolved_record),
        }))
    }

    async fn set_policy(
        &self,
        _request: Request<proto::SetPolicyRequest>,
    ) -> Result<Response<proto::SetPolicyResponse>, Status> {
        // Stub: policy storage not yet implemented in v0.1
        Ok(Response::new(proto::SetPolicyResponse { success: true }))
    }
}

/// gRPC implementation of ClusterService.
#[cfg(feature = "cluster")]
pub struct ClusterServiceImpl {
    cluster: Arc<ClusterNode>,
}

#[cfg(feature = "cluster")]
impl ClusterServiceImpl {
    pub fn new(cluster: Arc<ClusterNode>) -> Self {
        Self { cluster }
    }
}

#[cfg(feature = "cluster")]
#[tonic::async_trait]
impl proto::cluster_service_server::ClusterService for ClusterServiceImpl {
    async fn join(
        &self,
        request: Request<proto::JoinRequest>,
    ) -> Result<Response<proto::JoinResponse>, Status> {
        let req = request.into_inner();

        // Validate node_id format (alphanumeric + hyphens, max 64 chars)
        if req.node_id.is_empty() || req.node_id.len() > 64 {
            return Err(Status::invalid_argument("node_id must be 1-64 characters"));
        }
        if !req
            .node_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(Status::invalid_argument(
                "node_id contains invalid characters",
            ));
        }

        // handle_join validates cluster secret internally
        match self
            .cluster
            .handle_join(&req.node_id, &req.address, None)
            .await
        {
            Ok(status) => Ok(Response::new(proto::JoinResponse {
                accepted: true,
                cluster_status: Some(status),
            })),
            Err(reason) => Err(Status::permission_denied(reason)),
        }
    }

    async fn leave(
        &self,
        request: Request<proto::LeaveRequest>,
    ) -> Result<Response<proto::LeaveResponse>, Status> {
        let req = request.into_inner();
        let removed = self.cluster.handle_leave(&req.node_id).await;

        Ok(Response::new(proto::LeaveResponse {
            acknowledged: removed,
        }))
    }

    async fn status(
        &self,
        _request: Request<proto::StatusRequest>,
    ) -> Result<Response<proto::ClusterStatus>, Status> {
        let status = self.cluster.get_status().await;
        Ok(Response::new(status))
    }

    type SyncStream =
        Pin<Box<dyn Stream<Item = Result<proto::SyncEvent, Status>> + Send + 'static>>;

    async fn sync(
        &self,
        _request: Request<proto::SyncRequest>,
    ) -> Result<Response<Self::SyncStream>, Status> {
        // Stub: sync not implemented in single-node v0.1
        let stream = tokio_stream::empty();
        Ok(Response::new(Box::pin(stream)))
    }
}
