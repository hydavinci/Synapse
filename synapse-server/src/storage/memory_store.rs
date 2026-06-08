use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::proto;
use crate::storage::traits::StorageBackend;

/// In-memory storage backend.
///
/// Uses a single RwLock over a combined state struct to ensure atomic operations
/// across records, time index, and history.
struct StoreState {
    /// Primary storage: id -> MemoryRecord
    records: HashMap<String, proto::MemoryRecord>,
    /// Time index: (created_at_nanos, id) for time-ordered queries
    time_index: BTreeMap<(i64, String), String>,
    /// Version history: id -> Vec<MemoryRecord>
    history: HashMap<String, Vec<proto::MemoryRecord>>,
}

pub struct InMemoryStore {
    state: Arc<RwLock<StoreState>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(StoreState {
                records: HashMap::new(),
                time_index: BTreeMap::new(),
                history: HashMap::new(),
            })),
        }
    }

    fn timestamp_nanos(ts: &Option<prost_types::Timestamp>) -> i64 {
        ts.as_ref()
            .map(|t| t.seconds * 1_000_000_000 + t.nanos as i64)
            .unwrap_or(0)
    }
}

impl Default for InMemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for InMemoryStore {
    async fn add(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let id = record.id.clone();
        let time_key = Self::timestamp_nanos(&record.created_at);

        let mut state = self.state.write().await;
        if state.records.contains_key(&id) {
            return Err(anyhow!("Record with id '{}' already exists", id));
        }

        state.time_index.insert((time_key, id.clone()), id.clone());
        state.records.insert(id, record.clone());

        Ok(record)
    }

    async fn get(&self, id: &str) -> Result<Option<proto::MemoryRecord>> {
        let state = self.state.read().await;
        Ok(state.records.get(id).cloned())
    }

    async fn update(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let id = record.id.clone();
        let mut state = self.state.write().await;

        if !state.records.contains_key(&id) {
            return Err(anyhow!("Record '{}' not found", id));
        }

        state.records.insert(id, record.clone());
        Ok(record)
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let mut state = self.state.write().await;
        let removed = state.records.remove(id);

        if let Some(ref rec) = removed {
            let time_key = Self::timestamp_nanos(&rec.created_at);
            state.time_index.remove(&(time_key, id.to_string()));
        }

        // Also remove history
        state.history.remove(id);

        Ok(removed.is_some())
    }

    async fn delete_by_scope(
        &self,
        scope: &proto::Scope,
        before: Option<prost_types::Timestamp>,
    ) -> Result<u64> {
        let mut state = self.state.write().await;

        let before_nanos = before
            .as_ref()
            .map(|t| t.seconds * 1_000_000_000 + t.nanos as i64);

        // CVE-12 fix: Only delete records that are *visible* to the requesting scope.
        // This prevents cross-scope deletion of PRIVATE records.
        let to_remove: Vec<String> = state.records
            .iter()
            .filter(|(_id, record)| {
                if let Some(ref rec_scope) = record.scope {
                    // Must match scope fields
                    if !scope_matches(rec_scope, scope) {
                        return false;
                    }
                    // Must be visible from the requesting scope
                    if !scope_is_visible_for_delete(rec_scope, scope) {
                        return false;
                    }
                    // Time filter
                    if let Some(cutoff) = before_nanos {
                        let rec_time = Self::timestamp_nanos(&record.created_at);
                        return rec_time < cutoff;
                    }
                    true
                } else {
                    false
                }
            })
            .map(|(id, _)| id.clone())
            .collect();

        let count = to_remove.len() as u64;
        for id in &to_remove {
            if let Some(rec) = state.records.remove(id) {
                let time_key = Self::timestamp_nanos(&rec.created_at);
                state.time_index.remove(&(time_key, id.clone()));
            }
            state.history.remove(id);
        }

        Ok(count)
    }

    async fn list(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<proto::MemoryRecord>, u32)> {
        let state = self.state.read().await;

        let mut matching: Vec<&proto::MemoryRecord> = state.records
            .values()
            .filter(|r| {
                // Scope filter
                if let Some(s) = scope {
                    if let Some(ref rs) = r.scope {
                        if !scope_matches(rs, s) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                // Kind filter
                if !kinds.is_empty() && !kinds.contains(&r.kind) {
                    return false;
                }
                // Tag filter (AND)
                if !tags.is_empty() {
                    for tag in tags {
                        if !r.tags.contains(tag) {
                            return false;
                        }
                    }
                }
                true
            })
            .collect();

        let total = matching.len() as u32;

        // Sort by created_at descending
        matching.sort_by(|a, b| {
            let ta = Self::timestamp_nanos(&a.created_at);
            let tb = Self::timestamp_nanos(&b.created_at);
            tb.cmp(&ta)
        });

        let results: Vec<proto::MemoryRecord> = matching
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .cloned()
            .collect();

        Ok((results, total))
    }

    async fn history(&self, id: &str) -> Result<Vec<proto::MemoryRecord>> {
        let state = self.state.read().await;
        Ok(state.history.get(id).cloned().unwrap_or_default())
    }

    async fn store_version(&self, record: &proto::MemoryRecord) -> Result<()> {
        let mut state = self.state.write().await;
        state.history
            .entry(record.id.clone())
            .or_default()
            .push(record.clone());
        Ok(())
    }

    async fn get_all_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>> {
        let state = self.state.read().await;
        let embeddings: Vec<(String, Vec<f32>)> = state.records
            .values()
            .filter(|r| !r.embedding.is_empty())
            .map(|r| (r.id.clone(), r.embedding.clone()))
            .collect();
        Ok(embeddings)
    }

    async fn get_many(&self, ids: &[String]) -> Result<Vec<proto::MemoryRecord>> {
        let state = self.state.read().await;
        let results: Vec<proto::MemoryRecord> = ids
            .iter()
            .filter_map(|id| state.records.get(id).cloned())
            .collect();
        Ok(results)
    }

    async fn count(&self) -> Result<u64> {
        let state = self.state.read().await;
        Ok(state.records.len() as u64)
    }
}

/// Check if a record's scope matches a query scope.
/// Empty fields in the query scope are treated as wildcards.
fn scope_matches(record_scope: &proto::Scope, query_scope: &proto::Scope) -> bool {
    if !query_scope.org.is_empty() && record_scope.org != query_scope.org {
        return false;
    }
    if !query_scope.team.is_empty() && record_scope.team != query_scope.team {
        return false;
    }
    if !query_scope.agent.is_empty() && record_scope.agent != query_scope.agent {
        return false;
    }
    if !query_scope.user.is_empty() && record_scope.user != query_scope.user {
        return false;
    }
    if !query_scope.session.is_empty() && record_scope.session != query_scope.session {
        return false;
    }
    true
}

/// CVE-12 fix: Check if the requesting scope has permission to *delete* a record.
/// Only the owner (exact match) or a scope that is a parent with SCOPE_DOWN visibility
/// can delete records. PUBLIC/SHARED records can only be deleted by exact scope match.
fn scope_is_visible_for_delete(record_scope: &proto::Scope, requester_scope: &proto::Scope) -> bool {
    use crate::proto::Visibility;

    let vis = Visibility::try_from(record_scope.visibility)
        .unwrap_or(Visibility::Private);

    match vis {
        // PRIVATE: only exact owner can delete
        Visibility::Private | Visibility::VisibilityUnspecified => {
            exact_scope_match(record_scope, requester_scope)
        }
        // SCOPE_UP: owner or parent can delete
        Visibility::ScopeUp => {
            exact_scope_match(record_scope, requester_scope)
                || is_parent_scope(requester_scope, record_scope)
        }
        // SCOPE_DOWN: owner or child can delete their inherited view
        // but only owner should delete the source record
        Visibility::ScopeDown => {
            exact_scope_match(record_scope, requester_scope)
        }
        // SHARED: only within same org and exact match
        Visibility::Shared => {
            exact_scope_match(record_scope, requester_scope)
        }
        // PUBLIC: only exact owner can delete
        Visibility::Public => {
            exact_scope_match(record_scope, requester_scope)
        }
    }
}

/// Exact scope match (all non-empty fields of requester must match record).
fn exact_scope_match(record_scope: &proto::Scope, requester_scope: &proto::Scope) -> bool {
    // Requester must specify all fields that the record has
    if !record_scope.org.is_empty() && record_scope.org != requester_scope.org {
        return false;
    }
    if !record_scope.team.is_empty() && record_scope.team != requester_scope.team {
        return false;
    }
    if !record_scope.agent.is_empty() && record_scope.agent != requester_scope.agent {
        return false;
    }
    if !record_scope.user.is_empty() && record_scope.user != requester_scope.user {
        return false;
    }
    if !record_scope.session.is_empty() && record_scope.session != requester_scope.session {
        return false;
    }
    true
}

/// Check if parent scope is a proper parent of child scope.
fn is_parent_scope(parent: &proto::Scope, child: &proto::Scope) -> bool {
    let parent_depth = scope_depth(parent);
    let child_depth = scope_depth(child);
    if parent_depth >= child_depth {
        return false;
    }
    // All parent fields must match child
    if !parent.org.is_empty() && parent.org != child.org {
        return false;
    }
    if !parent.team.is_empty() && parent.team != child.team {
        return false;
    }
    if !parent.agent.is_empty() && parent.agent != child.agent {
        return false;
    }
    if !parent.user.is_empty() && parent.user != child.user {
        return false;
    }
    true
}

fn scope_depth(scope: &proto::Scope) -> u8 {
    let mut d = 0u8;
    if !scope.org.is_empty() { d += 1; }
    if !scope.team.is_empty() { d += 1; }
    if !scope.agent.is_empty() { d += 1; }
    if !scope.user.is_empty() { d += 1; }
    if !scope.session.is_empty() { d += 1; }
    d
}
