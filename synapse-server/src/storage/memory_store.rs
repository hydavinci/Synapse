use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::proto;
use crate::storage::traits::StorageBackend;

/// In-memory storage backend using HashMap for primary storage
/// and BTreeMap for time-based indexing.
pub struct InMemoryStore {
    /// Primary storage: id -> MemoryRecord
    records: Arc<RwLock<HashMap<String, proto::MemoryRecord>>>,
    /// Time index: (created_at_nanos, id) for time-ordered queries
    time_index: Arc<RwLock<BTreeMap<(i64, String), String>>>,
    /// Version history: id -> Vec<MemoryRecord>
    history: Arc<RwLock<HashMap<String, Vec<proto::MemoryRecord>>>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            time_index: Arc::new(RwLock::new(BTreeMap::new())),
            history: Arc::new(RwLock::new(HashMap::new())),
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

        let mut records = self.records.write().await;
        if records.contains_key(&id) {
            return Err(anyhow!("Record with id '{}' already exists", id));
        }

        let mut time_idx = self.time_index.write().await;
        time_idx.insert((time_key, id.clone()), id.clone());
        records.insert(id, record.clone());

        Ok(record)
    }

    async fn get(&self, id: &str) -> Result<Option<proto::MemoryRecord>> {
        let records = self.records.read().await;
        Ok(records.get(id).cloned())
    }

    async fn update(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let id = record.id.clone();
        let mut records = self.records.write().await;

        if !records.contains_key(&id) {
            return Err(anyhow!("Record '{}' not found", id));
        }

        records.insert(id, record.clone());
        Ok(record)
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let mut records = self.records.write().await;
        let removed = records.remove(id);

        if let Some(ref rec) = removed {
            let time_key = Self::timestamp_nanos(&rec.created_at);
            let mut time_idx = self.time_index.write().await;
            time_idx.remove(&(time_key, id.to_string()));
        }

        // Also remove history
        let mut hist = self.history.write().await;
        hist.remove(id);

        Ok(removed.is_some())
    }

    async fn delete_by_scope(
        &self,
        scope: &proto::Scope,
        before: Option<prost_types::Timestamp>,
    ) -> Result<u64> {
        let mut records = self.records.write().await;
        let mut time_idx = self.time_index.write().await;

        let before_nanos = before
            .as_ref()
            .map(|t| t.seconds * 1_000_000_000 + t.nanos as i64);

        let mut to_remove = Vec::new();

        for (id, record) in records.iter() {
            if let Some(ref rec_scope) = record.scope {
                if scope_matches(rec_scope, scope) {
                    if let Some(cutoff) = before_nanos {
                        let rec_time = Self::timestamp_nanos(&record.created_at);
                        if rec_time < cutoff {
                            to_remove.push(id.clone());
                        }
                    } else {
                        to_remove.push(id.clone());
                    }
                }
            }
        }

        let count = to_remove.len() as u64;
        for id in &to_remove {
            if let Some(rec) = records.remove(id) {
                let time_key = Self::timestamp_nanos(&rec.created_at);
                time_idx.remove(&(time_key, id.clone()));
            }
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
        let records = self.records.read().await;

        let mut matching: Vec<&proto::MemoryRecord> = records
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
        let hist = self.history.read().await;
        Ok(hist.get(id).cloned().unwrap_or_default())
    }

    async fn store_version(&self, record: &proto::MemoryRecord) -> Result<()> {
        let mut hist = self.history.write().await;
        hist.entry(record.id.clone())
            .or_default()
            .push(record.clone());
        Ok(())
    }

    async fn get_all_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>> {
        let records = self.records.read().await;
        let embeddings: Vec<(String, Vec<f32>)> = records
            .values()
            .filter(|r| !r.embedding.is_empty())
            .map(|r| (r.id.clone(), r.embedding.clone()))
            .collect();
        Ok(embeddings)
    }

    async fn get_many(&self, ids: &[String]) -> Result<Vec<proto::MemoryRecord>> {
        let records = self.records.read().await;
        let results: Vec<proto::MemoryRecord> = ids
            .iter()
            .filter_map(|id| records.get(id).cloned())
            .collect();
        Ok(results)
    }

    async fn count(&self) -> Result<u64> {
        let records = self.records.read().await;
        Ok(records.len() as u64)
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
