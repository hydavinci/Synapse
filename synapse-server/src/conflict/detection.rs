use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;

use crate::proto;
use crate::search::vector::cosine_similarity;

use super::store::ConflictStore;

/// Detects conflicts between memory records using vector clocks
/// and semantic similarity.
pub struct ConflictDetector {
    /// Stored conflicts: conflict_id -> Conflict (in-memory fallback)
    conflicts: Arc<RwLock<HashMap<String, proto::Conflict>>>,
    /// Semantic similarity threshold for considering records as targeting
    /// the same logical memory
    similarity_threshold: f32,
    /// Optional persistent conflict store
    persistent_store: Option<Arc<ConflictStore>>,
}

impl ConflictDetector {
    pub fn new(similarity_threshold: f32) -> Self {
        Self {
            conflicts: Arc::new(RwLock::new(HashMap::new())),
            similarity_threshold,
            persistent_store: None,
        }
    }

    /// Create a ConflictDetector with a persistent store for conflict durability.
    pub fn with_store(similarity_threshold: f32, store: Arc<ConflictStore>) -> Self {
        Self {
            conflicts: Arc::new(RwLock::new(HashMap::new())),
            similarity_threshold,
            persistent_store: Some(store),
        }
    }

    /// Check if two vector clocks are concurrent (neither dominates the other).
    /// Returns true if they conflict.
    pub fn clocks_are_concurrent(
        clock_a: &HashMap<String, u64>,
        clock_b: &HashMap<String, u64>,
    ) -> bool {
        let a_dominates = Self::clock_dominates(clock_a, clock_b);
        let b_dominates = Self::clock_dominates(clock_b, clock_a);
        // Concurrent = neither dominates
        !a_dominates && !b_dominates
    }

    /// Check if clock_a dominates clock_b (a >= b for all entries).
    fn clock_dominates(clock_a: &HashMap<String, u64>, clock_b: &HashMap<String, u64>) -> bool {
        // a dominates b if for all keys in b, a[key] >= b[key]
        // and for at least one key, a[key] > b[key]
        let mut strictly_greater = false;

        for (key, &val_b) in clock_b {
            let val_a = clock_a.get(key).copied().unwrap_or(0);
            if val_a < val_b {
                return false;
            }
            if val_a > val_b {
                strictly_greater = true;
            }
        }

        // Also check keys in a that aren't in b
        for (key, &val_a) in clock_a {
            if !clock_b.contains_key(key) && val_a > 0 {
                strictly_greater = true;
            }
        }

        strictly_greater
    }

    /// Detect whether a new record conflicts with an existing record.
    /// Conflict = semantically similar + concurrent vector clocks.
    pub fn detect(&self, existing: &proto::MemoryRecord, incoming: &proto::MemoryRecord) -> bool {
        // 1. Check semantic similarity (are they about the same thing?)
        let similarity = if !existing.embedding.is_empty() && !incoming.embedding.is_empty() {
            cosine_similarity(&existing.embedding, &incoming.embedding)
        } else {
            // Without embeddings, fall back to same-ID check
            if existing.id == incoming.id {
                1.0
            } else {
                0.0
            }
        };

        if similarity < self.similarity_threshold {
            debug!(
                similarity,
                threshold = self.similarity_threshold,
                "Records not similar enough for conflict"
            );
            return false;
        }

        // 2. Check vector clock concurrency
        let clock_a = existing
            .vector_clock
            .as_ref()
            .map(|vc| &vc.clock)
            .cloned()
            .unwrap_or_default();
        let clock_b = incoming
            .vector_clock
            .as_ref()
            .map(|vc| &vc.clock)
            .cloned()
            .unwrap_or_default();

        let concurrent = Self::clocks_are_concurrent(&clock_a, &clock_b);

        debug!(similarity, concurrent, "Conflict detection result");

        concurrent
    }

    /// Register a detected conflict.
    pub async fn register_conflict(&self, conflict: proto::Conflict) {
        // Persist if store is available
        if let Some(ref store) = self.persistent_store {
            if let Err(e) = store.save(&conflict).await {
                tracing::error!("Failed to persist conflict: {}", e);
            }
        }

        let mut conflicts = self.conflicts.write().await;
        conflicts.insert(conflict.id.clone(), conflict);
    }

    /// Get a conflict by ID.
    pub async fn get_conflict(&self, id: &str) -> Option<proto::Conflict> {
        // Try in-memory first
        let conflicts = self.conflicts.read().await;
        if let Some(c) = conflicts.get(id) {
            return Some(c.clone());
        }
        drop(conflicts);

        // Fall back to persistent store
        if let Some(ref store) = self.persistent_store {
            if let Ok(Some(c)) = store.get(id).await {
                return Some(c);
            }
        }

        None
    }

    /// List conflicts with optional status filter.
    pub async fn list_conflicts(
        &self,
        status_filter: Option<i32>,
        limit: u32,
        offset: u32,
    ) -> (Vec<proto::Conflict>, u32) {
        // If persistent store is available, use it
        if let Some(ref store) = self.persistent_store {
            if let Ok(result) = store.list(status_filter, limit, offset).await {
                return result;
            }
        }

        // Fall back to in-memory
        let conflicts = self.conflicts.read().await;

        let matching: Vec<&proto::Conflict> = conflicts
            .values()
            .filter(|c| {
                if let Some(status) = status_filter {
                    c.status == status
                } else {
                    true
                }
            })
            .collect();

        let total = matching.len() as u32;
        let results: Vec<proto::Conflict> = matching
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .cloned()
            .collect();

        (results, total)
    }

    /// Update a conflict's status and resolution.
    pub async fn resolve_conflict(
        &self,
        conflict_id: &str,
        status: i32,
        resolution: Option<proto::Resolution>,
    ) -> Option<proto::Conflict> {
        // Update persistent store if available
        if let Some(ref store) = self.persistent_store {
            let _ = store
                .update_status(conflict_id, status, resolution.as_ref())
                .await;
        }

        let mut conflicts = self.conflicts.write().await;
        if let Some(conflict) = conflicts.get_mut(conflict_id) {
            conflict.status = status;
            conflict.resolution = resolution;
            Some(conflict.clone())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clock_dominates() {
        let mut a = HashMap::new();
        a.insert("node1".to_string(), 3);
        a.insert("node2".to_string(), 2);

        let mut b = HashMap::new();
        b.insert("node1".to_string(), 2);
        b.insert("node2".to_string(), 1);

        assert!(ConflictDetector::clock_dominates(&a, &b));
        assert!(!ConflictDetector::clock_dominates(&b, &a));
    }

    #[test]
    fn test_concurrent_clocks() {
        let mut a = HashMap::new();
        a.insert("node1".to_string(), 3);
        a.insert("node2".to_string(), 2);

        let mut b = HashMap::new();
        b.insert("node1".to_string(), 2);
        b.insert("node2".to_string(), 4);

        assert!(ConflictDetector::clocks_are_concurrent(&a, &b));
    }

    #[test]
    fn test_non_concurrent_clocks() {
        let mut a = HashMap::new();
        a.insert("node1".to_string(), 3);
        a.insert("node2".to_string(), 2);

        let mut b = HashMap::new();
        b.insert("node1".to_string(), 2);
        b.insert("node2".to_string(), 1);

        // a dominates b, so not concurrent
        assert!(!ConflictDetector::clocks_are_concurrent(&a, &b));
    }
}
