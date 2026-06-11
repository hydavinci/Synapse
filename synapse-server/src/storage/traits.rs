use anyhow::Result;
use async_trait::async_trait;

use crate::proto;

/// Core storage backend trait. All storage implementations must satisfy this.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    /// Store a new memory record. Returns the stored record with generated fields.
    async fn add(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord>;

    /// Get a record by ID.
    async fn get(&self, id: &str) -> Result<Option<proto::MemoryRecord>>;

    /// Update an existing record. Returns the updated record.
    async fn update(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord>;

    /// Delete a record by ID. Returns true if it existed.
    async fn delete(&self, id: &str) -> Result<bool>;

    /// Delete records matching a scope and optional time filter.
    async fn delete_by_scope(
        &self,
        scope: &proto::Scope,
        before: Option<prost_types::Timestamp>,
    ) -> Result<u64>;

    /// List records with optional filters (offset-based pagination).
    async fn list(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<proto::MemoryRecord>, u32)>;

    /// List records with cursor-based pagination.
    /// Returns (records, next_cursor). Cursor format: "created_at_secs:id"
    async fn list_with_cursor(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<(Vec<proto::MemoryRecord>, Option<String>)>;

    /// Get version history of a record.
    async fn history(&self, id: &str) -> Result<Vec<proto::MemoryRecord>>;

    /// Store a version snapshot (for history tracking).
    async fn store_version(&self, record: &proto::MemoryRecord) -> Result<()>;

    /// Get all records (for vector search). Returns (id, embedding) pairs.
    async fn get_all_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>>;

    /// Get records by IDs.
    async fn get_many(&self, ids: &[String]) -> Result<Vec<proto::MemoryRecord>>;

    /// Get total record count.
    async fn count(&self) -> Result<u64>;

    /// Delete expired records (where expires_at <= now).
    /// Returns the number of records deleted.
    async fn cleanup_expired(&self) -> Result<u64>;
}
