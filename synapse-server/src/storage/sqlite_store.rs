use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use prost_types::Timestamp;
use rusqlite::{params, Connection};
use tokio::sync::Mutex;

use crate::proto;
use crate::storage::traits::StorageBackend;

/// Number of read connections in the pool.
const READ_POOL_SIZE: usize = 4;

/// SQLite persistent storage backend with read/write separation.
///
/// Uses a single write connection (serialized through Mutex) and a pool of
/// read connections with round-robin selection for concurrent reads.
/// All connections use WAL mode for optimal read concurrency.
///
/// ## Future Migration Path
/// The current approach (Arc<Mutex<Connection>> with manual round-robin) works
/// well for moderate concurrency. For higher-throughput deployments, consider
/// migrating to `deadpool-sqlite` or `r2d2-sqlite` for automatic pool management,
/// connection recycling, and configurable pool sizing with backpressure.
pub struct SqliteStore {
    /// Dedicated write connection (single writer, as SQLite requires)
    write_conn: Arc<Mutex<Connection>>,
    /// Pool of read connections for concurrent read operations
    read_pool: Vec<Arc<Mutex<Connection>>>,
    /// Round-robin counter for read connection selection
    read_counter: AtomicUsize,
}

impl SqliteStore {
    pub fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create write connection
        let write_conn = Self::open_connection(&path, true)?;

        // Create read pool
        let mut read_pool = Vec::with_capacity(READ_POOL_SIZE);
        for _ in 0..READ_POOL_SIZE {
            let conn = Self::open_connection(&path, false)?;
            read_pool.push(Arc::new(Mutex::new(conn)));
        }

        let store = Self {
            write_conn: Arc::new(Mutex::new(write_conn)),
            read_pool,
            read_counter: AtomicUsize::new(0),
        };
        store.init_schema_sync()?;
        Ok(store)
    }

    /// Open a SQLite connection with proper pragmas.
    /// `is_writer` determines if this is the write connection.
    fn open_connection(path: &PathBuf, is_writer: bool) -> Result<Connection> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA busy_timeout=5000;
             PRAGMA synchronous=NORMAL;
             PRAGMA foreign_keys=ON;",
        )?;

        if !is_writer {
            // Read connections can use a more relaxed isolation
            conn.execute_batch("PRAGMA query_only=ON;")?;
        }

        Ok(conn)
    }

    /// Get a read connection from the pool using round-robin.
    fn get_read_conn(&self) -> &Arc<Mutex<Connection>> {
        let idx = self.read_counter.fetch_add(1, AtomicOrdering::Relaxed) % READ_POOL_SIZE;
        &self.read_pool[idx]
    }

    fn init_schema_sync(&self) -> Result<()> {
        // We need blocking access here since this is called from new()
        let conn = self.write_conn.blocking_lock();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS memories (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                embedding BLOB,
                scope_org TEXT DEFAULT '',
                scope_team TEXT DEFAULT '',
                scope_user TEXT DEFAULT '',
                scope_agent TEXT DEFAULT '',
                scope_session TEXT DEFAULT '',
                scope_visibility INTEGER DEFAULT 0,
                kind INTEGER DEFAULT 0,
                confidence REAL DEFAULT 1.0,
                tags TEXT DEFAULT '[]',
                source_agent_id TEXT DEFAULT '',
                source_session_id TEXT DEFAULT '',
                version INTEGER DEFAULT 1,
                created_at_secs INTEGER DEFAULT 0,
                created_at_nanos INTEGER DEFAULT 0,
                updated_at_secs INTEGER DEFAULT 0,
                updated_at_nanos INTEGER DEFAULT 0,
                accessed_at_secs INTEGER DEFAULT 0,
                accessed_at_nanos INTEGER DEFAULT 0,
                expires_at_secs INTEGER,
                expires_at_nanos INTEGER,
                vector_clock TEXT DEFAULT '{}',
                lineage TEXT DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS memory_history (
                id TEXT NOT NULL,
                version INTEGER NOT NULL,
                content TEXT NOT NULL,
                tags TEXT DEFAULT '[]',
                kind INTEGER DEFAULT 0,
                updated_at_secs INTEGER DEFAULT 0,
                updated_at_nanos INTEGER DEFAULT 0,
                PRIMARY KEY (id, version)
            );

            CREATE TABLE IF NOT EXISTS memory_tags (
                memory_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY(memory_id, tag)
            );

            CREATE INDEX IF NOT EXISTS idx_memory_tags_tag ON memory_tags(tag);

            CREATE INDEX IF NOT EXISTS idx_memories_scope
                ON memories(scope_org, scope_team, scope_user, scope_agent);
            CREATE INDEX IF NOT EXISTS idx_memories_kind ON memories(kind);
            CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at_secs);
            CREATE INDEX IF NOT EXISTS idx_memories_expires ON memories(expires_at_secs);",
        )?;
        Ok(())
    }

    fn ts_to_proto(secs: i64, nanos: i32) -> Option<Timestamp> {
        if secs == 0 && nanos == 0 {
            None
        } else {
            Some(Timestamp {
                seconds: secs,
                nanos,
            })
        }
    }

    fn proto_to_ts(ts: &Option<Timestamp>) -> (i64, i32) {
        ts.as_ref().map(|t| (t.seconds, t.nanos)).unwrap_or((0, 0))
    }

    fn row_to_record(row: &rusqlite::Row) -> rusqlite::Result<proto::MemoryRecord> {
        let tags_json: String = row.get("tags")?;
        let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();

        let clock_json: String = row.get("vector_clock")?;
        let clock: std::collections::HashMap<String, u64> =
            serde_json::from_str(&clock_json).unwrap_or_default();

        let lineage_json: String = row.get("lineage")?;
        let lineage: Vec<String> = serde_json::from_str(&lineage_json).unwrap_or_default();

        let embedding_blob: Option<Vec<u8>> = row.get("embedding")?;
        let embedding: Vec<f32> = embedding_blob
            .map(|b| {
                b.chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect()
            })
            .unwrap_or_default();

        let created_at_secs: i64 = row.get("created_at_secs")?;
        let created_at_nanos: i32 = row.get("created_at_nanos")?;
        let updated_at_secs: i64 = row.get("updated_at_secs")?;
        let updated_at_nanos: i32 = row.get("updated_at_nanos")?;
        let accessed_at_secs: i64 = row.get("accessed_at_secs")?;
        let accessed_at_nanos: i32 = row.get("accessed_at_nanos")?;
        let expires_at_secs: Option<i64> = row.get("expires_at_secs")?;
        let expires_at_nanos: Option<i32> = row.get("expires_at_nanos")?;

        Ok(proto::MemoryRecord {
            id: row.get("id")?,
            content: row.get("content")?,
            embedding,
            scope: Some(proto::Scope {
                org: row.get("scope_org")?,
                team: row.get("scope_team")?,
                user: row.get("scope_user")?,
                agent: row.get("scope_agent")?,
                session: row.get("scope_session")?,
                visibility: row.get("scope_visibility")?,
            }),
            kind: row.get("kind")?,
            confidence: row.get("confidence")?,
            tags,
            source: Some(proto::Source {
                agent_id: row.get("source_agent_id")?,
                session_id: row.get("source_session_id")?,
                input_hash: String::new(),
                tool_call_id: String::new(),
            }),
            version: row.get("version")?,
            created_at: Self::ts_to_proto(created_at_secs, created_at_nanos),
            updated_at: Self::ts_to_proto(updated_at_secs, updated_at_nanos),
            accessed_at: Self::ts_to_proto(accessed_at_secs, accessed_at_nanos),
            expires_at: expires_at_secs
                .zip(expires_at_nanos)
                .and_then(|(s, n)| Self::ts_to_proto(s, n)),
            vector_clock: Some(proto::VectorClock { clock }),
            lineage,
        })
    }

    /// Insert tags into the memory_tags table for a given memory ID.
    fn insert_tags(conn: &Connection, memory_id: &str, tags: &[String]) -> Result<()> {
        for tag in tags {
            conn.execute(
                "INSERT OR IGNORE INTO memory_tags (memory_id, tag) VALUES (?1, ?2)",
                params![memory_id, tag],
            )?;
        }
        Ok(())
    }

    /// Remove all tags for a given memory ID from the memory_tags table.
    fn delete_tags(conn: &Connection, memory_id: &str) -> Result<()> {
        conn.execute(
            "DELETE FROM memory_tags WHERE memory_id = ?1",
            params![memory_id],
        )?;
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for SqliteStore {
    async fn add(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let conn = self.write_conn.clone();
        let record_clone = record.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();

            let scope = record_clone.scope.as_ref().cloned().unwrap_or_default();
            let source = record_clone.source.as_ref().cloned().unwrap_or_default();
            let (created_secs, created_nanos) = SqliteStore::proto_to_ts(&record_clone.created_at);
            let (updated_secs, updated_nanos) = SqliteStore::proto_to_ts(&record_clone.updated_at);
            let (accessed_secs, accessed_nanos) = SqliteStore::proto_to_ts(&record_clone.accessed_at);
            let (expires_secs, expires_nanos) = record_clone
                .expires_at
                .as_ref()
                .map(|t| (Some(t.seconds), Some(t.nanos)))
                .unwrap_or((None, None));

            let tags_json = serde_json::to_string(&record_clone.tags)?;
            let clock_json = serde_json::to_string(
                &record_clone
                    .vector_clock
                    .as_ref()
                    .map(|vc| &vc.clock)
                    .cloned()
                    .unwrap_or_default(),
            )?;
            let lineage_json = serde_json::to_string(&record_clone.lineage)?;

            let embedding_blob: Option<Vec<u8>> = if record_clone.embedding.is_empty() {
                None
            } else {
                Some(
                    record_clone
                        .embedding
                        .iter()
                        .flat_map(|f| f.to_le_bytes())
                        .collect(),
                )
            };

            conn.execute(
                "INSERT INTO memories (id, content, embedding, scope_org, scope_team, scope_user,
                 scope_agent, scope_session, scope_visibility, kind, confidence, tags,
                 source_agent_id, source_session_id, version,
                 created_at_secs, created_at_nanos, updated_at_secs, updated_at_nanos,
                 accessed_at_secs, accessed_at_nanos, expires_at_secs, expires_at_nanos,
                 vector_clock, lineage)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25)",
                params![
                    record_clone.id,
                    record_clone.content,
                    embedding_blob,
                    scope.org,
                    scope.team,
                    scope.user,
                    scope.agent,
                    scope.session,
                    scope.visibility,
                    record_clone.kind,
                    record_clone.confidence,
                    tags_json,
                    source.agent_id,
                    source.session_id,
                    record_clone.version,
                    created_secs,
                    created_nanos,
                    updated_secs,
                    updated_nanos,
                    accessed_secs,
                    accessed_nanos,
                    expires_secs,
                    expires_nanos,
                    clock_json,
                    lineage_json,
                ],
            )
            .map_err(|e| anyhow!("SQLite insert failed: {}", e))?;

            // Insert into memory_tags
            SqliteStore::insert_tags(&conn, &record_clone.id, &record_clone.tags)?;

            Ok(record_clone)
        })
        .await
        .unwrap()
    }

    async fn get(&self, id: &str) -> Result<Option<proto::MemoryRecord>> {
        let conn = self.get_read_conn().clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare("SELECT * FROM memories WHERE id = ?1")?;
            let result = stmt
                .query_row(params![id], SqliteStore::row_to_record)
                .optional()
                .map_err(|e| anyhow!("SQLite get failed: {}", e))?;
            Ok(result)
        })
        .await
        .unwrap()
    }

    async fn update(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let conn = self.write_conn.clone();
        let record_clone = record.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();

            let scope = record_clone.scope.as_ref().cloned().unwrap_or_default();
            let (updated_secs, updated_nanos) = SqliteStore::proto_to_ts(&record_clone.updated_at);
            let tags_json = serde_json::to_string(&record_clone.tags)?;
            let clock_json = serde_json::to_string(
                &record_clone
                    .vector_clock
                    .as_ref()
                    .map(|vc| &vc.clock)
                    .cloned()
                    .unwrap_or_default(),
            )?;

            let embedding_blob: Option<Vec<u8>> = if record_clone.embedding.is_empty() {
                None
            } else {
                Some(
                    record_clone
                        .embedding
                        .iter()
                        .flat_map(|f| f.to_le_bytes())
                        .collect(),
                )
            };

            let rows = conn.execute(
                "UPDATE memories SET content=?1, embedding=?2, scope_org=?3, scope_team=?4,
                 scope_user=?5, scope_agent=?6, scope_visibility=?7, kind=?8,
                 confidence=?9, tags=?10, version=?11, updated_at_secs=?12,
                 updated_at_nanos=?13, vector_clock=?14
                 WHERE id=?15",
                params![
                    record_clone.content,
                    embedding_blob,
                    scope.org,
                    scope.team,
                    scope.user,
                    scope.agent,
                    scope.visibility,
                    record_clone.kind,
                    record_clone.confidence,
                    tags_json,
                    record_clone.version,
                    updated_secs,
                    updated_nanos,
                    clock_json,
                    record_clone.id,
                ],
            )?;

            if rows == 0 {
                return Err(anyhow!("Record '{}' not found", record_clone.id));
            }

            // Update memory_tags: delete old, insert new
            SqliteStore::delete_tags(&conn, &record_clone.id)?;
            SqliteStore::insert_tags(&conn, &record_clone.id, &record_clone.tags)?;

            Ok(record_clone)
        })
        .await
        .unwrap()
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        let conn = self.write_conn.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let rows = conn.execute("DELETE FROM memories WHERE id = ?1", params![id])?;
            conn.execute("DELETE FROM memory_history WHERE id = ?1", params![id])?;
            // Cascade delete from memory_tags
            conn.execute("DELETE FROM memory_tags WHERE memory_id = ?1", params![id])?;
            Ok(rows > 0)
        })
        .await
        .unwrap()
    }

    async fn delete_by_scope(
        &self,
        scope: &proto::Scope,
        before: Option<Timestamp>,
    ) -> Result<u64> {
        let conn = self.write_conn.clone();
        let scope = scope.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();

            let mut conditions = Vec::new();
            let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if !scope.org.is_empty() {
                conditions.push("scope_org = ?");
                param_values.push(Box::new(scope.org.clone()));
            }
            if !scope.team.is_empty() {
                conditions.push("scope_team = ?");
                param_values.push(Box::new(scope.team.clone()));
            }
            if !scope.user.is_empty() {
                conditions.push("scope_user = ?");
                param_values.push(Box::new(scope.user.clone()));
            }
            if !scope.agent.is_empty() {
                conditions.push("scope_agent = ?");
                param_values.push(Box::new(scope.agent.clone()));
            }

            if let Some(ref ts) = before {
                conditions.push("created_at_secs < ?");
                param_values.push(Box::new(ts.seconds));
            }

            if conditions.is_empty() {
                return Ok(0);
            }

            let where_clause = conditions.join(" AND ");

            // First, delete from memory_tags for affected rows
            let select_sql = format!("SELECT id FROM memories WHERE {}", where_clause);
            let params_ref: Vec<&dyn rusqlite::ToSql> =
                param_values.iter().map(|p| p.as_ref()).collect();
            let mut stmt = conn.prepare(&select_sql)?;
            let ids: Vec<String> = stmt
                .query_map(params_ref.as_slice(), |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            for id in &ids {
                conn.execute("DELETE FROM memory_tags WHERE memory_id = ?1", params![id])?;
            }

            let sql = format!("DELETE FROM memories WHERE {}", where_clause);
            let params_ref: Vec<&dyn rusqlite::ToSql> =
                param_values.iter().map(|p| p.as_ref()).collect();
            let rows = conn.execute(&sql, params_ref.as_slice())?;

            Ok(rows as u64)
        })
        .await
        .unwrap()
    }

    async fn list(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<proto::MemoryRecord>, u32)> {
        let conn = self.get_read_conn().clone();
        let scope = scope.cloned();
        let kinds = kinds.to_vec();
        let tags = tags.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();

            let mut conditions: Vec<String> = vec!["1=1".to_string()];
            let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            if let Some(ref s) = scope {
                if !s.org.is_empty() {
                    conditions.push("m.scope_org = ?".to_string());
                    param_values.push(Box::new(s.org.clone()));
                }
                if !s.team.is_empty() {
                    conditions.push("m.scope_team = ?".to_string());
                    param_values.push(Box::new(s.team.clone()));
                }
                if !s.user.is_empty() {
                    conditions.push("m.scope_user = ?".to_string());
                    param_values.push(Box::new(s.user.clone()));
                }
                if !s.agent.is_empty() {
                    conditions.push("m.scope_agent = ?".to_string());
                    param_values.push(Box::new(s.agent.clone()));
                }
            }

            if !kinds.is_empty() {
                let placeholders: Vec<&str> = kinds.iter().map(|_| "?").collect();
                conditions.push(format!("m.kind IN ({})", placeholders.join(",")));
                for k in &kinds {
                    param_values.push(Box::new(*k));
                }
            }

            let where_clause = conditions.join(" AND ");

            // If tags are provided, use JOIN with memory_tags for efficient SQL filtering
            if !tags.is_empty() {
                let tag_placeholders: Vec<String> = tags.iter().map(|_| "?".to_string()).collect();
                let tag_count = tags.len() as u32;

                // Build params for the base WHERE clause + tag IN clause + HAVING count
                // For count query: base_params + tag_params + tag_count
                let mut count_param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
                if let Some(ref s) = scope {
                    if !s.org.is_empty() { count_param_values.push(Box::new(s.org.clone())); }
                    if !s.team.is_empty() { count_param_values.push(Box::new(s.team.clone())); }
                    if !s.user.is_empty() { count_param_values.push(Box::new(s.user.clone())); }
                    if !s.agent.is_empty() { count_param_values.push(Box::new(s.agent.clone())); }
                }
                for k in &kinds {
                    count_param_values.push(Box::new(*k));
                }
                for tag in &tags {
                    count_param_values.push(Box::new(tag.clone()));
                }
                count_param_values.push(Box::new(tag_count));

                let count_sql = format!(
                    "SELECT COUNT(*) FROM (
                        SELECT m.id FROM memories m
                        INNER JOIN memory_tags mt ON m.id = mt.memory_id
                        WHERE {} AND mt.tag IN ({})
                        GROUP BY m.id
                        HAVING COUNT(DISTINCT mt.tag) = ?
                    )",
                    where_clause,
                    tag_placeholders.join(",")
                );
                let count_params_ref: Vec<&dyn rusqlite::ToSql> =
                    count_param_values.iter().map(|p| p.as_ref()).collect();
                let total: u32 = conn.query_row(&count_sql, count_params_ref.as_slice(), |row| row.get(0))?;

                // Fetch page with tag filter
                // Params: base_params + tag_params + tag_count + limit + offset
                let mut fetch_param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
                if let Some(ref s) = scope {
                    if !s.org.is_empty() { fetch_param_values.push(Box::new(s.org.clone())); }
                    if !s.team.is_empty() { fetch_param_values.push(Box::new(s.team.clone())); }
                    if !s.user.is_empty() { fetch_param_values.push(Box::new(s.user.clone())); }
                    if !s.agent.is_empty() { fetch_param_values.push(Box::new(s.agent.clone())); }
                }
                for k in &kinds {
                    fetch_param_values.push(Box::new(*k));
                }
                for tag in &tags {
                    fetch_param_values.push(Box::new(tag.clone()));
                }
                fetch_param_values.push(Box::new(tag_count));
                fetch_param_values.push(Box::new(limit));
                fetch_param_values.push(Box::new(offset));

                let sql = format!(
                    "SELECT m.* FROM memories m
                     INNER JOIN memory_tags mt ON m.id = mt.memory_id
                     WHERE {} AND mt.tag IN ({})
                     GROUP BY m.id
                     HAVING COUNT(DISTINCT mt.tag) = ?
                     ORDER BY m.created_at_secs DESC
                     LIMIT ? OFFSET ?",
                    where_clause,
                    tag_placeholders.join(",")
                );
                let params_ref: Vec<&dyn rusqlite::ToSql> =
                    fetch_param_values.iter().map(|p| p.as_ref()).collect();

                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt
                    .query_map(params_ref.as_slice(), SqliteStore::row_to_record)?
                    .filter_map(|r| r.ok())
                    .collect::<Vec<_>>();

                Ok((rows, total))
            } else {
                // No tag filter - simple query
                let count_sql = format!("SELECT COUNT(*) FROM memories m WHERE {}", where_clause);
                let count_params: Vec<&dyn rusqlite::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();
                let total: u32 = conn.query_row(&count_sql, count_params.as_slice(), |row| row.get(0))?;

                // Fetch page
                let sql = format!(
                    "SELECT * FROM memories m WHERE {} ORDER BY m.created_at_secs DESC LIMIT ? OFFSET ?",
                    where_clause
                );
                param_values.push(Box::new(limit));
                param_values.push(Box::new(offset));
                let params_ref: Vec<&dyn rusqlite::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();

                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt
                    .query_map(params_ref.as_slice(), SqliteStore::row_to_record)?
                    .filter_map(|r| r.ok())
                    .collect::<Vec<_>>();

                Ok((rows, total))
            }
        })
        .await
        .unwrap()
    }

    async fn list_with_cursor(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<(Vec<proto::MemoryRecord>, Option<String>)> {
        let conn = self.get_read_conn().clone();
        let scope = scope.cloned();
        let kinds = kinds.to_vec();
        let tags = tags.to_vec();
        let cursor = cursor.map(|s| s.to_string());
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();

            let mut conditions: Vec<String> = vec!["1=1".to_string()];
            let mut param_values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

            // Parse cursor: format is "created_at_secs:id"
            if let Some(ref cursor_str) = cursor {
                if let Some((secs_str, cursor_id)) = cursor_str.split_once(':') {
                    if let Ok(secs) = secs_str.parse::<i64>() {
                        // For descending order: get records older than cursor
                        conditions.push(
                            "(m.created_at_secs < ? OR (m.created_at_secs = ? AND m.id < ?))".to_string(),
                        );
                        param_values.push(Box::new(secs));
                        param_values.push(Box::new(secs));
                        param_values.push(Box::new(cursor_id.to_string()));
                    }
                }
            }

            if let Some(ref s) = scope {
                if !s.org.is_empty() {
                    conditions.push("m.scope_org = ?".to_string());
                    param_values.push(Box::new(s.org.clone()));
                }
                if !s.team.is_empty() {
                    conditions.push("m.scope_team = ?".to_string());
                    param_values.push(Box::new(s.team.clone()));
                }
                if !s.user.is_empty() {
                    conditions.push("m.scope_user = ?".to_string());
                    param_values.push(Box::new(s.user.clone()));
                }
                if !s.agent.is_empty() {
                    conditions.push("m.scope_agent = ?".to_string());
                    param_values.push(Box::new(s.agent.clone()));
                }
            }

            if !kinds.is_empty() {
                let placeholders: Vec<&str> = kinds.iter().map(|_| "?").collect();
                conditions.push(format!("m.kind IN ({})", placeholders.join(",")));
                for k in &kinds {
                    param_values.push(Box::new(*k));
                }
            }

            let where_clause = conditions.join(" AND ");
            let fetch_limit = limit + 1;

            // If tags are provided, use JOIN with memory_tags
            let mut rows: Vec<proto::MemoryRecord> = if !tags.is_empty() {
                let tag_placeholders: Vec<String> = tags.iter().map(|_| "?".to_string()).collect();
                let tag_count = tags.len();

                let sql = format!(
                    "SELECT m.* FROM memories m
                     INNER JOIN memory_tags mt ON m.id = mt.memory_id
                     WHERE {} AND mt.tag IN ({})
                     GROUP BY m.id
                     HAVING COUNT(DISTINCT mt.tag) = ?
                     ORDER BY m.created_at_secs DESC, m.id DESC
                     LIMIT ?",
                    where_clause,
                    tag_placeholders.join(",")
                );
                for tag in &tags {
                    param_values.push(Box::new(tag.clone()));
                }
                param_values.push(Box::new(tag_count as u32));
                param_values.push(Box::new(fetch_limit));
                let params_ref: Vec<&dyn rusqlite::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();

                let mut stmt = conn.prepare(&sql)?;
                let results: Vec<proto::MemoryRecord> = stmt.query_map(params_ref.as_slice(), SqliteStore::row_to_record)?
                    .filter_map(|r| r.ok())
                    .collect();
                results
            } else {
                let sql = format!(
                    "SELECT * FROM memories m WHERE {} ORDER BY m.created_at_secs DESC, m.id DESC LIMIT ?",
                    where_clause
                );
                param_values.push(Box::new(fetch_limit));
                let params_ref: Vec<&dyn rusqlite::ToSql> =
                    param_values.iter().map(|p| p.as_ref()).collect();

                let mut stmt = conn.prepare(&sql)?;
                let results: Vec<proto::MemoryRecord> = stmt.query_map(params_ref.as_slice(), SqliteStore::row_to_record)?
                    .filter_map(|r| r.ok())
                    .collect();
                results
            };

            // Determine next_cursor
            let next_cursor = if rows.len() > limit as usize {
                rows.truncate(limit as usize);
                // Build cursor from last returned record
                rows.last().map(|r| {
                    let secs = r
                        .created_at
                        .as_ref()
                        .map(|t| t.seconds)
                        .unwrap_or(0);
                    format!("{}:{}", secs, r.id)
                })
            } else {
                None
            };

            Ok((rows, next_cursor))
        })
        .await
        .unwrap()
    }

    async fn history(&self, id: &str) -> Result<Vec<proto::MemoryRecord>> {
        let conn = self.get_read_conn().clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(
                "SELECT h.id, h.content, h.version, h.tags, h.kind,
                        h.updated_at_secs, h.updated_at_nanos
                 FROM memory_history h WHERE h.id = ?1 ORDER BY h.version ASC",
            )?;
            let records = stmt
                .query_map(params![id], |row| {
                    let tags_json: String = row.get("tags")?;
                    let tags: Vec<String> = serde_json::from_str(&tags_json).unwrap_or_default();
                    let updated_secs: i64 = row.get("updated_at_secs")?;
                    let updated_nanos: i32 = row.get("updated_at_nanos")?;

                    Ok(proto::MemoryRecord {
                        id: row.get("id")?,
                        content: row.get("content")?,
                        version: row.get("version")?,
                        kind: row.get("kind")?,
                        tags,
                        updated_at: SqliteStore::ts_to_proto(updated_secs, updated_nanos),
                        ..Default::default()
                    })
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(records)
        })
        .await
        .unwrap()
    }

    async fn store_version(&self, record: &proto::MemoryRecord) -> Result<()> {
        let conn = self.write_conn.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let (updated_secs, updated_nanos) = SqliteStore::proto_to_ts(&record.updated_at);
            let tags_json = serde_json::to_string(&record.tags)?;

            conn.execute(
                "INSERT OR REPLACE INTO memory_history (id, version, content, tags, kind, updated_at_secs, updated_at_nanos)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    record.id,
                    record.version,
                    record.content,
                    tags_json,
                    record.kind,
                    updated_secs,
                    updated_nanos,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap()
    }

    async fn get_all_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>> {
        let conn = self.get_read_conn().clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt =
                conn.prepare("SELECT id, embedding FROM memories WHERE embedding IS NOT NULL")?;
            let results = stmt
                .query_map([], |row| {
                    let id: String = row.get(0)?;
                    let blob: Vec<u8> = row.get(1)?;
                    let embedding: Vec<f32> = blob
                        .chunks_exact(4)
                        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                        .collect();
                    Ok((id, embedding))
                })?
                .filter_map(|r| r.ok())
                .collect();
            Ok(results)
        })
        .await
        .unwrap()
    }

    async fn get_many(&self, ids: &[String]) -> Result<Vec<proto::MemoryRecord>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let conn = self.get_read_conn().clone();
        let ids = ids.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let placeholders: Vec<&str> = ids.iter().map(|_| "?").collect();
            let sql = format!(
                "SELECT * FROM memories WHERE id IN ({})",
                placeholders.join(",")
            );
            let params: Vec<&dyn rusqlite::ToSql> =
                ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
            let mut stmt = conn.prepare(&sql)?;
            let results = stmt
                .query_map(params.as_slice(), SqliteStore::row_to_record)?
                .filter_map(|r| r.ok())
                .collect();
            Ok(results)
        })
        .await
        .unwrap()
    }

    async fn count(&self) -> Result<u64> {
        let conn = self.get_read_conn().clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let count: u64 = conn.query_row("SELECT COUNT(*) FROM memories", [], |row| row.get(0))?;
            Ok(count)
        })
        .await
        .unwrap()
    }

    async fn cleanup_expired(&self) -> Result<u64> {
        let conn = self.write_conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let now_secs = now.as_secs() as i64;

            // Also clean up memory_tags for expired records
            conn.execute(
                "DELETE FROM memory_tags WHERE memory_id IN (
                    SELECT id FROM memories WHERE expires_at_secs IS NOT NULL AND expires_at_secs > 0 AND expires_at_secs <= ?1
                )",
                params![now_secs],
            )?;

            let rows = conn.execute(
                "DELETE FROM memories WHERE expires_at_secs IS NOT NULL AND expires_at_secs > 0 AND expires_at_secs <= ?1",
                params![now_secs],
            )?;

            if rows > 0 {
                tracing::info!(count = rows, "Cleaned up expired memory records");
            }

            Ok(rows as u64)
        })
        .await
        .unwrap()
    }
}

// rusqlite optional helper
trait OptionalRow {
    fn optional(self) -> rusqlite::Result<Option<proto::MemoryRecord>>;
}

impl OptionalRow for rusqlite::Result<proto::MemoryRecord> {
    fn optional(self) -> rusqlite::Result<Option<proto::MemoryRecord>> {
        match self {
            Ok(r) => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}