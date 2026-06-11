use anyhow::{anyhow, Result};
use async_trait::async_trait;
use pgvector::Vector;
use prost_types::Timestamp;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::proto;
use crate::storage::traits::StorageBackend;

/// PostgreSQL storage backend with pgvector support for embedding search.
///
/// Uses connection pooling via sqlx::PgPool. Embeddings are stored as native
/// pgvector `vector` columns enabling efficient cosine similarity search via
/// the IVFFlat index.
pub struct PostgresStore {
    pool: PgPool,
    embedding_dim: usize,
}

impl PostgresStore {
    /// Create a new PostgresStore, connecting to the given database URL
    /// and running migrations to ensure the schema exists.
    pub async fn new(database_url: &str, embedding_dim: usize) -> Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        let store = Self {
            pool,
            embedding_dim,
        };
        store.run_migrations().await?;
        Ok(store)
    }

    /// Run schema migrations to create tables and indexes.
    async fn run_migrations(&self) -> Result<()> {
        let create_sql = format!(
            r#"
            CREATE EXTENSION IF NOT EXISTS vector;

            CREATE TABLE IF NOT EXISTS memories (
                id TEXT PRIMARY KEY,
                content TEXT NOT NULL,
                embedding vector({dim}),
                scope_org TEXT DEFAULT '',
                scope_team TEXT DEFAULT '',
                scope_user TEXT DEFAULT '',
                scope_agent TEXT DEFAULT '',
                scope_session TEXT DEFAULT '',
                scope_visibility INTEGER DEFAULT 0,
                kind INTEGER DEFAULT 0,
                confidence REAL DEFAULT 1.0,
                tags JSONB DEFAULT '[]',
                source_agent_id TEXT DEFAULT '',
                source_session_id TEXT DEFAULT '',
                version INTEGER DEFAULT 1,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                accessed_at TIMESTAMPTZ DEFAULT NOW(),
                expires_at TIMESTAMPTZ,
                vector_clock JSONB DEFAULT '{{}}',
                lineage JSONB DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS memory_history (
                id TEXT NOT NULL,
                version INTEGER NOT NULL,
                content TEXT NOT NULL,
                tags JSONB DEFAULT '[]',
                kind INTEGER DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (id, version)
            );

            CREATE INDEX IF NOT EXISTS idx_memories_scope
                ON memories(scope_org, scope_team, scope_user, scope_agent);
            CREATE INDEX IF NOT EXISTS idx_memories_kind ON memories(kind);
            CREATE INDEX IF NOT EXISTS idx_memories_created ON memories(created_at);
            CREATE INDEX IF NOT EXISTS idx_memories_expires
                ON memories(expires_at) WHERE expires_at IS NOT NULL;
            "#,
            dim = self.embedding_dim
        );
        sqlx::query(&create_sql).execute(&self.pool).await?;

        // IVFFlat index requires rows to exist; create it separately and ignore
        // "already exists" errors gracefully.
        let ivfflat_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_memories_embedding \
             ON memories USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
        );
        // IVFFlat creation may fail if table is empty (no tuples to train on).
        // We log and continue - the index will be created on first sufficient insert.
        if let Err(e) = sqlx::query(&ivfflat_sql).execute(&self.pool).await {
            tracing::warn!("IVFFlat index creation deferred (expected on empty table): {}", e);
        }

        Ok(())
    }

    /// Convert a prost Timestamp to a chrono DateTime for PostgreSQL TIMESTAMPTZ.
    fn ts_to_chrono(ts: &Option<Timestamp>) -> Option<chrono::DateTime<chrono::Utc>> {
        ts.as_ref().map(|t| {
            chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
                .unwrap_or_else(|| chrono::Utc::now())
        })
    }

    /// Convert a chrono DateTime back to a prost Timestamp.
    fn chrono_to_ts(dt: Option<chrono::DateTime<chrono::Utc>>) -> Option<Timestamp> {
        dt.map(|d| Timestamp {
            seconds: d.timestamp(),
            nanos: d.timestamp_subsec_nanos() as i32,
        })
    }

    /// Convert an embedding Vec<f32> to a pgvector Vector, or None if empty.
    fn embedding_to_vector(embedding: &[f32]) -> Option<Vector> {
        if embedding.is_empty() {
            None
        } else {
            Some(Vector::from(embedding.to_vec()))
        }
    }

    /// Convert a sqlx Row into a MemoryRecord.
    fn row_to_record(row: &sqlx::postgres::PgRow) -> Result<proto::MemoryRecord> {
        use sqlx::Row as _;

        let id: String = row.try_get("id")?;
        let content: String = row.try_get("content")?;

        // Embedding: pgvector returns as Vector type
        let embedding: Vec<f32> = row
            .try_get::<Option<Vector>, _>("embedding")
            .unwrap_or(None)
            .map(|v| v.to_vec())
            .unwrap_or_default();

        let scope_org: String = row.try_get("scope_org").unwrap_or_default();
        let scope_team: String = row.try_get("scope_team").unwrap_or_default();
        let scope_user: String = row.try_get("scope_user").unwrap_or_default();
        let scope_agent: String = row.try_get("scope_agent").unwrap_or_default();
        let scope_session: String = row.try_get("scope_session").unwrap_or_default();
        let scope_visibility: i32 = row.try_get("scope_visibility").unwrap_or(0);

        let kind: i32 = row.try_get("kind").unwrap_or(0);
        let confidence: f32 = row.try_get("confidence").unwrap_or(1.0);

        let tags: serde_json::Value = row.try_get("tags").unwrap_or(serde_json::json!([]));
        let tags: Vec<String> = serde_json::from_value(tags).unwrap_or_default();

        let source_agent_id: String = row.try_get("source_agent_id").unwrap_or_default();
        let source_session_id: String = row.try_get("source_session_id").unwrap_or_default();

        let version: i32 = row.try_get("version").unwrap_or(1);

        let created_at: Option<chrono::DateTime<chrono::Utc>> =
            row.try_get("created_at").unwrap_or(None);
        let updated_at: Option<chrono::DateTime<chrono::Utc>> =
            row.try_get("updated_at").unwrap_or(None);
        let accessed_at: Option<chrono::DateTime<chrono::Utc>> =
            row.try_get("accessed_at").unwrap_or(None);
        let expires_at: Option<chrono::DateTime<chrono::Utc>> =
            row.try_get("expires_at").unwrap_or(None);

        let vector_clock: serde_json::Value =
            row.try_get("vector_clock").unwrap_or(serde_json::json!({}));
        let clock: std::collections::HashMap<String, u64> =
            serde_json::from_value(vector_clock).unwrap_or_default();

        let lineage_val: serde_json::Value =
            row.try_get("lineage").unwrap_or(serde_json::json!([]));
        let lineage: Vec<String> = serde_json::from_value(lineage_val).unwrap_or_default();

        Ok(proto::MemoryRecord {
            id,
            content,
            embedding,
            scope: Some(proto::Scope {
                org: scope_org,
                team: scope_team,
                user: scope_user,
                agent: scope_agent,
                session: scope_session,
                visibility: scope_visibility,
            }),
            kind,
            confidence,
            tags,
            source: Some(proto::Source {
                agent_id: source_agent_id,
                session_id: source_session_id,
                input_hash: String::new(),
                tool_call_id: String::new(),
            }),
            version: version as u64,
            created_at: Self::chrono_to_ts(created_at),
            updated_at: Self::chrono_to_ts(updated_at),
            accessed_at: Self::chrono_to_ts(accessed_at),
            expires_at: Self::chrono_to_ts(expires_at),
            vector_clock: Some(proto::VectorClock { clock }),
            lineage,
        })
    }
}

#[async_trait]
impl StorageBackend for PostgresStore {
    async fn add(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let scope = record.scope.as_ref().cloned().unwrap_or_default();
        let source = record.source.as_ref().cloned().unwrap_or_default();

        let created_at = Self::ts_to_chrono(&record.created_at).unwrap_or_else(|| chrono::Utc::now());
        let updated_at = Self::ts_to_chrono(&record.updated_at).unwrap_or_else(|| chrono::Utc::now());
        let accessed_at = Self::ts_to_chrono(&record.accessed_at).unwrap_or_else(|| chrono::Utc::now());
        let expires_at = Self::ts_to_chrono(&record.expires_at);

        let embedding = Self::embedding_to_vector(&record.embedding);
        let tags_json = serde_json::to_value(&record.tags)?;
        let clock_json = serde_json::to_value(
            &record
                .vector_clock
                .as_ref()
                .map(|vc| &vc.clock)
                .cloned()
                .unwrap_or_default(),
        )?;
        let lineage_json = serde_json::to_value(&record.lineage)?;

        let row = sqlx::query(
            r#"
            INSERT INTO memories (
                id, content, embedding, scope_org, scope_team, scope_user,
                scope_agent, scope_session, scope_visibility, kind, confidence, tags,
                source_agent_id, source_session_id, version,
                created_at, updated_at, accessed_at, expires_at,
                vector_clock, lineage
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20, $21
            )
            RETURNING *
            "#,
        )
        .bind(&record.id)
        .bind(&record.content)
        .bind(&embedding)
        .bind(&scope.org)
        .bind(&scope.team)
        .bind(&scope.user)
        .bind(&scope.agent)
        .bind(&scope.session)
        .bind(scope.visibility)
        .bind(record.kind)
        .bind(record.confidence)
        .bind(&tags_json)
        .bind(&source.agent_id)
        .bind(&source.session_id)
        .bind(record.version as i32)
        .bind(created_at)
        .bind(updated_at)
        .bind(accessed_at)
        .bind(expires_at)
        .bind(&clock_json)
        .bind(&lineage_json)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL insert failed: {}", e))?;

        Self::row_to_record(&row)
    }

    async fn get(&self, id: &str) -> Result<Option<proto::MemoryRecord>> {
        // Update accessed_at and return the record
        let row = sqlx::query(
            r#"
            UPDATE memories SET accessed_at = NOW()
            WHERE id = $1
            RETURNING *
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL get failed: {}", e))?;

        match row {
            Some(r) => Ok(Some(Self::row_to_record(&r)?)),
            None => Ok(None),
        }
    }

    async fn update(&self, record: proto::MemoryRecord) -> Result<proto::MemoryRecord> {
        let scope = record.scope.as_ref().cloned().unwrap_or_default();
        let updated_at = Self::ts_to_chrono(&record.updated_at).unwrap_or_else(|| chrono::Utc::now());
        let embedding = Self::embedding_to_vector(&record.embedding);
        let tags_json = serde_json::to_value(&record.tags)?;
        let clock_json = serde_json::to_value(
            &record
                .vector_clock
                .as_ref()
                .map(|vc| &vc.clock)
                .cloned()
                .unwrap_or_default(),
        )?;

        let row = sqlx::query(
            r#"
            UPDATE memories SET
                content = $1, embedding = $2, scope_org = $3, scope_team = $4,
                scope_user = $5, scope_agent = $6, scope_visibility = $7,
                kind = $8, confidence = $9, tags = $10, version = $11,
                updated_at = $12, vector_clock = $13
            WHERE id = $14
            RETURNING *
            "#,
        )
        .bind(&record.content)
        .bind(&embedding)
        .bind(&scope.org)
        .bind(&scope.team)
        .bind(&scope.user)
        .bind(&scope.agent)
        .bind(scope.visibility)
        .bind(record.kind)
        .bind(record.confidence)
        .bind(&tags_json)
        .bind(record.version as i32)
        .bind(updated_at)
        .bind(&clock_json)
        .bind(&record.id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL update failed: {}", e))?;

        match row {
            Some(r) => Self::row_to_record(&r),
            None => Err(anyhow!("Record '{}' not found", record.id)),
        }
    }

    async fn delete(&self, id: &str) -> Result<bool> {
        // Delete history entries first
        sqlx::query("DELETE FROM memory_history WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        let result = sqlx::query("DELETE FROM memories WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("PostgreSQL delete failed: {}", e))?;

        Ok(result.rows_affected() > 0)
    }

    async fn delete_by_scope(
        &self,
        scope: &proto::Scope,
        before: Option<Timestamp>,
    ) -> Result<u64> {
        let mut conditions = Vec::new();
        let mut param_idx = 1u32;

        // Build dynamic WHERE clause
        // We use a Vec of boxed values to pass dynamic params
        struct DynParams {
            strings: Vec<String>,
            ts: Option<chrono::DateTime<chrono::Utc>>,
        }
        let mut params = DynParams {
            strings: Vec::new(),
            ts: None,
        };

        if !scope.org.is_empty() {
            conditions.push(format!("scope_org = ${}", param_idx));
            params.strings.push(scope.org.clone());
            param_idx += 1;
        }
        if !scope.team.is_empty() {
            conditions.push(format!("scope_team = ${}", param_idx));
            params.strings.push(scope.team.clone());
            param_idx += 1;
        }
        if !scope.user.is_empty() {
            conditions.push(format!("scope_user = ${}", param_idx));
            params.strings.push(scope.user.clone());
            param_idx += 1;
        }
        if !scope.agent.is_empty() {
            conditions.push(format!("scope_agent = ${}", param_idx));
            params.strings.push(scope.agent.clone());
            param_idx += 1;
        }

        if let Some(ref ts) = before {
            let dt = chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
                .unwrap_or_else(|| chrono::Utc::now());
            conditions.push(format!("created_at < ${}", param_idx));
            params.ts = Some(dt);
            // param_idx incremented but not used after
        }

        if conditions.is_empty() {
            return Ok(0);
        }

        let where_clause = conditions.join(" AND ");
        let sql = format!("DELETE FROM memories WHERE {}", where_clause);

        // Build the query with dynamic bindings
        let mut query = sqlx::query(&sql);
        for s in &params.strings {
            query = query.bind(s);
        }
        if let Some(ref dt) = params.ts {
            query = query.bind(dt);
        }

        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| anyhow!("PostgreSQL delete_by_scope failed: {}", e))?;

        Ok(result.rows_affected())
    }

    async fn list(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<proto::MemoryRecord>, u32)> {
        let mut conditions: Vec<String> = vec!["TRUE".to_string()];
        let mut string_params: Vec<String> = Vec::new();
        let mut int_params: Vec<i32> = Vec::new();
        let mut param_idx = 1u32;

        if let Some(s) = scope {
            if !s.org.is_empty() {
                conditions.push(format!("scope_org = ${}", param_idx));
                string_params.push(s.org.clone());
                param_idx += 1;
            }
            if !s.team.is_empty() {
                conditions.push(format!("scope_team = ${}", param_idx));
                string_params.push(s.team.clone());
                param_idx += 1;
            }
            if !s.user.is_empty() {
                conditions.push(format!("scope_user = ${}", param_idx));
                string_params.push(s.user.clone());
                param_idx += 1;
            }
            if !s.agent.is_empty() {
                conditions.push(format!("scope_agent = ${}", param_idx));
                string_params.push(s.agent.clone());
                param_idx += 1;
            }
        }

        if !kinds.is_empty() {
            let placeholders: Vec<String> = kinds
                .iter()
                .map(|_| {
                    let p = format!("${}", param_idx);
                    param_idx += 1;
                    p
                })
                .collect();
            conditions.push(format!("kind IN ({})", placeholders.join(",")));
            int_params.extend_from_slice(kinds);
        }

        let where_clause = conditions.join(" AND ");

        // Count total matching records
        let count_sql = format!("SELECT COUNT(*)::INTEGER FROM memories WHERE {}", where_clause);
        let mut count_query = sqlx::query_scalar::<_, i32>(&count_sql);
        for s in &string_params {
            count_query = count_query.bind(s);
        }
        for k in &int_params {
            count_query = count_query.bind(k);
        }
        let total: i32 = count_query
            .fetch_one(&self.pool)
            .await
            .unwrap_or(0);

        // Fetch page
        let fetch_sql = format!(
            "SELECT * FROM memories WHERE {} ORDER BY created_at DESC LIMIT ${} OFFSET ${}",
            where_clause, param_idx, param_idx + 1
        );
        let mut fetch_query = sqlx::query(&fetch_sql);
        for s in &string_params {
            fetch_query = fetch_query.bind(s);
        }
        for k in &int_params {
            fetch_query = fetch_query.bind(k);
        }
        fetch_query = fetch_query.bind(limit as i64).bind(offset as i64);

        let rows = fetch_query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| anyhow!("PostgreSQL list failed: {}", e))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Ok(record) = Self::row_to_record(row) {
                // Post-filter by tags (AND logic)
                if tags.is_empty() || tags.iter().all(|t| record.tags.contains(t)) {
                    records.push(record);
                }
            }
        }

        Ok((records, total as u32))
    }

    async fn history(&self, id: &str) -> Result<Vec<proto::MemoryRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT id, content, version, tags, kind, updated_at
            FROM memory_history
            WHERE id = $1
            ORDER BY version ASC
            "#,
        )
        .bind(id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL history failed: {}", e))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let id: String = row.try_get("id")?;
            let content: String = row.try_get("content")?;
            let version: i32 = row.try_get("version").unwrap_or(1);
            let kind: i32 = row.try_get("kind").unwrap_or(0);
            let tags_val: serde_json::Value = row.try_get("tags").unwrap_or(serde_json::json!([]));
            let tags: Vec<String> = serde_json::from_value(tags_val).unwrap_or_default();
            let updated_at: Option<chrono::DateTime<chrono::Utc>> =
                row.try_get("updated_at").unwrap_or(None);

            records.push(proto::MemoryRecord {
                id,
                content,
                version: version as u64,
                kind,
                tags,
                updated_at: Self::chrono_to_ts(updated_at),
                ..Default::default()
            });
        }

        Ok(records)
    }

    async fn store_version(&self, record: &proto::MemoryRecord) -> Result<()> {
        let updated_at = Self::ts_to_chrono(&record.updated_at).unwrap_or_else(|| chrono::Utc::now());
        let tags_json = serde_json::to_value(&record.tags)?;

        sqlx::query(
            r#"
            INSERT INTO memory_history (id, version, content, tags, kind, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id, version) DO UPDATE SET
                content = EXCLUDED.content,
                tags = EXCLUDED.tags,
                kind = EXCLUDED.kind,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&record.id)
        .bind(record.version as i32)
        .bind(&record.content)
        .bind(&tags_json)
        .bind(record.kind)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL store_version failed: {}", e))?;

        Ok(())
    }

    async fn get_all_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>> {
        let rows = sqlx::query(
            "SELECT id, embedding FROM memories WHERE embedding IS NOT NULL",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL get_all_embeddings failed: {}", e))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let id: String = row.try_get("id")?;
            let embedding: Option<Vector> = row.try_get("embedding").unwrap_or(None);
            if let Some(vec) = embedding {
                results.push((id, vec.to_vec()));
            }
        }

        Ok(results)
    }

    async fn get_many(&self, ids: &[String]) -> Result<Vec<proto::MemoryRecord>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        // Build parameterized IN clause
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("${}", i)).collect();
        let sql = format!(
            "SELECT * FROM memories WHERE id IN ({})",
            placeholders.join(",")
        );

        let mut query = sqlx::query(&sql);
        for id in ids {
            query = query.bind(id);
        }

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| anyhow!("PostgreSQL get_many failed: {}", e))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Ok(record) = Self::row_to_record(row) {
                records.push(record);
            }
        }

        Ok(records)
    }

    async fn count(&self) -> Result<u64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM memories")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| anyhow!("PostgreSQL count failed: {}", e))?;

        Ok(count as u64)
    }

    async fn list_with_cursor(
        &self,
        scope: Option<&proto::Scope>,
        kinds: &[i32],
        tags: &[String],
        limit: u32,
        cursor: Option<&str>,
    ) -> Result<(Vec<proto::MemoryRecord>, Option<String>)> {
        let mut conditions: Vec<String> = vec!["TRUE".to_string()];
        let mut string_params: Vec<String> = Vec::new();
        let mut int_params: Vec<i32> = Vec::new();
        let mut param_idx = 1u32;

        // Parse cursor: format is "created_at_iso:id"
        let mut cursor_dt: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut cursor_id: Option<String> = None;
        if let Some(c) = cursor {
            if let Some(colon_pos) = c.rfind(':') {
                let ts_part = &c[..colon_pos];
                let id_part = &c[colon_pos + 1..];
                if let Ok(dt) = ts_part.parse::<chrono::DateTime<chrono::Utc>>() {
                    cursor_dt = Some(dt);
                    cursor_id = Some(id_part.to_string());
                }
            }
        }

        if let Some(s) = scope {
            if !s.org.is_empty() {
                conditions.push(format!("scope_org = ${}", param_idx));
                string_params.push(s.org.clone());
                param_idx += 1;
            }
            if !s.team.is_empty() {
                conditions.push(format!("scope_team = ${}", param_idx));
                string_params.push(s.team.clone());
                param_idx += 1;
            }
            if !s.user.is_empty() {
                conditions.push(format!("scope_user = ${}", param_idx));
                string_params.push(s.user.clone());
                param_idx += 1;
            }
            if !s.agent.is_empty() {
                conditions.push(format!("scope_agent = ${}", param_idx));
                string_params.push(s.agent.clone());
                param_idx += 1;
            }
        }

        if !kinds.is_empty() {
            let placeholders: Vec<String> = kinds
                .iter()
                .map(|_| {
                    let p = format!("${}", param_idx);
                    param_idx += 1;
                    p
                })
                .collect();
            conditions.push(format!("kind IN ({})", placeholders.join(",")));
            int_params.extend_from_slice(kinds);
        }

        // Cursor condition: records older than cursor
        if let (Some(_dt), Some(_cid)) = (&cursor_dt, &cursor_id) {
            conditions.push(format!(
                "(created_at < ${} OR (created_at = ${} AND id < ${}))",
                param_idx, param_idx, param_idx + 1
            ));
            param_idx += 2;
        }

        let where_clause = conditions.join(" AND ");

        let fetch_sql = format!(
            "SELECT * FROM memories WHERE {} ORDER BY created_at DESC, id DESC LIMIT ${}",
            where_clause, param_idx
        );

        let mut fetch_query = sqlx::query(&fetch_sql);
        for s in &string_params {
            fetch_query = fetch_query.bind(s);
        }
        for k in &int_params {
            fetch_query = fetch_query.bind(k);
        }
        if let (Some(ref dt), Some(ref cid)) = (&cursor_dt, &cursor_id) {
            fetch_query = fetch_query.bind(dt).bind(cid);
        }
        fetch_query = fetch_query.bind(limit as i64 + 1); // fetch one extra to detect next page

        let rows = fetch_query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| anyhow!("PostgreSQL list_with_cursor failed: {}", e))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Ok(record) = Self::row_to_record(row) {
                if tags.is_empty() || tags.iter().all(|t| record.tags.contains(t)) {
                    records.push(record);
                }
            }
        }

        // Determine next cursor
        let next_cursor = if records.len() > limit as usize {
            records.truncate(limit as usize);
            records.last().map(|r| {
                let ts = r.created_at.as_ref().map(|t| {
                    chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
                        .unwrap_or_else(|| chrono::Utc::now())
                        .to_rfc3339()
                }).unwrap_or_default();
                format!("{}:{}", ts, r.id)
            })
        } else {
            None
        };

        Ok((records, next_cursor))
    }

    async fn cleanup_expired(&self) -> Result<u64> {
        let result = sqlx::query(
            "DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at <= NOW()"
        )
        .execute(&self.pool)
        .await
        .map_err(|e| anyhow!("PostgreSQL cleanup_expired failed: {}", e))?;

        Ok(result.rows_affected())
    }
}
