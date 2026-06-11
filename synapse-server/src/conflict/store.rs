use anyhow::Result;
use rusqlite::{params, Connection};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::proto;

/// Persistent storage for conflict records using SQLite.
///
/// Stores conflicts in a dedicated table alongside the main memory database,
/// ensuring conflicts survive server restarts.
pub struct ConflictStore {
    conn: Arc<Mutex<Connection>>,
}

impl ConflictStore {
    /// Create a new ConflictStore using an existing SQLite connection.
    /// Initializes the conflicts table if it doesn't exist.
    pub fn new(conn: Arc<Mutex<Connection>>) -> Result<Self> {
        {
            let c = conn.blocking_lock();
            c.execute_batch(
                "CREATE TABLE IF NOT EXISTS conflicts (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL,
                    status INTEGER DEFAULT 0,
                    created_at INTEGER DEFAULT 0
                );",
            )?;
        }
        Ok(Self { conn })
    }

    /// Save a conflict to persistent storage.
    pub async fn save(&self, conflict: &proto::Conflict) -> Result<()> {
        let conn = self.conn.clone();
        let data = serde_json::to_string(&ConflictData::from_proto(conflict))?;
        let id = conflict.id.clone();
        let status = conflict.status;
        let created_at = conflict
            .detected_at
            .as_ref()
            .map(|t| t.seconds)
            .unwrap_or(0);

        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT OR REPLACE INTO conflicts (id, data, status, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![id, data, status, created_at],
            )?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .unwrap()
    }

    /// Get a conflict by ID.
    pub async fn get(&self, id: &str) -> Result<Option<proto::Conflict>> {
        let conn = self.conn.clone();
        let id = id.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare("SELECT data FROM conflicts WHERE id = ?1")?;
            let result = stmt
                .query_row(params![id], |row| {
                    let data: String = row.get(0)?;
                    Ok(data)
                });
            match result {
                Ok(data) => {
                    let conflict_data: ConflictData = serde_json::from_str(&data)?;
                    Ok(Some(conflict_data.to_proto()))
                }
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(anyhow::anyhow!("Failed to get conflict: {}", e)),
            }
        })
        .await
        .unwrap()
    }

    /// List conflicts with optional status filter.
    pub async fn list(
        &self,
        status_filter: Option<i32>,
        limit: u32,
        offset: u32,
    ) -> Result<(Vec<proto::Conflict>, u32)> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();

            let (sql, count_sql) = if let Some(status) = status_filter {
                (
                    format!(
                        "SELECT data FROM conflicts WHERE status = {} ORDER BY created_at DESC LIMIT {} OFFSET {}",
                        status, limit, offset
                    ),
                    format!("SELECT COUNT(*) FROM conflicts WHERE status = {}", status),
                )
            } else {
                (
                    format!(
                        "SELECT data FROM conflicts ORDER BY created_at DESC LIMIT {} OFFSET {}",
                        limit, offset
                    ),
                    "SELECT COUNT(*) FROM conflicts".to_string(),
                )
            };

            let total: u32 = conn.query_row(&count_sql, [], |row| row.get(0))?;

            let mut stmt = conn.prepare(&sql)?;
            let conflicts: Vec<proto::Conflict> = stmt
                .query_map([], |row| {
                    let data: String = row.get(0)?;
                    Ok(data)
                })?
                .filter_map(|r| r.ok())
                .filter_map(|data| {
                    serde_json::from_str::<ConflictData>(&data)
                        .ok()
                        .map(|cd| cd.to_proto())
                })
                .collect();

            Ok((conflicts, total))
        })
        .await
        .unwrap()
    }

    /// Update the status of a conflict.
    pub async fn update_status(
        &self,
        id: &str,
        status: i32,
        resolution: Option<&proto::Resolution>,
    ) -> Result<Option<proto::Conflict>> {
        let _conn = self.conn.clone();
        let id = id.to_string();

        // First get the existing conflict
        let existing = self.get(&id).await?;
        if let Some(mut conflict) = existing {
            conflict.status = status;
            if let Some(res) = resolution {
                conflict.resolution = Some(res.clone());
            }
            // Re-save with updated data
            self.save(&conflict).await?;
            Ok(Some(conflict))
        } else {
            Ok(None)
        }
    }
}

/// Serializable representation of a Conflict for JSON storage.
#[derive(serde::Serialize, serde::Deserialize)]
struct ConflictData {
    id: String,
    record_ids: Vec<String>,
    status: i32,
    detected_at_secs: i64,
    detected_at_nanos: i32,
    #[serde(default)]
    resolution_strategy: i32,
    #[serde(default)]
    resolved_by: String,
    #[serde(default)]
    resolution_reasoning: String,
}

impl ConflictData {
    fn from_proto(conflict: &proto::Conflict) -> Self {
        let (det_secs, det_nanos) = conflict
            .detected_at
            .as_ref()
            .map(|t| (t.seconds, t.nanos))
            .unwrap_or((0, 0));
        Self {
            id: conflict.id.clone(),
            record_ids: conflict.records.iter().map(|r| r.id.clone()).collect(),
            status: conflict.status,
            detected_at_secs: det_secs,
            detected_at_nanos: det_nanos,
            resolution_strategy: conflict
                .resolution
                .as_ref()
                .map(|r| r.strategy)
                .unwrap_or(0),
            resolved_by: conflict
                .resolution
                .as_ref()
                .map(|r| r.resolved_by.clone())
                .unwrap_or_default(),
            resolution_reasoning: conflict
                .resolution
                .as_ref()
                .map(|r| r.reasoning.clone())
                .unwrap_or_default(),
        }
    }

    fn to_proto(&self) -> proto::Conflict {
        let detected_at = if self.detected_at_secs != 0 || self.detected_at_nanos != 0 {
            Some(prost_types::Timestamp {
                seconds: self.detected_at_secs,
                nanos: self.detected_at_nanos,
            })
        } else {
            None
        };

        let resolution = if self.resolution_strategy != 0 || !self.resolved_by.is_empty() {
            Some(proto::Resolution {
                strategy: self.resolution_strategy,
                result: None,
                reasoning: self.resolution_reasoning.clone(),
                resolved_by: self.resolved_by.clone(),
                resolved_at: None,
            })
        } else {
            None
        };

        proto::Conflict {
            id: self.id.clone(),
            records: vec![], // Records are fetched separately if needed
            status: self.status,
            detected_at,
            resolution,
        }
    }
}
