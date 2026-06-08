use prost_types::Timestamp;
use tracing::info;

use crate::proto;

/// Resolves conflicts between memory records using various strategies.
pub struct ConflictResolver;

impl ConflictResolver {
    /// Resolve a conflict using the specified strategy.
    /// Returns the resolved record and reasoning.
    pub fn resolve(
        records: &[proto::MemoryRecord],
        strategy: proto::ResolutionStrategy,
    ) -> (proto::MemoryRecord, String) {
        match strategy {
            proto::ResolutionStrategy::LastWriterWins => Self::last_writer_wins(records),
            proto::ResolutionStrategy::FirstWriterWins => Self::first_writer_wins(records),
            proto::ResolutionStrategy::KeepBoth => Self::keep_both(records),
            proto::ResolutionStrategy::ConfidenceWins => Self::confidence_wins(records),
            proto::ResolutionStrategy::LlmMerge => Self::llm_merge_stub(records),
            _ => Self::last_writer_wins(records), // default fallback
        }
    }

    /// Last writer wins: pick the record with the most recent updated_at.
    fn last_writer_wins(records: &[proto::MemoryRecord]) -> (proto::MemoryRecord, String) {
        let winner = records
            .iter()
            .max_by_key(|r| {
                r.updated_at
                    .as_ref()
                    .map(|t| t.seconds * 1_000_000_000 + t.nanos as i64)
                    .unwrap_or(0)
            })
            .cloned()
            .unwrap_or_default();

        info!(id = %winner.id, "Conflict resolved: LAST_WRITER_WINS");
        (
            winner,
            "Resolved by LAST_WRITER_WINS: most recent update wins".to_string(),
        )
    }

    /// First writer wins: pick the record with the earliest created_at.
    fn first_writer_wins(records: &[proto::MemoryRecord]) -> (proto::MemoryRecord, String) {
        let winner = records
            .iter()
            .min_by_key(|r| {
                r.created_at
                    .as_ref()
                    .map(|t| t.seconds * 1_000_000_000 + t.nanos as i64)
                    .unwrap_or(i64::MAX)
            })
            .cloned()
            .unwrap_or_default();

        info!(id = %winner.id, "Conflict resolved: FIRST_WRITER_WINS");
        (
            winner,
            "Resolved by FIRST_WRITER_WINS: earliest record preserved".to_string(),
        )
    }

    /// Keep both: returns the first record with lineage linking to all conflicting records.
    /// In practice, the caller should store all records as separate entries.
    fn keep_both(records: &[proto::MemoryRecord]) -> (proto::MemoryRecord, String) {
        let mut result = records.first().cloned().unwrap_or_default();

        // Link all record IDs in lineage
        result.lineage = records.iter().map(|r| r.id.clone()).collect();

        info!("Conflict resolved: KEEP_BOTH (all versions preserved)");
        (
            result,
            "Resolved by KEEP_BOTH: all conflicting versions preserved as separate records"
                .to_string(),
        )
    }

    /// Confidence wins: pick the record with the highest confidence score.
    fn confidence_wins(records: &[proto::MemoryRecord]) -> (proto::MemoryRecord, String) {
        let winner = records
            .iter()
            .max_by(|a, b| {
                a.confidence
                    .partial_cmp(&b.confidence)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
            .unwrap_or_default();

        let confidence = winner.confidence;
        info!(id = %winner.id, confidence, "Conflict resolved: CONFIDENCE_WINS");
        (
            winner,
            format!(
                "Resolved by CONFIDENCE_WINS: record with highest confidence ({}) wins",
                confidence
            ),
        )
    }

    /// LLM Merge stub: in v0.1, this falls back to LAST_WRITER_WINS
    /// with a note that LLM merge is not yet implemented.
    fn llm_merge_stub(records: &[proto::MemoryRecord]) -> (proto::MemoryRecord, String) {
        info!("LLM_MERGE requested but not yet implemented, falling back to LAST_WRITER_WINS");
        let (record, _) = Self::last_writer_wins(records);
        (
            record,
            "LLM_MERGE not yet implemented in v0.1; fell back to LAST_WRITER_WINS".to_string(),
        )
    }

    /// Create a Resolution message from resolved data.
    pub fn make_resolution(
        strategy: proto::ResolutionStrategy,
        result: proto::MemoryRecord,
        reasoning: String,
        resolved_by: String,
    ) -> proto::Resolution {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        proto::Resolution {
            strategy: strategy as i32,
            result: Some(result),
            reasoning,
            resolved_by,
            resolved_at: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
        }
    }
}
