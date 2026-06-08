use std::collections::BinaryHeap;
use std::cmp::Ordering;
use std::sync::Arc;

use crate::storage::StorageBackend;

/// A scored result for the min-heap (we want top-K max, so use min-heap and evict smallest).
#[derive(PartialEq)]
struct ScoredItem {
    score: f32,
    id: String,
}

impl Eq for ScoredItem {}

impl PartialOrd for ScoredItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order: smaller scores at top of heap (min-heap)
        // So `pop()` removes the smallest score, keeping top-K largest.
        other.score.partial_cmp(&self.score).unwrap_or(Ordering::Equal)
    }
}

/// In-memory vector search using cosine similarity with top-K heap optimization.
///
/// For v0.1 this scans all embeddings (brute-force). Performance characteristics:
/// - O(n) scan, O(n log k) for top-k extraction via min-heap
/// - Suitable for < 100k records. Beyond that, add HNSW index.
pub struct VectorSearch {
    store: Arc<dyn StorageBackend>,
}

impl VectorSearch {
    pub fn new(store: Arc<dyn StorageBackend>) -> Self {
        Self { store }
    }

    /// Search for the top-k most similar records to the given query embedding.
    /// Uses a min-heap to avoid sorting the full result set — O(n log k) vs O(n log n).
    ///
    /// Safety: top_k is capped at 1000 to prevent excessive memory use in results.
    /// The full scan is bounded by total record count (warn at 100k+).
    pub async fn search(
        &self,
        query_embedding: &[f32],
        top_k: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<(String, f32)>> {
        // CVE-6: Hard cap on top_k to prevent result-set OOM
        const MAX_TOP_K: usize = 1000;
        let top_k = top_k.min(MAX_TOP_K);

        // Dimension validation
        const MAX_DIMS: usize = 4096;
        if query_embedding.len() > MAX_DIMS {
            anyhow::bail!("query embedding exceeds maximum dimensions ({} > {})", query_embedding.len(), MAX_DIMS);
        }

        let all_embeddings = self.store.get_all_embeddings().await?;

        // Warn if corpus is very large (brute-force scan starts degrading)
        if all_embeddings.len() > 100_000 {
            tracing::warn!(
                count = all_embeddings.len(),
                "Vector search scanning >100k embeddings. Consider adding HNSW index."
            );
        }

        // Pre-compute query magnitude to avoid redundant sqrt per comparison
        let query_mag: f32 = query_embedding.iter().map(|x| x * x).sum::<f32>().sqrt();
        if query_mag == 0.0 {
            return Ok(vec![]);
        }

        let mut heap: BinaryHeap<ScoredItem> = BinaryHeap::with_capacity(top_k + 1);

        for (id, emb) in &all_embeddings {
            if emb.len() != query_embedding.len() {
                continue; // Dimension mismatch — skip
            }

            let score = cosine_similarity_precomputed(query_embedding, query_mag, emb);
            if score < min_score {
                continue;
            }

            heap.push(ScoredItem { score, id: id.clone() });
            if heap.len() > top_k {
                heap.pop(); // Evict smallest
            }
        }

        // Drain heap into sorted vec (descending by score)
        let mut results: Vec<(String, f32)> = heap
            .into_iter()
            .map(|item| (item.id, item.score))
            .collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        Ok(results)
    }

    /// Compute similarity between two records' embeddings.
    pub fn similarity(a: &[f32], b: &[f32]) -> f32 {
        cosine_similarity(a, b)
    }
}

/// Cosine similarity with pre-computed query magnitude.
/// Avoids redundant sqrt computation for the query vector on each comparison.
fn cosine_similarity_precomputed(query: &[f32], query_mag: f32, other: &[f32]) -> f32 {
    debug_assert_eq!(query.len(), other.len());

    let mut dot: f32 = 0.0;
    let mut other_mag_sq: f32 = 0.0;

    for (q, o) in query.iter().zip(other.iter()) {
        dot += q * o;
        other_mag_sq += o * o;
    }

    let other_mag = other_mag_sq.sqrt();
    if other_mag == 0.0 {
        return 0.0;
    }

    dot / (query_mag * other_mag)
}

/// Compute cosine similarity between two vectors.
/// Returns 0.0 if either vector is empty or has zero magnitude.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 0.0;
    }

    let mut dot: f32 = 0.0;
    let mut mag_a_sq: f32 = 0.0;
    let mut mag_b_sq: f32 = 0.0;

    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        mag_a_sq += x * x;
        mag_b_sq += y * y;
    }

    let mag_a = mag_a_sq.sqrt();
    let mag_b = mag_b_sq.sqrt();

    if mag_a == 0.0 || mag_b == 0.0 {
        return 0.0;
    }

    dot / (mag_a * mag_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity_identical() {
        let v = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&v, &v);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_opposite() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![-1.0, -2.0, -3.0];
        let sim = cosine_similarity(&a, &b);
        assert!((sim + 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_empty() {
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(cosine_similarity(&[1.0], &[]), 0.0);
    }

    #[test]
    fn test_cosine_similarity_dimension_mismatch() {
        assert_eq!(cosine_similarity(&[1.0, 2.0], &[1.0, 2.0, 3.0]), 0.0);
    }

    #[test]
    fn test_precomputed_matches_standard() {
        let a = vec![0.5, 0.3, 0.8, 0.1];
        let b = vec![0.2, 0.7, 0.4, 0.6];
        let standard = cosine_similarity(&a, &b);
        let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let precomputed = cosine_similarity_precomputed(&a, mag_a, &b);
        assert!((standard - precomputed).abs() < 1e-6);
    }
}
