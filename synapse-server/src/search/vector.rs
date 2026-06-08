use std::sync::Arc;

use crate::storage::StorageBackend;

/// Simple in-memory vector search using cosine similarity.
pub struct VectorSearch {
    store: Arc<dyn StorageBackend>,
}

impl VectorSearch {
    pub fn new(store: Arc<dyn StorageBackend>) -> Self {
        Self { store }
    }

    /// Search for the top-k most similar records to the given query embedding.
    /// Returns (record_id, score) pairs sorted by descending similarity.
    pub async fn search(
        &self,
        query_embedding: &[f32],
        top_k: usize,
        min_score: f32,
    ) -> anyhow::Result<Vec<(String, f32)>> {
        let all_embeddings = self.store.get_all_embeddings().await?;

        let mut scored: Vec<(String, f32)> = all_embeddings
            .iter()
            .filter_map(|(id, emb)| {
                let score = cosine_similarity(query_embedding, emb);
                if score >= min_score {
                    Some((id.clone(), score))
                } else {
                    None
                }
            })
            .collect();

        // Sort descending by score
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(top_k);

        Ok(scored)
    }

    /// Compute similarity between two records' embeddings.
    /// Used for deduplication and conflict detection.
    pub fn similarity(a: &[f32], b: &[f32]) -> f32 {
        cosine_similarity(a, b)
    }
}

/// Compute cosine similarity between two vectors.
/// Returns 0.0 if either vector is empty or has zero magnitude.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || b.is_empty() || a.len() != b.len() {
        return 0.0;
    }

    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let mag_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let mag_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

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
}
