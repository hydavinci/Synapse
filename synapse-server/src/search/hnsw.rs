use std::sync::Arc;

use anyhow::Result;
use tokio::sync::RwLock;
use tracing::info;
use usearch::{Index, IndexOptions, MetricKind, ScalarKind};

/// HNSW index wrapper using the `usearch` crate for approximate nearest neighbor search.
///
/// This index is built on startup from all stored embeddings and supports
/// incremental inserts for new records. The index uses cosine distance.
pub struct HnswIndex {
    inner: Arc<RwLock<HnswState>>,
}

struct HnswState {
    index: Index,
    /// Maps internal usearch key (u64) → memory record ID
    key_to_id: Vec<String>,
    /// Next key to assign
    next_key: u64,
    /// Embedding dimensions (set on first insert)
    #[allow(dead_code)]
    dimensions: usize,
}

impl HnswIndex {
    /// Create a new HNSW index and populate it from existing embeddings.
    ///
    /// # Arguments
    /// * `embeddings` - All (id, embedding) pairs from the storage backend
    pub fn build_from(embeddings: &[(String, Vec<f32>)]) -> Result<Self> {
        let dimensions = embeddings.first().map(|(_, e)| e.len()).unwrap_or(768);

        let options = IndexOptions {
            dimensions,
            metric: MetricKind::Cos,
            quantization: ScalarKind::F32,
            connectivity: 16,     // M parameter — good default for recall/speed tradeoff
            expansion_add: 128,   // ef_construction
            expansion_search: 64, // ef_search
            multi: false,
        };

        let index = Index::new(&options)?;
        // Reserve capacity for current + future growth
        let capacity = (embeddings.len() + 10_000).max(20_000);
        index.reserve(capacity)?;

        let mut key_to_id = Vec::with_capacity(embeddings.len());
        let mut next_key: u64 = 0;

        for (id, embedding) in embeddings {
            if embedding.len() != dimensions {
                continue; // Skip dimension mismatches
            }
            index.add(next_key, embedding)?;
            key_to_id.push(id.clone());
            next_key += 1;
        }

        info!(
            count = embeddings.len(),
            dimensions, "HNSW index built successfully"
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(HnswState {
                index,
                key_to_id,
                next_key,
                dimensions,
            })),
        })
    }

    /// Insert a new embedding into the index.
    #[allow(dead_code)]
    pub async fn insert(&self, id: &str, embedding: &[f32]) -> Result<()> {
        let mut state = self.inner.write().await;

        if embedding.len() != state.dimensions {
            anyhow::bail!(
                "Embedding dimension mismatch: expected {}, got {}",
                state.dimensions,
                embedding.len()
            );
        }

        // Grow capacity if needed
        if state.next_key as usize >= state.index.capacity() {
            let new_cap = state.index.capacity() + 10_000;
            state.index.reserve(new_cap)?;
        }

        state.index.add(state.next_key, embedding)?;
        state.key_to_id.push(id.to_string());
        state.next_key += 1;
        Ok(())
    }

    /// Search for the top-k nearest neighbors.
    /// Returns (record_id, distance) pairs. For cosine metric, distance = 1 - similarity.
    pub async fn search(
        &self,
        query: &[f32],
        top_k: usize,
    ) -> Result<Vec<(String, f32)>> {
        let state = self.inner.read().await;

        if state.next_key == 0 {
            return Ok(vec![]);
        }

        let results = state.index.search(query, top_k)?;

        let mut output = Vec::with_capacity(results.keys.len());
        for (key, distance) in results.keys.iter().zip(results.distances.iter()) {
            let idx = *key as usize;
            if idx < state.key_to_id.len() {
                // usearch cosine distance = 1 - similarity, convert back to similarity
                let similarity = 1.0 - distance;
                output.push((state.key_to_id[idx].clone(), similarity));
            }
        }

        Ok(output)
    }

    /// Get the number of vectors in the index.
    #[allow(dead_code)]
    pub async fn len(&self) -> usize {
        let state = self.inner.read().await;
        state.next_key as usize
    }
}
