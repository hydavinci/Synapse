mod api;
mod cluster;
mod config;
mod conflict;
mod scope;
mod search;
mod storage;

/// Auto-generated protobuf/gRPC code.
#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("synapse.v1");

    // Re-export resolution strategy for ergonomics
    impl ResolutionStrategy {
        pub fn try_from(value: i32) -> Option<Self> {
            match value {
                0 => Some(Self::ResolutionStrategyUnspecified),
                1 => Some(Self::LastWriterWins),
                2 => Some(Self::FirstWriterWins),
                3 => Some(Self::LlmMerge),
                4 => Some(Self::KeepBoth),
                5 => Some(Self::ManualResolution),
                6 => Some(Self::ConfidenceWins),
                7 => Some(Self::Custom),
                _ => None,
            }
        }
    }

    impl Visibility {
        pub fn try_from(value: i32) -> Option<Self> {
            match value {
                0 => Some(Self::VisibilityUnspecified),
                1 => Some(Self::Private),
                2 => Some(Self::ScopeUp),
                3 => Some(Self::ScopeDown),
                4 => Some(Self::Shared),
                5 => Some(Self::Public),
                _ => None,
            }
        }
    }

    impl EventType {
        pub const fn from_i32(value: i32) -> Self {
            match value {
                1 => Self::MemoryAdded,
                2 => Self::MemoryUpdated,
                3 => Self::MemoryForgotten,
                4 => Self::ConflictDetected,
                5 => Self::ConflictResolved,
                6 => Self::MemoryExpired,
                _ => Self::EventAll,
            }
        }
    }
}

use std::sync::Arc;

use tokio::sync::broadcast;
use tonic::transport::Server;
use tracing::info;

use api::grpc::{ClusterServiceImpl, ConflictServiceImpl, MemoryServiceImpl};
use cluster::ClusterNode;
use config::Config;
use conflict::ConflictDetector;
use proto::cluster_service_server::ClusterServiceServer;
use proto::conflict_service_server::ConflictServiceServer;
use proto::memory_service_server::MemoryServiceServer;
use search::VectorSearch;
use storage::InMemoryStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config
    let config = Config::load(None);

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.server.log_level.parse().unwrap_or_default()),
        )
        .init();

    info!("Starting Synapse Memory Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Node ID: {}", config.cluster.node_id);
    info!("Listen address: {}", config.listen_addr());

    // Initialize storage
    let store: Arc<dyn storage::StorageBackend> = Arc::new(InMemoryStore::new());
    info!("Storage backend: in-memory");

    // Initialize search
    let search = Arc::new(VectorSearch::new(store.clone()));

    // Initialize conflict detection
    let conflict_detector = Arc::new(ConflictDetector::new(config.conflict.similarity_threshold));
    info!(
        threshold = config.conflict.similarity_threshold,
        "Conflict detector initialized"
    );

    // Initialize cluster node
    let cluster = Arc::new(ClusterNode::new(
        config.cluster.node_id.clone(),
        config.listen_addr(),
    ));

    // Event broadcast channel
    let (events_tx, _) = broadcast::channel::<proto::MemoryEvent>(1024);

    // Build gRPC services
    let memory_service = MemoryServiceImpl::new(
        store.clone(),
        search.clone(),
        conflict_detector.clone(),
        cluster.clone(),
        events_tx.clone(),
    );

    let conflict_service = ConflictServiceImpl::new(conflict_detector.clone(), store.clone());
    let cluster_service = ClusterServiceImpl::new(cluster.clone());

    // Parse listen address
    let addr = config.listen_addr().parse()?;

    info!("Synapse server listening on {}", addr);

    // Start server
    Server::builder()
        .add_service(MemoryServiceServer::new(memory_service))
        .add_service(ConflictServiceServer::new(conflict_service))
        .add_service(ClusterServiceServer::new(cluster_service))
        .serve(addr)
        .await?;

    Ok(())
}
