mod api;
#[cfg(feature = "cluster")]
mod cluster;
mod config;
#[allow(dead_code)]
mod conflict;
mod metrics;
mod scope;
mod search;
mod storage;

/// Auto-generated protobuf/gRPC code.
#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("synapse.v1");
}

use std::sync::Arc;

use tokio::signal;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tower::limit::ConcurrencyLimitLayer;
use tracing::info;

mod auth;
mod ratelimit;

#[cfg(feature = "cluster")]
use api::grpc::ClusterServiceImpl;
use api::grpc::{ConflictServiceImpl, MemoryServiceImpl};
use auth::AuthInterceptor;
#[cfg(feature = "cluster")]
use cluster::ClusterNode;
use config::Config;
use conflict::ConflictDetector;
#[cfg(feature = "cluster")]
use proto::cluster_service_server::ClusterServiceServer;
use proto::conflict_service_server::ConflictServiceServer;
use proto::memory_service_server::MemoryServiceServer;
use search::VectorSearch;
use storage::{InMemoryStore, SqliteStore};

/// Stub cluster node used when the cluster feature is disabled.
#[cfg(not(feature = "cluster"))]
mod cluster_stub {
    use std::collections::HashMap;

    /// Minimal stub that satisfies the API without actual cluster logic.
    pub struct ClusterNode {
        pub node_id: String,
    }

    impl ClusterNode {
        pub fn new(node_id: String) -> Self {
            Self { node_id }
        }

        pub async fn tick(&self) -> HashMap<String, u64> {
            HashMap::from([(self.node_id.clone(), 0)])
        }

        pub async fn set_record_count(&self, _count: u64) {}
    }
}

#[cfg(not(feature = "cluster"))]
use cluster_stub::ClusterNode;

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

    info!(
        "Starting Synapse Memory Server v{}",
        env!("CARGO_PKG_VERSION")
    );
    info!("Node ID: {}", config.cluster.node_id);
    info!("Listen address: {}", config.listen_addr());

    // Register and start Prometheus metrics
    metrics::register_metrics();
    tokio::spawn(metrics::start_metrics_server(9093));
    info!("Prometheus metrics endpoint starting on :9093/metrics");

    // Initialize storage based on config
    let store: Arc<dyn storage::StorageBackend> = match config.storage.backend.as_str() {
        "sqlite" => {
            let path = config.storage.sqlite_path.clone();
            info!("Storage backend: sqlite ({})", path.display());
            Arc::new(SqliteStore::new(path)?)
        }
        "memory" => {
            info!("Storage backend: in-memory (data will not persist across restarts)");
            Arc::new(InMemoryStore::new())
        }
        other => {
            anyhow::bail!(
                "Unknown storage backend: '{}'. Supported: sqlite, memory",
                other
            );
        }
    };

    // Set initial memory count metric
    if let Ok(count) = store.count().await {
        metrics::MEMORIES_TOTAL.set(count as i64);
    }

    // Initialize search with HNSW index
    let search = Arc::new(VectorSearch::new(store.clone()));
    if let Err(e) = search.init_index().await {
        tracing::warn!("Failed to initialize HNSW index (falling back to brute-force): {}", e);
    }

    // Background task: TTL cleanup every 5 minutes
    {
        let store_cleanup = store.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
            interval.tick().await; // skip first immediate tick
            loop {
                interval.tick().await;
                match store_cleanup.cleanup_expired().await {
                    Ok(0) => {}
                    Ok(n) => info!("TTL cleanup: removed {} expired records", n),
                    Err(e) => tracing::error!("TTL cleanup failed: {}", e),
                }
            }
        });
        info!("TTL cleanup task scheduled (every 5 minutes)");
    }

    // Initialize conflict detection
    let conflict_detector = Arc::new(ConflictDetector::new(config.conflict.similarity_threshold));
    info!(
        threshold = config.conflict.similarity_threshold,
        "Conflict detector initialized"
    );

    // Initialize cluster node
    #[cfg(feature = "cluster")]
    let cluster = Arc::new(ClusterNode::with_secret(
        config.cluster.node_id.clone(),
        config.listen_addr(),
        config.cluster_secret.clone(),
    ));
    #[cfg(not(feature = "cluster"))]
    let cluster = Arc::new(ClusterNode::new(config.cluster.node_id.clone()));

    // Event broadcast channel
    // CVE-7: Reduced buffer to 256 to limit memory usage from slow consumers.
    // Lagged receivers will skip messages (BroadcastStream handles this gracefully).
    let (events_tx, _) = broadcast::channel::<proto::MemoryEvent>(256);

    // Health reporter (gRPC health checking protocol)
    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<MemoryServiceServer<MemoryServiceImpl>>()
        .await;
    health_reporter
        .set_serving::<ConflictServiceServer<ConflictServiceImpl>>()
        .await;
    #[cfg(feature = "cluster")]
    health_reporter
        .set_serving::<ClusterServiceServer<ClusterServiceImpl>>()
        .await;

    // Authentication interceptor
    let auth = AuthInterceptor::from_config(&config);
    if auth.is_enabled() {
        info!("Authentication enabled (Bearer token)");
    } else {
        tracing::warn!(
            "Authentication DISABLED — server is open to all. Set SYNAPSE_AUTH_TOKEN to secure."
        );
    }

    // Build gRPC services
    let rate_limiter = Arc::new(ratelimit::ScopeRateLimiter::new(
        ratelimit::RateLimitConfig {
            max_requests: config.rate_limit.max_requests,
            window: std::time::Duration::from_secs(config.rate_limit.window_secs),
            max_scopes: config.rate_limit.max_scopes,
        },
    ));

    let memory_service = MemoryServiceImpl::new(
        store.clone(),
        search.clone(),
        conflict_detector.clone(),
        cluster.clone(),
        events_tx.clone(),
        rate_limiter.clone(),
    );

    let conflict_service = ConflictServiceImpl::new(conflict_detector.clone(), store.clone());

    // Parse listen address
    let addr = config.listen_addr().parse()?;

    info!("Synapse server listening on {}", addr);

    // CVE-15: TLS support — must be configured before adding layers
    let mut server_builder = Server::builder();

    if let Some(ref tls_config) = config.tls {
        use tonic::transport::{Identity, ServerTlsConfig};
        let cert = tokio::fs::read(&tls_config.cert_path).await?;
        let key = tokio::fs::read(&tls_config.key_path).await?;
        let identity = Identity::from_pem(cert, key);

        let mut tls = ServerTlsConfig::new().identity(identity);

        if let Some(ref ca_path) = tls_config.ca_cert_path {
            let ca = tokio::fs::read(ca_path).await?;
            let ca_cert = tonic::transport::Certificate::from_pem(ca);
            tls = tls.client_ca_root(ca_cert);
            info!("mTLS enabled (client certificate verification)");
        }

        server_builder = server_builder.tls_config(tls)?;
        info!("TLS enabled");
    } else {
        tracing::warn!(
            "TLS DISABLED — gRPC traffic is unencrypted. Configure [tls] section to secure."
        );
    }

    // CVE-13: Global concurrency limit — max 256 concurrent requests
    let concurrency_limit = ConcurrencyLimitLayer::new(256);

    let router = server_builder
        .layer(concurrency_limit)
        .add_service(health_service)
        .add_service(MemoryServiceServer::with_interceptor(
            memory_service,
            auth.clone(),
        ))
        .add_service(ConflictServiceServer::with_interceptor(
            conflict_service,
            auth.clone(),
        ));

    // Add cluster service only when feature is enabled
    #[cfg(feature = "cluster")]
    let router = {
        let cluster_service = ClusterServiceImpl::new(cluster.clone());
        router.add_service(ClusterServiceServer::with_interceptor(
            cluster_service,
            auth,
        ))
    };

    #[cfg(not(feature = "cluster"))]
    let _ = auth; // suppress unused warning

    router.serve_with_shutdown(addr, shutdown_signal()).await?;

    info!("Synapse server shut down gracefully");
    Ok(())
}

/// Wait for SIGINT (Ctrl+C) or SIGTERM for graceful shutdown.
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down..."),
        _ = terminate => info!("Received SIGTERM, shutting down..."),
    }
}
