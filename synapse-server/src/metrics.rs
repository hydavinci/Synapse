use std::net::SocketAddr;

use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response};
use http_body_util::Full;
use hyper_util::rt::TokioIo;
use lazy_static::lazy_static;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
    TextEncoder,
};
use tokio::net::TcpListener;
use tracing::{error, info};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    /// Total number of memories currently stored.
    pub static ref MEMORIES_TOTAL: IntGauge =
        IntGauge::new("synapse_memories_total", "Total number of memory records stored")
            .expect("metric creation failed");

    /// Histogram of vector search durations.
    pub static ref SEARCH_DURATION_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("synapse_search_duration_seconds", "Vector search duration in seconds")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0])
    ).expect("metric creation failed");

    /// Histogram of embedding computation durations.
    pub static ref EMBEDDING_DURATION_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("synapse_embedding_duration_seconds", "Embedding computation duration in seconds")
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).expect("metric creation failed");

    /// Total number of conflicts detected.
    pub static ref CONFLICTS_TOTAL: IntCounter =
        IntCounter::new("synapse_conflicts_total", "Total number of conflicts detected")
            .expect("metric creation failed");

    /// Total number of gRPC requests processed, by method and status.
    pub static ref REQUESTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("synapse_requests_total", "Total gRPC requests by method and status"),
        &["method", "status"]
    ).expect("metric creation failed");
}

/// Register all metrics with the global registry.
/// Must be called once at startup.
pub fn register_metrics() {
    REGISTRY
        .register(Box::new(MEMORIES_TOTAL.clone()))
        .expect("failed to register MEMORIES_TOTAL");
    REGISTRY
        .register(Box::new(SEARCH_DURATION_SECONDS.clone()))
        .expect("failed to register SEARCH_DURATION_SECONDS");
    REGISTRY
        .register(Box::new(EMBEDDING_DURATION_SECONDS.clone()))
        .expect("failed to register EMBEDDING_DURATION_SECONDS");
    REGISTRY
        .register(Box::new(CONFLICTS_TOTAL.clone()))
        .expect("failed to register CONFLICTS_TOTAL");
    REGISTRY
        .register(Box::new(REQUESTS_TOTAL.clone()))
        .expect("failed to register REQUESTS_TOTAL");
}

/// Handle an HTTP request to the /metrics endpoint.
async fn metrics_handler(
    _req: Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .header("Content-Type", encoder.format_type())
        .body(Full::new(Bytes::from(buffer)))
        .unwrap();

    Ok(response)
}

/// Start the Prometheus metrics HTTP server on the given port.
/// This runs as a background task and serves GET /metrics.
pub async fn start_metrics_server(port: u16) {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind metrics server on {}: {}", addr, e);
            return;
        }
    };

    info!("Prometheus metrics server listening on {}", addr);

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Metrics server accept error: {}", e);
                continue;
            }
        };

        let io = TokioIo::new(stream);
        tokio::spawn(async move {
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service_fn(metrics_handler))
                .await
            {
                error!("Metrics connection error: {}", e);
            }
        });
    }
}
