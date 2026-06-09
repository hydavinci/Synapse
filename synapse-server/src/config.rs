use serde::Deserialize;
use std::path::PathBuf;

/// Server configuration loaded from file or environment variables.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_server")]
    pub server: ServerConfig,

    #[serde(default)]
    pub cluster: ClusterConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub conflict: ConflictConfig,

    /// Authentication token. Also overridable via SYNAPSE_AUTH_TOKEN env var.
    #[serde(default)]
    pub auth_token: Option<String>,

    /// Cluster join secret. Nodes must present this to join.
    #[serde(default)]
    pub cluster_secret: Option<String>,

    /// Maximum content size in bytes (default: 1MB).
    #[serde(default = "default_max_content_bytes")]
    pub max_content_bytes: usize,

    /// Maximum embedding dimensions (default: 4096).
    #[serde(default = "default_max_embedding_dims")]
    pub max_embedding_dims: usize,

    /// TLS configuration for gRPC server.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

/// TLS configuration for the gRPC server.
#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    /// Path to PEM-encoded server certificate.
    pub cert_path: PathBuf,
    /// Path to PEM-encoded server private key.
    pub key_path: PathBuf,
    /// Optional path to CA certificate for client verification (mTLS).
    pub ca_cert_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_log_level")]
    pub log_level: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    #[serde(default = "default_node_id")]
    pub node_id: String,

    #[serde(default)]
    pub peers: Vec<String>,

    #[serde(default = "default_consistency")]
    pub consistency: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_backend")]
    pub backend: String,

    #[serde(default = "default_max_records")]
    pub max_records: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConflictConfig {
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f32,

    #[serde(default = "default_resolution_strategy")]
    pub default_strategy: String,
}

// Defaults
fn default_server() -> ServerConfig {
    ServerConfig {
        host: default_host(),
        port: default_port(),
        log_level: default_log_level(),
    }
}

fn default_host() -> String {
    std::env::var("SYNAPSE_HOST").unwrap_or_else(|_| "0.0.0.0".to_string())
}

fn default_port() -> u16 {
    std::env::var("SYNAPSE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9090)
}

fn default_log_level() -> String {
    std::env::var("SYNAPSE_LOG_LEVEL").unwrap_or_else(|_| "info".to_string())
}

fn default_node_id() -> String {
    std::env::var("SYNAPSE_NODE_ID").unwrap_or_else(|_| {
        format!("node-{}", &ulid::Ulid::new().to_string()[..8])
    })
}

fn default_consistency() -> String {
    std::env::var("SYNAPSE_CONSISTENCY").unwrap_or_else(|_| "eventual".to_string())
}

fn default_backend() -> String {
    "memory".to_string()
}

fn default_max_records() -> u64 {
    1_000_000
}

fn default_similarity_threshold() -> f32 {
    std::env::var("SYNAPSE_SIMILARITY_THRESHOLD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.85)
}

fn default_resolution_strategy() -> String {
    "last_writer_wins".to_string()
}

fn default_max_content_bytes() -> usize {
    1_048_576 // 1MB
}

fn default_max_embedding_dims() -> usize {
    4096
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: default_server(),
            cluster: ClusterConfig::default(),
            storage: StorageConfig::default(),
            conflict: ConflictConfig::default(),
            auth_token: None,
            cluster_secret: None,
            max_content_bytes: default_max_content_bytes(),
            max_embedding_dims: default_max_embedding_dims(),
            tls: None,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_id: default_node_id(),
            peers: vec![],
            consistency: default_consistency(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            max_records: default_max_records(),
        }
    }
}

impl Default for ConflictConfig {
    fn default() -> Self {
        Self {
            similarity_threshold: default_similarity_threshold(),
            default_strategy: default_resolution_strategy(),
        }
    }
}

impl Config {
    /// Load config from a TOML file, with environment variable overrides.
    pub fn load(path: Option<PathBuf>) -> Self {
        let config = if let Some(p) = path {
            if p.exists() {
                let content = std::fs::read_to_string(&p).unwrap_or_default();
                toml::from_str(&content).unwrap_or_default()
            } else {
                Config::default()
            }
        } else {
            // Try default paths
            let candidates = ["synapse.toml", "config/default.toml"];
            let mut loaded = None;
            for candidate in candidates {
                let p = PathBuf::from(candidate);
                if p.exists() {
                    if let Ok(content) = std::fs::read_to_string(&p) {
                        loaded = toml::from_str(&content).ok();
                        break;
                    }
                }
            }
            loaded.unwrap_or_default()
        };

        config
    }

    /// Get the server listen address.
    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }
}
