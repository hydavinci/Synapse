use std::collections::HashMap;
use std::sync::Arc;

use prost_types::Timestamp;
use tokio::sync::RwLock;
use tracing::info;

use crate::proto;

/// Represents this node in a cluster.
/// For v0.1, this is a single-node implementation with stubs for multi-node operations.
pub struct ClusterNode {
    /// This node's unique ID
    pub node_id: String,
    /// This node's address
    pub address: String,
    /// This node's vector clock
    clock: Arc<RwLock<HashMap<String, u64>>>,
    /// Known peers (for future multi-node support)
    peers: Arc<RwLock<HashMap<String, proto::Node>>>,
    /// Total records tracked
    record_count: Arc<RwLock<u64>>,
}

impl ClusterNode {
    pub fn new(node_id: String, address: String) -> Self {
        info!(node_id = %node_id, address = %address, "Initializing cluster node");
        Self {
            node_id: node_id.clone(),
            address,
            clock: Arc::new(RwLock::new(HashMap::from([(node_id, 0)]))),
            peers: Arc::new(RwLock::new(HashMap::new())),
            record_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Increment this node's logical clock. Call on every write operation.
    pub async fn tick(&self) -> HashMap<String, u64> {
        let mut clock = self.clock.write().await;
        let entry = clock.entry(self.node_id.clone()).or_insert(0);
        *entry += 1;
        clock.clone()
    }

    /// Get the current vector clock.
    pub async fn get_clock(&self) -> HashMap<String, u64> {
        self.clock.read().await.clone()
    }

    /// Merge a remote clock into this node's clock (on receiving a sync).
    pub async fn merge_clock(&self, remote: &HashMap<String, u64>) {
        let mut clock = self.clock.write().await;
        for (node, &ts) in remote {
            let entry = clock.entry(node.clone()).or_insert(0);
            *entry = (*entry).max(ts);
        }
        // Also tick ourselves
        let entry = clock.entry(self.node_id.clone()).or_insert(0);
        *entry += 1;
    }

    /// Update the record count.
    pub async fn set_record_count(&self, count: u64) {
        let mut rc = self.record_count.write().await;
        *rc = count;
    }

    /// Handle a join request (stub for multi-node).
    pub async fn handle_join(&self, node_id: &str, address: &str) -> proto::ClusterStatus {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        let new_node = proto::Node {
            id: node_id.to_string(),
            address: address.to_string(),
            state: proto::NodeState::Active as i32,
            last_heartbeat: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            clock: Some(proto::VectorClock {
                clock: HashMap::new(),
            }),
        };

        let mut peers = self.peers.write().await;
        peers.insert(node_id.to_string(), new_node);
        info!(peer = node_id, "Peer joined cluster");

        self.get_status().await
    }

    /// Handle a leave request (stub for multi-node).
    pub async fn handle_leave(&self, node_id: &str) -> bool {
        let mut peers = self.peers.write().await;
        let removed = peers.remove(node_id).is_some();
        if removed {
            info!(peer = node_id, "Peer left cluster");
        }
        removed
    }

    /// Get cluster status.
    pub async fn get_status(&self) -> proto::ClusterStatus {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();

        let clock = self.clock.read().await;
        let peers = self.peers.read().await;
        let record_count = *self.record_count.read().await;

        // Build node list: self + peers
        let mut nodes = vec![proto::Node {
            id: self.node_id.clone(),
            address: self.address.clone(),
            state: proto::NodeState::Active as i32,
            last_heartbeat: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            clock: Some(proto::VectorClock {
                clock: clock.clone(),
            }),
        }];

        for peer in peers.values() {
            nodes.push(peer.clone());
        }

        proto::ClusterStatus {
            nodes,
            consistency: proto::ConsistencyLevel::Eventual as i32,
            total_records: record_count,
            pending_syncs: 0,
            active_conflicts: 0,
        }
    }
}
