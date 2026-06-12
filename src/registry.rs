use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio::sync::Mutex;

use crate::protocol::ProtocolError;

/// A registered component instance.
pub struct InstanceConnection {
    pub component_id: String,
    pub last_heartbeat: AtomicU64,
}

impl InstanceConnection {
    pub fn new(component_id: String) -> Self {
        Self {
            component_id,
            last_heartbeat: AtomicU64::new(now_millis()),
        }
    }

    pub fn heartbeat(&self) {
        self.last_heartbeat.store(now_millis(), Ordering::Relaxed);
    }

    pub fn is_healthy(&self, timeout_secs: u64) -> bool {
        let elapsed = now_millis() - self.last_heartbeat.load(Ordering::Relaxed);
        elapsed < timeout_secs * 1000
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// Shared stream wrapper for registered instances.
pub type SharedStream = Arc<Mutex<tokio::net::UnixStream>>;

/// Concurrent registry of component instances, keyed by component_id.
pub struct InstanceRegistry {
    instances: DashMap<String, Arc<InstanceConnection>>,
    timeout_secs: u64,
    /// Shared streams of connected instances, keyed by component_id.
    pub streams: Arc<Mutex<HashMap<String, SharedStream>>>,
}

impl InstanceRegistry {
    pub fn new(timeout_secs: u64) -> Arc<Self> {
        Arc::new(Self {
            instances: DashMap::new(),
            timeout_secs,
            streams: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Register a component instance.
    pub fn register(&self, component_id: String) -> Result<(), ProtocolError> {
        let conn = Arc::new(InstanceConnection::new(component_id.clone()));
        self.instances.insert(component_id, conn);
        Ok(())
    }

    /// Unregister a component instance.
    pub fn unregister(&self, component_id: &str) {
        self.instances.remove(component_id);
    }

    /// Get a connection by component_id.
    pub fn get(&self, component_id: &str) -> Option<Arc<InstanceConnection>> {
        self.instances
            .get(component_id)
            .map(|c| Arc::clone(&c))
    }

    /// Check if a component instance is registered and healthy.
    pub fn is_healthy(&self, component_id: &str) -> bool {
        self.instances
            .get(component_id)
            .map(|c| c.is_healthy(self.timeout_secs))
            .unwrap_or(false)
    }

    /// List all registered component_ids.
    pub fn list(&self) -> Vec<String> {
        self.instances.iter().map(|e| e.key().clone()).collect()
    }

    /// Remove stale instances that haven't heartbeated within the timeout.
    pub fn evict_stale(&self) {
        let timeout = self.timeout_secs;
        self.instances
            .retain(|_, conn| conn.is_healthy(timeout));
    }
}
