use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::net::{TcpStream, UnixStream};

use crate::protocol::ProtocolError;

/// Transport connection to a component instance.
pub enum Transport {
    Unix(UnixStream),
    Tcp(TcpStream),
}

/// A registered component instance.
pub struct InstanceConnection {
    pub component_id: String,
    pub transport: Transport,
    pub last_heartbeat: AtomicU64,
}

impl InstanceConnection {
    pub fn new(component_id: String, transport: Transport) -> Self {
        Self {
            component_id,
            transport,
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
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// Concurrent registry of component instances, keyed by component_id.
pub struct InstanceRegistry {
    instances: DashMap<String, Arc<InstanceConnection>>,
    timeout_secs: u64,
}

impl InstanceRegistry {
    pub fn new(timeout_secs: u64) -> Arc<Self> {
        Arc::new(Self {
            instances: DashMap::new(),
            timeout_secs,
        })
    }

    /// Register a component instance.
    pub fn register(
        &self,
        component_id: String,
        transport: Transport,
    ) -> Result<(), ProtocolError> {
        let conn = Arc::new(InstanceConnection::new(component_id.clone(), transport));
        self.instances.insert(component_id, conn);
        Ok(())
    }

    /// Unregister a component instance.
    pub fn unregister(&self, component_id: &str) {
        self.instances.remove(component_id);
    }

    /// Get a connection by component_id.
    pub fn get(&self, component_id: &str) -> Option<Arc<InstanceConnection>> {
        self.instances.get(component_id).map(|c| Arc::clone(&c))
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
        self.instances.retain(|_, conn| conn.is_healthy(timeout));
    }
}
