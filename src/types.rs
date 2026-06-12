use serde::{Deserialize, Serialize};

/// Unique identifier for a task. 128-bit allows randomized generation
/// without coordination (UUID v4/v7 compatible) OR sequential ULIDs
/// for sortability. We use TaskId = u128 for cache-line alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub u128);

/// Dependency edge with ordering semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EdgeId(pub u64);

/// Worker identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(pub u64);

/// Priority as an i8: [-128, 127]. Most agent tasks are Priority(0).
/// Negative = low priority cleanup. Positive = urgent blocking tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Priority(pub i8);

/// What kind of dependency. This is the key abstraction for DAG scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeKind {
    /// Hard dependency: B cannot start until A completes.
    Blocking,

    /// Weak dependency: B prefers that A completes first, but can start
    /// without it. Useful for soft ordering hints from agents.
    Preference,

    /// Data dependency: A produces an artifact that B consumes.
    /// The engine does NOT enforce ordering — the agent is responsible
    /// for ensuring the artifact exists. This is metadata for the agent,
    /// not schedulable.
    Artifact {
        artifact_type: u32,
        artifact_id: u128,
    },

    /// Child relationship: A is a decomposition of B.
    /// Orthogonal to Blocking/Preference.
    Parent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is defined but not yet schedulable. Waiting on at least
    /// one Blocking edge to be satisfied.
    Pending,

    /// All blocking dependencies satisfied. Ready to be picked up
    /// by a worker.
    Ready,

    /// Actively being executed by a worker.
    Running { worker_id: WorkerId },

    /// Execution finished. Ok or Failed variants carry the result
    /// artifact reference.
    Completed { result: TaskResult },

    /// Manually cancelled or orphaned due to cascade.
    Cancelled { reason: u32 },
}

/// Engine stats snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct EngineStats {
    pub pending: usize,
    pub ready: usize,
    pub running: usize,
    pub completed: usize,
    pub cancelled: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskResult {
    Ok {
        output_ref: u128,
    },
    Failed {
        error_code: u32,
        output_ref: Option<u128>,
    },
}

/// Defines a task. This is the CREATE payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub task_type: String,
    pub payload: Vec<u8>,
    pub priority: Priority,
    pub parent: Option<TaskId>,
    pub blocking_on: Vec<TaskId>,
    pub metadata: Vec<(String, Vec<u8>)>,
}

/// The live task. This is what lives in the TaskStore.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: TaskId,
    pub spec: TaskSpec,
    pub status: TaskStatus,
    pub created_at: u64,
    pub updated_at: u64,
    pub seq: u64,
    pub child_count: u16,
    pub edge_in_degree: u16,
}

/// An edge lives in a separate DashMap keyed by (TaskId, TaskId).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub id: EdgeId,
    pub from: TaskId,
    pub to: TaskId,
    pub kind: EdgeKind,
    pub seq: u64,
}
