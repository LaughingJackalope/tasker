use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{TaskId, TaskStatus, WorkerId};

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum EngineError {
    #[error("task {0:?} not found")]
    NotFound(TaskId),

    #[error("task {0:?} already exists")]
    AlreadyExists(TaskId),

    #[error("invalid transition for task {id:?}: cannot {attempted} from {from:?}")]
    InvalidTransition {
        id: TaskId,
        from: TaskStatus,
        attempted: &'static str,
    },

    #[error("cycle detected: path through tasks {path:?}")]
    CycleDetected { path: Vec<TaskId> },

    #[error("task {id:?} already running on worker {worker:?}")]
    AlreadyRunning { id: TaskId, worker: WorkerId },

    #[error("task {0:?} was cancelled")]
    Cancelled(TaskId),

    #[error("engine is shutting down")]
    EngineShuttingDown,

    #[error("corrupt journal at seq {0}")]
    CorruptJournal(u64),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("no instance registered for component {0}")]
    NoInstance(String),

    #[error("transport error: {0}")]
    TransportError(String),
}
