use dashmap::DashMap;
use rand::Rng;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Notify};

use crate::error::EngineError;
use crate::types::*;

/// RAII guard for in-progress tasks. Dropping without explicit
/// `complete()` transitions the task to Failed (implicit failure).
pub struct TaskGuard {
    engine: Arc<TaskEngine>,
    task_id: TaskId,
    worker: WorkerId,
    done: bool,
}

impl std::fmt::Debug for TaskGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskGuard")
            .field("task_id", &self.task_id)
            .field("worker", &self.worker)
            .field("done", &self.done)
            .finish()
    }
}
impl TaskGuard {
    /// Mark the task as completed with the given result.
    pub fn complete(&mut self, result: TaskResult) {
        self.engine.inner_complete(self.task_id, self.worker, result);
        self.done = true;
    }
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if !self.done {
            self.engine.inner_complete(
                self.task_id,
                self.worker,
                TaskResult::Failed {
                    error_code: 0,
                    output_ref: None,
                },
            );
        }
    }
}

/// Engine configuration.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Directory for journal and snapshots.
    pub data_dir: String,
    /// Maximum journal segment size in bytes before rotation.
    pub journal_max_segment_bytes: u64,
    /// How often to write snapshots (in number of records).
    pub snapshot_interval_records: u64,
    /// Timeout in seconds for graceful shutdown.
    pub shutdown_timeout_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: String::from("."),
            journal_max_segment_bytes: 64 * 1024 * 1024,
            snapshot_interval_records: 10_000,
            shutdown_timeout_secs: 30,
        }
    }
}

/// Lightweight counter bundle for O(1) stats.
struct Counters {
    pending: AtomicU64,
    ready: AtomicU64,
    running: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
}

impl Counters {
    fn new() -> Self {
        Self {
            pending: AtomicU64::new(0),
            ready: AtomicU64::new(0),
            running: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
        }
    }

    fn inc(&self, status: TaskStatus) {
        match status {
            TaskStatus::Pending => { self.pending.fetch_add(1, Ordering::Relaxed); }
            TaskStatus::Ready => { self.ready.fetch_add(1, Ordering::Relaxed); }
            TaskStatus::Running { .. } => { self.running.fetch_add(1, Ordering::Relaxed); }
            TaskStatus::Completed { .. } => { self.completed.fetch_add(1, Ordering::Relaxed); }
            TaskStatus::Cancelled { .. } => { self.cancelled.fetch_add(1, Ordering::Relaxed); }
        }
    }

    fn dec(&self, status: TaskStatus) {
        match status {
            TaskStatus::Pending => { self.pending.fetch_sub(1, Ordering::Relaxed); }
            TaskStatus::Ready => { self.ready.fetch_sub(1, Ordering::Relaxed); }
            TaskStatus::Running { .. } => { self.running.fetch_sub(1, Ordering::Relaxed); }
            TaskStatus::Completed { .. } => { self.completed.fetch_sub(1, Ordering::Relaxed); }
            TaskStatus::Cancelled { .. } => { self.cancelled.fetch_sub(1, Ordering::Relaxed); }
        }
    }

    fn snapshot(&self) -> EngineStats {
        EngineStats {
            pending: self.pending.load(Ordering::Relaxed) as usize,
            ready: self.ready.load(Ordering::Relaxed) as usize,
            running: self.running.load(Ordering::Relaxed) as usize,
            completed: self.completed.load(Ordering::Relaxed) as usize,
            cancelled: self.cancelled.load(Ordering::Relaxed) as usize,
        }
    }
}

/// Core state machine engine for task tracking.
pub struct TaskEngine {
    /// Primary store. TaskId → Task.
    tasks: DashMap<TaskId, Task>,
    /// Dependency graph. (from, to) → Dependency.
    edges: DashMap<(TaskId, TaskId), Dependency>,
    /// Reverse index: TaskId → set of blocking predecessors not yet completed.
    blocking_pending: DashMap<TaskId, HashSet<TaskId>>,
    /// Forward index: TaskId → set of blocking dependents.
    blocking_dependents: DashMap<TaskId, HashSet<TaskId>>,

    /// Ready queue channel.
    ready_tx: mpsc::Sender<TaskId>,
    ready_rx: std::sync::Mutex<Option<mpsc::Receiver<TaskId>>>,
    /// Wakes the scheduler when new Ready tasks arrive.
    ready_notify: Arc<Notify>,

    /// Subscriber notification. Any task status change broadcasts here.
    status_tx: broadcast::Sender<(TaskId, TaskStatus, u64)>,

    /// Monotonic sequence counter.
    seq: AtomicU64,
    /// Shutdown flag.
    shutdown: AtomicBool,
    /// O(1) status counters.
    counters: Counters,
}

impl TaskEngine {
    /// Create a new empty engine.
    pub fn new() -> Arc<Self> {
        let (ready_tx, ready_rx) = mpsc::channel(65_536);
        let (status_tx, _) = broadcast::channel(4096);
        Arc::new(Self {
            tasks: DashMap::new(),
            edges: DashMap::new(),
            blocking_pending: DashMap::new(),
            blocking_dependents: DashMap::new(),
            ready_tx,
            ready_rx: std::sync::Mutex::new(Some(ready_rx)),
            ready_notify: Arc::new(Notify::new()),
            status_tx,
            seq: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            counters: Counters::new(),
        })
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }

    /// Transition a task's status, update counters, broadcast.
    fn transition(
        &self,
        id: TaskId,
        from: TaskStatus,
        to: TaskStatus,
    ) {
        self.counters.dec(from);
        self.counters.inc(to);
        let _ = self.status_tx.send((id, to, self.next_seq()));
    }

    // =========================================================================
    //  Core state machine operations
    // =========================================================================

    /// Create a task. Returns the task's ID.
    pub fn create(self: &Arc<Self>, spec: TaskSpec) -> Result<TaskId, EngineError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(EngineError::EngineShuttingDown);
        }

        let id = TaskId(rand::thread_rng().random::<u128>());
        let now = self.now_millis();
        let seq = self.next_seq();

        // Compute initial in-degree: count blocking deps not already completed.
        let mut in_degree: u16 = 0;
        for dep_id in &spec.blocking_on {
            match self.tasks.get(dep_id) {
                Some(dep_task) => {
                    if !matches!(dep_task.status, TaskStatus::Completed { .. }) {
                        in_degree += 1;
                    }
                }
                None => {
                    // Dependency doesn't exist yet — treat as unsatisfied.
                    in_degree += 1;
                }
            }
        }

        let initial_status = if in_degree == 0 {
            TaskStatus::Ready
        } else {
            TaskStatus::Pending
        };

        let task = Task {
            id,
            spec: spec.clone(),
            status: initial_status,
            created_at: now,
            updated_at: now,
            seq,
            child_count: 0,
            edge_in_degree: in_degree,
        };

        // Insert into store.
        if self.tasks.insert(id, task).is_some() {
            return Err(EngineError::AlreadyExists(id));
        }

        let mut pending_set = HashSet::with_capacity(spec.blocking_on.len());
        for dep_id in &spec.blocking_on {
            // Forward index: dep_id has us as a dependent.
            self.blocking_dependents
                .entry(*dep_id)
                .or_insert_with(HashSet::new)
                .insert(id);

            // Insert edge for the blocking dep.
            let edge_id_var = EdgeId(self.next_seq());
            self.edges.insert(
                (*dep_id, id),
                Dependency {
                    id: edge_id_var,
                    from: *dep_id,
                    to: id,
                    kind: EdgeKind::Blocking,
                    seq,
                },
            );

            // If dep is not completed, track it in our pending set.
            let dep_completed = self
                .tasks
                .get(dep_id)
                .map(|t| matches!(t.status, TaskStatus::Completed { .. }))
                .unwrap_or(false);
            if !dep_completed {
                pending_set.insert(*dep_id);
            }
        }
        if !pending_set.is_empty() {
            self.blocking_pending.insert(id, pending_set);
        }

        // Parent edge.
        if let Some(parent_id) = spec.parent {
            if let Some(mut parent) = self.tasks.get_mut(&parent_id) {
                parent.child_count += 1;
            }
            self.edges.insert(
                (parent_id, id),
                Dependency {
                    id: EdgeId(self.next_seq()),
                    from: parent_id,
                    to: id,
                    kind: EdgeKind::Parent,
                    seq,
                },
            );
            self.blocking_dependents
                .entry(parent_id)
                .or_insert_with(HashSet::new)
                .insert(id);
        }

        // Update counters and maybe enqueue.
        self.counters.inc(initial_status);

        if initial_status == TaskStatus::Ready {
            let _ = self.ready_tx.try_send(id);
            self.ready_notify.notify_one();
        }

        let _ = self.status_tx.send((id, initial_status, seq));

        // TODO: journal record
        Ok(id)
    }

    /// Status transition: Ready → Running. Atomically via entry API.
    pub fn start(
        self: &Arc<Self>,
        id: TaskId,
        worker: WorkerId,
    ) -> Result<TaskGuard, EngineError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(EngineError::EngineShuttingDown);
        }

        let mut task = self.tasks.get_mut(&id).ok_or(EngineError::NotFound(id))?;

        match task.status {
            TaskStatus::Ready => {
                let old_status = task.status;
                let new_status = TaskStatus::Running { worker_id: worker };
                task.status = new_status;
                task.updated_at = self.now_millis();
                task.seq = self.next_seq();
                drop(task);

                self.transition(id, old_status, new_status);
                Ok(TaskGuard {
                    engine: Arc::clone(self),
                    task_id: id,
                    worker,
                    done: false,
                })
            }
            TaskStatus::Running { worker_id } => {
                Err(EngineError::AlreadyRunning { id, worker: worker_id })
            }
            other => Err(EngineError::InvalidTransition {
                id,
                from: other,
                attempted: "start",
            }),
        }
    }

    /// Internal: transition a task to Completed and wake dependents.
    pub(crate) fn inner_complete(
        &self,
        id: TaskId,
        _worker: WorkerId,
        result: TaskResult,
    ) {
        let mut task = match self.tasks.get_mut(&id) {
            Some(t) => t,
            None => return,
        };

        let old_status = task.status;
        let new_status = TaskStatus::Completed { result };
        task.status = new_status;
        task.updated_at = self.now_millis();
        task.seq = self.next_seq();
        drop(task);

        self.transition(id, old_status, new_status);

        // Wake dependents.
        self.on_task_completed(id);
    }
    fn on_task_completed(&self, id: TaskId) {
        let dependents: HashSet<TaskId> = self
            .blocking_dependents
            .remove(&id)
            .map(|(_, set)| set)
            .unwrap_or_default();

        for dep_id in dependents {
            if let Some(mut pending) = self.blocking_pending.get_mut(&dep_id) {
                pending.remove(&id);
                if pending.is_empty() {
                    drop(pending);
                    self.blocking_pending.remove(&dep_id);

                    if let Some(mut task) = self.tasks.get_mut(&dep_id) {
                        if task.status == TaskStatus::Pending {
                            let old_status = task.status;
                            let new_status = TaskStatus::Ready;
                            task.status = new_status;
                            task.updated_at = self.now_millis();
                            task.edge_in_degree = 0;
                            task.seq = self.next_seq();
                            drop(task);

                            self.transition(dep_id, old_status, new_status);
                            let _ = self.ready_tx.try_send(dep_id);
                            self.ready_notify.notify_one();
                        } else {
                            task.edge_in_degree = task.edge_in_degree.saturating_sub(1);
                        }
                    }
                } else {
                    if let Some(mut task) = self.tasks.get_mut(&dep_id) {
                        task.edge_in_degree = task.edge_in_degree.saturating_sub(1);
                    }
                }
            }
        }
    }
    /// Status transition: Any → Cancelled. Cascades to Running/Pending children.
    pub fn cancel(self: &Arc<Self>, id: TaskId, reason: u32) -> Result<(), EngineError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(EngineError::EngineShuttingDown);
        }

        let mut task = self.tasks.get_mut(&id).ok_or(EngineError::NotFound(id))?;

        // Terminal states — no-op.
        match task.status {
            TaskStatus::Completed { .. } | TaskStatus::Cancelled { .. } => {
                return Err(EngineError::InvalidTransition {
                    id,
                    from: task.status,
                    attempted: "cancel",
                });
            }
            _ => {}
        }

        let old_status = task.status;
        let new_status = TaskStatus::Cancelled { reason };
        task.status = new_status;
        task.updated_at = self.now_millis();
        task.seq = self.next_seq();
        drop(task);

        self.transition(id, old_status, new_status);
        let _ = self.status_tx.send((id, new_status, self.next_seq()));

        // Cascade to children via Parent edges.
        let children: Vec<TaskId> = self
            .edges
            .iter()
            .filter(|e| e.value().from == id && e.value().kind == EdgeKind::Parent)
            .map(|e| e.value().to)
            .collect();

        for child_id in children {
            if let Some(child) = self.tasks.get(&child_id) {
                if matches!(
                    child.status,
                    TaskStatus::Running { .. } | TaskStatus::Pending
                ) {
                    drop(child);
                    let _ = self.cancel(child_id, reason);
                }
            }
        }

        // If we were blocking dependents, wake them (they'll see us cancelled).
        if !matches!(old_status, TaskStatus::Completed { .. }) {
            // Treat like completion for dependency graph purposes —
            // cancelled tasks do NOT satisfy blocking deps.
            // Dependents remain pending. This is correct per spec section 6.1.
        }

        // TODO: journal record
        Ok(())
    }

    /// Add a dependency edge between two tasks.
    pub fn depends_on(
        self: &Arc<Self>,
        from: TaskId,
        to: TaskId,
        kind: EdgeKind,
    ) -> Result<EdgeId, EngineError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(EngineError::EngineShuttingDown);
        }

        // Verify both tasks exist.
        if !self.tasks.contains_key(&from) {
            return Err(EngineError::NotFound(from));
        }
        if !self.tasks.contains_key(&to) {
            return Err(EngineError::NotFound(to));
        }

        // Verify no duplicate edge.
        if self.edges.contains_key(&(from, to)) {
            return Err(EngineError::AlreadyExists(to));
        }

        // Cycle detection for Blocking edges.
        if matches!(kind, EdgeKind::Blocking) {
            let mut visited = HashSet::new();
            let mut queue = vec![to];
            while let Some(current) = queue.pop() {
                if current == from {
                    return Err(EngineError::CycleDetected { path: vec![] });
                }
                if !visited.insert(current) {
                    continue;
                }
                // Follow predecessors (tasks that 'current' blocks on).
                if let Some(preds) = self.blocking_pending.get(&current) {
                    for pred in preds.iter() {
                        if !visited.contains(pred) {
                            queue.push(*pred);
                        }
                    }
                }
                // Follow successors (tasks that block on 'current').
                if let Some(deps) = self.blocking_dependents.get(&current) {
                    for dep in deps.iter() {
                        if !visited.contains(dep) {
                            queue.push(*dep);
                        }
                    }
                }
            }
        }

        let edge_id = EdgeId(self.next_seq());
        let seq = self.next_seq();
        let dep = Dependency {
            id: edge_id,
            from,
            to,
            kind,
            seq,
        };

        self.edges.insert((from, to), dep);

        // Update forward index.
        self.blocking_dependents
            .entry(from)
            .or_insert_with(HashSet::new)
            .insert(to);

        // If Blocking and 'from' is not completed, update 'to' pending state.
        if matches!(kind, EdgeKind::Blocking) {
            let from_completed = self
                .tasks
                .get(&from)
                .map(|t| matches!(t.status, TaskStatus::Completed { .. }))
                .unwrap_or(false);

            if !from_completed {
                // Add 'from' to 'to's pending set.
                self.blocking_pending
                    .entry(to)
                    .or_insert_with(HashSet::new)
                    .insert(from);

                // Increment in_degree.
                if let Some(mut task) = self.tasks.get_mut(&to) {
                    task.edge_in_degree = task.edge_in_degree.saturating_add(1);

                    // If 'to' was Ready, demote to Pending.
                    if task.status == TaskStatus::Ready {
                        let old_status = task.status;
                        task.status = TaskStatus::Pending;
                        task.updated_at = self.now_millis();
                        task.seq = self.next_seq();
                        drop(task);

                        self.transition(to, old_status, TaskStatus::Pending);
                        let _ = self.status_tx.send((to, TaskStatus::Pending, self.next_seq()));
                    }
                }
            }
        }

        // TODO: journal record
        Ok(edge_id)
    }

    /// Remove a dependency. If it was the last unsatisfied blocking dep
    /// on `to`, then `to` becomes Ready.
    pub fn remove_dep(self: &Arc<Self>, edge_id: EdgeId) -> Result<(), EngineError> {
        let key = self
            .edges
            .iter()
            .find(|e| e.value().id == edge_id)
            .map(|e| *e.key())
            .ok_or(EngineError::NotFound(TaskId(edge_id.0.into())))?;

        let ((from, to), dep) = self.edges.remove(&key).unwrap();

        // Remove from forward index.
        if let Some(mut deps) = self.blocking_dependents.get_mut(&from) {
            deps.remove(&to);
        }

        // If Blocking and 'from' is not completed, update 'to' state.
        if matches!(dep.kind, EdgeKind::Blocking) {
            let from_completed = self
                .tasks
                .get(&from)
                .map(|t| matches!(t.status, TaskStatus::Completed { .. }))
                .unwrap_or(false);

            if !from_completed {
                if let Some(mut pending) = self.blocking_pending.get_mut(&to) {
                    pending.remove(&from);
                    if pending.is_empty() {
                        drop(pending);
                        self.blocking_pending.remove(&to);

                        if let Some(mut task) = self.tasks.get_mut(&to) {
                            if task.status == TaskStatus::Pending {
                                let old_status = task.status;
                                task.status = TaskStatus::Ready;
                                task.updated_at = self.now_millis();
                                task.edge_in_degree = 0;
                                task.seq = self.next_seq();
                                drop(task);

                                self.transition(to, old_status, TaskStatus::Ready);
                                let _ = self.ready_tx.try_send(to);
                                self.ready_notify.notify_one();
                            }
                        }
                    }
                }
            }
        }

        // TODO: journal record
        Ok(())
    }

    /// Subscribe to status changes. Yields (TaskId, TaskStatus, seq).
    pub fn watch(&self) -> broadcast::Receiver<(TaskId, TaskStatus, u64)> {
        self.status_tx.subscribe()
    }

    /// Count tasks by status (O(1) via counters).
    pub fn stats(&self) -> EngineStats {
        self.counters.snapshot()
    }

    /// Block until a Ready task is available, then return it.
    /// Returns None if the engine is shut down.
    pub async fn recv_ready(self: &Arc<Self>) -> Option<TaskId> {
        let mut rx = self.ready_rx.lock().unwrap();
        rx.as_mut()?.recv().await
    }

    /// Notify the scheduler that new Ready tasks are available.
    pub fn notify_ready(&self) {
        self.ready_notify.notify_one();
    }

    /// Graceful shutdown.
    pub async fn shutdown(self: &Arc<Self>) -> Result<(), EngineError> {
        self.shutdown.store(true, Ordering::Relaxed);

        // Drop the sender side of the ready channel.
        {
            let _ = self.ready_tx.closed().await;
        }

        // Cancel all Running tasks.
        let running_ids: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|t| matches!(t.status, TaskStatus::Running { .. }))
            .map(|t| *t.key())
            .collect();

        for id in running_ids {
            let _ = self.cancel(id, 0);
        }

        self.ready_notify.notify_waiters();

        // TODO: flush journal
        Ok(())
    }

    /// Access a task by ID (for testing / inspection).
    #[cfg(test)]
    pub fn get_task(&self, id: TaskId) -> Option<Task> {
        self.tasks.get(&id).map(|t| t.clone())
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_id() -> TaskId {
        TaskId(rand::thread_rng().random::<u128>())
    }

    fn make_spec(task_type: &str) -> TaskSpec {
        TaskSpec {
            task_type: task_type.to_string(),
            payload: vec![],
            priority: Priority(0),
            parent: None,
            blocking_on: vec![],
            metadata: vec![],
        }
    }

    #[test]
    fn test_create_task_no_deps_is_ready() {
        let engine = TaskEngine::new();
        let id = engine.create(make_spec("test")).unwrap();
        let task = engine.get_task(id).unwrap();
        assert!(matches!(task.status, TaskStatus::Ready));
        assert_eq!(task.edge_in_degree, 0);
    }

    #[test]
    fn test_create_task_with_deps_is_pending() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();
        let task = engine.get_task(b).unwrap();
        assert!(matches!(task.status, TaskStatus::Pending));
        assert_eq!(task.edge_in_degree, 1);
    }

    #[test]
    fn test_complete_task_unblocks_dependent() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();

        // Start and complete A.
        let mut guard = engine.start(a, WorkerId(1)).unwrap();
        guard.complete(TaskResult::Ok { output_ref: 42 });

        // B should now be Ready.
        let task = engine.get_task(b).unwrap();
        assert!(matches!(task.status, TaskStatus::Ready));
        assert_eq!(task.edge_in_degree, 0);
    }

    #[test]
    fn test_start_task_returns_guard() {
        let engine = TaskEngine::new();
        let id = engine.create(make_spec("test")).unwrap();
        let guard = engine.start(id, WorkerId(1)).unwrap();
        let task = engine.get_task(id).unwrap();
        assert!(matches!(task.status, TaskStatus::Running { worker_id: WorkerId(1) }));
        drop(guard);
    }

    #[test]
    fn test_guard_complete_transitions_to_completed() {
        let engine = TaskEngine::new();
        let id = engine.create(make_spec("test")).unwrap();
        let mut guard = engine.start(id, WorkerId(1)).unwrap();
        guard.complete(TaskResult::Ok { output_ref: 99 });

        let task = engine.get_task(id).unwrap();
        assert!(matches!(
            task.status,
            TaskStatus::Completed {
                result: TaskResult::Ok { output_ref: 99 }
            }
        ));
    }

    #[test]
    fn test_guard_drop_without_complete_transitions_to_failed() {
        let engine = TaskEngine::new();
        let id = engine.create(make_spec("test")).unwrap();
        let guard = engine.start(id, WorkerId(1)).unwrap();
        drop(guard);

        let task = engine.get_task(id).unwrap();
        assert!(matches!(
            task.status,
            TaskStatus::Completed {
                result: TaskResult::Failed {
                    error_code: 0,
                    output_ref: None
                }
            }
        ));
    }

    #[test]
    fn test_cannot_start_pending_task() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();
        let err = engine.start(b, WorkerId(1)).unwrap_err();
        assert!(matches!(
            err,
            EngineError::InvalidTransition {
                attempted: "start",
                ..
            }
        ));
    }

    #[test]
    fn test_cannot_start_already_running_task() {
        let engine = TaskEngine::new();
        let id = engine.create(make_spec("test")).unwrap();
        let _guard = engine.start(id, WorkerId(1)).unwrap();
        let err = engine.start(id, WorkerId(2)).unwrap_err();
        assert!(matches!(err, EngineError::AlreadyRunning { .. }));
    }

    #[test]
    fn test_cancel_running_cascades_to_children() {
        let engine = TaskEngine::new();
        let parent = engine
            .create(TaskSpec {
                parent: None,
                ..make_spec("parent")
            })
            .unwrap();
        let child = engine
            .create(TaskSpec {
                parent: Some(parent),
                ..make_spec("child")
            })
            .unwrap();
        // Start the child and keep it running.
        let _guard = engine.start(child, WorkerId(1)).unwrap();

        // Cancel parent — should cascade to child.
        let _ = engine.cancel(parent, 42);

        let child_task = engine.get_task(child).unwrap();
        assert!(matches!(child_task.status, TaskStatus::Cancelled { reason: 42 }));
    }

    #[test]
    fn test_depends_on_cycle_detected() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();

        // a depends on b (blocking) — should create a cycle.
        let err = engine.depends_on(b, a, EdgeKind::Blocking).unwrap_err();
        assert!(matches!(err, EngineError::CycleDetected { .. }));
    }

    #[test]
    fn test_depends_on_already_completed_edge_is_satisfied() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let mut guard = engine.start(a, WorkerId(1)).unwrap();
        guard.complete(TaskResult::Ok { output_ref: 1 });

        // Now create B with A as a blocking dep — A is already completed.
        let b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();

        let task = engine.get_task(b).unwrap();
        assert!(matches!(task.status, TaskStatus::Ready));
        assert_eq!(task.edge_in_degree, 0);
    }

    #[test]
    fn test_remove_dep_unblocks() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();

        // Find the edge ID.
        let edge_id = engine
            .edges
            .iter()
            .find(|e| e.value().from == a && e.value().to == b)
            .map(|e| e.value().id)
            .unwrap();

        // Remove it.
        engine.remove_dep(edge_id).unwrap();

        let task = engine.get_task(b).unwrap();
        assert!(matches!(task.status, TaskStatus::Ready));
    }

    #[test]
    fn test_watch_receives_status_changes() {
        let engine = TaskEngine::new();
        let mut rx = engine.watch();

        let id = engine.create(make_spec("test")).unwrap();

        // Should receive the Ready notification.
        let (recv_id, status, _) = rx.try_recv().unwrap();
        assert_eq!(recv_id, id);
        assert!(matches!(status, TaskStatus::Ready));
    }

    #[test]
    fn test_stats_counts() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let _b = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("b")
            })
            .unwrap();
        let _c = engine
            .create(TaskSpec {
                blocking_on: vec![a],
                ..make_spec("c")
            })
            .unwrap();

        let stats = engine.stats();
        assert_eq!(stats.ready, 1); // a
        assert_eq!(stats.pending, 2); // b, c
        assert_eq!(stats.running, 0);
        assert_eq!(stats.completed, 0);
    }

    #[test]
    fn test_multiple_blocking_deps() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine.create(make_spec("b")).unwrap();
        let c = engine
            .create(TaskSpec {
                blocking_on: vec![a, b],
                ..make_spec("c")
            })
            .unwrap();

        let task = engine.get_task(c).unwrap();
        assert_eq!(task.edge_in_degree, 2);
        assert!(matches!(task.status, TaskStatus::Pending));

        // Complete one — still pending.
        let mut guard = engine.start(a, WorkerId(1)).unwrap();
        guard.complete(TaskResult::Ok { output_ref: 1 });
        let task = engine.get_task(c).unwrap();
        assert!(matches!(task.status, TaskStatus::Pending));
        assert_eq!(task.edge_in_degree, 1);

        // Complete the other — now ready.
        let mut guard = engine.start(b, WorkerId(2)).unwrap();
        guard.complete(TaskResult::Ok { output_ref: 2 });
        let task = engine.get_task(c).unwrap();
        assert!(matches!(task.status, TaskStatus::Ready));
        assert_eq!(task.edge_in_degree, 0);
    }

    #[test]
    fn test_cancel_nonexistent_task() {
        let engine = TaskEngine::new();
        let err = engine.cancel(test_id(), 1).unwrap_err();
        assert!(matches!(err, EngineError::NotFound(_)));
    }

    #[test]
    fn test_cancel_already_completed() {
        let engine = TaskEngine::new();
        let id = engine.create(make_spec("test")).unwrap();
        let mut guard = engine.start(id, WorkerId(1)).unwrap();
        guard.complete(TaskResult::Ok { output_ref: 1 });

        let err = engine.cancel(id, 1).unwrap_err();
        assert!(matches!(
            err,
            EngineError::InvalidTransition {
                attempted: "cancel",
                ..
            }
        ));
    }

    #[test]
    fn test_depends_on_nonexistent_task() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let err = engine.depends_on(a, test_id(), EdgeKind::Blocking).unwrap_err();
        assert!(matches!(err, EngineError::NotFound(_)));
    }

    #[test]
    fn test_duplicate_edge_rejected() {
        let engine = TaskEngine::new();
        let a = engine.create(make_spec("a")).unwrap();
        let b = engine.create(make_spec("b")).unwrap();
        let _ = engine.depends_on(a, b, EdgeKind::Blocking).unwrap();
        let err = engine.depends_on(a, b, EdgeKind::Preference).unwrap_err();
        assert!(matches!(err, EngineError::AlreadyExists(_)));
    }

    #[test]
    fn test_parent_child_tracking() {
        let engine = TaskEngine::new();
        let parent = engine
            .create(TaskSpec {
                parent: None,
                ..make_spec("parent")
            })
            .unwrap();
        let child = engine
            .create(TaskSpec {
                parent: Some(parent),
                ..make_spec("child")
            })
            .unwrap();

        let parent_task = engine.get_task(parent).unwrap();
        assert_eq!(parent_task.child_count, 1);

        let child_task = engine.get_task(child).unwrap();
        assert_eq!(child_task.spec.parent, Some(parent));
    }
}
