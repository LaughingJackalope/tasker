# TASKER: Agent-Native Task Tracking Engine

## Architectural Blueprint v0.1

---

## 1. Core Thesis

Human task trackers optimize for *display*. Agents need a system optimized for *throughput*, *dependency resolution*, and *state determinism*. Tasker is not a database with an API — it is a **state machine** for autonomous cognition.

The central insight: agent task graphs are not trees. They are **DAGs with runtime-hydrated edges** — dependencies that may not exist until a predecessor produces them. This demands an in-memory first architecture with append-only persistence, not a disk-first RDBMS.

---

## 2. Communication Protocol

### Choice: Custom binary over Unix Domain Socket (local) / TCP (remote)

**Why not REST/JSON?**
- JSON parsing is a bottleneck at agent throughput levels (10k+ task ops/sec).
- REST's request-response model doesn't support push-based status propagation.
- Human-centric error codes (404, 409) are noise for agents. Agents need *typed failure modes*.

**Why not gRPC?**
- Protobuf is excellent for stable schemas, but agent task schemas evolve rapidly.
- gRPC's code generation pipeline adds friction for a system that will be iterated on heavily.
- HTTP/2 multiplexing is overkill for the local IPC case, and we get stream semantics ourselves.

**The Protocol: Framed Binary + Atomipc framing**

```
┌─────────────────────────────────────────┐
│  Magic (4 bytes): 0x5441534B ("TASK")  │
│  Version (1 byte)                       │
│  Message Type (1 byte)                  │
│  Stream ID (2 bytes)                    │
│  Payload Length (4 bytes, LE)          │
│  Payload (variable, MessagePack encoded)│
│  CRC32 (4 bytes)                        │
└─────────────────────────────────────────┘
```

Total frame overhead: 16 bytes. Payloads use **MessagePack** for a critical reason: it's schema-free on the wire, compact like binary, and every primitive maps trivially to Rust types via `rmpv`/`serde`. Agents send a `[stream_id, method_name, params_array]` tuple; the engine responds with `[stream_id, result_or_error]`. Stream IDs multiplex concurrent operations. No headers, no cookies, no negotiation.

**For programmatic use (agent embedding):** A direct Rust crate API (`tasker::Engine`) is the primary interface. The binary protocol is for *remote* agents or language-agnostic callers. The crate is the canonical API; the wire protocol is an implementation detail.

---

## 3. Storage Architecture

```
┌──────────────────────────────────────────────────┐
│                  Task Engine                      │
│  ┌─────────────┐  ┌──────────┐  ┌────────────┐  │
│  │ Task Store   │  │ Scheduler│  │  Watcher    │  │
│  │ (DashMap)    │←→│ (Bevy-   │  │  (Tokio     │  │
│  │ Lock-free    │  │  style)  │  │   streams)  │  │
│  │ concurrent   │  │          │  │             │  │
│  └──────┬───────┘  └──────────┘  └────────────┘  │
│         │                                          │
│  ┌──────▼──────────────────────────────────────┐  │
│  │         Append-Only Journal                  │  │
│  │  Segment files: journal-0001.log, 0002.log.. │  │
│  │  Format: [header][record][record]...[padding] │  │
│  │  Compaction: sort-merge by TaskId, keep last  │ │
│  └──────────────────────────────────────────────┘  │
│         │                                          │
│  ┌──────▼──────────────────────────────────────┐  │
│  │     Snapshot Store (periodic)                │  │
│  │  Full engine state → Tasker snapshot format  │  │
│  └──────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

### Why DashMap over LMDB/SQLite/RocksDB?

- **DashMap**: Lock-free concurrent hashmap. Read-heavy workloads (status queries, dependency checks) are wait-free. Write operations (task creation, state transitions) are fine-grained per-shard. This is the hot path — sub-microsecond lookups.
- **Append-Only Journal**: Durability without page fragmentation. Write amplification is exactly 1x (no B-tree rebalancing). Compaction is offline and parallel — merge-sort segments, keep latest state per TaskId.
- **Snapshots**: Periodic full-state serialization. Recovery = load latest snapshot + replay journal entries after snapshot's sequence number.

### Alternative evaluated and deferred: ECS (Entity-Component-System)

An ECS (Bevy-style) is tempting for the scheduler — each task is an entity, its status/blocking are components, and scheduling is a "system" that runs over the query. **Deferred** because:
1. ECS adds optimization for *homogeneous* iteration over *all* entities. Our workload is heterogeneous — the scheduler touches thousands of tasks concurrently with different priority/dependency profiles.
2. The memory layout optimization (SoA via archetypes) favors batch processing of identical task types. Agent task types are inherently diverse.

**Revisit if:** profiling shows the DashMap + dependency graph architecture is the bottleneck. The journal + snapshot design is agnostic to the in-memory store.

---

## 4. Foundational Data Structures

### 4.1 Task — The Core Unit

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Unique identifier for a task. 128-bit allows randomized generation
/// without coordination (UUID v4/v7 compatible) OR sequential ULIDs
/// for sortability. We use TaskId = u128 for cache-line alignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub u128);

/// Dependency edge with ordering semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EdgeId(pub u64);

/// What kind of dependency. This is the key abstraction for DAG scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EdgeKind {
    /// Hard dependency: B cannot start until A completes.
    /// This is the standard "blocks" relationship.
    Blocking,

    /// Weak dependency: B prefers that A completes first, but can start
    /// without it. Useful for soft ordering hints from agents.
    Preference,

    /// Data dependency: A produces an artifact that B consumes.
    /// The engine does NOT enforce ordering — the agent is responsible
    /// for ensuring the artifact exists. This is metadata for the agent,
    /// not schedulable.
    Artifact { artifact_type: u32, artifact_id: u128 },

    /// Child relationship: A is a decomposition of B.
    /// orthogonal to Blocking/Preference. A task can "block" on
    /// its parent completing, OR on all its children completing.
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

    /// Actively being executed by a worker. worker_id holds the
    /// executing agent/process.
    Running { worker_id: WorkerId },

    /// Execution finished. Ok or Failed variants carry the result
    /// artifact reference.
    Completed { result: TaskResult },

    /// Manually cancelled or orphaned due to cascade.
    Cancelled { reason: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaskResult {
    Ok { output_ref: u128 },
    Failed { error_code: u32, output_ref: Option<u128> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(pub u64);

/// Priority as an i8: [-128, 127]. Most agent tasks are Priority(0).
/// Negative = low priority cleanup. Positive = urgent blocking tasks.
/// The sign encoding (one's complement) is deliberate: Priority(-1) has
/// a smaller integer value than Priority(0), so natural sort ordering
/// gives correct precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Priority(pub i8);

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
    pub created_at: u64,   // Unix millis
    pub updated_at: u64,
    pub seq: u64,          // Monotonic sequence number for journal ordering
    pub child_count: u16,
    pub edge_in_degree: u16, // Number of unsatisfied Blocking edges
    pub cancellation_token: u128, // Links to shared cancel registry
}

/// An edge lives in a separate DashMap keyed by (TaskId, TaskId).
/// Separating from Task keeps the Task struct lean (cache-line-sized Engine
/// can process tasks without eviction of edge metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub id: EdgeId,
    pub from: TaskId,   // The dependency target (the "needs" side)
    pub to: TaskId,     // The dependent (the "blocked" side)
    pub kind: EdgeKind,
    pub seq: u64,
}
```

### 4.2 The Engine

```rust
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Notify};

pub struct TaskEngine {
    /// Primary store. TaskId → Task.
    tasks: DashMap<TaskId, Task>,

    /// Dependency graph. (from, to) → Dependency.
    edges: DashMap<(TaskId, TaskId), Dependency>,

    /// Reverse index: TaskId → set of blocking predecessors not yet completed.
    /// Used to decrement edge_in_degree and trigger Ready transitions.
    blocking_pending: DashMap<TaskId, HashSet<TaskId>>,

    /// Forward index: TaskId → set of blocking dependents.
    /// Used when a task completes: decrement in_degree of all dependents.
    blocking_dependents: DashMap<TaskId, HashSet<TaskId>>,

    /// Ready queue. Tasks with edge_in_degree == 0.
    /// channel to scheduler.
    ready_tx: mpsc::Sender<TaskId>,
    ready_rx: mpsc::Receiver<TaskId>,

    /// Subscriber notification. Any task status change broadcasts here.
    status_tx: broadcast::Sender<(TaskId, TaskStatus)>,

    /// Wakes the scheduler when new Ready tasks arrive.
    ready_notify: Arc<Notify>,

    /// Journal writer. Single-writer principle for ordering.
    journal: Arc<tokio::sync::Mutex<JournalWriter>>,

    /// Cancellation registry. TaskId → CancelToken.
    cancel_tokens: DashMap<TaskId, CancelToken>,
}
```

---

## 5. API Surface (crate API)

The wire protocol maps 1:1 to these functions. No impedance mismatch.

```rust
impl TaskEngine {
    /// Create a task. Returns the task's ID and its initial status.
    ///
    /// If `blocking_on` is empty → Status::Ready immediately.
    /// Otherwise → Status::Pending.
    pub async fn create(&self, spec: TaskSpec) -> Result<TaskId, EngineError>;

    /// Status transition: Ready → Running. Atomically.
    /// Returns a Guard that, when dropped, auto-transitions to Completed/Failed.
    pub async fn start(&self, id: TaskId, worker: WorkerId) -> Result<TaskGuard, EngineError>;

    /// Status transition: Running → Completed.
    pub fn complete(&self, id: TaskId, result: TaskResult);

    /// Status transition: Any → Cancelled.
    /// Cascades: all Running children of `id` are Cancelled.
    pub async fn cancel(&self, id: TaskId, reason: u32);

    /// Add a dependency edge between two tasks.
    /// If `kind` is Blocking and `from` is not Completed → `to` remains/enters Pending.
    /// If `kind` is Blocking and `from` IS Completed → edge is immediately satisfied.
    pub async fn depends_on(
        &self, from: TaskId, to: TaskId, kind: EdgeKind
    ) -> Result<EdgeId, EngineError>;

    /// Remove a dependency. If it was the last unsatisfied blocking dep
    /// on `to`, then `to` becomes Ready.
    pub async fn remove_dep(&self, edge: EdgeId) -> Result<(), EngineError>;

    /// Subscribe to status changes. Yields (TaskId, TaskStatus).
    /// Buffer size 4096 per subscriber; slow subscribers miss events.
    pub fn watch(&self) -> broadcast::Receiver<(TaskId, TaskStatus)>;

    /// Query ready tasks in priority order.
    pub fn peek_ready(&self, limit: usize) -> Vec<(TaskId, Priority)>;

    /// Count tasks by status.
    pub fn stats(&self) -> EngineStats;

    /// Graceful shutdown: stop accepting new tasks, wait for Running to complete,
    /// flush journal.
    pub async fn shutdown(&self) -> Result<(), EngineError>;
}

/// RAII guard for in-progress tasks. Dropping without explicit
/// `complete()` transitions the task to Failed (implicit failure).
pub struct TaskGuard {
    engine: Arc<TaskEngine>,
    task_id: TaskId,
    worker: WorkerId,
    done: bool,
}
```

---

## 6. State Machine — Task Lifecycle

```
                   ┌────────────────────┐
                   │     CREATE         │
                   └────────┬───────────┘
                            │
                   ┌────────▼───────────┐
              ┌────│      PENDING       │◄───────────────────────┐
              │    │  (has blocking     │                        │
              │    │   deps remaining)  │                        │
              │    └────────┬───────────┘                        │
              │             │ edge_in_degree → 0                 │
              │    ┌────────▼───────────┐                        │
              │    │      READY         │────────────────────┐   │
              │    │  (all deps met)    │                    │   │
              │    └────────┬───────────┘     cancel()       │   │
              │             │ start()                          │   │
              │    ┌────────▼───────────┐                    │   │
              │    │     RUNNING        │                    │   │
              │    │  (worker assigned)  │                    │   │
              │    └───┬────┬───────────┘                    │   │
              │        │    │                                  │   │
              │   ok   │    │  fail / cancel()                │   │
              │   ┌────▼──┐ │                                 │   │
              │   │COMPLETED│                                 │   │
              │   └────┬────┘                                  │   │
              │        │                                       │   │
              │        │ "unblocks dependents"                │   │
              │        │                                       │   │
              │    ┌───▼──────────────┐ ←─────────────────────┘   │
              │    │  edge_in_degree   │   (re-enters pending    │
              │    │  of dependent ↓   │    if new blocking dep  │
              │    └──────────────────┘    added post-completion) │
              │                                                    │
              │    ┌───────────────┐                               │
              └────►   CANCELLED   │◄──────────────────────────────┘
                   │  (terminal)   │
                   └───────────────┘
```

### 6.1 Cancellation Semantics

Cancellation propagates *downward through Parent edges only*. A cancelled task's blocking dependents remain pending — their dependencies are not satisfied by cancellation. Use-case: cancelling a stale branch of work without affecting siblings.

If the agent wants to cascade fully, it calls `cancel()` on each child. A convenience `cancel_tree(id)` API sugar exists.

---

## 7. Conflict and Edge Cases

### 7.1 Adding a dependency to a completed task

If `A.depends_on(B, Blocking)` is called after B is already Completed:
- The edge is created (always). But `A.edge_in_degree` is NOT incremented (the dep is already satisfied).
- If A was previously Pending and now has 0 blocking deps, A transitions to Ready.

### 7.2 Circular dependency detection

When `depends_on(from, to)` is called, the engine walks `to`'s transitive blocking predecessors. If `from` is reachable → `EngineError::CycleDetected`. This is O(V) in the worst case but the graph is typically shallow (< 5 levels). The walk uses the same DashMap lookups as the scheduler, so no separate index.

### 7.3 Concurrent start()

Multiple workers may race to `start()` the same Ready task. `start()` uses a compare-and-swap on the status field. First writer wins; second gets `EngineError::AlreadyRunning`.

### 7.4 Worker crash

The journal records task events with `seq` numbers. If a worker crashes mid-execution, the engine detects stale Running tasks (no heartbeat within timeout) and re-queues them to Ready. The `TaskGuard` pattern complements this: if the guard is dropped before completion, the task transitions to Failed — the guard is a local backstop; the heartbeat is the distributed one.

---

## 8. Snapshot Format (on-disk)

```
┌──────────────────────────────────────────────┐
│  Magic: "TSK1" (4 bytes)                     │
│  Snapshot Sequence Number (8 bytes, u64 LE)  │
│  Task Count (4 bytes, u32 LE)                │
│  Edge Count (4 bytes, u32 LE)                │
│                                              │
│  Task Count × [                              │
│    task_id (16 bytes, u128 LE)               │
│    task_type_len (2 bytes, u16 LE)           │
│    task_type (variable, UTF-8)               │
│    payload_len (4 bytes, u32 LE)             │
│    payload (variable)                        │
│    status_tag (1 byte)                       │
│    status_payload_len (2 bytes, u16 LE)      │
│    status_payload (variable)                 │
│    priority (1 byte, i8)                     │
│    created_at (8 bytes, u64 LE)             │
│    updated_at (8 bytes, u64 LE)             │
│    seq (8 bytes, u64 LE)                    │
│    parent_present (1 byte)                   │
│    parent_id (0 or 16 bytes)                 │
│    metadata_count (2 bytes, u16 LE)          │
│    metadata_count × [                        │
│      key_len (2 bytes)                       │
│      key (variable)                          │
│      val_len (2 bytes)                       │
│      val (variable)                          │
│    ]                                         │
│  ]                                           │
│                                              │
│  Edge Count × [                              │
│    from_id (16 bytes)                        │
│    to_id (16 bytes)                          │
│    kind_tag (1 byte)                         │
│    seq (8 bytes)                             │
│  ]                                           │
│                                              │
│  CRC32 (4 bytes)                             │
└──────────────────────────────────────────────┘
```

This format is trivially appendable (append segment for tasks, then segment for edges) and trivially readable (sequential scan). No seeking. No compression needed — the records are small (most fields < 32 bytes).

---

## 9. Journal Record Format

```rust
#[derive(Serialize, Deserialize)]
pub enum JournalRecord {
    TaskCreated { seq: u64, task: Task },
    TaskStatusChanged { seq: u64, id: TaskId, from: TaskStatus, to: TaskStatus },
    EdgeCreated { seq: u64, edge: Dependency },
    EdgeRemoved { seq: u64, edge_id: EdgeId },
    SnapshotTaken { seq: u64 },
}
```

Each record is: `[record_len: u32 LE][crc32: u32 LE][payload: MessagePack]`. The writer appends; the compactor reads all segments, filters to latest state per TaskId/EdgeId, writes new compacted segment, then truncates.

---

## 10. Error Codes (Wire Level)

Not HTTP codes. These are structured:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineError {
    NotFound(TaskId),
    AlreadyExists(TaskId),
    InvalidTransition { id: TaskId, from: TaskStatus, attempted: &'static str },
CycleDetected { path: Vec<TaskId> },
    CycleDetected { path: Vec<TaskId> },
    AlreadyRunning { id: TaskId, worker: WorkerId },
    Cancelled(TaskId),
    EngineShuttingDown,
    CorruptJournal(u64), // seq number of corrupt entry
    Internal(String),
}
```

---

## 11. Cargo.toml (Target)

```toml
[package]
name = "tasker"
version = "0.1.0"
edition = "2024"

[dependencies]
tokio = { version = "1.40", features = ["full"] }
dashmap = "6.0"
rmp-serde = "1.3"
rmpv = "1.0"
bytes = "1.7"
crc32fast = "1.3"
serde = { version = "1.0", features = ["derive"] }
serde_bytes = "0.11"
thiserror = "2.0"
tracing = "0.1"
rand = "0.9"

[dev-dependencies]
tempfile = "3.13"
criterion = "0.5"  # For benchmarks
proptest = "1.5"  # For state machine fuzzing
```

---

## 12. Implementation Phases (if approved)

```
Phase 1: Core in-memory Engine
  - DashMap store, state machine, journal writer
  - TaskGuard RAII, dependency ops
  - Unit tests including state machine fuzzing

Phase 2: Binary protocol + IPC
  - Frame parsing, stream multiplexing
  - Unix domain socket listener
  - Rust client crate

Phase 3: Persistence + recovery
  - Snapshot writer/reader
  - Journal compaction
  - Crash recovery tests (fork-based chaos)

Phase 4: Benchmarks + tuning
  - Criterion benchmarks: create/start/complete throughput
  - Proptest: state machine invariant checking
  - DashMap shard count tuning
```

---

## 13. Key Invariants (tested via proptest)

1. **No task in both ROOT and the waiting set of any alive worker.**
2. **A task in RUNNING status is claimed by at most one worker.**
3. **edge_in_degree == 0 ⟺ status ∈ {Ready, Running, Completed, Cancelled}.**
4. **edge_in_degree > 0 ⟺ status ∈ {Pending, Cancelled}.**
5. **Completing a task decrements edge_in_degree of all blocking dependents exactly once per edge.**
6. **A cycle is never successfully added to the graph.**
7. **After recovery from snapshot + journal, the task store state is equivalent to a fresh replay of all records.**

---

*This design prioritizes: throughput over latency for individual ops (batch amortization), correctness-by-construction (state machine enforced in Rust types), and agent ergonomics (binary protocol with crate-first API). Human readability is a non-goal.*
