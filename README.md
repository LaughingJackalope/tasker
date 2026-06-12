# Tasker

A high-throughput, agent-native task-tracking engine written in Rust. Designed as the state-management and memory layer for autonomous coding agents — not a human project tracker.

## Why

Human task trackers (Linear, Jira) optimize for display. They have arbitrary limits (250 tasks), UI-constrained APIs, and JSON-over-HTTP overhead. Autonomous agents generate thousands of micro-tasks per second with complex dependency graphs. Tasker is built for this:

- **In-memory first**: DashMap lock-free concurrent hashmap for sub-microsecond lookups
- **DAG dependency resolution**: Blocking, preference, artifact, and parent edges with cycle detection
- **Binary wire protocol**: 16-byte header + MessagePack payload over Unix domain sockets — no HTTP overhead
- **Append-only journal**: Segmented log with CRC32 integrity, online compaction
- **Periodic snapshots**: Fast recovery — load snapshot + replay journal

## Quick Start

```bash
# Build
cargo build --release

# Run the quickstart example
cargo run --example quickstart

# Run all tests
cargo test

# Run benchmarks
cargo bench
```

## Architecture

```
┌──────────────────────────────────────────────┐
│                  TaskEngine                    │
│  ┌──────────┐ ┌──────────┐ ┌──────────────┐ │
│  │TaskStore │ │  Graph   │ │   Counters   │ │
│  │(DashMap) │ │(DashMap) │ │  (AtomicU64) │ │
│  └──────────┘ └──────────┘ └──────────────┘ │
│  ┌──────────┐ ┌──────────────────────────┐   │
│  │  Guard   │ │  Journal + Snapshot      │   │
│  │  (RAII)  │ │  (append-only + CRC32)   │   │
│  └──────────┘ └──────────────────────────┘   │
└──────────────────────────────────────────────┘
         │
         │  Unix Domain Socket
         │  Binary protocol (MessagePack)
         ▼
┌──────────────────┐
│   TaskServer     │
│   TaskClient     │
└──────────────────┘
```

## Usage

### As a Rust crate

```rust
use tasker::{TaskEngine, TaskResult, TaskSpec, WorkerId, Priority};

let engine = TaskEngine::new();

// Create a task
let id = engine.create(TaskSpec {
    task_type: "compile".into(),
    payload: b"src/main.rs".to_vec(),
    priority: Priority(0),
    parent: None,
    blocking_on: vec![],
    metadata: vec![],
}).unwrap();

// Start work (returns RAII guard)
let mut guard = engine.start(id, WorkerId(1)).unwrap();

// Complete with result
guard.complete(TaskResult::Ok { output_ref: 42 });

// Or drop the guard for implicit failure
// drop(guard); → TaskStatus::Completed { result: TaskResult::Failed { error_code: 0, ... } }
```

### Task lifecycle

```
Pending → Ready → Running → Completed
                  ↘              ↗
                Cancelled    (implicit on guard drop)
```

### Dependencies

```rust
let a = engine.create(make_spec("a")).unwrap();
let b = engine.create(TaskSpec {
    blocking_on: vec![a],  // B waits for A
    ..make_spec("b")
}).unwrap();

// Or add edges dynamically
let edge_id = engine.depends_on(a, b, EdgeKind::Blocking).unwrap();
```

Edge kinds:
- `Blocking` — B cannot start until A completes
- `Preference` — soft ordering hint
- `Artifact` — data dependency (metadata, not enforced)
- `Parent` — decomposition relationship (cancellation cascades here)

### Wire protocol (for remote agents)

```rust
// Server
let server = TaskServer::new(engine, "/tmp/tasker.sock".into());
server.run().await?;

// Client
let mut client = TaskClient::connect(Path::new("/tmp/tasker.sock")).await?;
let id = client.create(spec).await?;
```

Wire frame format:
```
┌──────────────────────────────────────────┐
│  Magic: "TASK" (4 bytes)                 │
│  Version (1 byte)                        │
│  Message Type (1 byte)                   │
│  Stream ID (2 bytes, LE)                 │
│  Payload Length (4 bytes, LE)           │
│  Payload (MessagePack, variable)          │
│  CRC32 (4 bytes, LE)                    │
└──────────────────────────────────────────┘
```

## API Reference

### TaskEngine (crate API)

| Method | Signature | Description |
|--------|-----------|-------------|
| `create` | `fn create(&self, spec: TaskSpec) -> Result<TaskId>` | Create a task. Ready immediately if no deps. |
| `start` | `fn start(&self, id: TaskId, worker: WorkerId) -> Result<TaskGuard>` | CAS Ready→Running. Returns RAII guard. |
| `complete` | via `TaskGuard::complete(result)` | Transition to Completed. |
| `cancel` | `fn cancel(&self, id: TaskId, reason: u32) -> Result<()>` | Cancel task. Cascades to children via Parent edges. |
| `depends_on` | `fn depends_on(&self, from, to, kind) -> Result<EdgeId>` | Add dependency edge. Cycle detection for Blocking. |
| `remove_dep` | `fn remove_dep(&self, edge_id: EdgeId) -> Result<()>` | Remove edge. May unblock dependents. |
| `watch` | `fn watch(&self) -> broadcast::Receiver` | Subscribe to all status changes. |
| `stats` | `fn stats(&self) -> EngineStats` | O(1) counts by status. |
| `shutdown` | `async fn shutdown(&self) -> Result<()>` | Graceful shutdown. |

### TaskGuard (RAII)

```rust
let mut guard = engine.start(id, worker)?;
guard.complete(TaskResult::Ok { output_ref: 1 });
// Or: drop(guard) → implicit Failed { error_code: 0 }
```

## Performance

Measured on Apple M2:

| Operation | Time | Throughput |
|-----------|------|-----------|
| Create 10,000 tasks | 2.0 ms | ~5M/sec |
| Create 1,000 chain | 468 µs | ~2.1M/sec |
| Complete 1,000 chain | 755 µs | ~1.3M/sec |
| 100-wide DAG | 73 µs | ~13.7M/sec |
| Stats query | 950 ps | O(1) |

## Project Structure

```
src/
├── lib.rs          — Crate root
├── types.rs        — Core data structures (TaskId, TaskStatus, EdgeKind, etc.)
├── error.rs        — EngineError enum
├── engine.rs       — State machine + TaskGuard + unit tests
├── protocol.rs     — Wire frame encode/decode
├── rpc.rs          — TaskServer + TaskClient over UDS
├── journal.rs      — Append-only segmented journal
└── snapshot.rs     — Binary snapshot format
examples/
└── quickstart.rs   — End-to-end demo
tests/
└── recovery.rs     — Journal/snapshot roundtrip, segment rotation
benches/
└── throughput.rs   — Criterion benchmarks
```

## Spec

The full engineering specification is in `./docs/ENGINEERING_SPEC.md` — covers the architecture, data structures, state machine, wire protocol, journal format, and snapshot format in detail.
