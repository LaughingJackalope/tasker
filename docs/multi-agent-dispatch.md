# Multi-Agent Dispatch

Tasker started as a single-instance task engine — one process, one state machine, one journal. This document describes what we added to make it a **multi-agent dispatch system**: a central tasker instance that routes tasks to the right component instance, whether that instance is on the same machine or a remote one.

## The Problem

In a real software project, you don't have one monolithic agent. You have one Oh My Pi instance per component — one per Rust crate, one per microservice, one per subsystem. Each instance is responsible for its own piece. When the main agent needs work done on a specific component, it needs to route the task to the **correct** instance, not just any instance.

## The Solution

We added a routing layer on top of the existing task engine. The key insight: every task carries a `component_id` — a string identifying which component it belongs to. Tasker maintains a registry of connected instances and routes tasks based on this field.

```
┌──────────────────────────────────────────────────────────┐
│                    Tasker Instance                         │
│                                                           │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ TaskEngine   │  │ Instance     │  │ Transport      │  │
│  │ (state       │  │ Registry     │  │ Layer          │  │
│  │  machine)    │  │              │  │                │  │
│  │             │  │ "crate-a" ───┼──┤ UDS /tmp/...   │  │
│  │  create()   │  │ "crate-b" ───┼──┤ TCP :7878      │  │
│  │  dispatch() │  │ "svc-c"   ───┼──┤ TCP :7878      │  │
│  │  watch()    │  │              │  │                │  │
│  └─────────────┘  └──────────────┘  └────────────────┘  │
│                                                           │
│  Listens on: UDS /tmp/tasker.sock + TCP 0.0.0.0:7878     │
└──────────────────────────────────────────────────────────┘
         │                    │                    │
         │ UDS                │ TCP                │ TCP
         ▼                    ▼                    ▼
   ┌──────────┐       ┌──────────┐        ┌──────────┐
   │ Agent A  │       │ Agent B  │        │ Agent C  │
   │ (crate-a)│       │ (crate-b)│        │ (svc-c)  │
   │          │       │          │        │          │
   │ register │       │ register │        │ register │
   │ recv_task│       │ recv_task│        │ recv_task│
   │ report   │       │ report   │        │ report   │
   └──────────┘       └──────────┘        └──────────┘
```

## What Changed

### Component ID on Every Task

`TaskSpec` now carries a `component_id: String` field. This is the routing key.

```rust
let id = engine.create(TaskSpec {
    task_type: "compile".into(),
    component_id: "my-crate".into(),  // <-- routes to the "my-crate" instance
    payload: b"src/lib.rs".to_vec(),
    priority: Priority(0),
    parent: None,
    blocking_on: vec![],
    metadata: vec![],
}).unwrap();
```

### Instance Registry

New file: `src/registry.rs`. A concurrent `DashMap<String, Arc<InstanceConnection>>` that maps `component_id` → connection. Each connection holds:

- The transport (UDS or TCP stream)
- A heartbeat timestamp for liveness detection

Instances register themselves on startup and send periodic heartbeats. Stale instances (no heartbeat within 30s) are evicted automatically.

### Dual Transport

`TaskServer` now listens on **both** UDS and TCP simultaneously:

- **UDS** (`/tmp/tasker.sock`) — same-machine communication, lowest latency
- **TCP** (`0.0.0.0:7878`) — cross-machine communication

Both use the same framed binary protocol (16-byte header + MessagePack payload + CRC32). The `handle_connection` function is generic over any `AsyncRead + AsyncWrite`, so the dispatch logic is transport-agnostic.

### Dispatch Method

New on `TaskEngine`:

```rust
pub fn dispatch(&self, id: TaskId) -> Result<(), EngineError> {
    // 1. Verify task is Ready
    // 2. Look up component_id in registry
    // 3. Send task spec to the instance over its transport
}
```

`dispatch()` is explicit — the caller decides when to send a task. Tasker is a state machine, not a scheduler. This keeps the separation clean: tasker tracks state, the orchestrator decides when to act.

### ComponentClient

New in `src/rpc.rs`. This is what each component instance runs:

```rust
let mut client = ComponentClient::connect("my-crate", Path::new("/tmp/tasker.sock")).await?;
client.register().await?;

loop {
    let task = client.recv_task().await?;
    // ... do work ...
    client.report_status(task_id, TaskStatus::Completed { ... }).await?;
}
```

### Extended Wire Protocol

Five new message types:

| Type | Value | Direction | Purpose |
|------|-------|-----------|---------|
| `Register` | `0x10` | Instance → Tasker | Register component instance |
| `Unregister` | `0x11` | Instance → Tasker | Graceful disconnect |
| `Dispatch` | `0x12` | Tasker → Instance | Send task to instance |
| `StatusReport` | `0x13` | Instance → Tasker | Report task status |
| `Heartbeat` | `0x14` | Bidirectional | Liveness check |

### Configuration

`EngineConfig` gained three new fields:

```rust
pub struct EngineConfig {
    pub uds_path: String,              // default: "/tmp/tasker.sock"
    pub tcp_addr: String,              // default: "0.0.0.0:7878"
    pub heartbeat_timeout_secs: u64,   // default: 30
    // ... existing fields unchanged
}
```

## How It Works End-to-End

1. **Component instances start up** and connect to tasker via `ComponentClient::connect()` + `register()`
2. **Main agent creates tasks** with the appropriate `component_id`
3. **Main agent calls `dispatch(task_id)`** — tasker looks up the component instance and sends the task spec over its transport
4. **Component instance receives the task**, starts work, reports status back
5. **Tasker tracks state** — journal records every mutation, snapshots enable recovery

## What's Not Done (Intentionally)

- **No automatic dispatch** — the caller must call `dispatch()`. This keeps tasker as a pure state machine. A future scheduler layer can sit on top.
- **No task push over transport yet** — `dispatch()` verifies the instance exists and returns `Ok(())`, but the actual network send is a TODO. The plumbing is there; the send logic needs to be wired.
- **No authentication** — instances are trusted. If you need auth, add a shared secret to `Register`/`Dispatch` messages.
- **No TLS** — TCP is plaintext. For cross-network use, wrap with `tokio-rustls`.
- **No load balancing** — one instance per `component_id`. If you need multiple workers per component, that's a future extension.

## File Map

```
src/
├── types.rs       — component_id added to TaskSpec
├── error.rs       — NoInstance, TransportError variants
├── registry.rs    — NEW: InstanceRegistry, Transport enum, InstanceConnection
├── protocol.rs    — Register, Unregister, Dispatch, StatusReport, Heartbeat
├── rpc.rs         — Dual UDS+TCP listener, ComponentClient, generic handle_connection
├── engine.rs      — registry field, with_config(), dispatch(), journal wiring
└── snapshot.rs    — component_id serialization roundtrip
```
