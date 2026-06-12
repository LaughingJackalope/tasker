use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

use tasker::engine::TaskEngine;
use tasker::journal::{self, JournalRecord, JournalWriter};
use tasker::snapshot::{read_snapshot, write_snapshot};
use tasker::types::*;

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
fn test_journal_write_and_read() {
    let dir = TempDir::new().unwrap();
    let mut writer = JournalWriter::new(dir.path(), 1024 * 1024).unwrap();

    let task = Task {
        id: TaskId(42),
        spec: make_spec("test"),
        status: TaskStatus::Ready,
        created_at: 1000,
        updated_at: 1000,
        seq: 1,
        child_count: 0,
        edge_in_degree: 0,
    };

    writer
        .append(&JournalRecord::TaskCreated {
            seq: 1,
            task: task.clone(),
        })
        .unwrap();

    let edge = Dependency {
        id: EdgeId(100),
        from: TaskId(1),
        to: TaskId(2),
        kind: EdgeKind::Blocking,
        seq: 2,
    };

    writer
        .append(&JournalRecord::EdgeCreated {
            seq: 2,
            edge: edge.clone(),
        })
        .unwrap();

    drop(writer);

    let (records, max_seq) = journal::read_journal(dir.path()).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(max_seq, 2);

    match &records[0] {
        JournalRecord::TaskCreated { task: t, .. } => {
            assert_eq!(t.id, TaskId(42));
            assert!(matches!(t.status, TaskStatus::Ready));
        }
        _ => panic!("expected TaskCreated"),
    }

    match &records[1] {
        JournalRecord::EdgeCreated { edge: e, .. } => {
            assert_eq!(e.id, EdgeId(100));
            assert!(matches!(e.kind, EdgeKind::Blocking));
        }
        _ => panic!("expected EdgeCreated"),
    }
}

#[test]
fn test_snapshot_roundtrip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("snapshot-000001.tsk");

    let tasks = vec![
        Task {
            id: TaskId(1),
            spec: make_spec("root"),
            status: TaskStatus::Completed {
                result: TaskResult::Ok { output_ref: 42 },
            },
            created_at: 1000,
            updated_at: 2000,
            seq: 5,
            child_count: 1,
            edge_in_degree: 0,
        },
        Task {
            id: TaskId(2),
            spec: TaskSpec {
                task_type: "child".into(),
                payload: b"hello".to_vec(),
                priority: Priority(1),
                parent: Some(TaskId(1)),
                blocking_on: vec![],
                metadata: vec![("key".into(), b"val".to_vec())],
            },
            status: TaskStatus::Ready,
            created_at: 1500,
            updated_at: 1500,
            seq: 3,
            child_count: 0,
            edge_in_degree: 0,
        },
    ];

    let edges = vec![Dependency {
        id: EdgeId(10),
        from: TaskId(1),
        to: TaskId(2),
        kind: EdgeKind::Parent,
        seq: 4,
    }];

    write_snapshot(&path, 10, &tasks, &edges).unwrap();

    let data = read_snapshot(&path).unwrap();
    assert_eq!(data.seq, 10);
    assert_eq!(data.tasks.len(), 2);
    assert_eq!(data.edges.len(), 1);

    assert_eq!(data.tasks[0].id, TaskId(1));
    assert!(matches!(
        data.tasks[0].status,
        TaskStatus::Completed {
            result: TaskResult::Ok { output_ref: 42 }
        }
    ));

    assert_eq!(data.tasks[1].id, TaskId(2));
    assert_eq!(data.tasks[1].spec.task_type, "child");
    assert_eq!(data.tasks[1].spec.payload, b"hello");
    assert_eq!(data.tasks[1].spec.priority, Priority(1));
    assert_eq!(data.tasks[1].spec.parent, Some(TaskId(1)));
    assert_eq!(data.tasks[1].spec.metadata.len(), 1);
    assert_eq!(data.tasks[1].spec.metadata[0].0, "key");
    assert_eq!(data.tasks[1].spec.metadata[0].1, b"val");

    assert_eq!(data.edges[0].kind, EdgeKind::Parent);
    assert_eq!(data.edges[0].from, TaskId(1));
    assert_eq!(data.edges[0].to, TaskId(2));
}

#[test]
fn test_snapshot_with_running_and_cancelled() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("snapshot-000001.tsk");

    let tasks = vec![
        Task {
            id: TaskId(1),
            spec: make_spec("running"),
            status: TaskStatus::Running {
                worker_id: WorkerId(42),
            },
            created_at: 100,
            updated_at: 200,
            seq: 1,
            child_count: 0,
            edge_in_degree: 0,
        },
        Task {
            id: TaskId(2),
            spec: make_spec("cancelled"),
            status: TaskStatus::Cancelled { reason: 99 },
            created_at: 100,
            updated_at: 200,
            seq: 2,
            child_count: 0,
            edge_in_degree: 0,
        },
        Task {
            id: TaskId(3),
            spec: make_spec("failed"),
            status: TaskStatus::Completed {
                result: TaskResult::Failed {
                    error_code: 5,
                    output_ref: Some(12345678901234567890),
                },
            },
            created_at: 100,
            updated_at: 200,
            seq: 3,
            child_count: 0,
            edge_in_degree: 0,
        },
    ];

    write_snapshot(&path, 5, &tasks, &[]).unwrap();
    let data = read_snapshot(&path).unwrap();

    assert!(matches!(
        data.tasks[0].status,
        TaskStatus::Running {
            worker_id: WorkerId(42)
        }
    ));
    assert!(matches!(
        data.tasks[1].status,
        TaskStatus::Cancelled { reason: 99 }
    ));
    assert!(matches!(
        data.tasks[2].status,
        TaskStatus::Completed {
            result: TaskResult::Failed {
                error_code: 5,
                output_ref: Some(12345678901234567890)
            }
        }
    ));
}

#[test]
fn test_journal_segment_rotation() {
    let dir = TempDir::new().unwrap();
    // Very small segment size to force rotation.
    let mut writer = JournalWriter::new(dir.path(), 64).unwrap();

    for i in 0..20 {
        let task = Task {
            id: TaskId(i as u128),
            spec: make_spec(&format!("task_{}", i)),
            status: TaskStatus::Ready,
            created_at: i * 100,
            updated_at: i * 100,
            seq: i as u64,
            child_count: 0,
            edge_in_degree: 0,
        };
        writer
            .append(&JournalRecord::TaskCreated {
                seq: i as u64,
                task,
            })
            .unwrap();
    }

    drop(writer);

    // Should have multiple segment files.
    let count = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("journal-")
        })
        .count();
    assert!(count > 1, "expected multiple segments, got {}", count);

    // All records should be readable.
    let (records, _) = journal::read_journal(dir.path()).unwrap();
    assert_eq!(records.len(), 20);
}

#[test]
fn test_journal_purge_segments() {
    let dir = TempDir::new().unwrap();
    let mut writer = JournalWriter::new(dir.path(), 64).unwrap();

    for i in 0..30 {
        let task = Task {
            id: TaskId(i as u128),
            spec: make_spec(&format!("task_{}", i)),
            status: TaskStatus::Ready,
            created_at: i * 100,
            updated_at: i * 100,
            seq: i as u64,
            child_count: 0,
            edge_in_degree: 0,
        };
        writer
            .append(&JournalRecord::TaskCreated {
                seq: i as u64,
                task,
            })
            .unwrap();
    }
    drop(writer);

    // Purge segments older than 5.
    journal::purge_segments(dir.path(), 5).unwrap();

    let remaining: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("journal-")
        })
        .collect();

    for entry in &remaining {
        let name = entry
            .as_ref()
            .unwrap()
            .file_name()
            .to_string_lossy()
            .to_string();
        let id: u32 = name
            .strip_prefix("journal-")
            .unwrap()
            .strip_suffix(".log")
            .unwrap()
            .parse()
            .unwrap();
        assert!(id >= 5, "segment {} should have been purged", id);
    }
}
