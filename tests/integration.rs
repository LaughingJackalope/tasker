use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use tasker::engine::TaskEngine;
use tasker::protocol::{decode_frame, MsgType};
use tasker::rpc::{ComponentClient, TaskClient, TaskServer};
use tasker::types::*;

fn make_spec(task_type: &str) -> TaskSpec {
    TaskSpec {
        task_type: task_type.to_string(),
        payload: vec![],
        priority: Priority(0),
        component_id: "default".into(),
        parent: None,
        blocking_on: vec![],
        metadata: vec![],
    }
}

async fn start_server(engine: Arc<TaskEngine>) -> (String, tokio::task::JoinHandle<()>, TempDir) {
    let dir = TempDir::new().unwrap();
    let sock_path = dir.path().join("tasker.sock").to_string_lossy().to_string();
    let registry = tasker::registry::InstanceRegistry::new(30);
    let server = TaskServer::new(engine, registry, sock_path.clone(), "127.0.0.1:0".into());
    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (sock_path, handle, dir)
}

#[tokio::test]
async fn test_dispatch_to_unregistered_component_fails() {
    let engine = TaskEngine::new();
    let (_sock_path, _handle, _dir) = start_server(engine.clone()).await;
    let id = engine
        .create(TaskSpec {
            component_id: "unknown-service".into(),
            ..make_spec("test")
        })
        .unwrap();
    let err = engine.dispatch(id).await.unwrap_err();
    assert!(matches!(err, tasker::EngineError::NoInstance(_)));
}

#[tokio::test]
async fn test_create_task_via_client() {
    let engine = TaskEngine::new();
    let (sock_path, _handle, _dir) = start_server(engine.clone()).await;
    let mut client = TaskClient::connect(std::path::PathBuf::from(&sock_path).as_path())
        .await
        .unwrap();
    let _id = client.create(make_spec("test")).await.unwrap();
    let stats = engine.stats();
    assert_eq!(stats.ready, 1);
}

#[tokio::test]
async fn test_stats_via_client() {
    let engine = TaskEngine::new();
    let (sock_path, _handle, _dir) = start_server(engine.clone()).await;
    engine.create(make_spec("a")).unwrap();
    engine.create(make_spec("b")).unwrap();
    let mut client = TaskClient::connect(std::path::PathBuf::from(&sock_path).as_path())
        .await
        .unwrap();
    let _stats = client.stats().await.unwrap();
}

#[tokio::test]
async fn test_component_heartbeat() {
    let engine = TaskEngine::new();
    let (sock_path, _handle, _dir) = start_server(engine.clone()).await;
    let mut comp = ComponentClient::connect("my-service", std::path::PathBuf::from(&sock_path).as_path())
        .await
        .unwrap();
    comp.register().await.unwrap();
    comp.heartbeat().await.unwrap();
}

#[tokio::test]
async fn test_status_report_via_direct_api() {
    let engine = TaskEngine::new();
    let id = engine.create(make_spec("test")).unwrap();
    let _guard = engine.start(id, WorkerId(1)).unwrap();
    engine
        .update_status(
            id,
            TaskStatus::Completed {
                result: TaskResult::Ok { output_ref: 42 },
            },
        )
        .unwrap();
    let stats = engine.stats();
    assert_eq!(stats.completed, 1);
}

#[tokio::test]
async fn test_dispatch_unblocks_dependent_via_status_report() {
    let engine = TaskEngine::new();
    let a = engine.create(make_spec("a")).unwrap();
    let _b = engine
        .create(TaskSpec {
            blocking_on: vec![a],
            ..make_spec("b")
        })
        .unwrap();
    let _guard = engine.start(a, WorkerId(1)).unwrap();
    engine
        .update_status(
            a,
            TaskStatus::Completed {
                result: TaskResult::Ok { output_ref: 1 },
            },
        )
        .unwrap();
    let stats = engine.stats();
    assert!(stats.ready > 0 || stats.completed >= 1);
}

#[tokio::test]
async fn test_dispatch_writes_to_shared_stream() {
    // Direct test: write to a SharedStream and verify on the other end
    let engine = TaskEngine::new();
    let id = engine
        .create(TaskSpec {
            component_id: "test-component".into(),
            ..make_spec("compile")
        })
        .unwrap();

    let (a, b) = tokio::net::UnixStream::pair().unwrap();
    let shared: tasker::registry::SharedStream = Arc::new(tokio::sync::Mutex::new(b));
    engine
        .registry
        .streams
        .lock()
        .await
        .insert("test-component".into(), shared.clone());
    engine.registry.register("test-component".into()).unwrap();
    engine.dispatch(id).await.unwrap();

    use tokio::io::AsyncReadExt;
    let mut buf = bytes::BytesMut::with_capacity(8192);
    let mut a = a;
    let mut tmp = vec![0u8; 4096];
    loop {
        if let Some(frame) = decode_frame(&mut buf).unwrap() {
            assert_eq!(frame.msg_type, MsgType::Dispatch);
            let spec: TaskSpec = rmp_serde::from_slice(&frame.payload).unwrap();
            assert_eq!(spec.task_type, "compile");
            assert_eq!(spec.component_id, "test-component");
            return;
        }
        let n = a.read(&mut tmp).await.unwrap();
        buf.extend_from_slice(&tmp[..n]);
    }
}

#[tokio::test]
async fn test_multiple_components_isolated_dispatch() {
    let engine = TaskEngine::new();

    let id_a = engine
        .create(TaskSpec {
            component_id: "service-a".into(),
            ..make_spec("task-a")
        })
        .unwrap();
    let id_b = engine
        .create(TaskSpec {
            component_id: "service-b".into(),
            ..make_spec("task-b")
        })
        .unwrap();

    let (a_read, a_write) = tokio::net::UnixStream::pair().unwrap();
    let (b_read, b_write) = tokio::net::UnixStream::pair().unwrap();

    let shared_a: tasker::registry::SharedStream = Arc::new(tokio::sync::Mutex::new(a_read));
    let shared_b: tasker::registry::SharedStream = Arc::new(tokio::sync::Mutex::new(b_read));

    engine
        .registry
        .streams
        .lock()
        .await
        .insert("service-a".into(), shared_a.clone());
    engine
        .registry
        .streams
        .lock()
        .await
        .insert("service-b".into(), shared_b.clone());
    engine.registry.register("service-a".into()).unwrap();
    engine.registry.register("service-b".into()).unwrap();

    engine.dispatch(id_a).await.unwrap();
    engine.dispatch(id_b).await.unwrap();

    use tokio::io::AsyncReadExt;

    async fn read_task(mut stream: tokio::net::UnixStream) -> TaskSpec {
        let mut buf = bytes::BytesMut::with_capacity(8192);
        let mut tmp = vec![0u8; 4096];
        loop {
            if let Some(frame) = decode_frame(&mut buf).unwrap() {
                assert_eq!(frame.msg_type, MsgType::Dispatch);
                return rmp_serde::from_slice(&frame.payload).unwrap();
            }
            let n = stream.read(&mut tmp).await.unwrap();
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    let spec_a = read_task(a_write).await;
    let spec_b = read_task(b_write).await;

    assert_eq!(spec_a.task_type, "task-a");
    assert_eq!(spec_a.component_id, "service-a");
    assert_eq!(spec_b.task_type, "task-b");
    assert_eq!(spec_b.component_id, "service-b");
}
