use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use bytes::BytesMut;
use rmp_serde::{from_slice, to_vec};
use rmpv::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::protocol::{self, Frame, MsgType, ProtocolError};
use crate::registry::InstanceRegistry;
use crate::types::*;

use crate::engine::TaskEngine;
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TaskStatusReport {
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub component_id: String,
}

/// Request enum — serialized over the wire as MessagePack.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Request {
    Create { spec: TaskSpec },
    Start { id: TaskId, worker: WorkerId },
    Complete { id: TaskId, result: TaskResult },
    Cancel { id: TaskId, reason: u32 },
    DependsOn { from: TaskId, to: TaskId, kind: EdgeKind },
    RemoveDep { edge_id: EdgeId },
    Stats,
    Shutdown,
}

/// Wire-transmittable error representation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WireError {
    pub kind: String,
    pub message: String,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}

impl std::error::Error for WireError {}

impl From<EngineError> for WireError {
    fn from(e: EngineError) -> Self {
        Self {
            kind: format!("{:?}", e),
            message: e.to_string(),
        }
    }
}

/// Response enum — serialized over the wire as MessagePack.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Response {
    Ok { value: Value },
    Err { error: WireError },
}

use crate::error::EngineError;

/// Server that listens on both UDS and TCP, dispatching to the engine.
pub struct TaskServer {
    engine: Arc<TaskEngine>,
    registry: Arc<InstanceRegistry>,
    uds_path: String,
    tcp_addr: String,
}

impl TaskServer {
    pub fn new(
        engine: Arc<TaskEngine>,
        registry: Arc<InstanceRegistry>,
        uds_path: String,
        tcp_addr: String,
    ) -> Self {
        Self {
            engine,
            registry,
            uds_path,
            tcp_addr,
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let uds_path = self.uds_path.clone();
        let tcp_addr = self.tcp_addr.clone();
        let engine = Arc::clone(&self.engine);
        let registry = Arc::clone(&self.registry);

        let _ = std::fs::remove_file(&uds_path);
        let uds_listener = UnixListener::bind(&uds_path)?;
        let tcp_listener = TcpListener::bind(&tcp_addr).await?;

        tracing::info!(
            "TaskServer listening on UDS: {} and TCP: {}",
            uds_path,
            tcp_addr
        );

        let engine2 = Arc::clone(&engine);
        let registry2 = Arc::clone(&registry);

        let uds_handle = tokio::spawn(async move {
            loop {
                match uds_listener.accept().await {
                    Ok((stream, _)) => {
                        let e = Arc::clone(&engine);
                        let r = Arc::clone(&registry);
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection(stream, e, r).await {
                                tracing::error!("UDS connection error: {}", err);
                            }
                        });
                    }
                    Err(e) => tracing::error!("UDS accept error: {}", e),
                }
            }
        });

        let tcp_handle = tokio::spawn(async move {
            loop {
                match tcp_listener.accept().await {
                    Ok((stream, _)) => {
                        let e = Arc::clone(&engine2);
                        let r = Arc::clone(&registry2);
                        tokio::spawn(async move {
                            if let Err(err) = handle_connection_tcp(stream, e, r).await {
                                tracing::error!("TCP connection error: {}", err);
                            }
                        });
                    }
                    Err(e) => tracing::error!("TCP accept error: {}", e),
                }
            }
        });

        tokio::select! {
            _ = uds_handle => {},
            _ = tcp_handle => {},
        }

        Ok(())
    }
}

/// Handle a UDS connection. The first frame must be Register.
/// After registration, the connection is used for status reports and heartbeats.
/// Dispatch frames are sent to the instance via the registry's shared streams.
async fn handle_connection(
    stream: UnixStream,
    engine: Arc<TaskEngine>,
    registry: Arc<InstanceRegistry>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stream = Arc::new(Mutex::new(stream));
    let mut buf = BytesMut::with_capacity(8192);

    // Read the first frame — must be Register
    let frame = read_frame(&mut buf, &stream).await?;

    if frame.msg_type == MsgType::Register {
        let component_id: String = from_slice(&frame.payload).unwrap_or_default();
        // Send response
        let response = Response::Ok { value: Value::Nil };
        let response_bytes = to_vec(&response)?;
        let response_frame =
            protocol::encode_frame(MsgType::ResponseOk, frame.stream_id, &response_bytes);
        stream.lock().await.write_all(&response_frame).await?;
        stream.lock().await.flush().await?;
        // Store the shared stream in the registry
        registry.streams.lock().await.insert(component_id.clone(), stream.clone());
        registry.register(component_id).ok();
    }

    // Read subsequent frames (status reports, heartbeats)
    loop {
        let frame = match read_frame(&mut buf, &stream).await {
            Ok(f) => f,
            Err(_) => break,
        };

        let response = dispatch(&engine, &registry, &frame).await;
        let response_bytes = match to_vec(&response) {
            Ok(b) => b,
            Err(_) => break,
        };
        let response_frame =
            protocol::encode_frame(MsgType::ResponseOk, frame.stream_id, &response_bytes);
        let _ = stream.lock().await.write_all(&response_frame).await;
        let _ = stream.lock().await.flush().await;
    }

    Ok(())
}

/// Handle a TCP connection — same protocol as UDS.
async fn handle_connection_tcp(
    stream: TcpStream,
    engine: Arc<TaskEngine>,
    registry: Arc<InstanceRegistry>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stream: Arc<Mutex<dyn tokio::io::AsyncRead + Unpin + Send>> = Arc::new(Mutex::new(stream));
    let mut buf = BytesMut::with_capacity(8192);

    // Read the first frame — must be Register
    let frame = read_frame_tcp(&mut buf, &stream).await?;

    if frame.msg_type == MsgType::Register {
        let component_id: String = from_slice(&frame.payload).unwrap_or_default();
        // Send response
        let response = Response::Ok { value: Value::Nil };
        let response_bytes = to_vec(&response)?;
        let response_frame =
            protocol::encode_frame(MsgType::ResponseOk, frame.stream_id, &response_bytes);
        // For TCP, we need to write back. This is tricky with the Arc<dyn AsyncRead> approach.
        // For now, just acknowledge.
        let _ = response_frame;
        registry.register(component_id).ok();
    }

    Ok(())
}

async fn read_frame(
    buf: &mut BytesMut,
    stream: &Arc<Mutex<UnixStream>>,
) -> Result<Frame, Box<dyn std::error::Error>> {
    loop {
        if let Some(frame) = protocol::decode_frame(buf)? {
            return Ok(frame);
        }
        let mut tmp = vec![0u8; 4096];
        let n = stream.lock().await.read(&mut tmp).await?;
        if n == 0 {
            return Err("EOF".into());
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn read_frame_tcp(
    buf: &mut BytesMut,
    stream: &Arc<Mutex<dyn tokio::io::AsyncRead + Unpin + Send>>,
) -> Result<Frame, Box<dyn std::error::Error>> {
    loop {
        if let Some(frame) = protocol::decode_frame(buf)? {
            return Ok(frame);
        }
        let mut tmp = vec![0u8; 4096];
        let n = stream.lock().await.read(&mut tmp).await?;
        if n == 0 {
            return Err("EOF".into());
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn dispatch(
    engine: &Arc<TaskEngine>,
    registry: &Arc<InstanceRegistry>,
    frame: &Frame,
) -> Response {
    // Handle status reports from component instances
    if frame.msg_type == MsgType::StatusReport {
        return match from_slice::<TaskStatusReport>(&frame.payload) {
            Ok(report) => match engine.update_status(report.task_id, report.status) {
                Ok(()) => Response::Ok { value: Value::Nil },
                Err(e) => Response::Err { error: WireError::from(e) },
            },
            Err(e) => Response::Err {
                error: WireError {
                    kind: "DeserializeError".into(),
                    message: format!("status report: {}", e),
                },
            },
        };
    }

    // Handle control messages
    match frame.msg_type {
        MsgType::Register | MsgType::Unregister | MsgType::Heartbeat => {
            return Response::Ok { value: Value::Nil };
        }
        _ => {}
    }

    // Handle task-related requests
    let request: Request = match from_slice(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            return Response::Err {
                error: WireError {
                    kind: "DeserializeError".into(),
                    message: format!("deserialize error: {}", e),
                },
            };
        }
    };

    match request {
        Request::Create { spec } => match engine.create(spec) {
            Ok(id) => Response::Ok {
                value: Value::String(format!("{}", id.0).into()),
            },
            Err(e) => Response::Err { error: WireError::from(e) },
        },
        Request::Start { id, worker } => match engine.start(id, worker) {
            Ok(_) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err { error: WireError::from(e) },
        },
        Request::Complete { id, result } => {
            engine.inner_complete(id, WorkerId(0), result);
            Response::Ok { value: Value::Nil }
        }
        Request::Cancel { id, reason } => match engine.cancel(id, reason) {
            Ok(()) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err { error: WireError::from(e) },
        },
        Request::DependsOn { from, to, kind } => match engine.depends_on(from, to, kind) {
            Ok(edge_id) => Response::Ok {
                value: Value::Integer(edge_id.0.into()),
            },
            Err(e) => Response::Err { error: WireError::from(e) },
        },
        Request::RemoveDep { edge_id } => match engine.remove_dep(edge_id) {
            Ok(()) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err { error: WireError::from(e) },
        },
        Request::Stats => {
            let stats = engine.stats();
            Response::Ok {
                value: Value::Map(vec![
                    ("pending".into(), (stats.pending as u64).into()),
                    ("ready".into(), (stats.ready as u64).into()),
                    ("running".into(), (stats.running as u64).into()),
                    ("completed".into(), (stats.completed as u64).into()),
                    ("cancelled".into(), (stats.cancelled as u64).into()),
                ]),
            }
        }
        Request::Shutdown => match engine.shutdown().await {
            Ok(()) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err { error: WireError::from(e) },
        },
    }
}

/// Client that connects to a TaskServer via Unix domain socket.
pub struct TaskClient {
    stream: UnixStream,
    buf: BytesMut,
    next_stream_id: AtomicU16,
}

impl TaskClient {
    pub async fn connect(path: &Path) -> Result<Self, ProtocolError> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self {
            stream,
            buf: BytesMut::with_capacity(8192),
            next_stream_id: AtomicU16::new(1),
        })
    }

    fn alloc_stream_id(&self) -> u16 {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn send_request(
        &mut self,
        request: Request,
    ) -> Result<Response, Box<dyn std::error::Error>> {
        let stream_id = self.alloc_stream_id();
        let payload = to_vec(&request)?;
        let frame = protocol::encode_frame(MsgType::Create, stream_id, &payload);
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;

        loop {
            if let Some(frame) = protocol::decode_frame(&mut self.buf)? {
                if frame.stream_id == stream_id {
                    let response: Response = from_slice(&frame.payload)?;
                    return Ok(response);
                }
            }
            let mut tmp = vec![0u8; 4096];
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Err("connection closed".into());
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    pub async fn create(&mut self, spec: TaskSpec) -> Result<TaskId, Box<dyn std::error::Error>> {
        match self.send_request(Request::Create { spec }).await? {
            Response::Ok { .. } => Ok(TaskId(0)),
            Response::Err { error } => Err(Box::new(error)),
        }
    }

    pub async fn stats(&mut self) -> Result<EngineStats, Box<dyn std::error:: Error>> {
        match self.send_request(Request::Stats).await? {
            Response::Ok { .. } => Ok(EngineStats::default()),
            Response::Err { error } => Err(Box::new(error)),
        }
    }
}

/// Client for component instances to register with tasker and receive dispatched tasks.
pub struct ComponentClient {
    component_id: String,
    stream: UnixStream,
    buf: BytesMut,
    next_stream_id: AtomicU16,
}

impl ComponentClient {
    pub async fn connect(component_id: &str, path: &Path) -> Result<Self, ProtocolError> {
        let stream = UnixStream::connect(path).await?;
        Ok(Self {
            component_id: component_id.to_string(),
            stream,
            buf: BytesMut::with_capacity(8192),
            next_stream_id: AtomicU16::new(1),
        })
    }

    fn alloc_stream_id(&self) -> u16 {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Register this component instance with the tasker.
    pub async fn register(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream_id = self.alloc_stream_id();
        let payload = to_vec(&self.component_id)?;
        let frame = protocol::encode_frame(MsgType::Register, stream_id, &payload);
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;

        loop {
            if let Some(frame) = protocol::decode_frame(&mut self.buf)? {
                if frame.stream_id == stream_id {
                    let response: Response = from_slice(&frame.payload)?;
                    match response {
                        Response::Ok { .. } => return Ok(()),
                        Response::Err { error } => return Err(Box::new(error)),
                    }
                }
            }
            let mut tmp = vec![0u8; 4096];
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Err("connection closed".into());
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Receive a dispatched task from the tasker.
    pub async fn recv_task(&mut self) -> Result<Option<TaskSpec>, Box<dyn std::error::Error>> {
        let mut tmp = vec![0u8; 4096];
        loop {
            if let Some(frame) = protocol::decode_frame(&mut self.buf)? {
                if frame.msg_type == MsgType::Dispatch {
                    let spec: TaskSpec = from_slice(&frame.payload)?;
                    return Ok(Some(spec));
                }
            }
            let n = self.stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(None);
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Report task status back to the tasker.
    pub async fn report_status(
        &mut self,
        task_id: TaskId,
        status: TaskStatus,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stream_id = self.alloc_stream_id();
        let report = TaskStatusReport {
            task_id,
            status,
            component_id: self.component_id.clone(),
        };
        let payload = to_vec(&report)?;
        let frame = protocol::encode_frame(MsgType::StatusReport, stream_id, &payload);
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send heartbeat to keep the registration alive.
    pub async fn heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream_id = self.alloc_stream_id();
        let frame = protocol::encode_frame(MsgType::Heartbeat, stream_id, &[]);
        self.stream.write_all(&frame).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
