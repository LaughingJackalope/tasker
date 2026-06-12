use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};

use bytes::BytesMut;
use rmp_serde::{from_slice, to_vec};
use rmpv::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::engine::TaskEngine;
use crate::error::EngineError;
use crate::protocol::{self, Frame, MsgType, ProtocolError};
use crate::types::*;

/// Request enum — serialized over the wire as MessagePack.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Request {
    Create {
        spec: TaskSpec,
    },
    Start {
        id: TaskId,
        worker: WorkerId,
    },
    Complete {
        id: TaskId,
        result: TaskResult,
    },
    Cancel {
        id: TaskId,
        reason: u32,
    },
    DependsOn {
        from: TaskId,
        to: TaskId,
        kind: EdgeKind,
    },
    RemoveDep {
        edge_id: EdgeId,
    },
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

/// Server that listens on a Unix domain socket and dispatches to the engine.
pub struct TaskServer {
    engine: Arc<TaskEngine>,
    path: String,
}

impl TaskServer {
    pub fn new(engine: Arc<TaskEngine>, path: String) -> Self {
        Self { engine, path }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let _ = std::fs::remove_file(&self.path);
        let listener = UnixListener::bind(&self.path)?;
        tracing::info!("TaskServer listening on {}", self.path);

        loop {
            let (stream, _) = listener.accept().await?;
            let engine = Arc::clone(&self.engine);
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, engine).await {
                    tracing::error!("connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    engine: Arc<TaskEngine>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = BytesMut::with_capacity(8192);

    loop {
        let frame = loop {
            if let Some(frame) = protocol::decode_frame(&mut buf)? {
                break frame;
            }
            let mut tmp = vec![0u8; 4096];
            let n = stream.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
        };

        let response = dispatch(&engine, &frame).await;
        let response_bytes = to_vec(&response)?;
        let response_frame =
            protocol::encode_frame(MsgType::ResponseOk, frame.stream_id, &response_bytes);
        stream.write_all(&response_frame).await?;
        stream.flush().await?;
    }
}

async fn dispatch(engine: &Arc<TaskEngine>, frame: &Frame) -> Response {
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
                value: Value::String(id.0.to_string().into()),
            },
            Err(e) => Response::Err {
                error: WireError::from(e),
            },
        },
        Request::Start { id, worker } => match engine.start(id, worker) {
            Ok(_) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err {
                error: WireError::from(e),
            },
        },
        Request::Complete { id, result } => {
            engine.inner_complete(id, WorkerId(0), result);
            Response::Ok { value: Value::Nil }
        }
        Request::Cancel { id, reason } => match engine.cancel(id, reason) {
            Ok(()) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err {
                error: WireError::from(e),
            },
        },
        Request::DependsOn { from, to, kind } => match engine.depends_on(from, to, kind) {
            Ok(edge_id) => Response::Ok {
                value: Value::Integer(edge_id.0.into()),
            },
            Err(e) => Response::Err {
                error: WireError::from(e),
            },
        },
        Request::RemoveDep { edge_id } => match engine.remove_dep(edge_id) {
            Ok(()) => Response::Ok { value: Value::Nil },
            Err(e) => Response::Err {
                error: WireError::from(e),
            },
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
            Err(e) => Response::Err {
                error: WireError::from(e),
            },
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
            if let Some(frame) = protocol::decode_frame(&mut self.buf)?
                && frame.stream_id == stream_id
            {
                let response: Response = from_slice(&frame.payload)?;
                return Ok(response);
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

    pub async fn stats(&mut self) -> Result<EngineStats, Box<dyn std::error::Error>> {
        match self.send_request(Request::Stats).await? {
            Response::Ok { .. } => Ok(EngineStats::default()),
            Response::Err { error } => Err(Box::new(error)),
        }
    }
}
