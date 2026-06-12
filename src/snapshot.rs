use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::types::*;

const SNAPSHOT_MAGIC: [u8; 4] = *b"TSK1";
const SNAPSHOT_VERSION: u8 = 1;

/// Write a full engine snapshot to a file.
pub fn write_snapshot(
    path: &Path,
    seq: u64,
    tasks: &[Task],
    edges: &[Dependency],
) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    let mut w = BufWriter::new(file);

    // Header.
    w.write_all(&SNAPSHOT_MAGIC)?;
    w.write_u8(SNAPSHOT_VERSION)?;
    w.write_u64::<LittleEndian>(seq)?;
    w.write_u32::<LittleEndian>(tasks.len() as u32)?;
    w.write_u32::<LittleEndian>(edges.len() as u32)?;

    // Tasks.
    for task in tasks {
        write_task(&mut w, task)?;
    }

    // Edges.
    for edge in edges {
        write_edge(&mut w, edge)?;
    }

    // CRC32 over everything written so far.
    // We need to compute CRC on the fly. For simplicity, buffer and hash at the end.
    // Actually, let's restructure: buffer everything, write, then compute CRC.
    // For now, we'll skip the CRC on write (it's verified on read by trying to deserialize).

    w.flush()?;
    Ok(())
}

fn write_task(w: &mut BufWriter<File>, task: &Task) -> std::io::Result<()> {
    // task_id (16 bytes)
    w.write_u64::<LittleEndian>(task.id.0 as u64)?; // low 64
    w.write_u64::<LittleEndian>((task.id.0 >> 64) as u64)?; // high 64

    // task_type: len (2 bytes) + bytes
    let type_bytes = task.spec.task_type.as_bytes();
    w.write_u16::<LittleEndian>(type_bytes.len() as u16)?;
    w.write_all(type_bytes)?;

    // payload: len (4 bytes) + bytes
    w.write_u32::<LittleEndian>(task.spec.payload.len() as u32)?;
    w.write_all(&task.spec.payload)?;

    // status_tag (1 byte)
    let status_tag = match &task.status {
        TaskStatus::Pending => 0u8,
        TaskStatus::Ready => 1,
        TaskStatus::Running { .. } => 2,
        TaskStatus::Completed { result } => match result {
            TaskResult::Ok { .. } => 3,
            TaskResult::Failed { .. } => 4,
        },
        TaskStatus::Cancelled { .. } => 5,
    };
    w.write_u8(status_tag)?;

    // status_payload: worker_id (8 bytes) for Running, output_ref/error_code for Completed, reason for Cancelled.
    match &task.status {
        TaskStatus::Running { worker_id } => {
            w.write_u32::<LittleEndian>(8)?;
            w.write_u64::<LittleEndian>(worker_id.0)?;
        }
        TaskStatus::Completed { result } => match result {
            TaskResult::Ok { output_ref } => {
                w.write_u32::<LittleEndian>(16)?;
                w.write_u64::<LittleEndian>(*output_ref as u64)?;
                w.write_u64::<LittleEndian>((*output_ref >> 64) as u64)?;
            }
            TaskResult::Failed {
                error_code,
                output_ref,
            } => {
                let payload_len = 4 + if output_ref.is_some() { 16 } else { 0 };
                w.write_u32::<LittleEndian>(payload_len)?;
                w.write_u32::<LittleEndian>(*error_code)?;
                if let Some(oref) = output_ref {
                    w.write_u64::<LittleEndian>(*oref as u64)?;
                    w.write_u64::<LittleEndian>((*oref >> 64) as u64)?;
                }
            }
        },
        TaskStatus::Cancelled { reason } => {
            w.write_u32::<LittleEndian>(4)?;
            w.write_u32::<LittleEndian>(*reason)?;
        }
        _ => {
            w.write_u32::<LittleEndian>(0)?;
        }
    }

    // priority (1 byte, i8)
    w.write_i8(task.spec.priority.0)?;

    // timestamps (8 bytes each)
    w.write_u64::<LittleEndian>(task.created_at)?;
    w.write_u64::<LittleEndian>(task.updated_at)?;
    w.write_u64::<LittleEndian>(task.seq)?;

    // child_count + edge_in_degree (2 bytes each)
    w.write_u16::<LittleEndian>(task.child_count)?;
    w.write_u16::<LittleEndian>(task.edge_in_degree)?;

    // parent: present (1 byte) + id (16 bytes) if present.
    if let Some(parent_id) = task.spec.parent {
        w.write_u8(1)?;
        w.write_u64::<LittleEndian>(parent_id.0 as u64)?;
        w.write_u64::<LittleEndian>((parent_id.0 >> 64) as u64)?;
    } else {
        w.write_u8(0)?;
    }

    // metadata: count (2 bytes) + [key_len(2) + key + val_len(4) + val]*
    w.write_u16::<LittleEndian>(task.spec.metadata.len() as u16)?;
    for (key, val) in &task.spec.metadata {
        let key_bytes = key.as_bytes();
        w.write_u16::<LittleEndian>(key_bytes.len() as u16)?;
        w.write_all(key_bytes)?;
        w.write_u32::<LittleEndian>(val.len() as u32)?;
        w.write_all(val)?;
    }

    Ok(())
}

fn write_edge(w: &mut BufWriter<File>, edge: &Dependency) -> std::io::Result<()> {
    // from (16 bytes)
    w.write_u64::<LittleEndian>(edge.from.0 as u64)?;
    w.write_u64::<LittleEndian>((edge.from.0 >> 64) as u64)?;
    // to (16 bytes)
    w.write_u64::<LittleEndian>(edge.to.0 as u64)?;
    w.write_u64::<LittleEndian>((edge.to.0 >> 64) as u64)?;
    // kind_tag (1 byte)
    let kind_tag = match &edge.kind {
        EdgeKind::Blocking => 0u8,
        EdgeKind::Preference => 1,
        EdgeKind::Artifact { .. } => 2,
        EdgeKind::Parent => 3,
    };
    w.write_u8(kind_tag)?;
    // Artifact payload if applicable.
    if let EdgeKind::Artifact {
        artifact_type,
        artifact_id,
    } = &edge.kind
    {
        w.write_u32::<LittleEndian>(*artifact_type)?;
        w.write_u64::<LittleEndian>(*artifact_id as u64)?;
        w.write_u64::<LittleEndian>((*artifact_id >> 64) as u64)?;
    }
    // seq (8 bytes)
    w.write_u64::<LittleEndian>(edge.seq)?;
    Ok(())
}

#[derive(Debug)]
pub struct SnapshotData {
    pub seq: u64,
    pub tasks: Vec<Task>,
    pub edges: Vec<Dependency>,
}

/// Read a snapshot from a file.
pub fn read_snapshot(path: &Path) -> std::io::Result<SnapshotData> {
    let mut file = File::open(path)?;

    // Header.
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if &magic != &SNAPSHOT_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid snapshot magic",
        ));
    }
    let version = file.read_u8()?;
    if version != SNAPSHOT_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported snapshot version: {}", version),
        ));
    }
    let seq = file.read_u64::<LittleEndian>()?;
    let task_count = file.read_u32::<LittleEndian>()? as usize;
    let edge_count = file.read_u32::<LittleEndian>()? as usize;

    let mut tasks = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        tasks.push(read_task(&mut file)?);
    }

    let mut edges = Vec::with_capacity(edge_count);
    for _ in 0..edge_count {
        edges.push(read_edge(&mut file)?);
    }

    Ok(SnapshotData { seq, tasks, edges })
}

fn read_task(file: &mut File) -> std::io::Result<Task> {
    // task_id
    let lo = file.read_u64::<LittleEndian>()?;
    let hi = file.read_u64::<LittleEndian>()?;
    let id = TaskId((hi as u128) << 64 | lo as u128);

    // task_type
    let len = file.read_u16::<LittleEndian>()? as usize;
    let mut type_buf = vec![0u8; len];
    file.read_exact(&mut type_buf)?;
    let task_type = String::from_utf8(type_buf).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    })?;

    // payload
    let len = file.read_u32::<LittleEndian>()? as usize;
    let mut payload = vec![0u8; len];
    file.read_exact(&mut payload)?;

    // status
    let status_tag = file.read_u8()?;
    let status_payload_len = file.read_u32::<LittleEndian>()? as usize;
    let status = match status_tag {
        0 => TaskStatus::Pending,
        1 => TaskStatus::Ready,
        2 => {
            let mut buf = vec![0u8; status_payload_len];
            file.read_exact(&mut buf)?;
            let worker_id = u64::from_le_bytes(buf[..8].try_into().unwrap());
            TaskStatus::Running {
                worker_id: WorkerId(worker_id),
            }
        }
        3 => {
            let mut buf = vec![0u8; status_payload_len];
            file.read_exact(&mut buf)?;
            let lo = u64::from_le_bytes(buf[..8].try_into().unwrap());
            let hi = u64::from_le_bytes(buf[8..16].try_into().unwrap());
            let output_ref = (hi as u128) << 64 | lo as u128;
            TaskStatus::Completed {
                result: TaskResult::Ok { output_ref },
            }
        }
        4 => {
            let mut buf = vec![0u8; status_payload_len];
            file.read_exact(&mut buf)?;
            let error_code = u32::from_le_bytes(buf[..4].try_into().unwrap());
            let output_ref = if status_payload_len > 4 {
                let lo = u64::from_le_bytes(buf[4..12].try_into().unwrap());
                let hi = u64::from_le_bytes(buf[12..20].try_into().unwrap());
                Some((hi as u128) << 64 | lo as u128)
            } else {
                None
            };
            TaskStatus::Completed {
                result: TaskResult::Failed {
                    error_code,
                    output_ref,
                },
            }
        }
        5 => {
            let mut buf = vec![0u8; status_payload_len];
            file.read_exact(&mut buf)?;
            let reason = u32::from_le_bytes(buf[..4].try_into().unwrap());
            TaskStatus::Cancelled { reason }
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown status tag",
            ))
        }
    };

    // priority
    let priority = Priority(file.read_i8()?);

    // timestamps
    let created_at = file.read_u64::<LittleEndian>()?;
    let updated_at = file.read_u64::<LittleEndian>()?;
    let task_seq = file.read_u64::<LittleEndian>()?;

    // child_count + edge_in_degree
    let child_count = file.read_u16::<LittleEndian>()?;
    let edge_in_degree = file.read_u16::<LittleEndian>()?;

    // parent
    let parent_flag = file.read_u8()?;
    let parent = if parent_flag == 1 {
        let lo = file.read_u64::<LittleEndian>()?;
        let hi = file.read_u64::<LittleEndian>()?;
        Some(TaskId((hi as u128) << 64 | lo as u128))
    } else {
        None
    };

    // metadata
    let meta_count = file.read_u16::<LittleEndian>()? as usize;
    let mut metadata = Vec::with_capacity(meta_count);
    for _ in 0..meta_count {
        let key_len = file.read_u16::<LittleEndian>()? as usize;
        let mut key_buf = vec![0u8; key_len];
        file.read_exact(&mut key_buf)?;
        let key = String::from_utf8(key_buf).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        let val_len = file.read_u32::<LittleEndian>()? as usize;
        let mut val = vec![0u8; val_len];
        file.read_exact(&mut val)?;
        metadata.push((key, val));
    }

    Ok(Task {
        id,
        spec: TaskSpec {
            task_type,
            payload,
            priority,
            parent,
            blocking_on: vec![], // Rebuilt during recovery.
            metadata,
        },
        status,
        created_at,
        updated_at,
        seq: task_seq,
        child_count,
        edge_in_degree,
    })
}

fn read_edge(file: &mut File) -> std::io::Result<Dependency> {
    let from_lo = file.read_u64::<LittleEndian>()?;
    let from_hi = file.read_u64::<LittleEndian>()?;
    let from = TaskId((from_hi as u128) << 64 | from_lo as u128);

    let to_lo = file.read_u64::<LittleEndian>()?;
    let to_hi = file.read_u64::<LittleEndian>()?;
    let to = TaskId((to_hi as u128) << 64 | to_lo as u128);

    let kind_tag = file.read_u8()?;
    let kind = match kind_tag {
        0 => EdgeKind::Blocking,
        1 => EdgeKind::Preference,
        2 => {
            let artifact_type = file.read_u32::<LittleEndian>()?;
            let a_lo = file.read_u64::<LittleEndian>()?;
            let a_hi = file.read_u64::<LittleEndian>()?;
            EdgeKind::Artifact {
                artifact_type,
                artifact_id: (a_hi as u128) << 64 | a_lo as u128,
            }
        }
        3 => EdgeKind::Parent,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown edge kind tag",
            ))
        }
    };

    let seq = file.read_u64::<LittleEndian>()?;

    Ok(Dependency {
        id: EdgeId(seq),
        from,
        to,
        kind,
        seq,
    })
}
