use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

use crate::types::*;

/// Journal record format: [crc32: u32 LE][len: u32 LE][payload: MessagePack]
const JOURNAL_MAGIC: [u8; 4] = *b"JLOG";
const JOURNAL_VERSION: u8 = 1;

#[derive(Debug, Serialize, Deserialize)]
pub enum JournalRecord {
    TaskCreated {
        seq: u64,
        task: Task,
    },
    TaskStatusChanged {
        seq: u64,
        id: TaskId,
        from: TaskStatus,
        to: TaskStatus,
    },
    EdgeCreated {
        seq: u64,
        edge: Dependency,
    },
    EdgeRemoved {
        seq: u64,
        edge_id: EdgeId,
    },
    SnapshotTaken {
        seq: u64,
    },
}

/// Write-only journal that appends records to a segmented file.
pub struct JournalWriter {
    dir: PathBuf,
    current: BufWriter<File>,
    current_path: PathBuf,
    current_segment: u32,
    max_segment_bytes: u64,
    current_bytes: u64,
}

impl JournalWriter {
    pub fn new(dir: &Path, max_segment_bytes: u64) -> std::io::Result<Self> {
        fs::create_dir_all(dir)?;
        let current_path = dir.join(format!("journal-{:06}.log", 1));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_path)?;

        // Write segment header.
        let mut writer = BufWriter::new(file);
        writer.write_all(&JOURNAL_MAGIC)?;
        writer.write_u8(JOURNAL_VERSION)?;
        writer.flush()?;

        Ok(Self {
            dir: dir.to_path_buf(),
            current: writer,
            current_path,
            current_segment: 1,
            max_segment_bytes,
            current_bytes: 5, // magic(4) + version(1)
        })
    }

    pub fn append(&mut self, record: &JournalRecord) -> std::io::Result<()> {
        let payload = rmp_serde::to_vec(record)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let len = payload.len() as u32;
        let crc = crc32fast::hash(&payload);

        self.current.write_u32::<LittleEndian>(crc)?;
        self.current.write_u32::<LittleEndian>(len)?;
        self.current.write_all(&payload)?;
        self.current.flush()?;

        let record_bytes = 8 + payload.len() as u64; // crc(4) + len(4) + payload
        self.current_bytes += record_bytes;

        if self.current_bytes >= self.max_segment_bytes {
            self.rotate()?;
        }

        Ok(())
    }

    pub fn rotate(&mut self) -> std::io::Result<()> {
        self.current.flush()?;
        self.current_segment += 1;
        let new_path = self
            .dir
            .join(format!("journal-{:06}.log", self.current_segment));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&JOURNAL_MAGIC)?;
        writer.write_u8(JOURNAL_VERSION)?;
        writer.flush()?;
        self.current = writer;
        self.current_path = new_path;
        self.current_bytes = 5;
        Ok(())
    }

    /// Open an existing journal or create a new one.
    pub fn open_or_create(dir: &Path, max_segment_bytes: u64) -> std::io::Result<Self> {
        if dir.exists() {
            // Find the latest segment and open it for appending.
            let mut segments: Vec<u32> = fs::read_dir(dir)?
                .filter_map(|entry| {
                    let name = entry.ok()?.file_name().to_string_lossy().to_string();
                    let stem = name.strip_prefix("journal-")?.strip_suffix(".log")?;
                    stem.parse::<u32>().ok()
                })
                .collect();
            segments.sort();

            if let Some(&last) = segments.last() {
                let path = dir.join(format!("journal-{:06}.log", last));
                let metadata = fs::metadata(&path)?;
                let file = OpenOptions::new().append(true).open(&path)?;
                let mut writer = BufWriter::new(file);
                writer.flush()?;
                return Ok(Self {
                    dir: dir.to_path_buf(),
                    current: writer,
                    current_path: path,
                    current_segment: last,
                    max_segment_bytes,
                    current_bytes: metadata.len(),
                });
            }
        }

        Self::new(dir, max_segment_bytes)
    }
}

/// Read all records from all journal segments in order.
pub fn read_journal(dir: &Path) -> std::io::Result<(Vec<JournalRecord>, u64)> {
    let mut segments: Vec<u32> = fs::read_dir(dir)?
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().to_string_lossy().to_string();
            let stem = name.strip_prefix("journal-")?.strip_suffix(".log")?;
            stem.parse::<u32>().ok()
        })
        .collect();
    segments.sort();

    let mut records = Vec::new();
    let mut max_seq = 0u64;

    for segment_id in segments {
        let path = dir.join(format!("journal-{:06}.log", segment_id));
        let mut file = File::open(&path)?;

        // Read and verify header.
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if magic != JOURNAL_MAGIC {
            continue; // Skip non-journal files.
        }
        let version = file.read_u8()?;
        if version != JOURNAL_VERSION {
            continue;
        }

        // Read records.
        loop {
            let crc = match file.read_u32::<LittleEndian>() {
                Ok(v) => v,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            let len = file.read_u32::<LittleEndian>()? as usize;
            let mut payload = vec![0u8; len];
            file.read_exact(&mut payload)?;

            // Verify CRC.
            let actual_crc = crc32fast::hash(&payload);
            if actual_crc != crc {
                // Corrupt record — skip rest of this segment.
                break;
            }

            let record: JournalRecord = rmp_serde::from_slice(&payload)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            match &record {
                JournalRecord::TaskCreated { seq, .. } => max_seq = max_seq.max(*seq),
                JournalRecord::TaskStatusChanged { seq, .. } => max_seq = max_seq.max(*seq),
                JournalRecord::EdgeCreated { seq, .. } => max_seq = max_seq.max(*seq),
                JournalRecord::EdgeRemoved { seq, .. } => max_seq = max_seq.max(*seq),
                JournalRecord::SnapshotTaken { seq } => max_seq = max_seq.max(*seq),
            }
            records.push(record);
        }
    }

    Ok((records, max_seq))
}

/// Delete journal segments older than the given segment ID.
pub fn purge_segments(dir: &Path, keep_after: u32) -> std::io::Result<()> {
    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(stem) = name
            .strip_prefix("journal-")
            .and_then(|s| s.strip_suffix(".log"))
            && let Ok(id) = stem.parse::<u32>()
            && id < keep_after
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}
