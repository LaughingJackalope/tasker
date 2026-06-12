use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::hash;
use std::io;
use thiserror::Error;

pub const MAGIC: [u8; 4] = *b"TASK";
pub const VERSION: u8 = 1;

pub const HEADER_SIZE: usize = 16;

/// Message types for the wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MsgType {
    Create = 0x01,
    Start = 0x02,
    Complete = 0x03,
    Cancel = 0x04,
    DependsOn = 0x05,
    RemoveDep = 0x06,
    Watch = 0x07,
    Stats = 0x08,
    Shutdown = 0x0F,
    ResponseOk = 0x80,
    ResponseErr = 0x81,
    Event = 0x82,
}

impl TryFrom<u8> for MsgType {
    type Error = ProtocolError;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        use MsgType::*;
        Ok(match v {
            0x01 => Create,
            0x02 => Start,
            0x03 => Complete,
            0x04 => Cancel,
            0x05 => DependsOn,
            0x06 => RemoveDep,
            0x07 => Watch,
            0x08 => Stats,
            0x0F => Shutdown,
            0x80 => ResponseOk,
            0x81 => ResponseErr,
            0x82 => Event,
            _ => return Err(ProtocolError::UnknownMsgType(v)),
        })
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid magic bytes")]
    InvalidMagic,
    #[error("unsupported version {0}")]
    UnsupportedVersion(u8),
    #[error("checksum mismatch")]
    ChecksumMismatch,
    #[error("incomplete frame")]
    IncompleteFrame,
    #[error("unknown message type {0}")]
    UnknownMsgType(u8),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

/// A parsed frame from the wire.
#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: MsgType,
    pub stream_id: u16,
    pub payload: Bytes,
}

/// Encode a frame into bytes ready for writing to a socket.
pub fn encode_frame(msg_type: MsgType, stream_id: u16, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len() + 4);

    // Magic (4 bytes)
    buf.extend_from_slice(&MAGIC);
    // Version (1 byte)
    buf.put_u8(VERSION);
    // Message type (1 byte)
    buf.put_u8(msg_type as u8);
    // Stream ID (2 bytes, LE)
    buf.put_u16_le(stream_id);
    // Payload length (4 bytes, LE)
    let len = payload.len() as u32;
    buf.put_u32_le(len);
    // Payload
    buf.extend_from_slice(payload);
    // CRC32 over everything so far (header + payload)
    let crc = hash(&buf);
    buf.put_u32_le(crc);

    buf.freeze()
}

/// Try to decode one frame from the front of `buf`.
/// Returns `Ok(Some(frame))` if a complete frame is available,
/// `Ok(None)` if more data is needed,
/// or `Err` if the data is corrupt.
pub fn decode_frame(buf: &mut BytesMut) -> Result<Option<Frame>, ProtocolError> {
    if buf.len() < HEADER_SIZE {
        return Ok(None);
    }

    // Peek at the payload length without consuming.
    let payload_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize;

    let total = HEADER_SIZE + payload_len + 4; // +4 for CRC
    if buf.len() < total {
        return Ok(None);
    }

    // Split off the complete frame.
    let mut frame = buf.split_to(total);

    // Verify magic.
    if &frame[..4] != &MAGIC {
        return Err(ProtocolError::InvalidMagic);
    }

    // Verify version.
    let version = frame[4];
    if version != VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }

    let msg_type = MsgType::try_from(frame[5])?;
    let stream_id = u16::from_le_bytes([frame[6], frame[7]]);

    // Verify CRC (last 4 bytes of frame).
    let expected_crc =
        u32::from_le_bytes([frame[total - 4], frame[total - 3], frame[total - 2], frame[total - 1]]);
    let actual_crc = hash(&frame[..total - 4]);
    if expected_crc != actual_crc {
        return Err(ProtocolError::ChecksumMismatch);
    }

    // Extract payload.
    let payload = frame.split_to(payload_len).freeze();
    // Drop remaining (CRC bytes).
    drop(frame);

    Ok(Some(Frame {
        msg_type,
        stream_id,
        payload,
    }))
}
