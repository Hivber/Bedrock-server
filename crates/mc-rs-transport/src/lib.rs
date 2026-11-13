//! Transport-neutral IPC contract shared by the MC-RS runtime and the
//! Dragonfly/Gophertunnel edge. Frames contain either the initial JSON session
//! metadata or one complete, uncompressed Bedrock packet (packet ID + payload).
//! RakNet, encryption and compression remain exclusively at the Go edge.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_FRAME_SIZE: usize = 32 * 1024 * 1024;
pub const BEDROCK_PROTOCOL_1_26_60: i32 = 2225;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHello {
    #[serde(rename = "type")]
    pub kind: String,
    pub protocol: i32,
    pub username: String,
    pub uuid: String,
    #[serde(default)]
    pub xuid: String,
    pub remote_address: String,
}

impl SessionHello {
    pub fn validate(&self) -> Result<(), TransportError> {
        if self.kind != "open" {
            return Err(TransportError::InvalidSessionKind(self.kind.clone()));
        }
        if self.protocol != BEDROCK_PROTOCOL_1_26_60 {
            return Err(TransportError::UnsupportedProtocol(self.protocol));
        }
        if self.username.trim().is_empty() || self.uuid.trim().is_empty() {
            return Err(TransportError::InvalidIdentity);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BedrockPacket {
    pub packet_id: u32,
    pub payload: Vec<u8>,
}

impl BedrockPacket {
    /// Decode one Gophertunnel ReadBytes() packet (VarUInt32 ID + payload).
    pub fn decode(mut bytes: &[u8]) -> Result<Self, TransportError> {
        let packet_id = read_var_u32(&mut bytes)?;
        Ok(Self {
            packet_id,
            payload: bytes.to_vec(),
        })
    }

    /// Encode a packet for a Gophertunnel Conn.WritePacket(Unknown).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + self.payload.len());
        write_var_u32(self.packet_id, &mut out);
        out.extend_from_slice(&self.payload);
        out
    }
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid IPC frame size: {0}")]
    InvalidFrameSize(usize),
    #[error("invalid VarUInt32 packet header")]
    InvalidPacketHeader,
    #[error("unsupported Bedrock protocol {0}; expected {BEDROCK_PROTOCOL_1_26_60}")]
    UnsupportedProtocol(i32),
    #[error("invalid session kind {0}")]
    InvalidSessionKind(String),
    #[error("session identity is incomplete")]
    InvalidIdentity,
}

/// Read a single length-prefixed IPC frame. The 4-byte length is big-endian,
/// matching transport/dragonfly/main.go.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>, TransportError> {
    let size = reader.read_u32().await? as usize;
    if size == 0 || size > MAX_FRAME_SIZE {
        return Err(TransportError::InvalidFrameSize(size));
    }
    let mut payload = vec![0; size];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Write one length-prefixed IPC frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), TransportError> {
    if payload.is_empty() || payload.len() > MAX_FRAME_SIZE {
        return Err(TransportError::InvalidFrameSize(payload.len()));
    }
    writer.write_u32(payload.len() as u32).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub fn decode_session_hello(frame: &[u8]) -> Result<SessionHello, TransportError> {
    let hello: SessionHello = serde_json::from_slice(frame)?;
    hello.validate()?;
    Ok(hello)
}

fn read_var_u32(input: &mut &[u8]) -> Result<u32, TransportError> {
    let mut value = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *input.first().ok_or(TransportError::InvalidPacketHeader)?;
        *input = &input[1..];
        value |= ((byte & 0x7f) as u32) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(TransportError::InvalidPacketHeader)
}

fn write_var_u32(mut value: u32, out: &mut Vec<u8>) {
    loop {
        if value & !0x7f == 0 {
            out.push(value as u8);
            return;
        }
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn session_hello_requires_protocol_2225() {
        let json = br#"{"type":"open","protocol":2225,"username":"Steve","uuid":"00000000-0000-0000-0000-000000000001","remote_address":"127.0.0.1:19132"}"#;
        assert!(decode_session_hello(json).is_ok());
        let old = br#"{"type":"open","protocol":2193,"username":"Steve","uuid":"00000000-0000-0000-0000-000000000001","remote_address":"127.0.0.1:19132"}"#;
        assert!(matches!(decode_session_hello(old), Err(TransportError::UnsupportedProtocol(2193))));
    }

    #[test]
    fn packet_round_trip_handles_varuint_ids() {
        for id in [0x01, 0x7f, 0x80, 0x161, 0x3ff] {
            let packet = BedrockPacket { packet_id: id, payload: vec![0, 1, 2, 255] };
            assert_eq!(BedrockPacket::decode(&packet.encode()).unwrap(), packet);
        }
    }

    #[tokio::test]
    async fn ipc_frame_round_trip() {
        let (mut a, mut b) = duplex(128);
        let expected = b"bedrock-packet";
        let writer = tokio::spawn(async move { write_frame(&mut a, expected).await.unwrap() });
        let actual = read_frame(&mut b).await.unwrap();
        writer.await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn ipc_rejects_empty_frames() {
        let (mut a, mut b) = duplex(8);
        let writer = tokio::spawn(async move { a.write_u32(0).await.unwrap() });
        assert!(matches!(read_frame(&mut b).await, Err(TransportError::InvalidFrameSize(0))));
        writer.await.unwrap();
    }
}
