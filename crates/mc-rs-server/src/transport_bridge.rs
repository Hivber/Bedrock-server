//! Dragonfly/Gophertunnel transport adapter for the existing MC-RS gameplay loop.
//!
//! Gophertunnel owns Bedrock 1.26.60 RakNet, login, authentication, encryption
//! and compression. This module adapts its individual packet stream to the
//! original MC-RS peer/event API so all gameplay remains in the Rust runtime.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;

use mc_rs_proto::batch::{self, CompressionAlgorithm};
use mc_rs_raknet::protocol::datagram::Reliability;
use mc_rs_raknet::session::SessionEvent;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

pub use mc_rs_transport::{
    decode_session_hello, read_frame, write_frame, BedrockPacket, SessionHello, TransportError,
    BEDROCK_PROTOCOL_1_26_60,
};

pub struct RakNetPeer {
    pub addr: SocketAddr,
    pub event_rx: mpsc::UnboundedReceiver<SessionEvent>,
    pub hello: SessionHello,
}

struct PendingPeer {
    peer: RakNetPeer,
    outbound: mpsc::UnboundedSender<Vec<u8>>,
}

/// Compatibility facade retaining MC-RS's existing send/session API. It does
/// not open a Bedrock socket; only the Go Gophertunnel edge listens publicly.
pub struct RakNetServer {
    pending: VecDeque<RakNetPeer>,
    incoming_rx: mpsc::UnboundedReceiver<PendingPeer>,
    outbound: HashMap<SocketAddr, mpsc::UnboundedSender<Vec<u8>>>,
}

impl RakNetServer {
    pub async fn bind() -> io::Result<Self> {
        let endpoint = std::env::var("MCRS_TRANSPORT_BIND")
            .unwrap_or_else(|_| "127.0.0.1:19133".to_string());
        let listener = TcpListener::bind(&endpoint).await?;
        let local_addr = listener.local_addr()?;
        if !local_addr.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "MC-RS transport IPC must bind to a loopback address",
            ));
        }
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, remote)) => {
                        let tx = incoming_tx.clone();
                        tokio::spawn(async move {
                            if let Err(error) = serve_ipc_stream(stream, tx).await {
                                tracing::debug!("Dragonfly IPC peer {remote} ended: {error}");
                            }
                        });
                    }
                    Err(error) => {
                        tracing::warn!("Dragonfly IPC accept failed: {error}");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
        tracing::info!("MC-RS gameplay IPC listening on {local_addr}");
        Ok(Self {
            pending: VecDeque::new(),
            incoming_rx,
            outbound: HashMap::new(),
        })
    }

    pub async fn recv_and_process(&mut self) -> bool {
        match self.incoming_rx.recv().await {
            Some(incoming) => {
                let addr = incoming.peer.addr;
                if self.outbound.insert(addr, incoming.outbound).is_some() {
                    tracing::warn!("Replacing duplicate Dragonfly session address {addr}");
                }
                self.pending.push_back(incoming.peer);
                true
            }
            None => false,
        }
    }

    pub fn accept(&mut self) -> Option<RakNetPeer> {
        self.pending.pop_front()
    }

    pub async fn tick_sessions(&mut self) {
        self.outbound.retain(|_, sender| !sender.is_closed());
    }

    pub fn session_count(&self) -> usize {
        self.outbound.len()
    }

    /// Convert an MC-RS 0xFE batch to individual VarUInt32-ID packets. The
    /// per-client Gophertunnel connection applies encryption and framing.
    pub fn send_to_session(
        &mut self,
        addr: &SocketAddr,
        payload: Vec<u8>,
        _reliability: Reliability,
        _immediate: bool,
    ) {
        let Some(sender) = self.outbound.get(addr) else {
            return;
        };
        let packets = match unpack_gameplay_batch(&payload) {
            Ok(packets) => packets,
            Err(error) => {
                tracing::warn!("Could not adapt outgoing MC-RS packet for {addr}: {error}");
                return;
            }
        };
        for packet in packets {
            if sender.send(packet).is_err() {
                tracing::debug!("Dragonfly IPC writer for {addr} is closed");
                break;
            }
        }
    }
}

async fn serve_ipc_stream(
    stream: TcpStream,
    incoming: mpsc::UnboundedSender<PendingPeer>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut reader, mut writer) = stream.into_split();
    let hello_frame = read_frame(&mut reader).await?;
    let hello = decode_session_hello(&hello_frame)?;
    let addr: SocketAddr = hello.remote_address.parse()?;

    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let peer = RakNetPeer { addr, event_rx, hello };
    incoming.send(PendingPeer { peer, outbound: outbound_tx })
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "MC-RS main loop stopped"))?;
    let _ = event_tx.send(SessionEvent::Connected);

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let mut writer_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => return Ok::<(), TransportError>(()),
                item = outbound_rx.recv() => match item {
                    Some(packet) => write_frame(&mut writer, &packet).await?,
                    None => return Ok(()),
                }
            }
        }
    });

    loop {
        tokio::select! {
            result = &mut writer_task => {
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::debug!("Dragonfly IPC write failed for {addr}: {error}"),
                    Err(error) => tracing::debug!("Dragonfly IPC writer task failed for {addr}: {error}"),
                }
                break;
            }
            frame = read_frame(&mut reader) => {
                let raw_packet = match frame {
                    Ok(packet) => packet,
                    Err(error) => {
                        tracing::debug!("Dragonfly IPC read ended for {addr}: {error}");
                        break;
                    }
                };
                if let Err(error) = BedrockPacket::decode(&raw_packet) {
                    tracing::warn!("Dropping malformed Bedrock packet from {addr}: {error}");
                    continue;
                }
                let payload = batch::encode_batch(&[raw_packet], CompressionAlgorithm::None, 0);
                if event_tx.send(SessionEvent::Packet(batch::wrap_batch(&payload))).is_err() {
                    break;
                }
            }
        }
    }
    let _ = event_tx.send(SessionEvent::Disconnected);
    let _ = shutdown_tx.send(());
    if !writer_task.is_finished() {
        let _ = writer_task.await;
    }
    Ok(())
}

fn unpack_gameplay_batch(raw: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if raw.first() != Some(&0xFE) {
        BedrockPacket::decode(raw).map_err(|error| error.to_string())?;
        return Ok(vec![raw.to_vec()]);
    }
    if raw.len() < 3 {
        return Err("truncated 0xFE batch".into());
    }
    let algorithm = CompressionAlgorithm::from_u8(raw[1])
        .ok_or_else(|| format!("unknown compression algorithm 0x{:02x}", raw[1]))?;
    let packets = batch::decode_batch(&raw[2..], algorithm).map_err(|error| error.to_string())?;
    for packet in &packets {
        BedrockPacket::decode(packet).map_err(|error| error.to_string())?;
    }
    Ok(packets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outgoing_gameplay_batches_become_individual_bedrock_packets() {
        let a = vec![0x01, 0xAA, 0xBB];
        let b = vec![0x02, 0xCC];
        let payload = batch::encode_batch(&[a.clone(), b.clone()], CompressionAlgorithm::None, 0);
        assert_eq!(unpack_gameplay_batch(&batch::wrap_batch(&payload)).unwrap(), vec![a, b]);
    }

    #[test]
    fn incoming_bedrock_packet_round_trips_through_mc_rs_batch() {
        let packet = vec![0x13, 0x01, 0x02, 0x03];
        BedrockPacket::decode(&packet).unwrap();
        let wrapped = batch::wrap_batch(&batch::encode_batch(
            &[packet.clone()],
            CompressionAlgorithm::None,
            0,
        ));
        assert_eq!(batch::decode_batch(&wrapped[2..], CompressionAlgorithm::None).unwrap(), vec![packet]);
    }

    #[test]
    fn rejects_unknown_outgoing_compression() {
        assert!(unpack_gameplay_batch(&[0xFE, 0x77, 0x00]).is_err());
    }
}
