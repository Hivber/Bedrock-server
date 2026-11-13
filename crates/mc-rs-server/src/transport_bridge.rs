//! Rust-side transport contract. The actual event-loop handoff is integrated
//! in the server loop separately; this module deliberately contains no
//! RakNet listener and cannot open a second client-facing port.

pub use mc_rs_transport::{
    decode_session_hello, read_frame, write_frame, BedrockPacket, SessionHello, TransportError,
    BEDROCK_PROTOCOL_1_26_60,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_2225_sessions_before_gameplay() {
        let hello = SessionHello {
            kind: "open".into(),
            protocol: 2193,
            username: "Player".into(),
            uuid: "00000000-0000-0000-0000-000000000001".into(),
            xuid: String::new(),
            remote_address: "127.0.0.1:19132".into(),
        };
        assert!(hello.validate().is_err());
    }
}
