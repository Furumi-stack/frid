//! Protocol constants, network/schema identifiers and internal wire structures.

use std::fmt;

use serde::{Deserialize, Serialize};

/// ALPN identifier for the federation-net protocol.
///
/// [`NetworkId`] and [`SchemaId`] are deliberately not part of the ALPN;
/// they are verified during the application-level handshake instead.
pub const ALPN: &[u8] = b"/federation-net/1";

/// Version of the application-level wire protocol.
pub const PROTOCOL_VERSION: u16 = 1;

/// Maximum size of an encoded handshake frame.
///
/// Handshake frames are tiny; this limit only guards against malicious peers.
pub(crate) const MAX_HANDSHAKE_FRAME_SIZE: usize = 16 * 1024;

fn hash_with_domain(domain: &str, name: &str) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain.as_bytes());
    hasher.update(name.as_bytes());
    *hasher.finalize().as_bytes()
}

fn fmt_id(bytes: &[u8; 32], f: &mut fmt::Formatter<'_>) -> fmt::Result {
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

/// Identifier of a distinct P2P network.
///
/// Peers with different network ids refuse to establish an application-level
/// session even though they share the same transport protocol.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NetworkId([u8; 32]);

impl NetworkId {
    /// Creates a network id from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derives a network id from a human-readable name.
    ///
    /// The derivation is deterministic: the same name always yields the same
    /// id. Internally this computes `BLAKE3("federation-net:network:" + name)`.
    pub fn from_name(name: &str) -> Self {
        Self(hash_with_domain("federation-net:network:", name))
    }

    /// Returns the raw bytes of this id.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for NetworkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_id(&self.0, f)
    }
}

impl fmt::Debug for NetworkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NetworkId(")?;
        fmt_id(&self.0, f)?;
        write!(f, ")")
    }
}

/// Identifier of the domain message schema used on top of the network.
///
/// Peers on the same network but with different schema ids reject each other,
/// because they would not be able to decode each other's messages. A backwards
/// incompatible change to the domain message type requires a new schema id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SchemaId([u8; 32]);

impl SchemaId {
    /// Creates a schema id from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Derives a schema id from a human-readable name.
    ///
    /// The derivation is deterministic: the same name always yields the same
    /// id. Internally this computes `BLAKE3("federation-net:schema:" + name)`.
    pub fn from_name(name: &str) -> Self {
        Self(hash_with_domain("federation-net:schema:", name))
    }

    /// Returns the raw bytes of this id.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_id(&self.0, f)
    }
}

impl fmt::Debug for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SchemaId(")?;
        fmt_id(&self.0, f)?;
        write!(f, ")")
    }
}

/// Handshake sent by the connecting side on the first bidirectional stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Handshake {
    pub protocol_version: u16,
    pub network_id: NetworkId,
    pub schema_id: SchemaId,
}

/// Reply to a [`Handshake`], sent by the accepting side.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandshakeAck {
    pub accepted: bool,
    pub error: Option<HandshakeErrorCode>,
}

/// Reasons for rejecting a handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum HandshakeErrorCode {
    UnsupportedProtocolVersion,
    NetworkMismatch,
    SchemaMismatch,
    InvalidHandshake,
}

/// A single domain message request, sent on its own bidirectional stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MessageRequest {
    pub request_id: [u8; 16],
    pub payload: Vec<u8>,
}

/// Reasons for rejecting a [`MessageRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum MessageRejectReason {
    /// The payload could not be decoded into the domain message type.
    MalformedPayload,
    /// The receiving peer is shutting down.
    ShuttingDown,
    /// The receiving peer is overloaded and dropped the message.
    Overloaded,
}

impl fmt::Display for MessageRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedPayload => write!(f, "malformed payload"),
            Self::ShuttingDown => write!(f, "peer is shutting down"),
            Self::Overloaded => write!(f, "peer is overloaded"),
        }
    }
}

/// Reply to a [`MessageRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum MessageResponse {
    Accepted {
        request_id: [u8; 16],
    },
    Rejected {
        request_id: [u8; 16],
        reason: MessageRejectReason,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_id_from_name_is_deterministic() {
        let a = NetworkId::from_name("example-network");
        let b = NetworkId::from_name("example-network");
        assert_eq!(a, b);
    }

    #[test]
    fn different_network_names_produce_different_ids() {
        let a = NetworkId::from_name("network-a");
        let b = NetworkId::from_name("network-b");
        assert_ne!(a, b);
    }

    #[test]
    fn schema_id_from_name_is_deterministic() {
        let a = SchemaId::from_name("demo-message-v1");
        let b = SchemaId::from_name("demo-message-v1");
        assert_eq!(a, b);
        assert_ne!(a, SchemaId::from_name("demo-message-v2"));
    }

    #[test]
    fn network_and_schema_domains_are_separated() {
        // The same name hashed under different domain prefixes must differ.
        let network = NetworkId::from_name("same-name");
        let schema = SchemaId::from_name("same-name");
        assert_ne!(network.as_bytes(), schema.as_bytes());
    }
}
