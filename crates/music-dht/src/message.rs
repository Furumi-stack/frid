//! The domain protocol carried over `federation-net`.

use std::fmt;

use federation_net::EndpointId;
use serde::{Deserialize, Serialize};

use crate::record::{DhtKey, StoredRecord};
use crate::routing::{NodeContact, NodeId};

/// Version of the music-dht protocol.
pub const DHT_PROTOCOL_VERSION: u16 = 2;
/// Maximum number of contacts in a single [`PeerExchange`].
pub const MAX_PEER_EXCHANGE_CONTACTS: usize = 32;
/// Maximum number of records in a single [`FindValueResponse`].
pub const MAX_RECORDS_PER_RESPONSE: usize = 256;

/// Correlates a response with its request.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId([u8; 16]);

impl RequestId {
    /// Generates a random request id.
    pub fn random() -> Self {
        Self(rand::random())
    }

    /// Returns the raw bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RequestId(")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        write!(f, ")")
    }
}

/// Wraps a request payload with its correlation id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestEnvelope<T> {
    /// Correlation id; echoed back in the response.
    pub request_id: RequestId,
    /// The request itself.
    pub payload: T,
}

/// Wraps a response payload with the correlation id of its request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseEnvelope<T> {
    /// Correlation id of the request being answered.
    pub request_id: RequestId,
    /// The response itself.
    pub payload: T,
}

/// Introduction sent right after a connection is established.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    /// DHT identifier of the sender.
    pub node_id: NodeId,
    /// Transport identifier of the sender (informational; the authenticated
    /// id always comes from the connection itself).
    pub peer_id: EndpointId,
    /// Ticket other peers can use to reach the sender.
    pub ticket: String,
    /// Protocol version of the sender.
    pub protocol_version: u16,
}

/// A batch of known contacts, shared after [`Hello`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerExchange {
    /// Up to [`MAX_PEER_EXCHANGE_CONTACTS`] contacts.
    pub peers: Vec<NodeContact>,
}

/// Liveness probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingRequest {}

/// Reply to [`PingRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongResponse {
    /// DHT identifier of the responder.
    pub node_id: NodeId,
}

/// Asks for the closest known nodes to `target`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindNodeRequest {
    /// Point of the key space to search around.
    pub target: NodeId,
}

/// Reply to [`FindNodeRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindNodeResponse {
    /// Up to `K` known nodes closest to the target.
    pub nodes: Vec<NodeContact>,
}

/// Asks for records stored under `key`, or the closest nodes to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindValueRequest {
    /// The DHT key to look up.
    pub key: DhtKey,
}

/// Reply to [`FindValueRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FindValueResponse {
    /// The responder stores records under the key.
    Records {
        /// Up to [`MAX_RECORDS_PER_RESPONSE`] non-expired records.
        records: Vec<StoredRecord>,
    },
    /// The responder has nothing stored; here are closer nodes instead.
    CloserNodes {
        /// Up to `K` known nodes closest to the key.
        nodes: Vec<NodeContact>,
    },
}

/// Asks the receiver to store a replica of a record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRecordRequest {
    /// The key the record is published under.
    pub key: DhtKey,
    /// The record to store.
    pub record: StoredRecord,
}

/// Reply to [`StoreRecordRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRecordResponse {
    /// `true` if the record was accepted and stored (or refreshed).
    pub stored: bool,
}

/// Every message exchanged between music-dht peers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum MusicDhtMessage {
    /// Introduction after connect.
    Hello(Hello),
    /// Contact gossip after `Hello`.
    PeerExchange(PeerExchange),

    /// Liveness probe.
    Ping(RequestEnvelope<PingRequest>),
    /// Reply to `Ping`.
    Pong(ResponseEnvelope<PongResponse>),

    /// Node lookup request.
    FindNode(RequestEnvelope<FindNodeRequest>),
    /// Reply to `FindNode`.
    FindNodeResult(ResponseEnvelope<FindNodeResponse>),

    /// Value lookup request.
    FindValue(RequestEnvelope<FindValueRequest>),
    /// Reply to `FindValue`.
    FindValueResult(ResponseEnvelope<FindValueResponse>),

    /// Replication request.
    StoreRecord(RequestEnvelope<StoreRecordRequest>),
    /// Reply to `StoreRecord`.
    StoreRecordResult(ResponseEnvelope<StoreRecordResponse>),
}
