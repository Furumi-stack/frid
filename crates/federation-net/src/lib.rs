//! # federation-net
//!
//! A generic peer-to-peer networking engine built on [Iroh](https://iroh.computer).
//!
//! The library establishes direct (NAT-traversing, relay-assisted) QUIC
//! connections between peers and exchanges typed, application-defined
//! messages over them. It deliberately contains no domain logic: the
//! application chooses its own message type and the engine treats it as an
//! opaque, serde-serializable payload.
//!
//! ## Core concepts
//!
//! * [`NetworkId`] — isolates independent P2P networks from each other.
//! * [`SchemaId`] — isolates applications with incompatible message schemas.
//! * [`PeerTicket`] — a shareable string invitation used to reach a peer.
//! * [`RendezvousConfig`] — optional automatic peer discovery: peers of a
//!   network find each other through the BitTorrent Mainline DHT knowing
//!   nothing but the network id.
//! * [`NetworkEngine`] — the engine itself; [`NetworkEventReceiver`] delivers
//!   [`NetworkEvent`]s to the application.
//!
//! ## Example
//!
//! ```no_run
//! use federation_net::{NetworkConfig, NetworkEngine, NetworkId, SchemaId};
//!
//! #[derive(Debug, serde::Serialize, serde::Deserialize)]
//! enum MyMessage {
//!     Hello { from: String },
//! }
//!
//! # async fn run() -> federation_net::Result<()> {
//! let config = NetworkConfig::builder()
//!     .data_dir("./peer-a")
//!     .network_id(NetworkId::from_name("example-network"))
//!     .schema_id(SchemaId::from_name("my-message-v1"))
//!     .build()?;
//!
//! let (engine, mut events) = NetworkEngine::<MyMessage>::start(config).await?;
//! let ticket = engine.ticket().await?;
//! println!("share this ticket: {ticket}");
//!
//! while let Some(event) = events.recv().await {
//!     println!("{event:?}");
//! }
//! engine.shutdown().await?;
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod config;
mod engine;
mod error;
mod event;
mod identity;
mod protocol;
mod rendezvous;
mod ticket;
mod wire;

pub use config::{
    DEFAULT_EVENT_CHANNEL_CAPACITY, DEFAULT_MAX_CONCURRENT_STREAMS_PER_PEER,
    DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_REQUEST_TIMEOUT, NetworkConfig, NetworkConfigBuilder,
};
pub use engine::{
    ByteStream, ByteStreamConnectionStats, ConnectionPathKind, Message, NetworkEngine,
    StreamAcceptor,
};
pub use error::{NetworkError, Result};
pub use event::{ConnectionDirection, NetworkEvent, NetworkEventReceiver};
pub use protocol::{ALPN, NetworkId, PROTOCOL_VERSION, SchemaId};
pub use rendezvous::{DEFAULT_RENDEZVOUS_ENTRY_TTL, DEFAULT_RENDEZVOUS_INTERVAL, RendezvousConfig};
pub use ticket::{PeerTicket, TICKET_VERSION};

// Re-exported Iroh types that appear in the public API.
pub use iroh::endpoint::{RecvStream, SendStream};
pub use iroh::{EndpointAddr, EndpointId, SecretKey};
// Re-exported so applications can use the generic ticket helpers.
pub use iroh_tickets::Ticket;
