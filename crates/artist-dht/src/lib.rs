//! # artist-dht
//!
//! A proof-of-concept distributed artist directory on top of
//! [`federation_net`]. Every running node is simultaneously a client, a DHT
//! router and a storage node — there are no dedicated bootstrap, index or
//! search servers.
//!
//! ## How it works
//!
//! * Every peer derives a stable 256-bit [`NodeId`] from its persistent
//!   `federation-net` endpoint id.
//! * Artist records are published under BLAKE3-derived [`DhtKey`]s: one exact
//!   key for the whole normalized name plus one key per name token.
//! * Records are replicated to the `K` nodes whose ids are XOR-closest to
//!   each key, discovered with an iterative Kademlia-style lookup (never a
//!   broadcast).
//! * Peers learn about each other through a Hello/PeerExchange gossip that
//!   runs automatically on every new connection; connections to further
//!   nodes are opened on demand from stored tickets.
//! * Deletions propagate as tombstones that win over active records of the
//!   same or lower revision; replicas expire by TTL and owners republish
//!   periodically.
//!
//! ## Example
//!
//! ```no_run
//! use artist_dht::{ArtistDhtConfig, ArtistDhtService};
//! use federation_net::NetworkId;
//!
//! # async fn run() -> artist_dht::Result<()> {
//! let config = ArtistDhtConfig::builder()
//!     .data_dir("./peer-a")
//!     .network_id(NetworkId::from_name("demo-artists"))
//!     .build()?;
//! let (service, mut events) = ArtistDhtService::start(config).await?;
//! println!("share this ticket: {}", service.ticket().await?);
//!
//! let (artist, stats) = service.add_artist("Massive Attack".into()).await?;
//! println!("published {} under {} keys", artist.name, stats.keys);
//!
//! let outcome = service.search_network("massive").await?;
//! for artist in &outcome.network_results {
//!     println!("found {} owned by {}", artist.name, artist.owner);
//! }
//! # while let Some(event) = events.recv().await { drop(event); }
//! # service.shutdown().await
//! # }
//! ```

#![warn(missing_docs)]
#![forbid(unsafe_code)]

mod config;
mod database;
mod dht;
mod error;
mod message;
mod node;
mod normalization;
mod record;
mod request;
mod routing;
mod service;

pub use config::{
    ArtistDhtConfig, ArtistDhtConfigBuilder, DEFAULT_EXPIRE_INTERVAL, DEFAULT_LOOKUP_TIMEOUT,
    DEFAULT_REPUBLISH_INTERVAL, DEFAULT_REQUEST_TIMEOUT, DEFAULT_TRANSPORT_TIMEOUT,
};
pub use error::{ArtistDhtError, Result};
pub use message::{
    ArtistDhtMessage, DHT_PROTOCOL_VERSION, FindNodeRequest, FindNodeResponse, FindValueRequest,
    FindValueResponse, Hello, MAX_PEER_EXCHANGE_CONTACTS, MAX_RECORDS_PER_RESPONSE, PeerExchange,
    PingRequest, PongResponse, RequestEnvelope, RequestId, ResponseEnvelope, StoreRecordRequest,
    StoreRecordResponse,
};
pub use normalization::{normalize_artist_name, tokenize};
pub use record::{
    ACTIVE_RECORD_TTL, Artist, ArtistId, DhtKey, MAX_ARTIST_NAME_BYTES, MAX_TOKENS_PER_ARTIST,
    PeerId, StoredArtistRecord, TOMBSTONE_TTL,
};
pub use request::MAX_PENDING_REQUESTS;
pub use routing::{
    ALPHA, Distance, K, MAX_LOOKUP_REQUESTS, NodeContact, NodeId, RoutingTable, distance,
    key_distance,
};
pub use service::{
    ArtistDhtEvent, ArtistDhtEventReceiver, ArtistDhtService, PublishStats, SCHEMA_NAME,
    SearchOutcome,
};

// Re-exported types from the transport layer that appear in this API.
pub use federation_net::{EndpointId, NetworkId, PeerTicket, RendezvousConfig};
