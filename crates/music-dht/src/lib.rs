//! # music-dht
//!
//! A distributed music library directory on top of [`federation_net`]:
//! peers publish their local library (artists, releases, tracks — names and
//! small metadata, never files) into a Kademlia-style DHT and search each
//! other's libraries. Every running node is simultaneously a client, a DHT
//! router and a storage node — there are no dedicated bootstrap, index or
//! search servers. This crate is the grown-up sibling of the minimal
//! `artist-dht` example.
//!
//! ## How it works
//!
//! * Every peer derives a stable 256-bit [`NodeId`] from its persistent
//!   `federation-net` endpoint id.
//! * [`LibraryItem`] records are published under BLAKE3-derived [`DhtKey`]s:
//!   one exact key for the whole normalized name plus one key per token of
//!   the name and of every artist name (so a track is found by its artist).
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
//! use music_dht::{ItemKind, ItemSpec, MusicDhtConfig, MusicDhtService};
//! use federation_net::{NetworkId, RendezvousConfig};
//!
//! # async fn run() -> music_dht::Result<()> {
//! let config = MusicDhtConfig::builder()
//!     .data_dir("./peer-a")
//!     .network_id(NetworkId::from_name("my-music-network"))
//!     // Find other peers knowing only the network id.
//!     .rendezvous(RendezvousConfig::default())
//!     .build()?;
//! let (service, mut events) = MusicDhtService::start(config).await?;
//!
//! // Publish (and keep in sync) the local library.
//! service
//!     .sync_library(vec![ItemSpec {
//!         local_key: "track:42".into(),
//!         kind: ItemKind::Track,
//!         name: "Teardrop".into(),
//!         artist_names: vec!["Massive Attack".into()],
//!         year: Some(1998),
//!         release_type: None,
//!         duration_seconds: Some(330.0),
//!     }])
//!     .await?;
//!
//! // Search the whole network.
//! let outcome = service.search_network("massive attack").await?;
//! for item in &outcome.network_results {
//!     println!("{} {} owned by {}", item.kind, item.name, item.owner);
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
    DEFAULT_EXPIRE_INTERVAL, DEFAULT_LOOKUP_TIMEOUT, DEFAULT_REPUBLISH_INTERVAL,
    DEFAULT_REQUEST_TIMEOUT, DEFAULT_TRANSPORT_TIMEOUT, MusicDhtConfig, MusicDhtConfigBuilder,
};
pub use error::{MusicDhtError, Result};
pub use message::{
    DHT_PROTOCOL_VERSION, FindNodeRequest, FindNodeResponse, FindValueRequest, FindValueResponse,
    Hello, MAX_PEER_EXCHANGE_CONTACTS, MAX_RECORDS_PER_RESPONSE, MusicDhtMessage, PeerExchange,
    PingRequest, PongResponse, RequestEnvelope, RequestId, ResponseEnvelope, StoreRecordRequest,
    StoreRecordResponse,
};
pub use normalization::{normalize_name, tokenize};
pub use record::{
    ACTIVE_RECORD_TTL, DhtKey, ItemId, ItemKind, LibraryItem, MAX_ARTISTS_PER_ITEM,
    MAX_ITEM_NAME_BYTES, MAX_TOKENS_PER_ITEM, PeerId, StoredRecord, TOMBSTONE_TTL,
};
pub use request::MAX_PENDING_REQUESTS;
pub use routing::{
    ALPHA, Distance, K, MAX_LOOKUP_REQUESTS, NodeContact, NodeId, RoutingTable, distance,
    key_distance,
};
pub use service::{
    ItemSpec, MusicDhtEvent, MusicDhtEventReceiver, MusicDhtService, PublishStats, SCHEMA_NAME,
    SearchOutcome, SyncStats,
};

// Re-exported types from the transport layer that appear in this API.
pub use federation_net::{EndpointId, NetworkId, PeerTicket, RendezvousConfig};
