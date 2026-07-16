//! The public service facade: lifecycle, artist operations and search.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use federation_net::{EndpointId, NetworkConfig, NetworkEngine, PeerTicket, SchemaId};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::config::ArtistDhtConfig;
use crate::database::Database;
use crate::error::{ArtistDhtError, Result};
use crate::node::{Node, record_supersedes};
use crate::normalization::{normalize_artist_name, tokenize};
use crate::record::{Artist, ArtistId, DhtKey, PeerId, StoredArtistRecord, now_ms, validate_name};
use crate::routing::{NodeContact, NodeId};

/// Fixed schema of the artist-dht protocol; peers with a different schema are
/// rejected by `federation-net` during the handshake.
pub const SCHEMA_NAME: &str = "artist-dht-poc-v1";

/// Capacity of the application event channel.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Events delivered to the application.
#[derive(Debug)]
pub enum ArtistDhtEvent {
    /// A transport connection to a peer was established.
    PeerConnected {
        /// The connected peer.
        peer_id: EndpointId,
    },
    /// A transport connection to a peer closed.
    PeerDisconnected {
        /// The disconnected peer.
        peer_id: EndpointId,
    },
    /// A previously unknown DHT contact was learned.
    ContactDiscovered {
        /// The new contact.
        contact: NodeContact,
    },
    /// A non-fatal error occurred.
    Error {
        /// Human-readable description.
        message: String,
    },
}

/// Receiving side of the service event channel.
#[derive(Debug)]
pub struct ArtistDhtEventReceiver {
    rx: mpsc::Receiver<ArtistDhtEvent>,
}

impl ArtistDhtEventReceiver {
    /// Receives the next event; `None` after shutdown.
    pub async fn recv(&mut self) -> Option<ArtistDhtEvent> {
        self.rx.recv().await
    }
}

/// Statistics of one publish or republish operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishStats {
    /// Number of artist records published.
    pub records: usize,
    /// Total number of DHT keys published.
    pub keys: usize,
    /// Number of distinct remote nodes that accepted at least one replica.
    pub remote_nodes: usize,
    /// Whether a replica was also stored locally.
    pub local_replica: bool,
}

/// Result of a combined local + network search.
#[derive(Debug)]
pub struct SearchOutcome {
    /// Matches from the local `local_artists` table.
    pub local_results: Vec<Artist>,
    /// Matches found in the DHT (including local replicas), tombstones and
    /// duplicates already filtered out. Artists matching every query token
    /// come first.
    pub network_results: Vec<Artist>,
    /// Number of distinct peers queried during the lookups.
    pub queried_nodes: usize,
    /// Number of distinct nodes discovered during the lookups.
    pub discovered_nodes: usize,
    /// Total wall-clock duration of the search.
    pub duration: Duration,
}

/// A distributed artist directory node.
///
/// Every instance is simultaneously a client, a DHT router and a storage
/// node; there are no special server roles. See the crate documentation for
/// the protocol description.
pub struct ArtistDhtService {
    node: Arc<Node>,
    tasks: Vec<JoinHandle<()>>,
}

impl ArtistDhtService {
    /// Starts the service: opens the database, starts the network engine,
    /// loads persisted contacts and spawns the maintenance tasks.
    pub async fn start(config: ArtistDhtConfig) -> Result<(Self, ArtistDhtEventReceiver)> {
        let mut engine_builder = NetworkConfig::builder()
            .data_dir(&config.data_dir)
            .network_id(config.network_id)
            .schema_id(SchemaId::from_name(SCHEMA_NAME))
            .request_timeout(config.transport_timeout);
        if let Some(rendezvous) = config.rendezvous.clone() {
            engine_builder = engine_builder.rendezvous(rendezvous);
        }
        let engine_config = engine_builder
            .build()
            .map_err(|err| ArtistDhtError::Network(err.to_string()))?;
        let (engine, net_events) = NetworkEngine::start(engine_config).await?;
        let db = Database::open(&config.data_dir.join("state.sqlite3")).await?;

        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let node = Arc::new(Node::new(engine, db, config.clone(), event_tx));
        info!(
            endpoint_id = %node.endpoint_id,
            node_id = %node.node_id,
            "artist-dht node starting"
        );

        // Contacts persisted by earlier runs seed the routing table; if any
        // exist, local records are republished right away.
        let persisted = node.db.load_known_peers().await?;
        if !persisted.is_empty() {
            info!(count = persisted.len(), "loaded persisted DHT contacts");
            node.seed_contacts(persisted);
        }

        let tasks = vec![
            tokio::spawn(node.clone().run_event_loop(net_events)),
            tokio::spawn(republish_timer(node.clone())),
            tokio::spawn(expire_timer(node.clone())),
        ];
        node.maybe_trigger_initial_republish();

        Ok((
            Self { node, tasks },
            ArtistDhtEventReceiver { rx: event_rx },
        ))
    }

    /// Returns the transport identifier of this peer.
    pub fn endpoint_id(&self) -> EndpointId {
        self.node.endpoint_id
    }

    /// Returns the DHT identifier of this peer.
    pub fn node_id(&self) -> NodeId {
        self.node.node_id
    }

    /// Creates a shareable ticket for this peer.
    pub async fn ticket(&self) -> Result<PeerTicket> {
        self.node.ensure_running()?;
        self.node.engine.ticket().await.map_err(Into::into)
    }

    /// Connects to another peer by ticket. Contacts are exchanged
    /// automatically once the connection is up.
    pub async fn connect(&self, ticket: PeerTicket) -> Result<EndpointId> {
        self.node.ensure_running()?;
        let peer = self.node.engine.connect(ticket).await?;
        Ok(peer)
    }

    /// All DHT contacts currently known to this node.
    pub fn known_peers(&self) -> Vec<NodeContact> {
        self.node.known_contacts()
    }

    /// Transport connections that are currently open.
    pub fn connected_peers(&self) -> Vec<EndpointId> {
        self.node.engine.connected_peers()
    }

    /// Returns `true` if a transport connection to `peer` is open.
    pub fn is_connected(&self, peer: EndpointId) -> bool {
        self.node.engine.is_connected(peer)
    }

    /// Adds a new artist to the local database and publishes it to the DHT.
    pub async fn add_artist(&self, name: String) -> Result<(Artist, PublishStats)> {
        self.node.ensure_running()?;
        let name = name.trim().to_string();
        let normalized = validate_name(&name)?;
        let uuid = uuid::Uuid::now_v7();
        let artist = Artist {
            id: ArtistId::derive(&self.node.endpoint_id, &uuid),
            owner: self.node.endpoint_id,
            name,
            normalized_name: normalized,
            revision: 1,
            deleted: false,
            updated_at_ms: now_ms(),
        };
        self.node.db.upsert_local_artist(&artist).await?;
        info!(artist = %artist.id, name = %artist.name, "added local artist");
        let stats = self.node.publish_artist(&artist).await?;
        Ok((artist, stats))
    }

    /// Deletes a locally owned artist: stores a tombstone and publishes it.
    pub async fn delete_artist(&self, artist_id: ArtistId) -> Result<PublishStats> {
        self.node.ensure_running()?;
        let Some(mut artist) = self.node.db.get_local_artist(artist_id).await? else {
            return Err(ArtistDhtError::ArtistNotFound);
        };
        if artist.owner != self.node.endpoint_id {
            return Err(ArtistDhtError::CannotDeleteRemoteArtist);
        }
        if artist.deleted {
            return Err(ArtistDhtError::ArtistNotFound);
        }
        artist.revision += 1;
        artist.deleted = true;
        artist.updated_at_ms = now_ms();
        self.node.db.upsert_local_artist(&artist).await?;
        info!(artist = %artist.id, "deleted local artist; publishing tombstone");
        self.node.publish_artist(&artist).await
    }

    /// Resolves a (possibly shortened) hex artist id against local records.
    ///
    /// Returns [`ArtistDhtError::ArtistNotFound`] unless exactly one active
    /// local artist matches the prefix.
    pub async fn resolve_local_artist_id(&self, prefix: &str) -> Result<ArtistId> {
        let prefix = prefix.trim().to_lowercase();
        if prefix.len() < 4 || !prefix.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(ArtistDhtError::ArtistNotFound);
        }
        let matches = self.node.db.find_local_by_id_prefix(prefix).await?;
        match matches.as_slice() {
            [artist] => Ok(artist.id),
            _ => Err(ArtistDhtError::ArtistNotFound),
        }
    }

    /// Lists all locally owned active artists.
    pub async fn list_local_artists(&self) -> Result<Vec<Artist>> {
        self.node.db.list_local_artists(false).await
    }

    /// Searches only the local database.
    pub async fn search_local(&self, query: &str) -> Result<Vec<Artist>> {
        let normalized = normalize_artist_name(query);
        if normalized.is_empty() {
            return Err(ArtistDhtError::InvalidArtistName);
        }
        self.node.db.search_local(normalized).await
    }

    /// Searches locally and across the DHT.
    ///
    /// The exact key is looked up first; only if it yields nothing the token
    /// keys are tried. No broadcast is involved: every step is an iterative
    /// Kademlia-style lookup.
    pub async fn search_network(&self, query: &str) -> Result<SearchOutcome> {
        self.node.ensure_running()?;
        let started = Instant::now();
        let normalized = normalize_artist_name(query);
        if normalized.is_empty() {
            return Err(ArtistDhtError::InvalidArtistName);
        }
        let tokens = tokenize(&normalized);
        let network_id = self.node.config.network_id;

        let local_results = self.node.db.search_local(normalized.clone()).await?;

        let mut queried_nodes = 0usize;
        let mut discovered_nodes = 0usize;
        // (artist id, owner) -> best record seen so far.
        let mut merged: HashMap<(ArtistId, PeerId), StoredArtistRecord> = HashMap::new();
        fn merge(
            merged: &mut HashMap<(ArtistId, PeerId), StoredArtistRecord>,
            records: Vec<StoredArtistRecord>,
        ) {
            for record in records {
                let key = (record.artist.id, record.artist.owner);
                match merged.get(&key) {
                    Some(existing) if !record_supersedes(&record, existing) => {}
                    _ => {
                        merged.insert(key, record);
                    }
                }
            }
        }

        // Step 1: the exact key — local replicas, then the network.
        let exact_key = DhtKey::exact(&network_id, &normalized);
        merge(
            &mut merged,
            self.node.db.dht_records_by_key(exact_key, now_ms()).await?,
        );
        let outcome = self
            .node
            .lookup(*exact_key.as_bytes(), Some(exact_key))
            .await;
        queried_nodes += outcome.queried;
        discovered_nodes = discovered_nodes.max(outcome.discovered);
        merge(&mut merged, outcome.records);

        // Step 2: token keys, only when the exact key produced no live match.
        let has_live_match = merged.values().any(|record| !record.artist.deleted);
        if !has_live_match {
            for token in &tokens {
                let key = DhtKey::token(&network_id, token);
                merge(
                    &mut merged,
                    self.node.db.dht_records_by_key(key, now_ms()).await?,
                );
                let outcome = self.node.lookup(*key.as_bytes(), Some(key)).await;
                queried_nodes += outcome.queried;
                discovered_nodes = discovered_nodes.max(outcome.discovered);
                merge(&mut merged, outcome.records);
            }
        }

        // Drop tombstones and expired records, then rank: full-token matches
        // first, then alphabetically.
        let now = now_ms();
        let mut network_results: Vec<Artist> = merged
            .into_values()
            .filter(|record| !record.artist.deleted && record.expires_at_ms > now)
            .map(|record| record.artist)
            .collect();
        let matches_all_tokens = |artist: &Artist| {
            let artist_tokens = tokenize(&artist.normalized_name);
            tokens
                .iter()
                .all(|token| artist_tokens.iter().any(|t| t == token))
        };
        network_results.sort_by(|a, b| {
            matches_all_tokens(b)
                .cmp(&matches_all_tokens(a))
                .then_with(|| a.normalized_name.cmp(&b.normalized_name))
        });

        Ok(SearchOutcome {
            local_results,
            network_results,
            queried_nodes,
            discovered_nodes,
            duration: started.elapsed(),
        })
    }

    /// Pings a known peer: verifies liveness and DHT identity, returns the
    /// round-trip time and refreshes the contact.
    pub async fn ping(&self, peer: EndpointId) -> Result<Duration> {
        self.node.ensure_running()?;
        let contact = self
            .node
            .known_contacts()
            .into_iter()
            .find(|contact| contact.peer_id == peer)
            .ok_or_else(|| ArtistDhtError::Network(format!("unknown peer {peer}")))?;
        self.node.ping(&contact).await
    }

    /// Republishes all live local records immediately.
    pub async fn republish(&self) -> Result<PublishStats> {
        self.node.republish_all().await
    }

    /// Shuts the service down gracefully: stops the maintenance tasks, shuts
    /// the network engine down and closes the event channel.
    pub async fn shutdown(self) -> Result<()> {
        info!("artist-dht node shutting down");
        self.node.begin_shutdown();
        for task in &self.tasks {
            task.abort();
        }
        self.node.engine.clone().shutdown().await?;
        for task in self.tasks {
            let _ = task.await;
        }
        info!("artist-dht node shut down");
        Ok(())
    }
}

async fn republish_timer(node: Arc<Node>) {
    let mut interval = tokio::time::interval(node.config.republish_interval);
    // The first tick fires immediately; skip it, the initial republish is
    // triggered by contact discovery instead.
    interval.tick().await;
    loop {
        interval.tick().await;
        if node.is_shutting_down() {
            break;
        }
        if node.known_contacts().is_empty() {
            continue;
        }
        if let Err(err) = node.republish_all().await {
            tracing::warn!(error = %err, "periodic republish failed");
        }
    }
}

async fn expire_timer(node: Arc<Node>) {
    let mut interval = tokio::time::interval(node.config.expire_interval);
    interval.tick().await;
    loop {
        interval.tick().await;
        if node.is_shutting_down() {
            break;
        }
        node.sweep_expired().await;
    }
}
