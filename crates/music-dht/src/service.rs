//! The public service facade: lifecycle, item operations and search.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use federation_net::{
    ByteStream, EndpointAddr, EndpointId, NetworkConfig, NetworkEngine, NetworkHealthSnapshot,
    NetworkId, PeerTicket, SchemaId, SecretKey, Signature, StreamAcceptor,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;

use crate::config::MusicDhtConfig;
use crate::database::{Database, MusicDhtStorage};
use crate::error::{MusicDhtError, Result};
use crate::node::{Node, record_supersedes};
use crate::normalization::{normalize_name, tokenize};
use crate::record::{
    DhtKey, ItemId, ItemKind, LibraryItem, MAX_ARTISTS_PER_ITEM, MAX_ITEM_NAME_BYTES,
    MAX_TOKENS_PER_ITEM, PeerId, StoredRecord, normalize_content_id, now_ms, validate_name,
};
use crate::routing::{NodeContact, NodeId};

/// Fixed compatibility identifier of the music-dht protocol; peers with a
/// different schema are rejected by `federation-net` during the handshake.
///
/// The historical value is retained because changing it would partition
/// existing deployments.
pub const SCHEMA_NAME: &str = "music-dht-v5";

/// Capacity of the application event channel.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Events delivered to the application.
#[derive(Debug)]
pub enum MusicDhtEvent {
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
pub struct MusicDhtEventReceiver {
    rx: mpsc::Receiver<MusicDhtEvent>,
}

impl MusicDhtEventReceiver {
    /// Receives the next event; `None` after shutdown.
    pub async fn recv(&mut self) -> Option<MusicDhtEvent> {
        self.rx.recv().await
    }
}

/// Statistics of one publish or republish operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishStats {
    /// Number of item records published.
    pub records: usize,
    /// Total number of DHT keys published.
    pub keys: usize,
    /// Number of distinct remote nodes that accepted at least one replica.
    pub remote_nodes: usize,
    /// Whether a replica was also stored locally.
    pub local_replica: bool,
}

/// Description of one library item to publish, as provided by the
/// application.
#[derive(Debug, Clone, PartialEq)]
pub struct ItemSpec {
    /// Application-chosen stable key (e.g. `"track:42"`). The record id is
    /// derived from it, so republishing the same key updates the same record.
    pub local_key: String,
    /// Kind of the item.
    pub kind: ItemKind,
    /// Display name or title.
    pub name: String,
    /// Display names of the item's main artists (empty for artist records).
    pub artist_names: Vec<String>,
    /// Display names of the item's featured artists (track records only).
    pub featured_artist_names: Vec<String>,
    /// Release/track year, when known.
    pub year: Option<i32>,
    /// Release type (album, ep, ...) for releases and track release context.
    pub release_type: Option<String>,
    /// Release title for track records, when known.
    pub release_title: Option<String>,
    /// Track number inside the release, when known.
    pub track_number: Option<i32>,
    /// Disc number inside the release, when known.
    pub disc_number: Option<i32>,
    /// Track duration in seconds for tracks.
    pub duration_seconds: Option<f64>,
    /// Stable audio content id for tracks (`b3:<64 lowercase hex chars>`).
    pub content_id: Option<String>,
}

fn sanitize_artist_names(
    names: Vec<String>,
    seen: &mut HashSet<String>,
    limit: usize,
) -> Vec<String> {
    names
        .into_iter()
        .filter_map(|name| {
            let name = name.trim().to_string();
            if name.is_empty() || name.len() > MAX_ITEM_NAME_BYTES {
                return None;
            }
            let normalized = normalize_name(&name);
            if normalized.is_empty()
                || tokenize(&normalized).len() > MAX_TOKENS_PER_ITEM
                || !seen.insert(normalized)
            {
                return None;
            }
            Some(name)
        })
        .take(limit)
        .collect()
}

fn sanitize_optional_name(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_string();
    if value.is_empty() || value.len() > MAX_ITEM_NAME_BYTES {
        return None;
    }
    let normalized = normalize_name(&value);
    if normalized.is_empty() || tokenize(&normalized).len() > MAX_TOKENS_PER_ITEM {
        return None;
    }
    Some(value)
}

fn sanitize_optional_text(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_string();
    (!value.is_empty() && value.len() <= MAX_ITEM_NAME_BYTES).then_some(value)
}

fn positive_index(value: Option<i32>) -> Option<i32> {
    value.filter(|number| *number > 0)
}

/// Result of one [`MusicDhtService::sync_library`] call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Items published for the first time (or resurrected).
    pub added: usize,
    /// Items whose content changed and was republished.
    pub updated: usize,
    /// Items tombstoned because they left the library.
    pub removed: usize,
    /// Items already up to date.
    pub unchanged: usize,
    /// Items skipped or whose publish failed.
    pub failed: usize,
}

/// `true` if the two records describe the same content (revision, deletion
/// state and timestamps are ignored).
fn same_content(a: &LibraryItem, b: &LibraryItem) -> bool {
    a.kind == b.kind
        && a.name == b.name
        && a.artist_names == b.artist_names
        && a.featured_artist_names == b.featured_artist_names
        && a.year == b.year
        && a.release_type == b.release_type
        && a.release_title == b.release_title
        && a.track_number == b.track_number
        && a.disc_number == b.disc_number
        && a.duration_seconds == b.duration_seconds
        && a.content_id == b.content_id
}

fn merge_records(merged: &mut HashMap<(ItemId, PeerId), StoredRecord>, records: Vec<StoredRecord>) {
    for record in records {
        let key = (record.item.id, record.item.owner);
        match merged.get(&key) {
            Some(existing) if !record_supersedes(&record, existing) => {}
            _ => {
                merged.insert(key, record);
            }
        }
    }
}

/// Result of a combined local + network search.
#[derive(Debug)]
pub struct SearchOutcome {
    /// Matches from the local `local_items` table.
    pub local_results: Vec<LibraryItem>,
    /// Matches found in the DHT (including local replicas), tombstones and
    /// duplicates already filtered out. Artists matching every query token
    /// come first.
    pub network_results: Vec<LibraryItem>,
    /// Number of distinct peers queried during the lookups.
    pub queried_nodes: usize,
    /// Number of distinct nodes discovered during the lookups.
    pub discovered_nodes: usize,
    /// Total wall-clock duration of the search.
    pub duration: Duration,
}

/// A distributed item directory node.
///
/// Every instance is simultaneously a client, a DHT router and a storage
/// node; there are no special server roles. See the crate documentation for
/// the protocol description.
pub struct MusicDhtService {
    node: Arc<Node>,
    tasks: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl MusicDhtService {
    /// Starts the service: opens the database, starts the network engine,
    /// loads persisted contacts and spawns the maintenance tasks.
    pub async fn start(config: MusicDhtConfig) -> Result<(Self, MusicDhtEventReceiver)> {
        let db = Arc::new(Database::open(&config.data_dir.join("state.sqlite3")).await?);
        Self::start_with_storage(config, db).await
    }

    /// Starts the service with an application-provided persistence backend.
    ///
    /// This is useful for servers that already have durable storage and do
    /// not want the DHT state tied to a local SQLite file. The `data_dir`
    /// in `config` is still used by the transport layer for the peer
    /// identity.
    pub async fn start_with_storage(
        config: MusicDhtConfig,
        storage: Arc<dyn MusicDhtStorage>,
    ) -> Result<(Self, MusicDhtEventReceiver)> {
        Self::start_with_storage_inner(config, storage, None).await
    }

    /// Starts the service with application-provided persistence and identity.
    ///
    /// The identity controls the stable transport endpoint id. Supplying it
    /// lets applications store both the DHT state and the peer identity in
    /// their own durable database while keeping [`MusicDhtService::start`]
    /// as the SQLite/file-backed default.
    pub async fn start_with_storage_and_secret_key(
        config: MusicDhtConfig,
        storage: Arc<dyn MusicDhtStorage>,
        secret_key: SecretKey,
    ) -> Result<(Self, MusicDhtEventReceiver)> {
        Self::start_with_storage_inner(config, storage, Some(secret_key)).await
    }

    async fn start_with_storage_inner(
        config: MusicDhtConfig,
        storage: Arc<dyn MusicDhtStorage>,
        secret_key: Option<SecretKey>,
    ) -> Result<(Self, MusicDhtEventReceiver)> {
        let mut engine_builder = NetworkConfig::builder()
            .data_dir(&config.data_dir)
            .network_id(config.network_id)
            .schema_id(SchemaId::from_name(SCHEMA_NAME))
            // A full FindValue response (MAX_RECORDS_PER_RESPONSE records
            // with long names and artist lists) must fit into one frame.
            .max_message_size(1024 * 1024)
            .request_timeout(config.transport_timeout);
        if let Some(rendezvous) = config.rendezvous.clone() {
            engine_builder = engine_builder.rendezvous(rendezvous);
        }
        for alpn in &config.stream_protocols {
            engine_builder = engine_builder.stream_protocol(alpn.clone());
        }
        for alpn in &config.schema_independent_stream_protocols {
            engine_builder = engine_builder.schema_independent_stream_protocol(alpn.clone());
        }
        let engine_config = engine_builder
            .build()
            .map_err(|err| MusicDhtError::Network(err.to_string()))?;
        let (engine, net_events) = match secret_key {
            Some(secret_key) => {
                NetworkEngine::start_with_secret_key(engine_config, secret_key).await?
            }
            None => NetworkEngine::start(engine_config).await?,
        };

        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
        let node = Arc::new(Node::new(engine, storage, config.clone(), event_tx));
        info!(
            endpoint_id = %node.endpoint_id,
            node_id = %node.node_id,
            "music-dht node starting"
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
            Self {
                node,
                tasks: std::sync::Mutex::new(tasks),
            },
            MusicDhtEventReceiver { rx: event_rx },
        ))
    }

    /// Returns the transport identifier of this peer.
    pub fn endpoint_id(&self) -> EndpointId {
        self.node.endpoint_id
    }

    /// Returns the federation network this service participates in.
    pub fn network_id(&self) -> NetworkId {
        self.node.config.network_id
    }

    /// Signs a forwarded application record with this peer's persistent
    /// transport identity without exposing the secret key.
    pub fn sign_identity(&self, message: &[u8]) -> Signature {
        self.node.engine.sign_identity(message)
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

    /// Returns a point-in-time snapshot of transport and rendezvous health.
    pub fn network_health(&self) -> NetworkHealthSnapshot {
        self.node.engine.health()
    }

    /// Returns `true` if a transport connection to `peer` is open.
    pub fn is_connected(&self, peer: EndpointId) -> bool {
        self.node.engine.is_connected(peer)
    }

    /// Takes the acceptor of incoming raw byte streams for the stream
    /// protocol `alpn` declared in the configuration.
    ///
    /// The acceptor for a protocol can be taken exactly once; see
    /// [`federation_net::NetworkEngine::stream_acceptor`].
    pub fn stream_acceptor(&self, alpn: &[u8]) -> Result<StreamAcceptor> {
        self.node.ensure_running()?;
        self.node.engine.stream_acceptor(alpn).map_err(Into::into)
    }

    /// Opens a raw byte stream to `peer` on the auxiliary ALPN `alpn`.
    ///
    /// The peer is dialed using the address from its stored DHT contact
    /// when one is known (which is the case for every peer that appears in
    /// search results), falling back to whatever the transport itself knows
    /// about the peer. See [`federation_net::NetworkEngine::open_stream`].
    pub async fn open_stream(&self, peer: EndpointId, alpn: &[u8]) -> Result<ByteStream> {
        self.node.ensure_running()?;
        let contact_addr = self
            .node
            .known_contacts()
            .into_iter()
            .find(|contact| contact.peer_id == peer)
            .and_then(|contact| contact.ticket.parse::<PeerTicket>().ok())
            .map(|ticket| ticket.endpoint_addr);
        let addr: EndpointAddr = contact_addr.unwrap_or_else(|| peer.into());
        self.node
            .engine
            .open_stream(addr, alpn)
            .await
            .map_err(Into::into)
    }

    /// Opens a raw byte stream using a self-contained peer ticket.
    ///
    /// This is used by schema-independent overlays whose contacts can be
    /// learned independently of the main music-DHT routing table.
    pub async fn open_stream_to(&self, ticket: &PeerTicket, alpn: &[u8]) -> Result<ByteStream> {
        self.node.ensure_running()?;
        if ticket.network_id != self.node.config.network_id {
            return Err(MusicDhtError::Network(
                "peer ticket belongs to another network".to_string(),
            ));
        }
        self.node
            .engine
            .open_stream(ticket.endpoint_addr.clone(), alpn)
            .await
            .map_err(Into::into)
    }

    /// Synchronizes the published library with `specs`: the desired set of
    /// items this peer wants to share.
    ///
    /// Items are matched by their application-chosen `local_key` (the record
    /// id is derived from it), so calling this repeatedly is idempotent:
    /// new/changed items are (re)published with a bumped revision, items
    /// missing from `specs` are tombstoned, unchanged items are left alone.
    pub async fn sync_library(&self, specs: Vec<ItemSpec>) -> Result<SyncStats> {
        self.node.ensure_running()?;
        let owner = self.node.endpoint_id;
        let existing: HashMap<ItemId, LibraryItem> = self
            .node
            .db
            .list_local_items(true)
            .await?
            .into_iter()
            .map(|item| (item.id, item))
            .collect();

        let mut stats = SyncStats::default();
        let mut seen: HashSet<ItemId> = HashSet::new();
        let mut to_publish: Vec<LibraryItem> = Vec::new();

        for spec in specs {
            let name = spec.name.trim().to_string();
            let normalized = match validate_name(&name) {
                Ok(normalized) => normalized,
                Err(err) => {
                    tracing::debug!(key = %spec.local_key, error = %err, "skipping item with invalid name");
                    stats.failed += 1;
                    continue;
                }
            };
            let id = ItemId::derive(&owner, spec.kind, &spec.local_key);
            if !seen.insert(id) {
                // Duplicate local key in the input; first occurrence wins.
                continue;
            }
            let mut seen_artists = HashSet::new();
            let artist_names =
                sanitize_artist_names(spec.artist_names, &mut seen_artists, MAX_ARTISTS_PER_ITEM);
            let featured_artist_names = sanitize_artist_names(
                spec.featured_artist_names,
                &mut seen_artists,
                MAX_ARTISTS_PER_ITEM.saturating_sub(artist_names.len()),
            );
            let content_id = if spec.kind == ItemKind::Track {
                spec.content_id.as_deref().and_then(normalize_content_id)
            } else {
                None
            };
            let mut item = LibraryItem {
                id,
                owner,
                kind: spec.kind,
                name,
                normalized_name: normalized,
                artist_names,
                featured_artist_names,
                year: spec.year,
                release_type: sanitize_optional_text(spec.release_type),
                release_title: sanitize_optional_name(spec.release_title),
                track_number: positive_index(spec.track_number),
                disc_number: positive_index(spec.disc_number),
                duration_seconds: spec.duration_seconds.filter(|duration| *duration > 0.0),
                content_id,
                revision: 1,
                deleted: false,
                updated_at_ms: now_ms(),
            };
            match existing.get(&id) {
                Some(current) if !current.deleted && same_content(current, &item) => {
                    stats.unchanged += 1;
                }
                Some(current) => {
                    item.revision = current.revision + 1;
                    self.node.db.upsert_local_item(&item).await?;
                    if current.deleted {
                        stats.added += 1;
                    } else {
                        stats.updated += 1;
                    }
                    to_publish.push(item);
                }
                None => {
                    self.node.db.upsert_local_item(&item).await?;
                    stats.added += 1;
                    to_publish.push(item);
                }
            }
        }

        // Items that disappeared from the library become tombstones.
        for (id, current) in &existing {
            if seen.contains(id) || current.deleted {
                continue;
            }
            let mut item = current.clone();
            item.revision += 1;
            item.deleted = true;
            item.updated_at_ms = now_ms();
            self.node.db.upsert_local_item(&item).await?;
            stats.removed += 1;
            to_publish.push(item);
        }

        if let Err(err) = self.node.publish_items(&to_publish).await {
            tracing::warn!(
                items = to_publish.len(),
                error = %err,
                "failed to publish changed items"
            );
            stats.failed += to_publish.len();
        }
        if stats.added + stats.updated + stats.removed > 0 {
            info!(
                added = stats.added,
                updated = stats.updated,
                removed = stats.removed,
                unchanged = stats.unchanged,
                "library sync finished"
            );
        }
        Ok(stats)
    }

    /// Lists all locally owned active items.
    pub async fn list_local_items(&self) -> Result<Vec<LibraryItem>> {
        self.node.db.list_local_items(false).await
    }

    /// Counts non-expired DHT replica records stored by this node.
    pub async fn dht_record_count(&self) -> Result<usize> {
        self.node.ensure_running()?;
        self.node.db.dht_record_count(now_ms()).await
    }

    /// Searches only the local database.
    pub async fn search_local(&self, query: &str) -> Result<Vec<LibraryItem>> {
        let normalized = normalize_name(query);
        if normalized.is_empty() {
            return Err(MusicDhtError::InvalidItemName);
        }
        self.node.db.search_local(normalized).await
    }

    /// Searches locally and across the DHT.
    ///
    /// The exact key and every token key are looked up and their results
    /// merged, so a query for an artist also returns the artist's releases
    /// and tracks. No broadcast is involved: every step is an iterative
    /// Kademlia-style lookup.
    pub async fn search_network(&self, query: &str) -> Result<SearchOutcome> {
        self.node.ensure_running()?;
        let started = Instant::now();
        let normalized = normalize_name(query);
        if normalized.is_empty() {
            return Err(MusicDhtError::InvalidItemName);
        }
        let tokens = tokenize(&normalized);
        let network_id = self.node.config.network_id;

        let local_results = self.node.db.search_local(normalized.clone()).await?;

        // The exact key plus one key per unique token — always, not only as
        // a fallback. The exact key of "massive attack" carries the artist
        // record only; the artist's releases and tracks live under the token
        // keys.
        let mut keys = vec![DhtKey::exact(&network_id, &normalized)];
        let mut seen_keys: HashSet<DhtKey> = keys.iter().copied().collect();
        for token in &tokens {
            let key = DhtKey::token(&network_id, token);
            if seen_keys.insert(key) {
                keys.push(key);
            }
        }

        // (item id, owner) -> best record seen so far.
        let mut merged: HashMap<(ItemId, PeerId), StoredRecord> = HashMap::new();

        // Local replicas first, then all network lookups concurrently: one
        // slow key must not serialize the others.
        for key in &keys {
            merge_records(
                &mut merged,
                self.node.db.dht_records_by_key(*key, now_ms()).await?,
            );
        }
        let outcomes = futures::future::join_all(
            keys.iter()
                .map(|key| self.node.lookup(*key.as_bytes(), Some(*key), true)),
        )
        .await;
        let mut queried_nodes = 0usize;
        let mut discovered_nodes = 0usize;
        for outcome in outcomes {
            queried_nodes += outcome.queried;
            discovered_nodes = discovered_nodes.max(outcome.discovered);
            merge_records(&mut merged, outcome.records);
        }

        // Drop tombstones and expired records, then rank: full-token matches
        // first, then alphabetically.
        let now = now_ms();
        let mut network_results: Vec<LibraryItem> = merged
            .into_values()
            .filter(|record| !record.item.deleted && record.expires_at_ms > now)
            .map(|record| record.item)
            .collect();
        let matches_all_tokens = |item: &LibraryItem| {
            let item_tokens = item.search_tokens();
            tokens
                .iter()
                .all(|token| item_tokens.iter().any(|t| t == token))
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

    /// Searches locally and across the DHT for the exact same audio content.
    ///
    /// `content_id` must be the canonical `b3:<64 hex>` identifier generated
    /// from the audio file bytes. This lookup is intended for playback
    /// fallback: if the originally liked peer is offline, the application can
    /// find another peer that published the same track bytes.
    pub async fn search_content_id(&self, content_id: &str) -> Result<SearchOutcome> {
        self.node.ensure_running()?;
        let started = Instant::now();
        let content_id = normalize_content_id(content_id).ok_or(MusicDhtError::InvalidItemName)?;
        let network_id = self.node.config.network_id;

        let local_results: Vec<LibraryItem> = self
            .node
            .db
            .list_local_items(false)
            .await?
            .into_iter()
            .filter(|item| item.content_id.as_deref() == Some(content_id.as_str()))
            .collect();

        let key = DhtKey::content(&network_id, &content_id);
        let mut merged: HashMap<(ItemId, PeerId), StoredRecord> = HashMap::new();
        merge_records(
            &mut merged,
            self.node.db.dht_records_by_key(key, now_ms()).await?,
        );
        // Any single replica resolves a content id, so the first records win.
        let outcome = self.node.lookup(*key.as_bytes(), Some(key), false).await;
        let queried_nodes = outcome.queried;
        let discovered_nodes = outcome.discovered;
        merge_records(&mut merged, outcome.records);

        let now = now_ms();
        let mut network_results: Vec<LibraryItem> = merged
            .into_values()
            .filter(|record| !record.item.deleted && record.expires_at_ms > now)
            .filter(|record| record.item.content_id.as_deref() == Some(content_id.as_str()))
            .map(|record| record.item)
            .collect();
        network_results.sort_by(|a, b| {
            a.normalized_name
                .cmp(&b.normalized_name)
                .then_with(|| a.owner.to_string().cmp(&b.owner.to_string()))
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
            .ok_or_else(|| MusicDhtError::Network(format!("unknown peer {peer}")))?;
        self.node.ping(&contact).await
    }

    /// Republishes all live local records immediately.
    pub async fn republish(&self) -> Result<PublishStats> {
        self.node.republish_all().await
    }

    /// Shuts the service down gracefully: stops the maintenance tasks, shuts
    /// the network engine down and closes the event channel. Safe to call
    /// once from any handle; repeated calls are no-ops.
    pub async fn shutdown(&self) -> Result<()> {
        info!("music-dht node shutting down");
        self.node.begin_shutdown();
        let tasks: Vec<JoinHandle<()>> = {
            let mut guard = self
                .tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *guard)
        };
        for task in &tasks {
            task.abort();
        }
        self.node.engine.clone().shutdown().await?;
        for task in tasks {
            let _ = task.await;
        }
        info!("music-dht node shut down");
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
