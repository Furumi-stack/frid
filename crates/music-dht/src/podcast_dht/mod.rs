//! Public podcast discovery: signed podcast cards in a separate DHT overlay.
//!
//! This overlay implements decision R7 of the podcast plan and the
//! "Public discovery (podcast_dht)" section of `PODCASTS.md`. It follows the
//! pattern of [`crate::similarity_dht`]: a schema-independent Kademlia
//! overlay on its own ALPN ([`PODCAST_DHT_ALPN`]) carried by `music-dht` byte
//! streams, with its own wire version, its own SQLite cache and its own
//! routing contacts. The music DHT schema and `federation-net` are unchanged.
//!
//! * A [`PodcastCard`] is the public projection of one podcast published by
//!   one device: feed URL and channel metadata only, signed by the device's
//!   persistent identity over a domain-separated canonical encoding.
//!   Progress, history, subscription registers and device names never enter
//!   a card.
//! * Cards are stored under the podcast-id key and one key per normalized
//!   title/author token, so peers outside the publisher's trusted-device group
//!   find them by text or by id.
//! * A device publishes only podcasts for which [`effective_share`] holds
//!   ([`PodcastDht::publish`], [`PodcastDht::sync_shares`]). Unpublishing
//!   writes a signed [`PodcastTombstone`] with the next revision. Nothing is
//!   ever sent for a podcast this device never published.
//! * Every record is verified (bounds, network, key placement, lifetime and
//!   signature) when it is received and again whenever it is read from the
//!   cache.
//! * Own cards persisted by an earlier run are not sent again, neither
//!   republished nor served to lookups, until the client reconciles with
//!   [`PodcastDht::sync_shares`] in this process: a share revoked on another
//!   trusted device while this one was offline is never re-sent from the
//!   stale cache.
//!
//! Applications own the serving and maintenance tasks, exactly as for
//! [`crate::similarity_dht::SimilarityDht`]: declare [`PODCAST_DHT_ALPN`] with
//! [`crate::MusicDhtConfigBuilder::schema_independent_stream_protocol`], take
//! its acceptor and spawn [`PodcastDht::serve`] and [`PodcastDht::maintenance`].
//! Advertise the overlay with [`CapabilityManifest::with_podcast_dht`] (it is
//! opt-in) and pass peer manifests to [`PodcastDht::note_peer_capabilities`].
//!
//! Only the URL a podcast id hashes from is bound to the podcast; search hits
//! offer that URL ([`PodcastSearchHit::feed_url`]) and report moved-feed URLs
//! as unverified claims. The publisher key is the device's transport identity
//! and therefore linkable to its network address and music-DHT records.

mod card;
mod privacy;
mod search;
mod store;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use federation_net::{ByteStream, EndpointId, NetworkId, PeerTicket, StreamAcceptor};
use futures::stream::{self, FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::MusicDhtService;
use crate::capabilities::CapabilityManifest;
use crate::error::{MusicDhtError, Result};
use crate::podcast::{PodcastId, ShareRecord, SubscriptionRecord, effective_share};
use crate::record::now_ms;
use crate::routing::{ALPHA, K, MAX_LOOKUP_REQUESTS, NodeContact, NodeId, RoutingTable, distance};

pub use card::{
    MAX_CARD_AUTHOR_CHARS, MAX_CARD_CATEGORIES, MAX_CARD_CATEGORY_CHARS,
    MAX_CARD_DESCRIPTION_CHARS, MAX_CARD_INDEX_TOKENS, MAX_CARD_LANGUAGE_BYTES,
    MAX_CARD_TITLE_CHARS, MAX_INDEX_TOKEN_BYTES, MAX_PODCAST_RECORD_BYTES, MAX_RECORD_DHT_KEYS,
    PODCAST_CARD_SIGNING_DOMAIN, PODCAST_CARD_TTL, PODCAST_DHT_KEY_DOMAIN,
    PODCAST_DHT_MAX_CLOCK_SKEW, PODCAST_DHT_PROTOCOL_VERSION, PODCAST_TOMBSTONE_SIGNING_DOMAIN,
    PODCAST_TOMBSTONE_TTL, PodcastCard, PodcastCardSpec, PodcastDhtKey, PodcastRecordBody,
    PodcastTombstone, SignedPodcastRecord, card_index_tokens, compare_records,
};
pub use privacy::{
    FeedUrlPrivacyWarning, PRIVATE_FEED_HOSTS, SECRET_QUERY_WORDS, feed_url_privacy_warning,
};
pub use search::{
    ClaimedFeedUrl, FederatedSearchHit, MAX_HIT_FEED_URL_CLAIMS, MAX_HIT_PUBLISHERS,
    MAX_PODCAST_QUERY_TOKENS, PodcastSearchHit, SearchHitKind, merge_search_results,
    podcast_query_tokens,
};
pub use store::{
    MAX_PERSISTED_PODCAST_DHT_PEERS, MAX_PODCAST_CACHE_BYTES, MAX_PODCAST_CACHE_RECORDS,
    MAX_PODCAST_RECORDS_PER_KEY, MAX_PODCAST_RECORDS_PER_PUBLISHER,
    MAX_PODCAST_RESPONSE_TOMBSTONES, MAX_PODCAST_TOMBSTONES_PER_KEY,
    PODCAST_DHT_CACHE_SCHEMA_VERSION,
};

use card::publication_error;
use search::{aggregate_hits, rank_hits};
use store::{LocalPublication, PodcastDhtDatabase};

/// ALPN of the podcast-discovery overlay.
pub const PODCAST_DHT_ALPN: &[u8] = b"furumi-fd/podcast-dht/1";
/// Stable identifier advertised in capability manifests.
pub const PODCAST_DHT_ID: &str = "podcast_dht";
/// Maximum records returned for one key.
pub const MAX_PODCAST_DHT_RECORDS: usize = 128;
/// Maximum entries in one store request.
pub const MAX_PODCAST_STORE_RECORDS: usize = 16;
/// Maximum encoded request. A full store batch of worst-case records fits.
pub const MAX_PODCAST_DHT_REQUEST_BYTES: usize = 512 * 1024;
/// Maximum encoded response. A full key of worst-case records fits.
pub const MAX_PODCAST_DHT_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum hits returned by one search.
pub const MAX_PODCAST_SEARCH_RESULTS: usize = 64;
/// Text keys looked up per search: the longest query tokens. Every replica
/// filters by all query tokens, so one key is enough; three add redundancy.
pub const MAX_PODCAST_QUERY_LOOKUPS: usize = 3;
/// Republish cadence of own cards and tombstones: the main DHT's
/// [`crate::DEFAULT_REPUBLISH_INTERVAL`].
pub const PODCAST_REPUBLISH_INTERVAL: Duration = crate::config::DEFAULT_REPUBLISH_INTERVAL;
/// Replica expiry sweep: the main DHT's [`crate::DEFAULT_EXPIRE_INTERVAL`].
pub const PODCAST_EXPIRE_INTERVAL: Duration = crate::config::DEFAULT_EXPIRE_INTERVAL;
/// Periodic bootstrap for discovering upgraded peers.
pub const PODCAST_BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Maximum inbound requests processed concurrently; further streams are
/// dropped.
pub const MAX_CONCURRENT_PODCAST_DHT_REQUESTS: usize = 64;
/// Maximum inbound requests of one peer processed concurrently; further
/// streams of that peer are dropped.
pub const MAX_PODCAST_DHT_REQUESTS_PER_PEER: usize = 8;
/// Maximum distinct `(podcast_id, publisher)` records one lookup keeps;
/// records of further owners are ignored.
pub const MAX_PODCAST_LOOKUP_RECORDS: usize = 512;
/// Most new `(podcast_id, publisher)` owners one response (or the local
/// cache) may add to a lookup: a `1 / K` share of
/// [`MAX_PODCAST_LOOKUP_RECORDS`], so the first responders cannot fill the
/// lookup and crowd out later ones.
pub const MAX_PODCAST_LOOKUP_OWNERS_PER_RESPONSE: usize = MAX_PODCAST_LOOKUP_RECORDS / K;
/// A node stores a placement only when fewer than this many of its known
/// overlay contacts are closer to the key than itself (`2 * K`: the `K`
/// closest with slack for differing routing views).
pub const PODCAST_STORE_RESPONSIBILITY_WINDOW: usize = 2 * K;
/// How long a peer whose capability manifest lacks [`PODCAST_DHT_ID`] is
/// not contacted.
pub const UNSUPPORTED_PEER_BACKOFF: Duration = Duration::from_secs(60 * 60);

const MAX_BOOTSTRAP_PEERS: usize = 64;
const BOOTSTRAP_CONCURRENCY: usize = 8;
const PUBLISH_CONCURRENCY: usize = 8;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const SERVE_TIMEOUT: Duration = Duration::from_secs(10);
/// A peer that failed to answer (network error or timeout) is skipped by
/// lookups, publication and bootstrap for this long.
const FAILED_PEER_BACKOFF: Duration = Duration::from_secs(30);
/// A contact that failed this many times in a row (each after its backoff)
/// is removed from the routing table and the persisted peers.
const MAX_CONSECUTIVE_FAILURES: u32 = 3;
const MAX_TICKET_BYTES: usize = 16 * 1024;
const MAX_ERROR_BYTES: usize = 1024;

/// One request on [`PODCAST_DHT_ALPN`] (postcard, one per stream).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastDhtRequest {
    /// [`PODCAST_DHT_PROTOCOL_VERSION`].
    pub version: u16,
    /// Requesting overlay node; must match the authenticated peer.
    pub requester: NodeContact,
    /// Operation.
    pub operation: PodcastDhtOperation,
}

/// Overlay operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PodcastDhtOperation {
    /// Returns the `K` closest overlay contacts to `target`.
    FindNode {
        /// Target id.
        target: [u8; 32],
    },
    /// Returns records stored under `key` and the closest contacts.
    FindValue {
        /// Key.
        key: PodcastDhtKey,
        /// Normalized tokens every returned card must be indexed under
        /// (tombstones are always returned); at most
        /// [`MAX_PODCAST_QUERY_TOKENS`].
        filter_tokens: Vec<String>,
    },
    /// Stores records; at most [`MAX_PODCAST_STORE_RECORDS`] entries.
    StoreBatch {
        /// Entries.
        records: Vec<PodcastStoreEntry>,
    },
}

/// A record and the keys it is to be stored under on the receiving node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastStoreEntry {
    /// Distinct keys the record belongs under, at most [`MAX_RECORD_DHT_KEYS`].
    pub keys: Vec<PodcastDhtKey>,
    /// Signed record.
    pub record: SignedPodcastRecord,
}

/// Response to a [`PodcastDhtRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PodcastDhtResponse {
    /// [`PODCAST_DHT_PROTOCOL_VERSION`].
    pub version: u16,
    /// Whether the request was accepted.
    pub ok: bool,
    /// Diagnostic when `ok` is false, at most 1 KiB.
    pub error: Option<String>,
    /// Closest overlay contacts, at most `K`.
    pub nodes: Vec<NodeContact>,
    /// Records, at most [`MAX_PODCAST_DHT_RECORDS`].
    pub records: Vec<SignedPodcastRecord>,
    /// `(key, record)` rows stored by a `StoreBatch`.
    pub stored: u32,
}

impl PodcastDhtResponse {
    fn success(nodes: Vec<NodeContact>, records: Vec<SignedPodcastRecord>, stored: u32) -> Self {
        Self {
            version: PODCAST_DHT_PROTOCOL_VERSION,
            ok: true,
            error: None,
            nodes,
            records,
            stored,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        let mut message = message.into();
        if message.len() > MAX_ERROR_BYTES {
            let mut end = MAX_ERROR_BYTES;
            while !message.is_char_boundary(end) {
                end -= 1;
            }
            message.truncate(end);
        }
        Self {
            version: PODCAST_DHT_PROTOCOL_VERSION,
            ok: false,
            error: Some(message),
            nodes: Vec::new(),
            records: Vec::new(),
            stored: 0,
        }
    }
}

/// Statistics of one publication round.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PodcastDhtPublishStats {
    /// Signed records pushed.
    pub records: usize,
    /// `(key, record)` placements pushed.
    pub keys: usize,
    /// Remote overlay nodes that stored at least one placement.
    pub remote_nodes: usize,
    /// Whether this node kept at least one replica itself.
    pub local_replica: bool,
}

/// Result of [`PodcastDht::sync_shares`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodcastDhtSyncStats {
    /// Cards published with a new revision, or refreshed because they used
    /// half their lifetime.
    pub published: usize,
    /// Shared podcasts whose current card was already published. The first
    /// call in a process re-sends these cards as they are (counted in
    /// [`Self::push`]), since [`PodcastDht::republish`] withholds them until
    /// then.
    pub unchanged: usize,
    /// Podcasts unpublished with a tombstone.
    pub unpublished: usize,
    /// Shared podcasts not published because their card has privacy
    /// warnings the user has not confirmed. The UI asks, then calls
    /// [`PodcastDht::publish`] or `sync_shares` with
    /// [`PodcastShareState::privacy_confirmed`]. An earlier publication of
    /// the podcast stays as it was.
    pub held_back: Vec<PodcastPrivacyHold>,
    /// Podcasts whose card could not be built or signed. They are not
    /// published, and an earlier publication of them is unpublished.
    pub failed: Vec<PodcastSyncFailure>,
    /// Network statistics of the pushed records.
    pub push: PodcastDhtPublishStats,
}

/// A shared podcast held back by [`PodcastDht::sync_shares`] until the user
/// confirms publishing URLs that look private.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodcastPrivacyHold {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Warnings of [`PodcastCardSpec::privacy_warnings`].
    pub warnings: Vec<FeedUrlPrivacyWarning>,
}

/// A podcast [`PodcastDht::sync_shares`] could not publish or unpublish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodcastSyncFailure {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Diagnostic.
    pub error: String,
}

/// Personal registers of one podcast, as input to [`PodcastDht::sync_shares`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodcastShareState {
    /// Merged subscription register.
    pub subscription: SubscriptionRecord,
    /// Merged share register, if any.
    pub share: Option<ShareRecord>,
    /// Card to publish when shared; `None` derives it with
    /// [`PodcastCardSpec::from_subscription`].
    pub card: Option<PodcastCardSpec>,
    /// Whether the user confirmed publishing this card's feed URLs despite
    /// its [`PodcastCardSpec::privacy_warnings`]. Without it, a card with
    /// warnings is only published when this device already publishes the
    /// same feed URLs.
    pub privacy_confirmed: bool,
}

impl PodcastShareState {
    /// Whether the podcast must be published ([`effective_share`]).
    pub fn is_shared(&self) -> bool {
        effective_share(Some(&self.subscription), self.share.as_ref())
    }

    /// The bounded, validated card [`PodcastDht::sync_shares`] would
    /// publish: [`Self::card`] or one derived from the subscription.
    pub fn card_spec(&self) -> Result<PodcastCardSpec> {
        let card = match &self.card {
            Some(card) => card.bounded(),
            None => PodcastCardSpec::from_subscription(&self.subscription)?,
        };
        if card.podcast_id != self.subscription.podcast_id {
            return Err(publication_error(
                "card and subscription describe different podcasts",
            ));
        }
        card.validate()?;
        Ok(card)
    }
}

/// This device's publication state of one podcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishedPodcast {
    /// Podcast.
    pub podcast_id: PodcastId,
    /// Latest revision used by this device.
    pub revision: u64,
    /// Whether a card is published (false after unpublishing).
    pub active: bool,
    /// Latest signed record (card or tombstone).
    pub record: SignedPodcastRecord,
}

/// Podcast-discovery overlay attached to a [`MusicDhtService`].
pub struct PodcastDht {
    service: Arc<MusicDhtService>,
    database: PodcastDhtDatabase,
    network_id: NetworkId,
    own_contact: NodeContact,
    routing: Mutex<RoutingTable>,
    unsupported: Mutex<HashMap<EndpointId, u64>>,
    failed: Mutex<HashMap<EndpointId, u64>>,
    failures: Mutex<HashMap<EndpointId, u32>>,
    publish_gate: Semaphore,
    /// Set by the first [`PodcastDht::sync_shares`] of this process. Until
    /// then own cards from the persisted cache are neither republished nor
    /// served: the registers they were derived from may be stale.
    synced: AtomicBool,
    /// Podcasts published with [`PodcastDht::publish`] in this process; their
    /// cards are sent even before the first `sync_shares`.
    published_here: Mutex<HashSet<PodcastId>>,
    serve_limit: Arc<Semaphore>,
    inflight: Arc<Mutex<HashMap<EndpointId, usize>>>,
}

impl std::fmt::Debug for PodcastDht {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PodcastDht")
            .field("endpoint_id", &self.own_contact.peer_id)
            .field("known_peers", &lock(&self.routing).len())
            .finish_non_exhaustive()
    }
}

impl PodcastDht {
    /// Opens the overlay cache at `database_path` and restores previously
    /// learned overlay contacts. The caller owns the serving and maintenance
    /// tasks so they share the application's shutdown lifecycle.
    pub async fn open(
        service: Arc<MusicDhtService>,
        database_path: impl AsRef<Path>,
    ) -> Result<Arc<Self>> {
        let network_id = service.network_id();
        let database = PodcastDhtDatabase::open(database_path.as_ref(), network_id).await?;
        let ticket = service.ticket().await?.to_string();
        let owner = service.endpoint_id();
        let own_contact = NodeContact {
            node_id: NodeId::from_endpoint(&owner),
            peer_id: owner,
            ticket,
            last_seen_ms: now_ms(),
        };
        let mut routing = RoutingTable::new(own_contact.node_id);
        for contact in database.load_peers().await? {
            if validate_contact(&contact, &network_id, Some(owner)).is_ok() {
                routing.upsert(contact);
            }
        }
        Ok(Arc::new(Self {
            service,
            database,
            network_id,
            own_contact,
            routing: Mutex::new(routing),
            unsupported: Mutex::new(HashMap::new()),
            failed: Mutex::new(HashMap::new()),
            failures: Mutex::new(HashMap::new()),
            publish_gate: Semaphore::new(1),
            synced: AtomicBool::new(false),
            published_here: Mutex::new(HashSet::new()),
            serve_limit: Arc::new(Semaphore::new(MAX_CONCURRENT_PODCAST_DHT_REQUESTS)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
        }))
    }

    /// Number of known peers that implement the overlay.
    pub fn known_peers(&self) -> usize {
        lock(&self.routing).len()
    }

    /// Serves authenticated overlay requests until `acceptor` closes.
    pub async fn serve(self: Arc<Self>, mut acceptor: StreamAcceptor) {
        while let Some(stream) = acceptor.accept().await {
            let Some(slot) = PeerSlot::acquire(&self.inflight, stream.peer_id) else {
                debug!(peer = %stream.peer_id, "podcast DHT request dropped: peer limit");
                continue;
            };
            let Ok(permit) = Arc::clone(&self.serve_limit).try_acquire_owned() else {
                debug!(peer = %stream.peer_id, "podcast DHT request dropped: overloaded");
                continue;
            };
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                let _permit = permit;
                let _slot = slot;
                let peer = stream.peer_id;
                match tokio::time::timeout(SERVE_TIMEOUT, this.serve_one(stream)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => debug!(%peer, %error, "podcast DHT request failed"),
                    Err(_) => debug!(%peer, "podcast DHT request timed out"),
                }
            });
        }
    }

    /// Periodically discovers overlay peers, republishes own cards and
    /// tombstones every [`PODCAST_REPUBLISH_INTERVAL`] and removes expired
    /// replicas. The first bootstrap runs immediately. Own cards are only
    /// republished once the client called [`Self::sync_shares`] in this
    /// process (see [`Self::republish`]).
    pub async fn maintenance(self: Arc<Self>) {
        let mut bootstrap = tokio::time::interval(PODCAST_BOOTSTRAP_INTERVAL);
        let mut republish = tokio::time::interval(PODCAST_REPUBLISH_INTERVAL);
        let mut expire = tokio::time::interval(PODCAST_EXPIRE_INTERVAL);
        bootstrap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        republish.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        expire.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        republish.tick().await;
        loop {
            tokio::select! {
                _ = bootstrap.tick() => {
                    let before = self.known_peers();
                    if let Err(error) = self.bootstrap().await {
                        debug!(%error, "podcast DHT bootstrap failed");
                    } else if self.known_peers() > before
                        && let Err(error) = self.republish().await
                    {
                        warn!(%error, "podcast DHT publication after bootstrap failed");
                    }
                }
                _ = republish.tick() => {
                    if let Err(error) = self.republish().await {
                        warn!(%error, "podcast DHT republish failed");
                    }
                }
                _ = expire.tick() => {
                    match self.database.delete_expired(now_ms()).await {
                        Ok(0) => {}
                        Ok(count) => debug!(count, "expired podcast DHT replicas removed"),
                        Err(error) => warn!(%error, "podcast DHT expiry sweep failed"),
                    }
                }
            }
        }
    }

    /// Records what a peer's capability manifest says about this overlay.
    ///
    /// A peer whose manifest lacks [`PODCAST_DHT_ID`] at
    /// [`PODCAST_DHT_PROTOCOL_VERSION`] is removed from the overlay routing
    /// table and not contacted for [`UNSUPPORTED_PEER_BACKOFF`]. Peers only
    /// enter the routing table after answering a request on
    /// [`PODCAST_DHT_ALPN`] or sending one as an authenticated requester, so
    /// a peer without the overlay is never asked to store records and never
    /// counts for replica responsibility. A contact another peer returns is
    /// only a lookup candidate until it answers itself; a candidate that does
    /// not answer is backed off like any failed peer.
    pub async fn note_peer_capabilities(
        &self,
        peer: EndpointId,
        manifest: &CapabilityManifest,
    ) -> Result<()> {
        if manifest.supports_podcast_dht() {
            lock(&self.unsupported).remove(&peer);
            return Ok(());
        }
        let until = now_ms().saturating_add(UNSUPPORTED_PEER_BACKOFF.as_millis() as u64);
        lock(&self.unsupported).insert(peer, until);
        lock(&self.routing).remove(&peer);
        self.database.remove_peer(peer).await
    }

    /// Discovers overlay peers among the routing table and the music DHT
    /// contacts with one `FindNode` each, then asks the not yet known contacts
    /// they return once more. Music-DHT contacts whose capability manifest
    /// lacks the overlay ([`Self::note_peer_capabilities`]) are skipped;
    /// others are dialed on [`PODCAST_DHT_ALPN`]. A peer only becomes an
    /// overlay contact when it answers.
    pub async fn bootstrap(&self) -> Result<()> {
        let known_feature: HashSet<EndpointId> = lock(&self.routing)
            .contacts()
            .into_iter()
            .map(|contact| contact.peer_id)
            .collect();
        let mut candidates = lock(&self.routing).contacts();
        candidates.extend(
            self.service
                .known_peers()
                .into_iter()
                .filter(|contact| !known_feature.contains(&contact.peer_id)),
        );
        let mut seen: HashSet<EndpointId> = known_feature;
        seen.insert(self.own_contact.peer_id);
        let mut queried = HashSet::new();
        candidates.retain(|contact| {
            contact.peer_id != self.own_contact.peer_id
                && !self.is_unsupported(&contact.peer_id)
                && !self.is_backed_off(&contact.peer_id)
                && queried.insert(contact.peer_id)
        });
        seen.extend(queried);
        candidates.truncate(MAX_BOOTSTRAP_PEERS);
        let mut discovered = Vec::new();
        for (contact, response) in self.find_own_node(candidates).await {
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    debug!(peer = %contact.peer_id, %error, "podcast DHT bootstrap peer unavailable");
                    continue;
                }
            };
            self.learn_contact(contact).await?;
            for node in response.nodes {
                if validate_contact(&node, &self.network_id, Some(self.own_contact.peer_id)).is_ok()
                    && !self.is_unsupported(&node.peer_id)
                    && !self.is_backed_off(&node.peer_id)
                    && seen.insert(node.peer_id)
                {
                    discovered.push(node);
                }
            }
        }
        // Returned contacts are unverified: they join only by answering.
        discovered.truncate(MAX_BOOTSTRAP_PEERS);
        for (contact, response) in self.find_own_node(discovered).await {
            match response {
                Ok(_) => self.learn_contact(contact).await?,
                Err(error) => {
                    debug!(peer = %contact.peer_id, %error, "podcast DHT contact did not answer");
                }
            }
        }
        Ok(())
    }

    /// One `FindNode` for this node's id to each contact.
    async fn find_own_node(
        &self,
        contacts: Vec<NodeContact>,
    ) -> Vec<(NodeContact, Result<PodcastDhtResponse>)> {
        let target = *self.own_contact.node_id.as_bytes();
        stream::iter(contacts.into_iter().map(|contact| async move {
            let response = self
                .exchange(&contact, PodcastDhtOperation::FindNode { target })
                .await;
            (contact, response)
        }))
        .buffer_unordered(BOOTSTRAP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
    }

    /// Publishes (or refreshes) this device's card of a podcast.
    ///
    /// Refused with [`MusicDhtError::PodcastPublication`] unless
    /// [`effective_share`] holds for the given registers and they and `card`
    /// describe the same podcast. `card` is bounded with
    /// [`PodcastCardSpec::bounded`]. A changed card gets revision
    /// `max(previous + 1, now_ms)`; an unchanged one keeps its revision and
    /// is re-signed with a fresh lifetime.
    ///
    /// Call [`feed_url_privacy_warning`] (or
    /// [`PodcastCardSpec::privacy_warnings`]) first and ask the user before
    /// publishing a URL that looks private.
    pub async fn publish(
        &self,
        subscription: &SubscriptionRecord,
        share: &ShareRecord,
        card: PodcastCardSpec,
    ) -> Result<PodcastDhtPublishStats> {
        if card.podcast_id != subscription.podcast_id {
            return Err(publication_error(
                "card and subscription describe different podcasts",
            ));
        }
        if !effective_share(Some(subscription), Some(share)) {
            return Err(publication_error("the podcast is not shared"));
        }
        let card = card.bounded();
        card.validate()?;
        let _permit = self.publish_permit().await?;
        let podcast_id = card.podcast_id.clone();
        let record = self.sign_card_locked(card, true).await?;
        lock(&self.published_here).insert(podcast_id);
        self.push(record.into_iter().collect()).await
    }

    /// Unpublishes this device's card of `podcast_id` with a tombstone of
    /// revision `previous + 1`, retained by replicas for
    /// [`PODCAST_TOMBSTONE_TTL`].
    ///
    /// Does nothing, and sends nothing, when this device never published the
    /// podcast; re-sends the existing tombstone when already unpublished.
    pub async fn unpublish(&self, podcast_id: &PodcastId) -> Result<PodcastDhtPublishStats> {
        let _permit = self.publish_permit().await?;
        let record = self.unpublish_locked(podcast_id).await?;
        self.push(record.into_iter().collect()).await
    }

    /// Declaratively reconciles this device's publications with the merged
    /// personal registers.
    ///
    /// Podcasts for which [`effective_share`] holds are published when their
    /// card changed, is not published yet or has used half its lifetime;
    /// otherwise they count as unchanged (periodic refresh is
    /// [`Self::republish`]). Every other podcast this device published,
    /// including podcasts absent from `states`, is unpublished. The call is
    /// idempotent: repeating it publishes nothing new.
    ///
    /// The first successful call in a process arms [`Self::republish`] and
    /// serving of own cards: clients call it on start, after loading their
    /// merged registers, and again whenever they change. That call also
    /// re-sends the unchanged cards to the overlay as they are.
    ///
    /// Each podcast is handled on its own, so revocation is fail-safe: a
    /// shared podcast whose card cannot be built is reported in
    /// [`PodcastDhtSyncStats::failed`] and unpublished, and never prevents
    /// other podcasts from being unpublished. A card with
    /// [`PodcastCardSpec::privacy_warnings`] is held back
    /// ([`PodcastDhtSyncStats::held_back`]) unless
    /// [`PodcastShareState::privacy_confirmed`] is set or this device already
    /// publishes the same feed URLs.
    pub async fn sync_shares(&self, states: Vec<PodcastShareState>) -> Result<PodcastDhtSyncStats> {
        let _permit = self.publish_permit().await?;
        let first_sync = !self.synced.load(Ordering::SeqCst);
        let local = self.database.local_publications().await?;
        let active: HashMap<&PodcastId, &PodcastCardSpec> = local
            .iter()
            .filter_map(|publication| {
                publication
                    .spec
                    .as_ref()
                    .map(|spec| (&publication.podcast_id, spec))
            })
            .collect();
        let mut stats = PodcastDhtSyncStats::default();
        let mut desired: BTreeMap<PodcastId, PodcastCardSpec> = BTreeMap::new();
        let mut held: HashSet<PodcastId> = HashSet::new();
        for state in &states {
            if !state.is_shared() {
                continue;
            }
            let podcast_id = state.subscription.podcast_id.clone();
            let card = match state.card_spec() {
                Ok(card) => card,
                Err(error) => {
                    debug!(%podcast_id, %error, "podcast card cannot be published");
                    stats.failed.push(PodcastSyncFailure {
                        podcast_id,
                        error: error.to_string(),
                    });
                    continue;
                }
            };
            let warnings = card.privacy_warnings();
            let already_published = active.get(&podcast_id).is_some_and(|published| {
                published.feed_url == card.feed_url && published.id_feed_url == card.id_feed_url
            });
            if !warnings.is_empty() && !state.privacy_confirmed && !already_published {
                held.insert(podcast_id.clone());
                stats.held_back.push(PodcastPrivacyHold {
                    podcast_id,
                    warnings,
                });
                continue;
            }
            desired.insert(podcast_id, card);
        }

        let mut records = Vec::new();
        for publication in &local {
            let podcast_id = &publication.podcast_id;
            if publication.spec.is_none()
                || desired.contains_key(podcast_id)
                || held.contains(podcast_id)
            {
                continue;
            }
            match self.unpublish_locked(podcast_id).await {
                Ok(record) => {
                    records.extend(record);
                    stats.unpublished += 1;
                }
                Err(error) => stats.failed.push(PodcastSyncFailure {
                    podcast_id: podcast_id.clone(),
                    error: error.to_string(),
                }),
            }
        }
        for (podcast_id, card) in desired {
            match self.sign_card_locked(card, false).await {
                Ok(Some(record)) => {
                    records.push(record);
                    stats.published += 1;
                }
                Ok(None) => {
                    stats.unchanged += 1;
                    // Withheld by `republish` until now: send it as it is.
                    if first_sync
                        && let Some(publication) = local
                            .iter()
                            .find(|publication| publication.podcast_id == podcast_id)
                    {
                        records.push(publication.record.clone());
                    }
                }
                Err(error) => stats.failed.push(PodcastSyncFailure {
                    podcast_id,
                    error: error.to_string(),
                }),
            }
        }
        // Own publications now reflect the client's registers.
        self.synced.store(true, Ordering::SeqCst);
        stats.push = self.push(records).await?;
        Ok(stats)
    }

    /// Re-signs every published card with a fresh lifetime (same revision)
    /// and re-sends every unexpired tombstone. Called by
    /// [`Self::maintenance`] every [`PODCAST_REPUBLISH_INTERVAL`] and after a
    /// bootstrap that found new peers.
    ///
    /// Until [`Self::sync_shares`] has run in this process, only tombstones
    /// and cards published with [`Self::publish`] in this process are sent:
    /// the persisted cards were derived from registers that may have been
    /// revoked on another trusted device meanwhile.
    pub async fn republish(&self) -> Result<PodcastDhtPublishStats> {
        let _permit = self.publish_permit().await?;
        let now = now_ms();
        let mut records = Vec::new();
        let mut withheld = 0usize;
        for publication in self.database.local_publications().await? {
            match publication.spec {
                Some(spec) if self.may_send_card(&publication.podcast_id) => {
                    records.extend(self.sign_card_locked(spec, true).await?);
                }
                Some(_) => withheld += 1,
                None if publication.record.expires_at_ms() > now => {
                    records.push(publication.record);
                }
                None => {}
            }
        }
        if withheld > 0 {
            debug!(
                withheld,
                "own podcast cards wait for the first share reconciliation"
            );
        }
        if records.is_empty() {
            return Ok(PodcastDhtPublishStats::default());
        }
        let stats = self.push(records).await?;
        info!(
            records = stats.records,
            nodes = stats.remote_nodes,
            "podcast DHT records republished"
        );
        Ok(stats)
    }

    /// This device's publication state, sorted by podcast id.
    pub async fn published(&self) -> Result<Vec<PublishedPodcast>> {
        Ok(self
            .database
            .local_publications()
            .await?
            .into_iter()
            .map(|publication| PublishedPodcast {
                podcast_id: publication.podcast_id,
                revision: publication.revision,
                active: publication.spec.is_some(),
                record: publication.record,
            })
            .collect())
    }

    /// Finds public podcasts whose title or author tokens contain every
    /// normalized token of `query` ([`podcast_query_tokens`]).
    ///
    /// Hits are merged by `podcast_id` over publishers and ranked: exact
    /// normalized title first, then hits with a card under the origin feed
    /// URL. Partial results are returned when peers fail.
    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<PodcastSearchHit>> {
        let tokens = podcast_query_tokens(query);
        if tokens.is_empty() || limit == 0 {
            return Ok(Vec::new());
        }
        if self.known_peers() == 0 {
            self.bootstrap().await?;
        }
        let mut lookup_tokens = tokens.clone();
        lookup_tokens.sort_by(|left, right| right.len().cmp(&left.len()).then(left.cmp(right)));
        lookup_tokens.truncate(MAX_PODCAST_QUERY_LOOKUPS);
        let outcomes = stream::iter(lookup_tokens.into_iter().map(|token| {
            let filter = tokens.clone();
            async move {
                self.lookup(PodcastDhtKey::token(&self.network_id, &token), filter)
                    .await
            }
        }))
        .buffer_unordered(MAX_PODCAST_QUERY_LOOKUPS)
        .collect::<Vec<_>>()
        .await;
        let mut records = Vec::new();
        for outcome in outcomes {
            match outcome {
                Ok(found) => records.extend(found),
                Err(error) => debug!(%error, "podcast DHT text lookup failed"),
            }
        }
        let mut hits = rank_hits(aggregate_hits(records), query, &tokens);
        hits.truncate(limit.min(MAX_PODCAST_SEARCH_RESULTS));
        Ok(hits)
    }

    /// Finds the public cards of one podcast.
    pub async fn lookup_podcast(&self, podcast_id: &PodcastId) -> Result<Option<PodcastSearchHit>> {
        if self.known_peers() == 0 {
            self.bootstrap().await?;
        }
        let records = self
            .lookup(
                PodcastDhtKey::podcast(&self.network_id, podcast_id),
                Vec::new(),
            )
            .await?;
        Ok(aggregate_hits(records)
            .into_iter()
            .find(|hit| hit.podcast_id == *podcast_id))
    }

    /// Whether an own card of `podcast_id` may leave this device: after the
    /// first [`Self::sync_shares`] of this process, or when it was published
    /// with [`Self::publish`] in this process.
    fn may_send_card(&self, podcast_id: &PodcastId) -> bool {
        self.synced.load(Ordering::SeqCst) || lock(&self.published_here).contains(podcast_id)
    }

    async fn publish_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>> {
        self.publish_gate
            .acquire()
            .await
            .map_err(|_| publication_error("podcast DHT publisher closed"))
    }

    /// Signs and records a card. With `force` false, an unchanged active card
    /// that is valid for more than half its TTL is left alone and `None` is
    /// returned. The publish gate must be held.
    async fn sign_card_locked(
        &self,
        spec: PodcastCardSpec,
        force: bool,
    ) -> Result<Option<SignedPodcastRecord>> {
        let now = now_ms();
        let previous = self
            .database
            .local_publication(spec.podcast_id.clone())
            .await?;
        let revision = match &previous {
            Some(previous) if previous.spec.as_ref() == Some(&spec) => {
                // Unchanged and still fresh: leave it to the republish loop.
                let fresh_until = now.saturating_add(PODCAST_CARD_TTL.as_millis() as u64 / 2);
                if !force && previous.record.expires_at_ms() > fresh_until {
                    return Ok(None);
                }
                previous.revision
            }
            Some(previous) => previous.revision.saturating_add(1).max(now),
            None => now.max(1),
        };
        let card = PodcastCard::from_spec(
            spec.clone(),
            self.network_id,
            self.own_contact.peer_id,
            revision,
            now,
        );
        let record =
            SignedPodcastRecord::sign_card(card, |bytes| self.service.sign_identity(bytes))?;
        self.database
            .put_local_publication(
                LocalPublication {
                    podcast_id: spec.podcast_id.clone(),
                    revision,
                    spec: Some(spec),
                    record: record.clone(),
                },
                now,
            )
            .await?;
        Ok(Some(record))
    }

    /// Builds the tombstone of an own publication. The publish gate must be
    /// held.
    async fn unpublish_locked(
        &self,
        podcast_id: &PodcastId,
    ) -> Result<Option<SignedPodcastRecord>> {
        let now = now_ms();
        let Some(previous) = self.database.local_publication(podcast_id.clone()).await? else {
            return Ok(None);
        };
        if previous.spec.is_none() {
            return Ok(
                (previous.record.is_tombstone() && previous.record.expires_at_ms() > now)
                    .then_some(previous.record),
            );
        }
        let revision = previous.revision.saturating_add(1);
        let tombstone = PodcastTombstone::new(
            self.network_id,
            self.own_contact.peer_id,
            podcast_id.clone(),
            revision,
            now,
            previous.record.dht_keys(),
        );
        let record = SignedPodcastRecord::sign_tombstone(tombstone, |bytes| {
            self.service.sign_identity(bytes)
        })?;
        self.database
            .put_local_publication(
                LocalPublication {
                    podcast_id: podcast_id.clone(),
                    revision,
                    spec: None,
                    record: record.clone(),
                },
                now,
            )
            .await?;
        Ok(Some(record))
    }

    /// Stores records on the `K` closest overlay nodes of each of their keys
    /// (including this node when it is among them). When a remote node
    /// refuses a batch (`stored = 0`, for example because it does not count
    /// itself responsible for the keys) or fails, its records are offered
    /// once to the next closest nodes of the same keys, one per refusal.
    async fn push(&self, records: Vec<SignedPodcastRecord>) -> Result<PodcastDhtPublishStats> {
        let mut stats = PodcastDhtPublishStats {
            records: records.len(),
            ..PodcastDhtPublishStats::default()
        };
        if records.is_empty() {
            return Ok(stats);
        }
        let mut local = Vec::new();
        let mut plan = StorePlan::new();
        let mut fallbacks: HashMap<(usize, PodcastDhtKey), Vec<NodeContact>> = HashMap::new();
        for (index, record) in records.iter().enumerate() {
            let mut local_keys = Vec::new();
            for key in record.dht_keys() {
                stats.keys += 1;
                let targets = self.replica_targets(&key);
                if targets.local {
                    local_keys.push(key);
                }
                for contact in targets.primary {
                    plan.add(contact, index, key);
                }
                fallbacks.insert((index, key), targets.fallback);
            }
            if !local_keys.is_empty() {
                local.push((local_keys, record.clone()));
            }
        }
        if !local.is_empty() {
            self.database.store(local, now_ms()).await?;
            stats.local_replica = true;
        }
        let (mut accepted, refused) = self.send_store_plan(&records, plan).await;
        let retry = retry_plan(refused, &mut fallbacks);
        if !retry.is_empty() {
            accepted.extend(self.send_store_plan(&records, retry).await.0);
        }
        stats.remote_nodes = accepted.len();
        Ok(stats)
    }

    /// Sends a store plan. Returns the nodes that stored at least one
    /// placement and the `(record index, key)` placements offered in batches
    /// that were refused or failed.
    async fn send_store_plan(
        &self,
        records: &[SignedPodcastRecord],
        plan: StorePlan,
    ) -> (HashSet<EndpointId>, Vec<(usize, PodcastDhtKey)>) {
        let sends = stream::iter(plan.targets.into_values().map(|(contact, entries)| async move {
            let entries: Vec<(usize, PodcastStoreEntry)> = entries
                .into_iter()
                .map(|(index, keys)| {
                    (
                        index,
                        PodcastStoreEntry {
                            keys,
                            record: records[index].clone(),
                        },
                    )
                })
                .collect();
            let mut accepted = false;
            let mut refused = Vec::new();
            let mut failed = false;
            for batch in entries.chunks(MAX_PODCAST_STORE_RECORDS) {
                let stored = if failed {
                    false
                } else {
                    let response = self
                        .exchange(
                            &contact,
                            PodcastDhtOperation::StoreBatch {
                                records: batch.iter().map(|(_, entry)| entry.clone()).collect(),
                            },
                        )
                        .await;
                    match response {
                        Ok(response) => response.stored > 0,
                        Err(error) => {
                            debug!(peer = %contact.peer_id, %error, "podcast DHT store failed");
                            failed = true;
                            false
                        }
                    }
                };
                if stored {
                    accepted = true;
                } else {
                    for (index, entry) in batch {
                        refused.extend(entry.keys.iter().map(|key| (*index, *key)));
                    }
                }
            }
            (contact.peer_id, accepted, refused)
        }))
        .buffer_unordered(PUBLISH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut accepted = HashSet::new();
        let mut refused = Vec::new();
        for (peer, stored, keys) in sends {
            if stored {
                accepted.insert(peer);
            }
            refused.extend(keys);
        }
        (accepted, refused)
    }

    /// Iterative lookup of `key`: queries the `K` closest known overlay
    /// nodes until all have answered, the request budget is spent or the
    /// deadline passes. Returns the winning verified record per
    /// `(podcast_id, publisher)`.
    async fn lookup(
        &self,
        key: PodcastDhtKey,
        filter_tokens: Vec<String>,
    ) -> Result<Vec<SignedPodcastRecord>> {
        let mut winners: HashMap<(PodcastId, EndpointId), SignedPodcastRecord> = HashMap::new();
        let local = self
            .database
            .records_by_key(
                key,
                filter_tokens.clone(),
                now_ms(),
                MAX_PODCAST_DHT_RECORDS,
            )
            .await?;
        merge_records(&mut winners, local, &self.network_id, &key, &filter_tokens);

        let mut candidates = lock(&self.routing).closest(key.as_bytes(), K);
        let mut known: HashSet<_> = candidates.iter().map(|contact| contact.peer_id).collect();
        let mut queried = HashSet::new();
        let mut sent = 0usize;
        let deadline = tokio::time::Instant::now() + LOOKUP_TIMEOUT;
        while tokio::time::Instant::now() < deadline && sent < MAX_LOOKUP_REQUESTS {
            candidates.sort_by_key(|contact| distance(contact.node_id.as_bytes(), key.as_bytes()));
            let batch: Vec<_> = candidates
                .iter()
                .filter(|contact| !self.is_backed_off(&contact.peer_id))
                .take(K)
                .filter(|contact| !queried.contains(&contact.peer_id))
                .take(ALPHA.min(MAX_LOOKUP_REQUESTS - sent))
                .cloned()
                .collect();
            if batch.is_empty() {
                break;
            }
            sent += batch.len();
            queried.extend(batch.iter().map(|contact| contact.peer_id));
            let operation = PodcastDhtOperation::FindValue {
                key,
                filter_tokens: filter_tokens.clone(),
            };
            let mut futures: FuturesUnordered<_> = batch
                .into_iter()
                .map(|contact| {
                    let operation = operation.clone();
                    async move {
                        let response = self.exchange(&contact, operation).await;
                        (contact, response)
                    }
                })
                .collect();
            while let Ok(Some((contact, response))) =
                tokio::time::timeout_at(deadline, futures.next()).await
            {
                let Ok(response) = response else {
                    continue;
                };
                // It answered on the overlay: now it is a verified contact.
                self.learn_contact(contact).await?;
                merge_records(
                    &mut winners,
                    response.records,
                    &self.network_id,
                    &key,
                    &filter_tokens,
                );
                for node in response.nodes {
                    // Returned contacts are only candidates until they answer.
                    if validate_contact(&node, &self.network_id, Some(self.own_contact.peer_id))
                        .is_ok()
                        && !self.is_unsupported(&node.peer_id)
                        && known.insert(node.peer_id)
                    {
                        candidates.push(node);
                    }
                }
            }
        }
        Ok(winners.into_values().collect())
    }

    async fn serve_one(&self, mut stream: ByteStream) -> Result<()> {
        let authenticated_peer = stream.peer_id;
        let response = match read_postcard::<_, PodcastDhtRequest>(
            &mut stream.recv,
            MAX_PODCAST_DHT_REQUEST_BYTES,
        )
        .await
        {
            Ok(request) => match self.handle_request(authenticated_peer, request).await {
                Ok(response) => response,
                Err(error) => PodcastDhtResponse::error(error.to_string()),
            },
            Err(error) => PodcastDhtResponse::error(error.to_string()),
        };
        write_postcard(&mut stream.send, &response, MAX_PODCAST_DHT_RESPONSE_BYTES).await?;
        stream.send.finish().map_err(network_error)?;
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.send.stopped()).await;
        Ok(())
    }

    async fn handle_request(
        &self,
        authenticated_peer: EndpointId,
        request: PodcastDhtRequest,
    ) -> Result<PodcastDhtResponse> {
        if request.version != PODCAST_DHT_PROTOCOL_VERSION {
            return Err(protocol_error("unsupported podcast DHT request version"));
        }
        if request.requester.peer_id != authenticated_peer {
            return Err(protocol_error("podcast DHT requester identity mismatch"));
        }
        self.learn_contact(request.requester).await?;
        match request.operation {
            PodcastDhtOperation::FindNode { target } => Ok(PodcastDhtResponse::success(
                self.closest_contacts(&target, K),
                Vec::new(),
                0,
            )),
            PodcastDhtOperation::FindValue { key, filter_tokens } => {
                validate_filter_tokens(&filter_tokens)?;
                let mut records = self
                    .database
                    .records_by_key(key, filter_tokens, now_ms(), MAX_PODCAST_DHT_RECORDS)
                    .await?;
                // Own cards persisted by an earlier run wait for the first
                // share reconciliation, like republishing.
                let own = self.own_contact.peer_id;
                records.retain(|record| {
                    record.publisher() != own
                        || record.is_tombstone()
                        || self.may_send_card(record.podcast_id())
                });
                Ok(PodcastDhtResponse::success(
                    self.closest_contacts(key.as_bytes(), K),
                    records,
                    0,
                ))
            }
            PodcastDhtOperation::StoreBatch { records } => {
                let entries =
                    validate_store_batch(records, &self.network_id, authenticated_peer, now_ms())?;
                // Only verified contacts that are not backed off count.
                let nodes: Vec<NodeId> = lock(&self.routing)
                    .contacts()
                    .into_iter()
                    .filter(|contact| !self.is_backed_off(&contact.peer_id))
                    .map(|contact| contact.node_id)
                    .collect();
                let entries: Vec<_> = entries
                    .into_iter()
                    .filter_map(|(mut keys, record)| {
                        keys.retain(|key| is_responsible(&self.own_contact.node_id, &nodes, key));
                        (!keys.is_empty()).then_some((keys, record))
                    })
                    .collect();
                let stored = if entries.is_empty() {
                    0
                } else {
                    self.database.store(entries, now_ms()).await?
                };
                Ok(PodcastDhtResponse::success(
                    Vec::new(),
                    Vec::new(),
                    u32::try_from(stored).unwrap_or(u32::MAX),
                ))
            }
        }
    }

    /// One request with [`REQUEST_TIMEOUT`]. Network failures and timeouts
    /// put the peer in a short backoff; a success clears it.
    async fn exchange(
        &self,
        contact: &NodeContact,
        operation: PodcastDhtOperation,
    ) -> Result<PodcastDhtResponse> {
        let result = tokio::time::timeout(REQUEST_TIMEOUT, self.exchange_once(contact, operation))
            .await
            .unwrap_or(Err(MusicDhtError::Timeout));
        match &result {
            Ok(_) => {
                lock(&self.failed).remove(&contact.peer_id);
                lock(&self.failures).remove(&contact.peer_id);
            }
            Err(MusicDhtError::Network(_) | MusicDhtError::Timeout) => {
                let until = now_ms().saturating_add(FAILED_PEER_BACKOFF.as_millis() as u64);
                lock(&self.failed).insert(contact.peer_id, until);
                let failures = {
                    let mut failures = lock(&self.failures);
                    let count = failures.entry(contact.peer_id).or_default();
                    *count += 1;
                    *count
                };
                if failures >= MAX_CONSECUTIVE_FAILURES {
                    lock(&self.failures).remove(&contact.peer_id);
                    self.forget_contact(contact.peer_id).await;
                }
            }
            Err(_) => {}
        }
        result
    }

    async fn exchange_once(
        &self,
        contact: &NodeContact,
        operation: PodcastDhtOperation,
    ) -> Result<PodcastDhtResponse> {
        validate_contact(contact, &self.network_id, Some(self.own_contact.peer_id))?;
        let ticket = PeerTicket::from_str(&contact.ticket)
            .map_err(|error| MusicDhtError::InvalidTicket(error.to_string()))?;
        let mut stream = self
            .service
            .open_stream_to(&ticket, PODCAST_DHT_ALPN)
            .await?;
        let request = PodcastDhtRequest {
            version: PODCAST_DHT_PROTOCOL_VERSION,
            requester: self.fresh_own_contact(),
            operation,
        };
        write_postcard(&mut stream.send, &request, MAX_PODCAST_DHT_REQUEST_BYTES).await?;
        stream.send.finish().map_err(network_error)?;
        let response: PodcastDhtResponse =
            read_postcard(&mut stream.recv, MAX_PODCAST_DHT_RESPONSE_BYTES).await?;
        validate_response(&response)?;
        if !response.ok {
            return Err(protocol_error(
                response
                    .error
                    .as_deref()
                    .unwrap_or("podcast DHT request refused"),
            ));
        }
        Ok(response)
    }

    /// Removes a contact that keeps failing from routing and the cache.
    async fn forget_contact(&self, peer: EndpointId) {
        if lock(&self.routing).remove(&peer) {
            debug!(%peer, "podcast DHT contact dropped after repeated failures");
        }
        if let Err(error) = self.database.remove_peer(peer).await {
            debug!(%peer, %error, "podcast DHT contact removal failed");
        }
    }

    fn is_unsupported(&self, peer: &EndpointId) -> bool {
        still_listed(&self.unsupported, peer)
    }

    fn is_backed_off(&self, peer: &EndpointId) -> bool {
        still_listed(&self.failed, peer)
    }

    /// Replica targets of `key` among the contacts that are not backed off
    /// ([`replica_targets`]).
    fn replica_targets(&self, key: &PodcastDhtKey) -> ReplicaTargets {
        let contacts: Vec<_> = lock(&self.routing)
            .contacts()
            .into_iter()
            .filter(|contact| !self.is_backed_off(&contact.peer_id))
            .collect();
        replica_targets(self.fresh_own_contact(), contacts, key)
    }

    fn closest_contacts(&self, target: &[u8; 32], count: usize) -> Vec<NodeContact> {
        let mut contacts = lock(&self.routing).contacts();
        contacts.push(self.fresh_own_contact());
        contacts.sort_by_key(|contact| distance(contact.node_id.as_bytes(), target));
        contacts.dedup_by_key(|contact| contact.peer_id);
        contacts.truncate(count);
        contacts
    }

    fn fresh_own_contact(&self) -> NodeContact {
        let mut contact = self.own_contact.clone();
        contact.last_seen_ms = now_ms();
        contact
    }

    async fn learn_contact(&self, mut contact: NodeContact) -> Result<()> {
        validate_contact(&contact, &self.network_id, Some(self.own_contact.peer_id))?;
        contact.node_id = NodeId::from_endpoint(&contact.peer_id);
        // A remote peer does not get to extend its own lifetime with a future
        // timestamp; the authenticated contact is the local observation.
        contact.last_seen_ms = now_ms();
        let is_new = lock(&self.routing).upsert(contact.clone());
        self.database.upsert_peer(contact.clone()).await?;
        if is_new {
            debug!(peer = %contact.peer_id, "learned podcast DHT peer");
        }
        Ok(())
    }
}

/// Validates a store batch received from `requester`: bounds, that every
/// record is published by the requester itself, every record's signature and
/// lifetime, and that every key belongs to its record.
///
/// Replicas never forward records, so only a record's publisher stores it.
/// Nobody can therefore replay or inject another device's records, and
/// per-publisher quotas apply to the storing peer.
fn validate_store_batch(
    entries: Vec<PodcastStoreEntry>,
    network_id: &NetworkId,
    requester: EndpointId,
    now_ms: u64,
) -> Result<Vec<(Vec<PodcastDhtKey>, SignedPodcastRecord)>> {
    if entries.is_empty() || entries.len() > MAX_PODCAST_STORE_RECORDS {
        return Err(protocol_error("invalid podcast DHT store batch"));
    }
    entries
        .into_iter()
        .map(|entry| {
            let mut keys = entry.keys;
            let unique: HashSet<_> = keys.iter().collect();
            if keys.is_empty() || keys.len() > MAX_RECORD_DHT_KEYS || unique.len() != keys.len() {
                return Err(protocol_error("invalid podcast DHT store keys"));
            }
            if entry.record.publisher() != requester {
                return Err(protocol_error(
                    "podcast records must be stored by their publisher",
                ));
            }
            entry.record.verify_at(network_id, now_ms)?;
            let valid_keys = entry.record.dht_keys();
            if keys.iter().any(|key| !valid_keys.contains(key)) {
                return Err(protocol_error("podcast record does not belong under key"));
            }
            keys.sort();
            Ok((keys, entry.record))
        })
        .collect()
}

/// Whether a node is among the nodes that should store `key`: fewer than
/// [`PODCAST_STORE_RESPONSIBILITY_WINDOW`] known contacts are closer.
fn is_responsible(own: &NodeId, contacts: &[NodeId], key: &PodcastDhtKey) -> bool {
    let own_distance = distance(own.as_bytes(), key.as_bytes());
    contacts
        .iter()
        .filter(|node| distance(node.as_bytes(), key.as_bytes()) < own_distance)
        .take(PODCAST_STORE_RESPONSIBILITY_WINDOW)
        .count()
        < PODCAST_STORE_RESPONSIBILITY_WINDOW
}

fn validate_filter_tokens(tokens: &[String]) -> Result<()> {
    if tokens.len() > MAX_PODCAST_QUERY_TOKENS
        || tokens
            .iter()
            .any(|token| token.is_empty() || token.len() > MAX_INDEX_TOKEN_BYTES)
    {
        return Err(protocol_error("invalid podcast DHT filter tokens"));
    }
    Ok(())
}

/// Adds verified records of one response (or the local cache) found under
/// `key` to the per-owner winners. A newer record of a known owner always
/// replaces its winner; the response may add at most
/// [`MAX_PODCAST_LOOKUP_OWNERS_PER_RESPONSE`] new owners, and the lookup
/// keeps at most [`MAX_PODCAST_LOOKUP_RECORDS`]. So the order in which
/// responders answer does not let the first ones take every slot.
fn merge_records(
    winners: &mut HashMap<(PodcastId, EndpointId), SignedPodcastRecord>,
    records: Vec<SignedPodcastRecord>,
    network_id: &NetworkId,
    key: &PodcastDhtKey,
    filter_tokens: &[String],
) {
    let now = now_ms();
    let mut added = 0usize;
    for record in records {
        if record.verify_at(network_id, now).is_err() || !record.belongs_under(key) {
            continue;
        }
        if let Some(card) = record.card() {
            let tokens = card.index_tokens();
            if !filter_tokens.iter().all(|token| tokens.contains(token)) {
                continue;
            }
        }
        let owner = (record.podcast_id().clone(), record.publisher());
        match winners.get(&owner) {
            Some(current) if compare_records(current, &record).is_ge() => {}
            Some(_) => {
                winners.insert(owner, record);
            }
            None if winners.len() >= MAX_PODCAST_LOOKUP_RECORDS
                || added >= MAX_PODCAST_LOOKUP_OWNERS_PER_RESPONSE => {}
            None => {
                winners.insert(owner, record);
                added += 1;
            }
        }
    }
}

/// Replica nodes of one key, from [`replica_targets`].
#[derive(Debug)]
struct ReplicaTargets {
    /// Whether this node is among the `K` closest.
    local: bool,
    /// The other nodes among the `K` closest.
    primary: Vec<NodeContact>,
    /// The next up to `K` nodes, farthest first (popped closest first).
    fallback: Vec<NodeContact>,
}

/// The `K` closest nodes of `key` among `contacts` and `own` (this node), and
/// the next `K` as fallbacks.
fn replica_targets(
    own: NodeContact,
    mut contacts: Vec<NodeContact>,
    key: &PodcastDhtKey,
) -> ReplicaTargets {
    let own_peer = own.peer_id;
    contacts.push(own);
    contacts.sort_by_key(|contact| distance(contact.node_id.as_bytes(), key.as_bytes()));
    contacts.dedup_by_key(|contact| contact.peer_id);
    let mut fallback = contacts.split_off(K.min(contacts.len()));
    fallback.retain(|contact| contact.peer_id != own_peer);
    fallback.truncate(K);
    fallback.reverse();
    let local = contacts.iter().any(|contact| contact.peer_id == own_peer);
    contacts.retain(|contact| contact.peer_id != own_peer);
    ReplicaTargets {
        local,
        primary: contacts,
        fallback,
    }
}

/// One fallback node per refused `(record index, key)` placement: the
/// closest one not yet offered that placement.
fn retry_plan(
    refused: Vec<(usize, PodcastDhtKey)>,
    fallbacks: &mut HashMap<(usize, PodcastDhtKey), Vec<NodeContact>>,
) -> StorePlan {
    let mut retry = StorePlan::new();
    for (index, key) in refused {
        if let Some(contact) = fallbacks
            .get_mut(&(index, key))
            .and_then(|fallback| fallback.pop())
        {
            retry.add(contact, index, key);
        }
    }
    retry
}

/// Placements to offer per remote node: record index and keys.
struct StorePlan {
    targets: HashMap<EndpointId, (NodeContact, BTreeMap<usize, Vec<PodcastDhtKey>>)>,
}

impl StorePlan {
    fn new() -> Self {
        Self {
            targets: HashMap::new(),
        }
    }

    fn add(&mut self, contact: NodeContact, index: usize, key: PodcastDhtKey) {
        self.targets
            .entry(contact.peer_id)
            .or_insert_with(|| (contact, BTreeMap::new()))
            .1
            .entry(index)
            .or_default()
            .push(key);
    }

    fn is_empty(&self) -> bool {
        self.targets.is_empty()
    }
}

/// One in-flight inbound request of a peer; released on drop.
struct PeerSlot {
    inflight: Arc<Mutex<HashMap<EndpointId, usize>>>,
    peer: EndpointId,
}

impl PeerSlot {
    fn acquire(
        inflight: &Arc<Mutex<HashMap<EndpointId, usize>>>,
        peer: EndpointId,
    ) -> Option<Self> {
        let mut map = lock(inflight);
        let count = map.entry(peer).or_default();
        if *count >= MAX_PODCAST_DHT_REQUESTS_PER_PEER {
            return None;
        }
        *count += 1;
        Some(Self {
            inflight: Arc::clone(inflight),
            peer,
        })
    }
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let mut map = lock(&self.inflight);
        if let Some(count) = map.get_mut(&self.peer) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                map.remove(&self.peer);
            }
        }
    }
}

fn validate_contact(
    contact: &NodeContact,
    network_id: &NetworkId,
    excluded: Option<EndpointId>,
) -> Result<()> {
    if excluded == Some(contact.peer_id)
        || contact.node_id != NodeId::from_endpoint(&contact.peer_id)
        || contact.ticket.len() > MAX_TICKET_BYTES
    {
        return Err(protocol_error("invalid podcast DHT contact"));
    }
    let ticket = PeerTicket::from_str(&contact.ticket)
        .map_err(|error| MusicDhtError::InvalidTicket(error.to_string()))?;
    if ticket.endpoint_id() != contact.peer_id || &ticket.network_id != network_id {
        return Err(protocol_error("podcast DHT contact ticket mismatch"));
    }
    Ok(())
}

fn validate_response(response: &PodcastDhtResponse) -> Result<()> {
    if response.version != PODCAST_DHT_PROTOCOL_VERSION
        || response.nodes.len() > K
        || response.records.len() > MAX_PODCAST_DHT_RECORDS
        || response
            .error
            .as_ref()
            .is_some_and(|error| error.len() > MAX_ERROR_BYTES)
        || (response.ok && response.error.is_some())
        || (!response.ok && response.error.is_none())
    {
        return Err(protocol_error("invalid podcast DHT response"));
    }
    Ok(())
}

async fn write_postcard<W: AsyncWriteExt + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    maximum: usize,
) -> Result<()> {
    let payload = postcard::to_stdvec(value).map_err(protocol_error)?;
    if payload.len() > maximum {
        return Err(protocol_error("podcast DHT message is too large"));
    }
    writer.write_all(&payload).await.map_err(network_error)
}

async fn read_postcard<R: AsyncRead + Unpin, T: for<'de> Deserialize<'de>>(
    reader: &mut R,
    maximum: usize,
) -> Result<T> {
    let mut payload = Vec::new();
    reader
        .take(maximum as u64 + 1)
        .read_to_end(&mut payload)
        .await
        .map_err(network_error)?;
    if payload.is_empty() || payload.len() > maximum {
        return Err(protocol_error("invalid podcast DHT message size"));
    }
    postcard::from_bytes(&payload).map_err(protocol_error)
}

/// Whether `peer` is in a timed list; expired entries are removed.
fn still_listed(list: &Mutex<HashMap<EndpointId, u64>>, peer: &EndpointId) -> bool {
    let now = now_ms();
    let mut list = lock(list);
    match list.get(peer) {
        Some(until) if *until > now => true,
        Some(_) => {
            list.remove(peer);
            false
        }
        None => false,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn protocol_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(error.to_string())
}

fn network_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Network(error.to_string())
}
