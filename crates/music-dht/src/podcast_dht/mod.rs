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
//!
//! Applications own the serving and maintenance tasks, exactly as for
//! [`crate::similarity_dht::SimilarityDht`]: declare [`PODCAST_DHT_ALPN`] with
//! [`crate::MusicDhtConfigBuilder::schema_independent_stream_protocol`], take
//! its acceptor and spawn [`PodcastDht::serve`] and [`PodcastDht::maintenance`].

mod card;
mod privacy;
mod search;
mod store;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
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
    FederatedSearchHit, MAX_HIT_PUBLISHERS, MAX_PODCAST_QUERY_TOKENS, PodcastSearchHit,
    SearchHitKind, merge_search_results, podcast_query_tokens,
};
pub use store::{MAX_PODCAST_RECORDS_PER_KEY, PODCAST_DHT_CACHE_SCHEMA_VERSION};

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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PodcastDhtSyncStats {
    /// Cards published with a new revision, or refreshed because they used
    /// half their lifetime.
    pub published: usize,
    /// Shared podcasts whose current card was already published.
    pub unchanged: usize,
    /// Podcasts unpublished with a tombstone.
    pub unpublished: usize,
    /// Network statistics of the pushed records.
    pub push: PodcastDhtPublishStats,
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
}

impl PodcastShareState {
    /// Whether the podcast must be published ([`effective_share`]).
    pub fn is_shared(&self) -> bool {
        effective_share(Some(&self.subscription), self.share.as_ref())
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
    publish_gate: Semaphore,
    serve_limit: Arc<Semaphore>,
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
            publish_gate: Semaphore::new(1),
            serve_limit: Arc::new(Semaphore::new(MAX_CONCURRENT_PODCAST_DHT_REQUESTS)),
        }))
    }

    /// Number of known peers that implement the overlay.
    pub fn known_peers(&self) -> usize {
        lock(&self.routing).len()
    }

    /// Serves authenticated overlay requests until `acceptor` closes.
    pub async fn serve(self: Arc<Self>, mut acceptor: StreamAcceptor) {
        while let Some(stream) = acceptor.accept().await {
            let Ok(permit) = Arc::clone(&self.serve_limit).try_acquire_owned() else {
                debug!(peer = %stream.peer_id, "podcast DHT request dropped: overloaded");
                continue;
            };
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                let _permit = permit;
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
    /// replicas. The first bootstrap runs immediately.
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
    /// enter the routing table after answering on [`PODCAST_DHT_ALPN`] or
    /// being returned by an overlay peer, so a peer without the overlay is
    /// never queried for records or asked to store them.
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
    /// contacts.
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
        let mut seen = HashSet::new();
        candidates.retain(|contact| {
            contact.peer_id != self.own_contact.peer_id
                && !self.is_unsupported(&contact.peer_id)
                && !self.is_backed_off(&contact.peer_id)
                && seen.insert(contact.peer_id)
        });
        candidates.truncate(MAX_BOOTSTRAP_PEERS);
        let target = *self.own_contact.node_id.as_bytes();
        let responses = stream::iter(candidates.into_iter().map(|contact| async move {
            let response = self
                .exchange(&contact, PodcastDhtOperation::FindNode { target })
                .await;
            (contact, response)
        }))
        .buffer_unordered(BOOTSTRAP_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        for (contact, response) in responses {
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    debug!(peer = %contact.peer_id, %error, "podcast DHT bootstrap peer unavailable");
                    continue;
                }
            };
            self.learn_contact(contact).await?;
            self.learn_response_nodes(response.nodes).await;
        }
        Ok(())
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
        let record = self.sign_card_locked(card, true).await?;
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
    pub async fn sync_shares(&self, states: Vec<PodcastShareState>) -> Result<PodcastDhtSyncStats> {
        let mut desired: HashMap<PodcastId, PodcastCardSpec> = HashMap::new();
        for state in states {
            if !state.is_shared() {
                continue;
            }
            let card = match state.card {
                Some(card) => card.bounded(),
                None => PodcastCardSpec::from_subscription(&state.subscription)?,
            };
            if card.podcast_id != state.subscription.podcast_id {
                return Err(publication_error(
                    "card and subscription describe different podcasts",
                ));
            }
            card.validate()?;
            desired.insert(card.podcast_id.clone(), card);
        }

        let _permit = self.publish_permit().await?;
        let local = self.database.local_publications().await?;
        let mut stats = PodcastDhtSyncStats::default();
        let mut records = Vec::new();
        for publication in &local {
            if publication.spec.is_some() && !desired.contains_key(&publication.podcast_id) {
                records.extend(self.unpublish_locked(&publication.podcast_id).await?);
                stats.unpublished += 1;
            }
        }
        let mut desired: Vec<_> = desired.into_values().collect();
        desired.sort_by(|left, right| left.podcast_id.cmp(&right.podcast_id));
        for card in desired {
            match self.sign_card_locked(card, false).await? {
                Some(record) => {
                    records.push(record);
                    stats.published += 1;
                }
                None => stats.unchanged += 1,
            }
        }
        stats.push = self.push(records).await?;
        Ok(stats)
    }

    /// Re-signs every published card with a fresh lifetime (same revision)
    /// and re-sends every unexpired tombstone. Called by
    /// [`Self::maintenance`] every [`PODCAST_REPUBLISH_INTERVAL`].
    pub async fn republish(&self) -> Result<PodcastDhtPublishStats> {
        let _permit = self.publish_permit().await?;
        let now = now_ms();
        let mut records = Vec::new();
        for publication in self.database.local_publications().await? {
            match publication.spec {
                Some(spec) => records.extend(self.sign_card_locked(spec, true).await?),
                None if publication.record.expires_at_ms() > now => {
                    records.push(publication.record);
                }
                None => {}
            }
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
    /// normalized title first, then more publishers. Partial results are
    /// returned when peers fail.
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
    /// (including this node when it is among them).
    async fn push(&self, records: Vec<SignedPodcastRecord>) -> Result<PodcastDhtPublishStats> {
        let mut stats = PodcastDhtPublishStats {
            records: records.len(),
            ..PodcastDhtPublishStats::default()
        };
        if records.is_empty() {
            return Ok(stats);
        }
        let mut local = Vec::new();
        let mut remote: HashMap<EndpointId, (NodeContact, Vec<PodcastStoreEntry>)> = HashMap::new();
        for record in records {
            let mut local_keys = Vec::new();
            let mut remote_keys: HashMap<EndpointId, (NodeContact, Vec<PodcastDhtKey>)> =
                HashMap::new();
            for key in record.dht_keys() {
                stats.keys += 1;
                let (store_local, contacts) = self.replica_targets(&key);
                if store_local {
                    local_keys.push(key);
                }
                for contact in contacts {
                    remote_keys
                        .entry(contact.peer_id)
                        .or_insert_with(|| (contact, Vec::new()))
                        .1
                        .push(key);
                }
            }
            if !local_keys.is_empty() {
                local.push((local_keys, record.clone()));
            }
            for (peer, (contact, keys)) in remote_keys {
                remote
                    .entry(peer)
                    .or_insert_with(|| (contact, Vec::new()))
                    .1
                    .push(PodcastStoreEntry {
                        keys,
                        record: record.clone(),
                    });
            }
        }
        if !local.is_empty() {
            self.database.store(local, now_ms()).await?;
            stats.local_replica = true;
        }
        let sends = stream::iter(remote.into_values().map(|(contact, entries)| async move {
            let mut accepted = false;
            for batch in entries.chunks(MAX_PODCAST_STORE_RECORDS) {
                let response = self
                    .exchange(
                        &contact,
                        PodcastDhtOperation::StoreBatch {
                            records: batch.to_vec(),
                        },
                    )
                    .await;
                match response {
                    Ok(response) => accepted |= response.stored > 0,
                    Err(error) => {
                        debug!(peer = %contact.peer_id, %error, "podcast DHT store failed");
                        break;
                    }
                }
            }
            accepted
        }))
        .buffer_unordered(PUBLISH_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        stats.remote_nodes = sends.into_iter().filter(|accepted| *accepted).count();
        Ok(stats)
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
                .iter()
                .map(|contact| {
                    let operation = operation.clone();
                    async move { self.exchange(contact, operation).await }
                })
                .collect();
            while let Ok(Some(response)) = tokio::time::timeout_at(deadline, futures.next()).await {
                let Ok(response) = response else {
                    continue;
                };
                merge_records(
                    &mut winners,
                    response.records,
                    &self.network_id,
                    &key,
                    &filter_tokens,
                );
                for node in response.nodes {
                    if validate_contact(&node, &self.network_id, Some(self.own_contact.peer_id))
                        .is_ok()
                        && !self.is_unsupported(&node.peer_id)
                        && known.insert(node.peer_id)
                    {
                        self.learn_contact(node.clone()).await?;
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
                let records = self
                    .database
                    .records_by_key(key, filter_tokens, now_ms(), MAX_PODCAST_DHT_RECORDS)
                    .await?;
                Ok(PodcastDhtResponse::success(
                    self.closest_contacts(key.as_bytes(), K),
                    records,
                    0,
                ))
            }
            PodcastDhtOperation::StoreBatch { records } => {
                let entries = validate_store_batch(records, &self.network_id, now_ms())?;
                let stored = self.database.store(entries, now_ms()).await?;
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
            }
            Err(MusicDhtError::Network(_) | MusicDhtError::Timeout) => {
                let until = now_ms().saturating_add(FAILED_PEER_BACKOFF.as_millis() as u64);
                lock(&self.failed).insert(contact.peer_id, until);
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

    fn is_unsupported(&self, peer: &EndpointId) -> bool {
        still_listed(&self.unsupported, peer)
    }

    fn is_backed_off(&self, peer: &EndpointId) -> bool {
        still_listed(&self.failed, peer)
    }

    fn replica_targets(&self, key: &PodcastDhtKey) -> (bool, Vec<NodeContact>) {
        let mut contacts: Vec<_> = lock(&self.routing)
            .contacts()
            .into_iter()
            .filter(|contact| !self.is_backed_off(&contact.peer_id))
            .collect();
        contacts.push(self.fresh_own_contact());
        contacts.sort_by_key(|contact| distance(contact.node_id.as_bytes(), key.as_bytes()));
        contacts.dedup_by_key(|contact| contact.peer_id);
        contacts.truncate(K);
        let local = contacts
            .iter()
            .any(|contact| contact.peer_id == self.own_contact.peer_id);
        contacts.retain(|contact| contact.peer_id != self.own_contact.peer_id);
        (local, contacts)
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

    async fn learn_response_nodes(&self, nodes: Vec<NodeContact>) {
        for contact in nodes {
            if self.is_unsupported(&contact.peer_id) {
                continue;
            }
            if let Err(error) = self.learn_contact(contact).await {
                debug!(%error, "invalid podcast DHT contact ignored");
            }
        }
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

/// Validates a store batch received from a peer: bounds, every record's
/// signature and lifetime, and that every key belongs to its record.
fn validate_store_batch(
    entries: Vec<PodcastStoreEntry>,
    network_id: &NetworkId,
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

/// Adds verified records found under `key` to the per-owner winners.
fn merge_records(
    winners: &mut HashMap<(PodcastId, EndpointId), SignedPodcastRecord>,
    records: Vec<SignedPodcastRecord>,
    network_id: &NetworkId,
    key: &PodcastDhtKey,
    filter_tokens: &[String],
) {
    let now = now_ms();
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
            _ => {
                winners.insert(owner, record);
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
