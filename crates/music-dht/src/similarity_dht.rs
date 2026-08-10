//! Decentralized storage and lookup of signed similarity-LSH peer summaries.
//!
//! This is a schema-independent overlay carried by `music-dht` byte streams.
//! It reuses peer identities, tickets and Kademlia contact shapes, but keeps
//! replaceable routing summaries separate from durable music metadata.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use federation_net::{ByteStream, EndpointId, NetworkId, PeerTicket, StreamAcceptor};
use futures::stream::{self, FuturesUnordered, StreamExt};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::MusicDhtService;
use crate::error::{MusicDhtError, Result};
use crate::record::now_ms;
use crate::routing::{ALPHA, K, MAX_LOOKUP_REQUESTS, NodeContact, NodeId, RoutingTable, distance};
use crate::similarity_lsh::{
    ROUTING_RECORD_TTL, ROUTING_TABLES, SIMILARITY_DHT_ALPN, SIMILARITY_DHT_PROTOCOL_VERSION,
    SignedSimilarityRoute, SimilarityDhtKey, SimilarityRoutePayload, SimilarityRouteSpec,
    build_route_specs, routing_query, routing_signature_distance,
};

/// Maximum signed summaries returned for one coarse bucket.
pub const MAX_SIMILARITY_DHT_RECORDS: usize = 256;
/// Maximum summaries in a single replicated store request.
pub const MAX_SIMILARITY_STORE_RECORDS: usize = 64;
/// Maximum encoded request. Store batches are split below this bound.
pub const MAX_SIMILARITY_DHT_REQUEST_BYTES: usize = 512 * 1024;
/// Maximum encoded response. A full bucket of worst-case records is bounded.
pub const MAX_SIMILARITY_DHT_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
/// Periodic bootstrap interval for discovering upgraded routing peers.
pub const SIMILARITY_BOOTSTRAP_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Signed records are refreshed well before the shared 12-hour TTL.
pub const SIMILARITY_REPUBLISH_INTERVAL: Duration = Duration::from_secs(4 * 60 * 60);
/// Replica expiry sweep interval.
pub const SIMILARITY_EXPIRE_INTERVAL: Duration = Duration::from_secs(60);

const MAX_BOOTSTRAP_PEERS: usize = 64;
const BOOTSTRAP_CONCURRENCY: usize = 8;
const PUBLISH_CONCURRENCY: usize = 8;
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
const RECORD_FIXED_BYTES_ESTIMATE: usize = 192;
const ENTRY_BYTES_ESTIMATE: usize = 36;
const MAX_BATCH_BYTES_ESTIMATE: usize = 384 * 1024;

const STORAGE_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS similarity_dht_records (
    dht_key       BLOB NOT NULL,
    owner_peer_id TEXT NOT NULL,
    issued_at_ms  INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    payload        BLOB NOT NULL,
    PRIMARY KEY (dht_key, owner_peer_id)
);

CREATE INDEX IF NOT EXISTS idx_similarity_dht_records_expiry
    ON similarity_dht_records(expires_at_ms);

CREATE TABLE IF NOT EXISTS similarity_dht_peers (
    peer_id      TEXT PRIMARY KEY,
    node_id      BLOB NOT NULL,
    ticket       TEXT NOT NULL,
    last_seen_ms INTEGER NOT NULL
);
";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoutingRequest {
    version: u16,
    requester: NodeContact,
    operation: RoutingOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RoutingOperation {
    FindNode { target: [u8; 32] },
    FindValue { key: SimilarityDhtKey },
    StoreBatch { records: Vec<SignedSimilarityRoute> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RoutingResponse {
    version: u16,
    ok: bool,
    error: Option<String>,
    nodes: Vec<NodeContact>,
    records: Vec<SignedSimilarityRoute>,
    stored: u32,
}

impl RoutingResponse {
    fn success(nodes: Vec<NodeContact>, records: Vec<SignedSimilarityRoute>, stored: u32) -> Self {
        Self {
            version: SIMILARITY_DHT_PROTOCOL_VERSION,
            ok: true,
            error: None,
            nodes,
            records,
            stored,
        }
    }

    fn error(message: impl Into<String>) -> Self {
        Self {
            version: SIMILARITY_DHT_PROTOCOL_VERSION,
            ok: false,
            error: Some(message.into()),
            nodes: Vec::new(),
            records: Vec::new(),
            stored: 0,
        }
    }
}

/// Statistics for one local-summary synchronization or republish.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SimilarityDhtPublishStats {
    /// Peer summary records built from the local signature set.
    pub records: usize,
    /// Distinct coarse DHT keys published.
    pub keys: usize,
    /// Remote routing nodes that accepted at least one replica.
    pub remote_nodes: usize,
    /// Whether at least one local replica was retained.
    pub local_replica: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerScore {
    distance: u32,
    collisions: u16,
}

#[derive(Clone)]
struct RoutingDatabase {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for RoutingDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RoutingDatabase")
            .finish_non_exhaustive()
    }
}

impl RoutingDatabase {
    async fn open(path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(&path).map_err(|error| {
                MusicDhtError::Database(format!("failed to open {}: {error}", path.display()))
            })?;
            conn.execute_batch(STORAGE_SCHEMA).map_err(|error| {
                MusicDhtError::Database(format!(
                    "failed to initialize similarity DHT storage: {error}"
                ))
            })?;
            Ok(conn)
        })
        .await
        .map_err(|error| MusicDhtError::Database(format!("database task panicked: {error}")))??;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    async fn call<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        tokio::task::spawn_blocking(move || {
            let guard = lock(&conn);
            operation(&guard).map_err(|error| MusicDhtError::Database(error.to_string()))
        })
        .await
        .map_err(|error| MusicDhtError::Database(format!("database task panicked: {error}")))?
    }

    async fn store_records(&self, records: Vec<SignedSimilarityRoute>) -> Result<Vec<bool>> {
        self.call(move |conn| {
            let transaction = conn.unchecked_transaction()?;
            let mut stored = Vec::with_capacity(records.len());
            for record in records {
                let key = record
                    .key()
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                let owner = record.payload.owner.to_string();
                let issued_at_ms = record.payload.issued_at_ms;
                let expires_at_ms =
                    issued_at_ms.saturating_add(ROUTING_RECORD_TTL.as_millis() as u64);
                let payload = postcard::to_stdvec(&record)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                let changed = transaction.execute(
                    "INSERT INTO similarity_dht_records (
                         dht_key, owner_peer_id, issued_at_ms, expires_at_ms, payload
                     ) VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(dht_key, owner_peer_id) DO UPDATE SET
                         issued_at_ms = excluded.issued_at_ms,
                         expires_at_ms = excluded.expires_at_ms,
                         payload = excluded.payload
                     WHERE excluded.issued_at_ms >= similarity_dht_records.issued_at_ms",
                    params![
                        key.as_bytes().as_slice(),
                        owner,
                        issued_at_ms as i64,
                        expires_at_ms as i64,
                        payload,
                    ],
                )? > 0;
                stored.push(changed);
            }
            transaction.commit()?;
            Ok(stored)
        })
        .await
    }

    async fn records_by_key(
        &self,
        key: SimilarityDhtKey,
        at_ms: u64,
    ) -> Result<Vec<SignedSimilarityRoute>> {
        self.call(move |conn| {
            let mut statement = conn.prepare(
                "SELECT payload FROM similarity_dht_records
                 WHERE dht_key = ?1 AND expires_at_ms > ?2
                 ORDER BY expires_at_ms DESC, owner_peer_id
                 LIMIT ?3",
            )?;
            let rows = statement.query_map(
                params![
                    key.as_bytes().as_slice(),
                    at_ms as i64,
                    MAX_SIMILARITY_DHT_RECORDS as i64
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            let mut records = Vec::new();
            for row in rows {
                if let Ok(record) = postcard::from_bytes::<SignedSimilarityRoute>(&row?) {
                    records.push(record);
                }
            }
            Ok(records)
        })
        .await
    }

    async fn delete_expired(&self, at_ms: u64) -> Result<usize> {
        self.call(move |conn| {
            conn.execute(
                "DELETE FROM similarity_dht_records WHERE expires_at_ms <= ?1",
                [at_ms as i64],
            )
        })
        .await
    }

    async fn upsert_peer(&self, contact: NodeContact) -> Result<()> {
        self.call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO similarity_dht_peers
                 (peer_id, node_id, ticket, last_seen_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    contact.peer_id.to_string(),
                    contact.node_id.as_bytes().as_slice(),
                    contact.ticket,
                    contact.last_seen_ms as i64,
                ],
            )?;
            Ok(())
        })
        .await
    }

    async fn load_peers(&self) -> Result<Vec<NodeContact>> {
        self.call(move |conn| {
            let mut statement =
                conn.prepare("SELECT peer_id, ticket, last_seen_ms FROM similarity_dht_peers")?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            let mut peers = Vec::new();
            for row in rows {
                let (peer, ticket, last_seen_ms) = row?;
                let Ok(peer_id) = EndpointId::from_str(&peer) else {
                    continue;
                };
                peers.push(NodeContact {
                    node_id: NodeId::from_endpoint(&peer_id),
                    peer_id,
                    ticket,
                    last_seen_ms: last_seen_ms.max(0) as u64,
                });
            }
            Ok(peers)
        })
        .await
    }
}

/// Reusable similarity-routing DHT attached to a [`MusicDhtService`].
pub struct SimilarityDht {
    service: Arc<MusicDhtService>,
    database: RoutingDatabase,
    network_id: NetworkId,
    own_contact: NodeContact,
    routing: Mutex<RoutingTable>,
    local_specs: RwLock<Vec<SimilarityRouteSpec>>,
    publish_gate: Semaphore,
}

impl std::fmt::Debug for SimilarityDht {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SimilarityDht")
            .field("endpoint_id", &self.own_contact.peer_id)
            .field("known_peers", &lock(&self.routing).len())
            .finish_non_exhaustive()
    }
}

impl SimilarityDht {
    /// Opens the replaceable routing cache and restores previously learned
    /// similarity-routing contacts. The caller owns the serving and
    /// maintenance tasks so they share the application's shutdown lifecycle.
    pub async fn open(
        service: Arc<MusicDhtService>,
        database_path: impl AsRef<Path>,
    ) -> Result<Arc<Self>> {
        let database = RoutingDatabase::open(database_path.as_ref()).await?;
        let network_id = service.network_id();
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
            local_specs: RwLock::new(Vec::new()),
            publish_gate: Semaphore::new(1),
        }))
    }

    /// Number of currently known peers that implement the routing overlay.
    pub fn known_peers(&self) -> usize {
        lock(&self.routing).len()
    }

    /// Serves authenticated routing requests until `acceptor` closes.
    pub async fn serve(self: Arc<Self>, mut acceptor: StreamAcceptor) {
        while let Some(stream) = acceptor.accept().await {
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                let peer = stream.peer_id;
                if let Err(error) = this.serve_one(stream).await {
                    debug!(%peer, %error, "similarity DHT request failed");
                }
            });
        }
    }

    /// Periodically discovers upgraded peers, republishes local summaries and
    /// removes expired replicas. The first bootstrap runs immediately.
    pub async fn maintenance(self: Arc<Self>) {
        let mut bootstrap = tokio::time::interval(SIMILARITY_BOOTSTRAP_INTERVAL);
        let mut republish = tokio::time::interval(SIMILARITY_REPUBLISH_INTERVAL);
        let mut expire = tokio::time::interval(SIMILARITY_EXPIRE_INTERVAL);
        bootstrap.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        republish.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        expire.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // `republish` must wait for either a local sync or a full interval.
        republish.tick().await;
        loop {
            tokio::select! {
                _ = bootstrap.tick() => {
                    let before = self.known_peers();
                    if let Err(error) = self.bootstrap().await {
                        debug!(%error, "similarity DHT bootstrap failed");
                    } else {
                        let has_local_specs = !read(&self.local_specs).is_empty();
                        if self.known_peers() > before && has_local_specs
                            && let Err(error) = self.publish_local().await
                        {
                            warn!(%error, "similarity DHT publication after bootstrap failed");
                        }
                    }
                }
                _ = republish.tick() => {
                    let has_local_specs = !read(&self.local_specs).is_empty();
                    if has_local_specs
                        && let Err(error) = self.publish_local().await
                    {
                        warn!(%error, "similarity DHT republish failed");
                    }
                }
                _ = expire.tick() => {
                    match self.database.delete_expired(now_ms()).await {
                        Ok(0) => {}
                        Ok(count) => debug!(count, "expired similarity DHT replicas removed"),
                        Err(error) => warn!(%error, "similarity DHT expiry sweep failed"),
                    }
                }
            }
        }
    }

    /// Replaces the locally published active profile with summaries built
    /// from all local routing signatures, then publishes them immediately.
    pub async fn sync_local_signatures(
        &self,
        profile_id: String,
        signatures: Vec<[u8; 32]>,
    ) -> Result<SimilarityDhtPublishStats> {
        let specs =
            tokio::task::spawn_blocking(move || build_route_specs(&profile_id, &signatures))
                .await
                .map_err(|error| {
                    MusicDhtError::Protocol(format!("LSH build task panicked: {error}"))
                })??;
        *write(&self.local_specs) = specs;
        self.bootstrap().await?;
        self.publish_local().await
    }

    /// Stops advertising local summaries. Existing replicas disappear by the
    /// shared TTL; no network-wide delete broadcast is required.
    pub fn clear_local_signatures(&self) {
        write(&self.local_specs).clear();
    }

    /// Finds and ranks self-contained routes to peers likely to own tracks
    /// close to `vector`.
    pub async fn find_peers(
        &self,
        profile_id: &str,
        vector: &[f32],
        limit: usize,
    ) -> Result<Vec<PeerTicket>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let query = routing_query(vector)?;
        if self.known_peers() == 0 {
            self.bootstrap().await?;
        }
        let mut lookups = Vec::with_capacity(ROUTING_TABLES * 2);
        let mut table_probes = Vec::with_capacity(ROUTING_TABLES);
        for table in 0..ROUTING_TABLES as u8 {
            let probes = query.probes(profile_id, table)?;
            for primary_bucket in &probes.primary_buckets {
                lookups.push((
                    table,
                    *primary_bucket,
                    SimilarityDhtKey::derive(&self.network_id, profile_id, table, *primary_bucket)?,
                ));
            }
            table_probes.push(probes);
        }

        let outcomes =
            stream::iter(lookups.into_iter().map(|(table, primary, key)| async move {
                (table, primary, self.lookup(key).await)
            }))
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await;

        let mut scores: HashMap<EndpointId, (PeerScore, String, u64)> = HashMap::new();
        for (table, primary, outcome) in outcomes {
            let records = match outcome {
                Ok(records) => records,
                Err(error) => {
                    debug!(table, primary, %error, "similarity DHT bucket lookup failed");
                    continue;
                }
            };
            let probes = &table_probes[table as usize];
            for record in records {
                let payload = &record.payload;
                if payload.owner == self.own_contact.peer_id
                    || payload.profile_id != profile_id
                    || payload.table != table
                    || payload.primary_bucket != primary
                {
                    continue;
                }
                let representatives = probes.suffix_buckets.iter().filter_map(|suffix| {
                    payload
                        .entries
                        .binary_search_by_key(suffix, |entry| entry.suffix)
                        .ok()
                        .map(|index| payload.entries[index].representative)
                });
                for representative in representatives {
                    let candidate_distance =
                        routing_signature_distance(query.signature(), &representative);
                    scores
                        .entry(payload.owner)
                        .and_modify(|(score, ticket, issued_at_ms)| {
                            score.distance = score.distance.min(candidate_distance);
                            score.collisions = score.collisions.saturating_add(1);
                            if payload.issued_at_ms > *issued_at_ms {
                                ticket.clone_from(&payload.owner_ticket);
                                *issued_at_ms = payload.issued_at_ms;
                            }
                        })
                        .or_insert((
                            PeerScore {
                                distance: candidate_distance,
                                collisions: 1,
                            },
                            payload.owner_ticket.clone(),
                            payload.issued_at_ms,
                        ));
                }
            }
        }
        let mut peers: Vec<_> = scores.into_iter().collect();
        peers.sort_by_key(|(peer, (score, _, _))| {
            (score.distance, std::cmp::Reverse(score.collisions), *peer)
        });
        peers.truncate(limit);
        peers
            .into_iter()
            .map(|(_, (_, ticket, _))| {
                PeerTicket::from_str(&ticket)
                    .map_err(|error| MusicDhtError::InvalidTicket(error.to_string()))
            })
            .collect()
    }

    async fn serve_one(&self, mut stream: ByteStream) -> Result<()> {
        let authenticated_peer = stream.peer_id;
        let request: RoutingRequest =
            read_postcard(&mut stream.recv, MAX_SIMILARITY_DHT_REQUEST_BYTES).await?;
        let response = match self.handle_request(authenticated_peer, request).await {
            Ok(response) => response,
            Err(error) => RoutingResponse::error(error.to_string()),
        };
        write_postcard(
            &mut stream.send,
            &response,
            MAX_SIMILARITY_DHT_RESPONSE_BYTES,
        )
        .await?;
        stream.send.finish().map_err(network_error)?;
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.send.stopped()).await;
        Ok(())
    }

    async fn handle_request(
        &self,
        authenticated_peer: EndpointId,
        request: RoutingRequest,
    ) -> Result<RoutingResponse> {
        if request.version != SIMILARITY_DHT_PROTOCOL_VERSION {
            return Err(protocol_error("unsupported similarity DHT request version"));
        }
        if request.requester.peer_id != authenticated_peer {
            return Err(protocol_error("similarity DHT requester identity mismatch"));
        }
        self.learn_contact(request.requester).await?;
        match request.operation {
            RoutingOperation::FindNode { target } => Ok(RoutingResponse::success(
                self.closest_contacts(&target, K),
                Vec::new(),
                0,
            )),
            RoutingOperation::FindValue { key } => {
                let records = self.database.records_by_key(key, now_ms()).await?;
                Ok(RoutingResponse::success(
                    self.closest_contacts(key.as_bytes(), K),
                    records,
                    0,
                ))
            }
            RoutingOperation::StoreBatch { records } => {
                if records.is_empty() || records.len() > MAX_SIMILARITY_STORE_RECORDS {
                    return Err(protocol_error("invalid similarity DHT store batch"));
                }
                let now = now_ms();
                for record in &records {
                    validate_record(record, &self.network_id, now)?;
                }
                let stored = self
                    .database
                    .store_records(records)
                    .await?
                    .into_iter()
                    .filter(|stored| *stored)
                    .count();
                Ok(RoutingResponse::success(
                    Vec::new(),
                    Vec::new(),
                    stored as u32,
                ))
            }
        }
    }

    async fn bootstrap(&self) -> Result<()> {
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
            contact.peer_id != self.own_contact.peer_id && seen.insert(contact.peer_id)
        });
        candidates.truncate(MAX_BOOTSTRAP_PEERS);
        let target = self.own_contact.node_id.as_bytes().to_owned();
        let responses = stream::iter(candidates.into_iter().map(|contact| async move {
            let response = self
                .exchange(&contact, RoutingOperation::FindNode { target })
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
                    debug!(peer = %contact.peer_id, %error, "similarity DHT bootstrap peer unavailable");
                    continue;
                }
            };
            self.learn_contact(contact).await?;
            self.learn_response_nodes(response.nodes).await;
        }
        Ok(())
    }

    async fn publish_local(&self) -> Result<SimilarityDhtPublishStats> {
        let _permit = self
            .publish_gate
            .acquire()
            .await
            .map_err(|_| protocol_error("similarity DHT publisher closed"))?;
        let specs = read(&self.local_specs).clone();
        if specs.is_empty() {
            return Ok(SimilarityDhtPublishStats::default());
        }
        let issued_at_ms = now_ms();
        let mut stats = SimilarityDhtPublishStats {
            records: specs.len(),
            keys: specs.len(),
            ..SimilarityDhtPublishStats::default()
        };
        let mut accepted_peers = HashSet::new();
        for wave in specs.chunks(64) {
            let mut local = Vec::new();
            let mut remote: HashMap<EndpointId, (NodeContact, Vec<SignedSimilarityRoute>)> =
                HashMap::new();
            for spec in wave {
                let payload = SimilarityRoutePayload::from_spec(
                    spec.clone(),
                    self.network_id,
                    self.own_contact.peer_id,
                    self.own_contact.ticket.clone(),
                    issued_at_ms,
                )?;
                let record = SignedSimilarityRoute::sign(payload, |bytes| {
                    self.service.sign_identity(bytes)
                })?;
                let key = record.key()?;
                let (store_local, contacts) = self.replica_targets(&key);
                if store_local {
                    local.push(record.clone());
                }
                for contact in contacts {
                    remote
                        .entry(contact.peer_id)
                        .or_insert_with(|| (contact, Vec::new()))
                        .1
                        .push(record.clone());
                }
            }
            if !local.is_empty() {
                self.database.store_records(local).await?;
                stats.local_replica = true;
            }
            let sends = stream::iter(remote.into_values().map(|(contact, records)| async move {
                let accepted = self.store_on_peer(&contact, records).await.unwrap_or(false);
                (contact.peer_id, accepted)
            }))
            .buffer_unordered(PUBLISH_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
            for (peer, accepted) in sends {
                if accepted {
                    accepted_peers.insert(peer);
                }
            }
        }
        stats.remote_nodes = accepted_peers.len();
        info!(
            records = stats.records,
            nodes = stats.remote_nodes,
            "similarity DHT summaries published"
        );
        Ok(stats)
    }

    async fn store_on_peer(
        &self,
        contact: &NodeContact,
        records: Vec<SignedSimilarityRoute>,
    ) -> Result<bool> {
        let mut accepted = false;
        for batch in chunk_store_records(records) {
            let response = self
                .exchange(contact, RoutingOperation::StoreBatch { records: batch })
                .await?;
            accepted |= response.stored > 0;
        }
        Ok(accepted)
    }

    async fn lookup(&self, key: SimilarityDhtKey) -> Result<Vec<SignedSimilarityRoute>> {
        let local = self.database.records_by_key(key, now_ms()).await?;
        let mut records: HashMap<EndpointId, SignedSimilarityRoute> = HashMap::new();
        merge_records(&mut records, local, &self.network_id, key);

        let mut candidates = lock(&self.routing).closest(key.as_bytes(), K);
        let mut known: HashSet<_> = candidates.iter().map(|contact| contact.peer_id).collect();
        let mut queried = HashSet::new();
        let mut sent = 0usize;
        let deadline = tokio::time::Instant::now() + LOOKUP_TIMEOUT;
        while tokio::time::Instant::now() < deadline && sent < MAX_LOOKUP_REQUESTS {
            candidates.sort_by_key(|contact| distance(contact.node_id.as_bytes(), key.as_bytes()));
            let batch: Vec<_> = candidates
                .iter()
                .filter(|contact| !queried.contains(&contact.peer_id))
                .take(ALPHA.min(MAX_LOOKUP_REQUESTS - sent))
                .cloned()
                .collect();
            if batch.is_empty() {
                break;
            }
            sent += batch.len();
            queried.extend(batch.iter().map(|contact| contact.peer_id));
            let mut futures: FuturesUnordered<_> = batch
                .iter()
                .map(|contact| async move {
                    (
                        contact,
                        tokio::time::timeout(
                            REQUEST_TIMEOUT,
                            self.exchange(contact, RoutingOperation::FindValue { key }),
                        )
                        .await,
                    )
                })
                .collect();
            let mut found_in_round = false;
            while let Ok(Some((_, response))) =
                tokio::time::timeout_at(deadline, futures.next()).await
            {
                let Ok(Ok(response)) = response else {
                    continue;
                };
                if !response.records.is_empty() {
                    found_in_round = true;
                }
                merge_records(&mut records, response.records, &self.network_id, key);
                for node in response.nodes {
                    if validate_contact(&node, &self.network_id, Some(self.own_contact.peer_id))
                        .is_ok()
                        && known.insert(node.peer_id)
                    {
                        self.learn_contact(node.clone()).await?;
                        candidates.push(node);
                    }
                }
            }
            if found_in_round {
                break;
            }
        }
        Ok(records.into_values().collect())
    }

    async fn exchange(
        &self,
        contact: &NodeContact,
        operation: RoutingOperation,
    ) -> Result<RoutingResponse> {
        validate_contact(contact, &self.network_id, Some(self.own_contact.peer_id))?;
        let ticket = PeerTicket::from_str(&contact.ticket)
            .map_err(|error| MusicDhtError::InvalidTicket(error.to_string()))?;
        let mut stream = self
            .service
            .open_stream_to(&ticket, SIMILARITY_DHT_ALPN)
            .await?;
        let request = RoutingRequest {
            version: SIMILARITY_DHT_PROTOCOL_VERSION,
            requester: self.fresh_own_contact(),
            operation,
        };
        write_postcard(&mut stream.send, &request, MAX_SIMILARITY_DHT_REQUEST_BYTES).await?;
        stream.send.finish().map_err(network_error)?;
        let response: RoutingResponse =
            read_postcard(&mut stream.recv, MAX_SIMILARITY_DHT_RESPONSE_BYTES).await?;
        validate_response(&response)?;
        if !response.ok {
            return Err(protocol_error(
                response
                    .error
                    .as_deref()
                    .unwrap_or("routing request refused"),
            ));
        }
        Ok(response)
    }

    fn replica_targets(&self, key: &SimilarityDhtKey) -> (bool, Vec<NodeContact>) {
        let mut contacts = lock(&self.routing).contacts();
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
            if let Err(error) = self.learn_contact(contact).await {
                debug!(%error, "invalid similarity DHT contact ignored");
            }
        }
    }

    async fn learn_contact(&self, mut contact: NodeContact) -> Result<()> {
        validate_contact(&contact, &self.network_id, Some(self.own_contact.peer_id))?;
        contact.node_id = NodeId::from_endpoint(&contact.peer_id);
        // A remote peer does not get to extend its own lifetime by supplying
        // an arbitrary future timestamp. Successful authenticated contact is
        // the observation that matters locally.
        contact.last_seen_ms = now_ms();
        let is_new = lock(&self.routing).upsert(contact.clone());
        self.database.upsert_peer(contact.clone()).await?;
        if is_new {
            debug!(peer = %contact.peer_id, "learned similarity DHT peer");
        }
        Ok(())
    }
}

fn validate_record(
    record: &SignedSimilarityRoute,
    network_id: &NetworkId,
    at_ms: u64,
) -> Result<()> {
    record.verify(network_id)?;
    let future_limit = at_ms.saturating_add(MAX_CLOCK_SKEW.as_millis() as u64);
    let expiry = record
        .payload
        .issued_at_ms
        .saturating_add(ROUTING_RECORD_TTL.as_millis() as u64);
    if record.payload.issued_at_ms > future_limit || expiry <= at_ms {
        return Err(protocol_error("invalid similarity DHT record time"));
    }
    Ok(())
}

fn validate_contact(
    contact: &NodeContact,
    network_id: &NetworkId,
    excluded: Option<EndpointId>,
) -> Result<()> {
    if excluded == Some(contact.peer_id)
        || contact.node_id != NodeId::from_endpoint(&contact.peer_id)
        || contact.ticket.len() > 16 * 1024
    {
        return Err(protocol_error("invalid similarity DHT contact"));
    }
    let ticket = PeerTicket::from_str(&contact.ticket)
        .map_err(|error| MusicDhtError::InvalidTicket(error.to_string()))?;
    if ticket.endpoint_id() != contact.peer_id || &ticket.network_id != network_id {
        return Err(protocol_error("similarity DHT contact ticket mismatch"));
    }
    Ok(())
}

fn validate_response(response: &RoutingResponse) -> Result<()> {
    if response.version != SIMILARITY_DHT_PROTOCOL_VERSION
        || response.nodes.len() > K
        || response.records.len() > MAX_SIMILARITY_DHT_RECORDS
        || response
            .error
            .as_ref()
            .is_some_and(|error| error.len() > 1024)
        || (response.ok && response.error.is_some())
        || (!response.ok && response.error.is_none())
    {
        return Err(protocol_error("invalid similarity DHT response"));
    }
    Ok(())
}

fn merge_records(
    destination: &mut HashMap<EndpointId, SignedSimilarityRoute>,
    records: Vec<SignedSimilarityRoute>,
    network_id: &NetworkId,
    expected_key: SimilarityDhtKey,
) {
    let at_ms = now_ms();
    for record in records {
        if validate_record(&record, network_id, at_ms).is_err()
            || record.key().ok() != Some(expected_key)
        {
            continue;
        }
        let owner = record.payload.owner;
        match destination.get(&owner) {
            Some(existing) if existing.payload.issued_at_ms > record.payload.issued_at_ms => {}
            _ => {
                destination.insert(owner, record);
            }
        }
    }
}

fn chunk_store_records(records: Vec<SignedSimilarityRoute>) -> Vec<Vec<SignedSimilarityRoute>> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;
    for record in records {
        let estimate = RECORD_FIXED_BYTES_ESTIMATE
            + record.payload.owner_ticket.len()
            + record.payload.entries.len() * ENTRY_BYTES_ESTIMATE;
        if !current.is_empty()
            && (current.len() >= MAX_SIMILARITY_STORE_RECORDS
                || current_bytes.saturating_add(estimate) > MAX_BATCH_BYTES_ESTIMATE)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(estimate);
        current.push(record);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

async fn write_postcard<W: AsyncWriteExt + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    maximum: usize,
) -> Result<()> {
    let payload = postcard::to_stdvec(value).map_err(protocol_error)?;
    if payload.len() > maximum {
        return Err(protocol_error("similarity DHT message is too large"));
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
        return Err(protocol_error("invalid similarity DHT message size"));
    }
    postcard::from_bytes(&payload).map_err(protocol_error)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

fn protocol_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Protocol(error.to_string())
}

fn network_error(error: impl std::fmt::Display) -> MusicDhtError {
    MusicDhtError::Network(error.to_string())
}

#[cfg(test)]
mod tests {
    use federation_net::{
        EndpointAddr, PROTOCOL_VERSION, PeerTicket, SchemaId, SecretKey, TICKET_VERSION,
    };

    use super::*;
    use crate::SimilarityRouteEntry;
    use crate::similarity_lsh::{SimilarityRoutePayload, build_route_specs};

    fn ticket(network: NetworkId, owner: EndpointId) -> String {
        PeerTicket {
            ticket_version: TICKET_VERSION,
            protocol_version: PROTOCOL_VERSION,
            network_id: network,
            schema_id: SchemaId::from_name("similarity-test"),
            endpoint_addr: EndpointAddr::new(owner),
        }
        .to_string()
    }

    #[test]
    fn store_batches_obey_count_and_estimated_byte_bounds() {
        let network = NetworkId::from_name("batch");
        let owner = SecretKey::from_bytes(&[8; 32]);
        let spec = SimilarityRouteSpec {
            profile_id: "sim1:test".into(),
            table: 0,
            primary_bucket: 0,
            entries: (0..512)
                .map(|suffix| SimilarityRouteEntry {
                    suffix,
                    representative: [suffix as u8; 32],
                })
                .collect(),
        };
        let records: Vec<_> = (0..100)
            .map(|issued| {
                let payload = SimilarityRoutePayload::from_spec(
                    spec.clone(),
                    network,
                    owner.public(),
                    ticket(network, owner.public()),
                    issued + 1,
                )
                .unwrap();
                SignedSimilarityRoute::sign(payload, |bytes| owner.sign(bytes)).unwrap()
            })
            .collect();
        let chunks = chunk_store_records(records);
        assert!(chunks.len() > 1);
        assert!(
            chunks
                .iter()
                .all(|chunk| chunk.len() <= MAX_SIMILARITY_STORE_RECORDS)
        );
    }

    #[tokio::test]
    async fn routing_database_keeps_newest_signed_owner_record() {
        let temp = tempfile::tempdir().unwrap();
        let database = RoutingDatabase::open(&temp.path().join("routing.sqlite3"))
            .await
            .unwrap();
        let network = NetworkId::from_name("db");
        let owner = SecretKey::from_bytes(&[9; 32]);
        let spec = build_route_specs("sim1:test", &[[0x11; 32]])
            .unwrap()
            .remove(0);
        let make = |issued_at_ms| {
            let payload = SimilarityRoutePayload::from_spec(
                spec.clone(),
                network,
                owner.public(),
                ticket(network, owner.public()),
                issued_at_ms,
            )
            .unwrap();
            SignedSimilarityRoute::sign(payload, |bytes| owner.sign(bytes)).unwrap()
        };
        let newer = make(now_ms());
        let older = make(now_ms().saturating_sub(1));
        let key = newer.key().unwrap();
        database.store_records(vec![newer.clone()]).await.unwrap();
        database.store_records(vec![older]).await.unwrap();
        assert_eq!(database.records_by_key(key, 1).await.unwrap(), vec![newer]);
    }
}
