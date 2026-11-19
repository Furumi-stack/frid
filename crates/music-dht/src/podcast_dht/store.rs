//! SQLite cache of the podcast-discovery overlay: replicas, routing contacts
//! and this device's own publications.
//!
//! The cache is replaceable federation state, kept in its own database file
//! (like the similarity-routing cache). Every record read from it is decoded
//! and verified again before it is used or served.

use std::collections::HashSet;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use federation_net::{EndpointId, NetworkId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{MusicDhtError, Result};
use crate::podcast::PodcastId;
use crate::routing::{NodeContact, NodeId};

use super::MAX_PODCAST_DHT_RECORDS;
use super::card::{PodcastCardSpec, PodcastDhtKey, SignedPodcastRecord, compare_records};

/// `PRAGMA user_version` of the podcast-discovery cache schema. Version 1
/// (pre-release) stored one payload per key; its replica table is dropped on
/// open because the cache is replaceable. Own publications are kept.
pub const PODCAST_DHT_CACHE_SCHEMA_VERSION: i32 = 2;
/// Maximum records (cards and tombstones) indexed under one key. It equals
/// [`MAX_PODCAST_DHT_RECORDS`], so one `FindValue` response carries every
/// record of a key: nothing admitted is unreachable.
///
/// A full key admits a newcomer by evicting a random placement from its
/// younger half (by first indexing time). The older half is never evicted
/// for newcomers, so cards that were there before a flood stay, and the
/// younger half stays contestable, so nobody who stored first holds the key
/// for good.
pub const MAX_PODCAST_RECORDS_PER_KEY: usize = MAX_PODCAST_DHT_RECORDS;
/// Maximum tombstones indexed under one key, including tombstones that
/// replaced their owner's card in place. Over budget, a random tombstone of
/// another owner from the younger half of the key's tombstones is evicted.
/// It equals [`MAX_PODCAST_RESPONSE_TOMBSTONES`], so every tombstone of a key
/// is served.
pub const MAX_PODCAST_TOMBSTONES_PER_KEY: usize = MAX_PODCAST_RESPONSE_TOMBSTONES;
/// Maximum tombstones in one `FindValue` response; cards fill the rest.
pub const MAX_PODCAST_RESPONSE_TOMBSTONES: usize = 32;
/// Maximum `(podcast_id, publisher)` records one publisher device keeps in a
/// replica. A publisher over it is refused (nothing is evicted for it).
pub const MAX_PODCAST_RECORDS_PER_PUBLISHER: usize = 256;
/// Maximum `(podcast_id, publisher)` records in a replica cache. A full cache
/// admits a new record by evicting random records from the younger half of
/// the cache (by first indexing time), like a full key.
pub const MAX_PODCAST_CACHE_RECORDS: usize = 4_096;
/// Maximum total encoded size of the records in a replica cache, enforced
/// like [`MAX_PODCAST_CACHE_RECORDS`].
pub const MAX_PODCAST_CACHE_BYTES: usize = 32 * 1024 * 1024;
/// Maximum persisted overlay contacts; the least recently seen are dropped.
pub const MAX_PERSISTED_PODCAST_DHT_PEERS: usize = 1_024;
/// Maximum records evicted from the cache to admit one record; when that is
/// not enough room, the record is refused and nothing is evicted.
const MAX_EVICTIONS_PER_RECORD: usize = 16;

const _: () = assert!(
    MAX_PODCAST_RECORDS_PER_KEY <= MAX_PODCAST_DHT_RECORDS
        && MAX_PODCAST_TOMBSTONES_PER_KEY <= MAX_PODCAST_RESPONSE_TOMBSTONES
        && MAX_PODCAST_RESPONSE_TOMBSTONES <= MAX_PODCAST_DHT_RECORDS,
    "every record admitted under a key must fit one FindValue response"
);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS podcast_dht_records (
    podcast_id         TEXT NOT NULL,
    publisher          TEXT NOT NULL,
    tombstone          INTEGER NOT NULL,
    expires_at_ms      INTEGER NOT NULL,
    payload_bytes      INTEGER NOT NULL,
    payload            BLOB NOT NULL,
    PRIMARY KEY (podcast_id, publisher)
);

CREATE INDEX IF NOT EXISTS idx_podcast_dht_records_expiry
    ON podcast_dht_records(expires_at_ms);

CREATE INDEX IF NOT EXISTS idx_podcast_dht_records_publisher
    ON podcast_dht_records(publisher);

CREATE TABLE IF NOT EXISTS podcast_dht_keys (
    dht_key            BLOB NOT NULL,
    podcast_id         TEXT NOT NULL,
    publisher          TEXT NOT NULL,
    tombstone          INTEGER NOT NULL,
    expires_at_ms      INTEGER NOT NULL,
    first_stored_at_ms INTEGER NOT NULL,
    PRIMARY KEY (dht_key, podcast_id, publisher)
);

CREATE INDEX IF NOT EXISTS idx_podcast_dht_keys_owner
    ON podcast_dht_keys(podcast_id, publisher);

CREATE INDEX IF NOT EXISTS idx_podcast_dht_keys_expiry
    ON podcast_dht_keys(expires_at_ms);

CREATE TABLE IF NOT EXISTS podcast_dht_peers (
    peer_id      TEXT PRIMARY KEY,
    node_id      BLOB NOT NULL,
    ticket       TEXT NOT NULL,
    last_seen_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS podcast_dht_local (
    podcast_id    TEXT PRIMARY KEY,
    revision      INTEGER NOT NULL,
    spec          BLOB,
    record        BLOB NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
";

/// Admission limits of a replica cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoreLimits {
    pub per_key: usize,
    pub tombstones_per_key: usize,
    pub per_publisher: usize,
    pub records: usize,
    pub bytes: usize,
}

impl Default for StoreLimits {
    fn default() -> Self {
        Self {
            per_key: MAX_PODCAST_RECORDS_PER_KEY,
            tombstones_per_key: MAX_PODCAST_TOMBSTONES_PER_KEY,
            per_publisher: MAX_PODCAST_RECORDS_PER_PUBLISHER,
            records: MAX_PODCAST_CACHE_RECORDS,
            bytes: MAX_PODCAST_CACHE_BYTES,
        }
    }
}

/// This device's latest publication of one podcast.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalPublication {
    pub podcast_id: PodcastId,
    /// Greatest revision ever used for this podcast by this device.
    pub revision: u64,
    /// Published card content; `None` once unpublished.
    pub spec: Option<PodcastCardSpec>,
    /// Latest signed record (card or tombstone).
    pub record: SignedPodcastRecord,
}

#[derive(Clone)]
pub(crate) struct PodcastDhtDatabase {
    conn: Arc<Mutex<Connection>>,
    network_id: NetworkId,
    limits: StoreLimits,
}

impl std::fmt::Debug for PodcastDhtDatabase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PodcastDhtDatabase")
            .finish_non_exhaustive()
    }
}

impl PodcastDhtDatabase {
    pub async fn open(path: &Path, network_id: NetworkId) -> Result<Self> {
        Self::open_with_limits(path, network_id, StoreLimits::default()).await
    }

    pub async fn open_with_limits(
        path: &Path,
        network_id: NetworkId,
        limits: StoreLimits,
    ) -> Result<Self> {
        let path = path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(&path).map_err(|error| {
                MusicDhtError::Database(format!("failed to open {}: {error}", path.display()))
            })?;
            let version: i32 = conn
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .map_err(database_error)?;
            if version > PODCAST_DHT_CACHE_SCHEMA_VERSION {
                return Err(MusicDhtError::Database(format!(
                    "podcast DHT cache schema {version} is newer than supported {PODCAST_DHT_CACHE_SCHEMA_VERSION}"
                )));
            }
            if version == 1 {
                // Replicas are replaceable; the old layout is dropped.
                conn.execute_batch("DROP TABLE IF EXISTS podcast_dht_records;")
                    .map_err(database_error)?;
            }
            conn.execute_batch(SCHEMA).map_err(|error| {
                MusicDhtError::Database(format!("failed to initialize podcast DHT cache: {error}"))
            })?;
            conn.pragma_update(None, "user_version", PODCAST_DHT_CACHE_SCHEMA_VERSION)
                .map_err(database_error)?;
            Ok(conn)
        })
        .await
        .map_err(|error| MusicDhtError::Database(format!("database task panicked: {error}")))??;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            network_id,
            limits,
        })
    }

    async fn call<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&Connection, &NetworkId, &StoreLimits) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        let network_id = self.network_id;
        let limits = self.limits;
        tokio::task::spawn_blocking(move || {
            let guard = lock(&conn);
            operation(&guard, &network_id, &limits).map_err(database_error)
        })
        .await
        .map_err(|error| MusicDhtError::Database(format!("database task panicked: {error}")))?
    }

    /// Stores records under the given keys. The caller has verified every
    /// record, checked that each key belongs to it and that the sender is its
    /// publisher. Returns the number of `(key, record)` placements written.
    pub async fn store(
        &self,
        entries: Vec<(Vec<PodcastDhtKey>, SignedPodcastRecord)>,
        now_ms: u64,
    ) -> Result<usize> {
        self.call(move |conn, network_id, limits| {
            let transaction = conn.unchecked_transaction()?;
            let mut written = 0;
            for (keys, record) in entries {
                written += store_one(&transaction, network_id, limits, &keys, &record, now_ms)?;
            }
            transaction.commit()?;
            Ok(written)
        })
        .await
    }

    /// Valid, unexpired records under `key`: at most
    /// [`MAX_PODCAST_RESPONSE_TOMBSTONES`] tombstones (always returned so stale
    /// replicas elsewhere can be overruled) and cards containing every token
    /// in `filter_tokens` up to `limit` records in all. Records indexed longest
    /// come first. Cards precede tombstones. With the default limits and
    /// `limit` = [`MAX_PODCAST_DHT_RECORDS`], every record of the key fits.
    pub async fn records_by_key(
        &self,
        key: PodcastDhtKey,
        filter_tokens: Vec<String>,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<SignedPodcastRecord>> {
        self.call(move |conn, network_id, _| {
            let mut statement = conn.prepare(
                "SELECT r.payload FROM podcast_dht_keys k
                 JOIN podcast_dht_records r
                   ON r.podcast_id = k.podcast_id AND r.publisher = k.publisher
                 WHERE k.dht_key = ?1 AND k.tombstone = ?2 AND k.expires_at_ms > ?3
                 ORDER BY k.first_stored_at_ms, k.podcast_id, k.publisher",
            )?;
            let mut select = |tombstone: bool, limit: usize| -> rusqlite::Result<Vec<_>> {
                let mut records = Vec::new();
                if limit == 0 {
                    return Ok(records);
                }
                let rows = statement.query_map(
                    params![key.as_bytes().as_slice(), tombstone, clamp_i64(now_ms)],
                    |row| row.get::<_, Vec<u8>>(0),
                )?;
                for row in rows {
                    let Some(record) = decode_valid(&row?, network_id, now_ms) else {
                        continue;
                    };
                    if record.is_tombstone() != tombstone || !record.belongs_under(&key) {
                        continue;
                    }
                    let matches = record.card().is_none_or(|card| {
                        let tokens = card.index_tokens();
                        filter_tokens.iter().all(|token| tokens.contains(token))
                    });
                    if matches {
                        records.push(record);
                        if records.len() == limit {
                            break;
                        }
                    }
                }
                Ok(records)
            };
            let tombstones = select(true, MAX_PODCAST_RESPONSE_TOMBSTONES.min(limit))?;
            let mut records = select(false, limit - tombstones.len())?;
            records.extend(tombstones);
            Ok(records)
        })
        .await
    }

    /// Removes expired records and their key placements. Returns the number
    /// of records removed.
    pub async fn delete_expired(&self, now_ms: u64) -> Result<usize> {
        self.call(move |conn, _, _| {
            let now = clamp_i64(now_ms);
            conn.execute(
                "DELETE FROM podcast_dht_keys WHERE expires_at_ms <= ?1",
                [now],
            )?;
            conn.execute(
                "DELETE FROM podcast_dht_records WHERE expires_at_ms <= ?1",
                [now],
            )
        })
        .await
    }

    pub async fn upsert_peer(&self, contact: NodeContact) -> Result<()> {
        self.call(move |conn, _, _| {
            conn.execute(
                "INSERT OR REPLACE INTO podcast_dht_peers
                 (peer_id, node_id, ticket, last_seen_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    contact.peer_id.to_string(),
                    contact.node_id.as_bytes().as_slice(),
                    contact.ticket,
                    clamp_i64(contact.last_seen_ms),
                ],
            )?;
            conn.execute(
                "DELETE FROM podcast_dht_peers WHERE peer_id IN (
                     SELECT peer_id FROM podcast_dht_peers
                     ORDER BY last_seen_ms DESC, peer_id
                     LIMIT -1 OFFSET ?1)",
                [MAX_PERSISTED_PODCAST_DHT_PEERS as i64],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn remove_peer(&self, peer: EndpointId) -> Result<()> {
        self.call(move |conn, _, _| {
            conn.execute(
                "DELETE FROM podcast_dht_peers WHERE peer_id = ?1",
                [peer.to_string()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn load_peers(&self) -> Result<Vec<NodeContact>> {
        self.call(move |conn, _, _| {
            let mut statement =
                conn.prepare("SELECT peer_id, ticket, last_seen_ms FROM podcast_dht_peers")?;
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

    pub async fn local_publication(
        &self,
        podcast_id: PodcastId,
    ) -> Result<Option<LocalPublication>> {
        self.call(move |conn, _, _| {
            conn.query_row(
                "SELECT podcast_id, revision, spec, record FROM podcast_dht_local
                 WHERE podcast_id = ?1",
                [podcast_id.as_str()],
                local_from_row,
            )
            .optional()
            .map(Option::flatten)
        })
        .await
    }

    pub async fn local_publications(&self) -> Result<Vec<LocalPublication>> {
        self.call(move |conn, _, _| {
            let mut statement = conn.prepare(
                "SELECT podcast_id, revision, spec, record FROM podcast_dht_local
                 ORDER BY podcast_id",
            )?;
            let rows = statement.query_map([], local_from_row)?;
            let mut out = Vec::new();
            for row in rows {
                if let Some(publication) = row? {
                    out.push(publication);
                }
            }
            Ok(out)
        })
        .await
    }

    pub async fn put_local_publication(
        &self,
        publication: LocalPublication,
        now_ms: u64,
    ) -> Result<()> {
        self.call(move |conn, _, _| {
            let spec = publication
                .spec
                .as_ref()
                .map(postcard::to_stdvec)
                .transpose()
                .map_err(to_sql_error)?;
            let record = postcard::to_stdvec(&publication.record).map_err(to_sql_error)?;
            conn.execute(
                "INSERT OR REPLACE INTO podcast_dht_local
                 (podcast_id, revision, spec, record, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    publication.podcast_id.as_str(),
                    publication.revision as i64,
                    spec,
                    record,
                    clamp_i64(now_ms),
                ],
            )?;
            Ok(())
        })
        .await
    }
}

fn store_one(
    conn: &Connection,
    network_id: &NetworkId,
    limits: &StoreLimits,
    keys: &[PodcastDhtKey],
    record: &SignedPodcastRecord,
    now_ms: u64,
) -> rusqlite::Result<usize> {
    let podcast_id = record.podcast_id().as_str().to_string();
    let publisher = record.publisher().to_string();
    let now = clamp_i64(now_ms);

    // The cached record of this (podcast, publisher), re-verified.
    let cached: Option<(Vec<u8>, i64)> = conn
        .query_row(
            "SELECT payload, payload_bytes FROM podcast_dht_records
             WHERE podcast_id = ?1 AND publisher = ?2",
            params![podcast_id, publisher],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (existing, existing_bytes) = match cached {
        Some((payload, bytes)) => match decode_valid(&payload, network_id, now_ms) {
            Some(existing) => (Some(existing), bytes.max(0) as usize),
            None => {
                delete_owner(conn, &podcast_id, &publisher)?;
                (None, 0)
            }
        },
        None => {
            // Stray placements without a record.
            delete_owner(conn, &podcast_id, &publisher)?;
            (None, 0)
        }
    };

    // Higher revision, tombstone at an equal revision, later republish...
    let replace = match &existing {
        Some(existing) => match compare_records(existing, record) {
            std::cmp::Ordering::Greater => return Ok(0),
            std::cmp::Ordering::Equal => false,
            std::cmp::Ordering::Less => true,
        },
        None => true,
    };
    let payload = postcard::to_stdvec(record).map_err(to_sql_error)?;
    if replace
        && !make_room_for_record(
            conn,
            limits,
            (&podcast_id, &publisher),
            existing.is_none(),
            existing_bytes,
            payload.len(),
            now,
        )?
    {
        return Ok(0);
    }

    let mut indexed: HashSet<[u8; 32]> = HashSet::new();
    {
        let mut statement = conn.prepare(
            "SELECT dht_key FROM podcast_dht_keys WHERE podcast_id = ?1 AND publisher = ?2",
        )?;
        let rows = statement.query_map(params![podcast_id, publisher], |row| {
            row.get::<_, Vec<u8>>(0)
        })?;
        for row in rows {
            if let Ok(key) = <[u8; 32]>::try_from(row?.as_slice()) {
                indexed.insert(key);
            }
        }
    }

    let mut written = 0;
    if replace {
        // Placements the new version no longer belongs under are dropped (a
        // renamed card, or a tombstone that does not list the key); the others
        // now serve the new version.
        let belongs: HashSet<[u8; 32]> = record
            .dht_keys()
            .iter()
            .map(|key| *key.as_bytes())
            .collect();
        let stale: Vec<[u8; 32]> = indexed
            .iter()
            .filter(|key| !belongs.contains(*key))
            .copied()
            .collect();
        for key in stale {
            conn.execute(
                "DELETE FROM podcast_dht_keys
                 WHERE dht_key = ?1 AND podcast_id = ?2 AND publisher = ?3",
                params![key.as_slice(), podcast_id, publisher],
            )?;
            indexed.remove(&key);
        }
        written += conn.execute(
            "UPDATE podcast_dht_keys SET tombstone = ?3, expires_at_ms = ?4
             WHERE podcast_id = ?1 AND publisher = ?2",
            params![
                podcast_id,
                publisher,
                record.is_tombstone(),
                clamp_i64(record.expires_at_ms()),
            ],
        )?;
        // A card converted to a tombstone in place counts against the
        // tombstone budget of each key it stays under, like a new one.
        if record.is_tombstone() {
            for key in &indexed {
                let key = PodcastDhtKey::from_bytes(*key);
                make_room_under_key(conn, limits, &key, true, (&podcast_id, &publisher), now, 0)?;
            }
        }
    }

    // New placements. A tombstone only takes a new place under the podcast-id
    // key: under a text key it may only replace a record of its owner, so
    // nobody can index tombstones under someone else's search terms.
    let id_key = PodcastDhtKey::podcast(network_id, record.podcast_id());
    for key in keys {
        if indexed.contains(key.as_bytes()) {
            continue;
        }
        if record.is_tombstone() && *key != id_key {
            continue;
        }
        if !make_room_under_key(
            conn,
            limits,
            key,
            record.is_tombstone(),
            (&podcast_id, &publisher),
            now,
            1,
        )? {
            continue;
        }
        conn.execute(
            "INSERT INTO podcast_dht_keys
                 (dht_key, podcast_id, publisher, tombstone, expires_at_ms, first_stored_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                key.as_bytes().as_slice(),
                podcast_id,
                publisher,
                record.is_tombstone(),
                clamp_i64(record.expires_at_ms()),
                now,
            ],
        )?;
        indexed.insert(*key.as_bytes());
        written += 1;
    }

    if indexed.is_empty() {
        delete_owner(conn, &podcast_id, &publisher)?;
        return Ok(0);
    }
    if replace {
        conn.execute(
            "INSERT INTO podcast_dht_records
                 (podcast_id, publisher, tombstone, expires_at_ms, payload_bytes, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(podcast_id, publisher) DO UPDATE SET
                 tombstone = excluded.tombstone,
                 expires_at_ms = excluded.expires_at_ms,
                 payload_bytes = excluded.payload_bytes,
                 payload = excluded.payload",
            params![
                podcast_id,
                publisher,
                record.is_tombstone(),
                clamp_i64(record.expires_at_ms()),
                payload.len() as i64,
                payload,
            ],
        )?;
    }
    Ok(written)
}

/// Makes room in the cache for the new version of a record of `owner`.
///
/// * A publisher at [`StoreLimits::per_publisher`] gets no new record.
/// * When the record count (for a new owner) or the byte budget would be
///   exceeded, random records of other owners from the younger half of the
///   cache (by first indexing time) are evicted, at most
///   [`MAX_EVICTIONS_PER_RECORD`]. If that is not enough, nothing is evicted
///   and the record is refused.
fn make_room_for_record(
    conn: &Connection,
    limits: &StoreLimits,
    owner: (&str, &str),
    new_owner: bool,
    previous_bytes: usize,
    new_bytes: usize,
    now: i64,
) -> rusqlite::Result<bool> {
    if new_bytes > limits.bytes {
        return Ok(false);
    }
    if new_owner {
        let own: i64 = conn.query_row(
            "SELECT COUNT(*) FROM podcast_dht_records WHERE publisher = ?1 AND expires_at_ms > ?2",
            params![owner.1, now],
            |row| row.get(0),
        )?;
        if own.max(0) as usize >= limits.per_publisher {
            return Ok(false);
        }
    }
    let (records, bytes): (i64, i64) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM podcast_dht_records
         WHERE expires_at_ms > ?1",
        [now],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let mut records = records.max(0) as usize;
    let mut bytes = (bytes.max(0) as usize).saturating_sub(previous_bytes);
    let fits = |records: usize, bytes: usize| {
        bytes.saturating_add(new_bytes) <= limits.bytes && (!new_owner || records < limits.records)
    };
    if fits(records, bytes) {
        return Ok(true);
    }

    // Candidates: the younger half of the other owners' records, shuffled.
    let others = records.saturating_sub(usize::from(!new_owner));
    let mut statement = conn.prepare(
        "SELECT podcast_id, publisher, payload_bytes FROM (
             SELECT r.podcast_id, r.publisher, r.payload_bytes,
                    (SELECT MIN(k.first_stored_at_ms) FROM podcast_dht_keys k
                      WHERE k.podcast_id = r.podcast_id AND k.publisher = r.publisher) AS since
               FROM podcast_dht_records r
              WHERE r.expires_at_ms > ?1 AND NOT (r.podcast_id = ?2 AND r.publisher = ?3)
              ORDER BY since DESC, random()
              LIMIT ?4)
         ORDER BY random() LIMIT ?5",
    )?;
    let candidates = statement
        .query_map(
            params![
                now,
                owner.0,
                owner.1,
                others.div_ceil(2) as i64,
                MAX_EVICTIONS_PER_RECORD as i64
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut evict = Vec::new();
    for (podcast_id, publisher, payload_bytes) in candidates {
        if fits(records, bytes) {
            break;
        }
        records = records.saturating_sub(1);
        bytes = bytes.saturating_sub(payload_bytes.max(0) as usize);
        evict.push((podcast_id, publisher));
    }
    if !fits(records, bytes) {
        return Ok(false);
    }
    for (podcast_id, publisher) in evict {
        delete_owner(conn, &podcast_id, &publisher)?;
    }
    Ok(true)
}

/// Makes room under `key` for `incoming` more placements (0 when an existing
/// placement of `owner` was converted to a tombstone in place) and returns
/// whether the key has room.
///
/// Over [`StoreLimits::per_key`], or over [`StoreLimits::tombstones_per_key`]
/// for a tombstone, a random placement of another owner from the younger
/// half of the key (by first indexing time; only tombstones for the
/// tombstone budget) is evicted. The older half is never evicted for a
/// newcomer.
fn make_room_under_key(
    conn: &Connection,
    limits: &StoreLimits,
    key: &PodcastDhtKey,
    tombstone: bool,
    owner: (&str, &str),
    now: i64,
    incoming: usize,
) -> rusqlite::Result<bool> {
    for _ in 0..=MAX_EVICTIONS_PER_RECORD {
        let (total, tombstones): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(tombstone), 0) FROM podcast_dht_keys
             WHERE dht_key = ?1 AND expires_at_ms > ?2",
            params![key.as_bytes().as_slice(), now],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let over_tombstones =
            tombstone && tombstones.max(0) as usize + incoming > limits.tombstones_per_key;
        let over_total = total.max(0) as usize + incoming > limits.per_key;
        if !over_tombstones && !over_total {
            return Ok(true);
        }
        let Some((podcast_id, publisher)) =
            young_placement(conn, key, over_tombstones, owner, now)?
        else {
            return Ok(false);
        };
        conn.execute(
            "DELETE FROM podcast_dht_keys
             WHERE dht_key = ?1 AND podcast_id = ?2 AND publisher = ?3",
            params![key.as_bytes().as_slice(), podcast_id, publisher],
        )?;
        let placed: bool = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM podcast_dht_keys
                             WHERE podcast_id = ?1 AND publisher = ?2)",
            params![podcast_id, publisher],
            |row| row.get(0),
        )?;
        if !placed {
            delete_owner(conn, &podcast_id, &publisher)?;
        }
    }
    Ok(false)
}

/// A random unexpired placement of another owner than `owner` from the
/// younger half of `key` (only tombstones with `tombstones_only`).
fn young_placement(
    conn: &Connection,
    key: &PodcastDhtKey,
    tombstones_only: bool,
    owner: (&str, &str),
    now: i64,
) -> rusqlite::Result<Option<(String, String)>> {
    let filter = "dht_key = ?1 AND expires_at_ms > ?2 AND (?3 = 0 OR tombstone = 1)
                  AND NOT (podcast_id = ?4 AND publisher = ?5)";
    let count: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM podcast_dht_keys WHERE {filter}"),
        params![
            key.as_bytes().as_slice(),
            now,
            tombstones_only,
            owner.0,
            owner.1
        ],
        |row| row.get(0),
    )?;
    if count <= 0 {
        return Ok(None);
    }
    conn.query_row(
        &format!(
            "SELECT podcast_id, publisher FROM (
                 SELECT podcast_id, publisher FROM podcast_dht_keys WHERE {filter}
                  ORDER BY first_stored_at_ms DESC, random() LIMIT ?6)
             ORDER BY random() LIMIT 1"
        ),
        params![
            key.as_bytes().as_slice(),
            now,
            tombstones_only,
            owner.0,
            owner.1,
            (count as usize).div_ceil(2) as i64
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
}

fn delete_owner(conn: &Connection, podcast_id: &str, publisher: &str) -> rusqlite::Result<()> {
    conn.execute(
        "DELETE FROM podcast_dht_keys WHERE podcast_id = ?1 AND publisher = ?2",
        params![podcast_id, publisher],
    )?;
    conn.execute(
        "DELETE FROM podcast_dht_records WHERE podcast_id = ?1 AND publisher = ?2",
        params![podcast_id, publisher],
    )?;
    Ok(())
}

fn decode_valid(
    payload: &[u8],
    network_id: &NetworkId,
    now_ms: u64,
) -> Option<SignedPodcastRecord> {
    let record: SignedPodcastRecord = postcard::from_bytes(payload).ok()?;
    record.verify_at(network_id, now_ms).ok()?;
    Some(record)
}

fn local_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Option<LocalPublication>> {
    let podcast_id: String = row.get(0)?;
    let revision: i64 = row.get(1)?;
    let spec: Option<Vec<u8>> = row.get(2)?;
    let record: Vec<u8> = row.get(3)?;
    let Ok(podcast_id) = PodcastId::parse(&podcast_id) else {
        return Ok(None);
    };
    let spec = match spec {
        Some(bytes) => match postcard::from_bytes::<PodcastCardSpec>(&bytes) {
            Ok(spec) => Some(spec),
            Err(_) => return Ok(None),
        },
        None => None,
    };
    let Ok(record) = postcard::from_bytes::<SignedPodcastRecord>(&record) else {
        return Ok(None);
    };
    Ok(Some(LocalPublication {
        podcast_id,
        revision: revision as u64,
        spec,
        record,
    }))
}

fn clamp_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn to_sql_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn database_error(error: rusqlite::Error) -> MusicDhtError {
    MusicDhtError::Database(error.to_string())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
