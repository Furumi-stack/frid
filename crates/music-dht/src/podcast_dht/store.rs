//! SQLite cache of the podcast-discovery overlay: replicas, routing contacts
//! and this device's own publications.
//!
//! The cache is replaceable federation state, kept in its own database file
//! (like the similarity-routing cache). Every record read from it is decoded
//! and verified again before it is used or served.

use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use federation_net::{EndpointId, NetworkId};
use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{MusicDhtError, Result};
use crate::podcast::PodcastId;
use crate::routing::{NodeContact, NodeId};

use super::card::{
    PodcastCardSpec, PodcastDhtKey, SignedPodcastRecord, compare_records, supersedes_everywhere,
};

/// `PRAGMA user_version` of the podcast-discovery cache schema.
pub const PODCAST_DHT_CACHE_SCHEMA_VERSION: i32 = 1;
/// Maximum distinct `(podcast_id, publisher)` records cached under one key.
/// When full, the record expiring first is evicted if the new one outlives it.
pub const MAX_PODCAST_RECORDS_PER_KEY: usize = 256;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS podcast_dht_records (
    dht_key       BLOB NOT NULL,
    podcast_id    TEXT NOT NULL,
    publisher     TEXT NOT NULL,
    tombstone     INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    payload       BLOB NOT NULL,
    PRIMARY KEY (dht_key, podcast_id, publisher)
);

CREATE INDEX IF NOT EXISTS idx_podcast_dht_records_expiry
    ON podcast_dht_records(expires_at_ms);

CREATE INDEX IF NOT EXISTS idx_podcast_dht_records_owner
    ON podcast_dht_records(podcast_id, publisher);

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
        })
    }

    async fn call<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&Connection, &NetworkId) -> rusqlite::Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let conn = Arc::clone(&self.conn);
        let network_id = self.network_id;
        tokio::task::spawn_blocking(move || {
            let guard = lock(&conn);
            operation(&guard, &network_id).map_err(database_error)
        })
        .await
        .map_err(|error| MusicDhtError::Database(format!("database task panicked: {error}")))?
    }

    /// Stores records under the given keys. The caller has verified every
    /// record and checked that each key belongs to it. Returns the number of
    /// `(key, record)` rows written.
    pub async fn store(
        &self,
        entries: Vec<(Vec<PodcastDhtKey>, SignedPodcastRecord)>,
        now_ms: u64,
    ) -> Result<usize> {
        self.call(move |conn, network_id| {
            let transaction = conn.unchecked_transaction()?;
            let mut written = 0;
            for (keys, record) in entries {
                written += store_one(&transaction, network_id, &keys, &record, now_ms)?;
            }
            transaction.commit()?;
            Ok(written)
        })
        .await
    }

    /// Valid, unexpired records under `key`, newest expiry first. Cards must
    /// contain every token in `filter_tokens`; tombstones are always
    /// returned so stale replicas elsewhere can be overruled.
    pub async fn records_by_key(
        &self,
        key: PodcastDhtKey,
        filter_tokens: Vec<String>,
        now_ms: u64,
        limit: usize,
    ) -> Result<Vec<SignedPodcastRecord>> {
        self.call(move |conn, network_id| {
            let mut statement = conn.prepare(
                "SELECT payload FROM podcast_dht_records
                 WHERE dht_key = ?1 AND expires_at_ms > ?2
                 ORDER BY expires_at_ms DESC, podcast_id, publisher",
            )?;
            let rows = statement.query_map(
                params![key.as_bytes().as_slice(), clamp_i64(now_ms)],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            let mut records = Vec::new();
            for row in rows {
                let Some(record) = decode_valid(&row?, network_id, now_ms) else {
                    continue;
                };
                if !record.belongs_under(&key) {
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
        })
        .await
    }

    pub async fn delete_expired(&self, now_ms: u64) -> Result<usize> {
        self.call(move |conn, _| {
            conn.execute(
                "DELETE FROM podcast_dht_records WHERE expires_at_ms <= ?1",
                [clamp_i64(now_ms)],
            )
        })
        .await
    }

    pub async fn upsert_peer(&self, contact: NodeContact) -> Result<()> {
        self.call(move |conn, _| {
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
            Ok(())
        })
        .await
    }

    pub async fn remove_peer(&self, peer: EndpointId) -> Result<()> {
        self.call(move |conn, _| {
            conn.execute(
                "DELETE FROM podcast_dht_peers WHERE peer_id = ?1",
                [peer.to_string()],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn load_peers(&self) -> Result<Vec<NodeContact>> {
        self.call(move |conn, _| {
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
        self.call(move |conn, _| {
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
        self.call(move |conn, _| {
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
        self.call(move |conn, _| {
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
    keys: &[PodcastDhtKey],
    record: &SignedPodcastRecord,
    now_ms: u64,
) -> rusqlite::Result<usize> {
    let podcast_id = record.podcast_id().as_str().to_string();
    let publisher = record.publisher().to_string();

    // Every cached version of this (podcast, publisher), re-verified.
    let mut existing: HashMap<[u8; 32], SignedPodcastRecord> = HashMap::new();
    let mut invalid_keys = Vec::new();
    {
        let mut statement = conn.prepare(
            "SELECT dht_key, payload FROM podcast_dht_records
             WHERE podcast_id = ?1 AND publisher = ?2",
        )?;
        let rows = statement.query_map(params![podcast_id, publisher], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;
        for row in rows {
            let (key, payload) = row?;
            let decoded = decode_valid(&payload, network_id, now_ms);
            match (<[u8; 32]>::try_from(key.as_slice()), decoded) {
                (Ok(key_bytes), Some(cached)) => {
                    existing.insert(key_bytes, cached);
                }
                _ => invalid_keys.push(key),
            }
        }
    }
    for key in invalid_keys {
        conn.execute(
            "DELETE FROM podcast_dht_records
             WHERE dht_key = ?1 AND podcast_id = ?2 AND publisher = ?3",
            params![key, podcast_id, publisher],
        )?;
    }

    // A tombstone or a newer revision cached under any key wins everywhere.
    if existing
        .values()
        .any(|cached| supersedes_everywhere(cached, record))
    {
        return Ok(0);
    }

    let payload = postcard::to_stdvec(record).map_err(to_sql_error)?;
    let mut written = 0;
    for key in keys {
        if let Some(cached) = existing.get(key.as_bytes())
            && compare_records(cached, record).is_ge()
        {
            continue;
        }
        if !existing.contains_key(key.as_bytes()) && !make_room(conn, key, record, now_ms)? {
            continue;
        }
        conn.execute(
            "INSERT INTO podcast_dht_records
                 (dht_key, podcast_id, publisher, tombstone, expires_at_ms, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(dht_key, podcast_id, publisher) DO UPDATE SET
                 tombstone = excluded.tombstone,
                 expires_at_ms = excluded.expires_at_ms,
                 payload = excluded.payload",
            params![
                key.as_bytes().as_slice(),
                podcast_id,
                publisher,
                record.is_tombstone(),
                clamp_i64(record.expires_at_ms()),
                payload,
            ],
        )?;
        written += 1;
    }

    // Drop versions the new record makes obsolete under other keys too.
    if written > 0 {
        for (key, cached) in &existing {
            if !keys.iter().any(|stored| stored.as_bytes() == key)
                && supersedes_everywhere(record, cached)
            {
                conn.execute(
                    "DELETE FROM podcast_dht_records
                     WHERE dht_key = ?1 AND podcast_id = ?2 AND publisher = ?3",
                    params![key.as_slice(), podcast_id, publisher],
                )?;
            }
        }
    }
    Ok(written)
}

/// Ensures there is room for one more record under `key`.
fn make_room(
    conn: &Connection,
    key: &PodcastDhtKey,
    record: &SignedPodcastRecord,
    now_ms: u64,
) -> rusqlite::Result<bool> {
    conn.execute(
        "DELETE FROM podcast_dht_records WHERE dht_key = ?1 AND expires_at_ms <= ?2",
        params![key.as_bytes().as_slice(), clamp_i64(now_ms)],
    )?;
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM podcast_dht_records WHERE dht_key = ?1",
        [key.as_bytes().as_slice()],
        |row| row.get(0),
    )?;
    if (count as usize) < MAX_PODCAST_RECORDS_PER_KEY {
        return Ok(true);
    }
    let victim: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT podcast_id, publisher, expires_at_ms FROM podcast_dht_records
             WHERE dht_key = ?1
             ORDER BY expires_at_ms ASC, podcast_id, publisher
             LIMIT 1",
            [key.as_bytes().as_slice()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    match victim {
        Some((podcast_id, publisher, expires_at_ms))
            if expires_at_ms < clamp_i64(record.expires_at_ms()) =>
        {
            conn.execute(
                "DELETE FROM podcast_dht_records
                 WHERE dht_key = ?1 AND podcast_id = ?2 AND publisher = ?3",
                params![key.as_bytes().as_slice(), podcast_id, publisher],
            )?;
            Ok(true)
        }
        _ => Ok(false),
    }
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
