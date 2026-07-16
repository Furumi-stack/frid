//! Local SQLite persistence.
//!
//! `rusqlite` is synchronous, so every database call runs on the blocking
//! thread pool via `tokio::task::spawn_blocking`; the async runtime is never
//! blocked on file I/O.

use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use federation_net::EndpointId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::dht::{StoreDecision, decide_store};
use crate::error::{ArtistDhtError, Result};
use crate::message::MAX_RECORDS_PER_RESPONSE;
use crate::normalization::tokenize;
use crate::record::{Artist, ArtistId, DhtKey, StoredArtistRecord, TOMBSTONE_TTL};
use crate::routing::{NodeContact, NodeId};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS local_artists (
    id              BLOB PRIMARY KEY,
    owner_peer_id   TEXT NOT NULL,
    name            TEXT NOT NULL,
    normalized_name TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    deleted         INTEGER NOT NULL DEFAULT 0,
    updated_at_ms   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_local_artists_normalized_name
    ON local_artists(normalized_name);

CREATE TABLE IF NOT EXISTS dht_records (
    dht_key          BLOB NOT NULL,
    artist_id        BLOB NOT NULL,
    owner_peer_id    TEXT NOT NULL,
    payload          BLOB NOT NULL,
    revision         INTEGER NOT NULL,
    deleted          INTEGER NOT NULL,
    expires_at_ms    INTEGER NOT NULL,

    PRIMARY KEY (dht_key, artist_id, owner_peer_id)
);

CREATE INDEX IF NOT EXISTS idx_dht_records_expires_at
    ON dht_records(expires_at_ms);

CREATE TABLE IF NOT EXISTS known_peers (
    peer_id          TEXT PRIMARY KEY,
    node_id          BLOB NOT NULL,
    ticket           TEXT NOT NULL,
    last_seen_ms     INTEGER NOT NULL
);
";

/// Handle to the local SQLite database.
///
/// Cheap to clone; all clones share one connection guarded by a mutex that is
/// only ever locked from blocking-pool threads.
#[derive(Clone)]
pub(crate) struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Opens (creating if needed) the database at `path` and applies the
    /// schema.
    pub async fn open(path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(&path).map_err(|err| {
                ArtistDhtError::Database(format!("failed to open {}: {err}", path.display()))
            })?;
            conn.execute_batch(SCHEMA).map_err(|err| {
                ArtistDhtError::Database(format!("failed to apply schema: {err}"))
            })?;
            Ok(conn)
        })
        .await
        .map_err(|err| ArtistDhtError::Database(format!("database task panicked: {err}")))??;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Runs a closure against the connection on the blocking pool.
    async fn call<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Connection) -> rusqlite::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            f(&guard).map_err(|err| ArtistDhtError::Database(err.to_string()))
        })
        .await
        .map_err(|err| ArtistDhtError::Database(format!("database task panicked: {err}")))?
    }

    /// Inserts or replaces a locally owned artist record.
    pub async fn upsert_local_artist(&self, artist: &Artist) -> Result<()> {
        let artist = artist.clone();
        self.call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO local_artists
                 (id, owner_peer_id, name, normalized_name, revision, deleted, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    artist.id.as_bytes().as_slice(),
                    artist.owner.to_string(),
                    artist.name,
                    artist.normalized_name,
                    artist.revision as i64,
                    artist.deleted as i64,
                    artist.updated_at_ms as i64,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Fetches one locally owned artist by id.
    pub async fn get_local_artist(&self, id: ArtistId) -> Result<Option<Artist>> {
        self.call(move |conn| {
            conn.query_row(
                "SELECT id, owner_peer_id, name, normalized_name, revision, deleted, updated_at_ms
                 FROM local_artists WHERE id = ?1",
                params![id.as_bytes().as_slice()],
                artist_from_row,
            )
            .optional()
        })
        .await
    }

    /// Finds locally owned artists whose id starts with the given hex prefix.
    pub async fn find_local_by_id_prefix(&self, prefix: String) -> Result<Vec<Artist>> {
        let prefix = prefix.to_lowercase();
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, owner_peer_id, name, normalized_name, revision, deleted, updated_at_ms
                 FROM local_artists WHERE deleted = 0",
            )?;
            let rows = stmt.query_map([], artist_from_row)?;
            let mut result = Vec::new();
            for row in rows {
                let artist = row?;
                if artist.id.to_hex().starts_with(&prefix) {
                    result.push(artist);
                }
            }
            Ok(result)
        })
        .await
    }

    /// Lists locally owned artists. Tombstones are excluded unless
    /// `include_deleted` is set.
    pub async fn list_local_artists(&self, include_deleted: bool) -> Result<Vec<Artist>> {
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, owner_peer_id, name, normalized_name, revision, deleted, updated_at_ms
                 FROM local_artists ORDER BY normalized_name",
            )?;
            let rows = stmt.query_map([], artist_from_row)?;
            let mut result = Vec::new();
            for row in rows {
                let artist = row?;
                if include_deleted || !artist.deleted {
                    result.push(artist);
                }
            }
            Ok(result)
        })
        .await
    }

    /// Returns everything that must be republished: active records plus
    /// tombstones that have not outlived [`TOMBSTONE_TTL`] yet.
    pub async fn local_artists_for_republish(&self, now_ms: u64) -> Result<Vec<Artist>> {
        let all = self.list_local_artists(true).await?;
        let tombstone_ttl = TOMBSTONE_TTL.as_millis() as u64;
        Ok(all
            .into_iter()
            .filter(|artist| {
                !artist.deleted || artist.updated_at_ms.saturating_add(tombstone_ttl) > now_ms
            })
            .collect())
    }

    /// Searches locally owned active artists: exact normalized match, or all
    /// query tokens present in the artist's token set.
    pub async fn search_local(&self, normalized_query: String) -> Result<Vec<Artist>> {
        let all = self.list_local_artists(false).await?;
        let query_tokens = tokenize(&normalized_query);
        Ok(all
            .into_iter()
            .filter(|artist| {
                if artist.normalized_name == normalized_query {
                    return true;
                }
                if query_tokens.is_empty() {
                    return false;
                }
                let artist_tokens = tokenize(&artist.normalized_name);
                query_tokens
                    .iter()
                    .all(|token| artist_tokens.iter().any(|t| t == token))
            })
            .collect())
    }

    /// Applies a validated incoming record to the replica store, following
    /// the revision/tombstone rules. Returns `true` if the record was written
    /// or refreshed.
    pub async fn store_dht_record(&self, key: DhtKey, record: StoredArtistRecord) -> Result<bool> {
        self.call(move |conn| {
            let existing: Option<(i64, i64, i64)> = conn
                .query_row(
                    "SELECT revision, deleted, expires_at_ms FROM dht_records
                     WHERE dht_key = ?1 AND artist_id = ?2 AND owner_peer_id = ?3",
                    params![
                        key.as_bytes().as_slice(),
                        record.artist.id.as_bytes().as_slice(),
                        record.artist.owner.to_string(),
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let existing =
                existing.map(|(rev, del, exp)| (rev as u64, del != 0, exp as u64));
            match decide_store(existing, &record) {
                StoreDecision::Ignore => Ok(false),
                StoreDecision::Write | StoreDecision::RefreshExpiry(_) => {
                    let payload = postcard::to_stdvec(&record).map_err(|err| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(err))
                    })?;
                    conn.execute(
                        "INSERT OR REPLACE INTO dht_records
                         (dht_key, artist_id, owner_peer_id, payload, revision, deleted, expires_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                        params![
                            key.as_bytes().as_slice(),
                            record.artist.id.as_bytes().as_slice(),
                            record.artist.owner.to_string(),
                            payload,
                            record.artist.revision as i64,
                            record.artist.deleted as i64,
                            record.expires_at_ms as i64,
                        ],
                    )?;
                    Ok(true)
                }
            }
        })
        .await
    }

    /// Returns non-expired replicas stored under `key`, including tombstones
    /// (they inform other peers about deletions). Capped at
    /// [`MAX_RECORDS_PER_RESPONSE`].
    pub async fn dht_records_by_key(
        &self,
        key: DhtKey,
        now_ms: u64,
    ) -> Result<Vec<StoredArtistRecord>> {
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT payload FROM dht_records
                 WHERE dht_key = ?1 AND expires_at_ms > ?2
                 LIMIT ?3",
            )?;
            let rows = stmt.query_map(
                params![
                    key.as_bytes().as_slice(),
                    now_ms as i64,
                    MAX_RECORDS_PER_RESPONSE as i64
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?;
            let mut records = Vec::new();
            for row in rows {
                let payload = row?;
                // A payload we cannot decode is skipped, not fatal.
                if let Ok(record) = postcard::from_bytes::<StoredArtistRecord>(&payload) {
                    records.push(record);
                }
            }
            Ok(records)
        })
        .await
    }

    /// Deletes expired replicas. Returns the number of removed rows.
    pub async fn delete_expired_records(&self, now_ms: u64) -> Result<usize> {
        self.call(move |conn| {
            conn.execute(
                "DELETE FROM dht_records WHERE expires_at_ms <= ?1",
                params![now_ms as i64],
            )
        })
        .await
    }

    /// Inserts or refreshes a known peer contact.
    pub async fn upsert_known_peer(&self, contact: &NodeContact) -> Result<()> {
        let contact = contact.clone();
        self.call(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO known_peers (peer_id, node_id, ticket, last_seen_ms)
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

    /// Deletes a persisted peer contact (e.g. after repeated failed dials).
    pub async fn delete_known_peer(&self, peer_id: EndpointId) -> Result<()> {
        self.call(move |conn| {
            conn.execute(
                "DELETE FROM known_peers WHERE peer_id = ?1",
                params![peer_id.to_string()],
            )?;
            Ok(())
        })
        .await
    }

    /// Loads all persisted peer contacts.
    pub async fn load_known_peers(&self) -> Result<Vec<NodeContact>> {
        self.call(|conn| {
            let mut stmt =
                conn.prepare("SELECT peer_id, node_id, ticket, last_seen_ms FROM known_peers")?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            let mut contacts = Vec::new();
            for row in rows {
                let (peer_id, node_id, ticket, last_seen_ms) = row?;
                let Ok(peer_id) = EndpointId::from_str(&peer_id) else {
                    continue;
                };
                let Ok(node_id) = <[u8; 32]>::try_from(node_id.as_slice()) else {
                    continue;
                };
                contacts.push(NodeContact {
                    node_id: NodeId::from_bytes(node_id),
                    peer_id,
                    ticket,
                    last_seen_ms: last_seen_ms as u64,
                });
            }
            Ok(contacts)
        })
        .await
    }
}

fn artist_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Artist> {
    let id: Vec<u8> = row.get(0)?;
    let owner: String = row.get(1)?;
    let id = <[u8; 32]>::try_from(id.as_slice()).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Blob,
            "artist id must be 32 bytes".into(),
        )
    })?;
    let owner = EndpointId::from_str(&owner).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            1,
            rusqlite::types::Type::Text,
            format!("invalid owner peer id: {err}").into(),
        )
    })?;
    Ok(Artist {
        id: ArtistId::from_bytes(id),
        owner,
        name: row.get(2)?,
        normalized_name: row.get(3)?,
        revision: row.get::<_, i64>(4)? as u64,
        deleted: row.get::<_, i64>(5)? != 0,
        updated_at_ms: row.get::<_, i64>(6)? as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::now_ms;

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn artist(owner: EndpointId, name: &str, revision: u64, deleted: bool) -> Artist {
        Artist {
            id: ArtistId::derive(&owner, &uuid::Uuid::from_u128(1)),
            owner,
            name: name.to_string(),
            normalized_name: crate::normalization::normalize_artist_name(name),
            revision,
            deleted,
            updated_at_ms: now_ms(),
        }
    }

    async fn open_temp() -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(&dir.path().join("state.sqlite3"))
            .await
            .expect("open db");
        (dir, db)
    }

    #[tokio::test]
    async fn local_artist_round_trip() {
        let (_dir, db) = open_temp().await;
        let owner = test_peer(1);
        let artist = artist(owner, "Massive Attack", 1, false);
        db.upsert_local_artist(&artist).await.expect("upsert");
        let loaded = db.get_local_artist(artist.id).await.expect("get");
        assert_eq!(loaded, Some(artist.clone()));
        let found = db
            .search_local("massive attack".into())
            .await
            .expect("search");
        assert_eq!(found.len(), 1);
        let by_token = db.search_local("attack".into()).await.expect("search");
        assert_eq!(by_token.len(), 1);
        let none = db.search_local("portishead".into()).await.expect("search");
        assert!(none.is_empty());
    }

    #[tokio::test]
    async fn expired_dht_record_is_not_returned() {
        let (_dir, db) = open_temp().await;
        let owner = test_peer(1);
        let artist = artist(owner, "Massive Attack", 1, false);
        let key = DhtKey::exact(&federation_net::NetworkId::from_name("t"), "massive attack");
        let now = now_ms();
        let record = StoredArtistRecord {
            artist,
            publisher: owner,
            expires_at_ms: now + 50,
        };
        assert!(db.store_dht_record(key, record).await.expect("store"));
        assert_eq!(db.dht_records_by_key(key, now).await.expect("get").len(), 1);
        // After expiry the record is filtered out and then swept.
        let later = now + 100;
        assert!(
            db.dht_records_by_key(key, later)
                .await
                .expect("get")
                .is_empty()
        );
        assert_eq!(db.delete_expired_records(later).await.expect("sweep"), 1);
    }

    #[tokio::test]
    async fn known_peers_round_trip() {
        let (_dir, db) = open_temp().await;
        let peer = test_peer(2);
        let contact = NodeContact {
            node_id: NodeId::from_endpoint(&peer),
            peer_id: peer,
            ticket: "fnet-test".into(),
            last_seen_ms: 42,
        };
        db.upsert_known_peer(&contact).await.expect("upsert");
        let loaded = db.load_known_peers().await.expect("load");
        assert_eq!(loaded, vec![contact]);
    }
}
