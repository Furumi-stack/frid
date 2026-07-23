//! Local SQLite persistence.
//!
//! `rusqlite` is synchronous, so every database call runs on the blocking
//! thread pool via `tokio::task::spawn_blocking`; the async runtime is never
//! blocked on file I/O.

use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use federation_net::EndpointId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::dht::{StoreDecision, decide_store};
use crate::error::{MusicDhtError, Result};
use crate::message::MAX_RECORDS_PER_RESPONSE;
use crate::normalization::tokenize;
use crate::record::{DhtKey, LibraryItem, StoredRecord, TOMBSTONE_TTL};
use crate::routing::{NodeContact, NodeId};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS local_items (
    id              BLOB PRIMARY KEY,
    normalized_name TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    deleted         INTEGER NOT NULL DEFAULT 0,
    updated_at_ms   INTEGER NOT NULL,
    payload         BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_local_items_normalized_name
    ON local_items(normalized_name);

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

/// Persistence backend used by [`crate::MusicDhtService`] for local items,
/// replicated DHT records and learned peer contacts.
///
/// The default service start path uses the bundled SQLite implementation.
/// Applications that already have durable storage can pass their own backend
/// to [`crate::MusicDhtService::start_with_storage`].
#[async_trait]
pub trait MusicDhtStorage: std::fmt::Debug + Send + Sync {
    /// Inserts or replaces a locally owned item record.
    async fn upsert_local_item(&self, item: &LibraryItem) -> Result<()>;

    /// Lists locally owned items. Tombstones are excluded unless
    /// `include_deleted` is set.
    async fn list_local_items(&self, include_deleted: bool) -> Result<Vec<LibraryItem>>;

    /// Returns everything that must be republished: active records plus
    /// tombstones that have not outlived [`TOMBSTONE_TTL`] yet.
    async fn local_items_for_republish(&self, now_ms: u64) -> Result<Vec<LibraryItem>> {
        let all = self.list_local_items(true).await?;
        let tombstone_ttl = TOMBSTONE_TTL.as_millis() as u64;
        Ok(all
            .into_iter()
            .filter(|item| {
                !item.deleted || item.updated_at_ms.saturating_add(tombstone_ttl) > now_ms
            })
            .collect())
    }

    /// Searches locally owned active items: exact normalized match, or all
    /// query tokens present in the item's token set, including artist names.
    async fn search_local(&self, normalized_query: String) -> Result<Vec<LibraryItem>> {
        let all = self.list_local_items(false).await?;
        let query_tokens = tokenize(&normalized_query);
        Ok(all
            .into_iter()
            .filter(|item| {
                if item.normalized_name == normalized_query {
                    return true;
                }
                if query_tokens.is_empty() {
                    return false;
                }
                let artist_tokens = item.search_tokens();
                query_tokens
                    .iter()
                    .all(|token| artist_tokens.iter().any(|t| t == token))
            })
            .collect())
    }

    /// Applies a validated incoming record to the replica store, following
    /// the revision/tombstone rules. Returns `true` if the record was written
    /// or refreshed.
    async fn store_dht_record(&self, key: DhtKey, record: StoredRecord) -> Result<bool>;

    /// Applies a batch of validated incoming records, one flag per entry in
    /// order. Backends should override this with a single write transaction:
    /// per-record commits dominate the cost of replicating a large library.
    async fn store_dht_records(&self, entries: Vec<(DhtKey, StoredRecord)>) -> Result<Vec<bool>> {
        let mut stored = Vec::with_capacity(entries.len());
        for (key, record) in entries {
            stored.push(self.store_dht_record(key, record).await?);
        }
        Ok(stored)
    }

    /// Returns non-expired replicas stored under `key`, including tombstones.
    async fn dht_records_by_key(&self, key: DhtKey, now_ms: u64) -> Result<Vec<StoredRecord>>;

    /// Counts non-expired DHT replica records stored by this node.
    async fn dht_record_count(&self, _now_ms: u64) -> Result<usize> {
        Err(MusicDhtError::Database(
            "DHT record counts are not supported by this storage backend".to_string(),
        ))
    }

    /// Deletes expired replicas. Returns the number of removed rows.
    async fn delete_expired_records(&self, now_ms: u64) -> Result<usize>;

    /// Inserts or refreshes a known peer contact.
    async fn upsert_known_peer(&self, contact: &NodeContact) -> Result<()>;

    /// Deletes a persisted peer contact.
    async fn delete_known_peer(&self, peer_id: EndpointId) -> Result<()>;

    /// Loads all persisted peer contacts.
    async fn load_known_peers(&self) -> Result<Vec<NodeContact>>;
}

/// Handle to the local SQLite database.
///
/// Cheap to clone; all clones share one connection guarded by a mutex that is
/// only ever locked from blocking-pool threads.
#[derive(Clone)]
pub(crate) struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database").finish_non_exhaustive()
    }
}

impl Database {
    /// Opens (creating if needed) the database at `path` and applies the
    /// schema.
    pub async fn open(path: &Path) -> Result<Self> {
        let path = path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection> {
            let conn = Connection::open(&path).map_err(|err| {
                MusicDhtError::Database(format!("failed to open {}: {err}", path.display()))
            })?;
            conn.execute_batch(SCHEMA)
                .map_err(|err| MusicDhtError::Database(format!("failed to apply schema: {err}")))?;
            Ok(conn)
        })
        .await
        .map_err(|err| MusicDhtError::Database(format!("database task panicked: {err}")))??;
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
            f(&guard).map_err(|err| MusicDhtError::Database(err.to_string()))
        })
        .await
        .map_err(|err| MusicDhtError::Database(format!("database task panicked: {err}")))?
    }

    /// Inserts or replaces a locally owned item record.
    pub async fn upsert_local_item(&self, item: &LibraryItem) -> Result<()> {
        let item = item.clone();
        self.call(move |conn| {
            let payload = postcard::to_stdvec(&item)
                .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
            conn.execute(
                "INSERT OR REPLACE INTO local_items
                 (id, normalized_name, revision, deleted, updated_at_ms, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    item.id.as_bytes().as_slice(),
                    item.normalized_name,
                    item.revision as i64,
                    item.deleted as i64,
                    item.updated_at_ms as i64,
                    payload,
                ],
            )?;
            Ok(())
        })
        .await
    }

    /// Lists locally owned items. Tombstones are excluded unless
    /// `include_deleted` is set.
    pub async fn list_local_items(&self, include_deleted: bool) -> Result<Vec<LibraryItem>> {
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT payload
                 FROM local_items ORDER BY normalized_name",
            )?;
            let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
            let mut result = Vec::new();
            for row in rows {
                let payload = row?;
                let Ok(item) = postcard::from_bytes::<LibraryItem>(&payload) else {
                    continue;
                };
                if include_deleted || !item.deleted {
                    result.push(item);
                }
            }
            Ok(result)
        })
        .await
    }

    /// Returns everything that must be republished: active records plus
    /// tombstones that have not outlived [`TOMBSTONE_TTL`] yet.
    pub async fn local_items_for_republish(&self, now_ms: u64) -> Result<Vec<LibraryItem>> {
        let all = self.list_local_items(true).await?;
        let tombstone_ttl = TOMBSTONE_TTL.as_millis() as u64;
        Ok(all
            .into_iter()
            .filter(|item| {
                !item.deleted || item.updated_at_ms.saturating_add(tombstone_ttl) > now_ms
            })
            .collect())
    }

    /// Searches locally owned active items: exact normalized match, or all
    /// query tokens present in the item's token set, including artist names.
    pub async fn search_local(&self, normalized_query: String) -> Result<Vec<LibraryItem>> {
        let all = self.list_local_items(false).await?;
        let query_tokens = tokenize(&normalized_query);
        Ok(all
            .into_iter()
            .filter(|item| {
                if item.normalized_name == normalized_query {
                    return true;
                }
                if query_tokens.is_empty() {
                    return false;
                }
                let artist_tokens = item.search_tokens();
                query_tokens
                    .iter()
                    .all(|token| artist_tokens.iter().any(|t| t == token))
            })
            .collect())
    }

    /// Applies a validated incoming record to the replica store, following
    /// the revision/tombstone rules. Returns `true` if the record was written
    /// or refreshed.
    pub async fn store_dht_record(&self, key: DhtKey, record: StoredRecord) -> Result<bool> {
        self.call(move |conn| store_record_in_conn(conn, &key, &record))
            .await
    }

    /// Applies a whole batch of records inside one transaction: one commit
    /// instead of one per record.
    pub async fn store_dht_records(
        &self,
        entries: Vec<(DhtKey, StoredRecord)>,
    ) -> Result<Vec<bool>> {
        self.call(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let mut stored = Vec::with_capacity(entries.len());
            for (key, record) in &entries {
                stored.push(store_record_in_conn(&tx, key, record)?);
            }
            tx.commit()?;
            Ok(stored)
        })
        .await
    }

    /// Returns non-expired replicas stored under `key`, including tombstones
    /// (they inform other peers about deletions). Capped at
    /// [`MAX_RECORDS_PER_RESPONSE`] with a deterministic order (freshest
    /// replicas first), so an overfull bucket always returns the same
    /// subset instead of an arbitrary one.
    pub async fn dht_records_by_key(&self, key: DhtKey, now_ms: u64) -> Result<Vec<StoredRecord>> {
        self.call(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT payload FROM dht_records
                 WHERE dht_key = ?1 AND expires_at_ms > ?2
                 ORDER BY expires_at_ms DESC, artist_id
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
                if let Ok(record) = postcard::from_bytes::<StoredRecord>(&payload) {
                    records.push(record);
                }
            }
            Ok(records)
        })
        .await
    }

    /// Counts non-expired DHT replica records stored by this node.
    pub async fn dht_record_count(&self, now_ms: u64) -> Result<usize> {
        self.call(move |conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM dht_records WHERE expires_at_ms > ?1",
                params![now_ms as i64],
                |row| row.get(0),
            )?;
            Ok(count.max(0) as usize)
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

#[async_trait]
impl MusicDhtStorage for Database {
    async fn upsert_local_item(&self, item: &LibraryItem) -> Result<()> {
        Database::upsert_local_item(self, item).await
    }

    async fn list_local_items(&self, include_deleted: bool) -> Result<Vec<LibraryItem>> {
        Database::list_local_items(self, include_deleted).await
    }

    async fn local_items_for_republish(&self, now_ms: u64) -> Result<Vec<LibraryItem>> {
        Database::local_items_for_republish(self, now_ms).await
    }

    async fn search_local(&self, normalized_query: String) -> Result<Vec<LibraryItem>> {
        Database::search_local(self, normalized_query).await
    }

    async fn store_dht_record(&self, key: DhtKey, record: StoredRecord) -> Result<bool> {
        Database::store_dht_record(self, key, record).await
    }

    async fn store_dht_records(&self, entries: Vec<(DhtKey, StoredRecord)>) -> Result<Vec<bool>> {
        Database::store_dht_records(self, entries).await
    }

    async fn dht_records_by_key(&self, key: DhtKey, now_ms: u64) -> Result<Vec<StoredRecord>> {
        Database::dht_records_by_key(self, key, now_ms).await
    }

    async fn dht_record_count(&self, now_ms: u64) -> Result<usize> {
        Database::dht_record_count(self, now_ms).await
    }

    async fn delete_expired_records(&self, now_ms: u64) -> Result<usize> {
        Database::delete_expired_records(self, now_ms).await
    }

    async fn upsert_known_peer(&self, contact: &NodeContact) -> Result<()> {
        Database::upsert_known_peer(self, contact).await
    }

    async fn delete_known_peer(&self, peer_id: EndpointId) -> Result<()> {
        Database::delete_known_peer(self, peer_id).await
    }

    async fn load_known_peers(&self) -> Result<Vec<NodeContact>> {
        Database::load_known_peers(self).await
    }
}

/// Applies one validated record to the replica store, following the
/// revision/tombstone rules. Returns `true` if the record was written or
/// refreshed. Runs against a plain connection or an open transaction.
fn store_record_in_conn(
    conn: &Connection,
    key: &DhtKey,
    record: &StoredRecord,
) -> rusqlite::Result<bool> {
    let existing: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT revision, deleted, expires_at_ms FROM dht_records
             WHERE dht_key = ?1 AND artist_id = ?2 AND owner_peer_id = ?3",
            params![
                key.as_bytes().as_slice(),
                record.item.id.as_bytes().as_slice(),
                record.item.owner.to_string(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let existing = existing.map(|(rev, del, exp)| (rev as u64, del != 0, exp as u64));
    match decide_store(existing, record) {
        StoreDecision::Ignore => Ok(false),
        StoreDecision::Write | StoreDecision::RefreshExpiry(_) => {
            let payload = postcard::to_stdvec(record)
                .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
            conn.execute(
                "INSERT OR REPLACE INTO dht_records
                 (dht_key, artist_id, owner_peer_id, payload, revision, deleted, expires_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    key.as_bytes().as_slice(),
                    record.item.id.as_bytes().as_slice(),
                    record.item.owner.to_string(),
                    payload,
                    record.item.revision as i64,
                    record.item.deleted as i64,
                    record.expires_at_ms as i64,
                ],
            )?;
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{ItemId, ItemKind, now_ms};

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn item(owner: EndpointId, name: &str, revision: u64, deleted: bool) -> LibraryItem {
        LibraryItem {
            id: ItemId::derive(&owner, ItemKind::Artist, name),
            owner,
            kind: ItemKind::Artist,
            name: name.to_string(),
            normalized_name: crate::normalization::normalize_name(name),
            artist_names: Vec::new(),
            featured_artist_names: Vec::new(),
            year: None,
            release_type: None,
            release_title: None,
            track_number: None,
            disc_number: None,
            duration_seconds: None,
            content_id: None,
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
    async fn local_item_round_trip() {
        let (_dir, db) = open_temp().await;
        let owner = test_peer(1);
        let item = item(owner, "Massive Attack", 1, false);
        db.upsert_local_item(&item).await.expect("upsert");
        let listed = db.list_local_items(true).await.expect("list");
        assert_eq!(listed, vec![item.clone()]);
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
    async fn undecodable_local_item_payload_is_skipped() {
        let (_dir, db) = open_temp().await;
        db.call(|conn| {
            conn.execute(
                "INSERT INTO local_items
                 (id, normalized_name, revision, deleted, updated_at_ms, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    &[1u8; 32],
                    "legacy",
                    1_i64,
                    0_i64,
                    0_i64,
                    vec![0xff_u8, 0xff],
                ],
            )?;
            Ok(())
        })
        .await
        .expect("insert corrupt row");

        assert!(db.list_local_items(true).await.expect("list").is_empty());
        assert!(
            db.search_local("legacy".into())
                .await
                .expect("search")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn local_search_uses_artist_names() {
        let (_dir, db) = open_temp().await;
        let owner = test_peer(1);
        let mut track = item(owner, "Teardrop", 1, false);
        track.kind = ItemKind::Track;
        track.artist_names = vec!["Massive Attack".into()];
        db.upsert_local_item(&track).await.expect("upsert");

        let found = db
            .search_local("massive attack".into())
            .await
            .expect("search");
        assert_eq!(found, vec![track]);
    }

    #[tokio::test]
    async fn record_batch_stores_in_one_call() {
        let (_dir, db) = open_temp().await;
        let owner = test_peer(1);
        let net = federation_net::NetworkId::from_name("t");
        let now = now_ms();
        let entry = |name: &str, revision: u64| {
            let item = item(owner, name, revision, false);
            (
                DhtKey::exact(&net, &item.normalized_name),
                StoredRecord {
                    item,
                    publisher: owner,
                    expires_at_ms: now + 60_000,
                },
            )
        };

        let stored = db
            .store_dht_records(vec![entry("Massive Attack", 1), entry("Portishead", 1)])
            .await
            .expect("store batch");
        assert_eq!(stored, vec![true, true]);
        assert_eq!(db.dht_record_count(now).await.expect("count"), 2);

        // A replay of the same batch changes nothing but reports it honestly.
        let replayed = db
            .store_dht_records(vec![entry("Massive Attack", 1), entry("Portishead", 2)])
            .await
            .expect("store batch");
        assert_eq!(replayed, vec![false, true]);
        assert_eq!(db.dht_record_count(now).await.expect("count"), 2);
    }

    #[tokio::test]
    async fn expired_dht_record_is_not_returned() {
        let (_dir, db) = open_temp().await;
        let owner = test_peer(1);
        let item = item(owner, "Massive Attack", 1, false);
        let key = DhtKey::exact(&federation_net::NetworkId::from_name("t"), "massive attack");
        let now = now_ms();
        let record = StoredRecord {
            item,
            publisher: owner,
            expires_at_ms: now + 50,
        };
        assert!(db.store_dht_record(key, record).await.expect("store"));
        assert_eq!(db.dht_records_by_key(key, now).await.expect("get").len(), 1);
        assert_eq!(db.dht_record_count(now).await.expect("count"), 1);
        // After expiry the record is filtered out and then swept.
        let later = now + 100;
        assert_eq!(db.dht_record_count(later).await.expect("count"), 0);
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
