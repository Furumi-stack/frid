//! Local lyrics indexed by canonical audio identity, independent of file paths.
use crate::Library;
use anyhow::Result;
use music_dht::lyrics::{LyricsVersion, MAX_LYRICS_VERSIONS};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Stable key for the text representations, independent of provider labels and IDs.
pub fn identity(version: &LyricsVersion) -> String {
    let mut hash = blake3::Hasher::new();
    for text in [&version.plain_text, &version.synced_lrc] {
        hash.update(&[u8::from(text.is_some())]);
        if let Some(text) = text {
            hash.update(&(text.len() as u64).to_le_bytes());
            hash.update(text.as_bytes());
        }
    }
    hash.finalize().to_hex().to_string()
}

/// Appends distinct, validated alternatives while preserving local order and exclusions.
pub fn merge(
    versions: &mut Vec<LyricsVersion>,
    incoming: impl IntoIterator<Item = LyricsVersion>,
    hidden: &HashSet<String>,
) {
    let mut seen: HashSet<_> = versions.iter().map(identity).collect();
    for mut version in incoming {
        let id = identity(&version);
        if versions.len() == MAX_LYRICS_VERSIONS {
            break;
        }
        if !hidden.contains(&id) && seen.insert(id.clone()) && version.validate().is_ok() {
            version.id = id;
            versions.push(version);
        }
    }
}

/// Local alternatives and exclusions that prevent a deleted version resurfacing from a peer.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SavedLyrics {
    /// Alternatives in display order, with locally edited versions first.
    pub versions: Vec<LyricsVersion>,
    /// Content-based identities of alternatives explicitly removed by the user.
    pub hidden: HashSet<String>,
}

impl Library {
    /// Reads the local lyrics for these audio bytes.
    pub fn lyrics(&self, content_id: &str) -> Result<SavedLyrics> {
        validate_content_id(content_id)?;
        read(&self.lock(), content_id)
    }

    /// Keeps peer lyrics only when the corresponding audio has been imported.
    pub fn merge_lyrics(&self, content_id: &str, versions: Vec<LyricsVersion>) -> Result<()> {
        validate_content_id(content_id)?;
        if versions.is_empty() {
            return Ok(());
        }
        let mut db = self.lock();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let local: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM tracks WHERE content_id=?1)",
            [content_id],
            |row| row.get(0),
        )?;
        if local {
            let mut saved = read(&tx, content_id)?;
            let before = saved.versions.len();
            merge(&mut saved.versions, versions, &saved.hidden);
            if saved.versions.len() != before {
                write(&tx, content_id, &saved)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Adds, replaces or removes an alternative on a local track.
    ///
    /// Clients may supply a freshly computed audio identity for older tracks
    /// that have yet to finish content ID backfill. If the track already has
    /// an identity, the edit must still refer to those exact audio bytes.
    pub fn edit_lyrics(
        &self,
        track_id: i64,
        content_id: &str,
        replaced: Option<&str>,
        version: Option<LyricsVersion>,
    ) -> Result<SavedLyrics> {
        validate_content_id(content_id)?;
        let mut db = self.lock();
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<String> = tx
            .query_row(
                "SELECT content_id FROM tracks WHERE id=?1",
                [track_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| anyhow::anyhow!("The local track has been removed"))?;
        anyhow::ensure!(
            current.as_deref().is_none_or(|id| id == content_id),
            "The local audio has changed"
        );
        let mut saved = read(&tx, content_id)?;
        if let Some(id) = replaced {
            saved.hidden.insert(id.to_owned());
            saved.versions.retain(|entry| entry.id != id);
        }
        if let Some(mut version) = version {
            version.validate()?;
            let id = identity(&version);
            saved.hidden.remove(&id);
            saved.versions.retain(|entry| entry.id != id);
            anyhow::ensure!(
                saved.versions.len() < MAX_LYRICS_VERSIONS,
                "This track already has 16 versions"
            );
            version.id = id;
            saved.versions.insert(0, version);
        }
        write(&tx, content_id, &saved)?;
        tx.commit()?;
        Ok(saved)
    }
}

fn validate_content_id(content_id: &str) -> Result<()> {
    music_dht::lyrics::LyricsRequest::new(content_id)?;
    Ok(())
}

fn read(db: &Connection, content_id: &str) -> Result<SavedLyrics> {
    let json: Option<String> = db
        .query_row(
            "SELECT payload FROM track_lyrics WHERE content_id=?1",
            [content_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(json
        .map(|json| serde_json::from_str(&json))
        .transpose()?
        .unwrap_or_default())
}

fn write(tx: &Transaction<'_>, content_id: &str, saved: &SavedLyrics) -> Result<()> {
    tx.execute(
        "INSERT INTO track_lyrics (content_id, payload) VALUES (?1, ?2)
         ON CONFLICT(content_id) DO UPDATE SET payload=excluded.payload",
        (content_id, serde_json::to_string(saved)?),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(text: &str) -> LyricsVersion {
        LyricsVersion {
            id: "peer-id".into(),
            label: "Lyrics".into(),
            source: "manual".into(),
            source_id: None,
            language: None,
            plain_text: Some(text.into()),
            synced_lrc: None,
            instrumental: false,
        }
    }

    #[test]
    fn persists_only_for_local_audio_and_preserves_edits_across_merges() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("library.db");
        let library = Library::open(&path).unwrap();
        let content_id = format!("b3:{}", "a".repeat(64));
        let peer = version("Peer lyrics");
        library
            .merge_lyrics(&content_id, vec![peer.clone()])
            .unwrap();
        assert!(library.lyrics(&content_id).unwrap().versions.is_empty());
        library
            .lock()
            .execute_batch("INSERT INTO releases(id, title) VALUES (1, 'Release');")
            .unwrap();
        library
            .lock()
            .execute(
                "INSERT INTO tracks(id, title, release_id, file_path, content_id)
             VALUES (1, 'Track', 1, 'local.flac', ?1)",
                [&content_id],
            )
            .unwrap();
        library
            .merge_lyrics(&content_id, vec![peer.clone(), peer.clone()])
            .unwrap();
        assert_eq!(library.lyrics(&content_id).unwrap().versions.len(), 1);
        let editable = version("My lyrics");
        let peer_key = identity(&peer);
        library
            .edit_lyrics(1, &content_id, Some(&peer_key), Some(editable.clone()))
            .unwrap();
        library.merge_lyrics(&content_id, vec![peer]).unwrap();
        drop(library);
        let saved = Library::open(&path).unwrap().lyrics(&content_id).unwrap();
        assert_eq!(saved.versions.len(), 1);
        assert_eq!(saved.versions[0].id, identity(&editable));
        assert!(saved.hidden.contains(&peer_key));
    }
}
