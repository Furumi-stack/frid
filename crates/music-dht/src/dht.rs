//! DHT record validation and replacement rules.

use federation_net::NetworkId;

use crate::message::StoreRecordRequest;
use crate::normalization::{normalize_name, tokenize};
use crate::record::{
    ACTIVE_RECORD_TTL, DhtKey, MAX_ARTISTS_PER_ITEM, MAX_ITEM_NAME_BYTES, MAX_TOKENS_PER_ITEM,
    StoredRecord, TOMBSTONE_TTL,
};

/// Outcome of comparing an incoming record with the stored one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreDecision {
    /// No record stored yet, or the incoming one supersedes it: write it.
    Write,
    /// Same logical record: keep the stored row but extend its expiry.
    RefreshExpiry(u64),
    /// The incoming record is older or otherwise loses: ignore it.
    Ignore,
}

/// Decides what to do with an incoming record given the stored state
/// `(revision, deleted, expires_at_ms)` for the same
/// `(key, artist_id, owner)`.
///
/// Rules:
/// * a higher revision always wins;
/// * on equal revisions a tombstone beats an active record;
/// * on equal revisions and equal deletion state the record is the same —
///   only the expiry is refreshed (this is how republish extends TTL);
/// * an older revision never replaces a newer one, in particular an old
///   active record never resurrects a tombstone.
pub fn decide_store(existing: Option<(u64, bool, u64)>, incoming: &StoredRecord) -> StoreDecision {
    let Some((revision, deleted, expires_at_ms)) = existing else {
        return StoreDecision::Write;
    };
    let item = &incoming.item;
    if item.revision > revision {
        return StoreDecision::Write;
    }
    if item.revision < revision {
        return StoreDecision::Ignore;
    }
    // Equal revisions.
    match (deleted, item.deleted) {
        (false, true) => StoreDecision::Write,
        (true, false) => StoreDecision::Ignore,
        _ => {
            if incoming.expires_at_ms > expires_at_ms {
                StoreDecision::RefreshExpiry(incoming.expires_at_ms)
            } else {
                StoreDecision::Ignore
            }
        }
    }
}

/// Validates an incoming `StoreRecord` request.
///
/// Checks the size limits, that the record is internally consistent, that the
/// key actually corresponds to the record's name, or one of the item's/artist's
/// search tokens, and clamps the expiry to the maximum TTL allowed for the
/// record type. Returns the record with a possibly clamped `expires_at_ms`.
pub(crate) fn validate_store(
    request: StoreRecordRequest,
    network_id: &NetworkId,
    now_ms: u64,
) -> Result<StoredRecord, String> {
    let mut record = request.record;
    let item = &record.item;

    if item.name.len() > MAX_ITEM_NAME_BYTES {
        return Err("item name too long".into());
    }
    if item.normalized_name != normalize_name(&item.name) {
        return Err("normalized name does not match the item name".into());
    }
    if item.normalized_name.is_empty() {
        return Err("item name normalizes to nothing".into());
    }
    let tokens = tokenize(&item.normalized_name);
    if tokens.len() > MAX_TOKENS_PER_ITEM {
        return Err("too many tokens".into());
    }
    if item.artist_names.len() > MAX_ARTISTS_PER_ITEM {
        return Err("too many artists".into());
    }
    for artist in &item.artist_names {
        if artist.len() > MAX_ITEM_NAME_BYTES {
            return Err("artist name too long".into());
        }
        if tokenize(&normalize_name(artist)).len() > MAX_TOKENS_PER_ITEM {
            return Err("artist name has too many tokens".into());
        }
    }

    let key_matches = request.key == DhtKey::exact(network_id, &item.normalized_name)
        || item
            .search_tokens()
            .iter()
            .any(|token| request.key == DhtKey::token(network_id, token));
    if !key_matches {
        return Err("key does not correspond to the record".into());
    }

    if record.expires_at_ms <= now_ms {
        return Err("record is already expired".into());
    }
    let max_ttl_ms = if item.deleted {
        TOMBSTONE_TTL.as_millis() as u64
    } else {
        ACTIVE_RECORD_TTL.as_millis() as u64
    };
    record.expires_at_ms = record.expires_at_ms.min(now_ms + max_ttl_ms);

    Ok(record)
}

#[cfg(test)]
mod tests {
    use federation_net::EndpointId;

    use super::*;
    use crate::record::{ItemId, LibraryItem};

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn record(revision: u64, deleted: bool, expires_at_ms: u64) -> StoredRecord {
        let owner = test_peer(1);
        StoredRecord {
            item: LibraryItem {
                id: ItemId::from_bytes([9u8; 32]),
                owner,
                kind: crate::record::ItemKind::Artist,
                name: "Massive Attack".into(),
                normalized_name: "massive attack".into(),
                artist_names: Vec::new(),
                year: None,
                release_type: None,
                duration_seconds: None,
                revision,
                deleted,
                updated_at_ms: 0,
            },
            publisher: owner,
            expires_at_ms,
        }
    }

    fn track_with_artist(expires_at_ms: u64) -> StoredRecord {
        let owner = test_peer(1);
        StoredRecord {
            item: LibraryItem {
                id: ItemId::from_bytes([8u8; 32]),
                owner,
                kind: crate::record::ItemKind::Track,
                name: "Teardrop".into(),
                normalized_name: "teardrop".into(),
                artist_names: vec!["Massive Attack".into()],
                year: Some(1998),
                release_type: None,
                duration_seconds: Some(330.0),
                revision: 1,
                deleted: false,
                updated_at_ms: 0,
            },
            publisher: owner,
            expires_at_ms,
        }
    }

    #[test]
    fn newer_revision_replaces_older() {
        let incoming = record(2, false, 1000);
        assert_eq!(
            decide_store(Some((1, false, 500)), &incoming),
            StoreDecision::Write
        );
    }

    #[test]
    fn older_revision_is_ignored() {
        let incoming = record(1, false, 1000);
        assert_eq!(
            decide_store(Some((2, false, 500)), &incoming),
            StoreDecision::Ignore
        );
    }

    #[test]
    fn tombstone_beats_active_record_of_same_revision() {
        let incoming = record(1, true, 1000);
        assert_eq!(
            decide_store(Some((1, false, 500)), &incoming),
            StoreDecision::Write
        );
    }

    #[test]
    fn active_record_does_not_resurrect_tombstone() {
        let incoming = record(1, false, 1000);
        assert_eq!(
            decide_store(Some((1, true, 500)), &incoming),
            StoreDecision::Ignore
        );
        // Even an older active record loses to a newer tombstone.
        let incoming = record(1, false, 1000);
        assert_eq!(
            decide_store(Some((2, true, 500)), &incoming),
            StoreDecision::Ignore
        );
    }

    #[test]
    fn republish_refreshes_expiry() {
        let incoming = record(1, false, 2000);
        assert_eq!(
            decide_store(Some((1, false, 500)), &incoming),
            StoreDecision::RefreshExpiry(2000)
        );
        let stale = record(1, false, 100);
        assert_eq!(
            decide_store(Some((1, false, 500)), &stale),
            StoreDecision::Ignore
        );
    }

    #[test]
    fn missing_record_is_written() {
        let incoming = record(1, false, 1000);
        assert_eq!(decide_store(None, &incoming), StoreDecision::Write);
    }

    #[test]
    fn validate_checks_key_and_clamps_ttl() {
        let net = NetworkId::from_name("test");
        let now = 1_000_000;
        let rec = record(1, false, now + ACTIVE_RECORD_TTL.as_millis() as u64 * 10);

        // Correct exact key: accepted, expiry clamped to the maximum TTL.
        let ok = validate_store(
            StoreRecordRequest {
                key: DhtKey::exact(&net, "massive attack"),
                record: rec.clone(),
            },
            &net,
            now,
        )
        .expect("valid");
        assert_eq!(ok.expires_at_ms, now + ACTIVE_RECORD_TTL.as_millis() as u64);

        // Correct token key: accepted.
        assert!(
            validate_store(
                StoreRecordRequest {
                    key: DhtKey::token(&net, "massive"),
                    record: rec.clone(),
                },
                &net,
                now,
            )
            .is_ok()
        );

        // Unrelated key: rejected.
        assert!(
            validate_store(
                StoreRecordRequest {
                    key: DhtKey::exact(&net, "portishead"),
                    record: rec.clone(),
                },
                &net,
                now,
            )
            .is_err()
        );

        // Expired record: rejected.
        let expired = record(1, false, now - 1);
        assert!(
            validate_store(
                StoreRecordRequest {
                    key: DhtKey::exact(&net, "massive attack"),
                    record: expired,
                },
                &net,
                now,
            )
            .is_err()
        );

        // Inconsistent normalization: rejected.
        let mut bad = rec;
        bad.item.normalized_name = "something else".into();
        assert!(
            validate_store(
                StoreRecordRequest {
                    key: DhtKey::exact(&net, "something else"),
                    record: bad,
                },
                &net,
                now,
            )
            .is_err()
        );
    }

    #[test]
    fn validate_accepts_artist_token_keys() {
        let net = NetworkId::from_name("test");
        let now = 1_000_000;
        let rec = track_with_artist(now + ACTIVE_RECORD_TTL.as_millis() as u64);

        assert!(
            validate_store(
                StoreRecordRequest {
                    key: DhtKey::token(&net, "massive"),
                    record: rec.clone(),
                },
                &net,
                now,
            )
            .is_ok()
        );
        assert!(
            validate_store(
                StoreRecordRequest {
                    key: DhtKey::token(&net, "attack"),
                    record: rec,
                },
                &net,
                now,
            )
            .is_ok()
        );
    }
}
