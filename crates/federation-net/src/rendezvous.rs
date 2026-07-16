//! Automatic peer discovery ("rendezvous") over the BitTorrent Mainline DHT.
//!
//! Every peer of a network periodically publishes its own [`EndpointAddr`]
//! into a shared BEP44 mutable record whose signing key is derived from the
//! [`NetworkId`], and reads the addresses other peers published there.
//! Knowing the network id is therefore enough to find and join the network —
//! no tickets and no dedicated bootstrap servers are required.
//!
//! The record lives in the public Mainline DHT (the same network BitTorrent
//! and pkarr use), so the mechanism needs no infrastructure of its own. The
//! flip side is that the network id acts as a public rendezvous token: anyone
//! who knows it can discover and join the network. Treat the id of a private
//! network like a shared secret; the application-level handshake still
//! rejects peers whose network id does not match exactly.
//!
//! Multiple peers write the same record concurrently. Writers merge every
//! record instance they can read before publishing, so a lost update leaves
//! at most a temporarily incomplete peer list — it converges as everyone
//! republishes — and one reachable entry is enough to join, because contact
//! exchange takes over after the first connection.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use iroh::{EndpointAddr, EndpointId};
use mainline::async_dht::AsyncDht;
use mainline::{Dht, MutableItem, SigningKey};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::error::{NetworkError, Result};
use crate::protocol::NetworkId;

/// Default interval between rendezvous rounds (read + publish).
pub const DEFAULT_RENDEZVOUS_INTERVAL: Duration = Duration::from_secs(60);
/// Default time after which a published peer entry is considered stale.
pub const DEFAULT_RENDEZVOUS_ENTRY_TTL: Duration = Duration::from_secs(30 * 60);

/// Version of the rendezvous record wire format.
const RECORD_VERSION: u16 = 1;
/// Domain separation context for deriving the record signing key.
const KEY_DERIVATION_CONTEXT: &str = "federation-net:rendezvous:v1";
/// BEP44 caps mutable values at 1000 bytes; stay safely below.
const MAX_RECORD_BYTES: usize = 900;

/// Configuration of the mainline-DHT rendezvous.
///
/// Passing a `RendezvousConfig` to
/// [`crate::NetworkConfigBuilder::rendezvous`] enables automatic peer
/// discovery; by default it is disabled and peers are connected via tickets.
#[derive(Debug, Clone)]
pub struct RendezvousConfig {
    /// Interval between rendezvous rounds. Each round reads the record,
    /// republishes it with this peer merged in and dials newly seen peers.
    pub interval: Duration,
    /// Entries older than this are dropped from the record.
    pub entry_ttl: Duration,
    /// Overrides the DHT bootstrap nodes (`host:port` strings). `None` uses
    /// the public mainline defaults; tests point this at a local testnet.
    pub dht_bootstrap: Option<Vec<String>>,
}

impl Default for RendezvousConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_RENDEZVOUS_INTERVAL,
            entry_ttl: DEFAULT_RENDEZVOUS_ENTRY_TTL,
            dht_bootstrap: None,
        }
    }
}

/// One published peer: its dialable address and when it was last refreshed.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RendezvousEntry {
    addr: EndpointAddr,
    last_seen_ms: u64,
}

/// The record stored in the mainline DHT: a bounded list of recent peers.
#[derive(Debug, Serialize, Deserialize)]
struct RendezvousRecord {
    version: u16,
    entries: Vec<RendezvousEntry>,
}

/// Derives the shared record signing key from the network id.
///
/// The derivation is deterministic, so every member of the network can both
/// read and update the record.
fn signing_key(network_id: NetworkId) -> SigningKey {
    let seed = blake3::derive_key(KEY_DERIVATION_CONTEXT, network_id.as_bytes());
    SigningKey::from_bytes(&seed)
}

/// Current unix time in milliseconds.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn decode_record(bytes: &[u8]) -> Option<Vec<RendezvousEntry>> {
    let record: RendezvousRecord = postcard::from_bytes(bytes).ok()?;
    (record.version == RECORD_VERSION).then_some(record.entries)
}

/// Merges entries from every record instance seen this round with our own,
/// deduplicating by endpoint id (newest wins) and dropping stale entries.
fn merge_entries(
    seen: Vec<RendezvousEntry>,
    self_entry: Option<RendezvousEntry>,
    now_ms: u64,
    entry_ttl: Duration,
) -> Vec<RendezvousEntry> {
    let ttl_ms = entry_ttl.as_millis() as u64;
    let mut by_id: HashMap<EndpointId, RendezvousEntry> = HashMap::new();
    for mut entry in seen.into_iter().chain(self_entry) {
        // A peer with a fast clock must not pin its entry into the future.
        entry.last_seen_ms = entry.last_seen_ms.min(now_ms);
        if entry.last_seen_ms + ttl_ms <= now_ms {
            continue;
        }
        match by_id.get(&entry.addr.id) {
            Some(existing) if existing.last_seen_ms >= entry.last_seen_ms => {}
            _ => {
                by_id.insert(entry.addr.id, entry);
            }
        }
    }
    let mut entries: Vec<RendezvousEntry> = by_id.into_values().collect();
    entries.sort_by(|a, b| {
        b.last_seen_ms
            .cmp(&a.last_seen_ms)
            .then_with(|| a.addr.id.cmp(&b.addr.id))
    });
    entries
}

/// Encodes the record, dropping the oldest entries until it fits the BEP44
/// value size limit.
fn encode_record_capped(mut entries: Vec<RendezvousEntry>) -> Result<Vec<u8>> {
    loop {
        let record = RendezvousRecord {
            version: RECORD_VERSION,
            entries,
        };
        let encoded = postcard::to_stdvec(&record).map_err(|err| {
            NetworkError::Serialization(format!("failed to encode rendezvous record: {err}"))
        })?;
        if encoded.len() <= MAX_RECORD_BYTES {
            return Ok(encoded);
        }
        entries = record.entries;
        if entries.pop().is_none() {
            return Err(NetworkError::Serialization(
                "rendezvous record does not fit even when empty".to_string(),
            ));
        }
    }
}

/// A client of the shared rendezvous record of one network.
#[derive(Debug)]
pub(crate) struct RendezvousClient {
    dht: AsyncDht,
    key: SigningKey,
    entry_ttl: Duration,
}

impl RendezvousClient {
    /// Binds a mainline DHT client for the rendezvous record of `network_id`.
    pub(crate) fn new(network_id: NetworkId, config: &RendezvousConfig) -> Result<Self> {
        let mut builder = Dht::builder();
        if let Some(nodes) = &config.dht_bootstrap {
            builder.bootstrap(nodes);
        }
        let dht = builder
            .build()
            .map_err(|err| {
                NetworkError::Transport(format!("failed to start mainline DHT client: {err}"))
            })?
            .as_async();
        Ok(Self {
            dht,
            key: signing_key(network_id),
            entry_ttl: config.entry_ttl,
        })
    }

    /// Runs one rendezvous round: reads every reachable instance of the
    /// record, merges them with our own address, republishes the result and
    /// returns the addresses of the other known peers.
    ///
    /// `self_addr` is `None` while the local endpoint does not know any
    /// dialable address yet; the round then only reads.
    pub(crate) async fn round(
        &self,
        self_addr: Option<EndpointAddr>,
        now_ms: u64,
    ) -> Result<Vec<EndpointAddr>> {
        let public_key = self.key.verifying_key().to_bytes();
        let mut items = self.dht.get_mutable(&public_key, None, None);
        let mut seen = Vec::new();
        let mut max_seq = 0i64;
        while let Some(item) = items.next().await {
            max_seq = max_seq.max(item.seq());
            match decode_record(item.value()) {
                Some(entries) => seen.extend(entries),
                None => debug!("ignoring malformed rendezvous record instance"),
            }
        }

        let self_id = self_addr.as_ref().map(|addr| addr.id);
        let self_entry = self_addr.map(|addr| RendezvousEntry {
            addr,
            last_seen_ms: now_ms,
        });
        let publish = self_entry.is_some();
        let merged = merge_entries(seen, self_entry, now_ms, self.entry_ttl);

        if publish {
            let encoded = encode_record_capped(merged.clone())?;
            // Strictly newer than every instance seen this round; concurrent
            // writers race, but merging on read makes lost updates benign.
            let item = MutableItem::new(self.key.clone(), &encoded, max_seq + 1, None);
            if let Err(err) = self.dht.put_mutable(item, None).await {
                warn!(error = %err, "failed to publish the rendezvous record");
            }
        }

        Ok(merged
            .into_iter()
            .map(|entry| entry.addr)
            .filter(|addr| Some(addr.id) != self_id)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use iroh::SecretKey;

    use super::*;

    fn addr(port: u16) -> EndpointAddr {
        EndpointAddr::new(SecretKey::generate().public())
            .with_ip_addr(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
    }

    fn entry(addr: EndpointAddr, last_seen_ms: u64) -> RendezvousEntry {
        RendezvousEntry { addr, last_seen_ms }
    }

    #[test]
    fn signing_key_is_deterministic_and_network_specific() {
        let a = signing_key(NetworkId::from_name("net-a"));
        let b = signing_key(NetworkId::from_name("net-a"));
        let c = signing_key(NetworkId::from_name("net-b"));
        assert_eq!(a.to_bytes(), b.to_bytes());
        assert_ne!(a.to_bytes(), c.to_bytes());
    }

    #[test]
    fn record_round_trips() {
        let entries = vec![entry(addr(1000), 1), entry(addr(1001), 2)];
        let encoded = encode_record_capped(entries.clone()).expect("encode");
        let decoded = decode_record(&encoded).expect("decode");
        assert_eq!(decoded.len(), entries.len());
        assert_eq!(decoded[0].addr, entries[0].addr);
        assert_eq!(decoded[1].last_seen_ms, entries[1].last_seen_ms);
    }

    #[test]
    fn malformed_and_wrong_version_records_are_ignored() {
        assert!(decode_record(b"garbage").is_none());
        let record = RendezvousRecord {
            version: RECORD_VERSION + 1,
            entries: vec![],
        };
        let encoded = postcard::to_stdvec(&record).expect("encode");
        assert!(decode_record(&encoded).is_none());
    }

    #[test]
    fn merge_deduplicates_by_id_keeping_the_newest() {
        let a = addr(1000);
        let older = entry(a.clone(), 100);
        let newer = entry(a.clone(), 200);
        let merged = merge_entries(
            vec![older, newer],
            None,
            1000,
            Duration::from_millis(10_000),
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].last_seen_ms, 200);
    }

    #[test]
    fn merge_drops_expired_and_clamps_future_entries() {
        let now = 100_000;
        let ttl = Duration::from_millis(1_000);
        let expired = entry(addr(1000), now - 1_000);
        let fresh = entry(addr(1001), now - 500);
        let future = entry(addr(1002), now + 60_000);
        let merged = merge_entries(vec![expired, fresh, future], None, now, ttl);
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().all(|e| e.last_seen_ms <= now));
    }

    #[test]
    fn merge_inserts_self_and_sorts_newest_first() {
        let self_entry = entry(addr(1000), 300);
        let other = entry(addr(1001), 200);
        let merged = merge_entries(
            vec![other],
            Some(self_entry.clone()),
            300,
            Duration::from_millis(10_000),
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].addr.id, self_entry.addr.id);
    }

    #[test]
    fn oversized_records_are_trimmed_to_the_size_limit() {
        // Far more entries than can ever fit into one BEP44 value, sorted
        // newest-first as `merge_entries` produces them.
        let entries: Vec<RendezvousEntry> = (0..100)
            .map(|i| entry(addr(1000 + i), 10_000 - i as u64))
            .collect();
        let encoded = encode_record_capped(entries).expect("encode");
        assert!(encoded.len() <= MAX_RECORD_BYTES);
        let decoded = decode_record(&encoded).expect("decode");
        assert!(!decoded.is_empty());
        // Trimming drops from the tail, so the newest entry must survive.
        assert_eq!(decoded[0].last_seen_ms, 10_000);
    }
}
