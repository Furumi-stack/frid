//! Kademlia-style node identifiers, XOR distance and routing table.

use std::fmt;

use federation_net::EndpointId;
use serde::{Deserialize, Serialize};

use crate::record::DhtKey;

/// Replication factor: how many closest nodes store a record and how many
/// contacts a single response may carry.
pub const K: usize = 8;
/// Lookup parallelism: how many candidates are queried concurrently.
pub const ALPHA: usize = 3;
/// Hard budget of requests a single iterative lookup may send.
pub const MAX_LOOKUP_REQUESTS: usize = 32;

/// A 256-bit DHT node identifier, derived from the peer's endpoint id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId([u8; 32]);

impl NodeId {
    /// Derives the node id: `BLAKE3("artist-dht:node:" || endpoint id)`.
    ///
    /// The endpoint id is persistent, so the node id is stable across
    /// restarts.
    pub fn from_endpoint(endpoint_id: &EndpointId) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"artist-dht:node:");
        hasher.update(endpoint_id.as_bytes());
        Self(*hasher.finalize().as_bytes())
    }

    /// Creates a node id from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw bytes of this id.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({self})")
    }
}

/// XOR distance between two points of the 256-bit key space, compared as
/// unsigned big-endian integers.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Distance([u8; 32]);

/// Computes the XOR distance between two 256-bit values.
pub fn distance(a: &[u8; 32], b: &[u8; 32]) -> Distance {
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = a[i] ^ b[i];
    }
    Distance(out)
}

impl Distance {
    /// Index of the k-bucket this distance falls into: the position of the
    /// highest set bit (0..=255). Returns `None` for a zero distance (self).
    pub fn bucket_index(&self) -> Option<usize> {
        for (i, byte) in self.0.iter().enumerate() {
            if *byte != 0 {
                return Some(255 - (i * 8 + byte.leading_zeros() as usize));
            }
        }
        None
    }
}

/// Everything needed to reach another DHT node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeContact {
    /// DHT identifier of the node.
    pub node_id: NodeId,
    /// Transport identifier of the node.
    pub peer_id: EndpointId,
    /// `federation-net` ticket used to connect on demand.
    pub ticket: String,
    /// Unix timestamp (milliseconds) of the last observed activity.
    pub last_seen_ms: u64,
}

/// A simplified Kademlia routing table: 256 k-buckets of up to [`K`] contacts.
pub struct RoutingTable {
    own_id: NodeId,
    buckets: Vec<Vec<NodeContact>>,
}

impl RoutingTable {
    /// Creates an empty routing table for the given local node id.
    pub fn new(own_id: NodeId) -> Self {
        Self {
            own_id,
            buckets: vec![Vec::new(); 256],
        }
    }

    /// Returns the local node id.
    pub fn own_id(&self) -> NodeId {
        self.own_id
    }

    /// Inserts or refreshes a contact. Returns `true` if the contact was not
    /// known before.
    ///
    /// A full bucket evicts its least recently seen contact; the PoC skips
    /// the classic ping-before-evict procedure.
    pub fn upsert(&mut self, contact: NodeContact) -> bool {
        let Some(index) =
            distance(self.own_id.as_bytes(), contact.node_id.as_bytes()).bucket_index()
        else {
            // Zero distance: never store ourselves.
            return false;
        };
        let bucket = &mut self.buckets[index];
        if let Some(existing) = bucket
            .iter_mut()
            .find(|entry| entry.peer_id == contact.peer_id)
        {
            existing.node_id = contact.node_id;
            existing.ticket = contact.ticket;
            existing.last_seen_ms = existing.last_seen_ms.max(contact.last_seen_ms);
            return false;
        }
        if bucket.len() >= K {
            // Evict the least recently seen contact.
            if let Some((oldest, _)) = bucket
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.last_seen_ms)
            {
                bucket.remove(oldest);
            }
        }
        bucket.push(contact);
        true
    }

    /// Refreshes the `last_seen_ms` of a known peer. Returns `true` if the
    /// peer was found.
    pub fn touch(&mut self, peer_id: &EndpointId, now_ms: u64) -> bool {
        for bucket in &mut self.buckets {
            if let Some(entry) = bucket.iter_mut().find(|entry| &entry.peer_id == peer_id) {
                entry.last_seen_ms = entry.last_seen_ms.max(now_ms);
                return true;
            }
        }
        false
    }

    /// Removes a contact by its transport id. Returns `true` if it was known.
    pub fn remove(&mut self, peer_id: &EndpointId) -> bool {
        for bucket in &mut self.buckets {
            if let Some(index) = bucket.iter().position(|entry| &entry.peer_id == peer_id) {
                bucket.remove(index);
                return true;
            }
        }
        false
    }

    /// Looks up a contact by its transport id.
    pub fn get(&self, peer_id: &EndpointId) -> Option<NodeContact> {
        self.buckets
            .iter()
            .flatten()
            .find(|entry| &entry.peer_id == peer_id)
            .cloned()
    }

    /// Returns up to `count` known contacts closest to `target` by XOR
    /// distance.
    pub fn closest(&self, target: &[u8; 32], count: usize) -> Vec<NodeContact> {
        let mut contacts: Vec<NodeContact> = self.buckets.iter().flatten().cloned().collect();
        contacts.sort_by_key(|contact| distance(contact.node_id.as_bytes(), target));
        contacts.truncate(count);
        contacts
    }

    /// Returns all known contacts.
    pub fn contacts(&self) -> Vec<NodeContact> {
        self.buckets.iter().flatten().cloned().collect()
    }

    /// Returns the number of known contacts.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(Vec::len).sum()
    }

    /// Returns `true` if no contacts are known.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Convenience: distance from a node to a DHT key.
pub fn key_distance(node: &NodeId, key: &DhtKey) -> Distance {
    distance(node.as_bytes(), key.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn contact(seed: u8, last_seen_ms: u64) -> NodeContact {
        let peer = test_peer(seed);
        NodeContact {
            node_id: NodeId::from_endpoint(&peer),
            peer_id: peer,
            ticket: format!("fnet-test-{seed}"),
            last_seen_ms,
        }
    }

    #[test]
    fn node_id_is_deterministic() {
        let peer = test_peer(1);
        assert_eq!(NodeId::from_endpoint(&peer), NodeId::from_endpoint(&peer));
        assert_ne!(
            NodeId::from_endpoint(&peer),
            NodeId::from_endpoint(&test_peer(2))
        );
    }

    #[test]
    fn xor_distance_properties() {
        let a = [0b1010_0000u8; 32];
        let b = [0b0000_0000u8; 32];
        assert_eq!(distance(&a, &a), distance(&b, &b));
        assert_eq!(distance(&a, &b), distance(&b, &a));
        // d(a, a) == 0 and is the smallest possible distance.
        assert!(distance(&a, &a) < distance(&a, &b));

        // Big-endian comparison: a difference in the first byte outweighs
        // any difference in later bytes.
        let mut c = [0u8; 32];
        c[0] = 1;
        let mut d = [0u8; 32];
        d[31] = 0xff;
        assert!(distance(&c, &b) > distance(&d, &b));
    }

    #[test]
    fn bucket_index_matches_highest_bit() {
        let zero = [0u8; 32];
        let mut one = [0u8; 32];
        one[31] = 1;
        assert_eq!(distance(&zero, &one).bucket_index(), Some(0));
        let mut top = [0u8; 32];
        top[0] = 0x80;
        assert_eq!(distance(&zero, &top).bucket_index(), Some(255));
        assert_eq!(distance(&zero, &zero).bucket_index(), None);
    }

    #[test]
    fn closest_sorts_by_distance() {
        let own = NodeId::from_bytes([0u8; 32]);
        let mut table = RoutingTable::new(own);
        for seed in 1..=20u8 {
            table.upsert(contact(seed, seed as u64));
        }
        let target = [0x42u8; 32];
        let closest = table.closest(&target, K);
        assert!(closest.len() <= K);
        for pair in closest.windows(2) {
            assert!(
                distance(pair[0].node_id.as_bytes(), &target)
                    <= distance(pair[1].node_id.as_bytes(), &target)
            );
        }
    }

    #[test]
    fn bucket_is_capped_at_k() {
        // All contacts whose distance to `own` shares the same highest bit
        // land in one bucket; force that by controlling the node ids.
        let own = NodeId::from_bytes([0u8; 32]);
        let mut table = RoutingTable::new(own);
        for i in 0..(K as u8 + 4) {
            let peer = test_peer(i + 1);
            let mut id = [0x80u8; 32];
            id[31] = i;
            table.upsert(NodeContact {
                node_id: NodeId::from_bytes(id),
                peer_id: peer,
                ticket: String::new(),
                last_seen_ms: u64::from(i),
            });
        }
        assert_eq!(table.len(), K);
        // The oldest contacts (smallest last_seen_ms) were evicted.
        let contacts = table.contacts();
        assert!(contacts.iter().all(|c| c.last_seen_ms >= 4));
    }

    #[test]
    fn remove_deletes_contact() {
        let own = NodeId::from_bytes([0u8; 32]);
        let mut table = RoutingTable::new(own);
        table.upsert(contact(1, 10));
        table.upsert(contact(2, 10));
        assert!(table.remove(&test_peer(1)));
        assert!(!table.remove(&test_peer(1)));
        assert_eq!(table.len(), 1);
        assert!(table.get(&test_peer(1)).is_none());
    }

    #[test]
    fn upsert_refreshes_existing_contact() {
        let own = NodeId::from_bytes([0u8; 32]);
        let mut table = RoutingTable::new(own);
        assert!(table.upsert(contact(1, 10)));
        assert!(!table.upsert(contact(1, 20)));
        assert_eq!(table.len(), 1);
        assert_eq!(table.contacts()[0].last_seen_ms, 20);
    }
}
