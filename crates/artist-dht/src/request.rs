//! Tracking of in-flight DHT requests.

use std::collections::HashMap;
use std::sync::Mutex;

use federation_net::EndpointId;
use tokio::sync::oneshot;

use crate::error::{ArtistDhtError, Result};
use crate::message::{
    FindNodeResponse, FindValueResponse, PongResponse, RequestId, StoreRecordResponse,
};

/// Maximum number of simultaneously pending requests.
pub const MAX_PENDING_REQUESTS: usize = 1024;

/// A response payload of any DHT request type.
#[derive(Debug)]
pub(crate) enum DhtResponse {
    Pong(PongResponse),
    FindNode(FindNodeResponse),
    FindValue(FindValueResponse),
    Store(StoreRecordResponse),
}

struct PendingEntry {
    /// The peer the response is expected from.
    peer: EndpointId,
    sender: oneshot::Sender<DhtResponse>,
}

/// Correlates responses with awaiting requesters.
///
/// Entries are removed when the response arrives, and the requester removes
/// its own entry on timeout, so the map cannot grow without bound; a hard cap
/// of [`MAX_PENDING_REQUESTS`] guards against bugs.
#[derive(Default)]
pub(crate) struct PendingRequests {
    map: Mutex<HashMap<RequestId, PendingEntry>>,
}

impl PendingRequests {
    /// Registers a new pending request and returns the receiver for its
    /// response.
    pub fn register(
        &self,
        request_id: RequestId,
        peer: EndpointId,
    ) -> Result<oneshot::Receiver<DhtResponse>> {
        let mut map = lock(&self.map);
        if map.len() >= MAX_PENDING_REQUESTS {
            return Err(ArtistDhtError::Protocol(
                "too many pending requests".to_string(),
            ));
        }
        let (sender, receiver) = oneshot::channel();
        map.insert(request_id, PendingEntry { peer, sender });
        Ok(receiver)
    }

    /// Completes a pending request with a response from `from_peer`.
    ///
    /// The response is delivered only if it comes from the peer the request
    /// was sent to; otherwise the entry stays and the stray response is
    /// dropped. Returns `true` if a waiting requester was resolved.
    pub fn complete(
        &self,
        request_id: &RequestId,
        from_peer: &EndpointId,
        response: DhtResponse,
    ) -> bool {
        let mut map = lock(&self.map);
        match map.get(request_id) {
            Some(entry) if &entry.peer == from_peer => {
                if let Some(entry) = map.remove(request_id) {
                    // The requester may have timed out already; that is fine.
                    let _ = entry.sender.send(response);
                    return true;
                }
                false
            }
            _ => false,
        }
    }

    /// Removes a pending request, e.g. after a timeout.
    pub fn remove(&self, request_id: &RequestId) {
        lock(&self.map).remove(request_id);
    }

    /// Number of currently pending requests.
    pub fn len(&self) -> usize {
        lock(&self.map).len()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::NodeId;

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn pong() -> DhtResponse {
        DhtResponse::Pong(PongResponse {
            node_id: NodeId::from_bytes([0u8; 32]),
        })
    }

    #[tokio::test]
    async fn response_resolves_pending_request() {
        let pending = PendingRequests::default();
        let peer = test_peer(1);
        let id = RequestId::random();
        let receiver = pending.register(id, peer).expect("register");
        assert!(pending.complete(&id, &peer, pong()));
        assert!(receiver.await.is_ok());
        assert_eq!(pending.len(), 0);
    }

    #[tokio::test]
    async fn response_from_wrong_peer_is_ignored() {
        let pending = PendingRequests::default();
        let peer = test_peer(1);
        let wrong = test_peer(2);
        let id = RequestId::random();
        let _receiver = pending.register(id, peer).expect("register");
        assert!(!pending.complete(&id, &wrong, pong()));
        // The entry is still pending for the right peer.
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn entry_is_removed_after_timeout() {
        let pending = PendingRequests::default();
        let peer = test_peer(1);
        let id = RequestId::random();
        let receiver = pending.register(id, peer).expect("register");
        // Simulate the requester timing out: it removes its own entry.
        let result = tokio::time::timeout(std::time::Duration::from_millis(20), receiver).await;
        assert!(result.is_err());
        pending.remove(&id);
        assert_eq!(pending.len(), 0);
        // A late response finds nothing to complete.
        assert!(!pending.complete(&id, &peer, pong()));
    }

    #[test]
    fn pending_map_is_capped() {
        let pending = PendingRequests::default();
        let peer = test_peer(1);
        let mut receivers = Vec::new();
        for _ in 0..MAX_PENDING_REQUESTS {
            receivers.push(
                pending
                    .register(RequestId::random(), peer)
                    .expect("register"),
            );
        }
        assert!(pending.register(RequestId::random(), peer).is_err());
    }
}
