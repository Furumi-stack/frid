//! The DHT node: event handling, peer exchange, iterative lookups and
//! publication.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use federation_net::{EndpointId, NetworkEngine, NetworkEvent, NetworkEventReceiver, PeerTicket};
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::MusicDhtConfig;
use crate::database::Database;
use crate::dht::validate_store;
use crate::error::{MusicDhtError, Result};
use crate::message::{
    DHT_PROTOCOL_VERSION, FindNodeRequest, FindNodeResponse, FindValueRequest, FindValueResponse,
    Hello, MAX_PEER_EXCHANGE_CONTACTS, MusicDhtMessage, PeerExchange, PingRequest, PongResponse,
    RequestEnvelope, RequestId, ResponseEnvelope, StoreRecordRequest, StoreRecordResponse,
};
use crate::record::{ACTIVE_RECORD_TTL, DhtKey, LibraryItem, StoredRecord, TOMBSTONE_TTL, now_ms};
use crate::request::{DhtResponse, PendingRequests};
use crate::routing::{ALPHA, K, MAX_LOOKUP_REQUESTS, NodeContact, NodeId, RoutingTable, distance};
use crate::service::{MusicDhtEvent, PublishStats};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Base of the exponential backoff applied to a contact after a failed dial.
const DIAL_BACKOFF_BASE: Duration = Duration::from_secs(30);
/// Upper bound of the dial backoff.
const DIAL_BACKOFF_MAX: Duration = Duration::from_secs(10 * 60);
/// Consecutive failed dials after which a contact is evicted entirely.
const DIAL_FAILURES_BEFORE_EVICT: u32 = 5;

/// Dial-failure state of one currently unreachable contact.
#[derive(Debug, Clone, Copy)]
struct DialFailure {
    consecutive: u32,
    last_attempt_ms: u64,
}

/// How long a contact is skipped after `consecutive` failed dials:
/// 30s, 1m, 2m, 4m, 8m, then capped at 10 minutes.
fn dial_backoff(consecutive: u32) -> Duration {
    let exponent = consecutive.saturating_sub(1).min(8);
    DIAL_BACKOFF_BASE
        .saturating_mul(1u32 << exponent)
        .min(DIAL_BACKOFF_MAX)
}

/// An outbound DHT request, before it is wrapped in an envelope.
enum OutboundRequest {
    Ping,
    FindNode(FindNodeRequest),
    FindValue(FindValueRequest),
    Store(Box<StoreRecordRequest>),
}

/// Result of one iterative lookup.
pub(crate) struct LookupOutcome {
    /// Records found (value lookups only).
    pub records: Vec<StoredRecord>,
    /// Closest known contacts to the target, best first, at most `K`.
    pub closest: Vec<NodeContact>,
    /// Number of distinct peers actually queried.
    pub queried: usize,
    /// Number of distinct nodes known to the lookup (seeds + discovered).
    pub discovered: usize,
}

/// Shared state of one DHT node.
pub(crate) struct Node {
    pub engine: NetworkEngine<MusicDhtMessage>,
    pub db: Database,
    pub config: MusicDhtConfig,
    pub node_id: NodeId,
    pub endpoint_id: EndpointId,
    routing: Mutex<RoutingTable>,
    pending: PendingRequests,
    /// Peers we already introduced ourselves to (per connection).
    hello_sent: Mutex<HashSet<EndpointId>>,
    /// Peers we already gossiped contacts to (per connection).
    exchange_sent: Mutex<HashSet<EndpointId>>,
    /// Contacts that recently failed to dial, with their backoff state.
    dial_failures: Mutex<HashMap<EndpointId, DialFailure>>,
    events: Mutex<Option<mpsc::Sender<MusicDhtEvent>>>,
    /// Set once the post-startup republish has been triggered.
    initial_republish_done: AtomicBool,
    shutting_down: AtomicBool,
}

impl Node {
    pub fn new(
        engine: NetworkEngine<MusicDhtMessage>,
        db: Database,
        config: MusicDhtConfig,
        events: mpsc::Sender<MusicDhtEvent>,
    ) -> Self {
        let endpoint_id = engine.endpoint_id();
        let node_id = NodeId::from_endpoint(&endpoint_id);
        Self {
            engine,
            db,
            config,
            node_id,
            endpoint_id,
            routing: Mutex::new(RoutingTable::new(node_id)),
            pending: PendingRequests::default(),
            hello_sent: Mutex::new(HashSet::new()),
            exchange_sent: Mutex::new(HashSet::new()),
            dial_failures: Mutex::new(HashMap::new()),
            events: Mutex::new(Some(events)),
            initial_republish_done: AtomicBool::new(false),
            shutting_down: AtomicBool::new(false),
        }
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
        *lock(&self.events) = None;
    }

    pub fn ensure_running(&self) -> Result<()> {
        if self.is_shutting_down() {
            Err(MusicDhtError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    async fn emit(&self, event: MusicDhtEvent) {
        let sender = lock(&self.events).clone();
        if let Some(sender) = sender {
            let _ = sender.send(event).await;
        }
    }

    /// All known DHT contacts.
    pub fn known_contacts(&self) -> Vec<NodeContact> {
        lock(&self.routing).contacts()
    }

    /// Seeds the routing table (used at startup with persisted contacts).
    pub fn seed_contacts(&self, contacts: Vec<NodeContact>) {
        let mut routing = lock(&self.routing);
        for contact in contacts {
            if contact.peer_id != self.endpoint_id {
                routing.upsert(contact);
            }
        }
    }

    /// Adds or refreshes a contact learned from the network.
    ///
    /// The node id is always re-derived from the endpoint id instead of
    /// trusting the gossiped value. The first contact ever learned triggers
    /// the post-startup republish.
    async fn upsert_contact(self: &Arc<Self>, peer_id: EndpointId, ticket: String) {
        if peer_id == self.endpoint_id {
            return;
        }
        let contact = NodeContact {
            node_id: NodeId::from_endpoint(&peer_id),
            peer_id,
            ticket,
            last_seen_ms: now_ms(),
        };
        let is_new = lock(&self.routing).upsert(contact.clone());
        if let Err(err) = self.db.upsert_known_peer(&contact).await {
            warn!(error = %err, "failed to persist known peer");
        }
        if is_new {
            info!(peer = %contact.peer_id, node = %contact.node_id, "learned new DHT contact");
            self.emit(MusicDhtEvent::ContactDiscovered {
                contact: contact.clone(),
            })
            .await;
        }
        self.maybe_trigger_initial_republish();
    }

    /// Spawns the post-startup republish once at least one contact is known.
    pub fn maybe_trigger_initial_republish(self: &Arc<Self>) {
        if lock(&self.routing).is_empty() || self.is_shutting_down() {
            return;
        }
        if self.initial_republish_done.swap(true, Ordering::SeqCst) {
            return;
        }
        let node = self.clone();
        tokio::spawn(async move {
            match node.republish_all().await {
                Ok(stats) => info!(
                    records = stats.records,
                    keys = stats.keys,
                    nodes = stats.remote_nodes,
                    "post-startup republish finished"
                ),
                Err(err) => warn!(error = %err, "post-startup republish failed"),
            }
        });
    }

    /// Consumes `federation-net` events until the engine shuts down.
    pub async fn run_event_loop(
        self: Arc<Self>,
        mut receiver: NetworkEventReceiver<MusicDhtMessage>,
    ) {
        while let Some(event) = receiver.recv().await {
            match event {
                NetworkEvent::PeerConnected { peer_id, .. } => {
                    debug!(peer = %peer_id, "peer connected");
                    self.clear_dial_failures(&peer_id);
                    self.emit(MusicDhtEvent::PeerConnected { peer_id }).await;
                    self.send_hello(peer_id).await;
                }
                NetworkEvent::PeerDisconnected { peer_id, .. } => {
                    debug!(peer = %peer_id, "peer disconnected");
                    lock(&self.hello_sent).remove(&peer_id);
                    lock(&self.exchange_sent).remove(&peer_id);
                    self.emit(MusicDhtEvent::PeerDisconnected { peer_id }).await;
                }
                NetworkEvent::MessageReceived { peer_id, message } => {
                    self.on_message(peer_id, message).await;
                }
                NetworkEvent::ProtocolError { peer_id, error } => {
                    self.emit(MusicDhtEvent::Error {
                        message: match peer_id {
                            Some(peer) => format!("transport error with {peer}: {error}"),
                            None => format!("transport error: {error}"),
                        },
                    })
                    .await;
                }
            }
        }
        debug!("network event loop finished");
    }

    async fn send_message(&self, peer: EndpointId, message: &MusicDhtMessage) -> Result<()> {
        self.engine.send(peer, message).await.map_err(Into::into)
    }

    async fn send_hello(self: &Arc<Self>, peer: EndpointId) {
        // Mark before sending so a crossing Hello does not trigger an echo.
        if !lock(&self.hello_sent).insert(peer) {
            return;
        }
        let ticket = match self.engine.ticket().await {
            Ok(ticket) => ticket.to_string(),
            Err(err) => {
                warn!(error = %err, "cannot create own ticket for hello");
                lock(&self.hello_sent).remove(&peer);
                return;
            }
        };
        let hello = MusicDhtMessage::Hello(Hello {
            node_id: self.node_id,
            peer_id: self.endpoint_id,
            ticket,
            protocol_version: DHT_PROTOCOL_VERSION,
        });
        if let Err(err) = self.send_message(peer, &hello).await {
            debug!(peer = %peer, error = %err, "failed to send hello");
            lock(&self.hello_sent).remove(&peer);
        }
    }

    async fn send_peer_exchange(self: &Arc<Self>, peer: EndpointId) {
        if !lock(&self.exchange_sent).insert(peer) {
            return;
        }
        let mut peers: Vec<NodeContact> = self
            .known_contacts()
            .into_iter()
            .filter(|contact| contact.peer_id != peer && contact.peer_id != self.endpoint_id)
            .collect();
        // Prefer the most recently seen contacts.
        peers.sort_by_key(|contact| std::cmp::Reverse(contact.last_seen_ms));
        peers.truncate(MAX_PEER_EXCHANGE_CONTACTS);
        if peers.is_empty() {
            return;
        }
        debug!(peer = %peer, count = peers.len(), "sending peer exchange");
        let message = MusicDhtMessage::PeerExchange(PeerExchange { peers });
        if let Err(err) = self.send_message(peer, &message).await {
            debug!(peer = %peer, error = %err, "failed to send peer exchange");
        }
    }

    async fn on_message(self: &Arc<Self>, peer: EndpointId, message: MusicDhtMessage) {
        lock(&self.routing).touch(&peer, now_ms());
        match message {
            MusicDhtMessage::Hello(hello) => self.on_hello(peer, hello).await,
            MusicDhtMessage::PeerExchange(exchange) => {
                self.on_peer_exchange(peer, exchange).await;
            }
            MusicDhtMessage::Ping(env) => {
                let response = MusicDhtMessage::Pong(ResponseEnvelope {
                    request_id: env.request_id,
                    payload: PongResponse {
                        node_id: self.node_id,
                    },
                });
                let _ = self.send_message(peer, &response).await;
            }
            MusicDhtMessage::FindNode(env) => {
                let nodes = self.closest_for_response(env.payload.target.as_bytes(), &peer);
                let response = MusicDhtMessage::FindNodeResult(ResponseEnvelope {
                    request_id: env.request_id,
                    payload: FindNodeResponse { nodes },
                });
                let _ = self.send_message(peer, &response).await;
            }
            MusicDhtMessage::FindValue(env) => {
                let payload = self.answer_find_value(&env.payload, &peer).await;
                let response = MusicDhtMessage::FindValueResult(ResponseEnvelope {
                    request_id: env.request_id,
                    payload,
                });
                let _ = self.send_message(peer, &response).await;
            }
            MusicDhtMessage::StoreRecord(env) => {
                let stored = self.answer_store(env.payload, &peer).await;
                let response = MusicDhtMessage::StoreRecordResult(ResponseEnvelope {
                    request_id: env.request_id,
                    payload: StoreRecordResponse { stored },
                });
                let _ = self.send_message(peer, &response).await;
            }
            MusicDhtMessage::Pong(env) => {
                self.pending
                    .complete(&env.request_id, &peer, DhtResponse::Pong(env.payload));
            }
            MusicDhtMessage::FindNodeResult(env) => {
                self.pending
                    .complete(&env.request_id, &peer, DhtResponse::FindNode(env.payload));
            }
            MusicDhtMessage::FindValueResult(env) => {
                self.pending
                    .complete(&env.request_id, &peer, DhtResponse::FindValue(env.payload));
            }
            MusicDhtMessage::StoreRecordResult(env) => {
                self.pending
                    .complete(&env.request_id, &peer, DhtResponse::Store(env.payload));
            }
        }
    }

    async fn on_hello(self: &Arc<Self>, peer: EndpointId, hello: Hello) {
        if hello.protocol_version != DHT_PROTOCOL_VERSION {
            warn!(peer = %peer, version = hello.protocol_version, "unsupported DHT protocol version");
            return;
        }
        // The authenticated identity comes from the connection; the id fields
        // inside the payload must be consistent with it.
        if hello.peer_id != peer || hello.node_id != NodeId::from_endpoint(&peer) {
            warn!(peer = %peer, "hello with inconsistent identity; ignoring");
            self.emit(MusicDhtEvent::Error {
                message: format!("peer {peer} sent a hello with a mismatched identity"),
            })
            .await;
            return;
        }
        debug!(peer = %peer, "received hello");
        self.upsert_contact(peer, hello.ticket).await;
        // Introduce ourselves if the remote connected first, then gossip.
        self.send_hello(peer).await;
        self.send_peer_exchange(peer).await;
    }

    async fn on_peer_exchange(self: &Arc<Self>, peer: EndpointId, exchange: PeerExchange) {
        let contacts = sanitize_peer_exchange(self.endpoint_id, peer, exchange.peers);
        let accepted = contacts.len();
        for contact in contacts {
            self.upsert_contact(contact.peer_id, contact.ticket).await;
        }
        debug!(peer = %peer, accepted, "processed peer exchange");
    }

    /// Contacts for a FindNode/FindValue response: closest to the target,
    /// excluding the requester itself.
    fn closest_for_response(&self, target: &[u8; 32], requester: &EndpointId) -> Vec<NodeContact> {
        lock(&self.routing)
            .closest(target, K + 1)
            .into_iter()
            .filter(|contact| &contact.peer_id != requester)
            .take(K)
            .collect()
    }

    async fn answer_find_value(
        &self,
        request: &FindValueRequest,
        requester: &EndpointId,
    ) -> FindValueResponse {
        match self.db.dht_records_by_key(request.key, now_ms()).await {
            Ok(records) if !records.is_empty() => FindValueResponse::Records { records },
            Ok(_) => FindValueResponse::CloserNodes {
                nodes: self.closest_for_response(request.key.as_bytes(), requester),
            },
            Err(err) => {
                warn!(error = %err, "find-value lookup in the local store failed");
                FindValueResponse::CloserNodes {
                    nodes: self.closest_for_response(request.key.as_bytes(), requester),
                }
            }
        }
    }

    async fn answer_store(&self, request: StoreRecordRequest, sender: &EndpointId) -> bool {
        if self.is_shutting_down() {
            return false;
        }
        let key = request.key;
        match validate_store(request, &self.config.network_id, now_ms()) {
            Ok(record) => {
                let artist_id = record.item.id;
                let deleted = record.item.deleted;
                match self.db.store_dht_record(key, record).await {
                    Ok(stored) => {
                        if stored {
                            info!(
                                item = %artist_id,
                                tombstone = deleted,
                                from = %sender,
                                "stored DHT record"
                            );
                        }
                        stored
                    }
                    Err(err) => {
                        warn!(error = %err, "failed to store DHT record");
                        false
                    }
                }
            }
            Err(reason) => {
                warn!(from = %sender, reason = %reason, "rejected DHT store request");
                false
            }
        }
    }

    /// Makes sure a connection to the contact exists, dialing its ticket if
    /// necessary. The Hello exchange runs asynchronously via the event loop.
    ///
    /// On-demand dials are bounded by the (short) `dial_timeout` and feed the
    /// dial-failure backoff, so unreachable contacts cannot stall lookups.
    async fn ensure_connected(&self, contact: &NodeContact) -> Result<EndpointId> {
        self.ensure_running()?;
        if self.engine.is_connected(contact.peer_id) {
            return Ok(contact.peer_id);
        }
        let ticket: PeerTicket = contact
            .ticket
            .parse()
            .map_err(|err| MusicDhtError::InvalidTicket(format!("{err}")))?;
        debug!(peer = %contact.peer_id, "connecting on demand");
        let result = timeout(self.config.dial_timeout, self.engine.connect(ticket))
            .await
            .map_err(|_| MusicDhtError::Timeout)
            .and_then(|res| res.map_err(Into::into));
        match result {
            Ok(peer) => {
                self.clear_dial_failures(&peer);
                Ok(peer)
            }
            Err(err) => {
                self.note_dial_failure(contact).await;
                Err(err)
            }
        }
    }

    /// Forgets the dial-failure history of a peer (it proved reachable).
    fn clear_dial_failures(&self, peer: &EndpointId) {
        lock(&self.dial_failures).remove(peer);
    }

    /// `true` if the contact recently failed to dial and its backoff window
    /// has not elapsed yet. Connected peers are never considered backed off.
    fn dial_backoff_active(&self, peer: &EndpointId, now_ms: u64) -> bool {
        if self.engine.is_connected(*peer) {
            return false;
        }
        match lock(&self.dial_failures).get(peer) {
            Some(failure) => {
                let backoff = dial_backoff(failure.consecutive).as_millis() as u64;
                now_ms < failure.last_attempt_ms.saturating_add(backoff)
            }
            None => false,
        }
    }

    /// Records a failed dial; after [`DIAL_FAILURES_BEFORE_EVICT`] failures
    /// in a row the contact is dropped from the routing table and the
    /// database (gossip re-adds it with a clean slate if it comes back).
    async fn note_dial_failure(&self, contact: &NodeContact) {
        let consecutive = {
            let mut failures = lock(&self.dial_failures);
            let failure = failures.entry(contact.peer_id).or_insert(DialFailure {
                consecutive: 0,
                last_attempt_ms: 0,
            });
            failure.consecutive += 1;
            failure.last_attempt_ms = now_ms();
            failure.consecutive
        };
        if consecutive < DIAL_FAILURES_BEFORE_EVICT {
            debug!(
                peer = %contact.peer_id,
                consecutive,
                backoff_s = dial_backoff(consecutive).as_secs(),
                "dial failed; backing off"
            );
            return;
        }
        lock(&self.dial_failures).remove(&contact.peer_id);
        lock(&self.routing).remove(&contact.peer_id);
        if let Err(err) = self.db.delete_known_peer(contact.peer_id).await {
            warn!(error = %err, "failed to delete evicted peer from the database");
        }
        info!(peer = %contact.peer_id, "evicted unreachable DHT contact");
    }

    /// Sends one request and awaits its response, cleaning up the pending
    /// entry on timeout.
    async fn request(
        &self,
        contact: &NodeContact,
        request: OutboundRequest,
    ) -> Result<DhtResponse> {
        let peer = self.ensure_connected(contact).await?;
        let request_id = RequestId::random();
        let receiver = self.pending.register(request_id, peer)?;
        // Lookups may cancel this future (early exit); the guard makes sure
        // the pending entry never outlives it.
        let _cleanup = self.pending.remove_on_drop(request_id);
        tracing::trace!(pending = self.pending.len(), peer = %peer, "sending DHT request");
        let message = match request {
            OutboundRequest::Ping => MusicDhtMessage::Ping(RequestEnvelope {
                request_id,
                payload: PingRequest {},
            }),
            OutboundRequest::FindNode(payload) => MusicDhtMessage::FindNode(RequestEnvelope {
                request_id,
                payload,
            }),
            OutboundRequest::FindValue(payload) => MusicDhtMessage::FindValue(RequestEnvelope {
                request_id,
                payload,
            }),
            OutboundRequest::Store(payload) => MusicDhtMessage::StoreRecord(RequestEnvelope {
                request_id,
                payload: *payload,
            }),
        };
        self.send_message(peer, &message).await?;
        match timeout(self.config.request_timeout, receiver).await {
            Ok(Ok(response)) => {
                lock(&self.routing).touch(&peer, now_ms());
                Ok(response)
            }
            Ok(Err(_)) => Err(MusicDhtError::Protocol("response channel closed".into())),
            Err(_) => Err(MusicDhtError::Timeout),
        }
    }

    /// Measures the round-trip time to a known contact and verifies its
    /// DHT identity.
    pub async fn ping(&self, contact: &NodeContact) -> Result<std::time::Duration> {
        let started = Instant::now();
        match self.request(contact, OutboundRequest::Ping).await? {
            DhtResponse::Pong(pong) => {
                if pong.node_id != NodeId::from_endpoint(&contact.peer_id) {
                    return Err(MusicDhtError::Protocol(
                        "pong with a mismatched node id".into(),
                    ));
                }
                lock(&self.routing).touch(&contact.peer_id, now_ms());
                Ok(started.elapsed())
            }
            _ => Err(MusicDhtError::Protocol(
                "unexpected response to ping".into(),
            )),
        }
    }

    /// Iterative Kademlia-style lookup.
    ///
    /// With `find_value: None` this is a node lookup converging on the
    /// closest known nodes to `target`; with `Some(key)` it sends `FindValue`
    /// and returns as soon as the **first** records arrive — in-flight
    /// requests to slower or unreachable peers are cancelled instead of
    /// awaited. Contacts in dial backoff are skipped. Never broadcasts: at
    /// most [`ALPHA`] requests run concurrently and at most
    /// [`MAX_LOOKUP_REQUESTS`] are sent in total, all hard-bounded by the
    /// lookup timeout.
    pub async fn lookup(&self, target: [u8; 32], find_value: Option<DhtKey>) -> LookupOutcome {
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + self.config.lookup_timeout;
        let mut candidates: Vec<NodeContact> = lock(&self.routing).closest(&target, K);
        let mut known: HashSet<EndpointId> =
            candidates.iter().map(|contact| contact.peer_id).collect();
        let mut queried: HashSet<EndpointId> = HashSet::new();
        let mut records: HashMap<(crate::record::ItemId, EndpointId), StoredRecord> =
            HashMap::new();
        let mut sent = 0usize;

        debug!(target = %NodeId::from_bytes(target), seeds = candidates.len(), "lookup started");

        'rounds: loop {
            if tokio::time::Instant::now() >= deadline {
                debug!("lookup deadline reached");
                break;
            }
            candidates.sort_by_key(|contact| distance(contact.node_id.as_bytes(), &target));
            let budget = MAX_LOOKUP_REQUESTS.saturating_sub(sent);
            let round_now = now_ms();
            let batch: Vec<NodeContact> = candidates
                .iter()
                .filter(|contact| {
                    !queried.contains(&contact.peer_id)
                        && !self.dial_backoff_active(&contact.peer_id, round_now)
                })
                .take(ALPHA.min(budget))
                .cloned()
                .collect();
            if batch.is_empty() {
                break;
            }
            sent += batch.len();
            for contact in &batch {
                queried.insert(contact.peer_id);
            }

            // Process responses as they complete: one dead contact must not
            // hold back the answers of the live ones.
            let mut in_flight: FuturesUnordered<_> = batch
                .iter()
                .map(|contact| async move {
                    let request = match find_value {
                        Some(key) => OutboundRequest::FindValue(FindValueRequest { key }),
                        None => OutboundRequest::FindNode(FindNodeRequest {
                            target: NodeId::from_bytes(target),
                        }),
                    };
                    (contact, self.request(contact, request).await)
                })
                .collect();
            while let Ok(next) = tokio::time::timeout_at(deadline, in_flight.next()).await {
                let Some((contact, result)) = next else {
                    break; // The round is complete.
                };
                let mut found_records = false;
                let nodes = match result {
                    Ok(DhtResponse::FindNode(response)) => response.nodes,
                    Ok(DhtResponse::FindValue(FindValueResponse::Records { records: found })) => {
                        for record in found {
                            let key = (record.item.id, record.item.owner);
                            match records.get(&key) {
                                Some(existing) if !record_supersedes(&record, existing) => {}
                                _ => {
                                    records.insert(key, record);
                                }
                            }
                        }
                        found_records = true;
                        Vec::new()
                    }
                    Ok(DhtResponse::FindValue(FindValueResponse::CloserNodes { nodes })) => nodes,
                    Ok(_) => Vec::new(),
                    Err(err) => {
                        debug!(peer = %contact.peer_id, error = %err, "lookup request failed");
                        Vec::new()
                    }
                };
                for node in nodes.into_iter().take(K) {
                    if node.peer_id == self.endpoint_id || !known.insert(node.peer_id) {
                        continue;
                    }
                    // Re-derive the node id instead of trusting gossip.
                    candidates.push(NodeContact {
                        node_id: NodeId::from_endpoint(&node.peer_id),
                        peer_id: node.peer_id,
                        ticket: node.ticket,
                        last_seen_ms: now_ms(),
                    });
                }
                if find_value.is_some() && found_records {
                    // Dropping `in_flight` cancels the outstanding requests.
                    break 'rounds;
                }
            }
            if tokio::time::Instant::now() >= deadline {
                debug!("lookup deadline reached");
                break;
            }
            if sent >= MAX_LOOKUP_REQUESTS {
                debug!("lookup request budget exhausted");
                break;
            }
        }

        // The closest set feeds publishes (store targets); contacts that are
        // currently backed off would only burn a dial timeout each.
        let closest_now = now_ms();
        candidates.retain(|contact| !self.dial_backoff_active(&contact.peer_id, closest_now));
        candidates.sort_by_key(|contact| distance(contact.node_id.as_bytes(), &target));
        candidates.truncate(K);
        info!(
            queried = queried.len(),
            discovered = known.len(),
            records = records.len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "lookup finished"
        );
        LookupOutcome {
            records: records.into_values().collect(),
            closest: candidates,
            queried: queried.len(),
            discovered: known.len(),
        }
    }

    /// Publishes one item record (active or tombstone) under all its DHT
    /// keys to the closest known nodes. Returns
    /// `(keys, remote nodes stored, local replica stored)`.
    pub async fn publish_item(&self, item: &LibraryItem) -> Result<PublishStats> {
        self.ensure_running()?;
        let ttl = if item.deleted {
            TOMBSTONE_TTL
        } else {
            ACTIVE_RECORD_TTL
        };
        let record = StoredRecord {
            item: item.clone(),
            publisher: self.endpoint_id,
            expires_at_ms: now_ms() + ttl.as_millis() as u64,
        };
        let keys = item.dht_keys(&self.config.network_id);
        let mut remote_nodes: HashSet<EndpointId> = HashSet::new();
        let mut local_replica = false;

        for key in &keys {
            let outcome = self.lookup(*key.as_bytes(), None).await;
            let targets = outcome.closest;

            // The record belongs on this node too if it is among the K
            // closest (always true while the network is smaller than K).
            let own_distance = distance(self.node_id.as_bytes(), key.as_bytes());
            let self_is_close = targets.len() < K
                || targets.last().is_none_or(|farthest| {
                    own_distance <= distance(farthest.node_id.as_bytes(), key.as_bytes())
                });
            if self_is_close {
                match self.db.store_dht_record(*key, record.clone()).await {
                    Ok(_) => local_replica = true,
                    Err(err) => warn!(error = %err, "failed to store own replica"),
                }
            }

            let stores = targets.iter().map(|contact| async {
                let result = self
                    .request(
                        contact,
                        OutboundRequest::Store(Box::new(StoreRecordRequest {
                            key: *key,
                            record: record.clone(),
                        })),
                    )
                    .await;
                (contact.peer_id, result)
            });
            for (peer, result) in join_all(stores).await {
                match result {
                    Ok(DhtResponse::Store(StoreRecordResponse { stored: true })) => {
                        remote_nodes.insert(peer);
                    }
                    Ok(DhtResponse::Store(StoreRecordResponse { stored: false })) => {
                        debug!(peer = %peer, "peer declined to store the record");
                    }
                    Ok(_) => {}
                    Err(err) => debug!(peer = %peer, error = %err, "store request failed"),
                }
            }
        }
        info!(
            item = %item.id,
            tombstone = item.deleted,
            keys = keys.len(),
            nodes = remote_nodes.len(),
            "published item record"
        );
        Ok(PublishStats {
            records: 1,
            keys: keys.len(),
            remote_nodes: remote_nodes.len(),
            local_replica,
        })
    }

    /// Republishes every local record that is still alive.
    pub async fn republish_all(&self) -> Result<PublishStats> {
        self.ensure_running()?;
        let items = self.db.local_items_for_republish(now_ms()).await?;
        let mut total = PublishStats::default();
        for item in &items {
            let stats = self.publish_item(item).await?;
            total.records += 1;
            total.keys += stats.keys;
            // remote_nodes counts unique nodes per record; report the widest
            // replication seen across records.
            total.remote_nodes = total.remote_nodes.max(stats.remote_nodes);
            total.local_replica |= stats.local_replica;
        }
        info!(
            records = total.records,
            keys = total.keys,
            "republish finished"
        );
        Ok(total)
    }

    /// Drops expired replicas from the local store.
    pub async fn sweep_expired(&self) {
        match self.db.delete_expired_records(now_ms()).await {
            Ok(0) => {}
            Ok(count) => info!(count, "removed expired DHT records"),
            Err(err) => warn!(error = %err, "failed to sweep expired records"),
        }
    }
}

/// `true` if `candidate` should replace `existing` in a search result set.
pub(crate) fn record_supersedes(candidate: &StoredRecord, existing: &StoredRecord) -> bool {
    let (c, e) = (&candidate.item, &existing.item);
    c.revision > e.revision || (c.revision == e.revision && c.deleted && !e.deleted)
}

/// Filters an incoming peer-exchange batch: drops our own contact, the
/// sender's contact and duplicate endpoint ids, and enforces the batch cap.
pub(crate) fn sanitize_peer_exchange(
    own: EndpointId,
    sender: EndpointId,
    peers: Vec<NodeContact>,
) -> Vec<NodeContact> {
    let mut seen: HashSet<EndpointId> = HashSet::new();
    peers
        .into_iter()
        .take(MAX_PEER_EXCHANGE_CONTACTS)
        .filter(|contact| {
            contact.peer_id != own && contact.peer_id != sender && seen.insert(contact.peer_id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_peer(seed: u8) -> EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn contact(seed: u8) -> NodeContact {
        let peer = test_peer(seed);
        NodeContact {
            node_id: NodeId::from_endpoint(&peer),
            peer_id: peer,
            ticket: format!("fnet-test-{seed}"),
            last_seen_ms: 0,
        }
    }

    #[test]
    fn dial_backoff_doubles_and_is_capped() {
        assert_eq!(dial_backoff(1), Duration::from_secs(30));
        assert_eq!(dial_backoff(2), Duration::from_secs(60));
        assert_eq!(dial_backoff(3), Duration::from_secs(120));
        assert_eq!(dial_backoff(5), Duration::from_secs(480));
        // Capped at the maximum from the 6th failure on, even for huge counts.
        assert_eq!(dial_backoff(6), DIAL_BACKOFF_MAX);
        assert_eq!(dial_backoff(u32::MAX), DIAL_BACKOFF_MAX);
    }

    #[test]
    fn peer_exchange_drops_duplicates_self_and_sender() {
        let own = test_peer(1);
        let sender = test_peer(2);
        let peers = vec![
            contact(3),
            contact(3), // duplicate
            contact(1), // ourselves
            contact(2), // the sender
            contact(4),
        ];
        let sanitized = sanitize_peer_exchange(own, sender, peers);
        let ids: Vec<EndpointId> = sanitized.iter().map(|c| c.peer_id).collect();
        assert_eq!(ids, vec![test_peer(3), test_peer(4)]);
    }

    #[test]
    fn peer_exchange_is_capped() {
        let own = test_peer(1);
        let sender = test_peer(2);
        let peers: Vec<NodeContact> = (10..10 + MAX_PEER_EXCHANGE_CONTACTS as u8 + 8)
            .map(contact)
            .collect();
        let sanitized = sanitize_peer_exchange(own, sender, peers);
        assert_eq!(sanitized.len(), MAX_PEER_EXCHANGE_CONTACTS);
    }
}
