//! The network engine: endpoint lifecycle, connection registry and messaging.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::config::NetworkConfig;
use crate::error::{NetworkError, Result};
use crate::event::{ConnectionDirection, NetworkEvent, NetworkEventReceiver};
use crate::identity;
use crate::protocol::{
    ALPN, Handshake, HandshakeAck, HandshakeErrorCode, MAX_HANDSHAKE_FRAME_SIZE,
    MessageRejectReason, MessageRequest, MessageResponse, NetworkId, PROTOCOL_VERSION, SchemaId,
};
use crate::rendezvous::{RendezvousClient, RendezvousConfig, now_ms};
use crate::ticket::{PeerTicket, TICKET_VERSION};
use crate::wire;

/// Close code for a graceful, error-free close.
const CLOSE_CODE_OK: VarInt = VarInt::from_u32(0);
/// Close code used when the handshake was rejected by either side.
const CLOSE_CODE_HANDSHAKE_REJECTED: VarInt = VarInt::from_u32(1);
/// Close code used when the engine shuts down.
const CLOSE_CODE_SHUTDOWN: VarInt = VarInt::from_u32(2);
/// Close code used when a connection is replaced by a newer one.
const CLOSE_CODE_REPLACED: VarInt = VarInt::from_u32(3);

/// Extra bytes allowed on top of `max_message_size` for message framing
/// (request id, length varints and enum tags).
const MESSAGE_FRAME_OVERHEAD: usize = 64;
/// Maximum size of an encoded [`MessageResponse`] frame.
const MAX_RESPONSE_FRAME_SIZE: usize = 1024;
/// How long the accepting side keeps a rejected connection open so the
/// rejection ack reaches the initiator before the connection is torn down.
const REJECT_LINGER: Duration = Duration::from_secs(3);
/// How long [`NetworkEngine::shutdown`] waits for background tasks before
/// aborting them.
const SHUTDOWN_TASK_GRACE: Duration = Duration::from_secs(5);
/// Upper bound of the rendezvous round delay while no peer is connected yet.
const RENDEZVOUS_LONELY_INTERVAL: Duration = Duration::from_secs(15);

/// Bounds required of a domain message type.
///
/// This trait is implemented automatically for every type that satisfies the
/// bounds; applications never implement it by hand.
pub trait Message: Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<T> Message for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}

/// Locks a mutex, recovering the guard if the mutex was poisoned.
///
/// The engine never panics while holding one of its locks, so poisoning can
/// only originate from a panic in unrelated user code; recovering is safe.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// State kept for every active peer connection.
struct PeerState {
    connection: Connection,
    direction: ConnectionDirection,
    semaphore: Arc<Semaphore>,
    /// Monotonic registration counter, used so a stale connection's cleanup
    /// never removes a newer connection to the same peer.
    generation: u64,
}

/// State shared between the engine handles, the protocol handler and all
/// background tasks.
struct Shared<M> {
    config: NetworkConfig,
    endpoint: Endpoint,
    router: Mutex<Option<Router>>,
    peers: Mutex<HashMap<EndpointId, PeerState>>,
    events: Mutex<Option<mpsc::Sender<NetworkEvent<M>>>>,
    tasks: Mutex<JoinSet<()>>,
    next_generation: AtomicU64,
    shutting_down: AtomicBool,
    /// Broadcasts the start of the shutdown to long-running background
    /// loops so they can stop promptly instead of being aborted.
    shutdown_signal: watch::Sender<bool>,
}

impl<M: Message> Shared<M> {
    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    fn ensure_running(&self) -> Result<()> {
        if self.is_shutting_down() {
            Err(NetworkError::ShuttingDown)
        } else {
            Ok(())
        }
    }

    /// Delivers an event to the application.
    ///
    /// The channel is bounded; if it is full this awaits until the
    /// application consumes events, applying natural back-pressure without
    /// unbounded buffering.
    async fn emit(&self, event: NetworkEvent<M>) {
        let sender = lock(&self.events).clone();
        if let Some(sender) = sender
            && sender.send(event).await.is_err()
        {
            debug!("event receiver dropped; event discarded");
        }
    }

    /// Spawns a background task tracked until shutdown.
    fn spawn_task<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut tasks = lock(&self.tasks);
        // Reap already-finished tasks so the set does not grow unboundedly.
        while tasks.try_join_next().is_some() {}
        tasks.spawn(future);
    }

    /// Adds a connection to the registry, replacing (and closing) any previous
    /// connection to the same peer. Returns the registration generation.
    fn register_connection(
        &self,
        peer_id: EndpointId,
        connection: Connection,
        direction: ConnectionDirection,
    ) -> u64 {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let state = PeerState {
            connection,
            direction,
            semaphore: Arc::new(Semaphore::new(self.config.max_concurrent_streams_per_peer)),
            generation,
        };
        let replaced = lock(&self.peers).insert(peer_id, state);
        if let Some(old) = replaced {
            debug!(peer = %peer_id, "replacing existing connection");
            old.connection
                .close(CLOSE_CODE_REPLACED, b"replaced by a new connection");
        }
        generation
    }

    /// Removes the connection from the registry and reports the disconnect,
    /// unless a newer connection to the same peer took its place.
    async fn cleanup_connection(
        &self,
        peer_id: EndpointId,
        generation: u64,
        reason: Option<String>,
    ) {
        let removed = {
            let mut peers = lock(&self.peers);
            match peers.get(&peer_id) {
                Some(state) if state.generation == generation => peers.remove(&peer_id),
                _ => None,
            }
        };
        if let Some(state) = removed {
            info!(
                peer = %peer_id,
                direction = ?state.direction,
                reason = reason.as_deref().unwrap_or("unknown"),
                "peer disconnected"
            );
            self.emit(NetworkEvent::PeerDisconnected { peer_id, reason })
                .await;
        }
    }

    /// Validates a remote handshake against the local configuration.
    fn validate_handshake(&self, handshake: &Handshake) -> Option<HandshakeErrorCode> {
        if handshake.protocol_version != PROTOCOL_VERSION {
            Some(HandshakeErrorCode::UnsupportedProtocolVersion)
        } else if handshake.network_id != self.config.network_id {
            Some(HandshakeErrorCode::NetworkMismatch)
        } else if handshake.schema_id != self.config.schema_id {
            Some(HandshakeErrorCode::SchemaMismatch)
        } else {
            None
        }
    }

    /// Drives an established connection: accepts message streams until the
    /// connection closes, then cleans up the registry entry.
    async fn connection_loop(
        self: Arc<Self>,
        peer_id: EndpointId,
        connection: Connection,
        generation: u64,
    ) {
        let semaphore = {
            let peers = lock(&self.peers);
            match peers.get(&peer_id) {
                Some(state) if state.generation == generation => state.semaphore.clone(),
                // The connection was already replaced; nothing to drive.
                _ => return,
            }
        };
        loop {
            let (send, recv) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(_) => break,
            };
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let shared = self.clone();
            self.spawn_task(async move {
                shared
                    .handle_message_stream(peer_id, send, recv, permit)
                    .await;
                // The permit is released when the task finishes.
            });
        }
        let reason = connection.close_reason().map(|err| err.to_string());
        self.cleanup_connection(peer_id, generation, reason).await;
    }

    /// Handles a single incoming message stream: decodes the request, hands
    /// the message to the application and acknowledges it.
    async fn handle_message_stream(
        &self,
        peer_id: EndpointId,
        mut send: SendStream,
        mut recv: RecvStream,
        _permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let request_timeout = self.config.request_timeout;
        let max_frame = self.config.max_message_size + MESSAGE_FRAME_OVERHEAD;
        let request: MessageRequest =
            match timeout(request_timeout, wire::read_frame(&mut recv, max_frame)).await {
                Ok(Ok(request)) => request,
                Ok(Err(err)) => {
                    self.report_protocol_error(Some(peer_id), format!("bad message frame: {err}"))
                        .await;
                    return;
                }
                Err(_) => {
                    self.report_protocol_error(
                        Some(peer_id),
                        "timed out reading message frame".to_string(),
                    )
                    .await;
                    return;
                }
            };
        let request_id = request.request_id;
        let response = if self.is_shutting_down() {
            MessageResponse::Rejected {
                request_id,
                reason: MessageRejectReason::ShuttingDown,
            }
        } else {
            match wire::decode::<M>(&request.payload) {
                Ok(message) => {
                    self.emit(NetworkEvent::MessageReceived { peer_id, message })
                        .await;
                    MessageResponse::Accepted { request_id }
                }
                Err(err) => {
                    self.report_protocol_error(
                        Some(peer_id),
                        format!("failed to decode domain message: {err}"),
                    )
                    .await;
                    MessageResponse::Rejected {
                        request_id,
                        reason: MessageRejectReason::MalformedPayload,
                    }
                }
            }
        };
        let write = timeout(
            request_timeout,
            wire::write_frame(&mut send, &response, MAX_RESPONSE_FRAME_SIZE),
        )
        .await;
        match write {
            Ok(Ok(())) => {
                let _ = send.finish();
            }
            Ok(Err(err)) => {
                debug!(peer = %peer_id, error = %err, "failed to send message response");
            }
            Err(_) => {
                debug!(peer = %peer_id, "timed out sending message response");
            }
        }
    }

    async fn report_protocol_error(&self, peer_id: Option<EndpointId>, error: String) {
        match peer_id {
            Some(peer) => warn!(peer = %peer, error = %error, "protocol error"),
            None => warn!(error = %error, "protocol error"),
        }
        self.emit(NetworkEvent::ProtocolError { peer_id, error })
            .await;
    }

    /// Handles a freshly accepted incoming connection: performs the handshake
    /// and, if accepted, drives the connection until it closes.
    async fn handle_incoming(self: &Arc<Self>, connection: &Connection) -> Result<()> {
        let peer_id = connection.remote_id();
        if self.is_shutting_down() {
            connection.close(CLOSE_CODE_SHUTDOWN, b"engine is shutting down");
            return Err(NetworkError::ShuttingDown);
        }
        debug!(peer = %peer_id, "incoming connection");
        let request_timeout = self.config.request_timeout;

        let (mut send, mut recv) = timeout(request_timeout, connection.accept_bi())
            .await
            .map_err(|_| NetworkError::Timeout)
            .and_then(|res| {
                res.map_err(|err| {
                    NetworkError::Transport(format!("failed to accept handshake stream: {err}"))
                })
            })
            .inspect_err(|_| {
                connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"handshake failed");
            })?;

        let handshake: Result<Handshake> = timeout(
            request_timeout,
            wire::read_frame(&mut recv, MAX_HANDSHAKE_FRAME_SIZE),
        )
        .await
        .map_err(|_| NetworkError::Timeout)
        .and_then(|res| res);

        let verdict = match &handshake {
            Ok(handshake) => self.validate_handshake(handshake),
            Err(_) => Some(HandshakeErrorCode::InvalidHandshake),
        };

        if let Some(code) = verdict {
            warn!(peer = %peer_id, code = ?code, "handshake rejected");
            let ack = HandshakeAck {
                accepted: false,
                error: Some(code),
            };
            let sent = timeout(
                request_timeout,
                wire::write_frame(&mut send, &ack, MAX_HANDSHAKE_FRAME_SIZE),
            )
            .await;
            if matches!(sent, Ok(Ok(()))) {
                let _ = send.finish();
                // Keep the connection open briefly so QUIC delivers the ack;
                // the initiator closes as soon as it has read the rejection.
                let _ = timeout(REJECT_LINGER, connection.closed()).await;
            }
            connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"handshake rejected");
            return Err(handshake_code_to_error(code));
        }

        let ack = HandshakeAck {
            accepted: true,
            error: None,
        };
        timeout(
            request_timeout,
            wire::write_frame(&mut send, &ack, MAX_HANDSHAKE_FRAME_SIZE),
        )
        .await
        .map_err(|_| NetworkError::Timeout)
        .and_then(|res| res)
        .inspect_err(|_| {
            connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"handshake failed");
        })?;
        let _ = send.finish();

        let generation =
            self.register_connection(peer_id, connection.clone(), ConnectionDirection::Incoming);
        info!(peer = %peer_id, "peer connected (incoming)");
        self.emit(NetworkEvent::PeerConnected {
            peer_id,
            direction: ConnectionDirection::Incoming,
        })
        .await;
        // Drive the connection inside the router's accept task; it runs for
        // as long as the connection lives.
        self.clone()
            .connection_loop(peer_id, connection.clone(), generation)
            .await;
        Ok(())
    }

    /// Dials `addr`, runs the client handshake and registers the connection.
    ///
    /// An existing healthy connection to the same peer is reused. The remote
    /// identity is taken from the authenticated Iroh connection, never from
    /// `addr` itself.
    async fn connect_to_addr(self: &Arc<Self>, addr: EndpointAddr) -> Result<EndpointId> {
        self.ensure_running()?;
        let target = addr.id;
        if lock(&self.peers).contains_key(&target) {
            debug!(peer = %target, "reusing existing connection");
            return Ok(target);
        }

        let request_timeout = self.config.request_timeout;
        let connection = timeout(request_timeout, self.endpoint.connect(addr, ALPN))
            .await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(|err| NetworkError::Transport(format!("failed to connect: {err}")))?;
        let peer_id = connection.remote_id();

        let handshake = timeout(request_timeout, self.run_client_handshake(&connection))
            .await
            .map_err(|_| NetworkError::Timeout)
            .and_then(|res| res);
        if let Err(err) = handshake {
            connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"handshake failed");
            return Err(err);
        }

        let generation =
            self.register_connection(peer_id, connection.clone(), ConnectionDirection::Outgoing);
        info!(peer = %peer_id, "peer connected (outgoing)");
        self.emit(NetworkEvent::PeerConnected {
            peer_id,
            direction: ConnectionDirection::Outgoing,
        })
        .await;
        let loop_shared = self.clone();
        self.spawn_task(async move {
            loop_shared
                .connection_loop(peer_id, connection, generation)
                .await;
        });
        Ok(peer_id)
    }

    /// Runs the client side of the handshake on a fresh outgoing connection.
    async fn run_client_handshake(&self, connection: &Connection) -> Result<()> {
        let (mut send, mut recv) = connection.open_bi().await.map_err(|err| {
            NetworkError::Transport(format!("failed to open handshake stream: {err}"))
        })?;
        let handshake = Handshake {
            protocol_version: PROTOCOL_VERSION,
            network_id: self.config.network_id,
            schema_id: self.config.schema_id,
        };
        wire::write_frame(&mut send, &handshake, MAX_HANDSHAKE_FRAME_SIZE).await?;
        let _ = send.finish();
        let ack: HandshakeAck = wire::read_frame(&mut recv, MAX_HANDSHAKE_FRAME_SIZE).await?;
        if ack.accepted {
            Ok(())
        } else {
            let code = ack.error.unwrap_or(HandshakeErrorCode::InvalidHandshake);
            Err(handshake_code_to_error(code))
        }
    }
}

/// Periodically publishes this peer to the network's rendezvous record and
/// dials every other peer found there.
///
/// Failures of a single round or dial are logged and retried on the next
/// round; the loop only ends when the engine shuts down (the task is
/// aborted).
async fn rendezvous_loop<M: Message>(
    shared: Arc<Shared<M>>,
    client: RendezvousClient,
    config: RendezvousConfig,
) {
    let mut shutdown = shared.shutdown_signal.subscribe();
    // Give the endpoint a moment to learn its relay and direct addresses so
    // the very first published record is already dialable.
    let _ = timeout(shared.config.request_timeout, shared.endpoint.online()).await;
    let self_id = shared.endpoint.id();
    loop {
        if shared.is_shutting_down() {
            return;
        }
        let round = async {
            let addr = shared.endpoint.addr();
            let self_addr = (!addr.is_empty()).then_some(addr);
            match client.round(self_addr, now_ms()).await {
                Ok(peers) => {
                    for peer_addr in peers {
                        let peer = peer_addr.id;
                        if peer == self_id || lock(&shared.peers).contains_key(&peer) {
                            continue;
                        }
                        debug!(peer = %peer, "rendezvous discovered a peer; connecting");
                        if let Err(err) = shared.connect_to_addr(peer_addr).await {
                            // Stale entries (peers that left) fail here; they
                            // age out of the record by TTL.
                            debug!(peer = %peer, error = %err, "rendezvous connect attempt failed");
                        }
                    }
                }
                Err(err) => debug!(error = %err, "rendezvous round failed"),
            }
        };
        tokio::select! {
            _ = round => {}
            _ = shutdown.changed() => return,
        }
        // Poll faster while the peer is still alone: joining a network
        // should not have to wait a full interval for the next round.
        let delay = if lock(&shared.peers).is_empty() {
            config.interval.min(RENDEZVOUS_LONELY_INTERVAL)
        } else {
            config.interval
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown.changed() => return,
        }
    }
}

fn handshake_code_to_error(code: HandshakeErrorCode) -> NetworkError {
    match code {
        HandshakeErrorCode::UnsupportedProtocolVersion => NetworkError::UnsupportedProtocolVersion,
        HandshakeErrorCode::NetworkMismatch => NetworkError::NetworkMismatch,
        HandshakeErrorCode::SchemaMismatch => NetworkError::SchemaMismatch,
        HandshakeErrorCode::InvalidHandshake => NetworkError::HandshakeRejected,
    }
}

/// Protocol handler registered with the Iroh [`Router`] for [`ALPN`].
///
/// Holds only a weak reference to the shared state so the router does not
/// keep the engine alive after all engine handles are dropped.
struct FederationProtocol<M> {
    shared: Weak<Shared<M>>,
}

impl<M> fmt::Debug for FederationProtocol<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FederationProtocol").finish_non_exhaustive()
    }
}

impl<M: Message> ProtocolHandler for FederationProtocol<M> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Some(shared) = self.shared.upgrade() else {
            connection.close(CLOSE_CODE_SHUTDOWN, b"engine is gone");
            return Ok(());
        };
        let peer_id = connection.remote_id();
        if let Err(err) = shared.handle_incoming(&connection).await {
            shared
                .report_protocol_error(Some(peer_id), format!("incoming connection failed: {err}"))
                .await;
        }
        // Errors of a single connection never bring down the router; the
        // connection has already been closed with an appropriate code.
        Ok(())
    }
}

/// A generic peer-to-peer network engine on top of Iroh.
///
/// `M` is the application-defined domain message type; the engine treats it
/// as an opaque, postcard-serializable payload. The engine is cheaply
/// clonable: all clones share the same endpoint and connection registry.
pub struct NetworkEngine<M> {
    shared: Arc<Shared<M>>,
}

impl<M> Clone for NetworkEngine<M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<M> fmt::Debug for NetworkEngine<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NetworkEngine").finish_non_exhaustive()
    }
}

impl<M: Message> NetworkEngine<M> {
    /// Starts the engine: loads (or creates) the persistent identity, binds
    /// the Iroh endpoint and starts accepting connections.
    ///
    /// Returns the engine handle together with the single receiver for
    /// [`NetworkEvent`]s.
    pub async fn start(config: NetworkConfig) -> Result<(Self, NetworkEventReceiver<M>)> {
        let secret_key = identity::load_or_create(&config.data_dir).await?;
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(|err| NetworkError::Transport(format!("failed to bind endpoint: {err}")))?;
        let endpoint_id = endpoint.id();
        let (sender, receiver) = mpsc::channel(config.event_channel_capacity);
        let shared = Arc::new(Shared {
            config,
            endpoint: endpoint.clone(),
            router: Mutex::new(None),
            peers: Mutex::new(HashMap::new()),
            events: Mutex::new(Some(sender)),
            tasks: Mutex::new(JoinSet::new()),
            next_generation: AtomicU64::new(0),
            shutting_down: AtomicBool::new(false),
            shutdown_signal: watch::Sender::new(false),
        });
        let handler = FederationProtocol {
            shared: Arc::downgrade(&shared),
        };
        let router = Router::builder(endpoint).accept(ALPN, handler).spawn();
        *lock(&shared.router) = Some(router);
        if let Some(rendezvous) = shared.config.rendezvous.clone() {
            match RendezvousClient::new(shared.config.network_id, &rendezvous) {
                Ok(client) => {
                    let loop_shared = shared.clone();
                    shared.spawn_task(async move {
                        rendezvous_loop(loop_shared, client, rendezvous).await;
                    });
                }
                // Rendezvous is a convenience; the engine stays usable via
                // tickets even when the DHT client cannot start.
                Err(err) => warn!(error = %err, "peer rendezvous disabled"),
            }
        }
        info!(
            endpoint_id = %endpoint_id,
            network_id = %shared.config.network_id,
            schema_id = %shared.config.schema_id,
            "network engine started"
        );
        Ok((Self { shared }, NetworkEventReceiver::new(receiver)))
    }

    /// Returns the stable identifier of this peer.
    pub fn endpoint_id(&self) -> EndpointId {
        self.shared.endpoint.id()
    }

    /// Returns the network this engine participates in.
    pub fn network_id(&self) -> NetworkId {
        self.shared.config.network_id
    }

    /// Returns the message schema this engine uses.
    pub fn schema_id(&self) -> SchemaId {
        self.shared.config.schema_id
    }

    /// Creates a shareable [`PeerTicket`] for this peer.
    ///
    /// Waits (bounded by the configured request timeout) for the endpoint to
    /// come online so the ticket contains relay information whenever
    /// possible; on an isolated network the ticket falls back to the locally
    /// known direct addresses.
    pub async fn ticket(&self) -> Result<PeerTicket> {
        self.shared.ensure_running()?;
        let endpoint = &self.shared.endpoint;
        let has_relay = endpoint.addr().relay_urls().next().is_some();
        if !has_relay {
            let _ = timeout(self.shared.config.request_timeout, endpoint.online()).await;
        }
        let endpoint_addr = endpoint.addr();
        if endpoint_addr.is_empty() {
            return Err(NetworkError::Transport(
                "endpoint has no reachable addresses yet".to_string(),
            ));
        }
        Ok(PeerTicket {
            ticket_version: TICKET_VERSION,
            protocol_version: PROTOCOL_VERSION,
            network_id: self.shared.config.network_id,
            schema_id: self.shared.config.schema_id,
            endpoint_addr,
        })
    }

    /// Connects to the peer described by `ticket` and performs the handshake.
    ///
    /// Incompatible tickets (different network, schema or protocol version)
    /// are rejected before any connection is attempted. An existing healthy
    /// connection to the same peer is reused.
    pub async fn connect(&self, ticket: PeerTicket) -> Result<EndpointId> {
        let shared = &self.shared;
        shared.ensure_running()?;
        if ticket.ticket_version != TICKET_VERSION {
            return Err(NetworkError::InvalidTicket(format!(
                "unsupported ticket version {}",
                ticket.ticket_version
            )));
        }
        if ticket.protocol_version != PROTOCOL_VERSION {
            return Err(NetworkError::UnsupportedProtocolVersion);
        }
        if ticket.network_id != shared.config.network_id {
            return Err(NetworkError::NetworkMismatch);
        }
        if ticket.schema_id != shared.config.schema_id {
            return Err(NetworkError::SchemaMismatch);
        }
        if ticket.endpoint_id() == self.endpoint_id() {
            return Err(NetworkError::InvalidTicket(
                "the ticket points to this peer itself".to_string(),
            ));
        }
        shared.connect_to_addr(ticket.endpoint_addr).await
    }

    /// Sends a domain message to a connected peer.
    ///
    /// Returns only after the peer acknowledged the message. Fails with
    /// [`NetworkError::PeerNotConnected`] if there is no active connection to
    /// the peer; no implicit dialing or peer lookup is performed.
    pub async fn send(&self, peer: EndpointId, message: &M) -> Result<()> {
        let shared = &self.shared;
        shared.ensure_running()?;
        let connection = lock(&shared.peers)
            .get(&peer)
            .map(|state| state.connection.clone())
            .ok_or(NetworkError::PeerNotConnected(peer))?;
        let payload = wire::encode(message, shared.config.max_message_size)?;
        let request_id: [u8; 16] = rand::random();
        let request = MessageRequest {
            request_id,
            payload,
        };
        let max_frame = shared.config.max_message_size + MESSAGE_FRAME_OVERHEAD;
        timeout(shared.config.request_timeout, async move {
            let (mut send, mut recv) = connection.open_bi().await.map_err(|err| {
                NetworkError::Transport(format!("failed to open message stream: {err}"))
            })?;
            wire::write_frame(&mut send, &request, max_frame).await?;
            let _ = send.finish();
            let response: MessageResponse =
                wire::read_frame(&mut recv, MAX_RESPONSE_FRAME_SIZE).await?;
            match response {
                MessageResponse::Accepted { request_id: id } if id == request_id => Ok(()),
                MessageResponse::Rejected {
                    request_id: id,
                    reason,
                } if id == request_id => Err(NetworkError::MessageRejected(reason.to_string())),
                _ => Err(NetworkError::Transport(
                    "peer responded to a different request".to_string(),
                )),
            }
        })
        .await
        .map_err(|_| NetworkError::Timeout)?
    }

    /// Returns the ids of all currently connected peers.
    pub fn connected_peers(&self) -> Vec<EndpointId> {
        lock(&self.shared.peers).keys().copied().collect()
    }

    /// Returns `true` if there is an active connection to `peer`.
    pub fn is_connected(&self, peer: EndpointId) -> bool {
        lock(&self.shared.peers).contains_key(&peer)
    }

    /// Closes the connection to `peer` and removes it from the registry.
    pub async fn disconnect(&self, peer: EndpointId) -> Result<()> {
        let state = lock(&self.shared.peers)
            .remove(&peer)
            .ok_or(NetworkError::PeerNotConnected(peer))?;
        state
            .connection
            .close(CLOSE_CODE_OK, b"disconnected by local peer");
        info!(peer = %peer, "peer disconnected (local request)");
        self.shared
            .emit(NetworkEvent::PeerDisconnected {
                peer_id: peer,
                reason: Some("disconnected by local peer".to_string()),
            })
            .await;
        Ok(())
    }

    /// Shuts the engine down gracefully.
    ///
    /// Rejects new operations, closes all connections, stops the protocol
    /// router, closes the endpoint, waits for background tasks and finally
    /// closes the event channel. Calling `shutdown` on several clones of the
    /// same engine is safe; only the first call does the work.
    pub async fn shutdown(self) -> Result<()> {
        let shared = &self.shared;
        if shared.shutting_down.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        info!("network engine shutting down");
        let _ = shared.shutdown_signal.send(true);

        // Close all active connections.
        let states: Vec<PeerState> = lock(&shared.peers)
            .drain()
            .map(|(_, state)| state)
            .collect();
        for state in &states {
            state
                .connection
                .close(CLOSE_CODE_SHUTDOWN, b"engine is shutting down");
        }
        drop(states);

        // Shut down the router; this stops the protocol handler and closes
        // the endpoint.
        let router = lock(&shared.router).take();
        if let Some(router) = router
            && let Err(err) = router.shutdown().await
        {
            warn!(error = %err, "router shutdown reported an error");
        }
        shared.endpoint.close().await;

        // Wait for the remaining background tasks; abort stragglers.
        let mut tasks = std::mem::take(&mut *lock(&shared.tasks));
        let drained = timeout(SHUTDOWN_TASK_GRACE, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            warn!("background tasks did not finish in time; aborting them");
            tasks.abort_all();
        }

        // Close the event channel: dropping the last sender makes
        // `NetworkEventReceiver::recv` return `None` once drained.
        *lock(&shared.events) = None;
        info!("network engine shut down");
        Ok(())
    }
}
