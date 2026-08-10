//! The network engine: endpoint lifecycle, connection registry and messaging.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt, presets};
use iroh::protocol::{AcceptError, ProtocolHandler, Router};
use iroh::{Endpoint, EndpointAddr, EndpointId, SecretKey, Signature};
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
/// Capacity of the queue of accepted-but-not-yet-consumed incoming byte
/// streams, per stream protocol. A full queue delays the handshake ack of
/// further incoming streams (natural backpressure).
const STREAM_ACCEPT_QUEUE: usize = 16;

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

/// A raw bidirectional byte stream to a peer, running over a dedicated
/// connection on an auxiliary ALPN.
///
/// Returned by [`NetworkEngine::open_stream`] (outgoing) and by
/// [`StreamAcceptor::accept`] (incoming). Both sides passed the regular
/// network handshake, so the peer is authenticated and belongs to the same
/// network and schema. The connection behind the stream closes when both
/// halves are dropped.
pub struct ByteStream {
    /// The authenticated peer on the other side of the stream.
    pub peer_id: EndpointId,
    /// The sending half of the stream.
    pub send: SendStream,
    /// The receiving half of the stream.
    pub recv: RecvStream,
    /// Keeps the dedicated connection alive for the lifetime of the stream.
    _connection: Connection,
}

/// The currently selected transport path kind for a byte stream connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionPathKind {
    /// A direct UDP path to the remote peer.
    Direct,
    /// A path through the peer's home relay.
    Relay,
    /// A custom transport path.
    Custom,
    /// Iroh has not reported a selected path yet.
    Unknown,
}

impl ConnectionPathKind {
    /// Returns a compact stable label for logs and UI.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Relay => "relay",
            Self::Custom => "custom",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for ConnectionPathKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Snapshot of low-level transport statistics for a byte stream connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteStreamConnectionStats {
    /// The authenticated peer on the other side of the stream.
    pub peer_id: EndpointId,
    /// The transport path currently selected for application traffic.
    pub selected_path: ConnectionPathKind,
    /// Number of open paths on the underlying Iroh connection.
    pub open_paths: usize,
    /// Number of open direct paths.
    pub direct_paths: usize,
    /// Number of open relay paths.
    pub relay_paths: usize,
    /// Number of open custom transport paths.
    pub custom_paths: usize,
    /// RTT of the selected path, when available.
    pub selected_rtt: Option<Duration>,
    /// UDP bytes transmitted on the selected path.
    pub selected_tx_bytes: u64,
    /// UDP bytes received on the selected path.
    pub selected_rx_bytes: u64,
    /// UDP bytes transmitted on the whole connection.
    pub total_tx_bytes: u64,
    /// UDP bytes received on the whole connection.
    pub total_rx_bytes: u64,
    /// Packets lost on the whole connection.
    pub lost_packets: u64,
    /// Bytes lost on the whole connection.
    pub lost_bytes: u64,
}

impl ByteStreamConnectionStats {
    fn from_connection(peer_id: EndpointId, connection: &Connection) -> Self {
        let totals = connection.stats();
        let paths = connection.paths();
        let mut stats = Self {
            peer_id,
            selected_path: ConnectionPathKind::Unknown,
            open_paths: paths.len(),
            direct_paths: 0,
            relay_paths: 0,
            custom_paths: 0,
            selected_rtt: None,
            selected_tx_bytes: 0,
            selected_rx_bytes: 0,
            total_tx_bytes: totals.udp_tx.bytes,
            total_rx_bytes: totals.udp_rx.bytes,
            lost_packets: totals.lost_packets,
            lost_bytes: totals.lost_bytes,
        };

        for path in paths.iter() {
            let path_kind = if path.is_ip() {
                stats.direct_paths += 1;
                ConnectionPathKind::Direct
            } else if path.is_relay() {
                stats.relay_paths += 1;
                ConnectionPathKind::Relay
            } else {
                stats.custom_paths += 1;
                ConnectionPathKind::Custom
            };
            if path.is_selected() {
                let path_stats = path.stats();
                stats.selected_path = path_kind;
                stats.selected_rtt = Some(path_stats.rtt);
                stats.selected_tx_bytes = path_stats.udp_tx.bytes;
                stats.selected_rx_bytes = path_stats.udp_rx.bytes;
            }
        }

        stats
    }
}

impl ByteStream {
    /// Returns a point-in-time snapshot of the underlying Iroh connection.
    pub fn connection_stats(&self) -> ByteStreamConnectionStats {
        ByteStreamConnectionStats::from_connection(self.peer_id, &self._connection)
    }
}

impl fmt::Debug for ByteStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteStream")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

/// Receiver of incoming [`ByteStream`]s for one declared stream protocol.
///
/// Obtained once per protocol from [`NetworkEngine::stream_acceptor`].
/// Dropping the acceptor makes the engine refuse further incoming streams
/// on that protocol.
pub struct StreamAcceptor {
    alpn: Vec<u8>,
    rx: mpsc::Receiver<ByteStream>,
}

impl StreamAcceptor {
    /// Waits for the next incoming stream.
    ///
    /// Returns `None` after the engine shut down.
    pub async fn accept(&mut self) -> Option<ByteStream> {
        self.rx.recv().await
    }

    /// The ALPN of the stream protocol this acceptor serves.
    pub fn alpn(&self) -> &[u8] {
        &self.alpn
    }
}

impl fmt::Debug for StreamAcceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamAcceptor")
            .field("alpn", &String::from_utf8_lossy(&self.alpn))
            .finish_non_exhaustive()
    }
}

/// Producer/consumer pair behind one declared stream protocol.
struct StreamAcceptorSlot {
    sender: mpsc::Sender<ByteStream>,
    /// Taken (once) by [`NetworkEngine::stream_acceptor`].
    receiver: Option<mpsc::Receiver<ByteStream>>,
}

fn alpn_display(alpn: &[u8]) -> String {
    String::from_utf8_lossy(alpn).into_owned()
}

/// State shared between the engine handles, the protocol handler and all
/// background tasks.
struct Shared<M> {
    config: NetworkConfig,
    endpoint: Endpoint,
    router: Mutex<Option<Router>>,
    peers: Mutex<HashMap<EndpointId, PeerState>>,
    stream_acceptors: Mutex<HashMap<Vec<u8>, StreamAcceptorSlot>>,
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

    fn validate_stream_handshake(
        &self,
        alpn: &[u8],
        handshake: &Handshake,
    ) -> Option<HandshakeErrorCode> {
        if handshake.protocol_version != PROTOCOL_VERSION {
            Some(HandshakeErrorCode::UnsupportedProtocolVersion)
        } else if handshake.network_id != self.config.network_id {
            Some(HandshakeErrorCode::NetworkMismatch)
        } else if !self
            .config
            .schema_independent_stream_protocols
            .iter()
            .any(|protocol| protocol.as_slice() == alpn)
            && handshake.schema_id != self.config.schema_id
        {
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

    /// Handles a freshly accepted connection on an auxiliary stream ALPN:
    /// validates the stream handshake and hands the byte stream over to the
    /// application through the protocol's acceptor.
    ///
    /// Unlike message connections, stream connections are not registered in
    /// the peer registry: their lifecycle belongs entirely to the returned
    /// [`ByteStream`].
    async fn handle_incoming_stream(&self, connection: &Connection, alpn: &[u8]) -> Result<()> {
        let peer_id = connection.remote_id();
        if self.is_shutting_down() {
            connection.close(CLOSE_CODE_SHUTDOWN, b"engine is shutting down");
            return Err(NetworkError::ShuttingDown);
        }
        debug!(peer = %peer_id, alpn = %alpn_display(alpn), "incoming byte stream");
        let request_timeout = self.config.request_timeout;

        let (mut send, mut recv) = timeout(request_timeout, connection.accept_bi())
            .await
            .map_err(|_| NetworkError::Timeout)
            .and_then(|res| {
                res.map_err(|err| {
                    NetworkError::Transport(format!("failed to accept stream handshake: {err}"))
                })
            })
            .inspect_err(|_| {
                connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"stream handshake failed");
            })?;

        let handshake: Result<Handshake> = timeout(
            request_timeout,
            wire::read_frame(&mut recv, MAX_HANDSHAKE_FRAME_SIZE),
        )
        .await
        .map_err(|_| NetworkError::Timeout)
        .and_then(|res| res);

        let verdict = match &handshake {
            Ok(handshake) => self.validate_stream_handshake(alpn, handshake),
            Err(_) => Some(HandshakeErrorCode::InvalidHandshake),
        };
        if let Some(code) = verdict {
            warn!(peer = %peer_id, code = ?code, "stream handshake rejected");
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
                let _ = timeout(REJECT_LINGER, connection.closed()).await;
            }
            connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"stream handshake rejected");
            return Err(handshake_code_to_error(code));
        }

        // Reserve a queue slot before acking so a stream the application
        // will never consume is refused instead of silently dying after an
        // accepted handshake. A full queue delays the ack (backpressure).
        let sender = lock(&self.stream_acceptors)
            .get(alpn)
            .map(|slot| slot.sender.clone())
            .ok_or_else(|| NetworkError::UnknownStreamProtocol(alpn_display(alpn)))?;
        let Ok(permit) = sender.reserve().await else {
            connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"stream acceptor closed");
            return Err(NetworkError::Transport(
                "the stream acceptor was dropped by the application".to_string(),
            ));
        };

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
            connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"stream handshake failed");
        })?;
        // The stream is deliberately not finished: the application payload
        // follows on the same stream pair.

        info!(peer = %peer_id, alpn = %alpn_display(alpn), "byte stream accepted");
        permit.send(ByteStream {
            peer_id,
            send,
            recv,
            _connection: connection.clone(),
        });
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

/// Protocol handler registered with the Iroh [`Router`] for one auxiliary
/// stream ALPN declared in the configuration.
struct StreamProtocol<M> {
    shared: Weak<Shared<M>>,
    alpn: Vec<u8>,
}

impl<M> fmt::Debug for StreamProtocol<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamProtocol")
            .field("alpn", &alpn_display(&self.alpn))
            .finish_non_exhaustive()
    }
}

impl<M: Message> ProtocolHandler for StreamProtocol<M> {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Some(shared) = self.shared.upgrade() else {
            connection.close(CLOSE_CODE_SHUTDOWN, b"engine is gone");
            return Ok(());
        };
        let peer_id = connection.remote_id();
        if let Err(err) = shared.handle_incoming_stream(&connection, &self.alpn).await {
            shared
                .report_protocol_error(
                    Some(peer_id),
                    format!(
                        "incoming byte stream on {} failed: {err}",
                        alpn_display(&self.alpn)
                    ),
                )
                .await;
        }
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
        Self::start_with_secret_key(config, secret_key).await
    }

    /// Starts the engine with an application-provided identity key.
    ///
    /// Use this when the caller already stores the peer identity in durable
    /// application storage. The regular [`NetworkEngine::start`] method
    /// remains the file-backed default.
    pub async fn start_with_secret_key(
        config: NetworkConfig,
        secret_key: SecretKey,
    ) -> Result<(Self, NetworkEventReceiver<M>)> {
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(|err| NetworkError::Transport(format!("failed to bind endpoint: {err}")))?;
        let endpoint_id = endpoint.id();
        let (sender, receiver) = mpsc::channel(config.event_channel_capacity);
        let stream_acceptors = config
            .stream_protocols
            .iter()
            .chain(config.schema_independent_stream_protocols.iter())
            .map(|alpn| {
                let (sender, receiver) = mpsc::channel(STREAM_ACCEPT_QUEUE);
                (
                    alpn.clone(),
                    StreamAcceptorSlot {
                        sender,
                        receiver: Some(receiver),
                    },
                )
            })
            .collect();
        let shared = Arc::new(Shared {
            config,
            endpoint: endpoint.clone(),
            router: Mutex::new(None),
            peers: Mutex::new(HashMap::new()),
            stream_acceptors: Mutex::new(stream_acceptors),
            events: Mutex::new(Some(sender)),
            tasks: Mutex::new(JoinSet::new()),
            next_generation: AtomicU64::new(0),
            shutting_down: AtomicBool::new(false),
            shutdown_signal: watch::Sender::new(false),
        });
        let handler = FederationProtocol {
            shared: Arc::downgrade(&shared),
        };
        let mut router_builder = Router::builder(endpoint).accept(ALPN, handler);
        for alpn in &shared.config.stream_protocols {
            let handler = StreamProtocol::<M> {
                shared: Arc::downgrade(&shared),
                alpn: alpn.clone(),
            };
            router_builder = router_builder.accept(alpn.clone(), handler);
        }
        for alpn in &shared.config.schema_independent_stream_protocols {
            let handler = StreamProtocol::<M> {
                shared: Arc::downgrade(&shared),
                alpn: alpn.clone(),
            };
            router_builder = router_builder.accept(alpn.clone(), handler);
        }
        let router = router_builder.spawn();
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

    /// Signs application data with this peer's persistent transport identity.
    ///
    /// The secret key never leaves the engine. Protocols can attach the
    /// returned signature to records that may be forwarded by other peers;
    /// recipients verify it against [`Self::endpoint_id`].
    pub fn sign_identity(&self, message: &[u8]) -> Signature {
        self.shared.endpoint.secret_key().sign(message)
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

    /// Opens a raw bidirectional byte stream to a peer over a dedicated
    /// connection on the auxiliary ALPN `alpn`.
    ///
    /// The remote peer must have declared `alpn` in its configuration
    /// ([`crate::NetworkConfigBuilder::stream_protocol`]) and hold on to the
    /// matching [`StreamAcceptor`]. The stream begins with the regular
    /// network handshake, so it can only be established between peers of the
    /// same network and schema; afterwards the stream carries opaque
    /// application bytes with no framing imposed by the engine.
    ///
    /// `target` can be a full [`EndpointAddr`] (e.g. from a ticket) or a
    /// bare [`EndpointId`] when the transport already knows how to reach the
    /// peer (an active or recent connection, or discovery).
    pub async fn open_stream(
        &self,
        target: impl Into<EndpointAddr>,
        alpn: &[u8],
    ) -> Result<ByteStream> {
        let shared = &self.shared;
        shared.ensure_running()?;
        let addr: EndpointAddr = target.into();
        let request_timeout = shared.config.request_timeout;
        let connection = timeout(request_timeout, shared.endpoint.connect(addr, alpn))
            .await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(|err| {
                NetworkError::Transport(format!("failed to connect for a byte stream: {err}"))
            })?;
        let peer_id = connection.remote_id();
        let handshake = Handshake {
            protocol_version: PROTOCOL_VERSION,
            network_id: shared.config.network_id,
            schema_id: shared.config.schema_id,
        };
        let result = timeout(request_timeout, async {
            let (mut send, mut recv) = connection.open_bi().await.map_err(|err| {
                NetworkError::Transport(format!("failed to open the byte stream: {err}"))
            })?;
            wire::write_frame(&mut send, &handshake, MAX_HANDSHAKE_FRAME_SIZE).await?;
            // The stream is deliberately not finished: application payload
            // follows the handshake on the same stream.
            let ack: HandshakeAck = wire::read_frame(&mut recv, MAX_HANDSHAKE_FRAME_SIZE).await?;
            if ack.accepted {
                Ok((send, recv))
            } else {
                let code = ack.error.unwrap_or(HandshakeErrorCode::InvalidHandshake);
                Err(handshake_code_to_error(code))
            }
        })
        .await
        .map_err(|_| NetworkError::Timeout)
        .and_then(|res| res);
        match result {
            Ok((send, recv)) => {
                debug!(peer = %peer_id, alpn = %alpn_display(alpn), "byte stream opened");
                Ok(ByteStream {
                    peer_id,
                    send,
                    recv,
                    _connection: connection,
                })
            }
            Err(err) => {
                connection.close(CLOSE_CODE_HANDSHAKE_REJECTED, b"stream handshake failed");
                Err(err)
            }
        }
    }

    /// Takes the acceptor of incoming byte streams for the declared stream
    /// protocol `alpn`.
    ///
    /// The acceptor for a protocol can be taken exactly once. Streams whose
    /// handshake succeeded are delivered in accept order; the queue is small
    /// and bounded, so an unconsumed acceptor applies backpressure to
    /// remote peers.
    pub fn stream_acceptor(&self, alpn: &[u8]) -> Result<StreamAcceptor> {
        let mut acceptors = lock(&self.shared.stream_acceptors);
        let slot = acceptors
            .get_mut(alpn)
            .ok_or_else(|| NetworkError::UnknownStreamProtocol(alpn_display(alpn)))?;
        let rx = slot
            .receiver
            .take()
            .ok_or_else(|| NetworkError::StreamAcceptorTaken(alpn_display(alpn)))?;
        Ok(StreamAcceptor {
            alpn: alpn.to_vec(),
            rx,
        })
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
