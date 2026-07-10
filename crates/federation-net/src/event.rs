//! Network events delivered to the application.

use iroh::EndpointId;
use tokio::sync::mpsc;

/// Direction of an established peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionDirection {
    /// The remote peer connected to us.
    Incoming,
    /// We connected to the remote peer.
    Outgoing,
}

/// Events emitted by a [`crate::NetworkEngine`].
///
/// `M` is the application-defined domain message type.
#[derive(Debug)]
pub enum NetworkEvent<M> {
    /// A peer completed the handshake and is now connected.
    PeerConnected {
        /// Identifier of the connected peer.
        peer_id: EndpointId,
        /// Whether the peer connected to us or we connected to it.
        direction: ConnectionDirection,
    },

    /// A peer disconnected.
    PeerDisconnected {
        /// Identifier of the disconnected peer.
        peer_id: EndpointId,
        /// Human-readable reason, if one is known.
        reason: Option<String>,
    },

    /// A domain message was received from a peer.
    MessageReceived {
        /// Identifier of the sending peer.
        peer_id: EndpointId,
        /// The decoded domain message.
        message: M,
    },

    /// A protocol-level error occurred.
    ///
    /// Errors of a single connection never bring down the engine; they are
    /// reported here instead.
    ProtocolError {
        /// The peer involved, if known.
        peer_id: Option<EndpointId>,
        /// Human-readable error description.
        error: String,
    },
}

/// Receiving side of the engine's event channel.
///
/// There is exactly one receiver per engine; it is returned from
/// [`crate::NetworkEngine::start`]. The underlying channel is bounded, so the
/// application should consume events promptly to avoid back-pressuring the
/// engine.
#[derive(Debug)]
pub struct NetworkEventReceiver<M> {
    rx: mpsc::Receiver<NetworkEvent<M>>,
}

impl<M> NetworkEventReceiver<M> {
    pub(crate) fn new(rx: mpsc::Receiver<NetworkEvent<M>>) -> Self {
        Self { rx }
    }

    /// Receives the next event.
    ///
    /// Returns `None` after the engine has shut down and all pending events
    /// have been consumed.
    pub async fn recv(&mut self) -> Option<NetworkEvent<M>> {
        self.rx.recv().await
    }
}
