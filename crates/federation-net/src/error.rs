//! Error types for the federation-net library.

use iroh::EndpointId;

/// Convenient result alias used across the library.
pub type Result<T, E = NetworkError> = std::result::Result<T, E>;

/// All errors that can be returned by the public API of this library.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NetworkError {
    /// The provided [`crate::NetworkConfig`] is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Loading or persisting the peer identity failed.
    #[error("identity error: {0}")]
    Identity(String),

    /// A peer ticket could not be parsed or validated.
    #[error("invalid ticket: {0}")]
    InvalidTicket(String),

    /// The remote peer belongs to a different network.
    #[error("network id mismatch")]
    NetworkMismatch,

    /// The remote peer uses an incompatible message schema.
    #[error("schema id mismatch")]
    SchemaMismatch,

    /// The remote peer speaks an unsupported protocol version.
    #[error("unsupported protocol version")]
    UnsupportedProtocolVersion,

    /// The remote peer rejected the handshake as malformed.
    #[error("handshake rejected as invalid by remote peer")]
    HandshakeRejected,

    /// No active connection to the given peer exists.
    #[error("peer is not connected: {0}")]
    PeerNotConnected(EndpointId),

    /// A message exceeds the configured maximum size.
    #[error("message exceeds maximum size")]
    MessageTooLarge,

    /// The operation did not complete within the configured timeout.
    #[error("request timed out")]
    Timeout,

    /// Encoding or decoding a wire payload failed.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The underlying Iroh transport reported an error.
    #[error("transport error: {0}")]
    Transport(String),

    /// The remote peer rejected a message.
    #[error("message rejected by peer: {0}")]
    MessageRejected(String),

    /// The given ALPN was not declared as a stream protocol in the
    /// configuration.
    #[error("unknown stream protocol: {0}")]
    UnknownStreamProtocol(String),

    /// The incoming-stream acceptor for this protocol was already taken.
    #[error("stream acceptor already taken: {0}")]
    StreamAcceptorTaken(String),

    /// The engine is shutting down and no longer accepts operations.
    #[error("engine is shutting down")]
    ShuttingDown,
}
