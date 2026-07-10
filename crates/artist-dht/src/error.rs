//! Error types for the artist-dht library.

/// Convenient result alias used across the library.
pub type Result<T, E = ArtistDhtError> = std::result::Result<T, E>;

/// All errors that can be returned by the public API of this library.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ArtistDhtError {
    /// A local database operation failed.
    #[error("database error: {0}")]
    Database(String),

    /// The underlying network layer reported an error.
    #[error("network error: {0}")]
    Network(String),

    /// The artist name is empty or normalizes to nothing.
    #[error("invalid artist name")]
    InvalidArtistName,

    /// The artist name exceeds the maximum allowed length.
    #[error("artist name is too long")]
    ArtistNameTooLong,

    /// No artist with the given id exists locally.
    #[error("artist not found")]
    ArtistNotFound,

    /// Only locally created artists can be deleted.
    #[error("cannot delete a remote artist")]
    CannotDeleteRemoteArtist,

    /// A DHT request did not receive a response in time.
    #[error("request timed out")]
    Timeout,

    /// The lookup exhausted its request budget without finishing.
    #[error("lookup budget exhausted")]
    LookupBudgetExhausted,

    /// A stored peer ticket could not be parsed.
    #[error("invalid peer ticket: {0}")]
    InvalidTicket(String),

    /// A peer violated the DHT protocol.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The service is shutting down and no longer accepts operations.
    #[error("service is shutting down")]
    ShuttingDown,
}

impl From<federation_net::NetworkError> for ArtistDhtError {
    fn from(err: federation_net::NetworkError) -> Self {
        use federation_net::NetworkError;
        match err {
            NetworkError::Timeout => Self::Timeout,
            NetworkError::ShuttingDown => Self::ShuttingDown,
            NetworkError::InvalidTicket(msg) => Self::InvalidTicket(msg),
            other => Self::Network(other.to_string()),
        }
    }
}
