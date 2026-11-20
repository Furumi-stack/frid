//! Error types for the music-dht library.

/// Convenient result alias used across the library.
pub type Result<T, E = MusicDhtError> = std::result::Result<T, E>;

/// All errors that can be returned by the public API of this library.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum MusicDhtError {
    /// A local database operation failed.
    #[error("database error: {0}")]
    Database(String),

    /// The underlying network layer reported an error.
    #[error("network error: {0}")]
    Network(String),

    /// The item name is empty or normalizes to nothing.
    #[error("invalid item name")]
    InvalidItemName,

    /// The item name exceeds the maximum allowed length.
    #[error("item name is too long")]
    ItemNameTooLong,

    /// No item with the given id exists locally.
    #[error("item not found")]
    ItemNotFound,

    /// Only locally created items can be deleted.
    #[error("cannot delete a remote item")]
    CannotDeleteRemoteItem,

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

    /// A podcast card cannot be published, for example because the user did
    /// not share the podcast (see `music_dht::podcast::effective_share`).
    #[error("podcast publication refused: {0}")]
    PodcastPublication(String),
}

impl From<federation_net::NetworkError> for MusicDhtError {
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
