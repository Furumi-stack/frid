//! Length-prefixed framing of postcard-encoded values.
//!
//! Every frame on the wire is laid out as:
//!
//! ```text
//! 4 bytes: payload length, unsigned big-endian
//! N bytes: postcard payload
//! ```

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{NetworkError, Result};

/// Encodes a value with postcard, enforcing `max_size` on the encoded bytes.
pub(crate) fn encode<T: Serialize>(value: &T, max_size: usize) -> Result<Vec<u8>> {
    let bytes = postcard::to_stdvec(value)
        .map_err(|err| NetworkError::Serialization(format!("postcard encoding failed: {err}")))?;
    if bytes.len() > max_size {
        return Err(NetworkError::MessageTooLarge);
    }
    Ok(bytes)
}

/// Decodes a postcard-encoded value.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    postcard::from_bytes(bytes)
        .map_err(|err| NetworkError::Serialization(format!("postcard decoding failed: {err}")))
}

/// Writes one length-prefixed frame containing the postcard encoding of
/// `value`.
pub(crate) async fn write_frame<W, T>(writer: &mut W, value: &T, max_size: usize) -> Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let payload = encode(value, max_size)?;
    // `max_size` is validated to fit into u32 by the configuration.
    let len = payload.len() as u32;
    writer
        .write_all(&len.to_be_bytes())
        .await
        .map_err(|err| NetworkError::Transport(format!("failed to write frame header: {err}")))?;
    writer
        .write_all(&payload)
        .await
        .map_err(|err| NetworkError::Transport(format!("failed to write frame payload: {err}")))?;
    Ok(())
}

/// Reads one length-prefixed frame and decodes it with postcard.
///
/// The length prefix is validated against `max_size` before any payload
/// memory is allocated. A frame that exceeds the limit is rejected without
/// reading its payload.
pub(crate) async fn read_frame<R, T>(reader: &mut R, max_size: usize) -> Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut len_bytes = [0u8; 4];
    reader
        .read_exact(&mut len_bytes)
        .await
        .map_err(|err| NetworkError::Transport(format!("failed to read frame header: {err}")))?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len > max_size {
        return Err(NetworkError::Transport(format!(
            "incoming frame of {len} bytes exceeds limit of {max_size} bytes"
        )));
    }
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|err| NetworkError::Transport(format!("failed to read frame payload: {err}")))?;
    decode(&payload)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestValue {
        text: String,
        number: u64,
    }

    #[tokio::test]
    async fn frame_round_trip() {
        let value = TestValue {
            text: "hello".into(),
            number: 42,
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &value, 1024).await.expect("write");
        let decoded: TestValue = read_frame(&mut buf.as_slice(), 1024).await.expect("read");
        assert_eq!(decoded, value);
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_before_allocation() {
        // A header declaring a huge payload with no payload bytes present.
        // If the length check happened after allocation, this would try to
        // allocate 4 GiB; instead it must fail fast on the limit check.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_be_bytes());
        let result: Result<TestValue> = read_frame(&mut buf.as_slice(), 1024).await;
        assert!(matches!(result, Err(NetworkError::Transport(_))));
    }

    #[tokio::test]
    async fn oversized_value_is_rejected_on_write() {
        let value = TestValue {
            text: "x".repeat(2048),
            number: 1,
        };
        let mut buf = Vec::new();
        let result = write_frame(&mut buf, &value, 1024).await;
        assert!(matches!(result, Err(NetworkError::MessageTooLarge)));
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn malformed_payload_is_rejected() {
        // Valid header, but payload bytes that do not decode as TestValue.
        let payload = [0xffu8; 8];
        let mut buf = Vec::new();
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        let result: Result<TestValue> = read_frame(&mut buf.as_slice(), 1024).await;
        assert!(matches!(result, Err(NetworkError::Serialization(_))));
    }

    #[tokio::test]
    async fn truncated_stream_is_an_error() {
        let value = TestValue {
            text: "hello".into(),
            number: 42,
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &value, 1024).await.expect("write");
        buf.truncate(buf.len() - 1);
        let result: Result<TestValue> = read_frame(&mut buf.as_slice(), 1024).await;
        assert!(matches!(result, Err(NetworkError::Transport(_))));
    }
}
