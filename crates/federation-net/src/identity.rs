//! Persistent peer identity.
//!
//! Each peer owns a long-lived Iroh secret key stored at
//! `<data_dir>/identity.key`. The key is created on first start and loaded on
//! every subsequent start, so the peer's [`iroh::EndpointId`] stays stable
//! across restarts.

use std::path::Path;

use iroh::SecretKey;

use crate::error::{NetworkError, Result};

/// File name of the persisted secret key inside the data directory.
pub(crate) const IDENTITY_FILE_NAME: &str = "identity.key";

/// Loads the peer identity from `data_dir`, creating a new one if none exists.
///
/// File I/O runs on a blocking thread so the async runtime is never blocked.
pub(crate) async fn load_or_create(data_dir: &Path) -> Result<SecretKey> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || load_or_create_blocking(&dir))
        .await
        .map_err(|err| NetworkError::Identity(format!("identity task panicked: {err}")))?
}

fn load_or_create_blocking(dir: &Path) -> Result<SecretKey> {
    std::fs::create_dir_all(dir).map_err(|err| {
        NetworkError::Identity(format!(
            "failed to create data directory {}: {err}",
            dir.display()
        ))
    })?;
    let path = dir.join(IDENTITY_FILE_NAME);
    if path.exists() {
        load_key(&path)
    } else {
        let key = SecretKey::generate();
        store_key(dir, &path, &key)?;
        Ok(key)
    }
}

fn load_key(path: &Path) -> Result<SecretKey> {
    let content = std::fs::read_to_string(path).map_err(|err| {
        NetworkError::Identity(format!(
            "failed to read identity file {}: {err}",
            path.display()
        ))
    })?;
    let trimmed = content.trim();
    let bytes = data_encoding::HEXLOWER_PERMISSIVE
        .decode(trimmed.as_bytes())
        .map_err(|_| corrupted(path))?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| corrupted(path))?;
    Ok(SecretKey::from_bytes(&bytes))
}

fn corrupted(path: &Path) -> NetworkError {
    NetworkError::Identity(format!(
        "identity file {} is corrupted: expected 64 hex characters; \
         remove the file to generate a new identity",
        path.display()
    ))
}

/// Persists the key atomically: write to a temporary file in the same
/// directory, then rename it over the final path.
fn store_key(dir: &Path, path: &Path, key: &SecretKey) -> Result<()> {
    use std::io::Write;

    let tmp_path = dir.join(format!("{IDENTITY_FILE_NAME}.tmp"));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let write_result = (|| {
        let mut file = options.open(&tmp_path)?;
        let mut encoded = data_encoding::HEXLOWER.encode(&key.to_bytes());
        encoded.push('\n');
        file.write_all(encoded.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&tmp_path, path)
    })();
    if let Err(err) = write_result {
        // Best effort removal of the temporary file; the original error wins.
        let _ = std::fs::remove_file(&tmp_path);
        return Err(NetworkError::Identity(format!(
            "failed to persist identity file {}: {err}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn key_is_created_and_reloaded() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = load_or_create(dir.path()).await.expect("create key");
        let second = load_or_create(dir.path()).await.expect("load key");
        assert_eq!(first.public(), second.public());
        assert_eq!(first.to_bytes(), second.to_bytes());
    }

    #[tokio::test]
    async fn existing_key_is_not_overwritten() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = load_or_create(dir.path()).await.expect("create key");
        let path = dir.path().join(IDENTITY_FILE_NAME);
        let before = std::fs::read(&path).expect("read identity file");
        let second = load_or_create(dir.path()).await.expect("load key");
        let after = std::fs::read(&path).expect("read identity file");
        assert_eq!(before, after);
        assert_eq!(first.public(), second.public());
    }

    #[tokio::test]
    async fn corrupted_key_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(IDENTITY_FILE_NAME);
        std::fs::write(&path, "definitely not a hex key").expect("write garbage");
        let result = load_or_create(dir.path()).await;
        assert!(matches!(result, Err(NetworkError::Identity(_))));
    }

    #[tokio::test]
    async fn truncated_key_file_is_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(IDENTITY_FILE_NAME);
        // Valid hex, but not 32 bytes.
        std::fs::write(&path, "deadbeef").expect("write short key");
        let result = load_or_create(dir.path()).await;
        assert!(matches!(result, Err(NetworkError::Identity(_))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn key_file_has_restrictive_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        load_or_create(dir.path()).await.expect("create key");
        let metadata = std::fs::metadata(dir.path().join(IDENTITY_FILE_NAME)).expect("metadata");
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }
}
