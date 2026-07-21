//! Content-addressed binary blob storage.
//!
//! Documents in a workspace are text; their binary attachments (images,
//! audio, …) live out-of-band as **blobs** addressed by the SHA-256 of their
//! contents. Because the address is the hash, storing the same bytes twice is
//! idempotent and de-duplicates automatically — the property the remote sync
//! protocol relies on (`blob.put` / `blob.get` in `sapphire-framework-rpc`).
//!
//! The trait is intentionally **synchronous**, matching the rest of the
//! framework's stores (`sapphire_track::TrackStore`,
//! `sapphire_retrieve::RetrieveStore`). Async contexts wrap calls in
//! `spawn_blocking`. A future OPFS/S3 backend can add an async trait if needed.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

mod error;
pub use error::{Error, Result};

/// A content-addressed reference to a stored blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRef {
    /// Lower-case hex SHA-256 of the blob contents.
    pub hash: String,
    /// Length of the blob in bytes.
    pub len: u64,
}

/// Compute the lower-case hex SHA-256 of `bytes` — the content address used by
/// every [`BlobStore`].
pub fn hash_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Storage for content-addressed binary blobs.
pub trait BlobStore: Send + Sync {
    /// Store `bytes`, returning the content-addressed reference. Storing the
    /// same bytes again is a no-op that returns the same [`BlobRef`].
    fn put(&self, bytes: &[u8]) -> Result<BlobRef>;

    /// Fetch the blob with the given hex hash, or `None` if it is not stored.
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>>;

    /// Whether a blob with the given hex hash is stored.
    fn has(&self, hash: &str) -> Result<bool> {
        Ok(self.get(hash)?.is_some())
    }
}

/// Filesystem-backed [`BlobStore`].
///
/// Blobs live at `root/<hash[0..2]>/<hash>`. The two-character shard keeps any
/// single directory from accumulating an unbounded number of entries. Writes
/// go to a temporary file first and are then atomically renamed into place, so
/// a concurrent reader never observes a partial blob.
pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    /// Open (creating if necessary) a blob store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|source| Error::Io {
            path: root.clone(),
            source,
        })?;
        Ok(Self { root })
    }

    /// The directory this store writes to.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Absolute path of the blob with `hash` (whether or not it exists).
    fn blob_path(&self, hash: &str) -> PathBuf {
        let shard = if hash.len() >= 2 { &hash[0..2] } else { "__" };
        self.root.join(shard).join(hash)
    }
}

impl BlobStore for FsBlobStore {
    fn put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let hash = hash_bytes(bytes);
        let path = self.blob_path(&hash);
        let blob_ref = BlobRef {
            hash: hash.clone(),
            len: bytes.len() as u64,
        };

        // Already stored — content-addressing makes this a safe no-op.
        if path.exists() {
            return Ok(blob_ref);
        }

        let dir = path.parent().expect("blob_path always has a parent");
        std::fs::create_dir_all(dir).map_err(|source| Error::Io {
            path: dir.to_owned(),
            source,
        })?;

        // Write to a unique temp file then rename for atomicity. The temp name
        // includes the hash so parallel puts of *different* blobs never collide,
        // and the eventual rename is a no-op-safe overwrite of identical bytes.
        let tmp = dir.join(format!("{hash}.tmp"));
        std::fs::write(&tmp, bytes).map_err(|source| Error::Io {
            path: tmp.clone(),
            source,
        })?;
        std::fs::rename(&tmp, &path).map_err(|source| {
            // Best-effort cleanup so an interrupted rename doesn't leak temps.
            let _ = std::fs::remove_file(&tmp);
            Error::Io {
                path: path.clone(),
                source,
            }
        })?;

        Ok(blob_ref)
    }

    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.blob_path(hash);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    fn has(&self, hash: &str) -> Result<bool> {
        Ok(self.blob_path(hash).exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, FsBlobStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(tmp.path().join("blobs")).unwrap();
        (tmp, store)
    }

    #[test]
    fn put_get_roundtrip() {
        let (_tmp, store) = store();
        let r = store.put(b"hello world").unwrap();
        assert_eq!(r.len, 11);
        assert_eq!(store.get(&r.hash).unwrap().as_deref(), Some(&b"hello world"[..]));
    }

    #[test]
    fn content_addressed_and_idempotent() {
        let (_tmp, store) = store();
        let a = store.put(b"same").unwrap();
        let b = store.put(b"same").unwrap();
        assert_eq!(a.hash, b.hash, "identical bytes must hash identically");
        assert_eq!(a.len, b.len);
        // The blob is still readable after the second (no-op) put.
        assert_eq!(store.get(&a.hash).unwrap().as_deref(), Some(&b"same"[..]));
    }

    #[test]
    fn hash_matches_known_sha256() {
        // "abc" -> well-known SHA-256 test vector.
        assert_eq!(
            hash_bytes(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn missing_blob_returns_none() {
        let (_tmp, store) = store();
        assert_eq!(store.get("deadbeef").unwrap(), None);
        assert!(!store.has("deadbeef").unwrap());
    }

    #[test]
    fn has_reflects_presence() {
        let (_tmp, store) = store();
        let r = store.put(b"present").unwrap();
        assert!(store.has(&r.hash).unwrap());
    }
}
