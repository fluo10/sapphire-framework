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

/// Whether `hash` is a well-formed blob address: exactly 64 lowercase hex
/// characters, the output shape of [`hash_bytes`].
///
/// A [`BlobStore`] address is derived from content, never chosen by a caller,
/// so anything outside this shape is not a blob address at all. Filesystem
/// backends in particular must check this before touching a path: `blob.get`
/// is reachable from the wire, and an unchecked address turns a blob fetch
/// into an arbitrary file read (`"../../keys.toml"`, `"/etc/passwd"`).
pub fn is_valid_hash(hash: &str) -> bool {
    hash.len() == 64 && hash.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Storage for content-addressed binary blobs.
pub trait BlobStore: Send + Sync {
    /// Store `bytes`, returning the content-addressed reference. Storing the
    /// same bytes again is a no-op that returns the same [`BlobRef`].
    fn put(&self, bytes: &[u8]) -> Result<BlobRef>;

    /// Fetch the blob with the given hex hash, or `None` if it is not stored.
    ///
    /// A `hash` that is not a blob address (see [`is_valid_hash`]) is rejected
    /// with [`Error::InvalidHash`] rather than looked up.
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>>;

    /// Whether a blob with the given hex hash is stored. Rejects a malformed
    /// address exactly as [`get`](BlobStore::get) does.
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
    ///
    /// The validity check is the security boundary for this store, and it lives
    /// here rather than at the callers so that no future path-building route
    /// can skip it. `hash` arrives from the wire (`blob.get`); without the
    /// check, `join` happily accepts `"../.."` or an absolute path, and the
    /// `hash[0..2]` shard slice panics on a multi-byte character.
    fn blob_path(&self, hash: &str) -> Result<PathBuf> {
        if !crate::is_valid_hash(hash) {
            return Err(Error::InvalidHash {
                hash: hash.to_owned(),
            });
        }
        let shard = &hash[0..2];
        Ok(self.root.join(shard).join(hash))
    }
}

impl BlobStore for FsBlobStore {
    fn put(&self, bytes: &[u8]) -> Result<BlobRef> {
        let hash = hash_bytes(bytes);
        // Always valid: we just computed it. `?` only for the shared signature.
        let path = self.blob_path(&hash)?;
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
        let path = self.blob_path(hash)?;
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    fn has(&self, hash: &str) -> Result<bool> {
        Ok(self.blob_path(hash)?.exists())
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
        // Well-formed address, nothing stored under it.
        let absent = hash_bytes(b"never stored");
        assert_eq!(store.get(&absent).unwrap(), None);
        assert!(!store.has(&absent).unwrap());
    }

    #[test]
    fn has_reflects_presence() {
        let (_tmp, store) = store();
        let r = store.put(b"present").unwrap();
        assert!(store.has(&r.hash).unwrap());
    }

    #[test]
    fn a_hash_that_is_not_an_address_never_reaches_the_filesystem() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(tmp.path().join("blobs")).unwrap();

        // Somewhere the store must never reach: a sibling of its own root,
        // standing in for the server's plaintext key file.
        let secret = tmp.path().join("secret.toml");
        std::fs::write(&secret, "token = \"sjt_topsecret\"").unwrap();

        let hostile = [
            // Traversal, both bare and dressed up as a shard.
            "../secret.toml",
            "./../secret.toml",
            "..%2fsecret.toml",
            // An absolute path wins `Path::join` outright.
            secret.to_str().unwrap(),
            "/etc/passwd",
            // Multi-byte first characters used to panic slicing `hash[0..2]`.
            "€uro",
            "\u{3042}\u{3044}",
            // Right shape, wrong alphabet / length.
            "deadbeef",
            &"A".repeat(64),
            &"g".repeat(64),
            &"a".repeat(63),
            &"a".repeat(65),
            "",
        ];

        for hash in hostile {
            assert!(
                matches!(store.get(hash), Err(Error::InvalidHash { .. })),
                "get({hash:?}) must be rejected as a malformed address"
            );
            assert!(
                matches!(store.has(hash), Err(Error::InvalidHash { .. })),
                "has({hash:?}) must be rejected as a malformed address"
            );
        }

        assert_eq!(
            std::fs::read_to_string(&secret).unwrap(),
            "token = \"sjt_topsecret\"",
            "the file outside the store must be untouched"
        );
    }

    #[test]
    fn is_valid_hash_accepts_exactly_what_hash_bytes_produces() {
        assert!(is_valid_hash(&hash_bytes(b"anything")));
        assert!(is_valid_hash(&"0123456789abcdef".repeat(4)));
        assert!(!is_valid_hash(&"0123456789ABCDEF".repeat(4)));
    }
}
