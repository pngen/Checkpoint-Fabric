//! Integrity primitives.
//!
//! SHA-256 provides durable content identity; CRC-32C provides fast corruption
//! detection. A checkpoint-level integrity root is derived from component hashes
//! and canonical metadata.

use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use crc::{Crc, CRC_32_ISCSI};
use sha2::{Digest, Sha256};

use crate::errors::{FabricError, FabricResult};

fn crc_instance() -> &'static Crc<u32> {
    use std::sync::LazyLock;
    static CRC32C: LazyLock<Crc<u32>> = LazyLock::new(|| Crc::<u32>::new(&CRC_32_ISCSI));
    &CRC32C
}

/// Start a streaming CRC-32C digest.
pub fn crc_digest() -> crc::Digest<'static, u32> {
    crc_instance().digest()
}

/// CRC-32C (Castagnoli) checksum of bytes.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc_instance().checksum(bytes)
}

/// SHA-256 digest of bytes, hex-encoded.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Streamed SHA-256 of a file, hex-encoded.
pub fn sha256_file(path: &Path) -> FabricResult<String> {
    let f = std::fs::File::open(path)
        .map_err(|e| FabricError::Io(format!("open {}: {e}", path.display())))?;
    let mut reader = BufReader::with_capacity(1 << 20, f);
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| FabricError::Io(format!("read {}: {e}", path.display())))?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

/// Streamed CRC-32C of a file.
pub fn crc32c_file(path: &Path) -> FabricResult<u32> {
    let f = std::fs::File::open(path)
        .map_err(|e| FabricError::Io(format!("open {}: {e}", path.display())))?;
    let mut reader = BufReader::with_capacity(1 << 20, f);
    let mut digest = crc_digest();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| FabricError::Io(format!("read {}: {e}", path.display())))?;
        if n == 0 {
            break;
        }
        digest.update(&buf[..n]);
    }
    Ok(digest.finalize())
}

/// Compute the checkpoint-level integrity root.
///
/// `root = SHA-256(concat(component content hashes as raw hex, canonical manifest bytes))`.
/// The manifest bytes passed here must be the canonical serialization with the
/// integrity root field emptied, so the root is a function of content and metadata.
pub fn compute_integrity_root(component_hashes: &[String], manifest_bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    for c in component_hashes {
        h.update(c.as_bytes());
    }
    h.update(manifest_bytes);
    hex::encode(h.finalize())
}

/// Stream a payload to disk while hashing, and write `.sha256` and `.crc32c`
/// sidecar files next to it.
pub fn write_hashed_payload(path: &Path, payload: &[u8]) -> FabricResult<()> {
    let mut h = Sha256::new();
    let mut digest = crc_digest();
    h.update(payload);
    digest.update(payload);
    let content_hash = hex::encode(h.finalize());
    let crc = digest.finalize();

    let mut file = BufWriter::new(std::fs::File::create(path)?);
    file.write_all(payload)?;
    file.flush()?;
    file.get_ref().sync_all()?;
    drop(file);

    let sha_path = sidecar_sha_path(path);
    std::fs::write(&sha_path, format!("{content_hash}\n"))?;
    let crc_path = sidecar_crc_path(path);
    std::fs::write(&crc_path, format!("{crc:08x}\n"))?;
    Ok(())
}

fn sidecar_sha_path(path: &Path) -> std::path::PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".sha256");
    std::path::PathBuf::from(p)
}

fn sidecar_crc_path(path: &Path) -> std::path::PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(".crc32c");
    std::path::PathBuf::from(p)
}

/// Verify a stored payload file against expected content hash and (optionally)
/// a stored CRC-32C sidecar, to catch fast corruption before full hashing.
pub fn verify_stored_file(
    path: &Path,
    expected_content_hash: &str,
    expected_size: Option<u64>,
) -> FabricResult<()> {
    let meta = std::fs::metadata(path)
        .map_err(|e| FabricError::IntegrityFailure(format!("missing {}: {e}", path.display())))?;
    if let Some(size) = expected_size {
        if meta.len() != size {
            return Err(FabricError::IntegrityFailure(format!(
                "size mismatch for {}: expected {size}, got {}",
                path.display(),
                meta.len()
            )));
        }
    }
    let crc_path = sidecar_crc_path(path);
    if crc_path.exists() {
        let expected = std::fs::read_to_string(&crc_path)
            .map_err(|e| FabricError::IntegrityFailure(e.to_string()))?;
        let expected = expected.trim();
        let actual = crc32c_file(path)?;
        if format!("{actual:08x}") != expected {
            return Err(FabricError::IntegrityFailure(format!(
                "fast corruption detected for {}: crc mismatch",
                path.display()
            )));
        }
    }
    let actual = sha256_file(path)?;
    if actual != *expected_content_hash {
        return Err(FabricError::IntegrityFailure(format!(
            "content hash mismatch for {}: expected {expected_content_hash}, got {actual}",
            path.display()
        )));
    }
    Ok(())
}

/// Convenience: compute hash and CRC of a byte slice in one pass.
pub fn hash_and_crc(payload: &[u8]) -> (String, u32) {
    let mut h = Sha256::new();
    let mut digest = crc_digest();
    h.update(payload);
    digest.update(payload);
    (hex::encode(h.finalize()), digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn crc32c_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn integrity_root_changes_with_components() {
        let m = b"manifest-bytes";
        let r1 = compute_integrity_root(&["a".into(), "b".into()], m);
        let r2 = compute_integrity_root(&["a".into(), "c".into()], m);
        let r3 = compute_integrity_root(&["a".into(), "b".into()], b"other");
        assert_ne!(r1, r2);
        assert_ne!(r1, r3);
        assert_eq!(r1, compute_integrity_root(&["a".into(), "b".into()], m));
    }

    #[test]
    fn file_verification_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("payload.bin");
        std::fs::write(&p, b"hello world").unwrap();
        write_hashed_payload(&p, b"hello world").unwrap();
        let (h, _) = hash_and_crc(b"hello world");
        verify_stored_file(&p, &h, Some(11)).unwrap();

        std::fs::write(&p, b"hello worLd").unwrap();
        assert!(verify_stored_file(&p, &h, Some(11)).is_err());
    }
}
