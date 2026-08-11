//! Storage backend abstraction.
//!
//! The core runtime coordinates checkpoints; storage is pluggable. 1.0.0 ships a
//! local filesystem backend with atomic promotion. Interfaces are designed so
//! NVMe pools, object storage, and distributed filesystems can be added later.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::errors::{FabricError, FabricResult};
use crate::integrity;

/// Storage backend trait. All methods are bounded in resource use.
pub trait StorageBackend: Send + Sync {
    fn name(&self) -> &str;

    /// Where staging for an attempt lives.
    fn staging_dir(&self, attempt_id: &str) -> PathBuf;

    /// Where a committed checkpoint lives.
    fn commit_dir(&self, checkpoint_id: &crate::id::Id) -> PathBuf;

    /// Whether a committed checkpoint directory exists.
    fn has_committed(&self, checkpoint_id: &crate::id::Id) -> bool;

    /// Atomically promote a staging directory to a committed checkpoint directory.
    /// A partially written checkpoint must never appear committed.
    fn promote(&self, staging: &Path, commit: &Path) -> FabricResult<()>;

    /// Read a file relative to a committed checkpoint directory.
    fn read_checkpoint_file(
        &self,
        checkpoint_id: &crate::id::Id,
        rel: &str,
    ) -> FabricResult<Vec<u8>>;

    /// Remove a committed checkpoint directory (retirement).
    fn delete_committed(&self, checkpoint_id: &crate::id::Id) -> FabricResult<()>;

    /// Enumerate committed checkpoint directories.
    fn enumerate_committed(&self) -> FabricResult<Vec<crate::id::Id>>;

    /// Remove staging directories that are not in `keep`, returning what was removed.
    fn recover_partial(&self, keep: &HashSet<PathBuf>) -> FabricResult<Vec<PathBuf>>;

    /// Free bytes on the backing filesystem, if determinable.
    fn free_bytes(&self) -> FabricResult<u64>;
}

/// Local filesystem storage with atomic promotion semantics.
///
/// Layout under the root:
///   staging/<attempt_id>/          (invisible to committed enumeration)
///   checkpoints/<checkpoint_id>/   (committed, atomic via directory rename)
///
/// Durability: files are fsynced before promotion; the parent directory is fsynced
/// after promotion on platforms that allow directory handles (no-op on Windows,
/// documented in ARCHITECTURE.md).
#[derive(Debug)]
pub struct LocalStorage {
    root: PathBuf,
}

impl LocalStorage {
    pub fn new(root: &Path) -> FabricResult<Self> {
        fs::create_dir_all(root.join("staging"))?;
        fs::create_dir_all(root.join("checkpoints"))?;
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    fn checkpoints_dir(&self) -> PathBuf {
        self.root.join("checkpoints")
    }

    fn staging_root(&self) -> PathBuf {
        self.root.join("staging")
    }

    /// The root of the staging area (used by cleanup validation).
    pub fn staging_root_pub(&self) -> PathBuf {
        self.staging_root()
    }

    /// Remove only abandoned staging directories older than `ttl_ms`. Active
    /// attempt paths in `keep` are never age-expired while an operation is live.
    pub fn recover_partial_older_than(
        &self,
        keep: &HashSet<PathBuf>,
        ttl_ms: u64,
    ) -> FabricResult<Vec<PathBuf>> {
        let mut removed = Vec::new();
        if let Ok(entries) = fs::read_dir(self.staging_root()) {
            for entry in entries {
                let entry = entry?;
                let p = entry.path();
                if !p.is_dir() || keep.contains(&p) {
                    continue;
                }
                let old_enough = ttl_ms == 0
                    || entry
                        .metadata()
                        .and_then(|m| m.modified())
                        .ok()
                        .and_then(|modified| modified.elapsed().ok())
                        .is_some_and(|age| age.as_millis() >= u128::from(ttl_ms));
                if old_enough {
                    fs::remove_dir_all(&p)?;
                    removed.push(p);
                }
            }
        }
        Ok(removed)
    }

    fn is_committed_dir(&self, p: &Path) -> bool {
        p.is_dir()
            && p.join("manifest").is_file()
            && p.join("manifest.digest").is_file()
            && p.join("integrity").is_dir()
    }
}

impl StorageBackend for LocalStorage {
    fn name(&self) -> &str {
        "local-fs"
    }

    fn staging_dir(&self, attempt_id: &str) -> PathBuf {
        self.staging_root().join(sanitize_segment(attempt_id))
    }

    fn commit_dir(&self, checkpoint_id: &crate::id::Id) -> PathBuf {
        self.checkpoints_dir().join(checkpoint_id.to_hex())
    }

    fn has_committed(&self, checkpoint_id: &crate::id::Id) -> bool {
        self.is_committed_dir(&self.commit_dir(checkpoint_id))
    }

    fn promote(&self, staging: &Path, commit: &Path) -> FabricResult<()> {
        if commit.exists() {
            // Never silently overwrite existing committed checkpoints.
            return Err(FabricError::StorageError(format!(
                "commit path already exists: {}",
                commit.display()
            )));
        }
        let parent = commit
            .parent()
            .ok_or_else(|| FabricError::StorageError("commit path has no parent".into()))?;
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
        // fsync staging files before promotion for durability of content.
        sync_dir_children(staging)?;
        fs::rename(staging, commit)?;
        sync_dir(parent);
        Ok(())
    }

    fn read_checkpoint_file(
        &self,
        checkpoint_id: &crate::id::Id,
        rel: &str,
    ) -> FabricResult<Vec<u8>> {
        let base = self.commit_dir(checkpoint_id);
        let path = safe_join(&base, rel)?;
        fs::read(&path)
            .map_err(|e| FabricError::StorageError(format!("read {}: {e}", path.display())))
    }

    fn delete_committed(&self, checkpoint_id: &crate::id::Id) -> FabricResult<()> {
        let dir = self.commit_dir(checkpoint_id);
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        Ok(())
    }

    fn enumerate_committed(&self) -> FabricResult<Vec<crate::id::Id>> {
        let mut out = Vec::new();
        for entry in fs::read_dir(self.checkpoints_dir())? {
            let entry = entry?;
            let p = entry.path();
            if p.is_dir() {
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    if let Ok(id) = crate::id::Id::from_hex(name) {
                        if self.is_committed_dir(&p) {
                            out.push(id);
                        }
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn recover_partial(&self, keep: &HashSet<PathBuf>) -> FabricResult<Vec<PathBuf>> {
        self.recover_partial_older_than(keep, 0)
    }

    fn free_bytes(&self) -> FabricResult<u64> {
        // Deterministic capacity signal: the size of the root tree is reported
        // rather than pretending to know platform volume stats portably.
        let mut total = 0u64;
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            if let Ok(entries) = fs::read_dir(&dir) {
                for entry in entries.flatten() {
                    let p = entry.path();
                    if let Ok(ft) = entry.file_type() {
                        if ft.is_dir() {
                            stack.push(p);
                        } else if let Ok(m) = fs::metadata(p) {
                            total = total.saturating_add(m.len());
                        }
                    }
                }
            }
        }
        Ok(total)
    }
}

/// Reject path traversal and suspicious segments in user-supplied names.
pub fn sanitize_segment(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '@') {
            out.push(c);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() || out == "." || out == ".." {
        out.clear();
        out.push('_');
        for _ in 1..name.len().max(1) {
            out.push('_');
        }
    }
    out
}

/// Join a relative path to a base, rejecting traversal.
pub fn safe_join(base: &Path, rel: &str) -> FabricResult<PathBuf> {
    let rel_path = Path::new(rel);
    if rel_path.is_absolute()
        || rel_path.has_root()
        || rel_path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(FabricError::InvalidArgument(format!(
            "path traversal attempt rejected: {rel}"
        )));
    }
    Ok(base.join(rel_path))
}

/// Fsync all regular files in a directory (before promotion).
///
/// On Windows, `sync_all` on read-only handles fails with access-denied, so
/// per-file fsync before rename is skipped there (the write path already
/// fsyncs through write handles); this is documented in ARCHITECTURE.md.
pub fn sync_dir_children(dir: &Path) -> FabricResult<()> {
    #[cfg(windows)]
    {
        let _ = dir;
        Ok(())
    }
    #[cfg(unix)]
    {
        let entries = fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let f = fs::File::open(entry.path())?;
                f.sync_all()?;
            } else if entry.file_type()?.is_dir() {
                sync_dir_children(&entry.path())?;
            }
        }
        Ok(())
    }
}

/// Fsync a directory handle where the platform supports it (no-op on Windows).
pub fn sync_dir(_dir: &Path) {
    #[cfg(unix)]
    {
        if let Ok(f) = fs::File::open(_dir) {
            let _ = f.sync_all();
        }
    }
}

/// Verify the on-disk layout of a committed checkpoint: manifest, digest, root,
/// and per-component sidecars. Returns the parsed manifest digest.
pub fn verify_committed_layout(
    storage: &Arc<dyn StorageBackend>,
    checkpoint_id: &crate::id::Id,
) -> FabricResult<String> {
    let commit = storage.commit_dir(checkpoint_id);
    if !commit.join("manifest").is_file() {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "missing manifest for {checkpoint_id}"
        )));
    }
    if !commit.join("manifest.digest").is_file() {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "missing manifest.digest for {checkpoint_id}"
        )));
    }
    if !commit.join("integrity").is_dir() {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "missing integrity dir for {checkpoint_id}"
        )));
    }
    let manifest_bytes = fs::read(commit.join("manifest"))?;
    let manifest = crate::manifest::parse(&manifest_bytes)?;
    let digest = crate::integrity::sha256_hex(&manifest_bytes);
    let recorded_digest = fs::read_to_string(commit.join("manifest.digest"))?
        .trim()
        .to_string();
    if recorded_digest != digest {
        return Err(FabricError::IntegrityFailure(
            "manifest digest sidecar mismatch".into(),
        ));
    }
    let recorded_root = fs::read_to_string(commit.join("integrity/root"))?
        .trim()
        .to_string();
    if recorded_root != manifest.integrity.root {
        return Err(FabricError::IntegrityFailure(
            "manifest integrity-root sidecar mismatch".into(),
        ));
    }
    Ok(digest)
}

/// Verify one component payload file against its recorded stored hash and size.
pub fn verify_component_payload(
    commit: &Path,
    rel_path: &str,
    stored_hash: &str,
    stored_size: u64,
) -> FabricResult<()> {
    let path = safe_join(commit, rel_path)?;
    if !path.is_file() {
        return Err(FabricError::CorruptedCheckpoint(format!(
            "missing component payload {}",
            path.display()
        )));
    }
    integrity::verify_stored_file(&path, stored_hash, Some(stored_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_invisible_to_enumeration() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(dir.path()).unwrap();
        let id = crate::id::Id::random();
        let staging = storage.staging_dir("attempt-1");
        fs::create_dir_all(&staging).unwrap();
        assert!(storage.enumerate_committed().unwrap().is_empty());
        assert!(!storage.has_committed(&id));
    }

    #[test]
    fn promote_then_commit_visible() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(dir.path()).unwrap();
        let id = crate::id::Id::random();
        let staging = storage.staging_dir("attempt-1");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("manifest"), b"m").unwrap();
        fs::write(staging.join("manifest.digest"), b"d").unwrap();
        fs::create_dir_all(staging.join("integrity")).unwrap();
        let commit = storage.commit_dir(&id);
        storage.promote(&staging, &commit).unwrap();
        assert!(storage.has_committed(&id));
        assert_eq!(storage.enumerate_committed().unwrap(), vec![id]);
    }

    #[test]
    fn no_overwrite_of_committed() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(dir.path()).unwrap();
        let id = crate::id::Id::random();
        let staging = storage.staging_dir("attempt-1");
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("manifest"), b"m").unwrap();
        fs::write(staging.join("manifest.digest"), b"d").unwrap();
        fs::create_dir_all(staging.join("integrity")).unwrap();
        let commit = storage.commit_dir(&id);
        storage.promote(&staging, &commit).unwrap();
        let staging2 = storage.staging_dir("attempt-2");
        fs::create_dir_all(&staging2).unwrap();
        assert!(storage.promote(&staging2, &commit).is_err());
    }

    #[test]
    fn recover_partial_cleans_staging() {
        let dir = tempfile::tempdir().unwrap();
        let storage = LocalStorage::new(dir.path()).unwrap();
        let keep_path = storage.staging_dir("keep");
        let dead_path = storage.staging_dir("dead");
        fs::create_dir_all(&keep_path).unwrap();
        fs::create_dir_all(&dead_path).unwrap();
        let mut keep = HashSet::new();
        keep.insert(keep_path.clone());
        let removed = storage.recover_partial(&keep).unwrap();
        assert_eq!(removed, vec![dead_path]);
        assert!(keep_path.exists());
    }

    #[test]
    fn traversal_rejected() {
        assert!(safe_join(Path::new("base"), "../etc/passwd").is_err());
        assert!(safe_join(Path::new("base"), "/abs/path").is_err());
        assert!(safe_join(Path::new("base"), "components/a").is_ok());
        assert_eq!(sanitize_segment("a b/c"), "a_b_c");
    }
}
