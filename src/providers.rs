//! Capture providers: pluggable capture of typed execution-state components.
//!
//! The core runtime coordinates capture; providers own the actual state.
//! 1.0.0 ships: generic application-state provider, filesystem/blob provider,
//! process-metadata provider, host-memory-region provider, and an opaque custom
//! provider adapter. Unsupported process/accelerator state capture is never faked.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::capture::QuiescenceMode;
use crate::checkpoint::{ComponentEntry, ComponentType, StorageRepresentation};
use crate::compression::{self, CompressionSpec};
use crate::errors::{FabricError, FabricResult};
use crate::integrity;

/// Descriptor of a component a provider can capture/restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSpec {
    pub component_id: String,
    pub component_type: ComponentType,
    pub required: bool,
    pub schema_version: u32,
    pub restore_handler: String,
    pub compatibility: serde_json::Value,
    pub dependencies: Vec<String>,
}

/// Outcome of quiescence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuiesceOutcome {
    /// Cooperative ack from the application: a safe capture point.
    Acked,
    /// Forced quiescence applied at the platform level.
    Forced,
}

/// Context handed to providers during capture.
pub struct CaptureContext<'a> {
    pub workload_id: crate::id::Id,
    pub attempt_id: &'a str,
    pub staging_dir: &'a Path,
    pub compression: &'a CompressionSpec,
    pub quiescence: QuiescenceMode,
}

/// Context handed to providers during restore.
pub struct RestoreContext<'a> {
    pub checkpoint_id: crate::id::Id,
    pub workload_id: crate::id::Id,
    pub attempt_id: &'a str,
    pub staging_dir: &'a Path,
    pub commit_dir: &'a Path,
}

/// A captured component payload.
#[derive(Debug, Clone)]
pub struct CapturedComponent {
    pub spec: ProviderSpec,
    pub payload: Vec<u8>,
    pub content_hash: String,
}

/// The provider trait. Implementations must be `Send + Sync`.
pub trait CaptureProvider: Send + Sync {
    fn spec(&self) -> &ProviderSpec;

    /// Called before quiescence; providers stage resources.
    fn prepare_capture(&self, ctx: &CaptureContext<'_>) -> FabricResult<()>;

    /// Quiesce the source. `Cooperative` requires an application ack; `Forced`
    /// freezes supported state where platform semantics allow; `None` skips.
    fn quiesce(&self, ctx: &CaptureContext<'_>) -> FabricResult<QuiesceOutcome>;

    /// Capture the component payload.
    fn capture(&self, ctx: &CaptureContext<'_>) -> FabricResult<Vec<u8>>;

    /// Verify the captured payload.
    fn verify(&self, ctx: &CaptureContext<'_>) -> FabricResult<()>;

    /// Resume the source after a successful capture.
    fn resume_source(&self, ctx: &CaptureContext<'_>) -> FabricResult<()>;

    /// Abort a capture; restore any quiesced state.
    fn abort_capture(&self, ctx: &CaptureContext<'_>) -> FabricResult<()>;

    /// Restore the payload into the target.
    fn restore(&self, ctx: &RestoreContext<'_>, payload: &[u8]) -> FabricResult<()>;

    /// Verify the restored state.
    fn verify_restore(&self, ctx: &RestoreContext<'_>) -> FabricResult<()>;

    /// Deterministically clean up any partially restored state.
    fn cleanup_restore(&self, ctx: &RestoreContext<'_>) -> FabricResult<()>;

    /// Finalize a successful restore and discard any rollback journal retained
    /// until the coordinator durably commits generation and authority.
    fn commit_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }
}

/// Helper: write a component payload into the staging directory, applying the
/// checkpoint compression spec, and return the storage representation.
///
/// Sidecars written:
/// - `<payload>.sha256` / `<payload>.crc32c`: hash and crc of the *stored* bytes
///   (fast on-disk corruption detection).
/// - `integrity/<component_id>.sha256`: content hash of the *original* bytes
///   (the manifest's `content_hash`).
pub fn write_component_payload(
    staging: &Path,
    component_id: &str,
    payload: &[u8],
    compression: &CompressionSpec,
) -> FabricResult<StorageRepresentation> {
    let comp_dir = staging.join("components");
    let integrity_dir = staging.join("integrity");
    std::fs::create_dir_all(&comp_dir)?;
    std::fs::create_dir_all(&integrity_dir)?;
    let rel = format!("components/{}", sanitize(component_id));
    let path = comp_dir.join(sanitize(component_id));

    let stored = compression::compress_bytes(compression, payload)?;
    {
        let mut f = std::fs::File::create(&path)?;
        f.write_all(&stored)?;
        f.sync_all()?;
    }
    let stored_hash = integrity::sha256_hex(&stored);
    let sidecar_sha = format!("{}.sha256", path.display());
    let sidecar_crc = format!("{}.crc32c", path.display());
    std::fs::write(&sidecar_sha, format!("{stored_hash}\n"))?;
    std::fs::write(
        &sidecar_crc,
        format!("{:08x}\n", integrity::crc32c(&stored)),
    )?;
    std::fs::write(
        integrity_dir.join(format!("{}.sha256", sanitize(component_id))),
        format!("{}\n", integrity::sha256_hex(payload)),
    )?;

    Ok(StorageRepresentation {
        codec: compression.codec.as_str().to_string(),
        original_size: payload.len() as u64,
        stored_size: stored.len() as u64,
        stored_hash,
        relative_path: rel,
    })
}

/// Read and decode a stored component payload from a committed checkpoint dir,
/// verifying the decoded content against its recorded content hash and size.
pub fn read_component_payload(
    commit: &Path,
    repr: &StorageRepresentation,
    content_hash: &str,
    max_bytes: u64,
) -> FabricResult<Vec<u8>> {
    let path = crate::storage::safe_join(commit, &repr.relative_path)?;
    let stored = std::fs::read(&path)?;
    if (stored.len() as u64) > max_bytes {
        return Err(FabricError::IntegrityFailure(
            "component payload exceeds policy size bound".into(),
        ));
    }
    let codec = compression::Codec::from_str_strict(&repr.codec)?;
    let spec = CompressionSpec {
        codec,
        level: 0,
        format_version: "1".into(),
    };
    let out = compression::decompress_bytes(&spec, &stored, max_bytes)?;
    if (out.len() as u64) != repr.original_size {
        return Err(FabricError::IntegrityFailure(format!(
            "component decoded size {} does not match recorded original size {}",
            out.len(),
            repr.original_size
        )));
    }
    if integrity::sha256_hex(&out) != content_hash {
        return Err(FabricError::IntegrityFailure(format!(
            "component {} content hash mismatch after decode",
            repr.relative_path
        )));
    }
    Ok(out)
}

fn sanitize(s: &str) -> String {
    crate::storage::sanitize_segment(s)
}

/// A restore callback applying a payload into target state.
pub type RestoreCallback = Box<dyn Fn(&[u8]) -> FabricResult<()> + Send + Sync>;
/// A no-argument capture callback.
pub type CaptureCallback = Box<dyn Fn() -> FabricResult<Vec<u8>> + Send + Sync>;
/// A no-argument hook callback (quiesce/resume/cleanup).
pub type HookCallback = Box<dyn Fn() -> FabricResult<()> + Send + Sync>;
/// A named byte region.
pub type Region = (String, Vec<u8>);

/// Generic application-state provider driven by closures.
pub struct ApplicationStateProvider {
    pub spec: ProviderSpec,
    snapshot: CaptureCallback,
    apply: RestoreCallback,
    quiesce_hook: Option<HookCallback>,
    resume_hook: Option<HookCallback>,
    cleanup_hook: Option<HookCallback>,
}

impl ApplicationStateProvider {
    pub fn new(
        spec: ProviderSpec,
        snapshot: impl Fn() -> FabricResult<Vec<u8>> + Send + Sync + 'static,
        apply: impl Fn(&[u8]) -> FabricResult<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            spec,
            snapshot: Box::new(snapshot),
            apply: Box::new(apply),
            quiesce_hook: None,
            resume_hook: None,
            cleanup_hook: None,
        }
    }

    pub fn with_quiesce(
        mut self,
        hook: impl Fn() -> FabricResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.quiesce_hook = Some(Box::new(hook));
        self
    }

    pub fn with_resume(
        mut self,
        hook: impl Fn() -> FabricResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.resume_hook = Some(Box::new(hook));
        self
    }

    pub fn with_cleanup(
        mut self,
        hook: impl Fn() -> FabricResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.cleanup_hook = Some(Box::new(hook));
        self
    }
}

impl CaptureProvider for ApplicationStateProvider {
    fn spec(&self) -> &ProviderSpec {
        &self.spec
    }

    fn prepare_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn quiesce(&self, ctx: &CaptureContext<'_>) -> FabricResult<QuiesceOutcome> {
        if ctx.quiescence == QuiescenceMode::Cooperative {
            match &self.quiesce_hook {
                Some(hook) => {
                    hook()?;
                    Ok(QuiesceOutcome::Acked)
                }
                None => Err(FabricError::QuiescenceFailure(format!(
                    "provider '{}' has no cooperative quiesce hook",
                    self.spec.component_id
                ))),
            }
        } else {
            Ok(QuiesceOutcome::Forced)
        }
    }

    fn capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<Vec<u8>> {
        (self.snapshot)()
    }

    fn verify(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn resume_source(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        if let Some(hook) = &self.resume_hook {
            hook()?;
        }
        Ok(())
    }

    fn abort_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        self.resume_source(_ctx)
    }

    fn restore(&self, _ctx: &RestoreContext<'_>, payload: &[u8]) -> FabricResult<()> {
        (self.apply)(payload)
    }

    fn verify_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn cleanup_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        if let Some(hook) = &self.cleanup_hook {
            hook()?;
        }
        Ok(())
    }
}

/// Filesystem/blob provider: captures a set of files or directories.
///
/// Restore writes only under the target roots; path traversal is rejected.
pub struct FilesystemProvider {
    pub spec: ProviderSpec,
    /// Source paths to capture.
    pub source_paths: Vec<PathBuf>,
    /// Restore root(s): payload paths are joined under these.
    pub restore_roots: Vec<PathBuf>,
    restore_journals: Mutex<HashMap<String, FilesystemRestoreJournal>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FilesystemRestoreJournal {
    /// Normalized paths relative to the configured restore root. Durable
    /// journals never contain caller-controlled absolute rollback targets.
    files: Vec<(String, Option<Vec<u8>>)>,
    created_dirs: Vec<String>,
    root_created: bool,
}

const RESTORE_JOURNAL_DIR: &str = ".checkpoint-fabric-restore-journals";

fn remove_directory_if_empty(path: &Path) -> FabricResult<()> {
    let mut entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(FabricError::CleanupFailure(error.to_string())),
    };
    if entries.next().is_some() {
        return Ok(());
    }
    match std::fs::remove_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            let populated = match std::fs::read_dir(path) {
                Ok(mut current) => current.next().is_some(),
                Err(current) if current.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(_) => false,
            };
            if populated {
                Ok(())
            } else {
                Err(FabricError::CleanupFailure(error.to_string()))
            }
        }
    }
}

impl FilesystemProvider {
    pub fn new(
        spec: ProviderSpec,
        source_paths: Vec<PathBuf>,
        restore_roots: Vec<PathBuf>,
    ) -> Self {
        Self {
            spec,
            source_paths,
            restore_roots,
            restore_journals: Mutex::new(HashMap::new()),
        }
    }

    fn restore_root(&self) -> FabricResult<&Path> {
        self.restore_roots
            .first()
            .map(PathBuf::as_path)
            .ok_or_else(|| {
                FabricError::RestoreFailure("filesystem provider has no restore root".into())
            })
    }

    fn journal_path(&self, attempt_id: &str) -> FabricResult<PathBuf> {
        Ok(self.restore_root()?.join(RESTORE_JOURNAL_DIR).join(format!(
            "{}.json",
            integrity::sha256_hex(attempt_id.as_bytes())
        )))
    }

    fn persist_restore_journal(
        &self,
        attempt_id: &str,
        journal: &FilesystemRestoreJournal,
    ) -> FabricResult<()> {
        let path = self.journal_path(attempt_id)?;
        let dir = path
            .parent()
            .ok_or_else(|| FabricError::RestoreFailure("restore journal has no parent".into()))?;
        if dir.exists() {
            let metadata = std::fs::symlink_metadata(dir)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(FabricError::RestoreFailure(format!(
                    "restore journal directory {} is not a real directory",
                    dir.display()
                )));
            }
        } else {
            std::fs::create_dir_all(dir)?;
        }
        if path.exists() {
            return Err(FabricError::RestoreFailure(format!(
                "filesystem restore attempt {attempt_id} already has a durable journal"
            )));
        }
        let temp = dir.join(format!(".{}.tmp", crate::id::Id::random().to_hex()));
        let bytes = serde_json::to_vec(journal)?;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&temp, &path)?;
        Ok(())
    }

    fn load_restore_journal(
        &self,
        attempt_id: &str,
    ) -> FabricResult<Option<FilesystemRestoreJournal>> {
        let path = self.journal_path(attempt_id)?;
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path)?;
        let journal = serde_json::from_slice(&bytes).map_err(|e| {
            FabricError::CleanupFailure(format!("invalid durable restore journal: {e}"))
        })?;
        Ok(Some(journal))
    }

    fn checked_restore_relative(path: &str) -> FabricResult<PathBuf> {
        let relative = PathBuf::from(path);
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative.has_root()
            || relative
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
            || relative
                .components()
                .next()
                .is_some_and(|c| c.as_os_str() == RESTORE_JOURNAL_DIR)
        {
            return Err(FabricError::InvalidArgument(format!(
                "restore payload path traversal rejected: {path}"
            )));
        }
        Ok(relative)
    }

    fn remove_restore_journal(&self, attempt_id: &str) -> FabricResult<()> {
        let path = self.journal_path(attempt_id)?;
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        if let Some(dir) = path.parent() {
            remove_directory_if_empty(dir)?;
        }
        Ok(())
    }
}

impl CaptureProvider for FilesystemProvider {
    fn spec(&self) -> &ProviderSpec {
        &self.spec
    }

    fn prepare_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        for p in &self.source_paths {
            if !p.exists() {
                return Err(FabricError::CaptureProviderFailure(format!(
                    "filesystem provider source {} does not exist",
                    p.display()
                )));
            }
        }
        Ok(())
    }

    fn quiesce(&self, ctx: &CaptureContext<'_>) -> FabricResult<QuiesceOutcome> {
        // File capture without application quiescence yields at most
        // CRASH_CONSISTENT semantics; cooperative ack is unavailable.
        if ctx.quiescence == QuiescenceMode::Cooperative {
            Err(FabricError::QuiescenceFailure(
                "filesystem provider has no cooperative quiescence".into(),
            ))
        } else {
            Ok(QuiesceOutcome::Forced)
        }
    }

    fn capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<Vec<u8>> {
        let mut entries = Vec::new();
        for p in &self.source_paths {
            if p.is_dir() {
                let mut stack = vec![p.clone()];
                while let Some(d) = stack.pop() {
                    for entry in std::fs::read_dir(&d)? {
                        let entry = entry?;
                        let path = entry.path();
                        let ft = entry.file_type()?;
                        if ft.is_dir() {
                            stack.push(path);
                        } else if ft.is_file() {
                            let rel = path
                                .strip_prefix(p)
                                .map_err(|_| {
                                    FabricError::CaptureProviderFailure("strip_prefix".into())
                                })?
                                .to_string_lossy()
                                .replace('\\', "/");
                            entries.push((rel, std::fs::read(&path)?));
                        }
                    }
                }
            } else if p.is_file() {
                let name = p
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| p.to_string_lossy().to_string());
                entries.push((name, std::fs::read(p)?));
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let payload = serde_json::to_vec(&entries)?;
        Ok(payload)
    }

    fn verify(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn resume_source(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn abort_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn restore(&self, ctx: &RestoreContext<'_>, payload: &[u8]) -> FabricResult<()> {
        let entries: Vec<(String, Vec<u8>)> = serde_json::from_slice(payload)
            .map_err(|e| FabricError::RestoreFailure(format!("bad fs payload: {e}")))?;
        let root = self.restore_root()?;
        if root.exists() && std::fs::symlink_metadata(root)?.file_type().is_symlink() {
            return Err(FabricError::RestoreFailure(format!(
                "filesystem restore root {} is a symlink",
                root.display()
            )));
        }
        let root_created = !root.exists();
        if root_created {
            std::fs::create_dir_all(root)?;
        }
        let mut seen = std::collections::HashSet::new();
        let mut journal = FilesystemRestoreJournal {
            files: Vec::with_capacity(entries.len()),
            created_dirs: Vec::new(),
            root_created,
        };
        for (rel, bytes) in &entries {
            let rel = Self::checked_restore_relative(rel)?;
            let dest = root.join(&rel);
            if !seen.insert(dest.clone()) {
                return Err(FabricError::RestoreFailure(format!(
                    "duplicate filesystem restore destination {}",
                    dest.display()
                )));
            }
            let mut cursor = root.to_path_buf();
            for component in rel.components() {
                cursor.push(component.as_os_str());
                if cursor == dest {
                    break;
                }
                if cursor.exists() {
                    let metadata = std::fs::symlink_metadata(&cursor)?;
                    if metadata.file_type().is_symlink() || !metadata.is_dir() {
                        return Err(FabricError::RestoreFailure(format!(
                            "filesystem restore ancestor {} is not a real directory",
                            cursor.display()
                        )));
                    }
                } else {
                    let relative = cursor.strip_prefix(root).map_err(|_| {
                        FabricError::RestoreFailure("restore directory escaped root".into())
                    })?;
                    let relative = relative.to_string_lossy().to_string();
                    if !journal.created_dirs.contains(&relative) {
                        journal.created_dirs.push(relative);
                    }
                }
            }
            let previous = if dest.exists() {
                let metadata = std::fs::symlink_metadata(&dest)?;
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    return Err(FabricError::RestoreFailure(format!(
                        "filesystem restore destination {} is not a regular file",
                        dest.display()
                    )));
                }
                Some(std::fs::read(&dest)?)
            } else {
                None
            };
            journal
                .files
                .push((rel.to_string_lossy().to_string(), previous));
            let _ = bytes;
        }
        {
            let mut journals = self.restore_journals.lock().unwrap();
            if journals.contains_key(ctx.attempt_id) {
                return Err(FabricError::RestoreFailure(format!(
                    "filesystem restore attempt {} is already active",
                    ctx.attempt_id
                )));
            }
            self.persist_restore_journal(ctx.attempt_id, &journal)?;
            journals.insert(ctx.attempt_id.to_string(), journal);
        }
        for (rel, bytes) in &entries {
            let dest = root.join(rel);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, bytes)?;
        }
        Ok(())
    }

    fn verify_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn cleanup_restore(&self, ctx: &RestoreContext<'_>) -> FabricResult<()> {
        let memory_journal = self.restore_journals.lock().unwrap().remove(ctx.attempt_id);
        let Some(mut journal) = memory_journal.or(self.load_restore_journal(ctx.attempt_id)?)
        else {
            return Ok(());
        };
        let root = self.restore_root()?;
        for (relative, previous) in journal.files.iter().rev() {
            let path = root.join(Self::checked_restore_relative(relative)?);
            match previous {
                Some(bytes) => {
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&path, bytes)?;
                }
                None => {
                    if path.exists() {
                        std::fs::remove_file(&path)
                            .map_err(|e| FabricError::CleanupFailure(e.to_string()))?;
                    }
                }
            }
        }
        journal
            .created_dirs
            .sort_by_key(|p| std::cmp::Reverse(Path::new(p).components().count()));
        for relative in journal.created_dirs {
            let dir = root.join(Self::checked_restore_relative(&relative)?);
            if dir.exists() {
                remove_directory_if_empty(&dir)?;
            }
        }
        self.remove_restore_journal(ctx.attempt_id)?;
        if journal.root_created && root.exists() {
            remove_directory_if_empty(root)?;
        }
        Ok(())
    }

    fn commit_restore(&self, ctx: &RestoreContext<'_>) -> FabricResult<()> {
        self.restore_journals.lock().unwrap().remove(ctx.attempt_id);
        self.remove_restore_journal(ctx.attempt_id)
    }
}

/// Process-metadata provider: workload identity and diagnostics.
/// Never captures secrets; environment values are redacted.
pub struct ProcessMetadataProvider {
    pub spec: ProviderSpec,
    /// Extra environment variable names to include (values only for allowlisted keys).
    pub env_allowlist: Vec<String>,
}

impl ProcessMetadataProvider {
    pub fn new(spec: ProviderSpec) -> Self {
        Self {
            spec,
            env_allowlist: Vec::new(),
        }
    }

    pub fn with_env(mut self, keys: &[&str]) -> Self {
        self.env_allowlist = keys.iter().map(|k| k.to_string()).collect();
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessSnapshot {
    pid: u32,
    command: String,
    workload_id: String,
    captured_at_ms: u64,
    env_keys: Vec<String>,
    env_allowlisted: HashMap<String, String>,
}

impl CaptureProvider for ProcessMetadataProvider {
    fn spec(&self) -> &ProviderSpec {
        &self.spec
    }

    fn prepare_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn quiesce(&self, ctx: &CaptureContext<'_>) -> FabricResult<QuiesceOutcome> {
        if ctx.quiescence == QuiescenceMode::Cooperative {
            Err(FabricError::QuiescenceFailure(
                "process metadata provider cannot quiesce the source process".into(),
            ))
        } else {
            Ok(QuiesceOutcome::Forced)
        }
    }

    fn capture(&self, ctx: &CaptureContext<'_>) -> FabricResult<Vec<u8>> {
        let mut env_keys = Vec::new();
        let mut env_allowlisted = HashMap::new();
        for (k, v) in std::env::vars() {
            if self.env_allowlist.contains(&k) {
                env_allowlisted.insert(k, v);
            } else if k.starts_with("CF_") || k.starts_with("CHECKPOINTFABRIC_") {
                env_keys.push(k);
            }
        }
        env_keys.sort();
        let snap = ProcessSnapshot {
            pid: std::process::id(),
            command: std::env::args().collect::<Vec<_>>().join(" "),
            workload_id: ctx.workload_id.to_hex(),
            captured_at_ms: crate::time::now_ms(),
            env_keys,
            env_allowlisted,
        };
        serde_json::to_vec(&snap).map_err(|e| FabricError::Json(e.to_string()))
    }

    fn verify(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn resume_source(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn abort_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn restore(&self, _ctx: &RestoreContext<'_>, payload: &[u8]) -> FabricResult<()> {
        // Process metadata is diagnostic; it is stored but never re-injected into
        // a live process. Restore succeeds as a no-op with a recorded marker.
        let snap: ProcessSnapshot = serde_json::from_slice(payload)
            .map_err(|e| FabricError::RestoreFailure(format!("bad process snapshot: {e}")))?;
        log::info!(
            "restored process metadata for workload {} (source pid {})",
            snap.workload_id,
            snap.pid
        );
        Ok(())
    }

    fn verify_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn cleanup_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }
}

/// Host-memory-region provider: captures app-registered byte regions by snapshot.
pub struct MemoryRegionProvider {
    pub spec: ProviderSpec,
    /// Logical name -> current bytes (snapshot on demand).
    pub regions: Arc<std::sync::Mutex<Vec<Region>>>,
    pub target_regions: Arc<std::sync::Mutex<Vec<Region>>>,
}

impl MemoryRegionProvider {
    pub fn new(spec: ProviderSpec) -> Self {
        Self {
            spec,
            regions: Arc::new(std::sync::Mutex::new(Vec::new())),
            target_regions: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    pub fn register(&self, name: &str, bytes: Vec<u8>) {
        self.regions.lock().unwrap().push((name.to_string(), bytes));
    }
}

impl CaptureProvider for MemoryRegionProvider {
    fn spec(&self) -> &ProviderSpec {
        &self.spec
    }

    fn prepare_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn quiesce(&self, ctx: &CaptureContext<'_>) -> FabricResult<QuiesceOutcome> {
        if ctx.quiescence == QuiescenceMode::Cooperative {
            Err(FabricError::QuiescenceFailure(
                "memory region provider cannot quiesce the source process".into(),
            ))
        } else {
            Ok(QuiesceOutcome::Forced)
        }
    }

    fn capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<Vec<u8>> {
        let regions = self.regions.lock().unwrap();
        let mut entries: Vec<(String, Vec<u8>)> = regions.clone();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        serde_json::to_vec(&entries).map_err(|e| FabricError::Json(e.to_string()))
    }

    fn verify(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn resume_source(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn abort_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn restore(&self, _ctx: &RestoreContext<'_>, payload: &[u8]) -> FabricResult<()> {
        let entries: Vec<(String, Vec<u8>)> = serde_json::from_slice(payload)
            .map_err(|e| FabricError::RestoreFailure(format!("bad memory payload: {e}")))?;
        let mut target = self.target_regions.lock().unwrap();
        target.clear();
        target.extend(entries);
        Ok(())
    }

    fn verify_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn cleanup_restore(&self, ctx: &RestoreContext<'_>) -> FabricResult<()> {
        let mut target = self.target_regions.lock().unwrap();
        target.clear();
        let _ = ctx;
        Ok(())
    }
}

/// Opaque custom component provider: delegates all operations to closures.
pub struct CustomProvider {
    pub spec: ProviderSpec,
    capture_fn: CaptureCallback,
    restore_fn: RestoreCallback,
    cleanup_fn: HookCallback,
}

impl CustomProvider {
    pub fn new(
        spec: ProviderSpec,
        capture_fn: impl Fn() -> FabricResult<Vec<u8>> + Send + Sync + 'static,
        restore_fn: impl Fn(&[u8]) -> FabricResult<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            spec,
            capture_fn: Box::new(capture_fn),
            restore_fn: Box::new(restore_fn),
            cleanup_fn: Box::new(|| Ok(())),
        }
    }

    pub fn with_cleanup(
        mut self,
        cleanup_fn: impl Fn() -> FabricResult<()> + Send + Sync + 'static,
    ) -> Self {
        self.cleanup_fn = Box::new(cleanup_fn);
        self
    }
}

impl CaptureProvider for CustomProvider {
    fn spec(&self) -> &ProviderSpec {
        &self.spec
    }

    fn prepare_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn quiesce(&self, ctx: &CaptureContext<'_>) -> FabricResult<QuiesceOutcome> {
        if ctx.quiescence == QuiescenceMode::Cooperative {
            Err(FabricError::QuiescenceFailure(
                "custom provider has no cooperative quiescence".into(),
            ))
        } else {
            Ok(QuiesceOutcome::Forced)
        }
    }

    fn capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<Vec<u8>> {
        (self.capture_fn)()
    }

    fn verify(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn resume_source(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn abort_capture(&self, _ctx: &CaptureContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn restore(&self, _ctx: &RestoreContext<'_>, payload: &[u8]) -> FabricResult<()> {
        (self.restore_fn)(payload)
    }

    fn verify_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        Ok(())
    }

    fn cleanup_restore(&self, _ctx: &RestoreContext<'_>) -> FabricResult<()> {
        (self.cleanup_fn)()
    }
}

/// Registry of providers keyed by (workload id, component id).
#[derive(Default)]
pub struct ProviderRegistry {
    providers: std::sync::Mutex<HashMap<(crate::id::Id, String), Arc<dyn CaptureProvider>>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, workload_id: crate::id::Id, provider: Arc<dyn CaptureProvider>) {
        let spec = provider.spec().clone();
        self.providers
            .lock()
            .unwrap()
            .insert((workload_id, spec.component_id), provider);
    }

    pub fn get(
        &self,
        workload_id: &crate::id::Id,
        component_id: &str,
    ) -> Option<Arc<dyn CaptureProvider>> {
        self.providers
            .lock()
            .unwrap()
            .get(&(*workload_id, component_id.to_string()))
            .cloned()
    }

    pub fn list(&self, workload_id: &crate::id::Id) -> Vec<ProviderSpec> {
        let guard = self.providers.lock().unwrap();
        let mut specs: Vec<ProviderSpec> = guard
            .iter()
            .filter(|((wid, _), _)| wid == workload_id)
            .map(|((_, _), p)| p.spec().clone())
            .collect();
        specs.sort_by(|a, b| a.component_id.cmp(&b.component_id));
        specs
    }

    /// Unique restore-handler -> schema version map across all hosted providers.
    pub fn provider_versions(&self) -> std::collections::BTreeMap<String, String> {
        let guard = self.providers.lock().unwrap();
        let mut out = std::collections::BTreeMap::new();
        for (_, p) in guard.iter() {
            let s = p.spec();
            out.entry(s.restore_handler.clone())
                .or_insert_with(|| s.schema_version.to_string());
        }
        out
    }

    pub fn remove(&self, workload_id: &crate::id::Id, component_id: &str) {
        self.providers
            .lock()
            .unwrap()
            .remove(&(*workload_id, component_id.to_string()));
    }

    pub fn remove_workload(&self, workload_id: &crate::id::Id) {
        self.providers
            .lock()
            .unwrap()
            .retain(|key, _| key.0 != *workload_id);
    }
}

/// Build a `ComponentEntry` from a captured component.
pub fn to_component_entry(
    captured: &CapturedComponent,
    repr: &StorageRepresentation,
) -> ComponentEntry {
    ComponentEntry {
        component_id: captured.spec.component_id.clone(),
        component_type: captured.spec.component_type,
        generation: 0,
        required: captured.spec.required,
        logical_size: repr.original_size,
        storage_representation: repr.clone(),
        content_hash: captured.content_hash.clone(),
        schema_version: captured.spec.schema_version,
        restore_handler: captured.spec.restore_handler.clone(),
        compatibility: captured.spec.compatibility.clone(),
        dependencies: captured.spec.dependencies.clone(),
        capture_status: "captured".into(),
        restore_status: "pending".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(id: &str, required: bool) -> ProviderSpec {
        ProviderSpec {
            component_id: id.into(),
            component_type: ComponentType::CustomState,
            required,
            schema_version: 1,
            restore_handler: format!("test/{id}"),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
        }
    }

    #[test]
    fn application_provider_roundtrip() {
        let store = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let p = ApplicationStateProvider::new(spec("app", true), || Ok(b"state-v1".to_vec()), {
            let store = store.clone();
            move |bytes| {
                store.lock().unwrap().extend_from_slice(bytes);
                Ok(())
            }
        });
        assert!(p.spec().required);
        let payload = p
            .capture(&CaptureContext {
                workload_id: crate::id::Id::random(),
                attempt_id: "a",
                staging_dir: Path::new("."),
                compression: &CompressionSpec::none(),
                quiescence: QuiescenceMode::None,
            })
            .unwrap();
        assert_eq!(payload, b"state-v1");
        p.restore(
            &RestoreContext {
                checkpoint_id: crate::id::Id::random(),
                workload_id: crate::id::Id::random(),
                attempt_id: "a",
                staging_dir: Path::new("."),
                commit_dir: Path::new("."),
            },
            &payload,
        )
        .unwrap();
        assert_eq!(*store.lock().unwrap(), b"state-v1");
    }

    #[test]
    fn fs_provider_roundtrip_and_traversal() {
        let src = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("a.txt"), b"aaa").unwrap();
        std::fs::write(src.path().join("sub/b.txt"), b"bbb").unwrap();

        let dst = tempfile::tempdir().unwrap();
        let p = FilesystemProvider::new(
            spec("fs", true),
            vec![src.path().to_path_buf()],
            vec![dst.path().join("restore").to_path_buf()],
        );
        let ctx = CaptureContext {
            workload_id: crate::id::Id::random(),
            attempt_id: "a",
            staging_dir: src.path(),
            compression: &CompressionSpec::none(),
            quiescence: QuiescenceMode::None,
        };
        let payload = p.capture(&ctx).unwrap();
        let rctx = RestoreContext {
            checkpoint_id: crate::id::Id::random(),
            workload_id: crate::id::Id::random(),
            attempt_id: "a",
            staging_dir: Path::new("."),
            commit_dir: Path::new("."),
        };
        p.restore(&rctx, &payload).unwrap();
        assert_eq!(
            std::fs::read(dst.path().join("restore/sub/b.txt")).unwrap(),
            b"bbb"
        );
        p.cleanup_restore(&rctx).unwrap();
        assert!(!dst.path().join("restore").exists());
    }

    #[test]
    fn fs_restore_rollback_survives_provider_restart_and_preserves_unrelated_data() {
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("state.bin"), b"new-state").unwrap();
        let dst = tempfile::tempdir().unwrap();
        let restore_root = dst.path().join("restore");
        std::fs::create_dir_all(&restore_root).unwrap();
        std::fs::write(restore_root.join("state.bin"), b"old-state").unwrap();
        std::fs::write(restore_root.join("unrelated.bin"), b"unrelated").unwrap();

        let capture = FilesystemProvider::new(
            spec("fs", true),
            vec![src.path().to_path_buf()],
            vec![restore_root.clone()],
        );
        let capture_ctx = CaptureContext {
            workload_id: crate::id::Id::random(),
            attempt_id: "capture",
            staging_dir: src.path(),
            compression: &CompressionSpec::none(),
            quiescence: QuiescenceMode::None,
        };
        let payload = capture.capture(&capture_ctx).unwrap();
        let restore_ctx = RestoreContext {
            checkpoint_id: crate::id::Id::random(),
            workload_id: crate::id::Id::random(),
            attempt_id: "restore-crash",
            staging_dir: Path::new("."),
            commit_dir: Path::new("."),
        };
        capture.restore(&restore_ctx, &payload).unwrap();
        assert_eq!(
            std::fs::read(restore_root.join("state.bin")).unwrap(),
            b"new-state"
        );
        drop(capture);

        let restarted =
            FilesystemProvider::new(spec("fs", true), Vec::new(), vec![restore_root.clone()]);
        restarted.cleanup_restore(&restore_ctx).unwrap();
        assert_eq!(
            std::fs::read(restore_root.join("state.bin")).unwrap(),
            b"old-state"
        );
        assert_eq!(
            std::fs::read(restore_root.join("unrelated.bin")).unwrap(),
            b"unrelated"
        );
        assert!(!restore_root.join(RESTORE_JOURNAL_DIR).exists());
    }

    #[test]
    fn component_payload_write_read() {
        let dir = tempfile::tempdir().unwrap();
        let content = b"payload-data".repeat(10);
        let repr = write_component_payload(
            dir.path(),
            "c1",
            content.as_slice(),
            &CompressionSpec::zstd(3),
        )
        .unwrap();
        assert!(repr.stored_size < repr.original_size);
        let out =
            read_component_payload(dir.path(), &repr, &integrity::sha256_hex(&content), 1 << 20)
                .unwrap();
        assert_eq!(out, content);
    }

    #[test]
    fn bounded_read_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let content = b"1234567890";
        let repr =
            write_component_payload(dir.path(), "c1", content, &CompressionSpec::none()).unwrap();
        assert!(
            read_component_payload(dir.path(), &repr, &integrity::sha256_hex(content), 5).is_err()
        );
    }

    #[test]
    fn memory_provider_roundtrip() {
        let p = MemoryRegionProvider::new(spec("mem", true));
        p.register("r1", b"mem-bytes".to_vec());
        let payload = p
            .capture(&CaptureContext {
                workload_id: crate::id::Id::random(),
                attempt_id: "a",
                staging_dir: Path::new("."),
                compression: &CompressionSpec::none(),
                quiescence: QuiescenceMode::None,
            })
            .unwrap();
        p.restore(
            &RestoreContext {
                checkpoint_id: crate::id::Id::random(),
                workload_id: crate::id::Id::random(),
                attempt_id: "a",
                staging_dir: Path::new("."),
                commit_dir: Path::new("."),
            },
            &payload,
        )
        .unwrap();
        let t = p.target_regions.lock().unwrap();
        assert_eq!(t[0].1, b"mem-bytes");
    }

    #[test]
    fn registry_scoping() {
        let reg = ProviderRegistry::new();
        let wid = crate::id::Id::random();
        reg.register(
            wid,
            Arc::new(ApplicationStateProvider::new(
                spec("app", true),
                || Ok(Vec::new()),
                |_| Ok(()),
            )),
        );
        assert!(reg.get(&wid, "app").is_some());
        assert!(reg.get(&crate::id::Id::random(), "app").is_none());
        assert_eq!(reg.list(&wid).len(), 1);
        reg.remove_workload(&wid);
        assert!(reg.get(&wid, "app").is_none());
    }
}
