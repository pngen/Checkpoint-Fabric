//! Node runtime: registration, heartbeat, capture-provider hosting, checkpoint
//! capture/restore, integrity verification, temporary-storage management,
//! restart recovery, target provisioning, and source quiescence/resume.
//!
//! Node identity is `name@pid@boot-id`, which avoids collisions across restarts.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Seek, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};

use crate::capture::QuiescenceMode;
use crate::checkpoint::{ComponentEntry, HardwareCompatibilityDescriptor};
use crate::compatibility::RuntimeCompatibilityDescriptor;
use crate::errors::{FabricError, FabricResult};
use crate::failpoints;
use crate::id::Id;
use crate::integrity;
use crate::protocol::{self, Op};
use crate::providers::{
    read_component_payload, write_component_payload, CaptureContext, CaptureProvider,
    ProviderRegistry, ProviderSpec, QuiesceOutcome, RestoreContext,
};
use crate::storage::{LocalStorage, StorageBackend};
use crate::time::now_ms;
use crate::transport::{RequestHandler, RpcClient, Server};

/// A durable record of a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub addr: String,
    pub boot_id: String,
    pub status: String,
    pub registered_ms: u64,
    pub last_heartbeat_ms: u64,
    pub resources: String,
}

/// Configuration for a node runtime.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub name: String,
    pub coordinator_addr: SocketAddr,
    pub listen_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub heartbeat_ms: u64,
    pub max_connections: usize,
    pub staging_ttl_ms: u64,
    pub runtime: RuntimeCompatibilityDescriptor,
    pub hardware: HardwareCompatibilityDescriptor,
    pub resources: serde_json::Value,
    pub failpoints: Option<String>,
}

impl NodeConfig {
    pub fn default_in(name: &str, coordinator: SocketAddr, data_dir: PathBuf) -> Self {
        Self {
            name: name.to_string(),
            coordinator_addr: coordinator,
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            data_dir,
            heartbeat_ms: 1_000,
            max_connections: 16,
            staging_ttl_ms: 3_600_000,
            runtime: RuntimeCompatibilityDescriptor::local_default(),
            hardware: HardwareCompatibilityDescriptor::default(),
            resources: serde_json::json!({}),
            failpoints: None,
        }
    }
}

/// A workload attached to this node.
#[derive(Debug, Clone)]
pub struct AttachedWorkload {
    pub fence_token: String,
    pub execution_epoch: u64,
}

type PendingRestore = (Id, Id, Vec<String>);

/// The node runtime.
pub struct NodeRuntime {
    pub node_id: String,
    pub boot_id: String,
    pub config: NodeConfig,
    pub storage: Arc<LocalStorage>,
    pub providers: Arc<ProviderRegistry>,
    attached: Arc<Mutex<HashMap<Id, AttachedWorkload>>>,
    stop: Arc<AtomicBool>,
    coordinator_epoch: Arc<AtomicU64>,
    coordinator: Arc<Mutex<Option<RpcClient>>>,
    server: Option<Arc<Server>>,
    /// The bound listen address (set once at start).
    listen: Option<SocketAddr>,
    serve_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    heartbeat_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    fetch_tokens: Mutex<HashMap<(Id, String), u64>>,
    staging_leases: Arc<Mutex<HashMap<PathBuf, u64>>>,
    quiesced_captures: Mutex<HashMap<String, Id>>,
    pending_restores: Mutex<HashMap<String, PendingRestore>>,
}

impl NodeRuntime {
    /// Start the node: bind the listener, register with the coordinator (with
    /// bounded retries), and begin heartbeats.
    pub fn start(config: NodeConfig) -> FabricResult<Arc<Self>> {
        if let Some(fp) = &config.failpoints {
            failpoints::arm_from_spec(fp);
        }
        let boot_id = Id::random().to_hex();
        let node_id = format!("{}@{}@{}", config.name, std::process::id(), &boot_id[..8]);
        let storage = Arc::new(LocalStorage::new(&config.data_dir)?);
        // Startup is the only point at which no live operation can own staging.
        // Clear leftovers from a prior process before accepting work.
        storage.recover_partial(&HashSet::new())?;

        let listen_addr = config.listen_addr;
        let max_connections = config.max_connections;
        let listener = std::net::TcpListener::bind(listen_addr)
            .map_err(|e| FabricError::TransportError(format!("node listener: {e}")))?;
        let listen = listener.local_addr().ok();
        let this: Arc<NodeRuntime> = Arc::new_cyclic(|weak| {
            let w: Weak<NodeRuntime> = weak.clone();
            let handler: RequestHandler = Arc::new(move |_conn, op, _req_id, payload| {
                let node = w
                    .upgrade()
                    .ok_or_else(|| FabricError::Internal("node has shut down".into()))?;
                let resp = node.dispatch(op, payload);
                match resp {
                    Ok(p) => Ok(p),
                    Err(e) => {
                        log::warn!("node op {op:?} failed: {e}");
                        Ok(serde_json::to_vec(&protocol::Envelope::err(
                            &e.to_string(),
                        ))?)
                    }
                }
            });
            let server = Arc::new(
                Server::from_parts(listener, handler).with_max_connections(max_connections),
            );
            NodeRuntime {
                node_id,
                boot_id,
                config,
                storage,
                providers: Arc::new(ProviderRegistry::new()),
                attached: Arc::new(Mutex::new(HashMap::new())),
                stop: Arc::new(AtomicBool::new(false)),
                coordinator_epoch: Arc::new(AtomicU64::new(0)),
                coordinator: Arc::new(Mutex::new(None)),
                server: Some(server),
                listen,
                serve_thread: Mutex::new(None),
                heartbeat_thread: Mutex::new(None),
                fetch_tokens: Mutex::new(HashMap::new()),
                staging_leases: Arc::new(Mutex::new(HashMap::new())),
                quiesced_captures: Mutex::new(HashMap::new()),
                pending_restores: Mutex::new(HashMap::new()),
            }
        });

        // Register with the coordinator (bounded retries over ~30s).
        this.register_with_retries()?;

        let serve_self = this.clone();
        let serve_handle = std::thread::spawn(move || {
            if let Some(srv) = serve_self.server.as_ref() {
                if let Err(e) = srv.serve() {
                    log::error!("node server ended: {e}");
                }
            }
        });
        *this.serve_thread.lock().unwrap() = Some(serve_handle);

        let hb_cfg = this.config.clone();
        let hb_stop = this.stop.clone();
        let hb_epoch = this.coordinator_epoch.clone();
        let hb_coord = this.coordinator.clone();
        let hb_attached = this.attached.clone();
        let hb_node_id = this.node_id.clone();
        let hb_boot_id = this.boot_id.clone();
        let hb_storage = this.storage.clone();
        let hb_staging_leases = this.staging_leases.clone();
        let hb_staging_ttl_ms = this.config.staging_ttl_ms;
        let hb_heartbeat_ms = this.config.heartbeat_ms;
        let hb_providers = this.providers.clone();
        let handle = std::thread::spawn(move || {
            let mut consecutive_failures: u32 = 0;
            let mut tick: u64 = 0;
            loop {
                if hb_stop.load(Ordering::SeqCst) {
                    return;
                }
                let ok = heartbeat_tick(
                    &hb_node_id,
                    &hb_boot_id,
                    &hb_epoch,
                    &hb_coord,
                    &hb_attached,
                    &hb_cfg.resources,
                    &hb_providers,
                    &hb_storage,
                );
                if ok {
                    consecutive_failures = 0;
                } else {
                    consecutive_failures += 1;
                    if consecutive_failures >= 10 {
                        log::error!(
                            "node {hb_node_id} lost contact with coordinator; shutting down"
                        );
                        hb_stop.store(true, Ordering::SeqCst);
                        return;
                    }
                }
                tick += 1;
                if tick % 10 == 0 {
                    let keep: HashSet<PathBuf> = {
                        let leases = hb_staging_leases.lock().unwrap();
                        leases.keys().cloned().collect()
                    };
                    if let Ok(removed) =
                        hb_storage.recover_partial_older_than(&keep, hb_staging_ttl_ms)
                    {
                        if !removed.is_empty() {
                            log::info!("staging sweep removed {} stale directories", removed.len());
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(hb_heartbeat_ms));
            }
        });
        *this.heartbeat_thread.lock().unwrap() = Some(handle);
        Ok(this)
    }

    /// The address the node is actually listening on.
    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.listen
    }

    /// Shared stop flag (for graceful shutdown drivers).
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    fn register_with_retries(&self) -> FabricResult<()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut last_err = None;
        while std::time::Instant::now() < deadline {
            match self.register_once() {
                Ok(epoch) => {
                    self.coordinator_epoch.store(epoch, Ordering::SeqCst);
                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| FabricError::Timeout("node registration timed out".into())))
    }

    fn register_once(&self) -> FabricResult<u64> {
        let mut client = RpcClient::connect(&self.config.coordinator_addr)?;
        let req = protocol::NodeRegisterRequest {
            node_id: self.node_id.clone(),
            listen_addr: self
                .listen_addr()
                .ok_or_else(|| FabricError::Internal("node has no listener yet".into()))?
                .to_string(),
            boot_id: self.boot_id.clone(),
            data_dir: self.config.data_dir.to_string_lossy().to_string(),
            runtime: self.config.runtime.clone(),
            hardware: self.config.hardware.clone(),
            resources: self.config.resources.clone(),
            committed_checkpoints: self.storage.enumerate_committed()?,
            coordinator_epoch: self.coordinator_epoch.load(Ordering::SeqCst),
        };
        let resp: protocol::NodeRegisterResponse = client.call_json(Op::NodeRegister, &req)?;
        // Adopt the coordinator's epoch even on rejection so retries carry a
        // current epoch (the coordinator increments its epoch on every start).
        self.coordinator_epoch
            .store(resp.coordinator_epoch, Ordering::SeqCst);
        if resp.accepted {
            *self.coordinator.lock().unwrap() = Some(client);
            Ok(resp.coordinator_epoch)
        } else {
            Err(FabricError::FencingFailure(
                resp.error.unwrap_or_else(|| "registration rejected".into()),
            ))
        }
    }

    // ================= workload attachment =================

    /// Attach a workload: register providers locally and claim with the coordinator.
    pub fn attach_workload(
        &self,
        workload_id: &Id,
        fence_epoch: u64,
        providers: Vec<Arc<dyn CaptureProvider>>,
    ) -> FabricResult<AttachedWorkload> {
        let specs: Vec<ProviderSpec> = providers.iter().map(|p| p.spec().clone()).collect();
        for p in providers {
            self.providers.register(*workload_id, p);
        }
        let mut guard = self.coordinator.lock().unwrap();
        let client = guard
            .as_mut()
            .ok_or_else(|| FabricError::Internal("not connected to coordinator".into()))?;
        let req = protocol::WorkloadAttachRequest {
            workload_id: *workload_id,
            node: self.node_id.clone(),
            node_boot_id: self.boot_id.clone(),
            providers: specs,
            fence_epoch,
        };
        let resp: protocol::WorkloadAttachResponse = client.call_json(Op::WorkloadAttach, &req)?;
        if !resp.accepted {
            self.providers.remove_workload(workload_id);
            return Err(FabricError::FencingFailure(
                resp.error.unwrap_or_else(|| "attach rejected".into()),
            ));
        }
        let attached = AttachedWorkload {
            fence_token: resp.fence_token.unwrap_or_default(),
            execution_epoch: fence_epoch,
        };
        self.attached
            .lock()
            .unwrap()
            .insert(*workload_id, attached.clone());
        Ok(attached)
    }

    pub fn detach_workload(&self, workload_id: &Id) {
        self.attached.lock().unwrap().remove(workload_id);
        self.providers.remove_workload(workload_id);
    }

    pub fn is_attached(&self, workload_id: &Id) -> bool {
        self.attached.lock().unwrap().contains_key(workload_id)
    }

    pub fn attached_workloads(&self) -> Vec<Id> {
        self.attached.lock().unwrap().keys().copied().collect()
    }

    pub fn provider_specs(&self, workload_id: &Id) -> Vec<ProviderSpec> {
        self.providers.list(workload_id)
    }

    // ================= request dispatch =================

    fn dispatch(&self, op: Op, payload: &[u8]) -> FabricResult<Vec<u8>> {
        match op {
            Op::Ping => Ok(b"pong".to_vec()),
            Op::Hello => Ok(serde_json::to_vec(&protocol::Envelope::ok())?),
            Op::NodeCaptureRequest => {
                let req: protocol::NodeCaptureRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_capture(&req)?)?)
            }
            Op::NodePromoteRequest => {
                let req: protocol::NodePromoteRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_promote(&req)?)?)
            }
            Op::NodeResumeRequest => {
                let req: protocol::NodeResumeRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_resume(&req)?)?)
            }
            Op::NodeRestoreRequest => {
                let req: protocol::NodeRestoreRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_restore(&req)?)?)
            }
            Op::NodeVerifyRequest => {
                let req: protocol::NodeVerifyRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_verify(&req)?)?)
            }
            Op::NodeProbeRequest => {
                let req: protocol::NodeProbeRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_probe(&req)?)?)
            }
            Op::NodeCleanupRequest => {
                let req: protocol::NodeCleanupRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_cleanup(&req)?)?)
            }
            Op::NodeReplicateRequest => {
                let req: protocol::NodeReplicateRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                Ok(serde_json::to_vec(&self.do_replicate(&req)?)?)
            }
            Op::NodeAllowFetch => {
                let req: protocol::NodeAllowFetchRequest = serde_json::from_slice(payload)?;
                self.check_epoch(req.coordinator_epoch)?;
                self.fetch_tokens
                    .lock()
                    .unwrap()
                    .insert((req.checkpoint_id, req.fetch_token), now_ms() + req.ttl_ms);
                Ok(serde_json::to_vec(&protocol::Envelope::ok())?)
            }
            Op::NodeDetachRequest => {
                let req: protocol::WorkloadDetachRequest = serde_json::from_slice(payload)?;
                self.detach_workload(&req.workload_id);
                Ok(serde_json::to_vec(&protocol::Envelope::ok())?)
            }
            Op::FetchManifest => {
                let req: protocol::FetchManifestRequest = serde_json::from_slice(payload)?;
                Ok(serde_json::to_vec(&self.serve_manifest(&req)?)?)
            }
            Op::FetchComponent => {
                let req: protocol::FetchComponentRequest = serde_json::from_slice(payload)?;
                Ok(serde_json::to_vec(&self.serve_component(&req)?)?)
            }
            _ => Err(FabricError::ProtocolError(format!(
                "node cannot serve op {op:?}"
            ))),
        }
    }

    fn check_epoch(&self, claimed: u64) -> FabricResult<()> {
        let actual = self.coordinator_epoch.load(Ordering::SeqCst);
        if claimed != actual {
            return Err(FabricError::StaleCoordinatorEpoch {
                expected: actual,
                got: claimed,
            });
        }
        Ok(())
    }

    fn check_token(&self, checkpoint_id: &Id, token: &str) -> FabricResult<()> {
        let now = now_ms();
        let mut tokens = self.fetch_tokens.lock().unwrap();
        // Sweep expired tokens so a multi-fetch replication window (manifest
        // plus one call per component) cannot leak entries indefinitely.
        tokens.retain(|_, exp| *exp >= now);
        match tokens.get(&(*checkpoint_id, token.to_string())) {
            Some(_) => Ok(()),
            None => Err(FabricError::FencingFailure(format!(
                "unknown fetch token for {checkpoint_id}"
            ))),
        }
    }

    fn lease_staging(&self, path: &Path) {
        self.staging_leases
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), now_ms());
    }

    fn release_staging(&self, path: &Path) {
        self.staging_leases.lock().unwrap().remove(path);
    }

    // ================= capture =================

    fn do_capture(
        &self,
        req: &protocol::NodeCaptureRequest,
    ) -> FabricResult<protocol::NodeCaptureResult> {
        // Fencing is enforced by the coordinator before dispatch (single-active
        // workloads may only be captured from their active node). The node-side
        // attachment registry is advisory for provider hosting.
        if !self.is_attached(&req.workload_id) {
            log::warn!(
                "capture requested for workload {} with no attached providers on node {}",
                req.workload_id,
                self.node_id
            );
        }

        let staging = self.storage.staging_dir(&req.attempt_id);
        self.lease_staging(&staging);
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        std::fs::create_dir_all(staging.join("components"))?;
        std::fs::create_dir_all(staging.join("integrity"))?;
        std::fs::create_dir_all(staging.join("journal"))?;

        let mut prepared: Vec<Arc<dyn CaptureProvider>> = Vec::new();
        let mut captured: Vec<ComponentEntry> = Vec::new();
        let mut total_logical = 0u64;
        let mut total_stored = 0u64;
        let mut compressed = 0u64;
        let mut cooperative_ack = false;

        let result = (|| -> FabricResult<()> {
            // Select providers for the requested components.
            let mut providers: Vec<Arc<dyn CaptureProvider>> = Vec::new();
            for c in &req.components {
                match self.providers.get(&req.workload_id, &c.component_id) {
                    Some(p) => providers.push(p),
                    None if c.required => {
                        return Err(FabricError::MissingDependency(format!(
                            "required component '{}' has no provider attached",
                            c.component_id
                        )));
                    }
                    None => {
                        log::warn!(
                            "optional component '{}' skipped: no provider",
                            c.component_id
                        );
                    }
                }
            }

            let ctx = CaptureContext {
                workload_id: req.workload_id,
                attempt_id: &req.attempt_id,
                staging_dir: &staging,
                compression: &req.compression,
                quiescence: req.quiescence,
            };

            for p in &providers {
                p.prepare_capture(&ctx)?;
                prepared.push(p.clone());
            }

            // Quiescence.
            match req.quiescence {
                QuiescenceMode::None => {}
                QuiescenceMode::Cooperative => {
                    cooperative_ack = !providers.is_empty();
                    for p in &providers {
                        match p.quiesce(&ctx) {
                            Ok(QuiesceOutcome::Acked) => {}
                            Ok(QuiesceOutcome::Forced) => cooperative_ack = false,
                            Err(e) => return Err(e),
                        }
                    }
                }
                QuiescenceMode::Forced => {
                    for p in &providers {
                        let _ = p.quiesce(&ctx);
                    }
                }
            }

            for p in &providers {
                let spec = p.spec();
                let payload = p.capture(&ctx)?;
                p.verify(&ctx)?;
                let repr = write_component_payload(
                    &staging,
                    &spec.component_id,
                    &payload,
                    &req.compression,
                )?;
                let content_hash = integrity::sha256_hex(&payload);
                total_logical = total_logical.saturating_add(payload.len() as u64);
                total_stored = total_stored.saturating_add(repr.stored_size);
                if req.compression.is_compressed() {
                    compressed = compressed.saturating_add(repr.stored_size);
                }
                captured.push(ComponentEntry {
                    component_id: spec.component_id.clone(),
                    component_type: spec.component_type,
                    generation: 0,
                    required: spec.required,
                    logical_size: repr.original_size,
                    storage_representation: repr,
                    content_hash,
                    schema_version: spec.schema_version,
                    restore_handler: spec.restore_handler.clone(),
                    compatibility: spec.compatibility.clone(),
                    dependencies: spec.dependencies.clone(),
                    capture_status: "captured".into(),
                    restore_status: "pending".into(),
                });
            }
            Ok(())
        })();

        if let Err(e) = result {
            // Clean abort: resume quiesced providers, drop staging.
            for p in prepared.iter().rev() {
                let _ = p.abort_capture(&CaptureContext {
                    workload_id: req.workload_id,
                    attempt_id: &req.attempt_id,
                    staging_dir: &staging,
                    compression: &req.compression,
                    quiescence: req.quiescence,
                });
            }
            let _ = std::fs::remove_dir_all(&staging);
            self.release_staging(&staging);
            return Ok(protocol::NodeCaptureResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some(e.to_string()),
                components: Vec::new(),
                total_logical_bytes: 0,
                total_physical_bytes: 0,
                compressed_bytes: 0,
                cooperative_ack: false,
                staging_path: None,
            });
        }

        self.quiesced_captures
            .lock()
            .unwrap()
            .insert(req.attempt_id.clone(), req.workload_id);

        Ok(protocol::NodeCaptureResult {
            attempt_id: req.attempt_id.clone(),
            ok: true,
            error: None,
            components: captured,
            total_logical_bytes: total_logical,
            total_physical_bytes: total_stored,
            compressed_bytes: compressed,
            cooperative_ack,
            staging_path: Some(staging.to_string_lossy().to_string()),
        })
    }

    // ================= promote =================

    fn do_promote(
        &self,
        req: &protocol::NodePromoteRequest,
    ) -> FabricResult<protocol::NodePromoteResult> {
        let computed_digest = integrity::sha256_hex(&req.manifest_bytes);
        if computed_digest != req.digest {
            return Err(FabricError::IntegrityFailure(
                "promoted manifest does not match coordinator digest".into(),
            ));
        }
        let parsed = crate::manifest::parse(&req.manifest_bytes)?;
        if parsed.checkpoint_id != req.checkpoint_id
            || parsed.capture_attempt != req.attempt_id
            || parsed.integrity.root != req.integrity_root
        {
            return Err(FabricError::IntegrityFailure(
                "promoted manifest identity or integrity root mismatch".into(),
            ));
        }
        let staging = self.storage.staging_dir(&req.attempt_id);
        if !staging.exists() {
            return Ok(protocol::NodePromoteResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some("staging directory is missing".into()),
                commit_path: None,
            });
        }
        std::fs::write(staging.join("manifest"), &req.manifest_bytes)?;
        std::fs::write(staging.join("manifest.digest"), format!("{}\n", req.digest))?;
        std::fs::create_dir_all(staging.join("integrity"))?;
        std::fs::write(
            staging.join("integrity/root"),
            format!("{}\n", req.integrity_root),
        )?;
        let journal_line = format!("{} promote manifest\n", now_ms());
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(staging.join("journal/records.jsonl"))?
            .write_all(journal_line.as_bytes())?;

        let commit = self.storage.commit_dir(&req.checkpoint_id);
        match self.storage.promote(&staging, &commit) {
            Ok(()) => {
                self.release_staging(&staging);
                Ok(protocol::NodePromoteResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: true,
                    error: None,
                    commit_path: Some(commit.to_string_lossy().to_string()),
                })
            }
            Err(e) => Ok(protocol::NodePromoteResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some(e.to_string()),
                commit_path: None,
            }),
        }
    }

    // ================= resume =================

    fn do_resume(
        &self,
        req: &protocol::NodeResumeRequest,
    ) -> FabricResult<protocol::NodeResumeResult> {
        let authority_transfer = req.fence_token.is_some() || req.execution_epoch.is_some();
        if authority_transfer {
            let token = req.fence_token.clone().ok_or_else(|| {
                FabricError::FencingFailure("authority grant is missing fence token".into())
            })?;
            let execution_epoch = req.execution_epoch.ok_or_else(|| {
                FabricError::FencingFailure("authority grant is missing execution epoch".into())
            })?;
            self.attached.lock().unwrap().insert(
                req.workload_id,
                AttachedWorkload {
                    fence_token: token,
                    execution_epoch,
                },
            );
        } else if self
            .quiesced_captures
            .lock()
            .unwrap()
            .remove(&req.attempt_id)
            .is_none()
        {
            // An abort before the node quiesced anything is an idempotent no-op.
            return Ok(protocol::NodeResumeResult {
                attempt_id: req.attempt_id.clone(),
                ok: true,
                error: None,
            });
        }

        let providers = self.providers.list(&req.workload_id);
        let mut first_err = None;
        let restore_staging = self.storage.staging_dir(&req.attempt_id);
        let restore_commit = self.storage.commit_dir(&req.checkpoint_id);
        let ctx = CaptureContext {
            workload_id: req.workload_id,
            attempt_id: &req.attempt_id,
            staging_dir: Path::new(""),
            compression: &crate::compression::CompressionSpec::none(),
            quiescence: QuiescenceMode::None,
        };
        for spec in &providers {
            if let Some(p) = self.providers.get(&req.workload_id, &spec.component_id) {
                if authority_transfer {
                    let restore_ctx = RestoreContext {
                        checkpoint_id: req.checkpoint_id,
                        workload_id: req.workload_id,
                        attempt_id: &req.attempt_id,
                        staging_dir: &restore_staging,
                        commit_dir: &restore_commit,
                    };
                    if let Err(e) = p.commit_restore(&restore_ctx) {
                        first_err.get_or_insert(e);
                    }
                }
                if req.resume {
                    if let Err(e) = p.resume_source(&ctx) {
                        first_err.get_or_insert(e);
                    }
                }
            }
        }
        if authority_transfer && first_err.is_none() {
            self.pending_restores
                .lock()
                .unwrap()
                .remove(&req.attempt_id);
        }
        Ok(protocol::NodeResumeResult {
            attempt_id: req.attempt_id.clone(),
            ok: first_err.is_none(),
            error: first_err.map(|e| e.to_string()),
        })
    }

    // ================= restore =================

    fn do_restore(
        &self,
        req: &protocol::NodeRestoreRequest,
    ) -> FabricResult<protocol::NodeRestoreResult> {
        failpoints::fire("restore.node.before_apply")?;
        // Integrity-first: verify the local replica before applying anything.
        let commit = self.storage.commit_dir(&req.checkpoint_id);
        let manifest_bytes = match std::fs::read(commit.join("manifest")) {
            Ok(b) => b,
            Err(_) => {
                return Ok(protocol::NodeRestoreResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(format!(
                        "no local replica of {} on node {}",
                        req.checkpoint_id, self.node_id
                    )),
                    restored_components: Vec::new(),
                })
            }
        };
        let actual_digest = integrity::sha256_hex(&manifest_bytes);
        let sidecar_digest = std::fs::read_to_string(commit.join("manifest.digest"))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if actual_digest != req.expected_manifest_digest
            || sidecar_digest != req.expected_manifest_digest
        {
            return Ok(protocol::NodeRestoreResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some("manifest does not match coordinator integrity anchor".into()),
                restored_components: Vec::new(),
            });
        }
        let manifest = crate::manifest::parse(&manifest_bytes)?;
        if manifest.checkpoint_id != req.checkpoint_id || manifest.workload_id != req.workload_id {
            return Ok(protocol::NodeRestoreResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some("manifest identity does not match restore request".into()),
                restored_components: Vec::new(),
            });
        }
        for c in &manifest.components {
            let path = crate::storage::safe_join(&commit, &c.storage_representation.relative_path)?;
            if !path.is_file() {
                return Ok(protocol::NodeRestoreResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(format!(
                        "missing component payload {}",
                        c.storage_representation.relative_path
                    )),
                    restored_components: Vec::new(),
                });
            }
            // Fast corruption check via sidecar crc, then full content hash of
            // the stored bytes.
            if let Err(e) = integrity::verify_stored_file(
                &path,
                &c.storage_representation.stored_hash,
                Some(c.storage_representation.stored_size),
            ) {
                return Ok(protocol::NodeRestoreResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(format!(
                        "component '{}' failed integrity: {e}",
                        c.component_id
                    )),
                    restored_components: Vec::new(),
                });
            }
        }

        // Restore components in manifest order.
        let restore_staging = self.storage.staging_dir(&req.attempt_id);
        let ctx = RestoreContext {
            checkpoint_id: req.checkpoint_id,
            workload_id: req.workload_id,
            attempt_id: &req.attempt_id,
            staging_dir: &restore_staging,
            commit_dir: &commit,
        };
        let mut restored: Vec<String> = Vec::new();
        let mut restored_providers: Vec<Arc<dyn CaptureProvider>> = Vec::new();
        for c in &manifest.components {
            let provider = match self.providers.get(&req.workload_id, &c.component_id) {
                Some(p) => p,
                None if c.required => {
                    let _ = self.cleanup_restored(&restored_providers, &ctx);
                    return Ok(protocol::NodeRestoreResult {
                        attempt_id: req.attempt_id.clone(),
                        ok: false,
                        error: Some(format!(
                            "no restore handler '{}' for required component '{}'",
                            c.restore_handler, c.component_id
                        )),
                        restored_components: restored,
                    });
                }
                None => {
                    log::warn!(
                        "optional component '{}' skipped: no provider",
                        c.component_id
                    );
                    continue;
                }
            };
            let max_bytes = req.max_component_bytes.unwrap_or(1 << 30);
            match read_component_payload(
                &commit,
                &c.storage_representation,
                &c.content_hash,
                max_bytes,
            ) {
                Ok(payload) => {
                    // Include the current provider in rollback even when its
                    // restore or verification fails after making partial changes.
                    restored_providers.push(provider.clone());
                    if let Err(e) = provider.restore(&ctx, &payload) {
                        let _ = self.cleanup_restored(&restored_providers, &ctx);
                        return Ok(protocol::NodeRestoreResult {
                            attempt_id: req.attempt_id.clone(),
                            ok: false,
                            error: Some(format!("restore of '{}' failed: {e}", c.component_id)),
                            restored_components: restored,
                        });
                    }
                    if let Err(e) = provider.verify_restore(&ctx) {
                        let _ = self.cleanup_restored(&restored_providers, &ctx);
                        return Ok(protocol::NodeRestoreResult {
                            attempt_id: req.attempt_id.clone(),
                            ok: false,
                            error: Some(format!(
                                "verification of '{}' failed: {e}",
                                c.component_id
                            )),
                            restored_components: restored,
                        });
                    }
                    restored.push(c.component_id.clone());
                }
                Err(e) => {
                    let _ = self.cleanup_restored(&restored_providers, &ctx);
                    return Ok(protocol::NodeRestoreResult {
                        attempt_id: req.attempt_id.clone(),
                        ok: false,
                        error: Some(format!(
                            "payload decode of '{}' failed: {e}",
                            c.component_id
                        )),
                        restored_components: restored,
                    });
                }
            }
        }
        self.pending_restores.lock().unwrap().insert(
            req.attempt_id.clone(),
            (req.workload_id, req.checkpoint_id, restored.clone()),
        );
        Ok(protocol::NodeRestoreResult {
            attempt_id: req.attempt_id.clone(),
            ok: true,
            error: None,
            restored_components: restored,
        })
    }

    fn cleanup_restored(
        &self,
        providers: &[Arc<dyn CaptureProvider>],
        ctx: &RestoreContext<'_>,
    ) -> FabricResult<()> {
        let mut first_err: Option<FabricError> = None;
        for p in providers.iter().rev() {
            if let Err(e) = p.cleanup_restore(ctx) {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }
        match first_err {
            Some(e) => Err(FabricError::CleanupFailure(e.to_string())),
            None => Ok(()),
        }
    }

    // ================= verify / probe / cleanup =================

    fn do_verify(
        &self,
        req: &protocol::NodeVerifyRequest,
    ) -> FabricResult<protocol::NodeVerifyResult> {
        let commit = self.storage.commit_dir(&req.checkpoint_id);
        let manifest_bytes = match std::fs::read(commit.join("manifest")) {
            Ok(b) => b,
            Err(_) => {
                return Ok(protocol::NodeVerifyResult {
                    checkpoint_id: req.checkpoint_id,
                    ok: false,
                    error: Some("checkpoint not present on this node".into()),
                    manifest_digest: None,
                })
            }
        };
        let parsed = crate::manifest::parse(&manifest_bytes)?;
        let digest = integrity::sha256_hex(&manifest_bytes);
        let stored_digest = std::fs::read_to_string(commit.join("manifest.digest"))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if stored_digest != digest || digest != req.expected_manifest_digest {
            return Ok(protocol::NodeVerifyResult {
                checkpoint_id: req.checkpoint_id,
                ok: false,
                error: Some("manifest digest does not match coordinator anchor".into()),
                manifest_digest: None,
            });
        }
        let mut failed: Option<String> = None;
        for c in &parsed.components {
            let path =
                match crate::storage::safe_join(&commit, &c.storage_representation.relative_path) {
                    Ok(p) => p,
                    Err(e) => {
                        failed = Some(e.to_string());
                        break;
                    }
                };
            if let Err(e) = integrity::verify_stored_file(
                &path,
                &c.storage_representation.stored_hash,
                Some(c.storage_representation.stored_size),
            ) {
                failed = Some(format!("component '{}': {e}", c.component_id));
                break;
            }
        }
        Ok(protocol::NodeVerifyResult {
            checkpoint_id: req.checkpoint_id,
            ok: failed.is_none(),
            error: failed,
            manifest_digest: Some(digest),
        })
    }

    fn do_probe(
        &self,
        req: &protocol::NodeProbeRequest,
    ) -> FabricResult<protocol::NodeProbeResult> {
        Ok(protocol::NodeProbeResult {
            checkpoint_id: req.checkpoint_id,
            has_replica: self.storage.has_committed(&req.checkpoint_id),
        })
    }

    fn do_cleanup(
        &self,
        req: &protocol::NodeCleanupRequest,
    ) -> FabricResult<protocol::NodeCleanupResult> {
        let mut removed_paths = Vec::new();
        let mut removed_checkpoints = Vec::new();
        let mut error = None;
        for attempt in &req.staging_attempts {
            let p = self.storage.staging_dir(attempt);
            self.release_staging(&p);
            if p.exists() {
                match std::fs::remove_dir_all(&p) {
                    Ok(()) => removed_paths.push(p.to_string_lossy().to_string()),
                    Err(e) => error = Some(format!("cleanup attempt {attempt}: {e}")),
                }
            }
        }
        for sp in &req.staging_paths {
            let p = PathBuf::from(sp);
            if !self.is_under_staging(&p) {
                error = Some(format!("cleanup path outside staging: {sp}"));
                continue;
            }
            if p.exists() {
                self.release_staging(&p);
                match std::fs::remove_dir_all(&p) {
                    Ok(()) => removed_paths.push(sp.clone()),
                    Err(e) => error = Some(format!("cleanup {sp}: {e}")),
                }
            }
        }
        for target in &req.restore_attempts {
            let component_ids = self
                .pending_restores
                .lock()
                .unwrap()
                .remove(&target.attempt_id)
                .map(|(_, _, ids)| ids)
                .unwrap_or_else(|| {
                    self.providers
                        .list(&target.workload_id)
                        .into_iter()
                        .map(|p| p.component_id)
                        .collect()
                });
            let restore_staging = self.storage.staging_dir(&target.attempt_id);
            let restore_commit = self.storage.commit_dir(&target.checkpoint_id);
            let restore_ctx = RestoreContext {
                checkpoint_id: target.checkpoint_id,
                workload_id: target.workload_id,
                attempt_id: &target.attempt_id,
                staging_dir: &restore_staging,
                commit_dir: &restore_commit,
            };
            for component_id in component_ids.iter().rev() {
                if let Some(provider) = self.providers.get(&target.workload_id, component_id) {
                    if let Err(e) = provider.cleanup_restore(&restore_ctx) {
                        error = Some(format!(
                            "restore cleanup {} component {}: {e}",
                            target.attempt_id, component_id
                        ));
                    }
                }
            }
        }
        for cid in &req.checkpoint_ids {
            match self.storage.delete_committed(cid) {
                Ok(()) => removed_checkpoints.push(cid.to_string()),
                Err(e) => error = Some(format!("cleanup {cid}: {e}")),
            }
        }
        Ok(protocol::NodeCleanupResult {
            removed_paths,
            removed_checkpoints,
            error,
        })
    }

    fn is_under_staging(&self, p: &Path) -> bool {
        let staging_root = self.storage.staging_root_pub();
        let Ok(relative) = p.strip_prefix(&staging_root) else {
            return false;
        };
        let mut components = relative.components();
        if !matches!(components.next(), Some(std::path::Component::Normal(_)))
            || components.next().is_some()
        {
            return false;
        }
        if !p.exists() {
            return true;
        }
        match (staging_root.canonicalize(), p.canonicalize()) {
            (Ok(root), Ok(candidate)) => candidate.parent() == Some(root.as_path()),
            _ => false,
        }
    }

    // ================= replication / fetch =================

    fn do_replicate(
        &self,
        req: &protocol::NodeReplicateRequest,
    ) -> FabricResult<protocol::NodeReplicateResult> {
        let from_addr: SocketAddr = req
            .from_addr
            .parse()
            .map_err(|e| FabricError::InvalidArgument(format!("bad from_addr: {e}")))?;
        let mut peer = RpcClient::connect(&from_addr)?;

        let manifest_req = protocol::FetchManifestRequest {
            checkpoint_id: req.checkpoint_id,
            fetch_token: req.fetch_token.clone(),
        };
        let mresp: protocol::FetchManifestResponse =
            peer.call_json(Op::FetchManifest, &manifest_req)?;
        if !mresp.ok {
            return Ok(protocol::NodeReplicateResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: mresp.error,
                path: None,
            });
        }
        let manifest_bytes = mresp
            .manifest_bytes
            .ok_or_else(|| FabricError::ProtocolError("missing manifest bytes".into()))?;
        if integrity::sha256_hex(&manifest_bytes) != req.expected_manifest_digest {
            return Ok(protocol::NodeReplicateResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some("source manifest does not match coordinator anchor".into()),
                path: None,
            });
        }
        let manifest = crate::manifest::parse(&manifest_bytes)?;
        if manifest.checkpoint_id != req.checkpoint_id {
            return Err(FabricError::IntegrityFailure(
                "replication manifest checkpoint id mismatch".into(),
            ));
        }

        let staging = self.storage.staging_dir(&req.attempt_id);
        self.lease_staging(&staging);
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        std::fs::create_dir_all(staging.join("components"))?;
        std::fs::create_dir_all(staging.join("integrity"))?;
        std::fs::create_dir_all(staging.join("journal"))?;
        std::fs::write(staging.join("manifest"), &manifest_bytes)?;
        std::fs::write(
            staging.join("manifest.digest"),
            format!("{}\n", integrity::sha256_hex(&manifest_bytes)),
        )?;
        std::fs::write(
            staging.join("integrity/root"),
            format!("{}\n", manifest.integrity.root),
        )?;

        if mresp.components != manifest.components {
            let _ = std::fs::remove_dir_all(&staging);
            self.release_staging(&staging);
            return Ok(protocol::NodeReplicateResult {
                attempt_id: req.attempt_id.clone(),
                ok: false,
                error: Some("source component list does not match anchored manifest".into()),
                path: None,
            });
        }
        let comps = manifest.components.clone();
        for c in &comps {
            let safe_component = crate::storage::sanitize_segment(&c.component_id);
            let expected_relative = format!("components/{safe_component}");
            if safe_component != c.component_id
                || c.storage_representation.relative_path != expected_relative
            {
                let _ = std::fs::remove_dir_all(&staging);
                self.release_staging(&staging);
                return Ok(protocol::NodeReplicateResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(format!(
                        "unsafe or inconsistent component path for '{}'",
                        c.component_id
                    )),
                    path: None,
                });
            }
            let dest =
                crate::storage::safe_join(&staging, &c.storage_representation.relative_path)?;
            let mut file = std::fs::File::create(&dest)?;
            let mut offset = 0u64;
            loop {
                let creq = protocol::FetchComponentRequest {
                    checkpoint_id: req.checkpoint_id,
                    component_id: c.component_id.clone(),
                    offset,
                    fetch_token: req.fetch_token.clone(),
                };
                let chunk: protocol::FetchChunkResponse =
                    peer.call_json(Op::FetchComponent, &creq)?;
                if !chunk.ok {
                    let _ = std::fs::remove_dir_all(&staging);
                    self.release_staging(&staging);
                    return Ok(protocol::NodeReplicateResult {
                        attempt_id: req.attempt_id.clone(),
                        ok: false,
                        error: chunk.error,
                        path: None,
                    });
                }
                if chunk.offset != offset {
                    let _ = std::fs::remove_dir_all(&staging);
                    self.release_staging(&staging);
                    return Ok(protocol::NodeReplicateResult {
                        attempt_id: req.attempt_id.clone(),
                        ok: false,
                        error: Some(format!(
                            "fetch offset mismatch: expected {offset}, got {}",
                            chunk.offset
                        )),
                        path: None,
                    });
                }
                if (chunk.bytes.is_empty() && !chunk.last)
                    || offset.saturating_add(chunk.bytes.len() as u64)
                        > c.storage_representation.stored_size
                {
                    let _ = std::fs::remove_dir_all(&staging);
                    self.release_staging(&staging);
                    return Ok(protocol::NodeReplicateResult {
                        attempt_id: req.attempt_id.clone(),
                        ok: false,
                        error: Some(format!(
                            "component '{}' made no progress or exceeded declared size",
                            c.component_id
                        )),
                        path: None,
                    });
                }
                file.write_all(&chunk.bytes)?;
                offset = offset.saturating_add(chunk.bytes.len() as u64);
                if chunk.last {
                    break;
                }
            }
            if offset != c.storage_representation.stored_size {
                let _ = std::fs::remove_dir_all(&staging);
                self.release_staging(&staging);
                return Ok(protocol::NodeReplicateResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(format!(
                        "component '{}' ended at {offset} bytes, expected {}",
                        c.component_id, c.storage_representation.stored_size
                    )),
                    path: None,
                });
            }
            file.sync_all()?;
            let stored = std::fs::read(&dest)?;
            if integrity::sha256_hex(&stored) != c.storage_representation.stored_hash {
                let _ = std::fs::remove_dir_all(&staging);
                self.release_staging(&staging);
                return Ok(protocol::NodeReplicateResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(format!(
                        "fetched component '{}' failed hash check",
                        c.component_id
                    )),
                    path: None,
                });
            }
            // Sidecars: crc of stored bytes, sha256 of stored bytes.
            std::fs::write(
                format!("{}.crc32c", dest.display()),
                format!("{:08x}\n", integrity::crc32c(&stored)),
            )?;
            std::fs::write(
                format!("{}.sha256", dest.display()),
                format!("{}\n", integrity::sha256_hex(&stored)),
            )?;
            std::fs::write(
                staging
                    .join("integrity")
                    .join(format!("{}.sha256", safe_component)),
                format!("{}\n", c.content_hash),
            )?;
        }

        let commit = self.storage.commit_dir(&req.checkpoint_id);
        match self.storage.promote(&staging, &commit) {
            Ok(()) => {
                self.release_staging(&staging);
                Ok(protocol::NodeReplicateResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: true,
                    error: None,
                    path: Some(commit.to_string_lossy().to_string()),
                })
            }
            Err(e) => {
                self.release_staging(&staging);
                Ok(protocol::NodeReplicateResult {
                    attempt_id: req.attempt_id.clone(),
                    ok: false,
                    error: Some(e.to_string()),
                    path: None,
                })
            }
        }
    }

    fn serve_manifest(
        &self,
        req: &protocol::FetchManifestRequest,
    ) -> FabricResult<protocol::FetchManifestResponse> {
        self.check_token(&req.checkpoint_id, &req.fetch_token)?;
        let commit = self.storage.commit_dir(&req.checkpoint_id);
        let manifest_bytes = std::fs::read(commit.join("manifest")).map_err(|_| {
            FabricError::CorruptedCheckpoint(format!("no replica of {}", req.checkpoint_id))
        })?;
        let manifest = crate::manifest::parse(&manifest_bytes)?;
        Ok(protocol::FetchManifestResponse {
            ok: true,
            error: None,
            manifest_bytes: Some(manifest_bytes),
            components: manifest.components,
        })
    }

    fn serve_component(
        &self,
        req: &protocol::FetchComponentRequest,
    ) -> FabricResult<protocol::FetchChunkResponse> {
        self.check_token(&req.checkpoint_id, &req.fetch_token)?;
        let commit = self.storage.commit_dir(&req.checkpoint_id);
        let path = crate::storage::safe_join(
            &commit,
            &format!(
                "components/{}",
                crate::storage::sanitize_segment(&req.component_id)
            ),
        )?;
        if !path.is_file() {
            return Ok(protocol::FetchChunkResponse {
                ok: false,
                error: Some(format!("component '{}' not found", req.component_id)),
                offset: req.offset,
                bytes: Vec::new(),
                last: true,
            });
        }
        const CHUNK: u64 = 1 << 20;
        let mut file = std::fs::File::open(&path)?;
        file.seek(std::io::SeekFrom::Start(req.offset))?;
        let mut buf = vec![0u8; CHUNK as usize];
        let n = file.read(&mut buf)?;
        buf.truncate(n);
        let total = std::fs::metadata(&path)?.len();
        Ok(protocol::FetchChunkResponse {
            ok: true,
            error: None,
            offset: req.offset,
            bytes: buf,
            last: req.offset.saturating_add(n as u64) >= total,
        })
    }

    // ================= shutdown =================

    /// Graceful shutdown: stop heartbeat, close sockets, sweep staging.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(server) = &self.server {
            server.shutdown();
        }
        if let Some(handle) = self.heartbeat_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.serve_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
        self.coordinator.lock().unwrap().take();
        let keep: HashSet<PathBuf> = HashSet::new();
        let _ = self.storage.recover_partial(&keep);
        self.attached.lock().unwrap().clear();
    }
}

#[allow(clippy::too_many_arguments)]
fn heartbeat_tick(
    node_id: &str,
    boot_id: &str,
    epoch: &AtomicU64,
    coordinator: &Arc<Mutex<Option<RpcClient>>>,
    attached: &Arc<Mutex<HashMap<Id, AttachedWorkload>>>,
    resources: &serde_json::Value,
    providers: &Arc<ProviderRegistry>,
    storage: &Arc<LocalStorage>,
) -> bool {
    let mut guard = coordinator.lock().unwrap();
    let client = match guard.as_mut() {
        Some(c) => c,
        None => return false,
    };
    let workloads: Vec<protocol::HeartbeatWorkload> = attached
        .lock()
        .unwrap()
        .iter()
        .map(|(wid, w)| protocol::HeartbeatWorkload {
            workload_id: *wid,
            fence_token: w.fence_token.clone(),
            execution_epoch: w.execution_epoch,
        })
        .collect();
    let req = protocol::NodeHeartbeatRequest {
        node_id: node_id.to_string(),
        boot_id: boot_id.to_string(),
        coordinator_epoch: epoch.load(Ordering::SeqCst),
        workloads,
        resources: resources.clone(),
        committed_checkpoints: storage.enumerate_committed().unwrap_or_default(),
        provider_versions: providers.provider_versions(),
    };
    match client.call_json::<protocol::NodeHeartbeatRequest, protocol::NodeHeartbeatResponse>(
        Op::NodeHeartbeat,
        &req,
    ) {
        Ok(resp) => {
            if !resp.ok {
                // Coordinator restarted with a new epoch: re-register.
                *guard = None;
                return false;
            }
            if !resp.stale_workloads.is_empty() {
                let mut a = attached.lock().unwrap();
                for wid in &resp.stale_workloads {
                    a.remove(wid);
                    log::warn!("workload {wid} fence revoked; detached from node {node_id}");
                }
            }
            true
        }
        Err(e) => {
            log::warn!("heartbeat failed: {e}");
            *guard = None;
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::{Coordinator, CoordinatorConfig, OperationContext};
    use crate::lifecycle::LifecycleState;
    use crate::restore::RestoreOptions;
    use crate::workload::{ProtectionSpec, WorkloadSpec};

    pub(crate) fn test_harness() -> (
        tempfile::TempDir,
        tempfile::TempDir,
        tempfile::TempDir,
        Arc<crate::coordinator::Coordinator>,
        Arc<NodeRuntime>,
    ) {
        let coord_dir = tempfile::tempdir().unwrap();
        let node_dir = tempfile::tempdir().unwrap();
        let extra = tempfile::tempdir().unwrap();
        let cfg = CoordinatorConfig {
            data_dir: coord_dir.path().to_path_buf(),
            listen: Some("127.0.0.1:0".parse().unwrap()),
            epoch: Some(1),
            stale_ms: 60_000,
            policy: None,
            failpoints: None,
        };
        let coord = Coordinator::open(cfg).unwrap().start_server().unwrap();
        let node_cfg = NodeConfig::default_in(
            "n1",
            coord.listen_addr().unwrap(),
            node_dir.path().to_path_buf(),
        );
        let node = NodeRuntime::start(node_cfg).unwrap();
        (coord_dir, node_dir, extra, coord, node)
    }

    fn spec() -> WorkloadSpec {
        WorkloadSpec {
            workload_id: None,
            owner: "t".into(),
            class: "test".into(),
            backend_class: "cpu".into(),
            state_schema_version: 1,
            runtime: RuntimeCompatibilityDescriptor::local_default(),
            metadata: serde_json::json!({}),
            protection: ProtectionSpec::default(),
            single_active: true,
        }
    }

    #[test]
    fn node_registers_and_heartbeats() {
        let (_d1, _d2, _d3, coord, node) = test_harness();
        let nodes = coord.list_nodes().unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, node.node_id);
        assert_eq!(nodes[0].status, "ACTIVE");
        // heartbeat within timeout
        std::thread::sleep(std::time::Duration::from_millis(1500));
        let nodes = coord.list_nodes().unwrap();
        assert_eq!(nodes[0].status, "ACTIVE");
    }

    #[test]
    fn attach_then_capture_then_restore() {
        use crate::providers::{ApplicationStateProvider, ProviderSpec};
        let (_d1, node_dir, _d3, coord, node) = test_harness();
        let ctx = OperationContext {
            actor: "test".into(),
            roles: vec!["owner".into(), "operator".into()],
            stale_ms: 60_000,
        };
        let mut w = coord
            .create_workload(&spec(), Some(&node.node_id), &ctx)
            .unwrap();
        let state = std::sync::Arc::new(std::sync::Mutex::new(vec![0u8; 0]));
        let provider = ApplicationStateProvider::new(
            ProviderSpec {
                component_id: "app".into(),
                component_type: crate::checkpoint::ComponentType::ApplicationState,
                required: true,
                schema_version: 1,
                restore_handler: "application".into(),
                compatibility: serde_json::json!({}),
                dependencies: Vec::new(),
            },
            || Ok(b"state-v1".to_vec()),
            {
                let state = state.clone();
                move |bytes| {
                    *state.lock().unwrap() = bytes.to_vec();
                    Ok(())
                }
            },
        );
        // The coordinator's claim on create already fenced with node; attach
        // again would fail single-active fencing. Reset the workload first.
        coord.fence_workload(&w.workload_id, &ctx).unwrap();
        w = coord.inspect_workload(&w.workload_id).unwrap().unwrap();
        node.attach_workload(&w.workload_id, w.execution_epoch, vec![Arc::new(provider)])
            .unwrap();
        assert!(node.is_attached(&w.workload_id));

        let opts = crate::capture::CaptureOptions {
            components: vec![crate::capture::CaptureComponentRequest {
                component_id: "app".into(),
                component_type: crate::checkpoint::ComponentType::ApplicationState,
                required: true,
                schema_version: 1,
                restore_handler: "application".into(),
            }],
            ..Default::default()
        };
        let out = coord.request_capture(&w.workload_id, &opts, &ctx).unwrap();
        assert_eq!(out.state, "AVAILABLE");
        let ckpt_id = out.checkpoint_id.unwrap();
        let ckpt = coord.checkpoint_get(&ckpt_id).unwrap().unwrap();
        assert_eq!(ckpt.components.len(), 1);
        assert_eq!(ckpt.lifecycle, LifecycleState::Available);
        assert_eq!(ckpt.total_logical_bytes, 8);
        assert!(ckpt.manifest_digest.is_some());
        assert!(ckpt.manifest_json.is_some());

        // Raw restore options cannot bypass the operator-only rollback and
        // migration authorities.
        let owner_only = OperationContext {
            actor: "owner".into(),
            roles: vec!["owner".into()],
            stale_ms: 60_000,
        };
        assert!(coord
            .request_restore(
                &ckpt_id,
                &node.node_id,
                &RestoreOptions {
                    rollback: true,
                    ..RestoreOptions::default()
                },
                &owner_only,
            )
            .is_err());
        assert!(coord
            .request_restore(
                &ckpt_id,
                &node.node_id,
                &RestoreOptions {
                    migration: true,
                    ..RestoreOptions::default()
                },
                &owner_only,
            )
            .is_err());

        let workload_count = coord.list_workloads().unwrap().len();
        let child = coord.fork(&ckpt_id, &spec(), &ctx).unwrap();
        assert_eq!(child.parent_workload, Some(w.workload_id));
        assert_eq!(coord.list_workloads().unwrap().len(), workload_count + 1);

        // verify integrity via node
        let verify = coord.verify_checkpoint(&ckpt_id, &ctx).unwrap();
        assert!(verify.ok);

        // restore onto the same node
        let res = coord
            .request_restore(&ckpt_id, &node.node_id, &RestoreOptions::default(), &ctx)
            .unwrap();
        assert_eq!(res.state, "RESTORED");
        assert_eq!(*state.lock().unwrap(), b"state-v1");
        std::thread::sleep(std::time::Duration::from_millis(1200));
        assert!(node.is_attached(&w.workload_id));

        // A node restart gets a new transient id, but committed replicas on
        // the same data directory are advertised and rebound synchronously.
        node.shutdown();
        let restarted = NodeRuntime::start(NodeConfig::default_in(
            "n1-restarted",
            coord.listen_addr().unwrap(),
            node_dir.path().to_path_buf(),
        ))
        .unwrap();
        let rebound = coord.checkpoint_get(&ckpt_id).unwrap().unwrap();
        assert!(rebound
            .durable_locations
            .iter()
            .any(|location| location.node == restarted.node_id));
        assert!(coord.verify_checkpoint(&ckpt_id, &ctx).unwrap().ok);
        restarted.shutdown();
    }

    #[test]
    fn cleanup_rejects_lexical_escape_and_attempt_segments_stay_beneath_root() {
        let (_d1, node_dir, _d3, _coord, node) = test_harness();
        let parent = node_dir.path().parent().unwrap();
        let victim = tempfile::Builder::new()
            .prefix("cf-cleanup-victim-")
            .tempdir_in(parent)
            .unwrap();
        std::fs::write(victim.path().join("keep"), b"keep").unwrap();
        let victim_name = victim.path().file_name().unwrap();
        let escape = node
            .storage
            .staging_root_pub()
            .join("..")
            .join("..")
            .join(victim_name);
        let result = node
            .do_cleanup(&protocol::NodeCleanupRequest {
                staging_attempts: Vec::new(),
                staging_paths: vec![escape.to_string_lossy().to_string()],
                restore_attempts: Vec::new(),
                checkpoint_ids: Vec::new(),
                coordinator_epoch: node.coordinator_epoch.load(Ordering::SeqCst),
            })
            .unwrap();
        assert!(result.error.is_some());
        assert!(victim.path().join("keep").exists());

        let escaped_attempt = node.storage.staging_dir("..");
        assert_eq!(
            escaped_attempt.parent(),
            Some(node.storage.staging_root_pub().as_path())
        );
        assert_ne!(escaped_attempt, node.storage.staging_root_pub().join(".."));
    }
}
