//! Coordinator: deterministic authority and coordination for the fabric.
//!
//! The coordinator owns the durable store, the coordinator epoch, workload and
//! checkpoint generations, reservations, capture/restore attempts, lineage,
//! fencing, and audit. No two actors can independently commit the same checkpoint
//! generation; stale epochs and attempts are rejected; all commits are
//! idempotent and journaled for crash recovery.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use serde::{Deserialize, Serialize};

use crate::audit::AuditResult;
use crate::capture::{self, AttemptRecord, CaptureOptions, QuiescenceMode};
use crate::checkpoint::{
    CheckpointObject, ConsistencyClass, IntegrityState, ProtectionState, RetirementEligibility,
};
use crate::compatibility::{self, CompatVerdict, RuntimeCompatibilityDescriptor};
use crate::errors::{FabricError, FabricResult};
use crate::failpoints;
use crate::id::Id;
use crate::lifecycle::LifecycleState;
use crate::lineage::LineageRecord;
use crate::migration;
use crate::persistence::Store;
use crate::policy::{Authority, PolicySet};
use crate::protocol::{self, Envelope, Op};
use crate::recovery::{self, capture_states, restore_states, RecoveryOutcome};
use crate::restore::{self, RestoreOptions};
use crate::time::now_ms;
use crate::transport::{RequestHandler, RpcClient, Server};
use crate::workload::{Workload, WorkloadSpec};

/// Default RPC timeout for node operations.
pub const NODE_OP_TIMEOUT_MS: u64 = 300_000;
/// Default RPC timeout for quick coordinator operations.
pub const QUICK_OP_TIMEOUT_MS: u64 = 15_000;
/// Default reservation TTL.
pub const RESERVATION_TTL_MS: u64 = 600_000;
/// Default node staleness.
pub const DEFAULT_STALE_MS: u64 = 6_000;

/// Configuration for the coordinator.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    pub data_dir: PathBuf,
    /// Listen address for the RPC server (None = embedded core only).
    pub listen: Option<SocketAddr>,
    /// Explicit coordinator epoch override (default: stored epoch + 1).
    pub epoch: Option<u64>,
    /// Milliseconds after which a node with no heartbeat is stale.
    pub stale_ms: u64,
    /// Initial policy (default: PolicySet::default()).
    pub policy: Option<PolicySet>,
    /// Failpoint spec applied at startup.
    pub failpoints: Option<String>,
}

impl CoordinatorConfig {
    pub fn default_in(dir: PathBuf) -> Self {
        Self {
            data_dir: dir,
            listen: None,
            epoch: None,
            stale_ms: DEFAULT_STALE_MS,
            policy: None,
            failpoints: None,
        }
    }
}

/// Actor context for an operation.
#[derive(Debug, Clone)]
pub struct OperationContext {
    pub actor: String,
    pub roles: Vec<String>,
    pub stale_ms: u64,
}

impl OperationContext {
    pub fn for_authority(a: &Authority, stale_ms: u64) -> Self {
        Self {
            actor: a.actor.clone(),
            roles: a.roles.clone(),
            stale_ms,
        }
    }
}

/// Coordinator statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Stats {
    pub workloads: u64,
    pub checkpoints: u64,
    pub available: u64,
    pub failed: u64,
    pub retired: u64,
    pub active_nodes: u64,
    pub stale_nodes: u64,
    pub active_attempts: u64,
    pub audit_records: u64,
    pub total_logical_bytes: u64,
    pub total_physical_bytes: u64,
}

/// A capture or restore outcome reported to a caller.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationOutcome {
    pub attempt_id: String,
    pub checkpoint_id: Option<Id>,
    pub workload_id: Option<Id>,
    pub node: Option<String>,
    pub state: String,
    pub result: Option<String>,
    pub error: Option<String>,
    pub resumability: Option<crate::checkpoint::ResumabilityClass>,
    pub workload_generation: Option<u64>,
    pub execution_epoch: Option<u64>,
}

/// The coordinator runtime. Wraps an [`Arc`] of the inner state so the RPC
/// handler can share it via a `Weak` reference without reference cycles.
pub struct Coordinator {
    inner: Arc<CoordinatorInner>,
}

/// Shared coordinator state.
pub struct CoordinatorInner {
    pub store: Store,
    pub epoch: u64,
    pub policy: Arc<PolicySet>,
    pub config: CoordinatorConfig,
    /// The RPC server (Arc so the accept loop needs no lock; `shutdown` and
    /// `listen_addr` never block on it).
    server: Option<Arc<Server>>,
    /// The bound listen address (set once at start).
    listen: Option<SocketAddr>,
    pub stop: Arc<AtomicBool>,
    node_clients: Arc<Mutex<HashMap<String, RpcClient>>>,
    monitor_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    serve_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl std::ops::Deref for Coordinator {
    type Target = CoordinatorInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Coordinator {
    /// Open the coordinator core (embedded mode: no listener, no monitor).
    pub fn open(config: CoordinatorConfig) -> FabricResult<Self> {
        if let Some(fp) = &config.failpoints {
            failpoints::arm_from_spec(fp);
        }
        let store = Store::open(&config.data_dir)?;
        let epoch = store.claim_coordinator_epoch(config.epoch)?;

        let policy = match &config.policy {
            Some(p) => {
                store.policy_insert(p, true)?;
                p.clone()
            }
            None => {
                let versions = store.policy_versions()?;
                if versions.is_empty() {
                    let p = PolicySet::default();
                    store.policy_insert(&p, true)?;
                    p
                } else {
                    let mut loaded = store.policy_load_all()?;
                    loaded.sort_by_key(|p| p.version);
                    loaded.pop().unwrap_or_default()
                }
            }
        };

        Ok(Self {
            inner: Arc::new(CoordinatorInner {
                store,
                epoch,
                policy: Arc::new(policy),
                config,
                server: None,
                listen: None,
                stop: Arc::new(AtomicBool::new(false)),
                node_clients: Arc::new(Mutex::new(HashMap::new())),
                monitor_thread: Mutex::new(None),
                serve_thread: Mutex::new(None),
            }),
        })
    }

    /// Start the RPC server and the stale-node monitor. Returns the coordinator
    /// behind an `Arc` so the RPC handler may share it without cycles.
    pub fn start_server(self) -> FabricResult<Arc<Self>> {
        let addr = self
            .config
            .listen
            .ok_or_else(|| FabricError::InvalidArgument("no listen address configured".into()))?;
        let listener = std::net::TcpListener::bind(addr)
            .map_err(|e| FabricError::TransportError(format!("bind {addr}: {e}")))?;
        let listen = listener.local_addr().ok();
        let inner: Arc<CoordinatorInner> = Arc::new_cyclic(|weak| {
            let w: Weak<CoordinatorInner> = weak.clone();
            let handler: RequestHandler = Arc::new(move |_conn, op, _req_id, payload| {
                let c = w
                    .upgrade()
                    .ok_or_else(|| FabricError::Internal("coordinator has shut down".into()))?;
                let coord = Coordinator { inner: c };
                Self::dispatch(&coord, op, payload)
            });
            let server = Arc::new(Server::from_parts(listener, handler));
            CoordinatorInner {
                store: self.store.clone(),
                epoch: self.epoch,
                policy: self.policy.clone(),
                config: self.config.clone(),
                server: Some(server),
                listen,
                stop: self.stop.clone(),
                node_clients: self.node_clients.clone(),
                monitor_thread: Mutex::new(None),
                serve_thread: Mutex::new(None),
            }
        });
        let coordinator = Arc::new(Self {
            inner: inner.clone(),
        });
        Self::start_monitor(&inner);
        let serve_self = coordinator.clone();
        let handle = std::thread::spawn(move || {
            if let Some(srv) = serve_self.inner.server.as_ref() {
                if let Err(e) = srv.serve() {
                    log::error!("coordinator server ended: {e}");
                }
            }
        });
        *coordinator.inner.serve_thread.lock().unwrap() = Some(handle);
        Ok(coordinator)
    }

    fn start_monitor(inner: &CoordinatorInner) {
        let store = inner.store.clone();
        let stale_ms = inner.config.stale_ms;
        let stop = inner.stop.clone();
        let nodes = inner.node_clients.clone();
        let epoch = inner.epoch;
        let handle = std::thread::spawn(move || {
            let mut tick: u64 = 0;
            while !stop.load(Ordering::SeqCst) {
                std::thread::sleep(std::time::Duration::from_millis(250));
                tick += 1;
                if tick % 8 != 0 {
                    continue;
                }
                if let Err(e) = sweep_stale(&store, &nodes, stale_ms, epoch) {
                    log::warn!("stale sweep failed: {e}");
                }
                let _ = store.reservations_expire(now_ms());
            }
        });
        *inner.monitor_thread.lock().unwrap() = Some(handle);
    }

    /// Stop the server and monitor; join bounded.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(server) = &self.inner.server {
            server.shutdown();
        }
        if let Some(handle) = self.monitor_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.serve_thread.lock().unwrap().take() {
            let _ = handle.join();
        }
        self.node_clients.lock().unwrap().clear();
    }

    pub fn listen_addr(&self) -> Option<SocketAddr> {
        self.inner.listen
    }

    /// Shared stop flag (for graceful shutdown drivers).
    pub fn stop_flag(&self) -> Arc<AtomicBool> {
        self.stop.clone()
    }

    /// Request a graceful stop (as if a shutdown signal arrived).
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    pub fn is_running(&self) -> bool {
        !self.stop.load(Ordering::SeqCst)
    }

    /// Whether a node is currently ACTIVE.
    pub fn node_is_active(&self, node: &str) -> FabricResult<bool> {
        Ok(self
            .store
            .node_list()?
            .iter()
            .any(|n| n.id == node && n.status == "ACTIVE"))
    }

    /// Runtime descriptor of a registered node.
    pub fn node_runtime(&self, node: &str) -> FabricResult<RuntimeCompatibilityDescriptor> {
        for n in self.store.node_list()? {
            if n.id == node {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&n.resources) {
                    if let Some(rt) = v.get("runtime") {
                        return serde_json::from_value(rt.clone())
                            .map_err(|e| FabricError::Internal(e.to_string()));
                    }
                }
                return Err(FabricError::Internal(format!(
                    "node {node} has no runtime descriptor"
                )));
            }
        }
        Err(FabricError::Internal(format!("node {node} not registered")))
    }

    /// Merge provider handler versions into a node's stored runtime descriptor.
    pub fn merge_node_provider_versions(
        &self,
        node: &str,
        versions: &std::collections::BTreeMap<String, String>,
    ) -> FabricResult<()> {
        let nodes = self.store.node_list()?;
        let n = nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or_else(|| FabricError::Internal(format!("node {node} not registered")))?;
        let mut resources: serde_json::Value =
            serde_json::from_str(&n.resources).map_err(|e| FabricError::Internal(e.to_string()))?;
        let mut merged = versions.clone();
        if let Some(rt) = resources.get_mut("runtime") {
            if let Some(pv) = rt.get("provider_versions").and_then(|v| v.as_object()) {
                for (k, v) in pv {
                    merged
                        .entry(k.clone())
                        .or_insert_with(|| v.as_str().unwrap_or_default().to_string());
                }
            }
            if let Some(obj) = rt.as_object_mut() {
                obj.insert("provider_versions".into(), serde_json::to_value(merged)?);
            }
        }
        self.store
            .node_heartbeat(node, &serde_json::to_string(&resources)?)
    }

    fn audit(
        &self,
        ctx: &OperationContext,
        op: &str,
        workload_id: Option<&Id>,
        checkpoint_id: Option<&Id>,
        result: AuditResult,
        detail: &str,
    ) {
        if let Err(e) =
            self.store
                .audit_append(&ctx.actor, op, workload_id, checkpoint_id, result, detail)
        {
            log::warn!("audit append failed for {op}: {e}");
        }
    }

    // ================= workload operations =================

    pub fn create_workload(
        &self,
        spec: &WorkloadSpec,
        claim_node: Option<&str>,
        ctx: &OperationContext,
    ) -> FabricResult<Workload> {
        if let Some(node) = claim_node {
            if !self.node_is_active(node)? {
                return Err(FabricError::FencingFailure(format!(
                    "cannot claim new workload for inactive node {node}"
                )));
            }
        }
        let workload_id = spec.workload_id.unwrap_or_else(Id::random);
        if self.store.workload_get(&workload_id)?.is_some() {
            return Err(FabricError::InvalidArgument(format!(
                "workload {workload_id} already exists"
            )));
        }
        let w = Workload {
            workload_id,
            workload_generation: 0,
            owner: spec.owner.clone(),
            class: spec.class.clone(),
            created_ms: now_ms(),
            execution_epoch: 0,
            active_node: claim_node.map(|s| s.to_string()),
            backend_class: spec.backend_class.clone(),
            checkpoint_generation: 0,
            parent_workload: None,
            fork_generation: 0,
            policy_version: self.policy.version,
            metadata: spec.metadata.clone(),
            state_schema_version: spec.state_schema_version,
            runtime: spec.runtime.clone(),
            resumability_class: crate::checkpoint::ResumabilityClass::RestartFromCheckpoint,
            protection: spec.protection.clone(),
            single_active: spec.single_active,
            fence_token: if claim_node.is_some() {
                Some(migration::new_fence_token())
            } else {
                None
            },
            fence_epoch: 0,
        };
        self.store.workload_insert(&w)?;
        self.audit(
            ctx,
            "workload.create",
            Some(&workload_id),
            None,
            AuditResult::Ok,
            &format!("class={} backend={}", w.class, w.backend_class),
        );
        Ok(w)
    }

    pub fn inspect_workload(&self, id: &Id) -> FabricResult<Option<Workload>> {
        self.store.workload_get(id)
    }

    pub fn list_workloads(&self) -> FabricResult<Vec<Workload>> {
        self.store.workload_list()
    }

    pub fn fence_workload(&self, id: &Id, ctx: &OperationContext) -> FabricResult<Workload> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.fence)?;
        let w = self
            .store
            .workload_get(id)?
            .ok_or_else(|| FabricError::WorkloadNotFound(id.to_string()))?;
        self.store
            .workload_bump_fence(id, w.execution_epoch)
            .map_err(|_| FabricError::FencingFailure("workload fence race".into()))?;
        self.store
            .workload_release(id, w.active_node.as_deref().unwrap_or_default())?;
        self.audit(
            ctx,
            "workload.fence",
            Some(id),
            None,
            AuditResult::Ok,
            &format!("epoch bumped to {}", w.execution_epoch + 1),
        );
        Ok(self.store.workload_get(id)?.unwrap())
    }

    pub fn workload_lineage(&self, id: &Id) -> FabricResult<Vec<LineageRecord>> {
        self.store.lineage_query(Some(id), None)
    }

    /// Node-side claim: validate fencing and record the attachment.
    pub fn attach_workload(
        &self,
        workload_id: &Id,
        node: &str,
        node_boot_id: &str,
        fence_epoch: u64,
    ) -> FabricResult<String> {
        self.store.node_validate_identity(node, node_boot_id)?;
        let token = migration::new_fence_token();
        self.store
            .workload_claim(workload_id, node, &token, fence_epoch)
            .map_err(|e| match e {
                FabricError::FencingFailure(s) => {
                    FabricError::FencingFailure(format!("attach rejected: {s}"))
                }
                other => other,
            })?;
        Ok(token)
    }

    pub fn detach_workload(&self, workload_id: &Id, node: &str) -> FabricResult<()> {
        self.store.workload_release(workload_id, node)?;
        Ok(())
    }

    // ================= capture =================

    pub fn request_capture(
        &self,
        workload_id: &Id,
        options: &CaptureOptions,
        ctx: &OperationContext,
    ) -> FabricResult<OperationOutcome> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.capture)?;
        failpoints::fire("capture.before_validation")?;
        let workload = self
            .store
            .workload_get(workload_id)?
            .ok_or_else(|| FabricError::WorkloadNotFound(workload_id.to_string()))?;

        let last_seal = self
            .store
            .checkpoint_list(Some(workload_id))?
            .iter()
            .filter(|c| c.lifecycle == LifecycleState::Available)
            .map(|c| c.seal_ms)
            .max();
        capture::validate_capture_request(&workload, options, &self.policy, last_seal, now_ms())?;
        capture::workload_captureable(&workload, workload.active_node.as_deref())?;

        let node = self.pick_capture_node(&workload)?;
        let attempt_id = format!("capture-{}-{}", workload_id.to_hex(), Id::random().to_hex());
        let checkpoint_generation = self
            .store
            .checkpoint_max_generation(workload_id)?
            .saturating_add(1);
        let checkpoint_id = Id::random();

        self.store.reservation_create(
            &attempt_id,
            "capture",
            Some(workload_id),
            None,
            &node,
            RESERVATION_TTL_MS,
        )?;
        self.store.attempt_begin(
            &attempt_id,
            "capture",
            Some(&checkpoint_id),
            Some(workload_id),
            &node,
        )?;
        self.store.journal_append(
            "capture",
            &attempt_id,
            capture_states::RESERVED,
            "reservation committed",
        )?;

        let consistency = options
            .consistency
            .unwrap_or(ConsistencyClass::CrashConsistent);

        self.execute_capture(
            &workload,
            &node,
            &attempt_id,
            checkpoint_id,
            checkpoint_generation,
            consistency,
            options,
            ctx,
        )
        .map_err(|e| {
            let detail = format!("capture failed: {e}");
            let _ = self.abort_capture_attempt(
                &attempt_id,
                Some(&checkpoint_id),
                workload_id,
                &detail,
                ctx,
            );
            e
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_capture(
        &self,
        workload: &Workload,
        node: &str,
        attempt_id: &str,
        checkpoint_id: Id,
        checkpoint_generation: u64,
        requested_consistency: ConsistencyClass,
        options: &CaptureOptions,
        ctx: &OperationContext,
    ) -> FabricResult<OperationOutcome> {
        let mut ckpt = capture::checkpoint_scaffold(
            workload,
            checkpoint_id,
            checkpoint_generation,
            attempt_id,
            self.epoch,
            node,
            options,
            &self.policy,
        );
        self.store.checkpoint_insert(&ckpt)?;
        self.set_lifecycle(&ckpt, LifecycleState::Capturing)?;

        let req = protocol::NodeCaptureRequest {
            attempt_id: attempt_id.to_string(),
            checkpoint_id,
            workload_id: workload.workload_id,
            checkpoint_generation,
            consistency: requested_consistency,
            quiescence: options.quiescence,
            components: options.components.clone(),
            compression: self.policy.compression_spec(),
            coordinator_epoch: self.epoch,
        };
        let node_result: protocol::NodeCaptureResult =
            self.call_node(node, Op::NodeCaptureRequest, &req, NODE_OP_TIMEOUT_MS)?;
        if !node_result.ok {
            return Err(FabricError::CaptureProviderFailure(
                node_result
                    .error
                    .unwrap_or_else(|| "node capture failed".into()),
            ));
        }

        capture::validate_component_completeness(&options.components, &node_result.components)?;

        let frontier = options.frontier.clone();
        let achieved = capture::derive_consistency(
            Some(requested_consistency),
            options.quiescence,
            frontier.as_ref(),
            node_result.cooperative_ack,
        );
        crate::policy::enforce_consistency(&self.policy, achieved)?;
        let resumability = capture::derive_resumability(
            achieved,
            frontier.as_ref(),
            options.side_effects.as_ref(),
        );

        ckpt.components = node_result.components.clone();
        ckpt.consistency = achieved;
        ckpt.resumability = resumability;
        ckpt.total_logical_bytes = node_result.total_logical_bytes;
        ckpt.total_physical_bytes = node_result.total_physical_bytes;
        ckpt.compressed_bytes = node_result.compressed_bytes;
        let latest_gen = workload.checkpoint_generation.saturating_add(1);
        ckpt.protection = if checkpoint_generation
            >= latest_gen.saturating_sub(self.policy.protected_generations as u64)
        {
            ProtectionState::Protected
        } else {
            ProtectionState::None
        };
        self.set_lifecycle(&ckpt, LifecycleState::Captured)?;
        self.set_lifecycle(&ckpt, LifecycleState::Validating)?;

        failpoints::fire("capture.before_seal")?;
        let mut manifest = crate::manifest::scaffold(&ckpt);
        manifest.seal_ms = now_ms();
        ckpt.seal_ms = manifest.seal_ms;
        let sealed = crate::manifest::seal(manifest);
        ckpt.manifest_digest = Some(sealed.digest.clone());
        ckpt.manifest_json = Some(String::from_utf8_lossy(&sealed.canonical_bytes).into_owned());
        self.store.checkpoint_set_digest(
            &checkpoint_id,
            &sealed.digest,
            &String::from_utf8_lossy(&sealed.canonical_bytes),
        )?;
        self.set_lifecycle(&ckpt, LifecycleState::Sealed)?;
        self.store.journal_append(
            "capture",
            attempt_id,
            capture_states::SEALED,
            "manifest sealed",
        )?;
        self.store
            .checkpoint_set_integrity(&checkpoint_id, IntegrityState::Valid)?;

        self.set_lifecycle(&ckpt, LifecycleState::Persisting)?;
        let promote_req = protocol::NodePromoteRequest {
            attempt_id: attempt_id.to_string(),
            checkpoint_id,
            manifest_bytes: sealed.canonical_bytes,
            digest: sealed.digest.clone(),
            integrity_root: sealed.integrity_root,
            coordinator_epoch: self.epoch,
        };
        let promote_res: protocol::NodePromoteResult = self.call_node(
            node,
            Op::NodePromoteRequest,
            &promote_req,
            NODE_OP_TIMEOUT_MS,
        )?;
        if !promote_res.ok {
            return Err(FabricError::PersistenceError(
                promote_res.error.unwrap_or_else(|| "promote failed".into()),
            ));
        }
        ckpt.durable_locations = vec![crate::checkpoint::DurableLocation {
            node: node.to_string(),
            path: promote_res.commit_path.clone().unwrap_or_default(),
            verified: true,
        }];
        ckpt.replica_count = 1;
        ckpt.integrity_state = IntegrityState::Pending;
        ckpt.lifecycle = LifecycleState::Persisting;
        ckpt.retirement_eligibility = match ckpt.protection {
            ProtectionState::None => RetirementEligibility::Eligible,
            _ => RetirementEligibility::Protected,
        };
        // PERSISTED is journaled only after the full recoverable metadata image
        // (including components, location, and manifest anchor) is durable.
        self.store.checkpoint_update(&ckpt)?;
        self.store.journal_append(
            "capture",
            attempt_id,
            capture_states::PERSISTED,
            "checkpoint promoted on node",
        )?;

        failpoints::fire("capture.before_db_commit")?;
        self.store.capture_commit(&checkpoint_id, attempt_id)?;

        let resume_req = protocol::NodeResumeRequest {
            attempt_id: attempt_id.to_string(),
            checkpoint_id,
            workload_id: workload.workload_id,
            fence_token: None,
            execution_epoch: None,
            resume: true,
            coordinator_epoch: self.epoch,
        };
        let resume_res: protocol::NodeResumeResult =
            self.call_node(node, Op::NodeResumeRequest, &resume_req, NODE_OP_TIMEOUT_MS)?;
        if !resume_res.ok {
            let detail = format!(
                "checkpoint committed but source resume failed: {}",
                resume_res.error.unwrap_or_default()
            );
            self.store.attempt_finish(
                attempt_id,
                "RESUME_FAILED",
                Some("checkpoint committed"),
                Some(&detail),
            )?;
            self.audit(
                ctx,
                "capture.resume_failed",
                Some(&workload.workload_id),
                Some(&checkpoint_id),
                AuditResult::Failed,
                &detail,
            );
            return Err(FabricError::CaptureProviderFailure(detail));
        }
        self.store.journal_append(
            "capture",
            attempt_id,
            capture_states::RESUME_DONE,
            "source resumed",
        )?;

        let replicas = self.replicate_to_policy(&checkpoint_id, ctx)?;
        self.store
            .attempt_finish(attempt_id, "COMMITTED", Some("capture committed"), None)?;
        self.store.reservation_release(attempt_id)?;
        self.audit(
            ctx,
            "capture.commit",
            Some(&workload.workload_id),
            Some(&checkpoint_id),
            AuditResult::Ok,
            &format!(
                "generation {checkpoint_generation} on node {node}: {achieved:?} {resumability:?} replicas={replicas}"
            ),
        );
        failpoints::fire("capture.after_commit")?;

        Ok(OperationOutcome {
            attempt_id: attempt_id.to_string(),
            checkpoint_id: Some(checkpoint_id),
            workload_id: Some(workload.workload_id),
            node: Some(node.to_string()),
            state: "AVAILABLE".into(),
            result: Some("capture committed".into()),
            error: None,
            resumability: Some(resumability),
            workload_generation: Some(workload.workload_generation),
            execution_epoch: Some(workload.execution_epoch),
        })
    }

    fn abort_capture_attempt(
        &self,
        attempt_id: &str,
        checkpoint_id: Option<&Id>,
        workload_id: &Id,
        detail: &str,
        ctx: &OperationContext,
    ) -> FabricResult<()> {
        let committed =
            self.store
                .journal_has("capture", attempt_id, capture_states::DB_COMMITTED)?;
        if let Some(attempt) = self.store.attempt_get(attempt_id)? {
            let resume = protocol::NodeResumeRequest {
                attempt_id: attempt_id.to_string(),
                checkpoint_id: checkpoint_id
                    .copied()
                    .unwrap_or_else(|| Id::from_bytes([0; 16])),
                workload_id: *workload_id,
                fence_token: None,
                execution_epoch: None,
                resume: true,
                coordinator_epoch: self.epoch,
            };
            let _ = self.call_node::<_, protocol::NodeResumeResult>(
                &attempt.node,
                Op::NodeResumeRequest,
                &resume,
                QUICK_OP_TIMEOUT_MS,
            );
            let cleanup = protocol::NodeCleanupRequest {
                staging_attempts: vec![attempt_id.to_string()],
                staging_paths: Vec::new(),
                restore_attempts: Vec::new(),
                checkpoint_ids: if committed {
                    Vec::new()
                } else {
                    checkpoint_id.copied().into_iter().collect()
                },
                coordinator_epoch: self.epoch,
            };
            let _ = self.call_node::<_, protocol::NodeCleanupResult>(
                &attempt.node,
                Op::NodeCleanupRequest,
                &cleanup,
                QUICK_OP_TIMEOUT_MS,
            );
        }
        if let Some(cid) = checkpoint_id {
            if let Some(ck) = self.store.checkpoint_get(cid)? {
                if !committed
                    && ck.lifecycle != LifecycleState::Available
                    && ck.lifecycle != LifecycleState::Restored
                    && ck.lifecycle != LifecycleState::Retired
                {
                    self.store.checkpoint_set_lifecycle(
                        cid,
                        LifecycleState::Failed,
                        IntegrityState::Pending,
                    )?;
                }
            }
        }
        self.store.attempt_finish(
            attempt_id,
            if committed { "COMMITTED" } else { "FAILED" },
            committed.then_some("capture committed before later failure"),
            (!committed).then_some(detail),
        )?;
        self.store.reservation_release(attempt_id)?;
        self.audit(
            ctx,
            "capture.aborted",
            Some(workload_id),
            checkpoint_id,
            AuditResult::Failed,
            detail,
        );
        Ok(())
    }

    fn pick_capture_node(&self, workload: &Workload) -> FabricResult<String> {
        if let Some(node) = &workload.active_node {
            if self.node_is_active(node)? {
                return Ok(node.clone());
            }
        }
        let active: Vec<String> = self
            .store
            .node_list()?
            .iter()
            .filter(|n| n.status == "ACTIVE")
            .map(|n| n.id.clone())
            .collect();
        active
            .into_iter()
            .next()
            .ok_or_else(|| FabricError::Internal("no active nodes available for capture".into()))
    }

    fn set_lifecycle(&self, ckpt: &CheckpointObject, target: LifecycleState) -> FabricResult<()> {
        // The authoritative current lifecycle is the durable one, not the
        // in-memory scaffold (which lags behind earlier transitions).
        let cur = self.store.checkpoint_get(&ckpt.checkpoint_id)?;
        let current = cur.as_ref().map(|c| c.lifecycle).unwrap_or(ckpt.lifecycle);
        current.transition(target)?;
        self.store
            .checkpoint_set_lifecycle(&ckpt.checkpoint_id, target, IntegrityState::Pending)
    }

    fn replicate_to_policy(&self, checkpoint_id: &Id, ctx: &OperationContext) -> FabricResult<u32> {
        if self.policy.min_valid_replicas <= 1 {
            return Ok(1);
        }
        let mut ck = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        let have: Vec<String> = ck
            .durable_locations
            .iter()
            .map(|l| l.node.clone())
            .collect();
        let targets: Vec<String> = self
            .store
            .node_list()?
            .iter()
            .filter(|n| n.status == "ACTIVE" && !have.contains(&n.id))
            .map(|n| n.id.clone())
            .collect();
        for target in targets {
            if ck.replica_count >= self.policy.min_valid_replicas {
                break;
            }
            let source = ck.durable_locations[0].node.clone();
            let token = Id::random().to_hex();
            let allow = protocol::NodeAllowFetchRequest {
                checkpoint_id: *checkpoint_id,
                fetch_token: token.clone(),
                ttl_ms: 300_000,
                coordinator_epoch: self.epoch,
            };
            self.call_node::<protocol::NodeAllowFetchRequest, protocol::Envelope>(
                &source,
                Op::NodeAllowFetch,
                &allow,
                QUICK_OP_TIMEOUT_MS,
            )?;
            let from_addr = self.node_addr(&source)?.to_string();
            let req = protocol::NodeReplicateRequest {
                attempt_id: format!(
                    "replicate-{}-{}",
                    checkpoint_id.to_hex(),
                    Id::random().to_hex()
                ),
                checkpoint_id: *checkpoint_id,
                expected_manifest_digest: ck.manifest_digest.clone().ok_or_else(|| {
                    FabricError::IntegrityFailure(
                        "checkpoint has no coordinator manifest anchor".into(),
                    )
                })?,
                from_addr,
                fetch_token: token,
                coordinator_epoch: self.epoch,
            };
            let res: protocol::NodeReplicateResult =
                self.call_node(&target, Op::NodeReplicateRequest, &req, NODE_OP_TIMEOUT_MS)?;
            if res.ok {
                self.store.checkpoint_add_location(
                    checkpoint_id,
                    &crate::checkpoint::DurableLocation {
                        node: target.clone(),
                        path: res.path.unwrap_or_default(),
                        verified: true,
                    },
                )?;
            } else {
                log::warn!("replication to {target} failed: {:?}", res.error);
            }
            ck = self.store.checkpoint_get(checkpoint_id)?.unwrap();
        }
        self.audit(
            ctx,
            "checkpoint.replicate",
            Some(&ck.workload_id),
            Some(checkpoint_id),
            AuditResult::Ok,
            &format!("replica count now {}", ck.replica_count),
        );
        Ok(ck.replica_count)
    }

    // ================= restore / rollback / migrate =================

    pub fn request_restore(
        &self,
        checkpoint_id: &Id,
        target_node: &str,
        options: &RestoreOptions,
        ctx: &OperationContext,
    ) -> FabricResult<OperationOutcome> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.restore)?;
        if options.rollback && options.migration {
            return Err(FabricError::InvalidArgument(
                "restore cannot be both rollback and migration".into(),
            ));
        }
        if options.rollback {
            self.policy
                .authorize(&ctx.roles, &self.policy.authority.rollback)?;
        }
        if options.migration {
            self.policy
                .authorize(&ctx.roles, &self.policy.authority.migrate)?;
        }
        if !self.node_is_active(target_node)? {
            return Err(FabricError::RestoreFailure(format!(
                "restore target node {target_node} is not active"
            )));
        }
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        restore::validate_restore_request(&ckpt, &self.policy, target_node)?;
        restore::restore_eligible(&ckpt, &self.policy)?;
        restore::validate_context_refs(&ckpt, &options.resolved_context_refs)?;
        let workload = self
            .store
            .workload_get(&ckpt.workload_id)?
            .ok_or_else(|| FabricError::WorkloadNotFound(ckpt.workload_id.to_string()))?;
        restore::validate_target_claim(&workload, target_node, options.migration)?;

        let target_desc = self.node_runtime(target_node)?;
        let compat = compatibility::evaluate(&ckpt, &target_desc, &self.policy)?;
        if compat.verdict == CompatVerdict::Incompatible {
            self.audit(
                ctx,
                "restore.rejected",
                Some(&ckpt.workload_id),
                Some(checkpoint_id),
                AuditResult::Rejected,
                &format!("incompatible target: {}", compat.reasons.join("; ")),
            );
            return Err(FabricError::CompatibilityFailure(compat.reasons.join("; ")));
        }
        crate::policy::enforce_degraded(&self.policy, compat.verdict)?;

        let attempt_id = format!(
            "restore-{}-{}",
            checkpoint_id.to_hex(),
            Id::random().to_hex()
        );
        self.store.reservation_create(
            &attempt_id,
            "restore",
            None,
            Some(checkpoint_id),
            target_node,
            RESERVATION_TTL_MS,
        )?;
        self.store.attempt_begin(
            &attempt_id,
            if options.rollback {
                "rollback"
            } else {
                "restore"
            },
            Some(checkpoint_id),
            Some(&ckpt.workload_id),
            target_node,
        )?;
        self.store.journal_append(
            "restore",
            &attempt_id,
            restore_states::RESERVED,
            "restore reserved",
        )?;

        self.execute_restore(
            &ckpt,
            target_node,
            &attempt_id,
            options,
            compat.verdict,
            ctx,
        )
        .map_err(|e| {
            let detail = format!("restore failed: {e}");
            let _ = self.abort_restore_attempt(&attempt_id, &ckpt, &detail, ctx);
            e
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_restore(
        &self,
        ckpt: &CheckpointObject,
        target_node: &str,
        attempt_id: &str,
        options: &RestoreOptions,
        compat_verdict: CompatVerdict,
        ctx: &OperationContext,
    ) -> FabricResult<OperationOutcome> {
        self.set_lifecycle(ckpt, LifecycleState::RestorePending)?;
        self.store.journal_append(
            "restore",
            attempt_id,
            restore_states::PROVISIONED,
            "target provisioned",
        )?;

        let node_has = {
            let probe = protocol::NodeProbeRequest {
                checkpoint_id: ckpt.checkpoint_id,
                coordinator_epoch: self.epoch,
            };
            let res: protocol::NodeProbeResult = self.call_node(
                target_node,
                Op::NodeProbeRequest,
                &probe,
                QUICK_OP_TIMEOUT_MS,
            )?;
            res.has_replica
        };
        if !node_has {
            self.ensure_replica(target_node, ckpt, attempt_id)?;
        }

        self.set_lifecycle(ckpt, LifecycleState::Restoring)?;

        let req = protocol::NodeRestoreRequest {
            attempt_id: attempt_id.to_string(),
            checkpoint_id: ckpt.checkpoint_id,
            workload_id: ckpt.workload_id,
            expected_manifest_digest: ckpt.manifest_digest.clone().ok_or_else(|| {
                FabricError::IntegrityFailure(
                    "checkpoint has no coordinator manifest anchor".into(),
                )
            })?,
            max_component_bytes: self.policy.max_component_bytes,
            coordinator_epoch: self.epoch,
        };
        let res: protocol::NodeRestoreResult = self.call_node(
            target_node,
            Op::NodeRestoreRequest,
            &req,
            NODE_OP_TIMEOUT_MS,
        )?;
        if !res.ok {
            return Err(FabricError::RestoreFailure(
                res.error.unwrap_or_else(|| "node restore failed".into()),
            ));
        }
        self.store.journal_append(
            "restore",
            attempt_id,
            restore_states::COMPONENTS_RESTORED,
            "components restored",
        )?;

        failpoints::fire("restore.before_generation_commit")?;
        let token = options
            .target_fence_token
            .clone()
            .unwrap_or_else(migration::new_fence_token);
        self.store.restore_commit(
            &ckpt.checkpoint_id,
            attempt_id,
            target_node,
            &token,
            options.migration,
            options.rollback,
        )?;
        let w = self.store.workload_get(&ckpt.workload_id)?.unwrap();
        failpoints::fire("restore.after_generation_commit")?;

        let mut resumed = false;
        let mut resume_error: Option<String> = None;
        let resume_req = protocol::NodeResumeRequest {
            attempt_id: attempt_id.to_string(),
            checkpoint_id: ckpt.checkpoint_id,
            workload_id: ckpt.workload_id,
            fence_token: Some(token),
            execution_epoch: Some(w.execution_epoch),
            resume: options.resume,
            coordinator_epoch: self.epoch,
        };
        match self.call_node::<protocol::NodeResumeRequest, protocol::NodeResumeResult>(
            target_node,
            Op::NodeResumeRequest,
            &resume_req,
            NODE_OP_TIMEOUT_MS,
        ) {
            Ok(r) => {
                resumed = options.resume && r.ok;
                if !r.ok {
                    resume_error = r.error;
                }
            }
            Err(e) => resume_error = Some(e.to_string()),
        }
        self.store.journal_append(
            "restore",
            attempt_id,
            restore_states::RESUMED,
            if resumed { "resumed" } else { "not_resumed" },
        )?;

        let restored_class =
            restore::restored_resumability(ckpt.resumability, compat_verdict, resumed);

        self.store.attempt_finish(
            attempt_id,
            if resumed {
                "COMMITTED"
            } else {
                "COMMITTED_NO_RESUME"
            },
            Some(if resumed {
                "restore committed and resumed"
            } else {
                "restore committed without resume"
            }),
            resume_error.as_deref(),
        )?;
        self.store.reservation_release(attempt_id)?;
        self.audit(
            ctx,
            "restore.commit",
            Some(&ckpt.workload_id),
            Some(&ckpt.checkpoint_id),
            AuditResult::Ok,
            &format!(
                "restored to {target_node}: generation {} epoch {} class {restored_class:?} resumed={resumed}",
                w.workload_generation, w.execution_epoch
            ),
        );
        failpoints::fire("restore.after_commit")?;

        Ok(OperationOutcome {
            attempt_id: attempt_id.to_string(),
            checkpoint_id: Some(ckpt.checkpoint_id),
            workload_id: Some(ckpt.workload_id),
            node: Some(target_node.to_string()),
            state: if resumed {
                "RESTORED"
            } else {
                "RESTORED_NO_RESUME"
            }
            .into(),
            result: Some(
                if resumed {
                    "restored"
                } else {
                    "restored without resume"
                }
                .into(),
            ),
            error: resume_error,
            resumability: Some(restored_class),
            workload_generation: Some(w.workload_generation),
            execution_epoch: Some(w.execution_epoch),
        })
    }

    fn abort_restore_attempt(
        &self,
        attempt_id: &str,
        ckpt: &CheckpointObject,
        detail: &str,
        ctx: &OperationContext,
    ) -> FabricResult<()> {
        let committed =
            self.store
                .journal_has("restore", attempt_id, restore_states::GENERATION_COMMITTED)?;
        if committed {
            self.store.attempt_finish(
                attempt_id,
                "COMMITTED_NO_RESUME",
                Some("restore committed before post-commit failure"),
                Some(detail),
            )?;
            self.store.reservation_release(attempt_id)?;
            self.audit(
                ctx,
                "restore.post_commit_failure",
                Some(&ckpt.workload_id),
                Some(&ckpt.checkpoint_id),
                AuditResult::Recovered,
                detail,
            );
            return Ok(());
        }

        if let Some(attempt) = self.store.attempt_get(attempt_id)? {
            let cleanup = protocol::NodeCleanupRequest {
                staging_attempts: vec![attempt_id.to_string()],
                staging_paths: Vec::new(),
                restore_attempts: vec![protocol::RestoreCleanupTarget {
                    attempt_id: attempt_id.to_string(),
                    checkpoint_id: ckpt.checkpoint_id,
                    workload_id: ckpt.workload_id,
                }],
                checkpoint_ids: Vec::new(),
                coordinator_epoch: self.epoch,
            };
            let _ = self.call_node::<_, protocol::NodeCleanupResult>(
                &attempt.node,
                Op::NodeCleanupRequest,
                &cleanup,
                QUICK_OP_TIMEOUT_MS,
            );
        }
        if let Some(c) = self.store.checkpoint_get(&ckpt.checkpoint_id)? {
            if c.lifecycle == LifecycleState::RestorePending
                || c.lifecycle == LifecycleState::Restoring
            {
                c.lifecycle.transition(LifecycleState::Available)?;
                self.store.checkpoint_set_lifecycle(
                    &ckpt.checkpoint_id,
                    LifecycleState::Available,
                    IntegrityState::Valid,
                )?;
            }
        }
        self.store
            .attempt_finish(attempt_id, "FAILED", None, Some(detail))?;
        self.store.reservation_release(attempt_id)?;
        self.audit(
            ctx,
            "restore.aborted",
            Some(&ckpt.workload_id),
            Some(&ckpt.checkpoint_id),
            AuditResult::Failed,
            detail,
        );
        Ok(())
    }

    pub fn request_rollback(
        &self,
        checkpoint_id: &Id,
        target_node: &str,
        ctx: &OperationContext,
    ) -> FabricResult<OperationOutcome> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.rollback)?;
        let options = RestoreOptions {
            rollback: true,
            resume: true,
            ..RestoreOptions::default()
        };
        self.request_restore(checkpoint_id, target_node, &options, ctx)
    }

    pub fn fork(
        &self,
        checkpoint_id: &Id,
        spec: &WorkloadSpec,
        ctx: &OperationContext,
    ) -> FabricResult<Workload> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.fork)?;
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        let parent = self
            .store
            .workload_get(&ckpt.workload_id)?
            .ok_or_else(|| FabricError::WorkloadNotFound(ckpt.workload_id.to_string()))?;

        let child = Workload {
            workload_id: spec.workload_id.unwrap_or_else(Id::random),
            workload_generation: 0,
            owner: spec.owner.clone(),
            class: spec.class.clone(),
            created_ms: now_ms(),
            execution_epoch: 0,
            active_node: None,
            backend_class: spec.backend_class.clone(),
            checkpoint_generation: 0,
            parent_workload: Some(parent.workload_id),
            fork_generation: parent.fork_generation.saturating_add(1),
            policy_version: self.policy.version,
            metadata: spec.metadata.clone(),
            state_schema_version: spec.state_schema_version,
            runtime: spec.runtime.clone(),
            resumability_class: crate::checkpoint::ResumabilityClass::RestartFromCheckpoint,
            protection: spec.protection.clone(),
            single_active: spec.single_active,
            fence_token: None,
            fence_epoch: 0,
        };
        let detail = format!(
            "fork from checkpoint {checkpoint_id} (gen {})",
            ckpt.checkpoint_generation
        );
        self.store
            .workload_insert_fork(&child, &parent.workload_id, checkpoint_id, &detail)?;
        self.audit(
            ctx,
            "workload.fork",
            Some(&child.workload_id),
            Some(checkpoint_id),
            AuditResult::Ok,
            &format!("forked from workload {}", parent.workload_id),
        );
        Ok(self.store.workload_get(&child.workload_id)?.unwrap())
    }

    pub fn migrate(
        &self,
        checkpoint_id: &Id,
        target_node: &str,
        ctx: &OperationContext,
    ) -> FabricResult<OperationOutcome> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.migrate)?;
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        let workload = self
            .store
            .workload_get(&ckpt.workload_id)?
            .ok_or_else(|| FabricError::WorkloadNotFound(ckpt.workload_id.to_string()))?;
        let source_node = ckpt.source_node.clone();
        migration::validate_migration_request(
            &workload.workload_id,
            checkpoint_id,
            &source_node,
            target_node,
        )?;
        if !ckpt.lifecycle.is_restorable() {
            if !self.node_is_active(&source_node)? {
                return Err(FabricError::MigrationFailure(format!(
                    "source node {source_node} is not active; cannot capture fresh state"
                )));
            }
            let capture_outcome = self.request_capture(
                &workload.workload_id,
                &CaptureOptions {
                    quiescence: QuiescenceMode::None,
                    ..CaptureOptions::default()
                },
                ctx,
            )?;
            let new_id = capture_outcome
                .checkpoint_id
                .ok_or_else(|| FabricError::Internal("capture produced no checkpoint".into()))?;
            return self.migrate(&new_id, target_node, ctx);
        }
        let options = RestoreOptions {
            migration: true,
            resume: true,
            ..RestoreOptions::default()
        };
        self.request_restore(checkpoint_id, target_node, &options, ctx)
    }

    // ================= checkpoint maintenance =================

    pub fn verify_checkpoint(
        &self,
        checkpoint_id: &Id,
        ctx: &OperationContext,
    ) -> FabricResult<protocol::NodeVerifyResult> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.verify)?;
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        if ckpt.lifecycle == LifecycleState::Failed {
            return Err(FabricError::CorruptedCheckpoint(
                "failed checkpoint cannot be verified as valid".into(),
            ));
        }
        let node = ckpt
            .durable_locations
            .iter()
            .find(|l| self.node_is_active(&l.node).unwrap_or(false))
            .map(|l| l.node.clone())
            .ok_or_else(|| {
                FabricError::Internal("no active node holds a replica for verification".into())
            })?;
        let expected_manifest_digest = ckpt.manifest_digest.clone().ok_or_else(|| {
            FabricError::IntegrityFailure("checkpoint has no coordinator manifest anchor".into())
        })?;
        let req = protocol::NodeVerifyRequest {
            checkpoint_id: *checkpoint_id,
            expected_manifest_digest: expected_manifest_digest.clone(),
            coordinator_epoch: self.epoch,
        };
        let mut res: protocol::NodeVerifyResult =
            self.call_node(&node, Op::NodeVerifyRequest, &req, NODE_OP_TIMEOUT_MS)?;
        if res.ok && res.manifest_digest.as_deref() != Some(expected_manifest_digest.as_str()) {
            res.ok = false;
            res.error = Some("node verification did not match coordinator manifest anchor".into());
        }
        self.store.checkpoint_set_integrity(
            checkpoint_id,
            if res.ok {
                IntegrityState::Valid
            } else {
                IntegrityState::Corrupt
            },
        )?;
        self.audit(
            ctx,
            "checkpoint.verify",
            Some(&ckpt.workload_id),
            Some(checkpoint_id),
            if res.ok {
                AuditResult::Ok
            } else {
                AuditResult::Failed
            },
            &format!(
                "verification on node {node}: {}",
                res.error.as_deref().unwrap_or("ok")
            ),
        );
        Ok(res)
    }

    pub fn protect_checkpoint(
        &self,
        checkpoint_id: &Id,
        pinned: bool,
        ctx: &OperationContext,
    ) -> FabricResult<()> {
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        let p = if pinned {
            ProtectionState::Pinned
        } else {
            ProtectionState::Protected
        };
        self.store.checkpoint_set_protection(checkpoint_id, p)?;
        self.audit(
            ctx,
            if pinned {
                "checkpoint.pin"
            } else {
                "checkpoint.protect"
            },
            Some(&ckpt.workload_id),
            Some(checkpoint_id),
            AuditResult::Ok,
            &format!("protection set to {p:?}"),
        );
        Ok(())
    }

    pub fn unprotect_checkpoint(
        &self,
        checkpoint_id: &Id,
        ctx: &OperationContext,
    ) -> FabricResult<()> {
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        self.store
            .checkpoint_set_protection(checkpoint_id, ProtectionState::None)?;
        self.audit(
            ctx,
            "checkpoint.unprotect",
            Some(&ckpt.workload_id),
            Some(checkpoint_id),
            AuditResult::Ok,
            "protection cleared",
        );
        Ok(())
    }

    pub fn retire_checkpoint(
        &self,
        checkpoint_id: &Id,
        ctx: &OperationContext,
    ) -> FabricResult<()> {
        self.policy
            .authorize(&ctx.roles, &self.policy.authority.retire)?;
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        if self
            .store
            .reservation_active_for("restore", None, Some(checkpoint_id))?
            .is_some()
        {
            return Err(FabricError::PolicyViolation(
                "checkpoint has an active restore reservation; retire refused".into(),
            ));
        }
        if ckpt.protection != ProtectionState::None {
            return Err(FabricError::PolicyViolation(format!(
                "checkpoint is {:?}; unprotect before retiring",
                ckpt.protection
            )));
        }
        for loc in &ckpt.durable_locations {
            let req = protocol::NodeCleanupRequest {
                staging_attempts: Vec::new(),
                staging_paths: Vec::new(),
                restore_attempts: Vec::new(),
                checkpoint_ids: vec![*checkpoint_id],
                coordinator_epoch: self.epoch,
            };
            let result: protocol::NodeCleanupResult =
                self.call_node(&loc.node, Op::NodeCleanupRequest, &req, QUICK_OP_TIMEOUT_MS)?;
            if let Some(error) = result.error {
                return Err(FabricError::CleanupFailure(format!(
                    "retire cleanup on {} failed: {error}",
                    loc.node
                )));
            }
        }
        // Metadata remains discoverable until every physical replica confirms
        // deletion, so an interrupted retirement can be retried safely.
        self.store.checkpoint_retire(checkpoint_id)?;
        self.audit(
            ctx,
            "checkpoint.retire",
            Some(&ckpt.workload_id),
            Some(checkpoint_id),
            AuditResult::Ok,
            "checkpoint retired",
        );
        Ok(())
    }

    pub fn retirement_candidates(&self) -> FabricResult<Vec<crate::integrations::ReclaimView>> {
        let mut out = Vec::new();
        for w in self.store.workload_list()? {
            let mut cks = self.store.checkpoint_list(Some(&w.workload_id))?;
            cks.sort_by_key(|c| c.checkpoint_generation);
            let total = cks.len() as u32;
            for (i, c) in cks.iter().enumerate() {
                let is_retired = c.lifecycle == LifecycleState::Retired;
                let protected_gen = !is_retired
                    && c.checkpoint_generation
                        > w.checkpoint_generation
                            .saturating_sub(self.policy.protected_generations as u64);
                let min_kept = (total - i as u32) <= self.policy.min_generations_retained;
                let eligibility = if protected_gen {
                    RetirementEligibility::Protected
                } else if c.protection == ProtectionState::Pinned {
                    RetirementEligibility::Pinned
                } else if min_kept {
                    RetirementEligibility::MinimumGenerations
                } else if c.lifecycle != LifecycleState::Available {
                    RetirementEligibility::NotEligible
                } else {
                    RetirementEligibility::Eligible
                };
                out.push(crate::integrations::ReclaimView {
                    checkpoint_id: c.checkpoint_id,
                    workload_id: c.workload_id,
                    checkpoint_generation: c.checkpoint_generation,
                    lifecycle: c.lifecycle.as_str().to_string(),
                    value: c.restore_count + c.replica_count as u64,
                    protection: c.protection,
                    reconstructibility: if c.is_restorable() {
                        "restorable".into()
                    } else {
                        "not_restorable".into()
                    },
                    lineage_parents: c.lineage_parents.clone(),
                    superseded_by: c.superseded_by,
                    min_generations_retained: self.policy.min_generations_retained,
                    retirement_eligibility: eligibility,
                    total_physical_bytes: c.total_physical_bytes,
                    replicas: c.replica_count,
                });
            }
        }
        Ok(out)
    }

    pub fn checkpoint_lineage(&self, checkpoint_id: &Id) -> FabricResult<Vec<LineageRecord>> {
        self.store.lineage_query(None, Some(checkpoint_id))
    }

    pub fn compatibility(
        &self,
        checkpoint_id: &Id,
        target: &RuntimeCompatibilityDescriptor,
        ctx: &OperationContext,
    ) -> FabricResult<crate::compatibility::CompatibilityResult> {
        let ckpt = self
            .store
            .checkpoint_get(checkpoint_id)?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        self.audit(
            ctx,
            "checkpoint.compatibility",
            Some(&ckpt.workload_id),
            Some(checkpoint_id),
            AuditResult::Ok,
            &format!("evaluated against {}", target.runtime_version),
        );
        compatibility::evaluate(&ckpt, target, &self.policy)
    }

    pub fn audit_query(
        &self,
        since_ms: Option<u64>,
        limit: usize,
    ) -> FabricResult<Vec<crate::audit::AuditRecord>> {
        self.store.audit_query(since_ms, limit)
    }

    pub fn run_recovery(&self, ctx: &OperationContext) -> FabricResult<RecoveryOutcome> {
        let out = recovery::reconcile(&self.store, ctx)?;
        for action in &out.actions {
            let Some(attempt) = self.store.attempt_get(&action.key)? else {
                continue;
            };
            if !self.node_is_active(&attempt.node)? {
                continue;
            }
            match action.kind.as_str() {
                "capture" => {
                    if let (Some(checkpoint_id), Some(workload_id)) =
                        (attempt.checkpoint_id, attempt.workload_id)
                    {
                        let resume = protocol::NodeResumeRequest {
                            attempt_id: action.key.clone(),
                            checkpoint_id,
                            workload_id,
                            fence_token: None,
                            execution_epoch: None,
                            resume: true,
                            coordinator_epoch: self.epoch,
                        };
                        let result: protocol::NodeResumeResult = self.call_node(
                            &attempt.node,
                            Op::NodeResumeRequest,
                            &resume,
                            QUICK_OP_TIMEOUT_MS,
                        )?;
                        if !result.ok {
                            return Err(FabricError::CleanupFailure(format!(
                                "capture recovery resume on {} failed: {}",
                                attempt.node,
                                result.error.unwrap_or_default()
                            )));
                        }
                    }
                    let cleanup = protocol::NodeCleanupRequest {
                        staging_attempts: vec![action.key.clone()],
                        staging_paths: Vec::new(),
                        restore_attempts: Vec::new(),
                        checkpoint_ids: if action.state == "failed_cleanup" {
                            attempt.checkpoint_id.into_iter().collect()
                        } else {
                            Vec::new()
                        },
                        coordinator_epoch: self.epoch,
                    };
                    let result: protocol::NodeCleanupResult = self.call_node(
                        &attempt.node,
                        Op::NodeCleanupRequest,
                        &cleanup,
                        QUICK_OP_TIMEOUT_MS,
                    )?;
                    if let Some(error) = result.error {
                        return Err(FabricError::CleanupFailure(error));
                    }
                }
                "restore" if action.state == "failed_cleanup" => {
                    if let (Some(checkpoint_id), Some(workload_id)) =
                        (attempt.checkpoint_id, attempt.workload_id)
                    {
                        let cleanup = protocol::NodeCleanupRequest {
                            staging_attempts: vec![action.key.clone()],
                            staging_paths: Vec::new(),
                            restore_attempts: vec![protocol::RestoreCleanupTarget {
                                attempt_id: action.key.clone(),
                                checkpoint_id,
                                workload_id,
                            }],
                            checkpoint_ids: Vec::new(),
                            coordinator_epoch: self.epoch,
                        };
                        let result: protocol::NodeCleanupResult = self.call_node(
                            &attempt.node,
                            Op::NodeCleanupRequest,
                            &cleanup,
                            QUICK_OP_TIMEOUT_MS,
                        )?;
                        if let Some(error) = result.error {
                            return Err(FabricError::CleanupFailure(error));
                        }
                    }
                }
                "restore" if action.state == "resume_unverified" => {
                    if let (Some(checkpoint_id), Some(workload_id)) =
                        (attempt.checkpoint_id, attempt.workload_id)
                    {
                        let workload = self.store.workload_get(&workload_id)?.ok_or_else(|| {
                            FabricError::WorkloadNotFound(workload_id.to_string())
                        })?;
                        let resume = protocol::NodeResumeRequest {
                            attempt_id: action.key.clone(),
                            checkpoint_id,
                            workload_id,
                            fence_token: workload.fence_token.clone(),
                            execution_epoch: Some(workload.execution_epoch),
                            resume: false,
                            coordinator_epoch: self.epoch,
                        };
                        let result: protocol::NodeResumeResult = self.call_node(
                            &attempt.node,
                            Op::NodeResumeRequest,
                            &resume,
                            QUICK_OP_TIMEOUT_MS,
                        )?;
                        if !result.ok {
                            return Err(FabricError::CleanupFailure(format!(
                                "restore recovery finalization on {} failed: {}",
                                attempt.node,
                                result.error.unwrap_or_default()
                            )));
                        }
                    }
                }
                _ => {}
            }
        }
        self.audit(
            ctx,
            "coordinator.recovery",
            None,
            None,
            if out.ok {
                AuditResult::Recovered
            } else {
                AuditResult::Failed
            },
            &format!(
                "reconciled {} actions; {} stale nodes",
                out.actions.len(),
                out.stale_nodes.len()
            ),
        );
        Ok(out)
    }

    pub fn stats(&self) -> FabricResult<Stats> {
        self.store.stats()
    }

    pub fn list_nodes(&self) -> FabricResult<Vec<crate::node::NodeRecord>> {
        self.store.node_list()
    }

    pub fn capture_status(&self, attempt_id: &str) -> FabricResult<AttemptRecord> {
        self.store
            .attempt_get(attempt_id)?
            .ok_or_else(|| FabricError::Internal(format!("no such attempt: {attempt_id}")))
    }

    pub fn checkpoint_get(&self, checkpoint_id: &Id) -> FabricResult<Option<CheckpointObject>> {
        self.store.checkpoint_get(checkpoint_id)
    }

    pub fn checkpoint_list(&self, workload_id: Option<&Id>) -> FabricResult<Vec<CheckpointObject>> {
        self.store.checkpoint_list(workload_id)
    }

    // ================= node RPC plumbing =================

    fn ensure_replica(
        &self,
        target_node: &str,
        ckpt: &CheckpointObject,
        attempt_id: &str,
    ) -> FabricResult<()> {
        let source = ckpt
            .durable_locations
            .iter()
            .find(|l| self.node_is_active(&l.node).unwrap_or(false))
            .ok_or_else(|| {
                FabricError::Internal("no active node holds a replica for restore".into())
            })?
            .node
            .clone();
        let token = Id::random().to_hex();
        let allow = protocol::NodeAllowFetchRequest {
            checkpoint_id: ckpt.checkpoint_id,
            fetch_token: token.clone(),
            ttl_ms: 300_000,
            coordinator_epoch: self.epoch,
        };
        self.call_node::<protocol::NodeAllowFetchRequest, protocol::Envelope>(
            &source,
            Op::NodeAllowFetch,
            &allow,
            QUICK_OP_TIMEOUT_MS,
        )?;
        let from_addr = self.node_addr(&source)?.to_string();
        let req = protocol::NodeReplicateRequest {
            attempt_id: attempt_id.to_string(),
            checkpoint_id: ckpt.checkpoint_id,
            expected_manifest_digest: ckpt.manifest_digest.clone().ok_or_else(|| {
                FabricError::IntegrityFailure(
                    "checkpoint has no coordinator manifest anchor".into(),
                )
            })?,
            from_addr,
            fetch_token: token,
            coordinator_epoch: self.epoch,
        };
        let res: protocol::NodeReplicateResult = self.call_node(
            target_node,
            Op::NodeReplicateRequest,
            &req,
            NODE_OP_TIMEOUT_MS,
        )?;
        if !res.ok {
            return Err(FabricError::RestoreFailure(format!(
                "replica staging on target failed: {}",
                res.error.unwrap_or_default()
            )));
        }
        self.store.checkpoint_add_location(
            &ckpt.checkpoint_id,
            &crate::checkpoint::DurableLocation {
                node: target_node.to_string(),
                path: res.path.unwrap_or_default(),
                verified: true,
            },
        )?;
        Ok(())
    }

    fn node_addr(&self, node: &str) -> FabricResult<SocketAddr> {
        let nodes = self.store.node_list()?;
        let n = nodes
            .iter()
            .find(|n| n.id == node)
            .ok_or_else(|| FabricError::Internal(format!("node {node} not registered")))?;
        let addr = n.addr.clone();
        addr.parse()
            .map_err(|e| FabricError::Internal(format!("bad node addr {addr}: {e}")))
    }

    fn call_node<T: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        node: &str,
        op: Op,
        payload: &T,
        timeout_ms: u64,
    ) -> FabricResult<R> {
        let addr = self.node_addr(node)?;
        let bytes = serde_json::to_vec(payload)?;
        let _ = timeout_ms;
        // Take the client out of the map so no lock is held across blocking I/O.
        // A failed call is never replayed: after a complete write, a lost response
        // is ambiguous and retrying capture/restore could duplicate side effects.
        let mut client = {
            let mut guard = self.node_clients.lock().unwrap();
            match guard.remove(node) {
                Some(c) => c,
                None => RpcClient::connect(&addr)?,
            }
        };
        let resp = client.call(op, &bytes)?;
        self.node_clients
            .lock()
            .unwrap()
            .insert(node.to_string(), client);
        serde_json::from_slice(&resp)
            .map_err(|e| FabricError::ProtocolError(format!("bad node response: {e}")))
    }

    /// RPC dispatch: maps operations to core methods.
    fn dispatch(coordinator: &Coordinator, op: Op, payload: &[u8]) -> FabricResult<Vec<u8>> {
        match op {
            Op::Ping => Ok(b"pong".to_vec()),
            Op::Hello => {
                let _req: protocol::HelloRequest = serde_json::from_slice(payload)?;
                Ok(serde_json::to_vec(&Envelope::ok())?)
            }
            Op::CoordinatorShutdown => {
                // Acknowledge, then trigger the stop: the main process loop
                // observes the flag and performs the bounded join.
                coordinator.request_stop();
                if let Some(addr) = coordinator.listen_addr() {
                    // Self-connect unblocks the accept loop.
                    let _ = std::net::TcpStream::connect_timeout(
                        &addr,
                        std::time::Duration::from_millis(200),
                    );
                }
                Ok(serde_json::to_vec(&Envelope::ok())?)
            }
            Op::WorkloadCreate => {
                let req: protocol::WorkloadCreateRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.create_workload(&req.spec, req.node.as_deref(), &ctx) {
                    Ok(w) => Ok(serde_json::to_vec(&protocol::WorkloadCreateResponse {
                        workload: w,
                    })?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::WorkloadInspect => {
                let req: protocol::WorkloadIdRequest = serde_json::from_slice(payload)?;
                match coordinator.inspect_workload(&req.workload_id) {
                    Ok(Some(w)) => Ok(serde_json::to_vec(&w)?),
                    Ok(None) => Ok(serde_json::to_vec(&Envelope::err("workload not found"))?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::WorkloadList => match coordinator.list_workloads() {
                Ok(v) => Ok(serde_json::to_vec(&v)?),
                Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
            },
            Op::WorkloadFence => {
                let req: protocol::WorkloadIdRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.fence_workload(&req.workload_id, &ctx) {
                    Ok(w) => Ok(serde_json::to_vec(&w)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::WorkloadLineage => {
                let req: protocol::WorkloadIdRequest = serde_json::from_slice(payload)?;
                match coordinator.workload_lineage(&req.workload_id) {
                    Ok(v) => Ok(serde_json::to_vec(&v)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::WorkloadAttach => {
                let req: protocol::WorkloadAttachRequest = serde_json::from_slice(payload)?;
                match coordinator.attach_workload(
                    &req.workload_id,
                    &req.node,
                    &req.node_boot_id,
                    req.fence_epoch,
                ) {
                    Ok(token) => {
                        // Advertise the newly hosted provider handlers on the node.
                        let mut versions = std::collections::BTreeMap::new();
                        for p in &req.providers {
                            versions
                                .entry(p.restore_handler.clone())
                                .or_insert_with(|| p.schema_version.to_string());
                        }
                        if !versions.is_empty() {
                            if let Err(e) =
                                coordinator.merge_node_provider_versions(&req.node, &versions)
                            {
                                log::warn!("provider version merge failed: {e}");
                            }
                        }
                        Ok(serde_json::to_vec(&protocol::WorkloadAttachResponse {
                            accepted: true,
                            fence_token: Some(token),
                            error: None,
                        })?)
                    }
                    Err(e) => Ok(serde_json::to_vec(&protocol::WorkloadAttachResponse {
                        accepted: false,
                        fence_token: None,
                        error: Some(e.to_string()),
                    })?),
                }
            }
            Op::WorkloadDetach => {
                let req: protocol::WorkloadDetachRequest = serde_json::from_slice(payload)?;
                let _ = coordinator.detach_workload(&req.workload_id, &req.node);
                Ok(serde_json::to_vec(&Envelope::ok())?)
            }
            Op::Capture => {
                let req: protocol::CaptureRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.request_capture(&req.workload_id, &req.options, &ctx) {
                    Ok(o) => Ok(serde_json::to_vec(&o)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CaptureStatus => {
                let req: protocol::AttemptStatusRequest = serde_json::from_slice(payload)?;
                match coordinator.capture_status(&req.attempt_id) {
                    Ok(a) => Ok(serde_json::to_vec(&protocol::AttemptStatusResponse {
                        attempt: a.clone(),
                        checkpoint_id: a.checkpoint_id,
                        state: a.state,
                    })?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointInspect => {
                let req: protocol::CheckpointIdRequest = serde_json::from_slice(payload)?;
                match coordinator.checkpoint_get(&req.checkpoint_id) {
                    Ok(Some(c)) => Ok(serde_json::to_vec(&c)?),
                    Ok(None) => Ok(serde_json::to_vec(&Envelope::err("checkpoint not found"))?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointList => {
                let req: protocol::CheckpointListRequest = serde_json::from_slice(payload)?;
                match coordinator.checkpoint_list(req.workload_id.as_ref()) {
                    Ok(v) => Ok(serde_json::to_vec(&v)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointVerify => {
                let req: protocol::CheckpointIdRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.verify_checkpoint(&req.checkpoint_id, &ctx) {
                    Ok(r) => Ok(serde_json::to_vec(&r)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointProtect => {
                let req: protocol::CheckpointIdRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.protect_checkpoint(&req.checkpoint_id, false, &ctx) {
                    Ok(()) => Ok(serde_json::to_vec(&Envelope::ok())?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointUnprotect => {
                let req: protocol::CheckpointIdRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.unprotect_checkpoint(&req.checkpoint_id, &ctx) {
                    Ok(()) => Ok(serde_json::to_vec(&Envelope::ok())?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointRetire => {
                let req: protocol::CheckpointIdRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.retire_checkpoint(&req.checkpoint_id, &ctx) {
                    Ok(()) => Ok(serde_json::to_vec(&Envelope::ok())?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::CheckpointLineage => {
                let req: protocol::CheckpointIdRequest = serde_json::from_slice(payload)?;
                match coordinator.checkpoint_lineage(&req.checkpoint_id) {
                    Ok(v) => Ok(serde_json::to_vec(&v)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Restore | Op::Rollback => {
                let req: protocol::RestoreRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                let res = if op == Op::Rollback {
                    coordinator.request_rollback(&req.checkpoint_id, &req.node, &ctx)
                } else {
                    coordinator.request_restore(&req.checkpoint_id, &req.node, &req.options, &ctx)
                };
                match res {
                    Ok(o) => Ok(serde_json::to_vec(&o)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Fork => {
                let req: protocol::ForkRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.fork(&req.checkpoint_id, &req.spec, &ctx) {
                    Ok(w) => Ok(serde_json::to_vec(&protocol::ForkResponse { workload: w })?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Migrate => {
                let req: protocol::MigrateRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.migrate(&req.checkpoint_id, &req.target_node, &ctx) {
                    Ok(o) => Ok(serde_json::to_vec(&o)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Compatibility => {
                let req: protocol::CompatibilityRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.compatibility(&req.checkpoint_id, &req.target, &ctx) {
                    Ok(r) => Ok(serde_json::to_vec(&r)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Audit => {
                let req: protocol::AuditRequest = serde_json::from_slice(payload)?;
                match coordinator.audit_query(req.since_ms, req.limit) {
                    Ok(v) => Ok(serde_json::to_vec(&v)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Recovery => {
                let req: protocol::RecoveryRequest = serde_json::from_slice(payload)?;
                let ctx =
                    OperationContext::for_authority(&req.authority, coordinator.config.stale_ms);
                match coordinator.run_recovery(&ctx) {
                    Ok(o) => Ok(serde_json::to_vec(&o)?),
                    Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
                }
            }
            Op::Stats => match coordinator.stats() {
                Ok(s) => Ok(serde_json::to_vec(&s)?),
                Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
            },
            Op::NodeList => match coordinator.list_nodes() {
                Ok(v) => Ok(serde_json::to_vec(&v)?),
                Err(e) => Ok(serde_json::to_vec(&Envelope::err(&e.to_string()))?),
            },
            Op::NodeRegister => {
                let req: protocol::NodeRegisterRequest = serde_json::from_slice(payload)?;
                if req.coordinator_epoch != coordinator.epoch {
                    return Ok(serde_json::to_vec(&protocol::NodeRegisterResponse {
                        accepted: false,
                        coordinator_epoch: coordinator.epoch,
                        error: Some(format!(
                            "stale coordinator epoch: node holds {}, coordinator is {}",
                            req.coordinator_epoch, coordinator.epoch
                        )),
                    })?);
                }
                let resources = serde_json::json!({
                    "data_dir": req.data_dir,
                    "runtime": req.runtime,
                    "hardware": req.hardware,
                    "details": req.resources,
                });
                let mut ok = true;
                let mut err: Option<String> = None;
                if let Err(e) = coordinator.store.node_register(
                    &req.node_id,
                    &req.listen_addr,
                    &req.boot_id,
                    &serde_json::to_string(&resources)?,
                ) {
                    ok = false;
                    err = Some(e.to_string());
                }
                if ok {
                    for checkpoint_id in &req.committed_checkpoints {
                        if let Err(e) = coordinator.store.checkpoint_rebind_location(
                            checkpoint_id,
                            &req.node_id,
                            std::path::Path::new(&req.data_dir),
                        ) {
                            ok = false;
                            err = Some(e.to_string());
                            break;
                        }
                    }
                }
                Ok(serde_json::to_vec(&protocol::NodeRegisterResponse {
                    accepted: ok,
                    coordinator_epoch: coordinator.epoch,
                    error: err,
                })?)
            }
            Op::NodeHeartbeat => {
                let req: protocol::NodeHeartbeatRequest = serde_json::from_slice(payload)?;
                if req.coordinator_epoch != coordinator.epoch {
                    return Ok(serde_json::to_vec(&protocol::NodeHeartbeatResponse {
                        ok: false,
                        stale_workloads: Vec::new(),
                        error: Some("stale coordinator epoch".into()),
                    })?);
                }
                if let Err(e) = coordinator
                    .store
                    .node_validate_identity(&req.node_id, &req.boot_id)
                {
                    return Ok(serde_json::to_vec(&protocol::NodeHeartbeatResponse {
                        ok: false,
                        stale_workloads: Vec::new(),
                        error: Some(e.to_string()),
                    })?);
                }
                // Preserve the registration-time runtime/hardware descriptors
                // while refreshing the heartbeat payload and provider versions.
                let existing = coordinator
                    .store
                    .node_list()?
                    .iter()
                    .find(|n| n.id == req.node_id)
                    .and_then(|n| serde_json::from_str::<serde_json::Value>(&n.resources).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                let mut resources = serde_json::json!({
                    "data_dir": existing.get("data_dir").cloned().unwrap_or(serde_json::json!("")),
                    "runtime": existing.get("runtime").cloned().unwrap_or(serde_json::json!({})),
                    "hardware": existing.get("hardware").cloned().unwrap_or(serde_json::json!({})),
                    "details": req.resources,
                });
                if let Some(r) = resources.get_mut("runtime") {
                    if let Some(obj) = r.as_object_mut() {
                        obj.insert(
                            "provider_versions".into(),
                            serde_json::to_value(&req.provider_versions)?,
                        );
                    }
                }
                if let Err(e) = coordinator
                    .store
                    .node_heartbeat(&req.node_id, &serde_json::to_string(&resources)?)
                {
                    return Ok(serde_json::to_vec(&protocol::NodeHeartbeatResponse {
                        ok: false,
                        stale_workloads: Vec::new(),
                        error: Some(e.to_string()),
                    })?);
                }
                let data_dir = resources
                    .get("data_dir")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                for checkpoint_id in &req.committed_checkpoints {
                    coordinator.store.checkpoint_rebind_location(
                        checkpoint_id,
                        &req.node_id,
                        std::path::Path::new(data_dir),
                    )?;
                }
                let mut stale = Vec::new();
                for hb in &req.workloads {
                    if let Ok(Some(w)) = coordinator.store.workload_get(&hb.workload_id) {
                        if hb.execution_epoch != w.execution_epoch
                            || w.validate_fence(&req.node_id, &hb.fence_token).is_err()
                        {
                            stale.push(hb.workload_id);
                        }
                    }
                }
                Ok(serde_json::to_vec(&protocol::NodeHeartbeatResponse {
                    ok: true,
                    stale_workloads: stale,
                    error: None,
                })?)
            }
            _ => Err(FabricError::ProtocolError(format!(
                "coordinator cannot serve op {op:?}"
            ))),
        }
    }
}

fn sweep_stale(
    store: &Store,
    nodes: &Arc<Mutex<HashMap<String, RpcClient>>>,
    stale_ms: u64,
    epoch: u64,
) -> FabricResult<()> {
    let stale_before = now_ms().saturating_sub(stale_ms);
    let stale = store.nodes_sweep_stale(stale_before)?;
    if stale.is_empty() {
        return Ok(());
    }
    for n in &stale {
        store.node_set_status(n, "STALE")?;
        store.audit_append(
            "coordinator",
            "node.stale",
            None,
            None,
            AuditResult::Failed,
            &format!("node {n} declared stale (epoch {epoch})"),
        )?;
        nodes.lock().unwrap().remove(n);
        for w in store.workload_list()? {
            if w.active_node.as_deref() == Some(n) {
                let _ = store.workload_bump_fence(&w.workload_id, w.execution_epoch);
                store.audit_append(
                    "coordinator",
                    "workload.fence_by_stale_node",
                    Some(&w.workload_id),
                    None,
                    AuditResult::Recovered,
                    &format!("node {n} went stale; workload claim revoked"),
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::CaptureComponentRequest;
    use crate::checkpoint::ComponentType;

    pub(crate) fn test_coordinator() -> (tempfile::TempDir, Coordinator) {
        let dir = tempfile::tempdir().unwrap();
        let cfg = CoordinatorConfig {
            data_dir: dir.path().to_path_buf(),
            listen: None,
            epoch: Some(1),
            stale_ms: 60_000,
            policy: None,
            failpoints: None,
        };
        let coord = Coordinator::open(cfg).unwrap();
        (dir, coord)
    }

    fn owner() -> OperationContext {
        OperationContext {
            actor: "test".into(),
            roles: vec!["owner".into(), "operator".into()],
            stale_ms: 60_000,
        }
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
            protection: Default::default(),
            single_active: true,
        }
    }

    #[test]
    fn workload_lifecycle_ops() {
        let (_d, coord) = test_coordinator();
        let ctx = owner();
        let w = coord.create_workload(&spec(), None, &ctx).unwrap();
        assert_eq!(coord.list_workloads().unwrap().len(), 1);
        let fenced = coord.fence_workload(&w.workload_id, &ctx).unwrap();
        assert!(fenced.is_fenced());
        assert!(coord.create_workload(&spec(), None, &ctx).is_ok());
        assert!(coord.fence_workload(&Id::random(), &ctx).is_err());
    }

    #[test]
    fn epoch_is_monotonic_per_start() {
        let dir = tempfile::tempdir().unwrap();
        let c1 =
            Coordinator::open(CoordinatorConfig::default_in(dir.path().to_path_buf())).unwrap();
        let e1 = c1.epoch;
        drop(c1);
        let c2 =
            Coordinator::open(CoordinatorConfig::default_in(dir.path().to_path_buf())).unwrap();
        assert_eq!(c2.epoch, e1 + 1);
    }

    #[test]
    fn coordinator_lease_and_explicit_epoch_are_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = CoordinatorConfig::default_in(dir.path().to_path_buf());
        cfg.epoch = Some(5);
        let coordinator = Coordinator::open(cfg.clone()).unwrap();
        assert!(Coordinator::open(cfg.clone()).is_err());
        drop(coordinator);
        assert!(Coordinator::open(cfg.clone()).is_err());
        cfg.epoch = Some(6);
        assert_eq!(Coordinator::open(cfg).unwrap().epoch, 6);
    }

    #[test]
    fn capture_requires_active_node() {
        let (_d, coord) = test_coordinator();
        let ctx = owner();
        let w = coord.create_workload(&spec(), None, &ctx).unwrap();
        let res = coord.request_capture(&w.workload_id, &CaptureOptions::default(), &ctx);
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            FabricError::Internal(_) | FabricError::FencingFailure(_)
        ));
    }

    #[test]
    fn component_request_serde() {
        let req = CaptureComponentRequest {
            component_id: "app".into(),
            component_type: ComponentType::ApplicationState,
            required: true,
            schema_version: 1,
            restore_handler: "application".into(),
        };
        let j = serde_json::to_string(&req).unwrap();
        let back: CaptureComponentRequest = serde_json::from_str(&j).unwrap();
        assert_eq!(req, back);
    }

    #[test]
    fn authority_rejection() {
        let (_d, coord) = test_coordinator();
        let ctx = OperationContext {
            actor: "unauthorized".into(),
            roles: vec!["auditor".into()],
            stale_ms: 60_000,
        };
        let err = coord.fence_workload(&Id::random(), &ctx).unwrap_err();
        assert!(matches!(err, FabricError::PolicyViolation(_)));
    }
}
