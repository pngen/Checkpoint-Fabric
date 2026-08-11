#![allow(clippy::redundant_closure)]
//! Durable metadata store (SQLite).
//!
//! Workloads, checkpoints, lineage, attempts, nodes, reservations, policies,
//! journal, and audit records all live here. The schema is versioned; a database
//! with a newer schema than this binary understands is refused, never silently
//! mutated. All state changes that matter happen inside transactions.

use std::path::Path;
use std::sync::{Arc, Mutex};

use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};

use crate::audit::{AuditRecord, AuditResult};
use crate::checkpoint::{CheckpointObject, IntegrityState, ProtectionState, RetirementEligibility};
use crate::errors::{FabricError, FabricResult};
use crate::id::Id;
use crate::lifecycle::LifecycleState;
use crate::lineage::LineageRelation;
use crate::policy::PolicySet;
use crate::time::now_ms;
use crate::workload::Workload;

pub const SCHEMA_VERSION: i32 = 1;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS policies (
    version INTEGER PRIMARY KEY,
    json TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS workloads (
    id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    owner TEXT NOT NULL,
    class TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    execution_epoch INTEGER NOT NULL DEFAULT 0,
    active_node TEXT,
    backend_class TEXT NOT NULL,
    checkpoint_generation INTEGER NOT NULL DEFAULT 0,
    parent_workload TEXT,
    fork_generation INTEGER NOT NULL DEFAULT 0,
    policy_version INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    state_schema_version INTEGER NOT NULL,
    runtime_descriptor TEXT NOT NULL,
    resumability TEXT NOT NULL,
    protection TEXT NOT NULL,
    single_active INTEGER NOT NULL DEFAULT 1,
    fence_token TEXT,
    fence_epoch INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'ACTIVE'
);
CREATE TABLE IF NOT EXISTS checkpoints (
    id TEXT PRIMARY KEY,
    workload_id TEXT NOT NULL,
    workload_generation INTEGER NOT NULL,
    checkpoint_generation INTEGER NOT NULL,
    parent_checkpoint TEXT,
    capture_attempt TEXT NOT NULL,
    coordinator_epoch INTEGER NOT NULL,
    created_ms INTEGER NOT NULL,
    seal_ms INTEGER NOT NULL,
    source_node TEXT NOT NULL,
    source_backend TEXT NOT NULL,
    frontier TEXT NOT NULL,
    ctype TEXT NOT NULL,
    consistency TEXT NOT NULL,
    resumability TEXT NOT NULL,
    total_logical_bytes INTEGER NOT NULL,
    total_physical_bytes INTEGER NOT NULL,
    compressed_bytes INTEGER NOT NULL,
    replica_count INTEGER NOT NULL,
    policy_version INTEGER NOT NULL,
    runtime_descriptor TEXT NOT NULL,
    hardware_descriptor TEXT NOT NULL,
    deps TEXT NOT NULL,
    integrity_state TEXT NOT NULL,
    lifecycle TEXT NOT NULL,
    restore_count INTEGER NOT NULL DEFAULT 0,
    last_restore_result TEXT,
    protection TEXT NOT NULL,
    retirement_eligibility TEXT NOT NULL,
    metadata TEXT NOT NULL,
    manifest_digest TEXT,
    manifest_json TEXT,
    supersedes TEXT,
    superseded_by TEXT,
    locations TEXT NOT NULL,
    UNIQUE(workload_id, checkpoint_generation)
);
CREATE TABLE IF NOT EXISTS components (
    checkpoint_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    ctype TEXT NOT NULL,
    generation INTEGER NOT NULL,
    required INTEGER NOT NULL,
    logical_size INTEGER NOT NULL,
    storage_repr TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    restore_handler TEXT NOT NULL,
    compat TEXT NOT NULL,
    deps TEXT NOT NULL,
    capture_status TEXT NOT NULL,
    restore_status TEXT NOT NULL,
    PRIMARY KEY(checkpoint_id, component_id)
);
CREATE TABLE IF NOT EXISTS lineage (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    relation TEXT NOT NULL,
    workload_id TEXT,
    checkpoint_id TEXT,
    other_workload TEXT,
    other_checkpoint TEXT,
    ts_ms INTEGER NOT NULL,
    detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    checkpoint_id TEXT,
    workload_id TEXT,
    node TEXT,
    state TEXT NOT NULL,
    started_ms INTEGER NOT NULL,
    finished_ms INTEGER,
    result TEXT,
    error TEXT
);
CREATE TABLE IF NOT EXISTS nodes (
    id TEXT PRIMARY KEY,
    addr TEXT,
    boot_id TEXT NOT NULL,
    status TEXT NOT NULL,
    registered_ms INTEGER NOT NULL,
    last_heartbeat_ms INTEGER NOT NULL,
    resources TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    workload_id TEXT,
    checkpoint_id TEXT,
    node TEXT,
    state TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    expires_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_res_capture
    ON reservations(workload_id) WHERE kind='capture' AND state='ACTIVE';
CREATE UNIQUE INDEX IF NOT EXISTS uq_res_restore
    ON reservations(checkpoint_id) WHERE kind='restore' AND state='ACTIVE';
CREATE TABLE IF NOT EXISTS audit (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    actor TEXT NOT NULL,
    op TEXT NOT NULL,
    workload_id TEXT,
    checkpoint_id TEXT,
    result TEXT NOT NULL,
    detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    key TEXT NOT NULL,
    state TEXT NOT NULL,
    detail TEXT NOT NULL,
    ts_ms INTEGER NOT NULL,
    UNIQUE(kind, key, state)
);
CREATE INDEX IF NOT EXISTS idx_ckpt_workload ON checkpoints(workload_id, checkpoint_generation);
CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit(ts_ms);
CREATE INDEX IF NOT EXISTS idx_lineage_ckpt ON lineage(checkpoint_id);
CREATE INDEX IF NOT EXISTS idx_attempts_state ON attempts(state);
CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
";

/// The durable store. Single mutex over one SQLite connection: the coordinator
/// is the only writer, and all SQL statements are short and non-blocking.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Mutex<Connection>>,
    // Holding this handle keeps the cross-process exclusive coordinator lease.
    _lock: Arc<std::fs::File>,
    pub data_dir: std::path::PathBuf,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Store({:?})", self.data_dir)
    }
}

impl Store {
    /// Open (or create) the store at `data_dir/store.sqlite3`.
    pub fn open(data_dir: &Path) -> FabricResult<Self> {
        std::fs::create_dir_all(data_dir)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(data_dir.join("coordinator.lock"))?;
        FileExt::try_lock_exclusive(&lock).map_err(|e| {
            FabricError::PersistenceError(format!(
                "coordinator data directory {} is already owned by another process: {e}",
                data_dir.display()
            ))
        })?;
        let conn = Connection::open(data_dir.join("store.sqlite3"))?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(FabricError::from)?;
        if version > SCHEMA_VERSION {
            return Err(FabricError::PersistenceError(format!(
                "database schema v{version} is newer than supported v{SCHEMA_VERSION}; refusing to open"
            )));
        }
        if version == 0 {
            conn.execute_batch(SCHEMA)?;
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)?;
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(conn)),
            _lock: Arc::new(lock),
            data_dir: data_dir.to_path_buf(),
        })
    }

    /// Atomically claim the next coordinator epoch. Explicit epochs must move
    /// strictly forward; reuse or regression would let stale coordinators retain
    /// authority.
    pub fn claim_coordinator_epoch(&self, requested: Option<u64>) -> FabricResult<u64> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let stored: u64 = tx
            .query_row(
                "SELECT value FROM meta WHERE key='coordinator_epoch'",
                [],
                |r| r.get::<_, String>(0),
            )
            .optional()?
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let epoch = match requested {
            Some(v) if v > stored => v,
            Some(v) => {
                return Err(FabricError::StaleCoordinatorEpoch {
                    expected: stored.saturating_add(1),
                    got: v,
                })
            }
            None => stored.saturating_add(1),
        };
        tx.execute(
            "INSERT INTO meta(key,value) VALUES('coordinator_epoch',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [epoch.to_string()],
        )?;
        tx.commit()?;
        Ok(epoch)
    }

    pub fn meta_get(&self, key: &str) -> FabricResult<Option<String>> {
        let conn = self.inner.lock().unwrap();
        let v = conn
            .query_row("SELECT value FROM meta WHERE key=?1", [key], |r| r.get(0))
            .optional()?;
        Ok(v)
    }

    pub fn meta_put(&self, key: &str, value: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO meta(key,value) VALUES(?1,?2)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    // ---------------- policies ----------------

    pub fn policy_insert(&self, policy: &PolicySet, active: bool) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO policies(version,json,active) VALUES(?1,?2,?3)
             ON CONFLICT(version) DO UPDATE SET json=excluded.json, active=excluded.active",
            params![policy.version, policy.to_canonical_json()?, active as i32],
        )?;
        Ok(())
    }

    pub fn policy_versions(&self) -> FabricResult<Vec<u32>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT version FROM policies ORDER BY version")?;
        let rows = stmt.query_map([], |r| r.get::<_, u32>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn policy_load(&self, version: u32) -> FabricResult<Option<PolicySet>> {
        let conn = self.inner.lock().unwrap();
        let json: Option<String> = conn
            .query_row(
                "SELECT json FROM policies WHERE version=?1",
                [version],
                |r| r.get(0),
            )
            .optional()?;
        match json {
            None => Ok(None),
            Some(j) => Ok(Some(PolicySet::from_json(&j)?)),
        }
    }

    pub fn policy_load_all(&self) -> FabricResult<Vec<PolicySet>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT json FROM policies")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(PolicySet::from_json(&r?)?);
        }
        out.sort_by_key(|p| p.version);
        Ok(out)
    }

    // ---------------- workloads ----------------

    pub fn workload_insert(&self, w: &Workload) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO workloads(
                id,generation,owner,class,created_ms,execution_epoch,active_node,
                backend_class,checkpoint_generation,parent_workload,fork_generation,
                policy_version,metadata,state_schema_version,runtime_descriptor,
                resumability,protection,single_active,fence_token,fence_epoch,state)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21)",
            params![
                w.workload_id.to_hex(),
                w.workload_generation,
                w.owner,
                w.class,
                w.created_ms,
                w.execution_epoch,
                w.active_node,
                w.backend_class,
                w.checkpoint_generation,
                w.parent_workload.map(|i| i.to_hex()),
                w.fork_generation,
                w.policy_version,
                serde_json::to_string(&w.metadata)?,
                w.state_schema_version,
                serde_json::to_string(&w.runtime)?,
                serde_json::to_string(&w.resumability_class)?,
                serde_json::to_string(&w.protection)?,
                w.single_active as i32,
                w.fence_token,
                w.fence_epoch,
                "ACTIVE",
            ],
        )?;
        Ok(())
    }

    /// Atomically create a fork child and its lineage edge. A fork must never
    /// leave behind an ordinary orphan workload if lineage insertion fails.
    pub fn workload_insert_fork(
        &self,
        child: &Workload,
        parent: &Id,
        checkpoint: &Id,
        detail: &str,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO workloads(id,generation,owner,class,created_ms,execution_epoch,
             active_node,backend_class,checkpoint_generation,parent_workload,fork_generation,
             policy_version,metadata,state_schema_version,runtime_descriptor,resumability,
             protection,single_active,fence_token,fence_epoch)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            params![
                child.workload_id.to_hex(),
                child.workload_generation,
                child.owner,
                child.class,
                child.created_ms,
                child.execution_epoch,
                child.active_node,
                child.backend_class,
                child.checkpoint_generation,
                child.parent_workload.map(|i| i.to_hex()),
                child.fork_generation,
                child.policy_version,
                serde_json::to_string(&child.metadata)?,
                child.state_schema_version,
                serde_json::to_string(&child.runtime)?,
                serde_json::to_string(&child.resumability_class)?,
                serde_json::to_string(&child.protection)?,
                child.single_active as i32,
                child.fence_token,
                child.fence_epoch,
            ],
        )?;
        tx.execute(
            "INSERT INTO lineage(relation,workload_id,checkpoint_id,other_workload,other_checkpoint,ts_ms,detail)
             VALUES(?1,?2,NULL,?3,?4,?5,?6)",
            params![
                LineageRelation::ForkedFrom.as_str(),
                child.workload_id.to_hex(),
                parent.to_hex(),
                checkpoint.to_hex(),
                now_ms(),
                detail,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn workload_get(&self, id: &Id) -> FabricResult<Option<Workload>> {
        let conn = self.inner.lock().unwrap();
        let row = conn
            .query_row("SELECT * FROM workloads WHERE id=?1", [id.to_hex()], |r| {
                row_to_workload(r)
            })
            .optional()?;
        Ok(row)
    }

    pub fn workload_list(&self) -> FabricResult<Vec<Workload>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT * FROM workloads ORDER BY created_ms")?;
        let rows = stmt.query_map([], |r| row_to_workload(r))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn workload_update(&self, w: &Workload) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE workloads SET
                generation=?2, execution_epoch=?3, active_node=?4,
                checkpoint_generation=?5, policy_version=?6, resumability=?7,
                protection=?8, single_active=?9, fence_token=?10, fence_epoch=?11,
                state=?12
             WHERE id=?1",
            params![
                w.workload_id.to_hex(),
                w.workload_generation,
                w.execution_epoch,
                w.active_node,
                w.checkpoint_generation,
                w.policy_version,
                serde_json::to_string(&w.resumability_class)?,
                serde_json::to_string(&w.protection)?,
                w.single_active as i32,
                w.fence_token,
                w.fence_epoch,
                "ACTIVE",
            ],
        )?;
        Ok(())
    }

    /// Atomic check-and-set bump of the workload execution epoch for fencing.
    /// Clears the active claim and fence token: a fenced workload has no active
    /// continuation until it is re-claimed.
    pub fn workload_bump_fence(&self, id: &Id, expected_epoch: u64) -> FabricResult<u64> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let cur: Option<u64> = tx
            .query_row(
                "SELECT execution_epoch FROM workloads WHERE id=?1",
                [id.to_hex()],
                |r| r.get(0),
            )
            .optional()?;
        let cur = cur.ok_or_else(|| FabricError::WorkloadNotFound(id.to_string()))?;
        if cur != expected_epoch {
            return Err(FabricError::StaleWorkloadEpoch {
                expected: expected_epoch,
                got: cur,
            });
        }
        let new_epoch = cur + 1;
        tx.execute(
            "UPDATE workloads SET execution_epoch=?2, fence_token=NULL, active_node=NULL WHERE id=?1",
            params![id.to_hex(), new_epoch],
        )?;
        tx.commit()?;
        Ok(new_epoch)
    }

    /// Atomically claim a workload for a node: verify single-active fencing and
    /// set the new fence token. Returns the new token.
    pub fn workload_claim(
        &self,
        id: &Id,
        node: &str,
        token: &str,
        fence_epoch: u64,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let (active_node, single_active, cur_epoch): (Option<String>, i32, u64) = tx
            .query_row(
                "SELECT active_node, single_active, execution_epoch FROM workloads WHERE id=?1",
                [id.to_hex()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| FabricError::WorkloadNotFound(id.to_string()))?;
        if single_active == 1 {
            if let Some(an) = &active_node {
                if an != node && an != "MIGRATING" {
                    return Err(FabricError::FencingFailure(format!(
                        "workload is single-active and already claimed by node {an}"
                    )));
                }
            }
        }
        if cur_epoch != fence_epoch {
            return Err(FabricError::StaleWorkloadEpoch {
                expected: cur_epoch,
                got: fence_epoch,
            });
        }
        tx.execute(
            "UPDATE workloads SET active_node=?2, fence_token=?3, fence_epoch=?4 WHERE id=?1",
            params![id.to_hex(), node, token, cur_epoch],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Release a workload's active claim (fence revocation).
    pub fn workload_release(&self, id: &Id, node: &str) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "UPDATE workloads SET fence_token=NULL, active_node=NULL WHERE id=?1 AND active_node=?2",
            params![id.to_hex(), node],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Commit a new checkpoint generation on a workload (used at restore/rollback
    /// to bump workload generation). `bump_execution` also bumps the epoch.
    pub fn workload_advance(
        &self,
        id: &Id,
        new_workload_generation: u64,
        checkpoint_generation: u64,
        bump_execution: bool,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let cur: (u64, u64) = tx
            .query_row(
                "SELECT generation, execution_epoch FROM workloads WHERE id=?1",
                [id.to_hex()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| FabricError::WorkloadNotFound(id.to_string()))?;
        let new_gen = if bump_execution {
            cur.0 + 1
        } else {
            new_workload_generation
        };
        let new_epoch = if bump_execution { cur.1 + 1 } else { cur.1 };
        if new_gen <= cur.0 && bump_execution {
            return Err(FabricError::GenerationMismatch(format!(
                "workload generation must be monotonic: {} -> {new_gen}",
                cur.0
            )));
        }
        tx.execute(
            "UPDATE workloads SET generation=?2, checkpoint_generation=?3, execution_epoch=?4
             WHERE id=?1",
            params![id.to_hex(), new_gen, checkpoint_generation, new_epoch],
        )?;
        tx.commit()?;
        Ok(())
    }

    // ---------------- checkpoints ----------------

    /// Insert a new checkpoint. Conflicting identities or generations fail
    /// closed; silently ignoring the parent row while inserting component rows
    /// would create an orphan or hybrid checkpoint.
    pub fn checkpoint_insert(&self, c: &CheckpointObject) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO checkpoints(
                id,workload_id,workload_generation,checkpoint_generation,parent_checkpoint,
                capture_attempt,coordinator_epoch,created_ms,seal_ms,source_node,source_backend,
                frontier,ctype,consistency,resumability,total_logical_bytes,total_physical_bytes,
                compressed_bytes,replica_count,policy_version,runtime_descriptor,
                hardware_descriptor,deps,integrity_state,lifecycle,restore_count,
                last_restore_result,protection,retirement_eligibility,metadata,
                manifest_digest,manifest_json,supersedes,superseded_by,locations)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35)",
            params![
                c.checkpoint_id.to_hex(),
                c.workload_id.to_hex(),
                c.workload_generation,
                c.checkpoint_generation,
                c.parent_checkpoint.map(|i| i.to_hex()),
                c.capture_attempt,
                c.coordinator_epoch,
                c.created_ms,
                c.seal_ms,
                c.source_node,
                c.source_backend,
                serde_json::to_string(&c.frontier)?,
                serde_json::to_string(&c.checkpoint_type)?,
                serde_json::to_string(&c.consistency)?,
                serde_json::to_string(&c.resumability)?,
                c.total_logical_bytes,
                c.total_physical_bytes,
                c.compressed_bytes,
                c.replica_count,
                c.policy_version,
                serde_json::to_string(&c.runtime_descriptor)?,
                serde_json::to_string(&c.hardware_descriptor)?,
                serde_json::to_string(&c.dependencies)?,
                serde_json::to_string(&c.integrity_state)?,
                serde_json::to_string(&c.lifecycle)?,
                c.restore_count,
                c.last_restore_result,
                serde_json::to_string(&c.protection)?,
                serde_json::to_string(&c.retirement_eligibility)?,
                serde_json::to_string(&c.metadata)?,
                c.manifest_digest.as_deref(),
                c.manifest_json.as_deref(),
                c.supersedes.map(|i| i.to_hex()),
                c.superseded_by.map(|i| i.to_hex()),
                serde_json::to_string(&c.durable_locations)?,
            ],
        )
        .map_err(|e| {
            FabricError::PersistenceError(format!(
                "checkpoint {} insertion conflict: {e}",
                c.checkpoint_id
            ))
        })?;
        for comp in &c.components {
            tx.execute(
                "INSERT INTO components(
                    checkpoint_id,component_id,ctype,generation,required,logical_size,
                    storage_repr,content_hash,schema_version,restore_handler,compat,
                    deps,capture_status,restore_status)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                params![
                    c.checkpoint_id.to_hex(),
                    comp.component_id,
                    serde_json::to_string(&comp.component_type)?,
                    comp.generation,
                    comp.required as i32,
                    comp.logical_size,
                    serde_json::to_string(&comp.storage_representation)?,
                    comp.content_hash,
                    comp.schema_version,
                    comp.restore_handler,
                    serde_json::to_string(&comp.compatibility)?,
                    serde_json::to_string(&comp.dependencies)?,
                    comp.capture_status,
                    comp.restore_status,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Update a checkpoint's mutable fields in place (used when a scaffold
    /// inserted at CREATED time gains its final content at commit time).
    /// Component rows are replaced atomically.
    pub fn checkpoint_update(&self, c: &CheckpointObject) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let updated = tx.execute(
            "UPDATE checkpoints SET
                workload_generation=?2, checkpoint_generation=?3, parent_checkpoint=?4,
                capture_attempt=?5, coordinator_epoch=?6, created_ms=?7, seal_ms=?8,
                source_node=?9, source_backend=?10, frontier=?11, ctype=?12,
                consistency=?13, resumability=?14, total_logical_bytes=?15,
                total_physical_bytes=?16, compressed_bytes=?17, replica_count=?18,
                policy_version=?19, runtime_descriptor=?20, hardware_descriptor=?21,
                deps=?22, integrity_state=?23, lifecycle=?24, restore_count=?25,
                last_restore_result=?26, protection=?27, retirement_eligibility=?28,
                metadata=?29, manifest_digest=?30, manifest_json=?31,
                supersedes=?32, superseded_by=?33, locations=?34
             WHERE id=?1",
            params![
                c.checkpoint_id.to_hex(),
                c.workload_generation,
                c.checkpoint_generation,
                c.parent_checkpoint.map(|i| i.to_hex()),
                c.capture_attempt,
                c.coordinator_epoch,
                c.created_ms,
                c.seal_ms,
                c.source_node,
                c.source_backend,
                serde_json::to_string(&c.frontier)?,
                serde_json::to_string(&c.checkpoint_type)?,
                serde_json::to_string(&c.consistency)?,
                serde_json::to_string(&c.resumability)?,
                c.total_logical_bytes,
                c.total_physical_bytes,
                c.compressed_bytes,
                c.replica_count,
                c.policy_version,
                serde_json::to_string(&c.runtime_descriptor)?,
                serde_json::to_string(&c.hardware_descriptor)?,
                serde_json::to_string(&c.dependencies)?,
                serde_json::to_string(&c.integrity_state)?,
                serde_json::to_string(&c.lifecycle)?,
                c.restore_count,
                c.last_restore_result,
                serde_json::to_string(&c.protection)?,
                serde_json::to_string(&c.retirement_eligibility)?,
                serde_json::to_string(&c.metadata)?,
                c.manifest_digest.as_deref(),
                c.manifest_json.as_deref(),
                c.supersedes.map(|i| i.to_hex()),
                c.superseded_by.map(|i| i.to_hex()),
                serde_json::to_string(&c.durable_locations)?,
            ],
        )?;
        if updated != 1 {
            return Err(FabricError::CheckpointNotFound(c.checkpoint_id.to_string()));
        }
        tx.execute(
            "DELETE FROM components WHERE checkpoint_id=?1",
            [c.checkpoint_id.to_hex()],
        )?;
        for comp in &c.components {
            tx.execute(
                "INSERT INTO components(
                    checkpoint_id,component_id,ctype,generation,required,logical_size,
                    storage_repr,content_hash,schema_version,restore_handler,compat,
                    deps,capture_status,restore_status)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
                params![
                    c.checkpoint_id.to_hex(),
                    comp.component_id,
                    serde_json::to_string(&comp.component_type)?,
                    comp.generation,
                    comp.required as i32,
                    comp.logical_size,
                    serde_json::to_string(&comp.storage_representation)?,
                    comp.content_hash,
                    comp.schema_version,
                    comp.restore_handler,
                    serde_json::to_string(&comp.compatibility)?,
                    serde_json::to_string(&comp.dependencies)?,
                    comp.capture_status,
                    comp.restore_status,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Update lifecycle state and related bookkeeping of a checkpoint.
    pub fn checkpoint_set_lifecycle(
        &self,
        id: &Id,
        lifecycle: LifecycleState,
        integrity: IntegrityState,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        let n = conn.execute(
            "UPDATE checkpoints SET lifecycle=?2, integrity_state=?3 WHERE id=?1",
            params![
                id.to_hex(),
                serde_json::to_string(&lifecycle)?,
                serde_json::to_string(&integrity)?
            ],
        )?;
        if n == 0 {
            return Err(FabricError::CheckpointNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Atomically finalize a physically persisted capture. The checkpoint,
    /// reciprocal supersession links, lineage, workload counter/resumability,
    /// and recovery journal become visible in one SQLite commit.
    pub fn capture_commit(&self, id: &Id, attempt_id: &str) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let (
            workload_hex,
            workload_generation,
            checkpoint_generation,
            lifecycle,
            resumability,
            locations,
            manifest_digest,
        ): (String, u64, u64, String, String, String, Option<String>) = tx
            .query_row(
                "SELECT workload_id,workload_generation,checkpoint_generation,lifecycle,
                        resumability,locations,manifest_digest FROM checkpoints WHERE id=?1",
                [id.to_hex()],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                        r.get(6)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| FabricError::CheckpointNotFound(id.to_string()))?;
        let current = LifecycleState::from_str(&lifecycle);
        if current != LifecycleState::Persisting && current != LifecycleState::Available {
            return Err(FabricError::InvalidLifecycleTransition {
                from: current.as_str().into(),
                to: LifecycleState::Available.as_str().into(),
            });
        }
        let parsed_locations: Vec<crate::checkpoint::DurableLocation> =
            serde_json::from_str(&locations).map_err(|e| FabricError::Internal(e.to_string()))?;
        if parsed_locations.is_empty() || manifest_digest.as_deref().unwrap_or_default().is_empty()
        {
            return Err(FabricError::IncompleteCheckpoint(format!(
                "persisted checkpoint {id} lacks a durable location or manifest anchor"
            )));
        }
        let (current_workload_generation, current_checkpoint_generation): (u64, u64) = tx
            .query_row(
                "SELECT generation,checkpoint_generation FROM workloads WHERE id=?1",
                [&workload_hex],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| FabricError::WorkloadNotFound(workload_hex.clone()))?;
        if current == LifecycleState::Persisting
            && current_workload_generation != workload_generation
        {
            return Err(FabricError::GenerationMismatch(format!(
                "capture generation {workload_generation} is stale; workload is generation {current_workload_generation}"
            )));
        }

        let prev: Option<String> = tx
            .query_row(
                "SELECT id FROM checkpoints WHERE workload_id=?1 AND checkpoint_generation<?2
                 AND lifecycle IN ('\"AVAILABLE\"','\"RESTORED\"')
                 ORDER BY checkpoint_generation DESC LIMIT 1",
                params![&workload_hex, checkpoint_generation],
                |r| r.get(0),
            )
            .optional()?;
        let next: Option<String> = tx
            .query_row(
                "SELECT id FROM checkpoints WHERE workload_id=?1 AND checkpoint_generation>?2
                 AND lifecycle IN ('\"AVAILABLE\"','\"RESTORED\"')
                 ORDER BY checkpoint_generation ASC LIMIT 1",
                params![&workload_hex, checkpoint_generation],
                |r| r.get(0),
            )
            .optional()?;
        tx.execute(
            "UPDATE checkpoints SET lifecycle='\"AVAILABLE\"', integrity_state='\"valid\"',
                    supersedes=?2, superseded_by=?3 WHERE id=?1",
            params![id.to_hex(), prev.as_deref(), next.as_deref()],
        )?;
        if let Some(p) = &prev {
            tx.execute(
                "UPDATE checkpoints SET superseded_by=?2 WHERE id=?1",
                params![p, id.to_hex()],
            )?;
        }
        if let Some(n) = &next {
            tx.execute(
                "UPDATE checkpoints SET supersedes=?2 WHERE id=?1",
                params![n, id.to_hex()],
            )?;
        }
        if let Some(p) = &prev {
            tx.execute(
                "INSERT INTO lineage(relation,workload_id,checkpoint_id,other_checkpoint,ts_ms,detail)
                 SELECT ?1,?2,?3,?4,?5,?6
                 WHERE NOT EXISTS(SELECT 1 FROM lineage WHERE relation=?1 AND checkpoint_id=?3 AND other_checkpoint=?4)",
                params![
                    LineageRelation::Supersedes.as_str(),
                    &workload_hex,
                    id.to_hex(),
                    p,
                    now_ms(),
                    format!("generation {checkpoint_generation} supersedes {p}"),
                ],
            )?;
        }
        if checkpoint_generation >= current_checkpoint_generation {
            tx.execute(
                "UPDATE workloads SET checkpoint_generation=?2,resumability=?3 WHERE id=?1",
                params![&workload_hex, checkpoint_generation, resumability],
            )?;
        }
        tx.execute(
            "INSERT OR IGNORE INTO journal(kind,key,state,detail,ts_ms)
             VALUES('capture',?1,'db_committed','metadata committed atomically',?2)",
            params![attempt_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Atomically commit restored execution state, authority, checkpoint
    /// lifecycle/counts, lineage, and the recovery commit marker.
    pub fn restore_commit(
        &self,
        checkpoint_id: &Id,
        attempt_id: &str,
        target_node: &str,
        fence_token: &str,
        migration: bool,
        rollback: bool,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let (workload_hex, checkpoint_generation, lifecycle): (String, u64, String) = tx
            .query_row(
                "SELECT workload_id,checkpoint_generation,lifecycle FROM checkpoints WHERE id=?1",
                [checkpoint_id.to_hex()],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?
            .ok_or_else(|| FabricError::CheckpointNotFound(checkpoint_id.to_string()))?;
        let current = LifecycleState::from_str(&lifecycle);
        current.transition(LifecycleState::Restored)?;
        let (generation, execution_epoch): (u64, u64) = tx
            .query_row(
                "SELECT generation,execution_epoch FROM workloads WHERE id=?1",
                [&workload_hex],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| FabricError::WorkloadNotFound(workload_hex.clone()))?;
        let new_generation = generation.saturating_add(1);
        let new_epoch = execution_epoch
            .saturating_add(1)
            .saturating_add(u64::from(migration));
        tx.execute(
            "UPDATE workloads SET generation=?2,checkpoint_generation=?3,execution_epoch=?4,
                    active_node=?5,fence_token=?6,fence_epoch=?4 WHERE id=?1",
            params![
                &workload_hex,
                new_generation,
                checkpoint_generation,
                new_epoch,
                target_node,
                fence_token,
            ],
        )?;
        tx.execute(
            "UPDATE checkpoints SET lifecycle='\"RESTORED\"',integrity_state='\"valid\"',
                    restore_count=restore_count+1,last_restore_result='restored' WHERE id=?1",
            [checkpoint_id.to_hex()],
        )?;
        if rollback {
            tx.execute(
                "INSERT INTO lineage(relation,workload_id,checkpoint_id,ts_ms,detail)
                 VALUES(?1,?2,?3,?4,?5)",
                params![
                    LineageRelation::RollbackOf.as_str(),
                    &workload_hex,
                    checkpoint_id.to_hex(),
                    now_ms(),
                    format!("rollback: workload generation {generation} -> {new_generation} via checkpoint {checkpoint_generation}"),
                ],
            )?;
        }
        if migration {
            tx.execute(
                "INSERT INTO lineage(relation,workload_id,checkpoint_id,ts_ms,detail)
                 VALUES(?1,?2,?3,?4,?5)",
                params![
                    LineageRelation::MigratedFrom.as_str(),
                    &workload_hex,
                    checkpoint_id.to_hex(),
                    now_ms(),
                    format!("migrated to {target_node}"),
                ],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO journal(kind,key,state,detail,ts_ms)
                 VALUES('migration',?1,'authority_transferred','authority transferred atomically',?2)",
                params![attempt_id, now_ms()],
            )?;
        }
        tx.execute(
            "INSERT OR IGNORE INTO journal(kind,key,state,detail,ts_ms)
             VALUES('restore',?1,'generation_committed','generation committed atomically',?2)",
            params![attempt_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Highest committed checkpoint generation for a workload.
    pub fn checkpoint_max_generation(&self, workload_id: &Id) -> FabricResult<u64> {
        let conn = self.inner.lock().unwrap();
        let v: i64 = conn.query_row(
            "SELECT COALESCE(MAX(checkpoint_generation),0) FROM checkpoints WHERE workload_id=?1",
            [workload_id.to_hex()],
            |r| r.get(0),
        )?;
        Ok(v as u64)
    }

    /// The most recent AVAILABLE checkpoint id for a workload, if any.
    pub fn checkpoint_max_available(&self, workload_id: &Id) -> FabricResult<Option<Id>> {
        let conn = self.inner.lock().unwrap();
        let v: Option<String> = conn
            .query_row(
                "SELECT id FROM checkpoints WHERE workload_id=?1 AND lifecycle IN ('\"AVAILABLE\"','\"RESTORED\"')
                 ORDER BY checkpoint_generation DESC LIMIT 1",
                [workload_id.to_hex()],
                |r| r.get(0),
            )
            .optional()?;
        match v {
            Some(s) => Ok(Some(Id::from_hex(&s)?)),
            None => Ok(None),
        }
    }

    /// Update supersession links after a new generation commits.
    pub fn checkpoint_set_superseded(
        &self,
        id: &Id,
        superseded_by: Option<Id>,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE checkpoints SET superseded_by=?2 WHERE id=?1",
            params![id.to_hex(), superseded_by.map(|i| i.to_hex())],
        )?;
        Ok(())
    }

    /// Record an explicit supersession link (this checkpoint supersedes the given one).
    pub fn checkpoint_set_supersedes(&self, id: &Id, supersedes: Option<Id>) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE checkpoints SET supersedes=?2 WHERE id=?1",
            params![id.to_hex(), supersedes.map(|i| i.to_hex())],
        )?;
        Ok(())
    }

    pub fn checkpoint_get(&self, id: &Id) -> FabricResult<Option<CheckpointObject>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT * FROM checkpoints WHERE id=?1")?;
        let row = stmt
            .query_row([id.to_hex()], |r| row_to_checkpoint(r))
            .optional()?;
        drop(stmt);
        match row {
            Some(mut c) => {
                c.components = self.checkpoint_components_locked(&conn, id)?;
                Ok(Some(c))
            }
            None => Ok(None),
        }
    }

    fn checkpoint_components_locked(
        &self,
        conn: &Connection,
        id: &Id,
    ) -> FabricResult<Vec<crate::checkpoint::ComponentEntry>> {
        let mut stmt =
            conn.prepare("SELECT * FROM components WHERE checkpoint_id=?1 ORDER BY component_id")?;
        let rows = stmt.query_map([id.to_hex()], |r| row_to_component(r))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    pub fn checkpoint_list(&self, workload_id: Option<&Id>) -> FabricResult<Vec<CheckpointObject>> {
        let conn = self.inner.lock().unwrap();
        let mut out = Vec::new();
        match workload_id {
            None => {
                let mut stmt = conn.prepare("SELECT * FROM checkpoints ORDER BY created_ms")?;
                let rows = stmt.query_map([], |r| row_to_checkpoint(r))?;
                for r in rows {
                    let c = r?;
                    out.push(c);
                }
            }
            Some(wid) => {
                let mut stmt = conn.prepare(
                    "SELECT * FROM checkpoints WHERE workload_id=?1 ORDER BY checkpoint_generation",
                )?;
                let rows = stmt.query_map([wid.to_hex()], |r| row_to_checkpoint(r))?;
                for r in rows {
                    out.push(r?);
                }
            }
        }
        for c in &mut out {
            c.components = self.checkpoint_components_locked(&conn, &c.checkpoint_id)?;
        }
        Ok(out)
    }

    pub fn checkpoint_set_digest(
        &self,
        id: &Id,
        digest: &str,
        manifest_json: &str,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE checkpoints SET manifest_digest=?2, manifest_json=?3 WHERE id=?1",
            params![id.to_hex(), digest, manifest_json],
        )?;
        Ok(())
    }

    pub fn checkpoint_record_restore(&self, id: &Id, result: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE checkpoints SET restore_count=restore_count+1, last_restore_result=?2 WHERE id=?1",
            params![id.to_hex(), result],
        )?;
        Ok(())
    }

    pub fn checkpoint_set_protection(&self, id: &Id, p: ProtectionState) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE checkpoints SET protection=?2, retirement_eligibility=?3 WHERE id=?1",
            params![
                id.to_hex(),
                serde_json::to_string(&p)?,
                serde_json::to_string(&RetirementEligibility::Protected)?
            ],
        )?;
        Ok(())
    }

    pub fn checkpoint_retire(&self, id: &Id) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let (lifecycle, protection): (String, String) = tx
            .query_row(
                "SELECT lifecycle, protection FROM checkpoints WHERE id=?1",
                [id.to_hex()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| FabricError::CheckpointNotFound(id.to_string()))?;
        let ls = LifecycleState::from_str(&lifecycle);
        let prot: ProtectionState =
            serde_json::from_str(&protection).map_err(|e| FabricError::Internal(e.to_string()))?;
        if prot == ProtectionState::Pinned {
            return Err(FabricError::PolicyViolation(
                "pinned checkpoint cannot be retired".into(),
            ));
        }
        ls.transition(LifecycleState::Retired)?;
        tx.execute(
            "UPDATE checkpoints SET lifecycle='\"RETIRED\"', retirement_eligibility=?2,
             locations='[]', replica_count=0 WHERE id=?1",
            params![
                id.to_hex(),
                serde_json::to_string(&RetirementEligibility::NotEligible)?
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn checkpoint_set_integrity(&self, id: &Id, state: IntegrityState) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE checkpoints SET integrity_state=?2 WHERE id=?1",
            params![id.to_hex(), serde_json::to_string(&state)?],
        )?;
        Ok(())
    }

    pub fn checkpoint_add_location(
        &self,
        id: &Id,
        location: &crate::checkpoint::DurableLocation,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let locations_json: String = tx.query_row(
            "SELECT locations FROM checkpoints WHERE id=?1",
            [id.to_hex()],
            |r| r.get(0),
        )?;
        let mut locations: Vec<crate::checkpoint::DurableLocation> =
            serde_json::from_str(&locations_json)
                .map_err(|e| FabricError::Internal(e.to_string()))?;
        if !locations.iter().any(|l| l.node == location.node) {
            locations.push(location.clone());
        }
        tx.execute(
            "UPDATE checkpoints SET locations=?2, replica_count=?3 WHERE id=?1",
            params![
                id.to_hex(),
                serde_json::to_string(&locations)?,
                locations.len() as i64
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn checkpoint_remove_location(&self, id: &Id, node: &str) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let locations_json: String = tx.query_row(
            "SELECT locations FROM checkpoints WHERE id=?1",
            [id.to_hex()],
            |r| r.get(0),
        )?;
        let mut locations: Vec<crate::checkpoint::DurableLocation> =
            serde_json::from_str(&locations_json)
                .map_err(|e| FabricError::Internal(e.to_string()))?;
        locations.retain(|l| l.node != node);
        tx.execute(
            "UPDATE checkpoints SET locations=?2, replica_count=?3 WHERE id=?1",
            params![
                id.to_hex(),
                serde_json::to_string(&locations)?,
                locations.len() as i64
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    // ---------------- lineage ----------------

    pub fn lineage_append(
        &self,
        relation: LineageRelation,
        workload_id: Option<&Id>,
        checkpoint_id: Option<&Id>,
        other_workload: Option<&Id>,
        other_checkpoint: Option<&Id>,
        detail: &str,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO lineage(relation,workload_id,checkpoint_id,other_workload,other_checkpoint,ts_ms,detail)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                relation.as_str(),
                workload_id.map(|i| i.to_hex()),
                checkpoint_id.map(|i| i.to_hex()),
                other_workload.map(|i| i.to_hex()),
                other_checkpoint.map(|i| i.to_hex()),
                now_ms(),
                detail,
            ],
        )?;
        Ok(())
    }

    pub fn lineage_query(
        &self,
        workload_id: Option<&Id>,
        checkpoint_id: Option<&Id>,
    ) -> FabricResult<Vec<crate::lineage::LineageRecord>> {
        let conn = self.inner.lock().unwrap();
        let mut out = Vec::new();
        match (workload_id, checkpoint_id) {
            (Some(w), None) => {
                let mut stmt = conn.prepare(
                    "SELECT seq,relation,workload_id,checkpoint_id,other_workload,other_checkpoint,ts_ms,detail
                     FROM lineage WHERE workload_id=?1 OR other_workload=?1 ORDER BY seq",
                )?;
                let rows = stmt.query_map([w.to_hex()], |r| row_to_lineage(r))?;
                for r in rows {
                    out.push(r?);
                }
            }
            (None, Some(c)) => {
                let mut stmt = conn.prepare(
                    "SELECT seq,relation,workload_id,checkpoint_id,other_workload,other_checkpoint,ts_ms,detail
                     FROM lineage WHERE checkpoint_id=?1 OR other_checkpoint=?1 ORDER BY seq",
                )?;
                let rows = stmt.query_map([c.to_hex()], |r| row_to_lineage(r))?;
                for r in rows {
                    out.push(r?);
                }
            }
            _ => {
                let mut stmt = conn.prepare(
                    "SELECT seq,relation,workload_id,checkpoint_id,other_workload,other_checkpoint,ts_ms,detail
                     FROM lineage ORDER BY seq",
                )?;
                let rows = stmt.query_map([], |r| row_to_lineage(r))?;
                for r in rows {
                    out.push(r?);
                }
            }
        }
        Ok(out)
    }

    // ---------------- attempts ----------------

    pub fn attempt_begin(
        &self,
        id: &str,
        kind: &str,
        checkpoint_id: Option<&Id>,
        workload_id: Option<&Id>,
        node: &str,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO attempts(id,kind,checkpoint_id,workload_id,node,state,started_ms)
             VALUES(?1,?2,?3,?4,?5,'ACTIVE',?6)",
            params![
                id,
                kind,
                checkpoint_id.map(|i| i.to_hex()),
                workload_id.map(|i| i.to_hex()),
                node,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn attempt_finish(
        &self,
        id: &str,
        state: &str,
        result: Option<&str>,
        error: Option<&str>,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE attempts SET state=?2, result=?3, error=?4, finished_ms=?5 WHERE id=?1",
            params![id, state, result, error, now_ms()],
        )?;
        Ok(())
    }

    pub fn attempt_get(&self, id: &str) -> FabricResult<Option<crate::capture::AttemptRecord>> {
        let conn = self.inner.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id,kind,checkpoint_id,workload_id,node,state,started_ms,finished_ms,result,error
                 FROM attempts WHERE id=?1",
                [id],
                |r| {
                    Ok(crate::capture::AttemptRecord {
                        id: r.get(0)?,
                        kind: r.get(1)?,
                        checkpoint_id: r
                            .get::<_, Option<String>>(2)?
                            .map(|s| Id::from_hex(&s))
                            .transpose()
                            .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?,
                        workload_id: r
                            .get::<_, Option<String>>(3)?
                            .map(|s| Id::from_hex(&s))
                            .transpose()
                            .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?,
                        node: r.get(4)?,
                        state: r.get(5)?,
                        started_ms: r.get(6)?,
                        finished_ms: r.get(7)?,
                        result: r.get(8)?,
                        error: r.get(9)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    pub fn attempts_in_state(
        &self,
        state: &str,
    ) -> FabricResult<Vec<crate::capture::AttemptRecord>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT * FROM attempts WHERE state=?1 ORDER BY started_ms")?;
        let rows = stmt.query_map([state], |r| {
            Ok(crate::capture::AttemptRecord {
                id: r.get(0)?,
                kind: r.get(1)?,
                checkpoint_id: r
                    .get::<_, Option<String>>(2)?
                    .map(|s| Id::from_hex(&s))
                    .transpose()
                    .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?,
                workload_id: r
                    .get::<_, Option<String>>(3)?
                    .map(|s| Id::from_hex(&s))
                    .transpose()
                    .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?,
                node: r.get(4)?,
                state: r.get(5)?,
                started_ms: r.get(6)?,
                finished_ms: r.get(7)?,
                result: r.get(8)?,
                error: r.get(9)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ---------------- nodes ----------------

    pub fn node_register(
        &self,
        id: &str,
        addr: &str,
        boot_id: &str,
        resources: &str,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let existing: Option<(String, String)> = tx
            .query_row("SELECT boot_id, status FROM nodes WHERE id=?1", [id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        match existing {
            Some((old_boot, status)) if old_boot != boot_id => {
                // Stale registration with a different boot id: only replace if the
                // old registration is already stale.
                if status == "STALE" || status == "RETIRED" {
                    tx.execute(
                        "UPDATE nodes SET boot_id=?2, addr=?3, status='ACTIVE', registered_ms=?4, last_heartbeat_ms=?4, resources=?5 WHERE id=?1",
                        params![id, boot_id, addr, now_ms(), resources],
                    )?;
                } else {
                    return Err(FabricError::FencingFailure(format!(
                        "node {id} is already registered with a different boot id"
                    )));
                }
            }
            _ => {
                tx.execute(
                    "INSERT INTO nodes(id,addr,boot_id,status,registered_ms,last_heartbeat_ms,resources)
                     VALUES(?1,?2,?3,'ACTIVE',?4,?4,?5)
                     ON CONFLICT(id) DO UPDATE SET
                        addr=excluded.addr, boot_id=excluded.boot_id, status='ACTIVE',
                        last_heartbeat_ms=excluded.last_heartbeat_ms, resources=excluded.resources",
                    params![id, addr, boot_id, now_ms(), resources],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn node_heartbeat(&self, id: &str, resources: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        let n = conn.execute(
            "UPDATE nodes SET last_heartbeat_ms=?2, status='ACTIVE', resources=?3 WHERE id=?1",
            params![id, now_ms(), resources],
        )?;
        if n == 0 {
            return Err(FabricError::Internal(format!(
                "heartbeat for unknown node {id}"
            )));
        }
        Ok(())
    }

    pub fn node_set_status(&self, id: &str, status: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE nodes SET status=?2 WHERE id=?1",
            params![id, status],
        )?;
        Ok(())
    }

    pub fn node_list(&self) -> FabricResult<Vec<crate::node::NodeRecord>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id,addr,boot_id,status,registered_ms,last_heartbeat_ms,resources FROM nodes ORDER BY id")?;
        let rows = stmt.query_map([], |r| {
            Ok(crate::node::NodeRecord {
                id: r.get(0)?,
                addr: r.get(1)?,
                boot_id: r.get(2)?,
                status: r.get(3)?,
                registered_ms: r.get(4)?,
                last_heartbeat_ms: r.get(5)?,
                resources: r.get(6)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// Validate that a node-originated request belongs to the currently
    /// registered boot instance, not merely to a caller that knows the node id.
    pub fn node_validate_identity(&self, id: &str, boot_id: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        let registered: Option<(String, String)> = conn
            .query_row("SELECT boot_id,status FROM nodes WHERE id=?1", [id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;
        match registered {
            Some((registered_boot, status)) if registered_boot == boot_id && status == "ACTIVE" => {
                Ok(())
            }
            Some((_, status)) => Err(FabricError::FencingFailure(format!(
                "node {id} boot identity is stale or status is {status}"
            ))),
            None => Err(FabricError::FencingFailure(format!(
                "node {id} is not registered"
            ))),
        }
    }

    /// Rebind a checkpoint discovered on a restarted node to its new transient
    /// node id. Locations referring to the same physical commit path are
    /// replaced rather than counted as additional replicas.
    pub fn checkpoint_rebind_location(
        &self,
        id: &Id,
        node: &str,
        data_dir: &Path,
    ) -> FabricResult<()> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let locations_json: Option<String> = tx
            .query_row(
                "SELECT locations FROM checkpoints WHERE id=?1",
                [id.to_hex()],
                |r| r.get(0),
            )
            .optional()?;
        let Some(locations_json) = locations_json else {
            return Ok(());
        };
        let commit = data_dir.join("checkpoints").join(id.to_hex());
        let mut locations: Vec<crate::checkpoint::DurableLocation> =
            serde_json::from_str(&locations_json)
                .map_err(|e| FabricError::Internal(e.to_string()))?;
        let verified = locations
            .iter()
            .any(|l| (Path::new(&l.path) == commit || l.node == node) && l.verified);
        locations.retain(|l| Path::new(&l.path) != commit && l.node != node);
        locations.push(crate::checkpoint::DurableLocation {
            node: node.to_string(),
            path: commit.to_string_lossy().to_string(),
            // Full verification occurs against the coordinator-held digest
            // before restore or an explicit verify can mark it trusted.
            verified,
        });
        tx.execute(
            "UPDATE checkpoints SET locations=?2,replica_count=?3 WHERE id=?1",
            params![
                id.to_hex(),
                serde_json::to_string(&locations)?,
                locations.len() as u64,
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Mark nodes whose heartbeat is older than `stale_before_ms` as STALE.
    /// Returns the stale node ids.
    pub fn nodes_sweep_stale(&self, stale_before_ms: u64) -> FabricResult<Vec<String>> {
        let mut conn = self.inner.lock().unwrap();
        let tx = conn.transaction()?;
        let mut stale = Vec::new();
        {
            let mut stmt = tx
                .prepare("SELECT id FROM nodes WHERE status='ACTIVE' AND last_heartbeat_ms < ?1")?;
            let rows = stmt.query_map([stale_before_ms as i64], |r| r.get::<_, String>(0))?;
            for r in rows {
                stale.push(r?);
            }
        }
        for id in &stale {
            tx.execute(
                "UPDATE nodes SET status='STALE' WHERE id=?1 AND status='ACTIVE'",
                [id],
            )?;
        }
        tx.commit()?;
        Ok(stale)
    }

    pub fn node_delete(&self, id: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute("DELETE FROM nodes WHERE id=?1", [id])?;
        Ok(())
    }

    // ---------------- reservations ----------------

    /// Create an active reservation of the given kind scoped by workload or checkpoint.
    /// Returns an error on conflict (duplicate active reservation for the same scope).
    pub fn reservation_create(
        &self,
        id: &str,
        kind: &str,
        workload_id: Option<&Id>,
        checkpoint_id: Option<&Id>,
        node: &str,
        ttl_ms: u64,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        let res = conn.execute(
            "INSERT INTO reservations(id,kind,workload_id,checkpoint_id,node,state,created_ms,expires_ms)
             VALUES(?1,?2,?3,?4,?5,'ACTIVE',?6,?7)",
            params![
                id,
                kind,
                workload_id.map(|i| i.to_hex()),
                checkpoint_id.map(|i| i.to_hex()),
                node,
                now_ms(),
                now_ms() + ttl_ms,
            ],
        );
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.to_string().contains("UNIQUE") {
                    Err(FabricError::ReservationConflict(format!(
                        "an active {kind} reservation already exists for this scope"
                    )))
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub fn reservation_release(&self, id: &str) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "UPDATE reservations SET state='RELEASED' WHERE id=?1 AND state='ACTIVE'",
            [id],
        )?;
        Ok(())
    }

    pub fn reservation_active_for(
        &self,
        kind: &str,
        workload_id: Option<&Id>,
        checkpoint_id: Option<&Id>,
    ) -> FabricResult<Option<String>> {
        let conn = self.inner.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id FROM reservations WHERE kind=?1 AND state='ACTIVE'
                 AND (?2 IS NULL OR workload_id=?2) AND (?3 IS NULL OR checkpoint_id=?3)",
                params![
                    kind,
                    workload_id.map(|i| i.to_hex()),
                    checkpoint_id.map(|i| i.to_hex())
                ],
                |r| r.get::<_, String>(0),
            )
            .optional()?;
        Ok(row)
    }

    pub fn reservations_expire(&self, before_ms: u64) -> FabricResult<u64> {
        let conn = self.inner.lock().unwrap();
        let n = conn.execute(
            "UPDATE reservations SET state='EXPIRED' WHERE state='ACTIVE' AND expires_ms < ?1",
            [before_ms as i64],
        )?;
        Ok(n as u64)
    }

    // ---------------- audit ----------------

    pub fn audit_append(
        &self,
        actor: &str,
        op: &str,
        workload_id: Option<&Id>,
        checkpoint_id: Option<&Id>,
        result: AuditResult,
        detail: &str,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT INTO audit(ts_ms,actor,op,workload_id,checkpoint_id,result,detail)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![
                now_ms(),
                actor,
                op,
                workload_id.map(|i| i.to_hex()),
                checkpoint_id.map(|i| i.to_hex()),
                result.as_str(),
                detail,
            ],
        )?;
        Ok(())
    }

    pub fn audit_query(
        &self,
        since_ms: Option<u64>,
        limit: usize,
    ) -> FabricResult<Vec<AuditRecord>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq,ts_ms,actor,op,workload_id,checkpoint_id,result,detail
             FROM audit WHERE (?1 IS NULL OR ts_ms >= ?1) ORDER BY seq DESC LIMIT ?2",
        )?;
        let limit_i64 = (limit as i64).max(1);
        let rows = stmt.query_map(params![since_ms.map(|v| v as i64), limit_i64], |r| {
            Ok(AuditRecord {
                seq: r.get(0)?,
                ts_ms: r.get(1)?,
                actor: r.get(2)?,
                op: r.get(3)?,
                workload_id: r
                    .get::<_, Option<String>>(4)?
                    .and_then(|s| Id::from_hex(&s).ok()),
                checkpoint_id: r
                    .get::<_, Option<String>>(5)?
                    .and_then(|s| Id::from_hex(&s).ok()),
                result: AuditResult::parse_str(&r.get::<_, String>(6)?),
                detail: r.get(7)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ---------------- journal ----------------

    /// Append a recovery-journal record (durable, unique per (kind,key,state)).
    pub fn journal_append(
        &self,
        kind: &str,
        key: &str,
        state: &str,
        detail: &str,
    ) -> FabricResult<()> {
        let conn = self.inner.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO journal(kind,key,state,detail,ts_ms)
             VALUES(?1,?2,?3,?4,?5)",
            params![kind, key, state, detail, now_ms()],
        )?;
        Ok(())
    }

    /// Whether a journal state exists.
    pub fn journal_has(&self, kind: &str, key: &str, state: &str) -> FabricResult<bool> {
        let conn = self.inner.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM journal WHERE kind=?1 AND key=?2 AND state=?3",
            params![kind, key, state],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }

    /// All journal states for a (kind, key) pair, ordered by sequence.
    pub fn journal_states(
        &self,
        kind: &str,
        key: &str,
    ) -> FabricResult<Vec<(String, String, u64)>> {
        let conn = self.inner.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT state, detail, ts_ms FROM journal WHERE kind=?1 AND key=?2 ORDER BY seq",
        )?;
        let rows = stmt.query_map(params![kind, key], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)? as u64,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ---------------- stats ----------------

    pub fn stats(&self) -> FabricResult<crate::coordinator::Stats> {
        let conn = self.inner.lock().unwrap();
        let count = |sql: &str, param: Option<&str>| -> FabricResult<i64> {
            match param {
                None => Ok(conn.query_row(sql, [], |r| r.get(0))?),
                Some(p) => Ok(conn.query_row(sql, [p], |r| r.get(0))?),
            }
        };
        Ok(crate::coordinator::Stats {
            workloads: count("SELECT COUNT(*) FROM workloads", None)? as u64,
            checkpoints: count("SELECT COUNT(*) FROM checkpoints", None)? as u64,
            available: count(
                "SELECT COUNT(*) FROM checkpoints WHERE lifecycle='\"AVAILABLE\"' OR lifecycle='\"RESTORED\"'",
                None,
            )? as u64,
            failed: count("SELECT COUNT(*) FROM checkpoints WHERE lifecycle='\"FAILED\"'", None)? as u64,
            retired: count("SELECT COUNT(*) FROM checkpoints WHERE lifecycle='\"RETIRED\"'", None)? as u64,
            active_nodes: count("SELECT COUNT(*) FROM nodes WHERE status='ACTIVE'", None)? as u64,
            stale_nodes: count("SELECT COUNT(*) FROM nodes WHERE status='STALE'", None)? as u64,
            active_attempts: count("SELECT COUNT(*) FROM attempts WHERE state='ACTIVE'", None)? as u64,
            audit_records: count("SELECT COUNT(*) FROM audit", None)? as u64,
            total_logical_bytes: count("SELECT COALESCE(SUM(total_logical_bytes),0) FROM checkpoints", None)? as u64,
            total_physical_bytes: count("SELECT COALESCE(SUM(total_physical_bytes),0) FROM checkpoints", None)? as u64,
        })
    }
}

// ---------------- row mappers ----------------

pub(crate) type Row<'a> = &'a rusqlite::Row<'a>;

pub(crate) fn row_to_workload(r: Row) -> rusqlite::Result<Workload> {
    Ok(Workload {
        workload_id: Id::from_hex(&r.get::<_, String>(0)?).map_err(sql_err)?,
        workload_generation: r.get(1)?,
        owner: r.get(2)?,
        class: r.get(3)?,
        created_ms: r.get(4)?,
        execution_epoch: r.get(5)?,
        active_node: r.get(6)?,
        backend_class: r.get(7)?,
        checkpoint_generation: r.get(8)?,
        parent_workload: r
            .get::<_, Option<String>>(9)?
            .map(|s| Id::from_hex(&s))
            .transpose()
            .map_err(|e| rusqlite::Error::InvalidColumnName(e.to_string()))?,
        fork_generation: r.get(10)?,
        policy_version: r.get(11)?,
        metadata: serde_json::from_str(&r.get::<_, String>(12)?).map_err(sql_err)?,
        state_schema_version: r.get(13)?,
        runtime: serde_json::from_str(&r.get::<_, String>(14)?).map_err(sql_err)?,
        resumability_class: serde_json::from_str(&r.get::<_, String>(15)?).map_err(sql_err)?,
        protection: serde_json::from_str(&r.get::<_, String>(16)?).map_err(sql_err)?,
        single_active: r.get::<_, i32>(17)? != 0,
        fence_token: r.get(18)?,
        fence_epoch: r.get(19)?,
    })
}

fn sql_err<E: std::fmt::Display>(e: E) -> rusqlite::Error {
    rusqlite::Error::InvalidColumnName(e.to_string())
}

pub(crate) fn row_to_checkpoint(r: Row) -> rusqlite::Result<CheckpointObject> {
    let sup: Option<String> = r.get(32)?;
    let sup_by: Option<String> = r.get(33)?;
    let locations: Vec<crate::checkpoint::DurableLocation> =
        serde_json::from_str(&r.get::<_, String>(34)?).map_err(sql_err)?;
    let sup_id: Option<Id> = sup.map(|s| Id::from_hex(&s)).transpose().map_err(sql_err)?;
    let sup_by_id: Option<Id> = sup_by
        .map(|s| Id::from_hex(&s))
        .transpose()
        .map_err(sql_err)?;
    Ok(CheckpointObject {
        checkpoint_id: Id::from_hex(&r.get::<_, String>(0)?).map_err(sql_err)?,
        workload_id: Id::from_hex(&r.get::<_, String>(1)?).map_err(sql_err)?,
        workload_generation: r.get(2)?,
        checkpoint_generation: r.get(3)?,
        parent_checkpoint: r
            .get::<_, Option<String>>(4)?
            .map(|s| Id::from_hex(&s))
            .transpose()
            .map_err(sql_err)?,
        capture_attempt: r.get(5)?,
        coordinator_epoch: r.get(6)?,
        created_ms: r.get(7)?,
        seal_ms: r.get(8)?,
        source_node: r.get(9)?,
        source_backend: r.get(10)?,
        frontier: serde_json::from_str(&r.get::<_, String>(11)?).map_err(sql_err)?,
        checkpoint_type: serde_json::from_str(&r.get::<_, String>(12)?).map_err(sql_err)?,
        consistency: serde_json::from_str(&r.get::<_, String>(13)?).map_err(sql_err)?,
        resumability: serde_json::from_str(&r.get::<_, String>(14)?).map_err(sql_err)?,
        total_logical_bytes: r.get(15)?,
        total_physical_bytes: r.get(16)?,
        compressed_bytes: r.get(17)?,
        replica_count: r.get(18)?,
        policy_version: r.get(19)?,
        runtime_descriptor: serde_json::from_str(&r.get::<_, String>(20)?).map_err(sql_err)?,
        hardware_descriptor: serde_json::from_str(&r.get::<_, String>(21)?).map_err(sql_err)?,
        dependencies: serde_json::from_str(&r.get::<_, String>(22)?).map_err(sql_err)?,
        integrity_state: IntegrityState::from_str(&r.get::<_, String>(23)?),
        lifecycle: LifecycleState::from_str(&r.get::<_, String>(24)?),
        restore_count: r.get(25)?,
        last_restore_result: r.get(26)?,
        protection: serde_json::from_str(&r.get::<_, String>(27)?).map_err(sql_err)?,
        retirement_eligibility: serde_json::from_str(&r.get::<_, String>(28)?).map_err(sql_err)?,
        metadata: serde_json::from_str(&r.get::<_, String>(29)?).map_err(sql_err)?,
        manifest_digest: r.get(30)?,
        manifest_json: r.get(31)?,
        supersedes: sup_id,
        superseded_by: sup_by_id,
        durable_locations: locations,
        lineage_parents: {
            let parent = r
                .get::<_, Option<String>>(4)?
                .map(|s| Id::from_hex(&s))
                .transpose()
                .map_err(sql_err)?;
            let mut v = Vec::new();
            if let Some(p) = parent {
                v.push(p);
            }
            if let Some(p) = sup_id {
                v.push(p);
            }
            v
        },
        lineage_children: match sup_by_id {
            Some(s) => vec![s],
            None => Vec::new(),
        },
        components: Vec::new(),
    })
}

pub(crate) fn row_to_component(r: Row) -> rusqlite::Result<crate::checkpoint::ComponentEntry> {
    Ok(crate::checkpoint::ComponentEntry {
        component_id: r.get(1)?,
        component_type: serde_json::from_str(&r.get::<_, String>(2)?).map_err(sql_err)?,
        generation: r.get(3)?,
        required: r.get::<_, i32>(4)? != 0,
        logical_size: r.get(5)?,
        storage_representation: serde_json::from_str(&r.get::<_, String>(6)?).map_err(sql_err)?,
        content_hash: r.get(7)?,
        schema_version: r.get(8)?,
        restore_handler: r.get(9)?,
        compatibility: serde_json::from_str(&r.get::<_, String>(10)?).map_err(sql_err)?,
        dependencies: serde_json::from_str(&r.get::<_, String>(11)?).map_err(sql_err)?,
        capture_status: r.get(12)?,
        restore_status: r.get(13)?,
    })
}

pub(crate) fn row_to_lineage(r: Row) -> rusqlite::Result<crate::lineage::LineageRecord> {
    Ok(crate::lineage::LineageRecord {
        seq: r.get(0)?,
        relation: LineageRelation::parse_str(&r.get::<_, String>(1)?),
        workload_id: r
            .get::<_, Option<String>>(2)?
            .and_then(|s| Id::from_hex(&s).ok()),
        checkpoint_id: r
            .get::<_, Option<String>>(3)?
            .and_then(|s| Id::from_hex(&s).ok()),
        other_workload: r
            .get::<_, Option<String>>(4)?
            .and_then(|s| Id::from_hex(&s).ok()),
        other_checkpoint: r
            .get::<_, Option<String>>(5)?
            .and_then(|s| Id::from_hex(&s).ok()),
        ts_ms: r.get(6)?,
        detail: r.get(7)?,
    })
}

impl LifecycleState {
    pub(crate) fn from_str(s: &str) -> Self {
        serde_json::from_str(s).unwrap_or(Self::Failed)
    }
}

impl IntegrityState {
    pub(crate) fn from_str(s: &str) -> Self {
        serde_json::from_str(s).unwrap_or(Self::Unverifiable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{CheckpointType, ResumabilityClass};
    use crate::workload::ProtectionSpec;

    fn tmp_store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn workload(id: &Id) -> Workload {
        Workload {
            workload_id: *id,
            workload_generation: 0,
            owner: "t".into(),
            class: "test".into(),
            created_ms: now_ms(),
            execution_epoch: 0,
            active_node: None,
            backend_class: "cpu".into(),
            checkpoint_generation: 0,
            parent_workload: None,
            fork_generation: 0,
            policy_version: 1,
            metadata: serde_json::json!({}),
            state_schema_version: 1,
            runtime: crate::compatibility::RuntimeCompatibilityDescriptor::local_default(),
            resumability_class: ResumabilityClass::Equivalent,
            protection: ProtectionSpec::default(),
            single_active: true,
            fence_token: None,
            fence_epoch: 0,
        }
    }

    #[test]
    fn schema_versioned_and_refused_when_newer() {
        let (_d, store) = tmp_store();
        assert_eq!(store.meta_get("x").unwrap(), None);
        store.meta_put("x", "1").unwrap();
        assert_eq!(store.meta_get("x").unwrap(), Some("1".into()));

        let dir = store.data_dir.clone();
        let conn = Connection::open(dir.join("store.sqlite3")).unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        drop(conn);
        assert!(Store::open(&dir).is_err());
    }

    #[test]
    fn workload_crud() {
        let (_d, store) = tmp_store();
        let id = Id::random();
        let w = workload(&id);
        store.workload_insert(&w).unwrap();
        let got = store.workload_get(&id).unwrap().unwrap();
        assert_eq!(got, w);
        assert_eq!(store.workload_list().unwrap().len(), 1);
        assert!(store.workload_get(&Id::random()).unwrap().is_none());
    }

    #[test]
    fn checkpoint_crud_roundtrip() {
        let (_d, store) = tmp_store();
        let wid = Id::random();
        let ck = crate::compatibility::sample_checkpoint();
        let mut ck = ck.clone();
        ck.workload_id = wid;
        ck.checkpoint_type = CheckpointType::Full;
        store.checkpoint_insert(&ck).unwrap();
        let got = store.checkpoint_get(&ck.checkpoint_id).unwrap().unwrap();
        assert_eq!(got.checkpoint_type, CheckpointType::Full);
        assert_eq!(got.checkpoint_id, ck.checkpoint_id);
        assert_eq!(store.checkpoint_list(Some(&wid)).unwrap().len(), 1);
    }

    #[test]
    fn checkpoint_generation_unique_per_workload() {
        let (_d, store) = tmp_store();
        let a = crate::compatibility::sample_checkpoint();
        let mut b = a.clone();
        b.checkpoint_id = Id::random();
        b.checkpoint_generation = 2;
        store.checkpoint_insert(&a).unwrap();
        store.checkpoint_insert(&b).unwrap();
        // Replays are rejected explicitly rather than silently mixing component
        // rows into an existing immutable checkpoint.
        assert!(store.checkpoint_insert(&a).is_err());
        assert_eq!(store.checkpoint_list(None).unwrap().len(), 2);
        // A different checkpoint with the same generation must fail closed.
        let mut c = a.clone();
        c.checkpoint_id = Id::random();
        c.checkpoint_generation = a.checkpoint_generation;
        assert!(store.checkpoint_insert(&c).is_err());
        assert_eq!(store.checkpoint_list(None).unwrap().len(), 2);
    }

    #[test]
    fn reservations_conflict_and_release() {
        let (_d, store) = tmp_store();
        let wid = Id::random();
        store
            .reservation_create("r1", "capture", Some(&wid), None, "n1", 60_000)
            .unwrap();
        assert!(store
            .reservation_create("r2", "capture", Some(&wid), None, "n2", 60_000)
            .is_err());
        store.reservation_release("r1").unwrap();
        store
            .reservation_create("r2", "capture", Some(&wid), None, "n2", 60_000)
            .unwrap();
    }

    #[test]
    fn single_active_fencing() {
        let (_d, store) = tmp_store();
        let id = Id::random();
        let mut w = workload(&id);
        w.single_active = true;
        store.workload_insert(&w).unwrap();
        store.workload_claim(&id, "n1", "t1", 0).unwrap();
        assert!(store.workload_claim(&id, "n2", "t2", 0).is_err());
        store.workload_bump_fence(&id, 0).unwrap();
        assert!(store.workload_claim(&id, "n2", "t3", 0).is_err());
        assert!(store.workload_claim(&id, "n2", "future", 2).is_err());
        store.workload_claim(&id, "n2", "t3", 1).unwrap();
        store.workload_release(&id, "n2").unwrap();
        let released = store.workload_get(&id).unwrap().unwrap();
        assert!(released.active_node.is_none());
        assert!(released.fence_token.is_none());
    }

    #[test]
    fn journal_and_audit() {
        let (_d, store) = tmp_store();
        store
            .journal_append("capture", "k", "reserved", "d")
            .unwrap();
        assert!(store.journal_has("capture", "k", "reserved").unwrap());
        store
            .audit_append("test", "capture", None, None, AuditResult::Ok, "d")
            .unwrap();
        let recs = store.audit_query(None, 10).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].op, "capture");
    }

    #[test]
    fn stale_node_sweep() {
        let (_d, store) = tmp_store();
        store.node_register("n1", "addr", "boot", "{}").unwrap();
        store.node_heartbeat("n1", "{}").unwrap();
        store.node_register("n2", "addr", "boot", "{}").unwrap();
        // n2 has no heartbeat -> already stale by now
        let stale = store.nodes_sweep_stale(now_ms() + 1).unwrap();
        assert!(stale.contains(&"n2".to_string()));
    }
}
