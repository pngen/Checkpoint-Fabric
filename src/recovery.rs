//! Crash recovery: reconciliation of metadata and physical state after restart.
//!
//! The recovery journal (durable in SQLite) records each capture/restore commit
//! point. On restart the coordinator reconciles ambiguous states deterministically:
//! a physical commit whose metadata commit is missing is re-committed idempotently
//! (never silently dropped); a partially captured attempt is marked FAILED and its
//! staging cleaned by the owning node.

use crate::audit::AuditResult;
use crate::capture::AttemptRecord;
use crate::checkpoint::IntegrityState;
use crate::coordinator::OperationContext;
use crate::errors::{FabricError, FabricResult};
use crate::lifecycle::LifecycleState;
use crate::persistence::Store;
use crate::time::now_ms;

/// The recovery journal states used by capture.
pub mod capture_states {
    pub const RESERVED: &str = "reserved";
    pub const SEALED: &str = "sealed";
    pub const PERSISTED: &str = "persisted";
    pub const DB_COMMITTED: &str = "db_committed";
    pub const RESUME_DONE: &str = "resume_done";
}

/// The recovery journal states used by restore.
pub mod restore_states {
    pub const RESERVED: &str = "reserved";
    pub const PROVISIONED: &str = "provisioned";
    pub const COMPONENTS_RESTORED: &str = "components_restored";
    pub const GENERATION_COMMITTED: &str = "generation_committed";
    pub const RESUMED: &str = "resumed";
}

/// One deterministic recovery action.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryAction {
    pub kind: String,
    pub key: String,
    pub state: String,
    pub detail: String,
}

/// Outcome of a reconciliation pass.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RecoveryOutcome {
    pub actions: Vec<RecoveryAction>,
    pub stale_nodes: Vec<String>,
    pub expired_reservations: u64,
    pub ok: bool,
}

/// Reconcile the durable store after a restart.
///
/// Rules:
/// - Active attempts with no journal evidence of progress are failed (their
///   reservation is released and staging is left for node-side cleanup).
/// - Capture attempts whose journal shows `PERSISTED` but no `DB_COMMITTED` are
///   re-committed idempotently (the metadata commit completes the durable commit).
/// - Restore attempts whose journal shows `COMPONENTS_RESTORED` but no
///   `GENERATION_COMMITTED` are failed with a cleanup directive.
/// - Restore attempts whose journal shows `GENERATION_COMMITTED` but no `RESUMED`
///   are marked committed-with-unverified-resume.
/// - Stale nodes are swept and expired reservations released.
pub fn reconcile(store: &Store, ctx: &OperationContext) -> FabricResult<RecoveryOutcome> {
    let mut out = RecoveryOutcome {
        expired_reservations: store.reservations_expire(now_ms())?,
        ..RecoveryOutcome::default()
    };

    // Stale nodes.
    let stale_ms = ctx.stale_ms;
    let stale_before = now_ms().saturating_sub(stale_ms);
    let stale = store.nodes_sweep_stale(stale_before)?;
    for n in &stale {
        store.node_set_status(n, "STALE")?;
        store.audit_append(
            "coordinator",
            "node.stale",
            None,
            None,
            AuditResult::Failed,
            &format!("node {n} declared stale by recovery"),
        )?;
        out.stale_nodes.push(n.clone());
    }

    // Active attempts.
    let attempts = store.attempts_in_state("ACTIVE")?;
    for a in attempts {
        let action = reconcile_attempt(store, &a, ctx)?;
        out.actions.push(action);
    }

    out.ok = true;
    Ok(out)
}

fn reconcile_attempt(
    store: &Store,
    a: &AttemptRecord,
    _ctx: &OperationContext,
) -> FabricResult<RecoveryAction> {
    let key = a.id.clone();
    match a.kind.as_str() {
        "capture" => {
            let has_persisted = store.journal_has("capture", &key, capture_states::PERSISTED)?;
            let has_db = store.journal_has("capture", &key, capture_states::DB_COMMITTED)?;
            if has_db {
                store.attempt_finish(
                    &key,
                    "COMMITTED",
                    Some("capture committed before restart"),
                    None,
                )?;
                store.reservation_release(&key)?;
                return Ok(RecoveryAction {
                    kind: "capture".into(),
                    key: key.clone(),
                    state: "db_committed".into(),
                    detail: "capture metadata commit confirmed after restart".into(),
                });
            }
            if has_persisted {
                // Ambiguity: physical commit done, metadata commit missing.
                // The coordinator re-executes the idempotent metadata commit.
                let ckpt_id = a.checkpoint_id.ok_or_else(|| {
                    FabricError::Internal(format!("capture attempt {key} has no checkpoint id"))
                })?;
                let ckpt = store
                    .checkpoint_get(&ckpt_id)?
                    .ok_or_else(|| FabricError::CheckpointNotFound(ckpt_id.to_string()))?;
                if let Err(error) = store.capture_commit(&ckpt_id, &key) {
                    if ckpt.lifecycle == LifecycleState::Persisting
                        || ckpt.lifecycle == LifecycleState::Available
                    {
                        ckpt.lifecycle.transition(LifecycleState::Failed)?;
                        store.checkpoint_set_lifecycle(
                            &ckpt_id,
                            LifecycleState::Failed,
                            IntegrityState::Corrupt,
                        )?;
                    }
                    store.attempt_finish(
                        &key,
                        "FAILED",
                        None,
                        Some(&format!(
                            "persisted checkpoint metadata is incomplete: {error}"
                        )),
                    )?;
                    store.reservation_release(&key)?;
                    store.audit_append(
                        "coordinator",
                        "capture.recovery_rejected",
                        Some(&ckpt.workload_id),
                        Some(&ckpt_id),
                        AuditResult::Failed,
                        &format!(
                            "attempt {key}: refusing to fabricate an available checkpoint from incomplete persisted metadata: {error}"
                        ),
                    )?;
                    return Ok(RecoveryAction {
                        kind: "capture".into(),
                        key: key.clone(),
                        state: "failed_cleanup".into(),
                        detail: format!(
                            "persisted checkpoint metadata was incomplete and was rejected: {error}"
                        ),
                    });
                }
                store.attempt_finish(
                    &key,
                    "COMMITTED",
                    Some("capture metadata commit completed by recovery"),
                    None,
                )?;
                store.reservation_release(&key)?;
                store.audit_append(
                    "coordinator",
                    "capture.recovery_commit",
                    Some(&ckpt.workload_id),
                    Some(&ckpt_id),
                    AuditResult::Recovered,
                    &format!("attempt {key}: physical commit found without metadata commit; re-committed idempotently"),
                )?;
                return Ok(RecoveryAction {
                    kind: "capture".into(),
                    key: key.clone(),
                    state: "db_committed".into(),
                    detail: "capture physically persisted; metadata commit completed by recovery"
                        .into(),
                });
            }
            // Nothing durable: fail the attempt cleanly.
            store.attempt_finish(
                &key,
                "FAILED",
                None,
                Some("attempt interrupted before durable commit"),
            )?;
            store.reservation_release(&key)?;
            store.audit_append(
                "coordinator",
                "capture.aborted_by_recovery",
                a.workload_id.as_ref(),
                a.checkpoint_id.as_ref(),
                AuditResult::Failed,
                &format!("attempt {key} aborted: no durable commit evidence"),
            )?;
            Ok(RecoveryAction {
                kind: "capture".into(),
                key: key.clone(),
                state: "failed".into(),
                detail: "capture attempt aborted by recovery (no durable commit evidence)".into(),
            })
        }
        "restore" | "rollback" => {
            let has_restored =
                store.journal_has("restore", &key, restore_states::COMPONENTS_RESTORED)?;
            let has_committed =
                store.journal_has("restore", &key, restore_states::GENERATION_COMMITTED)?;
            let has_resumed = store.journal_has("restore", &key, restore_states::RESUMED)?;
            if has_committed {
                if !has_resumed {
                    store.attempt_finish(
                        &key,
                        "COMMITTED",
                        Some("generation committed; resume unverified after restart"),
                        None,
                    )?;
                    store.reservation_release(&key)?;
                    store.audit_append(
                        "coordinator",
                        "restore.resume_unverified",
                        a.workload_id.as_ref(),
                        a.checkpoint_id.as_ref(),
                        AuditResult::Recovered,
                        &format!("restore attempt {key}: generation committed before restart; resume state unverified"),
                    )?;
                    return Ok(RecoveryAction {
                        kind: "restore".into(),
                        key: key.clone(),
                        state: "resume_unverified".into(),
                        detail: "restore committed; resume unverified after restart".into(),
                    });
                }
                store.attempt_finish(
                    &key,
                    "COMMITTED",
                    Some("restore committed before restart"),
                    None,
                )?;
                store.reservation_release(&key)?;
                return Ok(RecoveryAction {
                    kind: "restore".into(),
                    key: key.clone(),
                    state: "committed".into(),
                    detail: "restore confirmed committed after restart".into(),
                });
            }
            if has_restored {
                if let Some(checkpoint_id) = a.checkpoint_id {
                    if let Some(checkpoint) = store.checkpoint_get(&checkpoint_id)? {
                        if checkpoint.lifecycle == LifecycleState::RestorePending
                            || checkpoint.lifecycle == LifecycleState::Restoring
                        {
                            checkpoint.lifecycle.transition(LifecycleState::Available)?;
                            store.checkpoint_set_lifecycle(
                                &checkpoint_id,
                                LifecycleState::Available,
                                IntegrityState::Valid,
                            )?;
                        }
                    }
                }
                store.attempt_finish(
                    &key,
                    "FAILED",
                    None,
                    Some("restore interrupted before generation commit"),
                )?;
                store.reservation_release(&key)?;
                store.audit_append(
                    "coordinator",
                    "restore.cleaned_by_recovery",
                    a.workload_id.as_ref(),
                    a.checkpoint_id.as_ref(),
                    AuditResult::Failed,
                    &format!("restore attempt {key}: components restored but generation commit missing; target cleanup required"),
                )?;
                return Ok(RecoveryAction {
                    kind: "restore".into(),
                    key: key.clone(),
                    state: "failed_cleanup".into(),
                    detail: "restore interrupted before generation commit; target cleanup required"
                        .into(),
                });
            }
            store.attempt_finish(
                &key,
                "FAILED",
                None,
                Some("restore interrupted before provisioning"),
            )?;
            store.reservation_release(&key)?;
            Ok(RecoveryAction {
                kind: "restore".into(),
                key: key.clone(),
                state: "failed".into(),
                detail: "restore attempt aborted by recovery (no durable progress)".into(),
            })
        }
        other => {
            store.attempt_finish(&key, "FAILED", None, Some("unknown attempt kind"))?;
            store.reservation_release(&key)?;
            Ok(RecoveryAction {
                kind: other.into(),
                key: key.clone(),
                state: "failed".into(),
                detail: "attempt of unknown kind aborted by recovery".into(),
            })
        }
    }
}

/// Node-side physical reconciliation: remove staging directories not covered by
/// active attempts, and confirm committed checkpoint layouts. Returns removed paths.
pub fn reconcile_node_storage(
    storage: &crate::storage::LocalStorage,
    active_attempts: &[String],
) -> FabricResult<Vec<std::path::PathBuf>> {
    use crate::storage::StorageBackend;
    let keep: std::collections::HashSet<std::path::PathBuf> = active_attempts
        .iter()
        .map(|a| storage.staging_dir(a))
        .collect();
    storage.recover_partial(&keep)
}

/// Placeholder to keep the module's error surface complete.
pub fn recovery_error(msg: &str) -> FabricError {
    FabricError::Internal(format!("recovery: {msg}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::AttemptRecord;
    use crate::checkpoint::{
        ComponentEntry, ComponentType, DurableLocation, ResumabilityClass, StorageRepresentation,
    };
    use crate::id::Id;
    use crate::workload::{ProtectionSpec, Workload};

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        (dir, store)
    }

    fn ctx() -> OperationContext {
        OperationContext {
            actor: "test".into(),
            roles: vec!["operator".into()],
            stale_ms: 60_000,
        }
    }

    fn workload(id: Id) -> Workload {
        Workload {
            workload_id: id,
            workload_generation: 0,
            owner: "test".into(),
            class: "recovery".into(),
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

    fn component() -> ComponentEntry {
        ComponentEntry {
            component_id: "state".into(),
            component_type: ComponentType::ApplicationState,
            generation: 0,
            required: true,
            logical_size: 1,
            storage_representation: StorageRepresentation {
                codec: "none".into(),
                original_size: 1,
                stored_size: 1,
                stored_hash: "stored".into(),
                relative_path: "components/state".into(),
            },
            content_hash: "content".into(),
            schema_version: 1,
            restore_handler: "application/state".into(),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
            capture_status: "captured".into(),
            restore_status: "pending".into(),
        }
    }

    #[test]
    fn partial_capture_aborted() {
        let (_d, store) = store();
        let wid = Id::random();
        store
            .attempt_begin("cap-1", "capture", None, Some(&wid), "n1")
            .unwrap();
        store
            .reservation_create("cap-1", "capture", Some(&wid), None, "n1", 60_000)
            .unwrap();
        let out = reconcile(&store, &ctx()).unwrap();
        assert!(out
            .actions
            .iter()
            .any(|a| a.key == "cap-1" && a.state == "failed"));
        assert_eq!(store.attempt_get("cap-1").unwrap().unwrap().state, "FAILED");
        assert!(store
            .reservation_active_for("capture", Some(&wid), None)
            .unwrap()
            .is_none());
    }

    #[test]
    fn persisted_capture_recommitted() {
        let (_d, store) = store();
        let wid = Id::random();
        let mut ck = crate::compatibility::sample_checkpoint();
        ck.workload_id = wid;
        ck.lifecycle = LifecycleState::Persisting;
        ck.components = vec![component()];
        ck.durable_locations = vec![DurableLocation {
            node: "n1".into(),
            path: "checkpoint/path".into(),
            verified: true,
        }];
        ck.manifest_digest = Some("anchor".into());
        store.workload_insert(&workload(wid)).unwrap();
        store.checkpoint_insert(&ck).unwrap();
        store
            .attempt_begin(
                "cap-2",
                "capture",
                Some(&ck.checkpoint_id),
                Some(&wid),
                "n1",
            )
            .unwrap();
        store
            .journal_append("capture", "cap-2", capture_states::PERSISTED, "physical")
            .unwrap();
        let out = reconcile(&store, &ctx()).unwrap();
        assert!(out
            .actions
            .iter()
            .any(|a| a.key == "cap-2" && a.state == "db_committed"));
        let c = store.checkpoint_get(&ck.checkpoint_id).unwrap().unwrap();
        assert_eq!(c.lifecycle, LifecycleState::Available);
        assert_eq!(
            store.attempt_get("cap-2").unwrap().unwrap().state,
            "COMMITTED"
        );
    }

    #[test]
    fn persisted_empty_scaffold_is_rejected() {
        let (_d, store) = store();
        let wid = Id::random();
        let mut ck = crate::compatibility::sample_checkpoint();
        ck.workload_id = wid;
        ck.lifecycle = LifecycleState::Persisting;
        ck.components.clear();
        ck.durable_locations.clear();
        ck.manifest_digest = None;
        store.workload_insert(&workload(wid)).unwrap();
        store.checkpoint_insert(&ck).unwrap();
        store
            .attempt_begin(
                "cap-scaffold",
                "capture",
                Some(&ck.checkpoint_id),
                Some(&wid),
                "n1",
            )
            .unwrap();
        store
            .journal_append(
                "capture",
                "cap-scaffold",
                capture_states::PERSISTED,
                "physical",
            )
            .unwrap();

        let out = reconcile(&store, &ctx()).unwrap();
        assert!(out
            .actions
            .iter()
            .any(|a| a.key == "cap-scaffold" && a.state == "failed_cleanup"));
        assert_eq!(
            store
                .checkpoint_get(&ck.checkpoint_id)
                .unwrap()
                .unwrap()
                .lifecycle,
            LifecycleState::Failed
        );
        assert!(!store
            .journal_has("capture", "cap-scaffold", capture_states::DB_COMMITTED)
            .unwrap());
    }

    #[test]
    fn restore_without_commit_fails() {
        let (_d, store) = store();
        let ck = crate::compatibility::sample_checkpoint();
        store.checkpoint_insert(&ck).unwrap();
        store
            .attempt_begin(
                "res-1",
                "restore",
                Some(&ck.checkpoint_id),
                Some(&ck.workload_id),
                "n1",
            )
            .unwrap();
        store
            .journal_append(
                "restore",
                "res-1",
                restore_states::COMPONENTS_RESTORED,
                "partial",
            )
            .unwrap();
        let out = reconcile(&store, &ctx()).unwrap();
        assert!(out
            .actions
            .iter()
            .any(|a| a.key == "res-1" && a.state == "failed_cleanup"));
        assert_eq!(store.attempt_get("res-1").unwrap().unwrap().state, "FAILED");
    }

    #[test]
    fn node_storage_reconcile() {
        let dir = tempfile::tempdir().unwrap();
        let storage = crate::storage::LocalStorage::new(dir.path()).unwrap();
        use crate::storage::StorageBackend;
        std::fs::create_dir_all(storage.staging_dir("live")).unwrap();
        std::fs::create_dir_all(storage.staging_dir("dead")).unwrap();
        let removed = reconcile_node_storage(&storage, &["live".into()]).unwrap();
        assert_eq!(removed.len(), 1);
        assert!(removed[0].to_string_lossy().contains("dead"));
    }

    #[test]
    fn attempt_record_roundtrip() {
        let (_d, store) = store();
        let a = AttemptRecord {
            id: "a1".into(),
            kind: "capture".into(),
            checkpoint_id: None,
            workload_id: None,
            node: "n".into(),
            state: "ACTIVE".into(),
            started_ms: 0,
            finished_ms: None,
            result: None,
            error: None,
        };
        store
            .attempt_begin(
                &a.id,
                &a.kind,
                a.checkpoint_id.as_ref(),
                a.workload_id.as_ref(),
                &a.node,
            )
            .unwrap();
        let got = store.attempt_get("a1").unwrap().unwrap();
        assert_eq!(got.kind, "capture");
        assert_eq!(got.state, "ACTIVE");
    }
}
