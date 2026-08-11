//! Example 11: crash/restart recovery.
//!
//! Simulates a crash leaving two ambiguous capture attempts in the durable
//! store, then restarts the coordinator and shows recovery reconciling each
//! deterministically: the physically-persisted-but-uncommitted checkpoint is
//! re-committed idempotently, and the attempt with no durable progress is
//! failed without fabricating success.

mod common;

use checkpoint_fabric::checkpoint::IntegrityState;
use checkpoint_fabric::coordinator::{Coordinator, CoordinatorConfig};
use checkpoint_fabric::lifecycle::LifecycleState;
use checkpoint_fabric::recovery::capture_states;

fn main() {
    common::run("11-crash-restart-recovery", || {
        let h = common::harness("11", common::HarnessOptions::default());

        common::banner("simulate a crash mid-capture: two interrupted attempts");
        // Start from a genuinely persisted checkpoint, then model the durable
        // state at the PERSISTED boundary under a recovery attempt key.
        let captured = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap();
        let ckpt = h
            .coord
            .checkpoint_get(&captured.checkpoint_id.unwrap())
            .unwrap()
            .unwrap();
        h.coord
            .store
            .checkpoint_set_lifecycle(
                &ckpt.checkpoint_id,
                LifecycleState::Persisting,
                IntegrityState::Pending,
            )
            .unwrap();
        h.coord
            .store
            .attempt_begin(
                "cap-crash-persisted",
                "capture",
                Some(&ckpt.checkpoint_id),
                Some(&h.workload.workload_id),
                &h.node.node_id,
            )
            .unwrap();
        h.coord
            .store
            .journal_append(
                "capture",
                "cap-crash-persisted",
                capture_states::PERSISTED,
                "physical commit done; process died before metadata commit",
            )
            .unwrap();

        // Attempt 2 died before any durable progress.
        h.coord
            .store
            .attempt_begin(
                "cap-crash-early",
                "capture",
                None,
                Some(&h.workload.workload_id),
                &h.node.node_id,
            )
            .unwrap();

        common::banner("restart the coordinator against the same data directory");
        let data_dir = h.coord.config.data_dir.clone();
        h.coord
            .store
            .node_set_status(&h.node.node_id, "STALE")
            .unwrap();
        let common::Harness {
            coord,
            node,
            ctx,
            temp: _temp_guard,
            ..
        } = h;
        node.shutdown();
        coord.shutdown();
        drop(node);
        drop(coord);
        let cfg = CoordinatorConfig {
            data_dir,
            listen: Some("127.0.0.1:0".parse().unwrap()),
            epoch: Some(2),
            stale_ms: 60_000,
            policy: None,
            failpoints: None,
        };
        let coord2: std::sync::Arc<Coordinator> =
            Coordinator::open(cfg).unwrap().start_server().unwrap();

        let rec = coord2.run_recovery(&ctx).unwrap();
        println!(
            "recovery reconciled {} actions; ok={}",
            rec.actions.len(),
            rec.ok
        );
        for a in &rec.actions {
            println!("  {} -> {}", a.key, a.state);
        }
        assert!(rec.ok);

        // The physically persisted checkpoint is re-committed idempotently.
        assert!(
            rec.actions
                .iter()
                .any(|a| a.key == "cap-crash-persisted" && a.state == "db_committed"),
            "persisted capture must be re-committed: {:?}",
            rec.actions
        );
        let recommitted = coord2.checkpoint_get(&ckpt.checkpoint_id).unwrap().unwrap();
        assert_eq!(recommitted.lifecycle, LifecycleState::Available);
        println!("recovery re-committed the physically persisted checkpoint");

        // The attempt with no durable evidence is failed, never committed.
        assert!(
            rec.actions
                .iter()
                .any(|a| a.key == "cap-crash-early" && a.state == "failed"),
            "interrupted capture attempt must be aborted, not committed: {:?}",
            rec.actions
        );
        println!("recovery failed the interrupted attempt without fabricating success");

        coord2.shutdown();
    });
}
