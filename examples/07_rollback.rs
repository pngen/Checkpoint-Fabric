//! Example 7: rollback to a prior checkpoint.
//!
//! Advances the workload past a failure, then rolls back to an earlier
//! checkpoint; the rollback creates a new execution generation.

mod common;

fn main() {
    common::run("07-rollback", || {
        let h = common::harness("07", common::HarnessOptions::default());
        *h.cell.lock().unwrap() = b"good-state-v1".to_vec();
        let first = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap()
            .checkpoint_id
            .unwrap();

        // The workload "fails" (moves to corrupted state) and captures again.
        *h.cell.lock().unwrap() = b"corrupted-state".to_vec();
        let _failed = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap()
            .checkpoint_id
            .unwrap();

        let before = h
            .coord
            .inspect_workload(&h.workload.workload_id)
            .unwrap()
            .unwrap();
        println!(
            "before rollback: workload generation {}",
            before.workload_generation
        );

        common::banner("roll back to the first checkpoint");
        let rolled = h
            .coord
            .request_rollback(&first, &h.node.node_id, &h.ctx)
            .unwrap();
        println!(
            "rollback: workload generation {} -> {}",
            before.workload_generation,
            rolled.workload_generation.unwrap()
        );

        // A rollback always creates a NEW execution generation.
        let after = h
            .coord
            .inspect_workload(&h.workload.workload_id)
            .unwrap()
            .unwrap();
        assert_eq!(after.workload_generation, before.workload_generation + 1);
        assert_eq!(after.execution_epoch, before.execution_epoch + 1);
        assert_eq!(*h.cell.lock().unwrap(), b"good-state-v1".to_vec());
        println!("state cell rolled back to: {:?}", *h.cell.lock().unwrap());

        let lineage = h.coord.workload_lineage(&h.workload.workload_id).unwrap();
        assert!(lineage
            .iter()
            .any(|l| l.relation == checkpoint_fabric::lineage::LineageRelation::RollbackOf));
        h.coord.shutdown();
        h.node.shutdown();
    });
}
