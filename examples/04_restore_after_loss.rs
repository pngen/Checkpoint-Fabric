//! Example 4: restore after simulated process loss.
//!
//! Captures a checkpoint, destroys the source process's state (simulated by
//! clearing the state cell), then restores the checkpoint back into the
//! application state.

mod common;

use checkpoint_fabric::restore::RestoreOptions;

fn main() {
    common::run("04-restore-after-process-loss", || {
        let h = common::harness("04", common::HarnessOptions::default());
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap();
        let ckpt_id = out.checkpoint_id.unwrap();

        common::banner("simulate process loss: wipe the application state cell");
        *h.cell.lock().unwrap() = Vec::new();
        assert!(h.cell.lock().unwrap().is_empty());

        common::banner("restore the checkpoint back into the state cell");
        let restored = h
            .coord
            .request_restore(
                &ckpt_id,
                &h.node.node_id,
                &RestoreOptions::default(),
                &h.ctx,
            )
            .unwrap();
        println!("restore state={}", restored.state);
        assert_eq!(
            *h.cell.lock().unwrap(),
            b"hello-checkpoint-fabric".to_vec(),
            "state cell must be repopulated by restore"
        );
        println!(
            "state cell restored to {} bytes",
            h.cell.lock().unwrap().len()
        );
        h.coord.shutdown();
        h.node.shutdown();
    });
}
