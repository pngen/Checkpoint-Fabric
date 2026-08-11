//! Example 3: checkpoint verification.
//!
//! Captures a checkpoint, verifies its integrity through the node, and shows a
//! tampered checkpoint being rejected.

mod common;

use checkpoint_fabric::storage::StorageBackend;

fn main() {
    common::run("03-checkpoint-verification", || {
        let h = common::harness("03", common::HarnessOptions::default());
        let opts = common::capture_options();
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &opts, &h.ctx)
            .unwrap();
        let ckpt_id = out.checkpoint_id.unwrap();

        common::banner("verify a pristine checkpoint");
        let verify = h.coord.verify_checkpoint(&ckpt_id, &h.ctx).unwrap();
        println!("verified={} digest={:?}", verify.ok, verify.manifest_digest);
        assert!(verify.ok);

        common::banner("tamper with the stored component and re-verify");
        let commit = h.node.storage.commit_dir(&ckpt_id);
        let payload = std::fs::read_dir(commit.join("components"))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let data = std::fs::read(&payload).unwrap();
        let mut corrupted = data.clone();
        let len = corrupted.len();
        corrupted[len / 2] ^= 0xff;
        std::fs::write(&payload, corrupted).unwrap();

        let verify2 = h.coord.verify_checkpoint(&ckpt_id, &h.ctx).unwrap();
        println!(
            "tampered checkpoint verified={} error={:?}",
            verify2.ok, verify2.error
        );
        assert!(!verify2.ok);

        // Restore of the corrupted checkpoint must be refused.
        let restore = h.coord.request_restore(
            &ckpt_id,
            &h.node.node_id,
            &checkpoint_fabric::restore::RestoreOptions::default(),
            &h.ctx,
        );
        println!(
            "restore of corrupted checkpoint refused: {}",
            restore.is_err()
        );
        assert!(restore.is_err());
        h.coord.shutdown();
        h.node.shutdown();
    });
}
