//! Example 10: corrupted-component rejection.
//!
//! Corrupts a stored component payload and shows that restore fails closed
//! with an integrity error instead of restoring garbage.

mod common;

use checkpoint_fabric::storage::StorageBackend;

fn main() {
    common::run("10-corrupted-component-rejection", || {
        let h = common::harness("10", common::HarnessOptions::default());
        let out = h
            .coord
            .request_capture(&h.workload.workload_id, &common::capture_options(), &h.ctx)
            .unwrap();
        let ckpt_id = out.checkpoint_id.unwrap();

        common::banner("corrupt the stored component payload on the node");
        let commit = h.node.storage.commit_dir(&ckpt_id);
        let payload_path = commit.join("components/app");
        let mut bytes = std::fs::read(&payload_path).unwrap();
        let len = bytes.len();
        bytes[0] ^= 0xff;
        bytes[len - 1] ^= 0xff;
        std::fs::write(&payload_path, bytes).unwrap();

        let result = h.coord.request_restore(
            &ckpt_id,
            &h.node.node_id,
            &checkpoint_fabric::restore::RestoreOptions::default(),
            &h.ctx,
        );
        let err = result.expect_err("restore must fail closed");
        println!("restore rejected with: {err}");
        assert!(err.to_string().contains("integrity") || err.to_string().contains("corrupt"));

        // The state cell must be untouched.
        assert_eq!(*h.cell.lock().unwrap(), b"hello-checkpoint-fabric".to_vec());
        println!("state cell untouched by the failed restore");
        h.coord.shutdown();
        h.node.shutdown();
    });
}
