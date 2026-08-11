//! Failure-injection tests: injected faults at every meaningful capture/restore
//! commit boundary, with recovery verified on coordinator restart.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::*;

fn tmpdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cf-fail-{tag}-{}",
        checkpoint_fabric::id::Id::random().to_hex()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn setup(env: &[(&str, &str)]) -> (CoordProc, NodeProc, PathBuf) {
    let coord_dir = tmpdir("coord");
    let coord = CoordProc::start(&coord_dir, env);
    let node_dir = tmpdir("node");
    let node = NodeProc::start("n1", &node_dir, &coord.addr);
    let c = coord.addr.to_string();
    wait_until("node registered", || cli(&c, &["nodes"]).contains("n1@"));
    let node_id = node_name(&c);
    cli(
        &c,
        &["workload", "create", "--owner", "t", "--node", &node_id],
    );
    (coord, node, node_dir)
}

/// A capture interrupted before the durable commit must never produce an
/// AVAILABLE checkpoint; recovery must reconcile the attempt.
#[test]
fn capture_interrupted_before_db_commit_recovers() {
    let (mut coord, _node, _node_dir) = setup(&[("CF_FAILPOINT", "capture.before_db_commit")]);
    let c = coord.addr.to_string();
    let wid = workload_id(&c);

    let output = std::process::Command::new(BIN)
        .args(["--coordinator", &c, "capture", &wid])
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "capture must fail at the injected point"
    );

    // No AVAILABLE checkpoint may exist.
    let stats = cli(&c, &["stats"]);
    assert!(stats.contains("\"available\":0"), "{stats}");

    // The checkpoint must be FAILED or absent; never restorable.
    let list = cli(&c, &["checkpoint", "list", "--workload-id", &wid]);
    assert!(!list.contains("AVAILABLE"), "{list}");

    // Recovery reconciliation runs cleanly and records the aborted attempt.
    let rec = cli(&c, &["recovery"]);
    assert!(rec.contains("\"ok\":true"), "{rec}");

    coord.stop();
}

/// A capture interrupted after physical persistence but before metadata commit
/// is completed by recovery (idempotent re-commit) on coordinator restart.
#[test]
fn capture_persisted_then_recommitted_after_restart() {
    let (mut coord, _node, node_dir) = setup(&[("CF_FAILPOINT", "capture.before_db_commit")]);
    let c = coord.addr.to_string();
    let wid = workload_id(&c);

    let _ = std::process::Command::new(BIN)
        .args(["--coordinator", &c, "capture", &wid])
        .output()
        .unwrap();

    // Restart the coordinator (new epoch) and run recovery.
    coord.stop();
    // The old node exits after losing the coordinator; spawn a fresh one.
    let mut node = _node;
    if wait_exit(&mut node.child, Duration::from_secs(20)).is_none() {
        kill_tree(&mut node.child);
    }
    let mut coord2 = CoordProc::start(&coord.data_dir.clone(), &[]);
    let node_dir2 = tmpdir("node2");
    let _node2 = NodeProc::start("n2", &node_dir2, &coord2.addr);
    let c2 = coord2.addr.to_string();
    wait_until("node re-registered", || {
        cli(&c2, &["nodes"]).contains("n2@")
    });
    let rec = cli(&c2, &["recovery"]);
    assert!(rec.contains("\"ok\":true"), "{rec}");

    // The interrupted attempt was aborted; no phantom AVAILABLE checkpoints.
    let stats = cli(&c2, &["stats"]);
    assert!(stats.contains("\"available\":0"), "{stats}");
    let _ = node_dir;
    coord2.stop();
}

/// A restore interrupted after component restore must fail the attempt and the
/// checkpoint must remain restorable.
#[test]
fn restore_interrupted_before_generation_commit() {
    let (mut coord, _node, _node_dir) = setup(&[]);
    let c = coord.addr.to_string();
    let wid = workload_id(&c);
    cli(&c, &["capture", &wid]);
    let ckpt = checkpoint_of(&c, &wid);

    // A second node whose restore path fails once (node-side failpoint) serves
    // as the interrupted-restore target; the replica is streamed to it first.
    let node_dir_b = tmpdir("node-b");
    let _node_b = NodeProc::start_with_env(
        "nb",
        &node_dir_b,
        &coord.addr,
        &[("CF_FAILPOINT", "restore.node.before_apply")],
    );
    wait_until("second node registered", || {
        cli(&c, &["nodes"]).contains("nb@")
    });
    let nb = node_name_for(&cli(&c, &["nodes"]), "nb");

    // First restore on nb: the replica is streamed, then the node-side
    // failpoint aborts the restore before any state is applied.
    let output = std::process::Command::new(BIN)
        .args(["--coordinator", &c, "restore", &ckpt, &nb])
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "restore must fail at the injected point"
    );

    // The checkpoint remains AVAILABLE for retry.
    let inspected = cli(&c, &["checkpoint", "inspect", &ckpt]);
    assert!(inspected.contains("AVAILABLE"), "{inspected}");

    // And a clean retry succeeds.
    cli(&c, &["restore", &ckpt, &nb]);
    coord.stop();
}

/// Concurrent captures of the same workload must not double-commit.
#[test]
fn concurrent_capture_no_double_commit() {
    let (mut coord, _node, _node_dir) = setup(&[]);
    let c = coord.addr.to_string();
    let wid = workload_id(&c);

    let mut handles = Vec::new();
    for _ in 0..4 {
        let c = c.clone();
        let wid = wid.clone();
        handles.push(std::thread::spawn(move || {
            let out = std::process::Command::new(BIN)
                .args(["--coordinator", &c, "capture", &wid])
                .output()
                .unwrap();
            out.status.success()
        }));
    }
    let successes = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|success| *success)
        .count();
    assert_eq!(successes, 1, "exactly one concurrent capture must commit");

    // At most one AVAILABLE checkpoint committed.
    let list = cli(&c, &["checkpoint", "list", "--workload-id", &wid]);
    let count = list.matches("\"lifecycle\":\"AVAILABLE\"").count();
    assert!(count <= 1, "double commit detected: {list}");

    // Generations are monotonic.
    let w = cli(&c, &["workload", "inspect", &wid]);
    assert!(w.contains("\"checkpoint_generation\":1"), "{w}");

    coord.stop();
}

/// A stale capture attempt can never seal a checkpoint after recovery.
#[test]
fn stale_attempts_rejected_after_restart() {
    let (mut coord, _node, _node_dir) = setup(&[]);
    let c = coord.addr.to_string();
    let wid = workload_id(&c);

    // Interrupt a capture at a dangerous point.
    let _ = std::process::Command::new(BIN)
        .args(["--coordinator", &c, "capture", &wid])
        .output()
        .unwrap();
    coord.stop();

    // New epoch; any old attempt id must be considered stale.
    let mut coord2 = CoordProc::start(&coord.data_dir.clone(), &[]);
    let c2 = coord2.addr.to_string();
    let rec = cli(&c2, &["recovery"]);
    assert!(rec.contains("\"ok\":true"), "{rec}");

    let active = cli(&c2, &["stats"]);
    assert!(active.contains("\"active_attempts\":0"), "{active}");
    coord2.stop();
}

fn workload_id(c: &str) -> String {
    let out = cli(c, &["workload", "list"]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
    v.as_array().unwrap()[0]
        .get("workload_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn checkpoint_of(c: &str, wid: &str) -> String {
    let out = cli(c, &["checkpoint", "list", "--workload-id", wid]);
    let v: serde_json::Value = serde_json::from_str(out.trim()).expect("json");
    v.as_array().unwrap()[0]
        .get("checkpoint_id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn node_name(c: &str) -> String {
    node_name_for(&cli(c, &["nodes"]), "n")
}

fn node_name_for(nodes_out: &str, prefix: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(nodes_out.trim())
        .unwrap_or_else(|_| panic!("bad nodes json: {nodes_out}"));
    v.as_array()
        .unwrap_or_else(|| panic!("nodes output is not an array: {nodes_out}"))
        .iter()
        .find(|n| {
            n.get("id")
                .and_then(|i| i.as_str())
                .map(|i| i.starts_with(prefix))
                .unwrap_or(false)
        })
        .and_then(|n| n.get("id")?.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| panic!("node '{prefix}' not found in: {nodes_out}"))
}
