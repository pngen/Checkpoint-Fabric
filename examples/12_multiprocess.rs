//! Example 12: multi-process coordinator + nodes.
//!
//! Spawns the real `checkpointfabric` binary as a coordinator and a node
//! process, then drives workload creation, capture, and restore through the
//! real framed TCP CLI.

mod common;

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn main() {
    common::run("12-multi-process-coordinator-nodes", || {
        let tmp = common::temp_dir("12");
        let coord_dir = tmp.path().join("coord");
        let node_dir = tmp.path().join("node");
        std::fs::create_dir_all(&coord_dir).unwrap();
        std::fs::create_dir_all(&node_dir).unwrap();
        let port = free_port();

        // Spawn the coordinator process.
        let mut coord = OwnedChild::spawn(
            Command::new(common::bin())
                .args(["coordinator", "start", "--data-dir"])
                .arg(&coord_dir)
                .args(["--listen", &format!("127.0.0.1:{port}")])
                .stdout(Stdio::null())
                .stderr(Stdio::null()),
        );
        let addr = format!("127.0.0.1:{port}");
        wait_until("coordinator up", || {
            std::net::TcpStream::connect(&addr).is_ok()
        });

        // Spawn the node process.
        let mut node = OwnedChild::spawn(
            Command::new(common::bin())
                .args(["node", "start", "--name", "proc-node", "--data-dir"])
                .arg(&node_dir)
                .args(["--coordinator", &addr, "--heartbeat-ms", "250"])
                .stdout(Stdio::null())
                .stderr(Stdio::null()),
        );

        wait_until("node registered", || {
            cli(&addr, &["nodes"]).contains("proc-node@")
        });
        let node_id = node_name(&cli(&addr, &["nodes"]));

        // Create a workload on the node process.
        let out = cli(
            &addr,
            &[
                "workload", "create", "--owner", "example", "--node", &node_id,
            ],
        );
        let wid = json_get(&out, "workload_id");

        // Capture and restore through the CLI.
        cli(&addr, &["capture", &wid]);
        let list = cli(&addr, &["checkpoint", "list", "--workload-id", &wid]);
        let ckpt = json_get(&list, "checkpoint_id");
        println!("captured checkpoint {ckpt} via CLI");

        let restored = cli(&addr, &["restore", &ckpt, &node_id]);
        assert!(restored.contains("RESTORED"), "{restored}");
        println!("restore via CLI: state={}", json_get(&restored, "state"));

        // Shut down cleanly: the node exits when the coordinator stops.
        let _ = Command::new(common::bin())
            .args(["--coordinator", &addr, "coordinator", "stop"])
            .output()
            .unwrap();
        assert!(wait_exit(&mut coord.0, Duration::from_secs(10)).is_some());
        assert!(wait_exit(&mut node.0, Duration::from_secs(30)).is_some());
        println!("both processes exited cleanly");
    });
}

struct OwnedChild(Child);

impl OwnedChild {
    fn spawn(command: &mut Command) -> Self {
        Self(command.spawn().expect("spawn example child"))
    }
}

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = wait_exit(&mut self.0, Duration::from_secs(5));
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("timeout waiting for {what}");
}

fn wait_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(s) = child.try_wait().unwrap() {
            return Some(s);
        }
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn cli(coordinator: &str, args: &[&str]) -> String {
    let out = Command::new(common::bin())
        .args(["--coordinator", coordinator])
        .args(args)
        .output()
        .unwrap();
    if !out.status.success() {
        panic!(
            "cli {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn json_get(out: &str, key: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(out.trim())
        .unwrap_or_else(|error| panic!("invalid JSON for {key}: {error}; output={out:?}"));
    v.get(key)
        .or_else(|| v.as_array().and_then(|values| values.first()?.get(key)))
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("no {key} in {out}"))
        .to_string()
}

fn node_name(out: &str) -> String {
    json_get(out, "id")
}
