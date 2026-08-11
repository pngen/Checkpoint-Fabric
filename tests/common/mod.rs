//! Shared test utilities for integration and multi-process tests.
//!
//! All multi-process tests use the real `checkpointfabric` binary (via
//! `CARGO_BIN_EXE_checkpointfabric`) and real framed TCP. Watchdogs bound every
//! wait; on timeout, owned process trees are terminated and cleaned up.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use checkpoint_fabric::transport::RpcClient;

pub const BIN: &str = env!("CARGO_BIN_EXE_checkpointfabric");
pub const WATCHDOG: Duration = Duration::from_secs(60);

/// Poll a condition with a watchdog. Panics with `what` on timeout.
pub fn wait_until<F: FnMut() -> bool>(what: &str, mut cond: F) {
    let deadline = Instant::now() + WATCHDOG;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("watchdog timeout waiting for: {what}");
}

/// Wait for a child process to exit with a bounded timeout. Returns exit status.
pub fn wait_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Kill a process tree deterministically.
pub fn kill_tree(child: &mut Child) {
    let _ = child.kill();
    let _ = wait_exit(child, Duration::from_secs(5));
}

/// Spawn the coordinator binary as a real process.
pub fn spawn_coordinator(data_dir: &Path, port: u16, extra_env: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(BIN);
    cmd.args(["coordinator", "start", "--data-dir"])
        .arg(data_dir)
        .args(["--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn coordinator")
}

/// Spawn the node binary as a real process.
pub fn spawn_node(
    name: &str,
    data_dir: &Path,
    coordinator_addr: &str,
    extra_env: &[(&str, &str)],
) -> Child {
    let mut cmd = Command::new(BIN);
    cmd.args(["node", "start", "--name", name, "--data-dir"])
        .arg(data_dir)
        .args(["--coordinator", coordinator_addr, "--heartbeat-ms", "250"])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn node")
}

/// Find a free TCP port.
pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Run a CLI command against the coordinator; returns stdout. Fails the test on
/// non-zero exit.
pub fn cli(coordinator: &str, args: &[&str]) -> String {
    let output = Command::new(BIN)
        .args(["--coordinator", coordinator])
        .args(args)
        .output()
        .expect("run cli");
    if !output.status.success() {
        panic!(
            "cli {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// A coordinator child process with automatic cleanup.
pub struct CoordProc {
    pub child: Child,
    pub addr: SocketAddr,
    pub data_dir: PathBuf,
}

impl CoordProc {
    pub fn start(data_dir: &Path, extra_env: &[(&str, &str)]) -> Self {
        let port = free_port();
        let child = spawn_coordinator(data_dir, port, extra_env);
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        wait_until("coordinator accepting connections", || {
            RpcClient::connect(&addr).is_ok()
        });
        Self {
            child,
            addr,
            data_dir: data_dir.to_path_buf(),
        }
    }

    pub fn stop(&mut self) {
        // Graceful shutdown via RPC.
        if let Ok(mut client) = RpcClient::connect(&self.addr) {
            let _ = client.call(checkpoint_fabric::protocol::Op::CoordinatorShutdown, b"{}");
        }
        if wait_exit(&mut self.child, Duration::from_secs(10)).is_none() {
            kill_tree(&mut self.child);
        }
    }
}

impl Drop for CoordProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = wait_exit(&mut self.child, Duration::from_secs(5));
    }
}

/// A node child process with automatic cleanup.
pub struct NodeProc {
    pub child: Child,
    pub name: String,
    pub data_dir: PathBuf,
}

impl NodeProc {
    pub fn start(name: &str, data_dir: &Path, coord_addr: &SocketAddr) -> Self {
        Self::start_with_env(name, data_dir, coord_addr, &[])
    }

    pub fn start_with_env(
        name: &str,
        data_dir: &Path,
        coord_addr: &SocketAddr,
        extra_env: &[(&str, &str)],
    ) -> Self {
        let child = spawn_node(name, data_dir, &coord_addr.to_string(), extra_env);
        Self {
            child,
            name: name.to_string(),
            data_dir: data_dir.to_path_buf(),
        }
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = wait_exit(&mut self.child, Duration::from_secs(5));
    }
}
