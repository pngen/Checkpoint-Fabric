//! Command-line interface for Checkpoint Fabric.
//!
//! The CLI is a thin client over the coordinator's framed TCP RPC; it never
//! performs fabric operations in-process. `--json` enables machine-readable
//! output where applicable.

use clap::{Parser, Subcommand};

/// Checkpoint Fabric: what execution state must survive?
#[derive(Debug, Parser)]
#[command(name = "checkpointfabric", version, about, long_about = None)]
pub struct Cli {
    /// Coordinator listen address.
    #[arg(long, global = true, default_value = "127.0.0.1:7901")]
    pub coordinator: String,

    /// Emit machine-readable JSON for structured results.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Coordinator process control.
    Coordinator {
        #[command(subcommand)]
        cmd: CoordinatorCmd,
    },
    /// Node process control.
    Node {
        #[command(subcommand)]
        cmd: NodeCmd,
    },
    /// Workload management.
    Workload {
        #[command(subcommand)]
        cmd: WorkloadCmd,
    },
    /// Capture a workload checkpoint.
    Capture {
        /// Workload id (hex).
        workload_id: String,
        /// Desired consistency class.
        #[arg(long)]
        consistency: Option<String>,
        /// Quiescence mode.
        #[arg(long, default_value = "none")]
        quiescence: String,
        /// Comma-separated component ids to capture.
        #[arg(long, default_value = "")]
        components: String,
        /// Optional JSON metadata.
        #[arg(long)]
        metadata: Option<String>,
    },
    /// Capture attempt status.
    CaptureStatus { attempt_id: String },
    /// Checkpoint management.
    Checkpoint {
        #[command(subcommand)]
        cmd: CheckpointCmd,
    },
    /// Restore a checkpoint onto a node.
    Restore {
        checkpoint_id: String,
        node: String,
        /// Restore but do not resume execution.
        #[arg(long)]
        no_resume: bool,
        /// Force this restore (ignores fencing constraints).
        #[arg(long)]
        force: bool,
    },
    /// Roll back a workload to a checkpoint (new execution generation).
    Rollback { checkpoint_id: String, node: String },
    /// Fork a new workload from a checkpoint.
    Fork {
        checkpoint_id: String,
        #[arg(long, default_value = "operator")]
        owner: String,
        #[arg(long, default_value = "fork")]
        class: String,
        #[arg(long, default_value = "cpu")]
        backend: String,
        #[arg(long)]
        single_active: bool,
    },
    /// Migrate a workload to a new node via checkpoint.
    Migrate { checkpoint_id: String, node: String },
    /// Evaluate restore compatibility.
    Compatibility {
        checkpoint_id: String,
        /// Target os override (defaults to current).
        #[arg(long)]
        os: Option<String>,
        /// Target arch override (defaults to current).
        #[arg(long)]
        arch: Option<String>,
        /// Target backend override (defaults to checkpoint's).
        #[arg(long)]
        backend: Option<String>,
    },
    /// Show audit records.
    Audit {
        #[arg(long)]
        since_ms: Option<u64>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Run coordinator recovery reconciliation.
    Recovery {
        #[arg(long)]
        dry_run: bool,
    },
    /// Coordinator statistics.
    Stats,
    /// List nodes.
    Nodes,
}

#[derive(Debug, Subcommand)]
pub enum CoordinatorCmd {
    /// Start the coordinator (blocking until shutdown).
    Start {
        #[arg(long)]
        data_dir: String,
        /// Listen address (default 127.0.0.1:7901).
        #[arg(long, default_value = "127.0.0.1:7901")]
        listen: String,
        /// Explicit coordinator epoch.
        #[arg(long)]
        epoch: Option<u64>,
        /// Policy file (JSON).
        #[arg(long)]
        policy_file: Option<String>,
        /// Node staleness in ms.
        #[arg(long)]
        stale_ms: Option<u64>,
    },
    /// Request graceful shutdown of a running coordinator.
    Stop,
}

#[derive(Debug, Subcommand)]
pub enum NodeCmd {
    /// Start a node (blocking until shutdown).
    Start {
        #[arg(long)]
        name: String,
        #[arg(long)]
        data_dir: String,
        /// Coordinator address.
        #[arg(long, default_value = "127.0.0.1:7901")]
        coordinator: String,
        /// Local listen address for coordinator->node RPC.
        #[arg(long, default_value = "127.0.0.1:0")]
        listen: String,
        /// Heartbeat interval in ms.
        #[arg(long)]
        heartbeat_ms: Option<u64>,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkloadCmd {
    /// Create a workload.
    Create {
        #[arg(long)]
        owner: String,
        #[arg(long, default_value = "generic")]
        class: String,
        #[arg(long, default_value = "cpu")]
        backend: String,
        #[arg(long, default_value = "1")]
        schema: u32,
        #[arg(long)]
        single_active: bool,
        /// Node to claim on creation.
        #[arg(long)]
        node: Option<String>,
    },
    /// Inspect a workload.
    Inspect { workload_id: String },
    /// List workloads.
    List,
    /// Fence a workload (revoke active authority).
    Fence { workload_id: String },
    /// Workload lineage.
    Lineage { workload_id: String },
}

#[derive(Debug, Subcommand)]
pub enum CheckpointCmd {
    /// Inspect a checkpoint.
    Inspect { checkpoint_id: String },
    /// List checkpoints (optionally for a workload).
    List {
        #[arg(long)]
        workload_id: Option<String>,
    },
    /// Verify checkpoint integrity (via a replica node).
    Verify { checkpoint_id: String },
    /// Protect a checkpoint from retirement.
    Protect { checkpoint_id: String },
    /// Pin a checkpoint (strongest protection).
    Pin { checkpoint_id: String },
    /// Remove protection.
    Unprotect { checkpoint_id: String },
    /// Retire a checkpoint.
    Retire { checkpoint_id: String },
    /// Checkpoint lineage.
    Lineage { checkpoint_id: String },
}

/// Entry point for `checkpointfabric` binary.
pub fn run(cli: &Cli) -> anyhow::Result<()> {
    crate::cli_impl::run_impl(cli)
}
