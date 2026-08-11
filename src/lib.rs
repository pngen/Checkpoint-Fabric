//! Checkpoint Fabric: a vendor-neutral execution-survival runtime for AI infrastructure.
//!
//! Checkpoint Fabric answers one question: *what execution state must survive?*
//!
//! It is the fifth and final runtime in the accelerator-infrastructure sequence
//! FlashTier -> Context Fabric -> Compute Fabric -> Reclaim Fabric -> Checkpoint Fabric.
//! It captures, validates, seals, persists, replicates, restores, resumes, migrates,
//! forks, rolls back, and retires coherent execution state without binding to one
//! model framework, accelerator vendor, orchestration layer, or storage backend.
//!
//! The library is organized by real system responsibility:
//! [`coordinator`], [`node`], [`workload`], [`checkpoint`], [`frontier`],
//! [`lifecycle`], [`capture`], [`restore`], [`migration`], [`lineage`],
//! [`compatibility`], [`manifest`], [`integrity`], [`compression`], [`storage`],
//! [`persistence`], [`transport`], [`recovery`], [`audit`], [`policy`],
//! [`providers`], [`protocol`], and [`integrations`].

pub mod audit;
pub mod capture;
pub mod checkpoint;
pub mod cli;
pub mod cli_impl;
pub mod compatibility;
pub mod compression;
pub mod coordinator;
pub mod errors;
pub mod failpoints;
pub mod frontier;
pub mod id;
pub mod integrations;
pub mod integrity;
pub mod lifecycle;
pub mod lineage;
pub mod manifest;
pub mod migration;
pub mod node;
pub mod persistence;
pub mod policy;
pub mod protocol;
pub mod providers;
pub mod recovery;
pub mod restore;
pub mod sideeffect;
pub mod storage;
pub mod time;
pub mod transport;
pub mod workload;

pub use errors::{FabricError, FabricResult};
pub use id::Id;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const PROTOCOL_MAGIC: &[u8; 4] = b"CFAB";
pub const PROTOCOL_VERSION: u8 = 1;
pub const FORMAT_VERSION: u32 = 1;
