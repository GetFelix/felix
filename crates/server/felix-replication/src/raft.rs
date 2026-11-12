//! The per-shard Raft runtime: many openraft groups on one broker, each on
//! a shard's log through `felix_storage::raft_log`, all driven by one
//! ticker.
//!
//! A [`GroupRuntime`] holds this broker's member of every group it was told
//! to open. It owns no timer per group: every group is built with openraft's
//! own tick off, and the runtime's one ticker task runs each group's timer
//! check every [`RuntimeConfig::tick`]. Messages between brokers go through a
//! [`RaftTransport`], and [`GroupRuntime::handle`] takes the ones that
//! arrive.
//!
//! Nothing in the broker calls this yet. No shard runs on it, and the
//! `raft` feature that compiles it is in no release. The design is in
//! `docs/per-shard-raft.md`, "Group runtime".
//!
//! openraft types stay inside this module and `felix_storage::raft_log`.

mod codec;
mod group;
mod machine;
mod network;
mod runtime;
#[cfg(test)]
mod tests;
mod ticker;
mod types;

pub use network::{Addressee, AgreeingPeers, RaftReply, RaftRequest, RaftTransport, Refused};
pub use runtime::{
    GroupRuntime, GroupStatus, InitializeError, InvalidTimers, OpenError, Role, RuntimeConfig,
};
pub use types::{GroupId, MemberId};

pub use felix_storage::raft_log::{LogPoint, LogPositions};
