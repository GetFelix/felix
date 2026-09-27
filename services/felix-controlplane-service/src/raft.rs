//! The metadata Raft core: group lifecycle, persistent log, and the seam.
//!
//! This module is the **entire** openraft surface of the control plane — no
//! openraft type escapes it. Everything outside talks to [`RaftHandle`] and
//! implements [`AppStateMachine`], which is what lets the consensus library
//! be upgraded (openraft is pre-1.0) or replaced without touching the store
//! traits or handlers. The design, including why openraft and why the log
//! lives outside `felix-storage`, is `docs/metadata-raft-design.md`.
//!
//! What lives where:
//! - `handle` — starting a member, forming and changing the group, stopping.
//! - `proposal` — committing a command from any member, forwarding to the
//!   leader when this one is not it.
//! - `leader` — requests only the leader answers, from state it keeps
//!   outside the log, forwarded there from any member.
//! - `join` — entering the group at startup: forming it on first boot, or
//!   catching up without a vote after starting empty.
//! - `health` — whether this member is fit to serve, and its gauges.
//! - `store` — the Raft log, vote, and current snapshot, in one crash-safe
//!   redb file per instance. Consensus state is the one thing here that must
//!   never lie about being on disk.
//! - `network` / `http` — Raft RPCs as JSON over HTTP between instances,
//!   on a peer listener of their own, never the public API one.
//! - `peer` — how that traffic is authenticated: cluster id, peer token,
//!   optional mTLS.
//! - `types` — the openraft type configuration. Commands and responses are
//!   opaque bytes at this layer; their meaning belongs to the application
//!   state machine (#338 gives them theirs).
mod handle;
mod health;
mod http;
mod join;
mod leader;
mod network;
mod peer;
mod proposal;
mod store;
mod types;

pub use handle::RaftHandle;
pub use leader::{AskLeaderError, LeaderService, NotLeader};
pub use peer::{CLUSTER_ID_HEADER, PeerSecurity, PeerTls};

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

/// This instance's identity within the Raft group.
pub type NodeId = u64;

/// Everything needed to start this instance's member of the group.
#[derive(Debug, Clone)]
pub struct RaftSettings {
    pub node_id: NodeId,
    /// Where the log, vote, and snapshots live. One directory per instance,
    /// surviving restarts — this is the state that makes a restart a rejoin
    /// rather than a fresh member.
    pub data_dir: PathBuf,
    /// How often the leader heartbeats followers.
    pub heartbeat_interval: Duration,
    /// Election timeout range; the minimum must comfortably exceed the
    /// heartbeat interval or healthy followers call elections.
    pub election_timeout: (Duration, Duration),
    /// Snapshot after this many log entries since the last one. Metadata
    /// state is small, so snapshots are cheap and the log stays short.
    pub snapshot_logs_since_last: u64,
    /// Log entries to keep behind the snapshot, so a briefly-lagging
    /// follower catches up from the log rather than a snapshot install.
    pub logs_kept_behind_snapshot: u64,
    /// Overall budget for one proposal, elections and forwarding included.
    ///
    /// openraft's own write path waits indefinitely for a commit — a leader
    /// that lost quorum queues proposals forever — so the bound has to live
    /// here, where "no quorum" becomes an error a caller can surface
    /// instead of a hang inside the seam.
    pub write_timeout: Duration,
    /// How peer traffic is authenticated, both directions.
    pub security: PeerSecurity,
    /// Where each member's peer listener is, by id. Consulted before the
    /// address recorded in the membership, so moving the peer port is a
    /// config change rather than a membership change. Empty means the
    /// recorded addresses are used as they are.
    pub peer_addrs: BTreeMap<NodeId, String>,
}

impl RaftSettings {
    /// Defaults sized for a three-instance metadata group: elections settle
    /// in about a second, and the log is compacted often because state is
    /// kilobytes.
    pub fn new(node_id: NodeId, data_dir: PathBuf, security: PeerSecurity) -> Self {
        Self {
            node_id,
            data_dir,
            security,
            peer_addrs: BTreeMap::new(),
            heartbeat_interval: Duration::from_millis(150),
            election_timeout: (Duration::from_millis(600), Duration::from_millis(1200)),
            snapshot_logs_since_last: 500,
            logs_kept_behind_snapshot: 100,
            write_timeout: Duration::from_secs(10),
        }
    }
}

/// Whether an empty member may form a brand-new group, etcd-style.
///
/// A member with no state cannot tell a first boot from a lost volume, and
/// a majority of lost volumes would otherwise vote a fresh, empty group into
/// existence: new signing keys, every generation back to zero. `Existing`
/// makes that impossible; `New` is the operator saying this is day 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum InitialClusterState {
    New,
    #[default]
    Existing,
}

/// The application half of the state machine seam.
///
/// The Raft core feeds every committed command to exactly one of these, in
/// log order, on every member. The contract that makes that meaningful:
///
/// - **`apply` must be deterministic.** Same command sequence, same state,
///   byte-identical `snapshot()` — on every instance, every time. No clocks,
///   no randomness, nothing read from outside the command and prior state.
///   Anything nondeterministic (timestamps, generated keys) is decided
///   *before* the command is proposed and carried inside it.
/// - The implementation owns its interior mutability; calls arrive
///   serialized from a single apply loop, but snapshot building may read
///   concurrently with nothing else, so a lock the implementation already
///   holds for serving reads is the right shape (the metadata store's
///   `RwLock`s, in #338).
/// - `restore` replaces the whole state with a previously produced snapshot.
///
/// Async because the metadata store's interior locks are async; the
/// determinism rule is about *what* apply computes, not how it schedules.
#[async_trait::async_trait]
pub trait AppStateMachine: Send + Sync + 'static {
    async fn apply(&self, command: &[u8]) -> Vec<u8>;
    async fn snapshot(&self) -> Vec<u8>;
    async fn restore(&self, snapshot: &[u8]);

    /// Replace any clock reading the proposer baked into `command` with
    /// `now_millis`, or return `None` to propose the bytes unchanged.
    ///
    /// Determinism forces the nondeterministic parts of a command to be
    /// decided before it is proposed, which means some commands carry a wall
    /// clock. Whose clock that is matters when the value is later compared
    /// against one read somewhere else: the receiving instance's makes the
    /// comparison depend on two machines' clocks agreeing. This is the hook
    /// that lets the leader's be the only one appended — the seam calls it
    /// on the leader, immediately before the entry enters the log, so
    /// commands stay opaque here and their meaning stays with the
    /// application.
    fn restamp(&self, command: &[u8], now_millis: u64) -> Option<Vec<u8>> {
        let _ = (command, now_millis);
        None
    }
}

/// A snapshot of where this member stands, in the seam's own vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftStatus {
    pub id: NodeId,
    /// The leader as this member currently believes; `None` during an
    /// election or before the group is initialized.
    pub leader: Option<NodeId>,
    pub term: u64,
    pub last_applied_index: Option<u64>,
    /// Voting members of the current membership config.
    pub voters: Vec<NodeId>,
}

/// Whether a background task that must run on exactly one instance should
/// run here, now.
///
/// `Always` is the non-Raft deployments' answer — the Postgres and in-memory
/// stores make duplicate sweeps safe, so every instance runs them. Under Raft the gate is a
/// **linearizable leadership check** (openraft's read-index), which answers
/// two questions at once: this instance is the leader, *and* its applied
/// state is current enough to decide from — a deposed leader that has not
/// heard the news yet fails the check rather than sweeping from stale state.
#[derive(Clone)]
pub enum LeadershipGate {
    Always,
    Leader(RaftHandle),
}

impl LeadershipGate {
    pub async fn holds(&self) -> bool {
        match self {
            Self::Always => true,
            Self::Leader(handle) => handle.confirm_leadership().await,
        }
    }
}
