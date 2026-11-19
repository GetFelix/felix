//! What this broker has done when it acknowledges a publish to a stream, for
//! `stream_shards_view`. See `docs/protocol.md`, "AckDurability".

use felix_storage::log::FsyncMode;
use felix_wire::{AckCopies, AckDurability, AckFsync, AckPoint};

/// What decides the answer, read off the broker and the stream.
pub(super) struct Facts {
    pub(super) durable: bool,
    pub(super) quorum_consistency: bool,
    /// Every shard of the stream is a member of a Raft group on this broker,
    /// so every ack is a majority commit. A shard owned by another broker, or
    /// a group not yet formed here, makes this false.
    pub(super) raft: bool,
    /// Every shard of the stream has a replica besides its leader in this
    /// broker's routing snapshot.
    pub(super) replicated: bool,
    /// Acks wait for the write whatever the stream: the broker-wide setting or
    /// this client's `FEATURE_ACK_ON_COMMIT`.
    pub(super) commit_ack: bool,
    /// Acks to a durable stream wait for the write.
    pub(super) ack_durable_on_commit: bool,
    pub(super) fsync: Option<FsyncMode>,
}

/// Understates rather than overstates (`raft` and `replicated` hold only when true of every shard): a publish forwarded to another owner
/// is answered after the owner's write whatever this says.
pub(super) fn describe(facts: &Facts) -> AckDurability {
    let waits_for_majority = facts.quorum_consistency || (facts.raft && facts.durable);
    let ack_at =
        if facts.commit_ack || (facts.ack_durable_on_commit && facts.durable) || waits_for_majority
        {
            AckPoint::Write
        } else {
            AckPoint::Enqueue
        };
    // A majority is claimed only when every shard is known to have a replica
    // and the stream has a log. A single node, an in-memory stream or a shard
    // with no replica holds one copy whatever the level says.
    let copies = if waits_for_majority && facts.durable && facts.replicated {
        AckCopies::Quorum
    } else {
        AckCopies::Leader
    };
    let fsync = facts
        .durable
        .then_some(())
        .and(facts.fsync)
        .map(|mode| match mode {
            FsyncMode::None => AckFsync::None,
            FsyncMode::Periodic { .. } => AckFsync::Periodic,
            FsyncMode::OnCommit => AckFsync::OnCommit,
        });
    AckDurability {
        ack_at,
        copies,
        fsync,
    }
}

#[cfg(test)]
mod tests;
