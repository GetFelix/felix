//! What a reader is told when its shard moves to another broker, or its
//! stream is deleted.
//!
//! A broker that stops serving a shard ends that shard's subscriptions and
//! cache watches. Each reader's feed records why before it closes, so the
//! delivery path can send a last frame saying where to resume, or that there
//! is nothing to resume, instead of just finishing the stream.

/// Where a shard went when this broker stopped serving it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardHandoff {
    /// The broker taking the shard, when this one knows.
    pub node_id: Option<String>,
    /// That broker's client address, when the cluster publishes one.
    pub addr: Option<String>,
    /// The assignment generation that moved the shard.
    pub generation: u64,
}

/// A reader's feed ended because its shard moved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMoved {
    /// The first offset this reader was not offered; see
    /// `felix_wire::Message::ShardMoved`. `None` when there is no offset that
    /// means anything on the next owner.
    pub resume_from: Option<u64>,
    /// Where the shard went.
    pub to: ShardHandoff,
}

/// Why a stream's subscriptions are being ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Ending {
    /// The shard went elsewhere.
    Moved(ShardHandoff),
    /// The stream was deleted.
    Deleted,
}

/// Why a subscription's feed ended, recorded before its queue closes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FeedEnd {
    Moved(ShardMoved),
    Deleted,
}
