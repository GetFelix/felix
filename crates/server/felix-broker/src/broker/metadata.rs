//! What the broker records about each registered stream and cache.

/// How much of a shard's replica set must hold a record before the publish that
/// wrote it is acknowledged.
///
/// The levels differ in what a client may conclude from an acknowledgement, and
/// `docs/replication-design.md` states each precisely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsistencyLevel {
    /// Durable on the leader. Exposes the leader-only loss window: records
    /// acknowledged but not yet shipped are lost if the leader's storage is.
    /// The window is bounded by the replication lag, which is exported.
    ///
    /// The default, so a stream created before replication existed — or by a
    /// caller that does not ask — behaves exactly as it did.
    #[default]
    Leader,
    /// Durable on a majority of the replica set, the leader included. No loss
    /// window: any failure within that majority preserves the record.
    Quorum,
}

impl ConsistencyLevel {
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            Self::Leader => 0,
            Self::Quorum => 1,
        }
    }

    /// Anything unrecognised reads as `Leader`, which is unreachable: the only
    /// writer is [`Self::as_u8`].
    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Quorum,
            _ => Self::Leader,
        }
    }
}

/// What the broker records about a registered stream.
#[derive(Debug, Clone)]
pub struct StreamMetadata {
    /// When true, every publish is written to disk before it is fanned out or
    /// acknowledged. Requires the broker to have been built with
    /// [`Broker::with_durable_storage`](crate::Broker::with_durable_storage).
    pub durable: bool,
    pub shards: u32,
    /// What an acknowledgement of a publish to this stream means.
    pub consistency: ConsistencyLevel,
    /// This stream's retention. A bound left unset is the broker's own
    /// (`FELIX_DURABLE_RETENTION_BYTES`, `FELIX_DURABLE_RETENTION_SECONDS`).
    /// Ignored for a stream that is not durable.
    pub retention: felix_storage::log::Retention,
}

impl Default for StreamMetadata {
    fn default() -> Self {
        Self {
            durable: false,
            shards: 1,
            consistency: ConsistencyLevel::Leader,
            retention: felix_storage::log::Retention::default(),
        }
    }
}

/// What the broker knows about a registered cache.
#[derive(Debug, Clone, Default)]
pub struct CacheMetadata {
    /// What acknowledging a write to this cache means. A cache's shards are
    /// replicated like a stream's, so `Quorum` holds the acknowledgement until
    /// a majority of the shard's replica set has the write.
    pub consistency: ConsistencyLevel,
}
