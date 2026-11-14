//! What names a group and a member, and the openraft type config every
//! shard group shares.

use std::fmt;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use felix_storage::log::{BatchPiece, ProducerBatch, RecordMark};
use felix_storage::raft_log::WrittenToken;
use serde::{Deserialize, Serialize};

use crate::ShardKey;

/// One Raft group: a shard, and how many times its group has been re-formed.
///
/// The epoch starts at 0. Messages carry it, so members of two epochs of
/// one shard never hear each other. A stream shard's broker uses the
/// stream's incarnation.
///
/// openraft's router clones the id for every message, so the shard key is
/// shared and a clone allocates nothing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupId {
    pub shard: Arc<ShardKey>,
    pub epoch: u64,
}

impl GroupId {
    pub fn new(shard: ShardKey, epoch: u64) -> Self {
        Self {
            shard: Arc::new(shard),
            epoch,
        }
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ShardKey {
            tenant_id,
            namespace,
            stream,
            shard,
            kind,
        } = &*self.shard;
        write!(
            f,
            "{tenant_id}/{namespace}/{stream}/{shard} ({kind:?}, epoch {})",
            self.epoch
        )
    }
}

/// One member of a group: a broker, and which copy of the shard it holds.
///
/// The copy id is minted in the shard's directory when a member is first
/// made there, so a copy that lost its directory comes back as a member Raft
/// has never seen. `felix_storage::raft_copy` mints it, and
/// [`super::GroupRuntime::open_copy`] reads it from the directory.
///
/// openraft clones member ids many times per heartbeat (its metrics hold
/// several maps keyed by them), so the broker name is shared: a clone
/// allocates nothing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MemberId {
    pub broker: Arc<str>,
    pub copy: u64,
}

impl MemberId {
    pub fn new(broker: impl Into<Arc<str>>, copy: u64) -> Self {
        Self {
            broker: broker.into(),
            copy,
        }
    }
}

impl fmt::Display for MemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}", self.broker, self.copy)
    }
}

/// The idempotent producer's batch a record belongs to, and its place in
/// it. Stored as the record's mark, so every member's log knows the
/// producer's place the way the leader's does.
///
/// Each record of a batch is an entry of its own and names the whole
/// batch, so a member tells which records of it are in its log even when a
/// leader change has put other entries between them. The broker still
/// refuses a new batch of several records; only the rest of one a log
/// already holds part of is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProducerMark {
    pub(crate) producer_id: u64,
    pub(crate) sequence: u64,
    /// The record's place in the batch, from zero.
    pub(crate) position: u32,
    /// Records in the batch.
    pub(crate) len: u32,
}

impl ProducerMark {
    pub(crate) fn record_mark(mark: Option<Self>) -> RecordMark {
        match mark {
            None => RecordMark::None,
            Some(Self {
                producer_id,
                sequence,
                position: 0,
                len,
            }) => RecordMark::Opens(ProducerBatch {
                producer_id,
                sequence,
                len,
            }),
            Some(Self {
                producer_id,
                sequence,
                position,
                len,
            }) => RecordMark::Piece(BatchPiece {
                producer_id,
                sequence,
                position,
                len,
            }),
        }
    }

    /// The producer mark a stored record carries, if it carries one. A
    /// record that continues the one before it without naming its batch is
    /// refused: no Raft log is written one, and an entry cannot be told
    /// from its neighbours.
    pub(crate) fn of_record(mark: RecordMark) -> io::Result<Option<Self>> {
        match mark {
            RecordMark::Opens(ProducerBatch {
                producer_id,
                sequence,
                len,
            }) => Ok(Some(Self {
                producer_id,
                sequence,
                position: 0,
                len,
            })),
            RecordMark::Piece(BatchPiece {
                producer_id,
                sequence,
                position,
                len,
            }) => Ok(Some(Self {
                producer_id,
                sequence,
                position,
                len,
            })),
            RecordMark::Continues => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a Raft log holds a record that continues a batch without naming it",
            )),
            _ => Ok(None),
        }
    }
}

/// One record of a shard, as an entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Command {
    pub(crate) timestamp_micros: u64,
    pub(crate) payload: Bytes,
    /// Set on the records of an idempotent producer's batch.
    pub(crate) producer: Option<ProducerMark>,
    /// Set by a proposer that waits to hear where the record was written.
    /// It rides on the entry from `client_write` to the leader's own append
    /// and nowhere else: it is not in the record and not in a message, so a
    /// follower's command, and one read back from the log, has none.
    #[serde(skip)]
    pub(crate) written: Option<WrittenToken>,
}

/// The same record, whoever waits on it.
impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        // Destructured so a new field has to be placed here.
        let Self {
            timestamp_micros,
            payload,
            producer,
            written: _,
        } = self;
        *timestamp_micros == other.timestamp_micros
            && *payload == other.payload
            && *producer == other.producer
    }
}

impl Eq for Command {}

impl fmt::Display for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "record of {} bytes", self.payload.len())
    }
}

/// What applying an entry answers. A shard's state is its log, so there is
/// nothing to say.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Applied;

openraft::declare_raft_types!(
    pub(crate) ShardTypes:
        D = Command,
        R = Applied,
        NodeId = MemberId,
        Node = openraft::impls::EmptyNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, MemberId>,
);

pub(crate) type Membership = openraft::impls::Membership<MemberId, openraft::impls::EmptyNode>;
