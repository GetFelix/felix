//! What names a group and a member, and the openraft type config every
//! shard group shares.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::ShardKey;

/// One Raft group: a shard, and how many times its group has been re-formed.
///
/// The epoch starts at 0. Messages carry it, so members of two epochs of
/// one shard never hear each other.
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
/// has never seen. Minting it is not this module's job; the caller passes it.
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

/// One record of a shard, as an entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Command {
    pub(crate) timestamp_micros: u64,
    pub(crate) payload: Bytes,
}

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
