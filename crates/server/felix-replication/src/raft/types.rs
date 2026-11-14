//! What names a group and a member, and the openraft type config every
//! shard group shares.

use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::sync::Arc;

use bytes::Bytes;
use felix_storage::log::{ProducerBatch, RecordMark};
use felix_storage::raft_log::WrittenToken;
use openraft::errors::MembershipError;
use openraft::impls::{ChangeMembers, EmptyNode};
use openraft::membership::{NodeRef, RaftMembership, RaftMembershipEdit, RaftMembershipEditOps};
use serde::{Deserialize, Serialize};

use super::membership::TargetVersion;
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

/// The idempotent producer's batch a record is. Stored as the record's
/// mark, so every member's log knows the producer's place the way the
/// leader's does.
///
/// A batch is one record. One of several would be several entries, which a
/// leader change or a lapsed lease can cut anywhere, so the broker refuses
/// it before it gets here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ProducerMark {
    pub(crate) producer_id: u64,
    pub(crate) sequence: u64,
}

impl ProducerMark {
    pub(crate) fn record_mark(mark: Option<Self>) -> RecordMark {
        match mark {
            None => RecordMark::None,
            Some(Self {
                producer_id,
                sequence,
            }) => RecordMark::Opens(ProducerBatch {
                producer_id,
                sequence,
                len: 1,
            }),
        }
    }

    /// The producer mark a stored record carries, if it carries one. A
    /// record of a longer batch is refused: no Raft log is written one, and
    /// read as a batch of its own it would move the producer's place.
    pub(crate) fn of_record(mark: RecordMark) -> io::Result<Option<Self>> {
        match mark {
            RecordMark::Opens(ProducerBatch {
                producer_id,
                sequence,
                len: 1,
            }) => Ok(Some(Self {
                producer_id,
                sequence,
            })),
            RecordMark::Opens(_) | RecordMark::Continues => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a Raft log holds a record of an idempotent batch of several records",
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

/// The voter sets and learners of a group, as openraft's own type holds them.
pub(crate) type Sets = openraft::impls::Membership<MemberId, EmptyNode>;

/// A group's membership: who votes and who only receives the log, and the
/// version of the target it was made from. The version rides in the
/// membership entry itself, so it is committed, replicated and kept through
/// a snapshot with the sets it describes.
///
/// Quorums are openraft's, taken from the sets alone. The version changes
/// no vote.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Membership {
    sets: Sets,
    version: TargetVersion,
}

impl Membership {
    /// `voters` is one set, or two during a joint change. Every member of
    /// them and of `learners` is in the membership.
    pub(crate) fn new(
        voters: Vec<BTreeSet<MemberId>>,
        learners: BTreeSet<MemberId>,
        version: TargetVersion,
    ) -> Self {
        let members: BTreeSet<MemberId> =
            voters.iter().flatten().cloned().chain(learners).collect();
        Self {
            sets: Sets::new_with_defaults(voters, members),
            version,
        }
    }

    pub(crate) fn version(&self) -> TargetVersion {
        self.version
    }

    /// The voter sets: one, or two during a joint change.
    pub(crate) fn get_joint_config(&self) -> &Vec<BTreeSet<MemberId>> {
        self.sets.get_joint_config()
    }

    pub(crate) fn voter_ids(&self) -> impl Iterator<Item = MemberId> {
        self.sets.voter_ids()
    }

    /// The members that are not voters.
    pub(crate) fn learner_ids(&self) -> impl Iterator<Item = MemberId> + '_ {
        self.sets.non_write_voter_ids()
    }
}

/// A group formed from its first set carries the zero version, which no
/// target is older than.
impl From<BTreeSet<MemberId>> for Membership {
    fn from(voters: BTreeSet<MemberId>) -> Self {
        Self::new(vec![voters], BTreeSet::new(), TargetVersion::default())
    }
}

impl fmt::Display for Membership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at version {}", self.sets, self.version)
    }
}

impl RaftMembership for Membership {
    type NodeId = MemberId;
    type Node = EmptyNode;
    type QuorumSet = <Sets as RaftMembership>::QuorumSet;
    type Iter = <Sets as RaftMembership>::Iter;

    fn read_quorum_set(&self) -> Self::QuorumSet {
        self.sets.read_quorum_set()
    }

    fn write_quorum_set(&self) -> Self::QuorumSet {
        self.sets.write_quorum_set()
    }

    // openraft's own answer. The version must never make a change direct
    // that the voter sets need a joint for.
    fn supports_direct_transition_to(&self, other: &Self) -> bool {
        self.sets.supports_direct_transition_to(&other.sets)
    }

    fn get_node(&self, node_id: &MemberId) -> Option<NodeRef<'_, EmptyNode>> {
        RaftMembership::get_node(&self.sets, node_id)
    }

    fn node_ids(&self) -> Self::Iter {
        RaftMembership::node_ids(&self.sets)
    }

    // The joint on the way to `goal` is made from `goal`'s target, so it
    // carries that version: a leader that finds the joint must not take an
    // older target for newer than it.
    fn joint_with(&self, goal: &Self) -> Self {
        Self {
            sets: self.sets.joint_with(&goal.sets),
            version: goal.version,
        }
    }
}

/// A change to a group's membership, and the version the result carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MembershipEdit {
    change: ChangeMembers<MemberId, EmptyNode>,
    /// `None` keeps the version of the membership the edit is applied to.
    version: Option<TargetVersion>,
}

impl MembershipEdit {
    /// Add `member` as a learner, at the version the membership has.
    pub(crate) fn learner(member: MemberId) -> Self {
        <Self as RaftMembershipEditOps<Membership>>::add_learner(member, EmptyNode {})
    }

    /// Make `voters` the only voters, at `version`. Every one of them must
    /// already be a member.
    pub(crate) fn voters(voters: BTreeSet<MemberId>, version: TargetVersion) -> Self {
        Self {
            change: ChangeMembers::ReplaceAllVoters(voters),
            version: Some(version),
        }
    }

    /// Leave a joint membership for `voters`, its new set, at the version
    /// the joint carries.
    pub(crate) fn flatten(voters: BTreeSet<MemberId>) -> Self {
        Self {
            change: ChangeMembers::ReplaceAllVoters(voters),
            version: None,
        }
    }
}

impl fmt::Display for MembershipEdit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.version {
            Some(version) => write!(f, "{} at version {version}", self.change),
            None => write!(f, "{}", self.change),
        }
    }
}

impl RaftMembershipEdit<Membership> for MembershipEdit {
    type Error = Infallible;

    fn apply(
        self,
        membership: &Membership,
        _retain: bool,
    ) -> Result<Membership, MembershipError<MemberId, Infallible>> {
        // Never keep a voter that leaves as a learner, whatever the caller
        // asks. openraft lets a learner go on leading, and a leader that
        // wrote itself out would hold a lease on a set it is not in.
        let sets = self.change.apply(&membership.sets, false)?;
        Ok(Membership {
            sets,
            version: self.version.unwrap_or(membership.version),
        })
    }
}

impl RaftMembershipEditOps<Membership> for MembershipEdit {
    fn add_learner(node_id: MemberId, node: EmptyNode) -> Self {
        Self {
            change:
                <ChangeMembers<MemberId, EmptyNode> as RaftMembershipEditOps<Sets>>::add_learner(
                    node_id, node,
                ),
            version: None,
        }
    }
}

openraft::declare_raft_types!(
    pub(crate) ShardTypes:
        D = Command,
        R = Applied,
        NodeId = MemberId,
        Node = EmptyNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, MemberId>,
        AsyncRuntime = super::draws::ShardRuntime,
        Membership = Membership,
        MembershipEdit = MembershipEdit,
);
