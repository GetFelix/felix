//! The openraft type configuration for the metadata group.
//!
//! Commands (`D`) and responses (`R`) are opaque byte vectors here on
//! purpose: this layer moves and persists them, and only the application
//! state machine knows what they mean. That keeps the consensus core and the
//! command set (#338) evolvable independently — a new command variant is not
//! a change to anything in `raft/`.

/// A command's bytes. openraft wants its data to be `Display`; the serialized
/// form is the plain byte array a `Vec<u8>` has, which is what every stored
/// log entry already holds.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub(crate) struct Command(pub(crate) Vec<u8>);

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "command of {} bytes", self.0.len())
    }
}

// On redb the leader id stays openraft's default (`leader_id_adv`, a term
// and a node id): it is part of every stored log id and of the peer format.
#[cfg(not(felix_segment_store))]
openraft::declare_raft_types!(
    pub(crate) TypeConfig:
        D = Command,
        R = Vec<u8>,
        NodeId = u64,
        Node = openraft::impls::BasicNode,
);

// The segment store's log id is a term and an index, so the leader id is
// the standard one: one leader per term.
#[cfg(felix_segment_store)]
openraft::declare_raft_types!(
    pub(crate) TypeConfig:
        D = Command,
        R = Vec<u8>,
        NodeId = u64,
        Node = openraft::impls::BasicNode,
        LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);

#[cfg(not(felix_segment_store))]
pub(crate) type SnapshotData = std::io::Cursor<Vec<u8>>;
/// Built in memory, sent from the stored file, received into a file of its
/// own: a snapshot is never held whole on the way through.
#[cfg(felix_segment_store)]
pub(crate) type SnapshotData = felix_storage::raft_log::SnapshotData;
pub(crate) type Raft = openraft::Raft<TypeConfig, super::store::StateMachineStore>;
/// What a vote names.
pub(crate) type LeaderId = openraft::type_config::alias::LeaderIdOf<TypeConfig>;
/// What a log id names: the same on redb, the term alone on the segment
/// store.
pub(crate) type CommittedLeaderId = openraft::type_config::alias::CommittedLeaderIdOf<TypeConfig>;
pub(crate) type LogId = openraft::LogId<CommittedLeaderId>;
pub(crate) type Vote = openraft::impls::Vote<LeaderId>;
pub(crate) type Membership = openraft::impls::Membership<u64, openraft::impls::BasicNode>;
pub(crate) type Entry = openraft::impls::Entry<CommittedLeaderId, EntryPayload>;
pub(crate) type EntryPayload = openraft::impls::EntryPayload<Command, Membership>;
pub(crate) type StoredMembership = openraft::StoredMembership<CommittedLeaderId, Membership>;
pub(crate) type SnapshotMeta = openraft::storage::SnapshotMeta<CommittedLeaderId, Membership>;
pub(crate) type Snapshot = openraft::storage::Snapshot<CommittedLeaderId, Membership, SnapshotData>;

/// The log id of `term` at `index`. On redb a log id also names its leader,
/// `node`.
#[cfg(test)]
pub(crate) fn test_log_id(term: u64, node: u64, index: u64) -> LogId {
    #[cfg(not(felix_segment_store))]
    let leader = LeaderId {
        term,
        node_id: node,
    };
    #[cfg(felix_segment_store)]
    let leader = {
        let _ = node;
        term.into()
    };
    LogId::new(leader, index)
}
