//! The openraft type configuration for the metadata group.
//!
//! Commands (`D`) and responses (`R`) are opaque byte vectors here on
//! purpose: this layer moves and persists them, and only the application
//! state machine knows what they mean. That keeps the consensus core and the
//! command set (#338) evolvable independently — a new command variant is not
//! a change to anything in `raft/`.
use std::io::Cursor;

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

// The leader id stays openraft's default (`leader_id_adv`, a term and a node
// id): it is part of every stored log id and of the peer format.
openraft::declare_raft_types!(
    pub(crate) TypeConfig:
        D = Command,
        R = Vec<u8>,
        NodeId = u64,
        Node = openraft::impls::BasicNode,
);

pub(crate) type SnapshotData = Cursor<Vec<u8>>;
pub(crate) type Raft = openraft::Raft<TypeConfig, super::store::StateMachineStore>;
pub(crate) type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
pub(crate) type LogId = openraft::LogId<LeaderId>;
pub(crate) type Vote = openraft::impls::Vote<LeaderId>;
pub(crate) type Membership = openraft::impls::Membership<u64, openraft::impls::BasicNode>;
pub(crate) type Entry = openraft::impls::Entry<LeaderId, EntryPayload>;
pub(crate) type EntryPayload = openraft::impls::EntryPayload<Command, Membership>;
pub(crate) type StoredMembership = openraft::StoredMembership<LeaderId, Membership>;
pub(crate) type SnapshotMeta = openraft::storage::SnapshotMeta<LeaderId, Membership>;
pub(crate) type Snapshot = openraft::storage::Snapshot<LeaderId, Membership, SnapshotData>;
