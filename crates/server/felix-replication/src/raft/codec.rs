//! How a shard group's entries, membership and member ids are stored.

use std::collections::BTreeSet;
use std::io;

use bytes::Bytes;
use felix_storage::log::{AppendRecord, LogRecord};
use felix_storage::raft_log::{EntryCodec, WrittenToken};
use serde::{Deserialize, Serialize};

use super::membership::TargetVersion;
use super::types::{Command, MemberId, Membership, ProducerMark, ShardTypes};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ShardCodec;

/// A membership as a membership record's payload, in JSON. Felix's own
/// layout, so an openraft upgrade cannot change bytes already on disk.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredMembership {
    /// 2, or 1 from before memberships carried a target version. A reader
    /// refuses any other.
    pub(crate) version: u32,
    /// The voter sets: one, or two during a joint change.
    pub(crate) voters: Vec<BTreeSet<MemberId>>,
    /// Members that are not voters.
    pub(crate) learners: BTreeSet<MemberId>,
    /// The version of the target the membership was made from. Layout 1
    /// has none, which reads as the zero version.
    #[serde(default)]
    pub(crate) target: Option<TargetVersion>,
}

pub(crate) const MEMBERSHIP_VERSION: u32 = 2;
/// The layout without a target version.
const UNVERSIONED: u32 = 1;

impl StoredMembership {
    pub(crate) fn of(membership: &Membership) -> Self {
        Self {
            version: MEMBERSHIP_VERSION,
            voters: membership.get_joint_config().clone(),
            learners: membership.learner_ids().collect(),
            target: Some(membership.version()),
        }
    }

    pub(crate) fn membership(self) -> io::Result<Membership> {
        // Layout 2 always names a version and layout 1 never does. A
        // payload that says otherwise was not written by a build that knew
        // what the field means, and reading a missing version as zero
        // would let any target pass for newer.
        let target = match (self.version, self.target) {
            (MEMBERSHIP_VERSION, Some(target)) => target,
            (UNVERSIONED, None) => TargetVersion::default(),
            (MEMBERSHIP_VERSION, None) => {
                return Err(invalid(
                    "a layout 2 membership names no target version".into(),
                ));
            }
            (UNVERSIONED, Some(_)) => {
                return Err(invalid(
                    "a layout 1 membership names a target version".into(),
                ));
            }
            (other, _) => {
                return Err(invalid(format!(
                    "membership layout {other} is not one this build reads"
                )));
            }
        };
        Ok(Membership::new(self.voters, self.learners, target))
    }
}

impl EntryCodec<ShardTypes> for ShardCodec {
    fn encode_data(&self, data: &Command) -> io::Result<AppendRecord> {
        Ok(AppendRecord {
            payload: data.payload.clone(),
            timestamp_micros: data.timestamp_micros,
            mark: ProducerMark::record_mark(data.producer),
            publisher: None,
            headers: None,
        })
    }

    fn decode_data(&self, record: LogRecord) -> io::Result<Command> {
        Ok(Command {
            timestamp_micros: record.timestamp_micros,
            payload: record.payload,
            producer: ProducerMark::of_record(record.mark)?,
            written: None,
        })
    }

    fn written_token(&self, data: &Command) -> Option<WrittenToken> {
        data.written.clone()
    }

    fn encode_membership(&self, membership: &Membership) -> io::Result<Bytes> {
        serde_json::to_vec(&StoredMembership::of(membership))
            .map(Bytes::from)
            .map_err(io::Error::other)
    }

    fn decode_membership(&self, payload: &[u8]) -> io::Result<Membership> {
        serde_json::from_slice::<StoredMembership>(payload)
            .map_err(|err| invalid(format!("a membership record does not parse: {err}")))?
            .membership()
    }

    fn encode_node_id(&self, node_id: &MemberId) -> String {
        node_id.to_string()
    }

    fn decode_node_id(&self, leader: &str) -> io::Result<MemberId> {
        // A broker id may hold a `#`, a copy id cannot.
        let (broker, copy) = leader
            .rsplit_once('#')
            .ok_or_else(|| invalid(format!("{leader:?} names no member")))?;
        let copy = copy
            .parse()
            .map_err(|_| invalid(format!("{leader:?} names no member")))?;
        Ok(MemberId::new(broker, copy))
    }
}

pub(crate) fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
