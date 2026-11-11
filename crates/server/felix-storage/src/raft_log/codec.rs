//! Entries as records and back.

use std::io;
use std::ops::Deref;

use bytes::Bytes;
use openraft::RaftTypeConfig;
use openraft::entry::{Entry, EntryPayload, RaftEntry};
use openraft::type_config::alias::CommittedLeaderIdOf;

use super::{LogTypes, generation_of, invalid, log_id, term_of};
use crate::log::{AppendRecord, LogRecord, Offset, RecordMark};

/// How one user of the log stores what only it understands: its commands,
/// its membership and its node ids.
///
/// Storage keeps the bytes as given. A decode that fails stops the read.
pub trait EntryCodec<C: RaftTypeConfig>: Send + Sync + 'static {
    /// A command as the record that holds it. The record's mark must not be
    /// [`RecordMark::GenerationStart`] or [`RecordMark::Membership`], which
    /// are how a blank and a membership entry are stored.
    fn encode_data(&self, data: &C::D) -> io::Result<AppendRecord>;

    /// The command a record holds. Given every record that is not a blank or
    /// a membership entry.
    fn decode_data(&self, record: LogRecord) -> io::Result<C::D>;

    /// A membership as a [`RecordMark::Membership`] record's payload.
    fn encode_membership(&self, membership: &C::Membership) -> io::Result<Bytes>;

    fn decode_membership(&self, payload: &[u8]) -> io::Result<C::Membership>;

    /// A node id as the ballot file names a leader.
    fn encode_node_id(&self, node_id: &C::NodeId) -> String;

    fn decode_node_id(&self, leader: &str) -> io::Result<C::NodeId>;
}

/// The entry the record holds. `first_index` is the log's first index: a
/// record below it was purged, or predates the Raft log, and is never an
/// entry.
pub(super) fn entry_from_record<C, K>(
    codec: &K,
    record: LogRecord,
    first_index: Offset,
) -> io::Result<C::Entry>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    let offset = record.offset;
    if offset < first_index {
        return Err(invalid(format!(
            "the record at offset {offset} is below the log's first index {first_index}"
        )));
    }
    let term = term_of(record.generation, offset)?;
    let payload = match record.mark {
        RecordMark::GenerationStart => {
            // The payload repeats the generation. One that disagrees with
            // the header was not written as this term's blank entry.
            let stamped = generation_of(term)?.to_be_bytes();
            if record.payload.as_ref() != stamped.as_slice() {
                return Err(invalid(format!(
                    "the generation-start record at offset {offset} names another generation than its header"
                )));
            }
            EntryPayload::Blank
        }
        RecordMark::Membership => {
            EntryPayload::Membership(codec.decode_membership(&record.payload)?)
        }
        _ => EntryPayload::Normal(codec.decode_data(record)?),
    };
    Ok(C::Entry::new(log_id::<C>(term, offset), payload))
}

/// The record that holds `entry`, and the generation to stamp it with. The
/// record goes at offset `entry.log_id.index`.
// Only the tests write until the write side lands.
#[cfg_attr(not(test), expect(dead_code))]
pub(super) fn record_from_entry<C, K>(
    codec: &K,
    entry: &Entry<CommittedLeaderIdOf<C>, EntryPayload<C::D, C::Membership>>,
    timestamp_micros: u64,
) -> io::Result<(AppendRecord, u64)>
where
    C: LogTypes,
    CommittedLeaderIdOf<C>: From<u64> + Deref<Target = u64>,
    K: EntryCodec<C>,
{
    let generation = generation_of(**entry.log_id.committed_leader_id())?;
    let internal = |mark, payload| AppendRecord {
        payload,
        timestamp_micros,
        mark,
        publisher: None,
        headers: None,
    };
    let record = match &entry.payload {
        EntryPayload::Blank => internal(
            RecordMark::GenerationStart,
            Bytes::copy_from_slice(&generation.to_be_bytes()),
        ),
        EntryPayload::Membership(membership) => {
            internal(RecordMark::Membership, codec.encode_membership(membership)?)
        }
        EntryPayload::Normal(data) => {
            let record = codec.encode_data(data)?;
            if matches!(
                record.mark,
                RecordMark::GenerationStart | RecordMark::Membership
            ) {
                return Err(invalid(format!(
                    "a command was encoded as a {:?} record",
                    record.mark
                )));
            }
            record
        }
    };
    Ok((record, generation))
}
