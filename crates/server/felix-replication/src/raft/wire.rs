//! The bytes of a Raft message between brokers: the body of a `Raft` or
//! `RaftAnswer` frame.
//!
//! Felix's own layout, written field by field, so an openraft upgrade cannot
//! change what is on the wire. Every count is checked against the bytes left
//! before anything is sized from it. The layout is in
//! `docs/internal-protocol.md`, "Raft messages".

use std::collections::BTreeSet;
use std::io;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use felix_storage::raft_log::LogPoint;
use felix_wire::internal::{MAX_IDENT_BYTES, RaftLayouts};
use openraft::entry::RaftEntry;
use openraft::impls::leader_id_std::LeaderId;
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, ConflictHint, SnapshotResponse,
    TransferLeaderError, TransferLeaderRequest, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotMetaOf, VoteOf};
use openraft::{EntryPayload, LogId, StoredMembership, Vote};

use super::codec::invalid;
use super::membership::TargetVersion;
use super::network::{Addressee, Ask, Beat, RaftReply, RaftRequest, Refused, Reply, Request};
use super::types::{Command, CondPlace, GroupId, MemberId, Membership, ProducerMark, ShardTypes};
use crate::{ShardKey, ShardKind};

/// The first byte of every body. A reader refuses any other, so a peer
/// on another layout is refused by name rather than misread. 2 added the
/// report byte to the request header, 3 the data of a snapshot, 4 the target
/// version to a membership, 5 the piece entry, 6 the send number after the
/// gap, 7 the sender's re-form epoch after the group.
const LAYOUT: u8 = 7;

/// The layouts this build reads and writes, which it states in the peer
/// handshake. A sender writes only [`LAYOUT`], so it sends to a peer whose
/// range holds it and to no other.
pub(crate) const LAYOUTS: RaftLayouts = match RaftLayouts::new(LAYOUT, LAYOUT) {
    Some(layouts) => layouts,
    None => panic!("LAYOUT is not a layout"),
};

/// What a peer that offers `RAFT` and states no layouts is taken to read:
/// the layout every build had when the handshake started carrying them.
/// Frozen, whatever [`LAYOUT`] becomes. A peer read wrongly this way still
/// refuses the body at its first byte, so nothing is misread.
pub(crate) const UNSTATED_LAYOUTS: RaftLayouts = match RaftLayouts::new(6, 6) {
    Some(layouts) => layouts,
    None => panic!("6 is a layout"),
};

/// The layout to send a peer in: the highest both read. `None` when they
/// share none, and nothing may be sent. `theirs` is `None` for a peer that
/// stated none.
pub(crate) fn choose_layout(ours: RaftLayouts, theirs: Option<RaftLayouts>) -> Option<u8> {
    ours.common(theirs.unwrap_or(UNSTATED_LAYOUTS))
}

/// The second byte of a request: which message it is.
const APPEND: u8 = 1;
/// An append that carries entries. The pool reads this byte to send it on a
/// replication stream, so a heartbeat never waits behind a batch.
pub(crate) const APPEND_ENTRIES: u8 = 2;
const VOTE: u8 = 3;
const PRE_VOTE: u8 = 4;
const SNAPSHOT: u8 = 5;
const TRANSFER_LEADER: u8 = 6;
const WRITTEN: u8 = 7;
/// Sent only once the fleet finalized `raft_follower_read`, so a peer that
/// predates it is never asked.
const READ_INDEX: u8 = 8;
/// Many groups' heartbeats for members on one broker, each a whole body of
/// its own. Sent only to a peer that offered `RAFT_BEATS`.
pub(crate) const BEATS: u8 = 9;
/// A peer that predates it refuses it, and the asking member asks again
/// later: a mark is never needed for safety.
const WATCHED: u8 = 10;

/// The second byte of an answer.
const APPENDED: u8 = 1;
const VOTED: u8 = 2;
const SNAPSHOT_TAKEN: u8 = 3;
const TRANSFERRED: u8 = 4;
const WRITTEN_TAKEN: u8 = 5;
const READ_INDEX_TAKEN: u8 = 6;
/// The answer to `BEATS`: one answer body for each heartbeat, in order.
const BEATS_TAKEN: u8 = 7;
const WATCHED_TAKEN: u8 = 8;
const UNREACHABLE: u8 = 16;
const NO_SUCH_MEMBER: u8 = 17;
const STOPPED: u8 = 18;
const NO_SNAPSHOTS: u8 = 19;
const OTHER_COPY: u8 = 20;
/// Only inside `BEATS_TAKEN`, which only a peer that offered `RAFT_BEATS`
/// is sent.
const SLOW: u8 = 21;

/// Most members one voter set or the learners may name. Far more than a
/// shard's copies; it bounds what a peer can make this broker allocate.
const MAX_MEMBERS: usize = 64;
/// Most voter sets: two during a joint change.
const MAX_CONFIGS: usize = 2;
/// The most heartbeats one `BEATS` body holds.
pub(crate) const MAX_BEATS: usize = 512;
/// The fewest bytes one heartbeat takes inside a `BEATS` body: its length,
/// then at least the fixed part of a request header.
const MIN_BEAT_BYTES: usize = 4 + 23;
/// Longest reason a refusal carries. Longer ones are cut.
const MAX_REASON_BYTES: usize = 1024;
/// Smallest encoded entry: a log id and a payload tag.
const MIN_ENTRY_BYTES: usize = 17;
/// Most record bytes one append carries, well inside a frame. openraft
/// takes the shorter append as a partial acknowledgement and sends the rest
/// next, so a follower far behind still catches up.
const MAX_APPEND_BYTES: usize = 32 * 1024 * 1024;
/// The link an append is sized for: 1 Gbit/s, in bytes a second.
const LINK_BYTES_PER_SEC: u128 = 125_000_000;
/// The share of its deadline an append may spend on that link. The rest is
/// for the follower's fsync, queueing, and the answer.
const WIRE_SHARE_OF_DEADLINE: u128 = 4;
/// The least an append is cut to, so a very short deadline still moves.
const MIN_APPEND_BYTES: usize = 256 * 1024;
/// What an entry costs on the wire besides its record.
const ENTRY_OVERHEAD: usize = 64;
/// The tag and flags byte that wrap a record with a publisher or headers.
const ATTRIBUTED_WRAPPER: usize = 2;

/// Smallest encoded log id in a conflict hint.
const LOG_ID_BYTES: usize = 16;

/// The record bytes an append with this deadline carries: what crosses a
/// 1 Gbit/s link in a quarter of it. An append that cannot arrive in time
/// is cut the same way on every retry, so a follower far behind would never
/// catch up.
pub(crate) fn append_budget(deadline: Duration) -> usize {
    let bytes = deadline.as_millis() * LINK_BYTES_PER_SEC / 1000 / WIRE_SHARE_OF_DEADLINE;
    usize::try_from(bytes)
        .unwrap_or(usize::MAX)
        .clamp(MIN_APPEND_BYTES, MAX_APPEND_BYTES)
}

/// The most record bytes one entry on a Raft shard may hold: the most one
/// append carries. It keeps every entry well inside a frame and its deadline
/// short, so the proposer refuses a larger record.
pub(crate) const MAX_RECORD_BYTES: usize = MAX_APPEND_BYTES;

/// The deadline for an append, given the one openraft set. An append always
/// carries its first entry, so a record past the budget of `deadline` gets
/// as long as it needs at the same rate. Otherwise it would time out the
/// same way on every retry and the follower would never get past it.
pub(crate) fn append_deadline(deadline: Duration, body: &Request) -> Duration {
    let Request::Append(rpc) = body else {
        return deadline;
    };
    let Some(first) = rpc.entries.first() else {
        return deadline;
    };
    let bytes = entry_bytes(first);
    if bytes <= append_budget(deadline) {
        return deadline;
    }
    let millis = (bytes as u128 * WIRE_SHARE_OF_DEADLINE * 1000).div_ceil(LINK_BYTES_PER_SEC);
    let needed = Duration::from_millis(u64::try_from(millis).unwrap_or(u64::MAX));
    needed.clamp(deadline, crate::peer::RAFT_MAX_DEADLINE.max(deadline))
}

/// What one entry costs an append's budget.
fn entry_bytes(entry: &EntryOf<ShardTypes>) -> usize {
    ENTRY_OVERHEAD
        + match &entry.payload {
            EntryPayload::Normal(command) => command_bytes(command),
            _ => 0,
        }
}

/// A command's record bytes: its payload, headers and publisher, and the
/// attributed wrapper around them.
pub(crate) fn command_bytes(command: &Command) -> usize {
    // A tag, a flags byte, and a length for each block it holds.
    let wrapper = match (&command.headers, &command.publisher) {
        (None, None) => 0,
        (Some(_), Some(_)) => ATTRIBUTED_WRAPPER + 8,
        _ => ATTRIBUTED_WRAPPER + 4,
    };
    command.payload.len()
        + command.headers.as_ref().map_or(0, |headers| headers.len())
        + command.publisher.as_ref().map_or(0, Bytes::len)
        + wrapper
}

/// Cut an append's entries to its [`append_budget`], keeping at least one.
pub(crate) fn fit(request: &mut RaftRequest) {
    let budget = append_budget(request.deadline);
    let Request::Append(rpc) = &mut request.body else {
        return;
    };
    let mut bytes = 0;
    let keep = rpc
        .entries
        .iter()
        .position(|entry| {
            bytes += entry_bytes(entry);
            bytes > budget
        })
        .unwrap_or(rpc.entries.len())
        .max(1);
    rpc.entries.truncate(keep);
}

pub(crate) fn encode_request(request: &RaftRequest) -> io::Result<Bytes> {
    let mut buf = BytesMut::new();
    buf.put_u8(LAYOUT);
    buf.put_u8(match &request.body {
        Request::Append(rpc) if rpc.entries.is_empty() => APPEND,
        Request::Append(_) => APPEND_ENTRIES,
        Request::Vote(_) => VOTE,
        Request::PreVote(_) => PRE_VOTE,
        Request::Snapshot { .. } => SNAPSHOT,
        Request::TransferLeader(_) => TRANSFER_LEADER,
        Request::Written { .. } => WRITTEN,
        Request::ReadIndex => READ_INDEX,
        Request::Watched => WATCHED,
    });
    // At a fixed place, so a listener reads it without reading the rest.
    buf.put_u32(u32::try_from(request.deadline.as_millis()).unwrap_or(u32::MAX));
    put_group(&mut buf, &request.group)?;
    buf.put_u64(request.reform_epoch);
    put_member(&mut buf, &request.to)?;
    buf.put_u64(request.beat.gap);
    buf.put_u64(request.beat.seq);
    // 0 and 1 read as the boolean older builds take.
    buf.put_u8(match request.report {
        Ask::Nothing => 0,
        Ask::Written => 1,
    });
    match &request.body {
        Request::Append(rpc) => {
            put_vote(&mut buf, &rpc.vote)?;
            put_opt_log_id(&mut buf, rpc.prev_log_id.as_ref());
            put_opt_log_id(&mut buf, rpc.leader_commit.as_ref());
            if !rpc.entries.is_empty() {
                buf.put_u32(len_u32(rpc.entries.len())?);
                for entry in &rpc.entries {
                    put_entry(&mut buf, entry)?;
                }
            }
        }
        Request::Vote(rpc) | Request::PreVote(rpc) => {
            put_vote(&mut buf, &rpc.vote)?;
            put_opt_log_id(&mut buf, rpc.last_log_id.as_ref());
            match &rpc.leadership_transfer_from {
                Some(vote) => {
                    buf.put_u8(1);
                    put_vote(&mut buf, vote)?;
                }
                None => buf.put_u8(0),
            }
        }
        Request::Snapshot {
            vote,
            meta,
            data_len,
            offset,
            data,
        } => {
            put_vote(&mut buf, vote)?;
            put_opt_log_id(&mut buf, meta.last_log_id.as_ref());
            put_opt_log_id(&mut buf, meta.last_membership.log_id().as_ref());
            put_membership(&mut buf, meta.last_membership.membership())?;
            buf.put_u64(*data_len);
            buf.put_u64(*offset);
            buf.put_u32(len_u32(data.len())?);
            buf.extend_from_slice(data);
        }
        Request::TransferLeader(rpc) => {
            put_vote(&mut buf, rpc.from_leader())?;
            put_member(&mut buf, rpc.to_node_id())?;
            put_opt_log_id(&mut buf, rpc.last_log_id());
        }
        Request::Written { from, at } => {
            put_member(&mut buf, from)?;
            buf.put_u64(at.term);
            buf.put_u64(at.index);
        }
        Request::ReadIndex | Request::Watched => {}
    }
    Ok(buf.freeze())
}

pub(crate) fn decode_request(mut buf: Bytes) -> io::Result<RaftRequest> {
    expect_layout(&mut buf)?;
    let tag = take_u8(&mut buf)?;
    let deadline = Duration::from_millis(u64::from(take_u32(&mut buf)?));
    let group = take_group(&mut buf)?;
    let reform_epoch = take_u64(&mut buf)?;
    let to = take_member(&mut buf)?;
    let beat = Beat {
        gap: take_u64(&mut buf)?,
        seq: take_u64(&mut buf)?,
    };
    let report = match take_u8(&mut buf)? {
        0 => Ask::Nothing,
        // 2 is what older written-commit leaders sent. They asked only
        // followers that fsync first; reporting anyway is what they lacked.
        1 | 2 => Ask::Written,
        other => return Err(invalid(format!("{other} is not a report ask"))),
    };
    let body = match tag {
        APPEND | APPEND_ENTRIES => {
            let vote = take_vote(&mut buf)?;
            let prev_log_id = take_opt_log_id(&mut buf)?;
            let leader_commit = take_opt_log_id(&mut buf)?;
            let mut entries = Vec::new();
            if tag == APPEND_ENTRIES {
                let count = take_count(&mut buf, MIN_ENTRY_BYTES)?;
                if count == 0 {
                    return Err(invalid("an append that carries entries names none".into()));
                }
                entries.reserve(entry_room(count, buf.remaining()));
                for _ in 0..count {
                    entries.push(take_entry(&mut buf)?);
                }
            }
            Request::Append(AppendEntriesRequest {
                vote,
                prev_log_id,
                entries,
                leader_commit,
            })
        }
        VOTE | PRE_VOTE => {
            let vote = take_vote(&mut buf)?;
            let last_log_id = take_opt_log_id(&mut buf)?;
            let leadership_transfer_from = match take_u8(&mut buf)? {
                0 => None,
                1 => Some(take_vote(&mut buf)?),
                other => return Err(invalid(format!("{other} is not an option tag"))),
            };
            let rpc = VoteRequest {
                vote,
                last_log_id,
                leadership_transfer_from,
            };
            if tag == VOTE {
                Request::Vote(rpc)
            } else {
                Request::PreVote(rpc)
            }
        }
        SNAPSHOT => {
            let vote = take_vote(&mut buf)?;
            let last_log_id = take_opt_log_id(&mut buf)?;
            let membership_log_id = take_opt_log_id(&mut buf)?;
            let membership = take_membership(&mut buf)?;
            let data_len = take_u64(&mut buf)?;
            let offset = take_u64(&mut buf)?;
            let len = take_u32(&mut buf)? as usize;
            if buf.remaining() < len {
                return Err(short());
            }
            Request::Snapshot {
                vote,
                meta: SnapshotMetaOf::<ShardTypes> {
                    last_log_id,
                    last_membership: StoredMembership::new(membership_log_id, membership),
                },
                data_len,
                offset,
                data: buf.split_to(len),
            }
        }
        TRANSFER_LEADER => {
            let from = take_vote(&mut buf)?;
            let to = take_member(&mut buf)?;
            let last_log_id = take_opt_log_id(&mut buf)?;
            Request::TransferLeader(TransferLeaderRequest::new(from, to, last_log_id))
        }
        WRITTEN => {
            let from = take_member(&mut buf)?;
            let term = take_u64(&mut buf)?;
            let index = take_u64(&mut buf)?;
            Request::Written {
                from,
                at: LogPoint { term, index },
            }
        }
        READ_INDEX => Request::ReadIndex,
        WATCHED => Request::Watched,
        other => return Err(invalid(format!("{other} is not a Raft request"))),
    };
    expect_empty(&buf)?;
    Ok(RaftRequest {
        group,
        reform_epoch,
        to,
        beat,
        report,
        deadline,
        body,
    })
}

pub(crate) fn encode_reply(reply: &Result<RaftReply, Refused>) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(LAYOUT);
    // Only names and log ids go in, and both were bounded when they were
    // read. A refusal that still does not encode goes as unreachable.
    if put_reply(&mut buf, reply).is_err() {
        buf.truncate(1);
        buf.put_u8(UNREACHABLE);
        put_reason(&mut buf, "the answer did not encode");
    }
    buf.freeze()
}

fn put_reply(buf: &mut BytesMut, reply: &Result<RaftReply, Refused>) -> io::Result<()> {
    match reply {
        Ok(RaftReply(Reply::Append(result))) => {
            buf.put_u8(APPENDED);
            put_append_result(buf, result)?;
        }
        Ok(RaftReply(Reply::Vote(response))) => {
            buf.put_u8(VOTED);
            put_vote(buf, &response.vote)?;
            buf.put_u8(u8::from(response.vote_granted));
            put_opt_log_id(buf, response.last_log_id.as_ref());
        }
        Ok(RaftReply(Reply::Snapshot(response))) => {
            buf.put_u8(SNAPSHOT_TAKEN);
            put_vote(buf, &response.vote)?;
        }
        Ok(RaftReply(Reply::TransferLeader(result))) => {
            buf.put_u8(TRANSFERRED);
            match result {
                Ok(()) => buf.put_u8(0),
                Err(TransferLeaderError::VoteChanged { expected, actual }) => {
                    buf.put_u8(1);
                    put_vote(buf, expected)?;
                    put_vote(buf, actual)?;
                }
                Err(TransferLeaderError::LogNotFlushed { expected, actual }) => {
                    buf.put_u8(2);
                    put_opt_log_id(buf, expected.as_ref());
                    put_opt_log_id(buf, actual.as_ref());
                }
            }
        }
        Ok(RaftReply(Reply::Written)) => buf.put_u8(WRITTEN_TAKEN),
        Ok(RaftReply(Reply::Watched)) => buf.put_u8(WATCHED_TAKEN),
        Ok(RaftReply(Reply::ReadIndex { index })) => {
            buf.put_u8(READ_INDEX_TAKEN);
            match index {
                Some(index) => {
                    buf.put_u8(1);
                    buf.put_u64(*index);
                }
                None => buf.put_u8(0),
            }
        }
        Err(Refused::Unreachable(reason)) => {
            buf.put_u8(UNREACHABLE);
            put_reason(buf, reason);
        }
        Err(Refused::NoSuchMember(to)) => {
            buf.put_u8(NO_SUCH_MEMBER);
            put_addressee(buf, to)?;
        }
        Err(Refused::Stopped(to, reason)) => {
            buf.put_u8(STOPPED);
            put_addressee(buf, to)?;
            put_reason(buf, reason);
        }
        Err(Refused::NoSnapshots(to)) => {
            buf.put_u8(NO_SNAPSHOTS);
            put_addressee(buf, to)?;
        }
        Err(Refused::OtherCopy(to, copy)) => {
            buf.put_u8(OTHER_COPY);
            put_addressee(buf, to)?;
            buf.put_u64(*copy);
        }
        Err(Refused::Slow(to)) => {
            buf.put_u8(SLOW);
            put_addressee(buf, to)?;
        }
    }
    Ok(())
}

pub(crate) fn decode_reply(mut buf: Bytes) -> io::Result<Result<RaftReply, Refused>> {
    expect_layout(&mut buf)?;
    let reply = match take_u8(&mut buf)? {
        APPENDED => Ok(Reply::Append(take_append_result(&mut buf)?)),
        VOTED => {
            let vote = take_vote(&mut buf)?;
            let granted = take_bool(&mut buf)?;
            let last_log_id = take_opt_log_id(&mut buf)?;
            Ok(Reply::Vote(VoteResponse::new(vote, last_log_id, granted)))
        }
        SNAPSHOT_TAKEN => Ok(Reply::Snapshot(SnapshotResponse::new(take_vote(&mut buf)?))),
        TRANSFERRED => Ok(Reply::TransferLeader(match take_u8(&mut buf)? {
            0 => Ok(()),
            1 => Err(TransferLeaderError::VoteChanged {
                expected: take_vote(&mut buf)?,
                actual: take_vote(&mut buf)?,
            }),
            2 => Err(TransferLeaderError::LogNotFlushed {
                expected: take_opt_log_id(&mut buf)?,
                actual: take_opt_log_id(&mut buf)?,
            }),
            other => return Err(invalid(format!("{other} is not a transfer outcome"))),
        })),
        WRITTEN_TAKEN => Ok(Reply::Written),
        WATCHED_TAKEN => Ok(Reply::Watched),
        READ_INDEX_TAKEN => Ok(Reply::ReadIndex {
            index: match take_u8(&mut buf)? {
                0 => None,
                1 => Some(take_u64(&mut buf)?),
                other => return Err(invalid(format!("{other} is not an option tag"))),
            },
        }),
        UNREACHABLE => Err(Refused::Unreachable(take_str(&mut buf, MAX_REASON_BYTES)?)),
        NO_SUCH_MEMBER => Err(Refused::NoSuchMember(take_addressee(&mut buf)?)),
        STOPPED => {
            let to = take_addressee(&mut buf)?;
            Err(Refused::Stopped(to, take_str(&mut buf, MAX_REASON_BYTES)?))
        }
        NO_SNAPSHOTS => Err(Refused::NoSnapshots(take_addressee(&mut buf)?)),
        OTHER_COPY => {
            let to = take_addressee(&mut buf)?;
            Err(Refused::OtherCopy(to, take_u64(&mut buf)?))
        }
        SLOW => Err(Refused::Slow(take_addressee(&mut buf)?)),
        other => return Err(invalid(format!("{other} is not a Raft answer"))),
    };
    expect_empty(&buf)?;
    Ok(reply.map(RaftReply))
}

/// Whether `body` is a `BEATS` body, by its second byte.
pub(crate) fn is_beats(body: &[u8]) -> bool {
    body.get(1) == Some(&BEATS)
}

/// Heartbeats for members on one broker as one body: the layout byte, the
/// tag, the longest of their deadlines where every request names one, the
/// receiver's wait in microseconds, a count, then each heartbeat's whole
/// body behind its length.
pub(crate) fn encode_beats(beats: &[RaftRequest], wait: Duration) -> io::Result<Bytes> {
    if beats.is_empty() || beats.len() > MAX_BEATS {
        return Err(invalid(format!(
            "{} heartbeats do not go in one body",
            beats.len()
        )));
    }
    let deadline = beats
        .iter()
        .map(|beat| beat.deadline)
        .max()
        .unwrap_or_default();
    let mut buf = BytesMut::new();
    buf.put_u8(LAYOUT);
    buf.put_u8(BEATS);
    buf.put_u32(u32::try_from(deadline.as_millis()).unwrap_or(u32::MAX));
    buf.put_u32(u32::try_from(wait.as_micros()).unwrap_or(u32::MAX));
    buf.put_u16(beats.len() as u16);
    for beat in beats {
        if !matches!(&beat.body, Request::Append(rpc) if rpc.entries.is_empty()) {
            return Err(invalid("only a heartbeat goes in a heartbeats body".into()));
        }
        let body = encode_request(beat)?;
        buf.put_u32(len_u32(body.len())?);
        buf.extend_from_slice(&body);
    }
    Ok(buf.freeze())
}

/// The heartbeats in a `BEATS` body, and how long to wait for a member
/// once the others have answered. Anything but a heartbeat inside refuses
/// the whole body.
pub(crate) fn decode_beats(mut buf: Bytes) -> io::Result<(Vec<RaftRequest>, Duration)> {
    expect_layout(&mut buf)?;
    if take_u8(&mut buf)? != BEATS {
        return Err(invalid("not a heartbeats body".into()));
    }
    let _deadline = take_u32(&mut buf)?;
    let wait = Duration::from_micros(u64::from(take_u32(&mut buf)?));
    let count = usize::from(take_u16(&mut buf)?);
    if count == 0 || count > MAX_BEATS {
        return Err(invalid(format!("{count} heartbeats do not go in one body")));
    }
    if buf.remaining() < count * MIN_BEAT_BYTES {
        return Err(short());
    }
    let mut beats = Vec::with_capacity(count);
    for _ in 0..count {
        let body = take_block(&mut buf)?;
        let beat = decode_request(body)?;
        if !matches!(&beat.body, Request::Append(rpc) if rpc.entries.is_empty()) {
            return Err(invalid("a heartbeats body holds something else".into()));
        }
        beats.push(beat);
    }
    expect_empty(&buf)?;
    Ok((beats, wait))
}

/// One answer body for each heartbeat of a `BEATS` body, in its order.
pub(crate) fn encode_beats_reply(answers: &[Result<RaftReply, Refused>]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(LAYOUT);
    buf.put_u8(BEATS_TAKEN);
    buf.put_u16(answers.len() as u16);
    for answer in answers {
        let body = encode_reply(answer);
        buf.put_u32(body.len() as u32);
        buf.extend_from_slice(&body);
    }
    buf.freeze()
}

/// The answers to a `BEATS` body of `count` heartbeats. Any other count is
/// refused: an answer must not be matched to the wrong group.
pub(crate) fn decode_beats_reply(
    mut buf: Bytes,
    count: usize,
) -> io::Result<Vec<Result<RaftReply, Refused>>> {
    if buf.get(1) != Some(&BEATS_TAKEN) {
        // The whole body refused, as one it could not read is.
        return match decode_reply(buf)? {
            Err(refused) => {
                let reason = refused.to_string();
                Ok((0..count)
                    .map(|_| Err(Refused::Unreachable(reason.clone())))
                    .collect())
            }
            Ok(_) => Err(invalid(
                "a heartbeats body was answered with one answer".into(),
            )),
        };
    }
    expect_layout(&mut buf)?;
    buf.advance(1);
    let answered = usize::from(take_u16(&mut buf)?);
    if answered != count {
        return Err(invalid(format!(
            "{answered} answers came back for {count} heartbeats"
        )));
    }
    let mut answers = Vec::with_capacity(count);
    for _ in 0..count {
        answers.push(decode_reply(take_block(&mut buf)?)?);
    }
    expect_empty(&buf)?;
    Ok(answers)
}

fn put_append_result(buf: &mut BytesMut, result: &AppendResult<ShardTypes>) -> io::Result<()> {
    match result {
        Ok(last) => {
            buf.put_u8(0);
            put_opt_log_id(buf, last.as_ref());
        }
        Err(AppendError::Conflict { log_id, hint }) => {
            buf.put_u8(1);
            put_log_id(buf, log_id);
            match hint {
                Some(hint) => {
                    buf.put_u8(1);
                    buf.put_u32(len_u32(hint.log_ids.len())?);
                    for log_id in &hint.log_ids {
                        put_log_id(buf, log_id);
                    }
                }
                None => buf.put_u8(0),
            }
        }
        Err(AppendError::HigherVote(vote)) => {
            buf.put_u8(2);
            put_vote(buf, vote)?;
        }
    }
    Ok(())
}

fn take_append_result(buf: &mut Bytes) -> io::Result<AppendResult<ShardTypes>> {
    Ok(match take_u8(buf)? {
        0 => Ok(take_opt_log_id(buf)?),
        1 => {
            let log_id = take_log_id(buf)?;
            let hint = match take_u8(buf)? {
                0 => None,
                1 => {
                    let count = take_count(buf, LOG_ID_BYTES)?;
                    let mut log_ids = Vec::with_capacity(count);
                    for _ in 0..count {
                        log_ids.push(take_log_id(buf)?);
                    }
                    Some(ConflictHint::new(log_ids))
                }
                other => return Err(invalid(format!("{other} is not an option tag"))),
            };
            Err(AppendError::Conflict { log_id, hint })
        }
        2 => Err(AppendError::HigherVote(take_vote(buf)?)),
        other => return Err(invalid(format!("{other} is not an append outcome"))),
    })
}

/// Entry payload tags. 0 is a blank entry and 2 a membership.
const ENTRY_RECORD: u8 = 1;
/// A record that is an idempotent producer's batch: the producer id and the
/// sequence come before the record.
const ENTRY_SEQUENCED: u8 = 3;
/// A record of an idempotent producer's batch of several: the producer id,
/// the sequence, the record's position and the batch's length come before
/// the record. A batch of one keeps [`ENTRY_SEQUENCED`], so its frames are
/// the ones a build without this tag reads.
const ENTRY_PIECE: u8 = 4;
/// A conditional publish's record: the offset it must be written at comes
/// before the record. Never an idempotent producer's. openraft replicates
/// entries read back from the log, which hold only the outcome, so none is
/// sent today. A new tag rather than a new layout: a build without it
/// refuses the entry by its tag rather than store the record
/// unconditionally, and every other frame stays byte-identical.
const ENTRY_CONDITIONAL: u8 = 5;
/// A head trim: the floor comes before the record, which holds the trim's
/// headers. Sent as read back from the log, so never conditional. A new tag
/// for the same reason as [`ENTRY_CONDITIONAL`]: a build without it refuses
/// the entry by its tag rather than store the headers as a client record.
const ENTRY_TRIM: u8 = 6;
/// A record with its publisher, its headers block, or both: a flags byte
/// says which follow, each as a length and its bytes, and then the record
/// as one of the tags above lays it out. A wrapper, so a record without
/// either keeps its frame, and a build without it refuses the entry by its
/// tag rather than store the record without them.
const ENTRY_ATTRIBUTED: u8 = 7;
/// An atomic commit: laid out as [`ENTRY_RECORD`], the record being the
/// encoded commit. Sent as read back from the log, so never conditional and
/// never a producer's. A new tag so a build without it refuses the entry
/// rather than store the commit as a plain record no state view reads.
const ENTRY_COMMIT: u8 = 8;
/// A consumer group's write: laid out as [`ENTRY_RECORD`], the record being
/// the encoded group entry. Sent as read back from the log, so never
/// conditional, and it names no producer, publisher or headers. A new tag so
/// a build without it refuses the entry rather than store the write as a
/// client record that no group table folds.
const ENTRY_GROUP: u8 = 9;
/// A record of a conditional batch of more than one record: a flag byte
/// (1 when the batch's expected offset follows, 0 for a piece read back
/// from the log), the offset, then the piece's position and the batch's
/// length, each a u32, then laid out as [`ENTRY_RECORD`]. A new tag so a
/// build without it refuses the entry rather than store a piece as a plain
/// record its readers would show apart from the rest of its batch.
const ENTRY_COND_PIECE: u8 = 10;
/// [`ENTRY_ATTRIBUTED`] flags.
const HAS_PUBLISHER: u8 = 1;
const HAS_HEADERS: u8 = 2;

fn put_entry(buf: &mut BytesMut, entry: &EntryOf<ShardTypes>) -> io::Result<()> {
    put_log_id(buf, &entry.log_id);
    match &entry.payload {
        EntryPayload::Blank => buf.put_u8(0),
        EntryPayload::Normal(command) => {
            if command.publisher.is_some() || command.headers.is_some() {
                if command.trim.is_some() {
                    return Err(invalid("a trim names a publisher or headers".to_string()));
                }
                if command.group {
                    return Err(invalid(
                        "a group write names a publisher or headers".to_string(),
                    ));
                }
                buf.put_u8(ENTRY_ATTRIBUTED);
                let mut flags = 0;
                if command.publisher.is_some() {
                    flags |= HAS_PUBLISHER;
                }
                if command.headers.is_some() {
                    flags |= HAS_HEADERS;
                }
                buf.put_u8(flags);
                for block in [command.publisher.as_ref(), command.headers.as_deref()]
                    .into_iter()
                    .flatten()
                {
                    buf.put_u32(len_u32(block.len())?);
                    buf.extend_from_slice(block);
                }
            }
            // A record of no producer's batch keeps the layout it always had.
            match (command.producer, command.expected) {
                _ if let Some(cond) = command.cond => {
                    if command.group
                        || command.commit
                        || command.trim.is_some()
                        || command.producer.is_some()
                    {
                        return Err(invalid(
                            "a conditional piece names a group, a commit, a trim or a producer"
                                .to_string(),
                        ));
                    }
                    if cond.len < 2 || cond.position >= cond.len {
                        return Err(invalid(format!(
                            "piece {} of {} is no conditional batch's",
                            cond.position, cond.len
                        )));
                    }
                    buf.put_u8(ENTRY_COND_PIECE);
                    match command.expected {
                        Some(expected) => {
                            buf.put_u8(1);
                            buf.put_u64(expected);
                        }
                        None => buf.put_u8(0),
                    }
                    buf.put_u32(cond.position);
                    buf.put_u32(cond.len);
                }
                _ if command.group
                    && (command.commit
                        || command.trim.is_some()
                        || command.producer.is_some()
                        || command.expected.is_some()) =>
                {
                    return Err(invalid(
                        "a group write names a commit, a trim, a producer or an offset".to_string(),
                    ));
                }
                (None, None) if command.group => buf.put_u8(ENTRY_GROUP),
                _ if command.commit && (command.trim.is_some() || command.producer.is_some()) => {
                    return Err(invalid("a commit names a trim or a producer".to_string()));
                }
                (None, Some(_)) if command.commit => {
                    return Err(invalid("a conditional commit is not sent".to_string()));
                }
                (None, None) if command.commit => buf.put_u8(ENTRY_COMMIT),
                _ if command.trim.is_some() && command.expected.is_some() => {
                    return Err(invalid("a conditional trim is not sent".to_string()));
                }
                (Some(_), _) if command.trim.is_some() => {
                    return Err(invalid("a trim names a producer".to_string()));
                }
                (None, None) if let Some(before) = command.trim => {
                    buf.put_u8(ENTRY_TRIM);
                    buf.put_u64(before);
                }
                (None, None) => buf.put_u8(ENTRY_RECORD),
                (None, Some(expected)) => {
                    buf.put_u8(ENTRY_CONDITIONAL);
                    buf.put_u64(expected);
                }
                (Some(_), Some(_)) => {
                    return Err(invalid(
                        "a conditional publish names a producer".to_string(),
                    ));
                }
                (
                    Some(ProducerMark {
                        producer_id,
                        sequence,
                        position: 0,
                        len: 1,
                    }),
                    None,
                ) => {
                    buf.put_u8(ENTRY_SEQUENCED);
                    buf.put_u64(producer_id);
                    buf.put_u64(sequence);
                }
                (
                    Some(ProducerMark {
                        producer_id,
                        sequence,
                        position,
                        len,
                    }),
                    None,
                ) => {
                    buf.put_u8(ENTRY_PIECE);
                    buf.put_u64(producer_id);
                    buf.put_u64(sequence);
                    buf.put_u32(position);
                    buf.put_u32(len);
                }
            }
            buf.put_u64(command.timestamp_micros);
            buf.put_u32(len_u32(command.payload.len())?);
            buf.extend_from_slice(&command.payload);
        }
        EntryPayload::Membership(membership) => {
            buf.put_u8(2);
            put_membership(buf, membership)?;
        }
    }
    Ok(())
}

fn take_entry(buf: &mut Bytes) -> io::Result<EntryOf<ShardTypes>> {
    let log_id = take_log_id(buf)?;
    Ok(match take_u8(buf)? {
        0 => EntryOf::<ShardTypes>::new_blank(log_id),
        ENTRY_ATTRIBUTED => {
            let flags = take_u8(buf)?;
            if flags == 0 || flags & !(HAS_PUBLISHER | HAS_HEADERS) != 0 {
                return Err(invalid(format!("{flags:#x} is not a record's attributes")));
            }
            let publisher = match flags & HAS_PUBLISHER {
                0 => None,
                _ => Some(take_block(buf)?),
            };
            let headers = match flags & HAS_HEADERS {
                0 => None,
                _ => Some(Box::new(take_block(buf)?)),
            };
            let tag = take_u8(buf)?;
            if !matches!(
                tag,
                ENTRY_RECORD
                    | ENTRY_SEQUENCED
                    | ENTRY_PIECE
                    | ENTRY_CONDITIONAL
                    | ENTRY_COMMIT
                    | ENTRY_COND_PIECE
            ) {
                return Err(invalid(format!("{tag} is not a record a publisher names")));
            }
            let mut command = take_command(buf, tag)?;
            command.publisher = publisher;
            command.headers = headers;
            EntryOf::<ShardTypes>::new_normal(log_id, command)
        }
        tag @ (ENTRY_RECORD | ENTRY_SEQUENCED | ENTRY_PIECE | ENTRY_CONDITIONAL | ENTRY_TRIM
        | ENTRY_COMMIT | ENTRY_GROUP | ENTRY_COND_PIECE) => {
            EntryOf::<ShardTypes>::new_normal(log_id, take_command(buf, tag)?)
        }
        2 => {
            // A config with no voters would let a joint quorum pass with no
            // votes. Only a snapshot's default membership is empty.
            let membership = take_membership(buf)?;
            let configs = membership.get_joint_config();
            if configs.is_empty() || configs.iter().any(BTreeSet::is_empty) {
                return Err(invalid("a membership entry names no voters".into()));
            }
            EntryOf::<ShardTypes>::new_membership(log_id, membership)
        }
        other => return Err(invalid(format!("{other} is not an entry payload"))),
    })
}

/// A record entry's body after its tag `tag`.
fn take_command(buf: &mut Bytes, tag: u8) -> io::Result<Command> {
    let (expected, cond) = match tag {
        ENTRY_CONDITIONAL => (Some(take_u64(buf)?), None),
        ENTRY_COND_PIECE => {
            let expected = match take_u8(buf)? {
                0 => None,
                1 => Some(take_u64(buf)?),
                other => return Err(invalid(format!("{other} is not a piece's offset flag"))),
            };
            let cond = CondPlace {
                position: take_u32(buf)?,
                len: take_u32(buf)?,
            };
            // The same shape the log's codec stores, so a frame no leader
            // sends is refused here rather than at the follower's append.
            if cond.len < 2 || cond.position >= cond.len {
                return Err(invalid(format!(
                    "piece {} of {} is no conditional batch's",
                    cond.position, cond.len
                )));
            }
            (expected, Some(cond))
        }
        _ => (None, None),
    };
    let trim = match tag {
        ENTRY_TRIM => Some(take_u64(buf)?),
        _ => None,
    };
    let producer = match tag {
        ENTRY_SEQUENCED => Some(ProducerMark {
            producer_id: take_u64(buf)?,
            sequence: take_u64(buf)?,
            position: 0,
            len: 1,
        }),
        ENTRY_PIECE => Some(ProducerMark {
            producer_id: take_u64(buf)?,
            sequence: take_u64(buf)?,
            position: take_u32(buf)?,
            len: take_u32(buf)?,
        }),
        _ => None,
    };
    let timestamp_micros = take_u64(buf)?;
    let len = take_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(short());
    }
    // A slice of the frame: the record is not copied.
    let payload = buf.split_to(len);
    Ok(Command {
        timestamp_micros,
        payload,
        producer,
        expected,
        trim,
        commit: tag == ENTRY_COMMIT,
        group: tag == ENTRY_GROUP,
        publisher: None,
        headers: None,
        written: None,
        cond,
    })
}

/// A length, then that many bytes: a slice of the frame, not a copy.
fn take_block(buf: &mut Bytes) -> io::Result<Bytes> {
    let len = take_u32(buf)? as usize;
    if buf.remaining() < len {
        return Err(short());
    }
    Ok(buf.split_to(len))
}

/// The voter sets, the members that are not voters, then the target
/// version.
fn put_membership(buf: &mut BytesMut, membership: &Membership) -> io::Result<()> {
    let configs = membership.get_joint_config();
    if configs.len() > MAX_CONFIGS {
        return Err(invalid(format!("{} voter sets", configs.len())));
    }
    buf.put_u8(configs.len() as u8);
    for voters in configs {
        put_members(buf, voters)?;
    }
    let learners: BTreeSet<MemberId> = membership.learner_ids().collect();
    put_members(buf, &learners)?;
    let version = membership.version();
    buf.put_u64(version.incarnation);
    buf.put_u64(version.counter);
    Ok(())
}

fn take_membership(buf: &mut Bytes) -> io::Result<Membership> {
    let count = take_u8(buf)? as usize;
    if count > MAX_CONFIGS {
        return Err(invalid(format!("{count} voter sets")));
    }
    let mut configs = Vec::with_capacity(count);
    for _ in 0..count {
        configs.push(take_members(buf)?);
    }
    let learners = take_members(buf)?;
    let version = TargetVersion {
        incarnation: take_u64(buf)?,
        counter: take_u64(buf)?,
    };
    Ok(Membership::new(configs, learners, version))
}

fn put_members(buf: &mut BytesMut, members: &BTreeSet<MemberId>) -> io::Result<()> {
    if members.len() > MAX_MEMBERS {
        return Err(invalid(format!("{} members in one set", members.len())));
    }
    buf.put_u8(members.len() as u8);
    for member in members {
        put_member(buf, member)?;
    }
    Ok(())
}

fn take_members(buf: &mut Bytes) -> io::Result<BTreeSet<MemberId>> {
    let count = take_u8(buf)? as usize;
    if count > MAX_MEMBERS {
        return Err(invalid(format!("{count} members in one set")));
    }
    let mut members = BTreeSet::new();
    for _ in 0..count {
        if !members.insert(take_member(buf)?) {
            return Err(invalid("a member set names one member twice".into()));
        }
    }
    Ok(members)
}

fn put_group(buf: &mut BytesMut, group: &GroupId) -> io::Result<()> {
    let ShardKey {
        tenant_id,
        namespace,
        stream,
        shard,
        kind,
    } = &*group.shard;
    put_str(buf, tenant_id)?;
    put_str(buf, namespace)?;
    put_str(buf, stream)?;
    buf.put_u32(*shard);
    buf.put_u8(match kind {
        ShardKind::Stream => 0,
        ShardKind::Cache => 1,
    });
    buf.put_u64(group.epoch);
    Ok(())
}

fn take_group(buf: &mut Bytes) -> io::Result<GroupId> {
    let tenant_id = take_str(buf, MAX_IDENT_BYTES)?;
    let namespace = take_str(buf, MAX_IDENT_BYTES)?;
    let stream = take_str(buf, MAX_IDENT_BYTES)?;
    let shard = take_u32(buf)?;
    let kind = match take_u8(buf)? {
        0 => ShardKind::Stream,
        1 => ShardKind::Cache,
        other => return Err(invalid(format!("{other} is not a shard kind"))),
    };
    let epoch = take_u64(buf)?;
    Ok(GroupId::new(
        ShardKey {
            tenant_id,
            namespace,
            stream,
            shard,
            kind,
        },
        epoch,
    ))
}

fn put_addressee(buf: &mut BytesMut, to: &Addressee) -> io::Result<()> {
    put_group(buf, &to.group)?;
    put_member(buf, &to.to)
}

fn take_addressee(buf: &mut Bytes) -> io::Result<Box<Addressee>> {
    let group = take_group(buf)?;
    let to = take_member(buf)?;
    Ok(Box::new(Addressee { group, to }))
}

fn put_member(buf: &mut BytesMut, member: &MemberId) -> io::Result<()> {
    put_str(buf, &member.broker)?;
    buf.put_u64(member.copy);
    Ok(())
}

fn take_member(buf: &mut Bytes) -> io::Result<MemberId> {
    let broker = take_str(buf, MAX_IDENT_BYTES)?;
    let copy = take_u64(buf)?;
    Ok(MemberId::new(broker, copy))
}

/// The term, the member voted for, and whether a quorum granted it.
fn put_vote(buf: &mut BytesMut, vote: &VoteOf<ShardTypes>) -> io::Result<()> {
    buf.put_u64(vote.leader_id.term);
    put_member(buf, &vote.leader_id.voted_for)?;
    buf.put_u8(u8::from(vote.committed));
    Ok(())
}

fn take_vote(buf: &mut Bytes) -> io::Result<VoteOf<ShardTypes>> {
    let term = take_u64(buf)?;
    let voted_for = take_member(buf)?;
    let committed = take_bool(buf)?;
    Ok(Vote {
        leader_id: LeaderId { term, voted_for },
        committed,
    })
}

/// The term of the leader that wrote it, and its index.
fn put_log_id(buf: &mut BytesMut, log_id: &LogIdOf<ShardTypes>) {
    buf.put_u64(log_id.leader_id.term);
    buf.put_u64(log_id.index);
}

fn take_log_id(buf: &mut Bytes) -> io::Result<LogIdOf<ShardTypes>> {
    let term = take_u64(buf)?;
    let index = take_u64(buf)?;
    Ok(LogId {
        leader_id: CommittedLeaderIdOf::<ShardTypes> { term },
        index,
    })
}

fn put_opt_log_id(buf: &mut BytesMut, log_id: Option<&LogIdOf<ShardTypes>>) {
    match log_id {
        Some(log_id) => {
            buf.put_u8(1);
            put_log_id(buf, log_id);
        }
        None => buf.put_u8(0),
    }
}

fn take_opt_log_id(buf: &mut Bytes) -> io::Result<Option<LogIdOf<ShardTypes>>> {
    match take_u8(buf)? {
        0 => Ok(None),
        1 => Ok(Some(take_log_id(buf)?)),
        other => Err(invalid(format!("{other} is not an option tag"))),
    }
}

fn put_str(buf: &mut BytesMut, value: &str) -> io::Result<()> {
    if value.len() > MAX_IDENT_BYTES {
        return Err(invalid(format!("a name of {} bytes", value.len())));
    }
    buf.put_u16(value.len() as u16);
    buf.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_reason(buf: &mut BytesMut, reason: &str) {
    let mut end = reason.len().min(MAX_REASON_BYTES);
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    buf.put_u16(end as u16);
    buf.extend_from_slice(&reason.as_bytes()[..end]);
}

fn take_str(buf: &mut Bytes, max: usize) -> io::Result<String> {
    let len = take_u16(buf)? as usize;
    if len > max {
        return Err(invalid(format!("a string of {len} bytes")));
    }
    if buf.remaining() < len {
        return Err(short());
    }
    String::from_utf8(buf.split_to(len).to_vec())
        .map_err(|_| invalid("a string is not UTF-8".into()))
}

/// A `u32` count of items at least `min` bytes each, refused if the bytes
/// left cannot hold that many.
fn take_count(buf: &mut Bytes, min: usize) -> io::Result<usize> {
    let count = take_u32(buf)? as usize;
    if count > buf.remaining() / min {
        return Err(invalid(format!(
            "a count of {count}, more than the {} bytes left hold",
            buf.remaining()
        )));
    }
    Ok(count)
}

/// Room for `count` entries read from `left` bytes, held to what those bytes
/// would take in memory. An entry in memory is larger than its smallest
/// encoding, so a forged count would otherwise reserve several times the
/// frame before the first entry fails to read.
fn entry_room(count: usize, left: usize) -> usize {
    count.min(left / size_of::<EntryOf<ShardTypes>>())
}

fn len_u32(len: usize) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| invalid(format!("{len} items")))
}

fn expect_layout(buf: &mut Bytes) -> io::Result<()> {
    match take_u8(buf)? {
        LAYOUT => Ok(()),
        other => Err(invalid(format!(
            "Raft message layout {other} is not one this build reads"
        ))),
    }
}

fn expect_empty(buf: &Bytes) -> io::Result<()> {
    if buf.has_remaining() {
        return Err(invalid(format!(
            "{} bytes after the message",
            buf.remaining()
        )));
    }
    Ok(())
}

fn take_bool(buf: &mut Bytes) -> io::Result<bool> {
    match take_u8(buf)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(invalid(format!("{other} is not a boolean"))),
    }
}

fn take_u8(buf: &mut Bytes) -> io::Result<u8> {
    if buf.remaining() < 1 {
        return Err(short());
    }
    Ok(buf.get_u8())
}

fn take_u16(buf: &mut Bytes) -> io::Result<u16> {
    if buf.remaining() < 2 {
        return Err(short());
    }
    Ok(buf.get_u16())
}

fn take_u32(buf: &mut Bytes) -> io::Result<u32> {
    if buf.remaining() < 4 {
        return Err(short());
    }
    Ok(buf.get_u32())
}

fn take_u64(buf: &mut Bytes) -> io::Result<u64> {
    if buf.remaining() < 8 {
        return Err(short());
    }
    Ok(buf.get_u64())
}

fn short() -> io::Error {
    invalid("a Raft message ends early".into())
}

#[cfg(test)]
mod tests;
