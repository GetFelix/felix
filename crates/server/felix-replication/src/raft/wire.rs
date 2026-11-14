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
use felix_wire::internal::MAX_IDENT_BYTES;
use openraft::entry::RaftEntry;
use openraft::impls::leader_id_std::LeaderId;
use openraft::raft::{
    AppendEntriesRequest, AppendError, AppendResult, ConflictHint, SnapshotResponse,
    TransferLeaderError, TransferLeaderRequest, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotMetaOf, VoteOf};
use openraft::{EntryPayload, LogId, StoredMembership, Vote};

use super::codec::invalid;
use super::network::{Addressee, RaftReply, RaftRequest, Refused, Reply, Request};
use super::types::{Command, GroupId, MemberId, Membership, ShardTypes};
use crate::{ShardKey, ShardKind};

/// The first byte of every body. A reader refuses any other, so a peer
/// on another layout is refused by name rather than misread. 2 added the
/// report byte to the request header.
const LAYOUT: u8 = 2;

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

/// The second byte of an answer.
const APPENDED: u8 = 1;
const VOTED: u8 = 2;
const SNAPSHOT_TAKEN: u8 = 3;
const TRANSFERRED: u8 = 4;
const WRITTEN_TAKEN: u8 = 5;
const UNREACHABLE: u8 = 16;
const NO_SUCH_MEMBER: u8 = 17;
const STOPPED: u8 = 18;
const NO_SNAPSHOTS: u8 = 19;
const OTHER_COPY: u8 = 20;

/// Most members one voter set or the learners may name. Far more than a
/// shard's copies; it bounds what a peer can make this broker allocate.
const MAX_MEMBERS: usize = 64;
/// Most voter sets: two during a joint change.
const MAX_CONFIGS: usize = 2;
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
            bytes += ENTRY_OVERHEAD
                + match &entry.payload {
                    EntryPayload::Normal(command) => command.payload.len(),
                    _ => 0,
                };
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
    });
    // At a fixed place, so a listener reads it without reading the rest.
    buf.put_u32(u32::try_from(request.deadline.as_millis()).unwrap_or(u32::MAX));
    put_group(&mut buf, &request.group)?;
    put_member(&mut buf, &request.to)?;
    buf.put_u64(request.beat);
    buf.put_u8(u8::from(request.report));
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
        Request::Snapshot { vote, meta } => {
            put_vote(&mut buf, vote)?;
            put_opt_log_id(&mut buf, meta.last_log_id.as_ref());
            put_opt_log_id(&mut buf, meta.last_membership.log_id().as_ref());
            put_membership(&mut buf, meta.last_membership.membership())?;
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
    }
    Ok(buf.freeze())
}

pub(crate) fn decode_request(mut buf: Bytes) -> io::Result<RaftRequest> {
    expect_layout(&mut buf)?;
    let tag = take_u8(&mut buf)?;
    let deadline = Duration::from_millis(u64::from(take_u32(&mut buf)?));
    let group = take_group(&mut buf)?;
    let to = take_member(&mut buf)?;
    let beat = take_u64(&mut buf)?;
    let report = take_bool(&mut buf)?;
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
            Request::Snapshot {
                vote,
                meta: SnapshotMetaOf::<ShardTypes> {
                    last_log_id,
                    last_membership: StoredMembership::new(membership_log_id, membership),
                },
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
        other => return Err(invalid(format!("{other} is not a Raft request"))),
    };
    expect_empty(&buf)?;
    Ok(RaftRequest {
        group,
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
        other => return Err(invalid(format!("{other} is not a Raft answer"))),
    };
    expect_empty(&buf)?;
    Ok(reply.map(RaftReply))
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

fn put_entry(buf: &mut BytesMut, entry: &EntryOf<ShardTypes>) -> io::Result<()> {
    put_log_id(buf, &entry.log_id);
    match &entry.payload {
        EntryPayload::Blank => buf.put_u8(0),
        EntryPayload::Normal(command) => {
            buf.put_u8(1);
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
        1 => {
            let timestamp_micros = take_u64(buf)?;
            let len = take_u32(buf)? as usize;
            if buf.remaining() < len {
                return Err(short());
            }
            // A slice of the frame: the record is not copied.
            let payload = buf.split_to(len);
            EntryOf::<ShardTypes>::new_normal(
                log_id,
                Command {
                    timestamp_micros,
                    payload,
                    written: None,
                },
            )
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

/// The voter sets, then the members that are not voters.
fn put_membership(buf: &mut BytesMut, membership: &Membership) -> io::Result<()> {
    let configs = membership.get_joint_config();
    if configs.len() > MAX_CONFIGS {
        return Err(invalid(format!("{} voter sets", configs.len())));
    }
    buf.put_u8(configs.len() as u8);
    for voters in configs {
        put_members(buf, voters)?;
    }
    let learners: BTreeSet<MemberId> = membership.non_write_voter_ids().collect();
    put_members(buf, &learners)
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
    let members: BTreeSet<MemberId> = configs.iter().flatten().chain(&learners).cloned().collect();
    Ok(Membership::new_with_defaults(configs, members))
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
