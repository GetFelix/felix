use std::collections::BTreeSet;

use bytes::{BufMut, Bytes, BytesMut};
use openraft::entry::RaftEntry;
use openraft::raft::{
    AppendEntriesRequest, AppendError, ConflictHint, SnapshotResponse, TransferLeaderError,
    TransferLeaderRequest, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotMetaOf, VoteOf};
use openraft::vote::{RaftLeaderId, RaftVote};
use openraft::{EntryPayload, StoredMembership};

use super::*;
use crate::raft::network::{Addressee, RaftReply, RaftRequest, Refused, Reply, Request};
use crate::raft::types::{Command, GroupId, MemberId, Membership, ShardTypes};
use crate::{ShardKey, ShardKind};

fn group() -> GroupId {
    GroupId::new(
        ShardKey {
            tenant_id: "t".into(),
            namespace: "ns".into(),
            stream: "orders".into(),
            shard: 3,
            kind: ShardKind::Cache,
        },
        7,
    )
}

fn member(broker: &str) -> MemberId {
    MemberId::new(broker, 11)
}

fn vote(term: u64, committed: bool) -> VoteOf<ShardTypes> {
    let leader = openraft::impls::leader_id_std::LeaderId::new(term, member("a"));
    let vote = VoteOf::<ShardTypes>::from_leader_id(leader, committed);
    assert_eq!(vote.committed, committed);
    vote
}

fn log_id(term: u64, index: u64) -> LogIdOf<ShardTypes> {
    LogIdOf::<ShardTypes> {
        leader_id: CommittedLeaderIdOf::<ShardTypes> { term },
        index,
    }
}

fn membership() -> Membership {
    let voters: BTreeSet<MemberId> = [member("a"), member("b"), member("c")].into();
    let joint: BTreeSet<MemberId> = [member("a"), member("d")].into();
    let all = voters
        .iter()
        .chain(&joint)
        .cloned()
        .chain([member("learner")])
        .collect::<BTreeSet<_>>();
    Membership::new_with_defaults(vec![voters, joint], all)
}

fn request(body: Request) -> RaftRequest {
    RaftRequest {
        group: group(),
        to: member("b"),
        beat: 2_000,
        report: false,
        deadline: std::time::Duration::from_millis(200),
        body,
    }
}

fn every_request() -> Vec<RaftRequest> {
    let entries: Vec<EntryOf<ShardTypes>> = vec![
        EntryOf::<ShardTypes>::new_blank(log_id(2, 5)),
        EntryOf::<ShardTypes>::new_normal(
            log_id(2, 6),
            Command {
                timestamp_micros: 99,
                payload: Bytes::from_static(b"a record"),
                producer: None,
                written: None,
            },
        ),
        EntryOf::<ShardTypes>::new_membership(log_id(2, 7), membership()),
        EntryOf::<ShardTypes>::new_normal(
            log_id(2, 8),
            Command {
                timestamp_micros: 100,
                payload: Bytes::from_static(b"a producer's batch"),
                producer: Some(ProducerMark {
                    producer_id: 0xA1B2_C3D4_E5F6_0718,
                    sequence: 41,
                }),
                written: None,
            },
        ),
    ];
    vec![
        request(Request::Append(AppendEntriesRequest {
            vote: vote(2, true),
            prev_log_id: None,
            entries: Vec::new(),
            leader_commit: None,
        })),
        request(Request::Append(AppendEntriesRequest {
            vote: vote(2, true),
            prev_log_id: Some(log_id(1, 4)),
            entries,
            leader_commit: Some(log_id(1, 3)),
        })),
        request(Request::Vote(VoteRequest::new(
            vote(3, false),
            Some(log_id(2, 7)),
        ))),
        request(Request::PreVote(VoteRequest {
            vote: vote(3, false),
            last_log_id: None,
            leadership_transfer_from: Some(vote(2, true)),
        })),
        request(Request::Snapshot {
            vote: vote(2, true),
            meta: SnapshotMetaOf::<ShardTypes> {
                last_log_id: Some(log_id(2, 7)),
                last_membership: StoredMembership::new(Some(log_id(2, 7)), membership()),
            },
            data_len: 9,
            offset: 4,
            data: Bytes::from_static(b"state"),
        }),
        request(Request::TransferLeader(TransferLeaderRequest::new(
            vote(2, true),
            member("c"),
            Some(log_id(2, 7)),
        ))),
        request(Request::Written {
            from: member("c"),
            at: LogPoint { term: 2, index: 7 },
        }),
        RaftRequest {
            report: true,
            ..request(Request::Append(AppendEntriesRequest {
                vote: vote(2, true),
                prev_log_id: Some(log_id(2, 7)),
                entries: vec![EntryOf::<ShardTypes>::new_blank(log_id(2, 8))],
                leader_commit: None,
            }))
        },
    ]
}

fn addressee() -> Box<Addressee> {
    Box::new(Addressee {
        group: group(),
        to: member("b"),
    })
}

fn every_reply() -> Vec<Result<RaftReply, Refused>> {
    vec![
        Ok(RaftReply(Reply::Append(Ok(None)))),
        Ok(RaftReply(Reply::Append(Ok(Some(log_id(2, 7)))))),
        Ok(RaftReply(Reply::Append(Err(AppendError::Conflict {
            log_id: log_id(1, 4),
            hint: None,
        })))),
        Ok(RaftReply(Reply::Append(Err(AppendError::Conflict {
            log_id: log_id(1, 4),
            hint: Some(ConflictHint::new(vec![log_id(1, 1), log_id(1, 2)])),
        })))),
        Ok(RaftReply(Reply::Append(Err(AppendError::HigherVote(
            vote(4, false),
        ))))),
        Ok(RaftReply(Reply::Vote(VoteResponse::new(
            vote(3, false),
            Some(log_id(2, 7)),
            true,
        )))),
        Ok(RaftReply(Reply::Snapshot(SnapshotResponse::new(vote(
            2, true,
        ))))),
        Ok(RaftReply(Reply::TransferLeader(Ok(())))),
        Ok(RaftReply(Reply::TransferLeader(Err(
            TransferLeaderError::VoteChanged {
                expected: vote(2, true),
                actual: vote(3, false),
            },
        )))),
        Ok(RaftReply(Reply::TransferLeader(Err(
            TransferLeaderError::LogNotFlushed {
                expected: Some(log_id(2, 7)),
                actual: None,
            },
        )))),
        Ok(RaftReply(Reply::Written)),
        Err(Refused::Unreachable("down".into())),
        Err(Refused::NoSuchMember(addressee())),
        Err(Refused::Stopped(addressee(), "fatal".into())),
        Err(Refused::NoSnapshots(addressee())),
    ]
}

/// openraft's messages do not all compare, so a round trip is checked as
/// the same bytes after decoding, plus the fields a mix-up would move.
#[test]
fn every_request_reads_back_as_written() {
    for written in every_request() {
        let bytes = encode_request(&written).expect("encode");
        let read = decode_request(bytes.clone()).expect("decode");
        assert_eq!(read.group, written.group);
        assert_eq!(read.to, written.to);
        assert_eq!(read.beat, written.beat);
        assert_eq!(read.report, written.report);
        assert_eq!(format!("{read:?}"), format!("{written:?}"));
        assert_eq!(encode_request(&read).expect("encode"), bytes, "{written:?}");
    }
}

#[test]
fn entries_read_back_with_their_payloads() {
    let written = &every_request()[1];
    let bytes = encode_request(written).expect("encode");
    let Request::Append(read) = decode_request(bytes.clone()).expect("decode").body else {
        panic!("not an append");
    };
    let Request::Append(sent) = &written.body else {
        unreachable!()
    };
    assert_eq!(read.entries.len(), 4);
    for (read, sent) in read.entries.iter().zip(&sent.entries) {
        assert_eq!(read.log_id, sent.log_id);
        assert!(read.payload == sent.payload, "{:?}", read.payload);
    }
    // The record is a slice of the frame, not a copy.
    let EntryPayload::Normal(command) = &read.entries[1].payload else {
        panic!("not a record");
    };
    let frame = bytes.as_ptr() as usize..bytes.as_ptr() as usize + bytes.len();
    assert!(frame.contains(&(command.payload.as_ptr() as usize)));
    assert_eq!(read.vote, sent.vote);
    assert_eq!(read.prev_log_id, sent.prev_log_id);
    assert_eq!(read.leader_commit, sent.leader_commit);
}

#[test]
fn every_reply_reads_back_as_written() {
    for written in every_reply() {
        let bytes = encode_reply(&written);
        let read = decode_reply(bytes.clone()).expect("decode");
        assert_eq!(encode_reply(&read), bytes);
        match (&read, &written) {
            (Ok(RaftReply(Reply::Append(a))), Ok(RaftReply(Reply::Append(b)))) => {
                assert_eq!(a, b)
            }
            (Ok(RaftReply(Reply::Vote(a))), Ok(RaftReply(Reply::Vote(b)))) => assert_eq!(a, b),
            (Ok(RaftReply(Reply::Snapshot(a))), Ok(RaftReply(Reply::Snapshot(b)))) => {
                assert_eq!(a, b)
            }
            (Ok(RaftReply(Reply::TransferLeader(a))), Ok(RaftReply(Reply::TransferLeader(b)))) => {
                assert_eq!(a, b)
            }
            (Ok(RaftReply(Reply::Written)), Ok(RaftReply(Reply::Written))) => {}
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
            _ => panic!("an answer read back as another kind"),
        }
    }
}

/// The layout is pinned: a vote, byte by byte.
#[test]
fn a_vote_matches_its_golden_vector() {
    let written = request(Request::Vote(VoteRequest::new(vote(3, false), None)));
    let expected: Vec<u8> = [
        &[3, 3][..],
        // the deadline: 200 ms
        &[0, 0, 0, 200][..],
        // the group: tenant, namespace, stream, shard, kind, epoch
        &[0, 1, b't'][..],
        &[0, 2, b'n', b's'][..],
        &[0, 6][..],
        b"orders",
        &[0, 0, 0, 3][..],
        &[1][..],
        &[0, 0, 0, 0, 0, 0, 0, 7][..],
        // the member: broker, copy
        &[0, 1, b'b'][..],
        &[0, 0, 0, 0, 0, 0, 0, 11][..],
        // the beat
        &[0, 0, 0, 0, 0, 0, 0x07, 0xD0][..],
        // no report asked for
        &[0][..],
        // the vote: term, voted for, committed
        &[0, 0, 0, 0, 0, 0, 0, 3][..],
        &[0, 1, b'a'][..],
        &[0, 0, 0, 0, 0, 0, 0, 11][..],
        &[0][..],
        // no last log id, no transfer
        &[0, 0][..],
    ]
    .concat();
    assert_eq!(encode_request(&written).expect("encode").as_ref(), expected);
}

#[test]
fn a_cut_message_is_refused_at_every_length() {
    for written in every_request() {
        let bytes = encode_request(&written).expect("encode");
        for cut in 0..bytes.len() {
            assert!(
                decode_request(bytes.slice(..cut)).is_err(),
                "{written:?} cut at {cut}"
            );
        }
    }
    for written in every_reply() {
        let bytes = encode_reply(&written);
        for cut in 0..bytes.len() {
            assert!(decode_reply(bytes.slice(..cut)).is_err(), "cut at {cut}");
        }
    }
}

#[test]
fn trailing_bytes_and_other_layouts_are_refused() {
    let bytes = encode_request(&every_request()[2]).expect("encode");
    let mut longer = BytesMut::from(&bytes[..]);
    longer.put_u8(0);
    assert!(decode_request(longer.freeze()).is_err());
    let mut other = BytesMut::from(&bytes[..]);
    other[0] = 4;
    assert!(decode_request(other.freeze()).is_err());

    let reply = encode_reply(&every_reply()[0]);
    let mut other = BytesMut::from(&reply[..]);
    other[0] = 4;
    assert!(decode_reply(other.freeze()).is_err());
}

/// A build from before the report byte writes layout 1 with a header one
/// byte shorter. It is refused by its layout, before any field is read, so
/// the error names the mismatch instead of a length.
#[test]
fn a_layout_1_header_is_refused_by_layout() {
    let sent = every_request().remove(2);
    let bytes = encode_request(&sent).expect("encode");
    // Layout 1 had the same fields up to the beat and no report byte.
    let mut head = BytesMut::new();
    put_group(&mut head, &sent.group).expect("group");
    put_member(&mut head, &sent.to).expect("member");
    let report_at = 2 + 4 + head.len() + 8;
    let mut old = bytes.to_vec();
    old[0] = 1;
    old.remove(report_at);
    let refused = decode_request(Bytes::from(old)).expect_err("refused");
    assert!(
        refused.to_string().contains("layout 1"),
        "refused for something else: {refused}"
    );

    let reply = encode_reply(&every_reply()[0]);
    let mut old = BytesMut::from(&reply[..]);
    old[0] = 1;
    let Err(refused) = decode_reply(old.freeze()) else {
        panic!("a layout 1 answer was read");
    };
    assert!(refused.to_string().contains("layout 1"), "{refused}");
}

/// A build from before snapshots carried data writes layout 2, whose
/// snapshot ends at the membership. It is refused by its layout: read as
/// this one it would only look cut short.
#[test]
fn a_layout_2_snapshot_is_refused_by_layout() {
    let sent = every_request().remove(4);
    let Request::Snapshot { data, .. } = &sent.body else {
        panic!("not the snapshot");
    };
    let bytes = encode_request(&sent).expect("encode");
    // The data's length, the piece's offset, its length and its bytes.
    let mut old = bytes[..bytes.len() - (8 + 8 + 4 + data.len())].to_vec();
    old[0] = 2;
    let refused = decode_request(Bytes::from(old)).expect_err("refused");
    assert!(
        refused.to_string().contains("layout 2"),
        "refused for something else: {refused}"
    );

    let reply = encode_reply(&every_reply()[0]);
    let mut old = BytesMut::from(&reply[..]);
    old[0] = 2;
    let Err(refused) = decode_reply(old.freeze()) else {
        panic!("a layout 2 answer was read");
    };
    assert!(refused.to_string().contains("layout 2"), "{refused}");
}

/// A peer cannot make this broker reserve room for entries the body does
/// not hold.
#[test]
fn an_entry_count_past_the_body_is_refused() {
    let mut written = every_request().remove(1);
    let Request::Append(rpc) = &mut written.body else {
        unreachable!()
    };
    rpc.entries.truncate(1);
    let bytes = encode_request(&written).expect("encode");
    // The count sits just before the one blank entry, which is 17 bytes.
    let at = bytes.len() - 17 - 4;
    let mut forged = BytesMut::from(&bytes[..]);
    forged[at..at + 4].copy_from_slice(&u32::MAX.to_be_bytes());
    let refused = decode_request(forged.freeze()).expect_err("refused");
    assert!(refused.to_string().contains("more than"), "{refused}");
}

#[test]
fn single_byte_corruption_never_panics() {
    for written in every_request() {
        let bytes = encode_request(&written).expect("encode");
        for at in 0..bytes.len() {
            for bit in 0..8 {
                let mut corrupt = bytes.to_vec();
                corrupt[at] ^= 1 << bit;
                let _ = decode_request(Bytes::from(corrupt));
            }
        }
    }
}

#[test]
fn the_pool_reads_the_append_entries_tag_where_it_is() {
    let bytes = encode_request(&every_request()[1]).expect("encode");
    assert_eq!(bytes[1], crate::peer::RAFT_APPEND_ENTRIES);
    let heartbeat = encode_request(&every_request()[0]).expect("encode");
    assert_ne!(heartbeat[1], crate::peer::RAFT_APPEND_ENTRIES);
}

/// An append too big for one frame is cut, and openraft sends the rest
/// next: without the cut it would fail to encode on every retry.
#[test]
fn a_large_append_is_cut_to_fit_a_frame() {
    let record = Bytes::from(vec![7u8; 1024 * 1024]);
    let entries = (1..=100)
        .map(|index| {
            EntryOf::<ShardTypes>::new_normal(
                log_id(2, index),
                Command {
                    timestamp_micros: 1,
                    payload: record.clone(),
                    producer: None,
                    written: None,
                },
            )
        })
        .collect();
    let mut written = request(Request::Append(AppendEntriesRequest {
        vote: vote(2, true),
        prev_log_id: None,
        entries,
        leader_commit: None,
    }));
    fit(&mut written);
    let Request::Append(rpc) = &written.body else {
        unreachable!()
    };
    assert!(
        (1..100).contains(&rpc.entries.len()),
        "{}",
        rpc.entries.len()
    );
    let encoded = encode_request(&written).expect("encode");
    assert!(encoded.len() < felix_wire::internal::MAX_BODY_BYTES as usize);

    // One record is always sent, however large.
    let mut one = written.clone_append(1);
    fit(&mut one);
    let Request::Append(rpc) = &one.body else {
        unreachable!()
    };
    assert_eq!(rpc.entries.len(), 1);
}

/// An append is cut to what crosses a 1 Gbit/s link in a quarter of its
/// deadline, so a cut plus a follower's fsync fits the deadline. A 32 MiB
/// cut takes about 270 ms on that link, past the 200 ms heartbeat.
#[test]
fn an_append_is_cut_to_what_crosses_a_gigabit_link_in_time() {
    let record = Bytes::from(vec![7u8; 1024 * 1024]);
    let entries: Vec<_> = (1..=100)
        .map(|index| {
            EntryOf::<ShardTypes>::new_normal(
                log_id(2, index),
                Command {
                    timestamp_micros: 1,
                    payload: record.clone(),
                    producer: None,
                    written: None,
                },
            )
        })
        .collect();
    let append = |deadline| {
        let mut written = request(Request::Append(AppendEntriesRequest {
            vote: vote(2, true),
            prev_log_id: None,
            entries: entries.clone(),
            leader_commit: None,
        }));
        written.deadline = deadline;
        fit(&mut written);
        encode_request(&written).expect("encode").len()
    };
    let heartbeat = std::time::Duration::from_millis(200);
    let on_the_wire = append(heartbeat) as f64 / 125_000_000.0;
    assert!(on_the_wire <= 0.05, "{on_the_wire} s of a 0.2 s deadline");
    // However long the deadline, the cut stays inside a frame.
    let long = append(std::time::Duration::from_secs(3600));
    assert!(
        long < felix_wire::internal::MAX_BODY_BYTES as usize,
        "{long}"
    );
}

/// The listener reads the sender's deadline from fixed bytes, without
/// decoding the message, and holds it to its own limit.
#[test]
fn the_listener_reads_the_deadline_where_it_is() {
    for written in every_request() {
        let mut written = written;
        written.deadline = std::time::Duration::from_millis(1234);
        let bytes = encode_request(&written).expect("encode");
        assert_eq!(
            crate::peer::raft_deadline(&bytes),
            std::time::Duration::from_millis(1234)
        );
        let read = decode_request(bytes).expect("decode");
        assert_eq!(read.deadline, written.deadline);
    }
    let mut forever = every_request().remove(0);
    forever.deadline = std::time::Duration::from_secs(u64::MAX / 2000);
    let bytes = encode_request(&forever).expect("encode");
    assert_eq!(
        crate::peer::raft_deadline(&bytes),
        crate::peer::RAFT_MAX_DEADLINE
    );
    assert_eq!(
        crate::peer::raft_deadline(b"\x01\x01"),
        crate::peer::RAFT_MAX_DEADLINE
    );
}

/// A forged entry count passes the check against the bytes left whenever
/// each entry could be 17 bytes, but an entry in memory is larger, so room
/// for the count is held to what the bytes left would take in memory.
#[test]
fn room_for_entries_is_held_to_the_bytes_left() {
    let left = 32 * 1024 * 1024;
    let forged = left / MIN_ENTRY_BYTES;
    let room = entry_room(forged, left);
    assert!(
        room * size_of::<EntryOf<ShardTypes>>() <= left,
        "{room} entries reserved from {left} bytes"
    );
    assert_eq!(entry_room(3, left), 3);
}

#[test]
fn a_membership_entry_with_no_voters_is_refused() {
    let empty = Membership::new_with_defaults(vec![BTreeSet::new()], BTreeSet::new());
    let written = request(Request::Append(AppendEntriesRequest {
        vote: vote(2, true),
        prev_log_id: None,
        entries: vec![EntryOf::<ShardTypes>::new_membership(log_id(2, 1), empty)],
        leader_commit: None,
    }));
    let bytes = encode_request(&written).expect("encode");
    let refused = decode_request(bytes).expect_err("refused");
    assert!(refused.to_string().contains("no voters"), "{refused}");
}

/// Tag 4 was a record continuing a batch of several. Nothing sends one, and
/// read as a plain record it would lose its producer.
#[test]
fn an_entry_that_continues_a_batch_is_refused() {
    let record = Command {
        timestamp_micros: 100,
        payload: Bytes::from_static(b"a record"),
        producer: None,
        written: None,
    };
    let written = request(Request::Append(AppendEntriesRequest {
        vote: vote(2, true),
        prev_log_id: None,
        entries: vec![EntryOf::<ShardTypes>::new_normal(log_id(2, 1), record)],
        leader_commit: None,
    }));
    let mut bytes = BytesMut::from(&encode_request(&written).expect("encode")[..]);
    let payload = bytes.windows(8).position(|at| at == b"a record");
    // The tag, the timestamp and the length come before the payload.
    let tag = payload.expect("the record") - 4 - 8 - 1;
    assert_eq!(bytes[tag], 1, "not the entry's tag");
    bytes[tag] = 4;
    assert!(decode_request(bytes.freeze()).is_err());
}

impl RaftRequest {
    /// The first `n` entries of an append, with the rest of the request.
    fn clone_append(&self, n: usize) -> RaftRequest {
        let Request::Append(rpc) = &self.body else {
            unreachable!()
        };
        request(Request::Append(AppendEntriesRequest {
            vote: rpc.vote.clone(),
            prev_log_id: rpc.prev_log_id,
            entries: rpc.entries[..n].to_vec(),
            leader_commit: rpc.leader_commit,
        }))
    }
}
