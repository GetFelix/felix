//! The `ELECTION` messages: a candidate's fence, a stamped batch and the
//! pre-vote.

use super::*;

fn member(node: &str, storage: Option<u8>) -> SetMember {
    SetMember {
        node_id: node.to_string(),
        storage_id: storage.map(|byte| [byte; 16]),
        learner: false,
    }
}

fn view(generation: u64, nodes: &[&str]) -> ReplicaSetView {
    ReplicaSetView {
        generation,
        members: nodes
            .iter()
            .zip(1..)
            .map(|(node, id)| member(node, Some(id)))
            .collect(),
    }
}

fn election() -> FenceElection {
    FenceElection {
        candidate: true,
        set: view(9, &["a", "b", "c"]),
        leaving: Some(view(8, &["a", "b", "d"])),
    }
}

fn fence(election: Option<FenceElection>) -> Fence {
    Fence {
        correlation_id: 42,
        shard: shard(),
        log: ReplicaLog::Stream,
        election,
    }
}

fn stamped(message: fn(ReplicateRecords) -> InternalMessage) -> InternalMessage {
    let InternalMessage::ReplicateRecords(mut records) = replicate() else {
        unreachable!()
    };
    records.set_generation = Some(11);
    message(records)
}

fn pre_vote() -> PreVote {
    PreVote {
        correlation_id: 42,
        shard: shard(),
        log: ReplicaLog::Stream,
        last_generation: 6,
        log_end: 120,
        set_generation: 9,
    }
}

fn frame(kind: Kind, body: &[u8]) -> Bytes {
    let mut frame = BytesMut::new();
    InternalHeader {
        kind,
        length: body.len() as u32,
    }
    .encode(&mut frame);
    frame.extend_from_slice(body);
    frame.freeze()
}

/// A fence body up to and including its log, ready for the election fields.
fn fence_prefix() -> BytesMut {
    let encoded = InternalMessage::Fence(fence(None))
        .encode()
        .expect("encode");
    BytesMut::from(&encoded[InternalHeader::LEN..])
}

fn put_set(body: &mut BytesMut, generation: u64, members: &[(&[u8], u8, Option<u8>)]) {
    body.put_u64(generation);
    body.put_u16(members.len() as u16);
    for (node, flags, storage) in members {
        body.put_u16(node.len() as u16);
        body.put_slice(node);
        body.put_u8(*flags);
        if let Some(byte) = storage {
            body.put_slice(&[*byte; 16]);
        }
    }
}

fn election_fence(
    candidate: u8,
    set: impl FnOnce(&mut BytesMut),
    leaving: Option<&dyn Fn(&mut BytesMut)>,
) -> Bytes {
    let mut body = fence_prefix();
    body.put_u8(candidate);
    set(&mut body);
    match leaving {
        Some(put) => {
            body.put_u8(1);
            put(&mut body);
        }
        None => body.put_u8(0),
    }
    frame(Kind::ElectionFence, &body)
}

#[test]
fn the_election_bit_is_the_reserved_one() {
    assert_eq!(PeerCapabilities::ELECTION.bits(), 1 << 9);
    let hello = InternalMessage::Hello(Hello {
        correlation_id: 1,
        node_id: "a".to_string(),
        capabilities: Some(PeerCapabilities::ELECTION),
    });
    let back = InternalMessage::decode(hello.encode().expect("encode")).expect("decode");
    assert_eq!(back, hello);
}

#[test]
fn the_new_kind_discriminants_are_fixed() {
    assert_eq!(Kind::ElectionFence as u16, 41);
    assert_eq!(Kind::SetStamped as u16, 42);
    assert_eq!(Kind::PreVote as u16, 43);
    assert_eq!(Kind::PreVoteAnswer as u16, 44);
    assert!(Kind::from_u16(45).is_err());
}

#[test]
fn a_candidate_fence_round_trips() {
    for election in [
        election(),
        FenceElection {
            candidate: false,
            leaving: None,
            ..election()
        },
        FenceElection {
            candidate: true,
            set: ReplicaSetView {
                generation: 3,
                members: vec![
                    member("a", None),
                    SetMember {
                        learner: true,
                        ..member("b", Some(2))
                    },
                ],
            },
            leaving: None,
        },
    ] {
        let message = InternalMessage::Fence(fence(Some(election)));
        assert_eq!(message.kind(), Kind::ElectionFence);
        assert_eq!(
            message.required_capability(),
            Some(PeerCapabilities::ELECTION)
        );
        let encoded = message.encode().expect("encode");
        assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);
    }
}

#[test]
fn a_candidate_fence_for_an_incarnated_stream_round_trips() {
    let mut fence = fence(Some(election()));
    fence.shard.incarnation = Some(5);
    let message = InternalMessage::Fence(fence);
    let encoded = message.encode().expect("encode");
    assert_eq!(
        InternalHeader::decode(&encoded).expect("header").kind,
        Kind::Incarnated
    );
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);
}

/// The layout, pinned: the plain fence's body, then the candidate flag, the
/// set and the set being left.
#[test]
fn a_candidate_fence_matches_its_golden_vector() {
    let message = InternalMessage::Fence(fence(Some(FenceElection {
        candidate: true,
        set: ReplicaSetView {
            generation: 9,
            members: vec![SetMember {
                node_id: "a".to_string(),
                storage_id: Some([1; 16]),
                learner: true,
            }],
        },
        leaving: None,
    })));
    let mut expected = fence_prefix();
    expected.put_u8(1);
    expected.put_u64(9);
    expected.put_u16(1);
    expected.put_u16(1);
    expected.put_slice(b"a");
    expected.put_u8(3);
    expected.put_slice(&[1; 16]);
    expected.put_u8(0);
    assert_eq!(
        message.encode().expect("encode"),
        frame(Kind::ElectionFence, &expected)
    );
}

#[test]
fn a_set_at_the_limits_is_accepted_and_one_past_them_is_not() {
    let node = "n".repeat(MAX_SET_NODE_BYTES);
    let largest = FenceElection {
        candidate: true,
        set: ReplicaSetView {
            generation: 1,
            members: (0..MAX_SET_MEMBERS as u8)
                .map(|i| member(&format!("{i:03}{}", &node[3..]), Some(i)))
                .collect(),
        },
        leaving: None,
    };
    let message = InternalMessage::Fence(fence(Some(largest.clone())));
    let encoded = message.encode().expect("the limits themselves are fine");
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);

    let mut too_many = largest.clone();
    too_many.set.members.push(member("extra", None));
    let mut too_long = largest;
    too_long.set.members[0].node_id.push('x');
    for election in [too_many, too_long] {
        let refused = InternalMessage::Fence(fence(Some(election))).encode();
        assert!(matches!(refused, Err(Error::FrameTooLarge)), "{refused:?}");
    }
}

#[test]
fn a_set_past_the_limits_is_refused_on_decode() {
    let many: Vec<(Vec<u8>, u8, Option<u8>)> = (0..=MAX_SET_MEMBERS as u8)
        .map(|i| (vec![b'a', i], 0, None))
        .collect();
    let many: Vec<(&[u8], u8, Option<u8>)> =
        many.iter().map(|(n, f, s)| (&n[..], *f, *s)).collect();
    let refused = InternalMessage::decode(election_fence(1, |b| put_set(b, 1, &many), None));
    assert!(matches!(refused, Err(Error::Incomplete)), "{refused:?}");

    let long = vec![b'n'; MAX_SET_NODE_BYTES + 1];
    let refused = InternalMessage::decode(election_fence(
        1,
        |b| put_set(b, 1, &[(&long, 0, None)]),
        None,
    ));
    assert!(matches!(refused, Err(Error::Incomplete)), "{refused:?}");

    // A claimed count the body cannot hold is refused before anything is
    // sized from it.
    let refused = InternalMessage::decode(election_fence(
        1,
        |b| {
            b.put_u64(1);
            b.put_u16(MAX_SET_MEMBERS as u16);
        },
        None,
    ));
    assert!(matches!(refused, Err(Error::Incomplete)), "{refused:?}");
}

#[test]
fn a_malformed_candidate_fence_is_refused() {
    let one = |b: &mut BytesMut| put_set(b, 5, &[(b"a", 0, None)]);
    // Not a boolean.
    assert!(InternalMessage::decode(election_fence(2, one, None)).is_err());
    // An unknown member flag.
    assert!(
        InternalMessage::decode(election_fence(
            1,
            |b| put_set(b, 5, &[(b"a", 4, None)]),
            None
        ))
        .is_err()
    );
    // The storage flag with no storage id after it.
    assert!(
        InternalMessage::decode(election_fence(
            1,
            |b| put_set(b, 5, &[(b"a", 1, None)]),
            None
        ))
        .is_err()
    );
    // A node listed twice, and a storage id listed twice.
    assert!(
        InternalMessage::decode(election_fence(
            1,
            |b| put_set(b, 5, &[(b"a", 0, None), (b"a", 0, None)]),
            None
        ))
        .is_err()
    );
    assert!(
        InternalMessage::decode(election_fence(
            1,
            |b| put_set(b, 5, &[(b"a", 1, Some(1)), (b"b", 1, Some(1))]),
            None
        ))
        .is_err()
    );
    // Node ids are text.
    assert!(
        InternalMessage::decode(election_fence(
            1,
            |b| put_set(b, 5, &[(&[0xff, 0xfe], 0, None)]),
            None
        ))
        .is_err()
    );
    // A set being left must be older than the one replacing it.
    for left in [5, 6] {
        assert!(
            InternalMessage::decode(election_fence(
                1,
                one,
                Some(&|b| put_set(b, left, &[(b"a", 0, None)]))
            ))
            .is_err(),
            "left at {left}",
        );
    }
    assert!(
        InternalMessage::decode(election_fence(
            1,
            one,
            Some(&|b| put_set(b, 4, &[(b"a", 0, None)]))
        ))
        .is_ok()
    );
    // The leaving marker is a boolean too.
    let mut body = fence_prefix();
    body.put_u8(1);
    one(&mut body);
    body.put_u8(2);
    assert!(InternalMessage::decode(frame(Kind::ElectionFence, &body)).is_err());
    // Slack after the fields, and every truncation.
    let good = election_fence(1, one, None);
    let mut slack = BytesMut::from(&good[InternalHeader::LEN..]);
    slack.put_u8(0);
    assert!(InternalMessage::decode(frame(Kind::ElectionFence, &slack)).is_err());
    let body = &good[InternalHeader::LEN..];
    for cut in 0..body.len() {
        assert!(
            InternalMessage::decode(frame(Kind::ElectionFence, &body[..cut])).is_err(),
            "cut at {cut}"
        );
    }
}

#[test]
fn a_duplicate_or_backwards_set_is_not_encoded() {
    let mut duplicate = election();
    duplicate.set.members.push(member("a", None));
    let mut backwards = election();
    backwards.leaving = Some(view(9, &["a"]));
    for election in [duplicate, backwards] {
        assert!(
            InternalMessage::Fence(fence(Some(election)))
                .encode()
                .is_err()
        );
    }
}

#[test]
fn a_stamped_batch_round_trips_for_every_log() {
    for message in [
        stamped(InternalMessage::ReplicateRecords),
        stamped(InternalMessage::ReplicateCacheRecords),
        stamped(InternalMessage::ReplicateGroupRecords),
        stamped(InternalMessage::ReplicateDeadLetterRecords),
        stamped(InternalMessage::ReplicateCounterRecords),
    ] {
        assert_eq!(
            message.required_capability(),
            Some(PeerCapabilities::ELECTION)
        );
        let encoded = message.encode().expect("encode");
        assert_eq!(
            InternalHeader::decode(&encoded).expect("header").kind,
            Kind::SetStamped
        );
        assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);
    }
}

#[test]
fn a_stamp_keeps_every_other_field_of_the_batch() {
    let InternalMessage::ReplicateRecords(plain) = replicate() else {
        unreachable!()
    };
    let variants = [
        ReplicateRecords {
            commit_offset: Some(90),
            ..plain.clone()
        },
        ReplicateRecords {
            generations: Some(vec![GenerationStart {
                generation: 6,
                start_offset: 100,
            }]),
            times: Some(vec![1, 2]),
            ..plain.clone()
        },
        ReplicateRecords {
            marks: vec![
                ProducerMark::Opens {
                    producer_id: 3,
                    sequence: 1,
                    len: 2,
                },
                ProducerMark::Continues,
            ],
            ..plain.clone()
        },
        ReplicateRecords {
            shard: ShardRef {
                incarnation: Some(5),
                ..shard()
            },
            ..plain
        },
    ];
    for variant in variants {
        let stamped = ReplicateRecords {
            set_generation: Some(0),
            ..variant.clone()
        };
        let wrap = |records: ReplicateRecords| {
            if records.marks.is_empty() {
                InternalMessage::ReplicateRecords(records)
            } else {
                InternalMessage::ReplicateMarkedRecords(records)
            }
        };
        let (plain, stamped) = (wrap(variant), wrap(stamped));
        let plain_frame = plain.encode().expect("encode");
        let stamped_frame = stamped.encode().expect("encode");
        assert_eq!(
            InternalMessage::decode(stamped_frame.clone()).expect("decode"),
            stamped
        );
        // Outside the stamp is the plain frame's own kind and body.
        assert_eq!(
            &stamped_frame[InternalHeader::LEN + 18..],
            &plain_frame[InternalHeader::LEN..]
        );
    }
}

#[test]
fn a_stamp_is_refused_around_anything_but_records() {
    let stamp = |inner: Bytes| {
        let mut body = BytesMut::new();
        body.put_u64(42);
        body.put_u64(11);
        body.put_u16(InternalHeader::decode(&inner).expect("header").kind as u16);
        body.put_slice(&inner[InternalHeader::LEN..]);
        frame(Kind::SetStamped, &body)
    };
    for inner in [
        InternalMessage::Ping(Ping { correlation_id: 42 }),
        InternalMessage::Fence(fence(None)),
        InternalMessage::PreVote(pre_vote()),
        replicate_ok(),
    ] {
        let refused = InternalMessage::decode(stamp(inner.encode().expect("encode")));
        assert!(refused.is_err(), "{inner:?}");
    }
    // Not around another stamp, and not inside an incarnation.
    let once = stamp(replicate().encode().expect("encode"));
    assert!(InternalMessage::decode(stamp(once.clone())).is_err());
    let mut inside = BytesMut::new();
    inside.put_u64(42);
    inside.put_u64(5);
    inside.put_u16(Kind::SetStamped as u16);
    inside.put_slice(&once[InternalHeader::LEN..]);
    assert!(InternalMessage::decode(frame(Kind::Incarnated, &inside)).is_err());
    // The id outside has to be the one inside.
    let mut wrong = BytesMut::from(&once[InternalHeader::LEN..]);
    wrong[7] = 43;
    assert!(InternalMessage::decode(frame(Kind::SetStamped, &wrong)).is_err());
}

fn replicate_ok() -> InternalMessage {
    InternalMessage::ReplicateOk(ReplicateOk {
        correlation_id: 42,
        durable_offset: 1,
    })
}

#[test]
fn a_pre_vote_round_trips_and_needs_election() {
    let message = InternalMessage::PreVote(pre_vote());
    assert_eq!(message.kind(), Kind::PreVote);
    assert_eq!(
        message.required_capability(),
        Some(PeerCapabilities::ELECTION)
    );
    let encoded = message.encode().expect("encode");
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);

    let mut incarnated = pre_vote();
    incarnated.shard.incarnation = Some(5);
    let message = InternalMessage::PreVote(incarnated);
    let encoded = message.encode().expect("encode");
    assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);
}

#[test]
fn an_answer_is_granted_or_refused_with_a_reason() {
    for refusal in [
        None,
        Some(PreVoteRefusal::LeaderReachable),
        Some(PreVoteRefusal::LogBehind),
        Some(PreVoteRefusal::StaleGeneration),
        Some(PreVoteRefusal::StaleSet),
        Some(PreVoteRefusal::NotVoting),
        Some(PreVoteRefusal::UnknownShard),
    ] {
        let answer = PreVoteAnswer {
            correlation_id: 42,
            refusal,
        };
        assert_eq!(answer.granted(), refusal.is_none());
        let message = InternalMessage::PreVoteAnswer(answer);
        assert_eq!(
            message.required_capability(),
            Some(PeerCapabilities::ELECTION)
        );
        let encoded = message.encode().expect("encode");
        assert_eq!(InternalMessage::decode(encoded).expect("decode"), message);
    }
}

#[test]
fn a_malformed_answer_is_refused() {
    let answer = |granted: u8, reason: u8| {
        let mut body = BytesMut::new();
        body.put_u64(42);
        body.put_u8(granted);
        body.put_u8(reason);
        frame(Kind::PreVoteAnswer, &body)
    };
    assert!(InternalMessage::decode(answer(1, 0)).is_ok());
    assert!(InternalMessage::decode(answer(0, 1)).is_ok());
    // A grant with a reason, a refusal with none or an unknown one, and a
    // flag that is not a boolean each have a second reading.
    for (granted, reason) in [(1, 1), (0, 0), (0, 7), (2, 0), (0, 255)] {
        assert!(
            InternalMessage::decode(answer(granted, reason)).is_err(),
            "{granted} {reason}"
        );
    }
}

#[test]
fn a_malformed_pre_vote_is_refused() {
    let good = InternalMessage::PreVote(pre_vote())
        .encode()
        .expect("encode");
    let body = &good[InternalHeader::LEN..];
    for cut in 0..body.len() {
        assert!(InternalMessage::decode(frame(Kind::PreVote, &body[..cut])).is_err());
    }
    let mut slack = BytesMut::from(body);
    slack.put_u8(0);
    assert!(InternalMessage::decode(frame(Kind::PreVote, &slack)).is_err());
}

#[test]
fn the_new_kinds_begin_with_their_correlation_id() {
    for message in [
        InternalMessage::Fence(fence(Some(election()))),
        stamped(InternalMessage::ReplicateRecords),
        InternalMessage::PreVote(pre_vote()),
        InternalMessage::PreVoteAnswer(PreVoteAnswer {
            correlation_id: 42,
            refusal: None,
        }),
    ] {
        let encoded = message.encode().expect("encode");
        assert_eq!(
            correlation_id_in(&encoded[InternalHeader::LEN..]),
            Some(42),
            "{message:?}"
        );
    }
}

/// The fuzzer starts from these, so each has to be a frame that decodes and
/// encodes back to itself.
#[test]
fn the_fuzz_seeds_for_the_new_kinds_are_valid_frames() {
    for (name, seed, kind) in [
        (
            "election_fence",
            &include_bytes!("../../../fuzz/seeds/internal_message/election_fence")[..],
            Kind::ElectionFence,
        ),
        (
            "set_stamped",
            &include_bytes!("../../../fuzz/seeds/internal_message/set_stamped")[..],
            Kind::SetStamped,
        ),
        (
            "pre_vote",
            &include_bytes!("../../../fuzz/seeds/internal_message/pre_vote")[..],
            Kind::PreVote,
        ),
        (
            "pre_vote_refused",
            &include_bytes!("../../../fuzz/seeds/internal_message/pre_vote_refused")[..],
            Kind::PreVoteAnswer,
        ),
    ] {
        let seed = Bytes::copy_from_slice(seed);
        assert_eq!(InternalHeader::decode(&seed).expect(name).kind, kind);
        let message = InternalMessage::decode(seed.clone()).expect(name);
        assert_eq!(message.encode().expect(name), seed, "{name}");
    }
}
