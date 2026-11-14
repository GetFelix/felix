//! The rules of `docs/formal/FelixRaftCache.tla`, one test per rule. Each
//! names the model's check it stands for.

use bytes::Bytes;

use super::{Answer, CacheCommand, CacheState, Condition, Read, Request, floor, stamp};

fn request(producer: u64, sequence: u64) -> Request {
    Request {
        producer,
        sequence,
        floor: 0,
    }
}

fn put_with(
    key: &str,
    value: &str,
    expires_at: u64,
    cond: Condition,
    request: Request,
) -> CacheCommand {
    CacheCommand::Put {
        key: key.into(),
        value: Bytes::copy_from_slice(value.as_bytes()),
        expires_at,
        cond,
        request,
    }
}

fn put(key: &str, value: &str, request: Request) -> CacheCommand {
    put_with(key, value, 0, Condition::None, request)
}

fn delete(key: &str, version: Option<u64>, request: Request) -> CacheCommand {
    CacheCommand::Delete {
        key: key.into(),
        version,
        request,
    }
}

fn add(key: &str, delta: i64, request: Request) -> CacheCommand {
    CacheCommand::Add {
        key: key.into(),
        delta,
        request,
    }
}

fn expire(key: &str, version: u64) -> CacheCommand {
    CacheCommand::Expire {
        key: key.into(),
        version,
    }
}

fn value(state: &CacheState, key: &str, clock: u64) -> Option<(String, u64)> {
    match state.read(key, clock) {
        Read::Value { value, version } => {
            Some((String::from_utf8(value.to_vec()).unwrap(), version))
        }
        Read::Absent => None,
        Read::Expire { .. } => panic!("the read has to commit an expiry first"),
    }
}

/// AppliedOnce, AnswersKept, SumExact.
#[test]
fn a_retry_of_each_write_kind_is_applied_once_and_gets_its_first_answer() {
    let mut state = CacheState::default();
    // A put, then a newer put by someone else, then the first one's retry.
    let first = put("k", "one", request(1, 1));
    assert_eq!(
        state.apply(1, 10, &first),
        Some(Answer::Written { version: 1 })
    );
    state.apply(2, 10, &put("k", "two", request(2, 1)));
    assert_eq!(
        state.apply(3, 10, &first),
        Some(Answer::Written { version: 1 })
    );
    assert_eq!(value(&state, "k", 10), Some(("two".into(), 2)));

    // A delete, a newer put, the delete's retry.
    let gone = delete("k", None, request(1, 2));
    assert_eq!(
        state.apply(4, 10, &gone),
        Some(Answer::Removed { version: 2 })
    );
    state.apply(5, 10, &put("k", "three", request(2, 2)));
    assert_eq!(
        state.apply(6, 10, &gone),
        Some(Answer::Removed { version: 2 })
    );
    assert_eq!(value(&state, "k", 10), Some(("three".into(), 5)));

    // A put-if-absent, a delete, the put-if-absent's retry.
    let taken = put_with("a", "mine", 0, Condition::Absent, request(1, 3));
    assert_eq!(
        state.apply(7, 10, &taken),
        Some(Answer::Written { version: 7 })
    );
    state.apply(8, 10, &delete("a", None, request(2, 3)));
    assert_eq!(
        state.apply(9, 10, &taken),
        Some(Answer::Written { version: 7 })
    );
    assert_eq!(value(&state, "a", 10), None);

    // A put and a delete that name a version.
    let swap = put_with("k", "four", 0, Condition::Version(5), request(1, 4));
    assert_eq!(
        state.apply(10, 10, &swap),
        Some(Answer::Written { version: 10 })
    );
    assert_eq!(
        state.apply(11, 10, &swap),
        Some(Answer::Written { version: 10 })
    );
    let drop = delete("k", Some(10), request(1, 5));
    assert_eq!(
        state.apply(12, 10, &drop),
        Some(Answer::Removed { version: 10 })
    );
    state.apply(13, 10, &put("k", "five", request(2, 4)));
    assert_eq!(
        state.apply(14, 10, &drop),
        Some(Answer::Removed { version: 10 })
    );
    assert_eq!(value(&state, "k", 10), Some(("five".into(), 13)));

    // An add.
    let bump = add("n", 2, request(1, 6));
    assert_eq!(state.apply(15, 10, &bump), Some(Answer::Sum(2)));
    assert_eq!(state.apply(16, 10, &bump), Some(Answer::Sum(2)));
    assert_eq!(state.sum("n"), Some(2));
    assert_eq!(state.answer(1, 6), Some(Answer::Sum(2)));
}

/// AppliedOnce: a second commit moves the log's time and nothing else.
#[test]
fn a_retry_moves_only_the_logs_time() {
    let mut state = CacheState::default();
    let bump = add("n", 1, request(1, 1));
    state.apply(1, 10, &bump);
    let mut expected = state.clone();
    state.apply(2, 25, &bump);
    expected.time = 25;
    assert_eq!(state, expected);
}

/// SnapshotDropsDedup.
#[test]
fn a_snapshot_keeps_the_answer_table() {
    let mut state = CacheState::default();
    let bump = add("n", 1, request(1, 1));
    state.apply(1, 10, &bump);
    let mut restored = CacheState::restore(&state.snapshot(), true).unwrap();
    assert_eq!(restored.apply(2, 11, &bump), Some(Answer::Sum(1)));
    assert_eq!(restored.sum("n"), Some(1));
}

/// SnapshotDropsTime, StampsMonotone.
#[test]
fn a_leader_that_installed_a_snapshot_stamps_from_it() {
    let mut state = CacheState::default();
    state.apply(1, 100, &put("k", "v", request(1, 1)));
    let restored = CacheState::restore(&state.snapshot(), true).unwrap();
    assert_eq!(restored.time(), 100);
    // Its log holds only the blank entry of its own term, and its clock is
    // behind the leader that stamped 100.
    assert_eq!(stamp(5, floor([None], restored.time())), 100);
}

/// FloorFromCommitted, FloorCountsBlank.
#[test]
fn a_new_leader_stamps_above_an_inherited_tail() {
    // Newest first: its own blank, an uncommitted entry of the old leader,
    // a committed one. The snapshot is older than all of them.
    let tail = [None, Some(70), Some(50)];
    assert_eq!(floor(tail, 10), 70);
    assert_eq!(stamp(60, floor(tail, 10)), 70);
}

/// StampNotMonotone.
#[test]
fn a_stamp_never_goes_back() {
    assert_eq!(stamp(5, 9), 9);
    assert_eq!(stamp(12, 9), 12);
    assert_eq!(floor([None, None], 4), 4);
    assert_eq!(floor([], 4), 4);
}

/// ExpiryRemovesItsVersion.
#[test]
fn a_refreshed_key_survives_its_expiry_delete() {
    let mut state = CacheState::default();
    state.apply(
        1,
        0,
        &put_with("k", "old", 10, Condition::None, request(1, 1)),
    );
    // The timer saw version 1 expire. A put got into the log ahead of its
    // delete.
    assert_eq!(state.due(10, 8), vec![("k".to_string(), 1)]);
    state.apply(
        2,
        10,
        &put_with("k", "new", 30, Condition::None, request(1, 2)),
    );
    assert_eq!(state.apply(3, 11, &expire("k", 1)), None);
    assert_eq!(value(&state, "k", 11), Some(("new".into(), 2)));
    // The delete of the version it names does remove it.
    state.apply(4, 30, &expire("k", 2));
    assert_eq!(value(&state, "k", 30), None);
    assert_eq!(state.due(u64::MAX, 8), vec![]);
}

/// ReadsNotStale.
#[test]
fn a_get_on_an_idle_group_never_returns_an_expired_key() {
    let mut state = CacheState::default();
    state.apply(
        1,
        0,
        &put_with("k", "v", 10, Condition::None, request(1, 1)),
    );
    assert_eq!(value(&state, "k", 9), Some(("v".into(), 1)));
    // Nothing was written since, so the log's time is still 0.
    assert_eq!(state.read("k", 10), Read::Expire { version: 1 });
    // A clock behind the log's time does not bring a key back.
    state.apply(2, 10, &add("n", 1, request(1, 2)));
    assert_eq!(state.read("k", 3), Read::Absent);
}

/// OneClock.
#[test]
fn an_absent_get_is_never_followed_by_a_refused_put_if_absent() {
    let mut state = CacheState::default();
    state.apply(
        1,
        0,
        &put_with("k", "v", 10, Condition::None, request(1, 1)),
    );
    // The leader's clock says expired, the log does not: no "absent" yet.
    let Read::Expire { version } = state.read("k", 10) else {
        panic!("the read answered from the leader's clock alone");
    };
    let at = stamp(10, state.time());
    state.apply(2, at, &expire("k", version));
    assert_eq!(state.read("k", 10), Read::Absent);
    // A new leader whose clock is at 4 stamps from the log's time, and its
    // put-if-absent finds the key absent, as the read said.
    let at = stamp(4, floor([None, Some(state.time())], 0));
    let taken = put_with("k", "w", 0, Condition::Absent, request(2, 1));
    assert_eq!(
        state.apply(3, at, &taken),
        Some(Answer::Written { version: 3 })
    );
}

/// OneClock with a delete (the SeesExpired twin for "delete").
#[test]
fn a_delete_of_an_expired_key_answers_absent() {
    let mut state = CacheState::default();
    state.apply(
        1,
        0,
        &put_with("k", "v", 10, Condition::None, request(1, 1)),
    );
    // Another write moves the log's time past the expiry. No expiry delete
    // has run, so the record is still held.
    state.apply(2, 10, &add("n", 1, request(1, 2)));
    assert_eq!(state.read("k", 0), Read::Absent);
    let before = state.clone();
    assert_eq!(
        state.apply(3, 10, &delete("k", None, request(1, 3))),
        Some(Answer::Absent)
    );
    // It changed nothing but the answer it keeps.
    assert!(state.view().entries().eq(before.view().entries()));
}

/// OneClock and CondExact for putAbsent, putIf and deleteIf.
#[test]
fn an_expired_key_is_absent_to_conditional_writes() {
    let mut state = CacheState::default();
    state.apply(
        1,
        0,
        &put_with("k", "v", 10, Condition::None, request(1, 1)),
    );
    let swap = put_with("k", "w", 0, Condition::Version(1), request(1, 2));
    assert_eq!(
        state.apply(2, 10, &swap),
        Some(Answer::Refused { version: None })
    );
    let drop = delete("k", Some(1), request(1, 3));
    assert_eq!(
        state.apply(3, 10, &drop),
        Some(Answer::Refused { version: None })
    );
    let taken = put_with("k", "x", 0, Condition::Absent, request(1, 4));
    assert_eq!(
        state.apply(4, 10, &taken),
        Some(Answer::Written { version: 4 })
    );

    // Unexpired, each goes by the version.
    let stale = put_with("k", "y", 0, Condition::Version(1), request(1, 5));
    assert_eq!(
        state.apply(5, 10, &stale),
        Some(Answer::Refused { version: Some(4) })
    );
    let held = put_with("k", "z", 0, Condition::Absent, request(1, 6));
    assert_eq!(
        state.apply(6, 10, &held),
        Some(Answer::Refused { version: Some(4) })
    );
    let stale = delete("k", Some(1), request(1, 7));
    assert_eq!(
        state.apply(7, 10, &stale),
        Some(Answer::Refused { version: Some(4) })
    );
    assert_eq!(value(&state, "k", 10), Some(("x".into(), 4)));
}

/// MembersAgree with SnapshotDropsExpired.
#[test]
fn a_member_restored_from_a_snapshot_answers_like_one_that_replayed() {
    let mut replayed = CacheState::default();
    replayed.apply(
        1,
        0,
        &put_with("k", "v", 10, Condition::None, request(1, 1)),
    );
    replayed.apply(2, 10, &add("n", 1, request(1, 2)));
    let mut restored = CacheState::restore(&replayed.snapshot(), true).unwrap();
    // The snapshot left the expired record out; the other member holds it.
    assert!(replayed.view().entry("k").is_some());
    assert!(restored.view().entry("k").is_none());
    for command in [
        delete("k", None, request(1, 3)),
        delete("k", Some(1), request(1, 4)),
        put_with("k", "w", 0, Condition::Version(1), request(1, 5)),
        put_with("k", "x", 0, Condition::Absent, request(1, 6)),
    ] {
        assert_eq!(
            replayed.apply(3, 10, &command),
            restored.apply(3, 10, &command)
        );
    }
    assert_eq!(replayed.snapshot(), restored.snapshot());
}

/// A step of a small generator, so the run is the same every time.
fn next(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *seed >> 33
}

fn random_command(seed: &mut u64, index: u64, stamp: u64) -> CacheCommand {
    let key = ["a", "b", "c"][next(seed) as usize % 3];
    // A third of the requests are retries of an earlier sequence.
    let sequence = match next(seed) % 3 {
        0 => next(seed) % index.max(1),
        _ => index,
    };
    let request = Request {
        producer: next(seed) % 2,
        sequence,
        floor: sequence.saturating_sub(4),
    };
    let version = next(seed) % index.max(1);
    match next(seed) % 9 {
        0 => put(key, "v", request),
        1 => put_with(
            key,
            "t",
            stamp + 1 + next(seed) % 3,
            Condition::None,
            request,
        ),
        2 => put_with(key, "a", 0, Condition::Absent, request),
        3 => put_with(key, "c", 0, Condition::Version(version), request),
        4 => delete(key, None, request),
        5 => delete(key, Some(version), request),
        6 => expire(key, version),
        7 => CacheCommand::Retire {
            producer: request.producer,
            floor: sequence + 1,
        },
        _ => add(key, 1, request),
    }
}

/// MembersAgree, ProjectionIsFold: a member that replayed the whole log, one
/// that restarted from its own snapshot and one that installed it give the
/// same answers and write the same snapshot at every index after.
#[test]
fn members_agree_at_every_index() {
    for run in 0..200u64 {
        let mut seed = run;
        let cut = 1 + next(&mut seed) % 40;
        let mut replayed = CacheState::default();
        let mut restored = None::<CacheState>;
        let mut stamp = 0;
        for index in 1..=60 {
            stamp += next(&mut seed) % 2;
            let command = random_command(&mut seed, index, stamp);
            let decoded = CacheCommand::decode(&command.encode()).unwrap();
            assert_eq!(decoded, command);
            let answer = replayed.apply(index, stamp, &command);
            if let Some(restored) = &mut restored {
                assert_eq!(
                    restored.apply(index, stamp, &decoded),
                    answer,
                    "run {run} index {index}"
                );
                assert_eq!(
                    restored.snapshot(),
                    replayed.snapshot(),
                    "run {run} index {index}"
                );
            }
            if index == cut {
                restored = Some(CacheState::restore(&replayed.snapshot(), true).unwrap());
            }
        }
    }
}

/// The trim rule: an entry's floor drops its producer's older answers, and a
/// retry from below it is refused, never applied again.
#[test]
fn an_answer_below_the_floor_is_forgotten_not_applied_again() {
    let mut state = CacheState::default();
    let first = add("n", 1, request(1, 1));
    state.apply(1, 0, &first);
    let later = CacheCommand::Add {
        key: "n".into(),
        delta: 1,
        request: Request {
            producer: 1,
            sequence: 9,
            floor: 5,
        },
    };
    state.apply(2, 0, &later);
    assert_eq!(state.answer(1, 1), None);
    assert_eq!(state.apply(3, 0, &first), Some(Answer::Forgotten));
    assert_eq!(state.sum("n"), Some(2));
    // A floor past the request's own sequence keeps the request's answer.
    let eager = CacheCommand::Add {
        key: "n".into(),
        delta: 1,
        request: Request {
            producer: 1,
            sequence: 10,
            floor: 99,
        },
    };
    state.apply(4, 0, &eager);
    assert_eq!(state.apply(5, 0, &eager), Some(Answer::Sum(3)));
    // The floor is in the snapshot.
    let mut restored = CacheState::restore(&state.snapshot(), true).unwrap();
    assert_eq!(restored.apply(6, 0, &first), Some(Answer::Forgotten));
    // Another producer's table is its own.
    assert_eq!(
        restored.apply(7, 0, &add("n", 1, request(2, 1))),
        Some(Answer::Sum(4))
    );

    restored.apply(
        8,
        0,
        &CacheCommand::Retire {
            producer: 1,
            floor: 11,
        },
    );
    assert_eq!(restored.answer(1, 10), None);
    assert_eq!(restored.answer(2, 1), Some(Answer::Sum(4)));
}

/// RetireDropsFloor: a retire keeps the producer's floor as a tombstone. A
/// copy of an answered request, held in a queue or on the network, can
/// commit after the retire; it answers forgotten and is not applied again,
/// on a member that replayed and on one that restored a snapshot.
#[test]
fn a_copy_that_commits_after_a_retire_is_not_applied_again() {
    let mut state = CacheState::default();
    let first = CacheCommand::Add {
        key: "n".into(),
        delta: 1,
        request: Request {
            producer: 7,
            sequence: 3,
            floor: 3,
        },
    };
    assert_eq!(state.apply(1, 0, &first), Some(Answer::Sum(1)));
    let retire = CacheCommand::Retire {
        producer: 7,
        floor: 4,
    };
    let decoded = CacheCommand::decode(&retire.encode()).unwrap();
    assert_eq!(decoded, retire);
    state.apply(2, 0, &decoded);
    assert_eq!(state.answer(7, 3), None);
    let mut restored = CacheState::restore(&state.snapshot(), true).unwrap();
    for member in [&mut state, &mut restored] {
        assert_eq!(member.apply(3, 0, &first), Some(Answer::Forgotten));
        assert_eq!(member.apply(4, 0, &first), Some(Answer::Forgotten));
        assert_eq!(member.sum("n"), Some(1));
    }
}

#[test]
fn bytes_this_build_cannot_read_are_refused() {
    let entry = put("k", "v", request(1, 1)).encode();
    let mut wrong_version = entry.to_vec();
    wrong_version[0] = 9;
    assert!(CacheCommand::decode(&wrong_version.into()).is_err());
    let mut wrong_kind = entry.to_vec();
    wrong_kind[1] = 9;
    assert!(CacheCommand::decode(&wrong_kind.into()).is_err());
    assert!(CacheCommand::decode(&entry.slice(..20)).is_err());
    let mut long = add("n", 1, request(1, 1)).encode().to_vec();
    long.push(0);
    assert!(CacheCommand::decode(&long.into()).is_err());

    let mut state = CacheState::default();
    state.apply(1, 3, &put("k", "v", request(1, 1)));
    let snapshot = state.snapshot();
    assert_eq!(
        CacheState::restore(&snapshot, true).unwrap().snapshot(),
        snapshot
    );
    assert!(CacheState::restore(&snapshot.slice(..snapshot.len() - 1), true).is_err());
    let mut long = snapshot.to_vec();
    long.push(0);
    assert!(CacheState::restore(&long.into(), true).is_err());
    assert_eq!(
        CacheState::restore(&Bytes::new(), false).unwrap(),
        CacheState::default()
    );
    // A snapshot as of an entry always has data: none is a bad one.
    assert!(CacheState::restore(&Bytes::new(), true).is_err());
}

/// A restored value does not hold on to the snapshot it came from.
#[test]
fn a_restored_value_is_its_own_copy() {
    let mut state = CacheState::default();
    state.apply(1, 3, &put("k", "v", request(1, 1)));
    let snapshot = state.snapshot();
    let restored = CacheState::restore(&snapshot, true).unwrap();
    let value = &restored.view().entry("k").unwrap().value;
    let within = snapshot.as_ptr_range();
    assert!(!within.contains(&value.as_ptr()));
}

/// ApplyLocalClock: apply, the snapshot and the record have no clock to read.
#[test]
fn apply_and_snapshot_read_no_clock() {
    for source in [
        include_str!("state.rs"),
        include_str!("snapshot.rs"),
        include_str!("record.rs"),
    ] {
        for clock in ["SystemTime", "Instant", "now("] {
            assert!(
                !source.contains(clock),
                "{clock} in the cache state machine"
            );
        }
    }
}

/// What reads of `state` find for the keys, producers and sequences the
/// tests here use, and the snapshot it would write.
fn seen(state: &CacheState) -> String {
    let mut seen = Vec::new();
    for key in ["a", "b", "c", "d"] {
        for clock in [0, 15, u64::MAX] {
            seen.push(format!("{key}@{clock}: {:?}", state.read(key, clock)));
        }
        seen.push(format!("{key} sum: {:?}", state.sum(key)));
    }
    for producer in 0..3 {
        for sequence in 0..90 {
            if let Some(answer) = state.answer(producer, sequence) {
                seen.push(format!("{producer}/{sequence}: {answer:?}"));
            }
        }
    }
    seen.push(format!("next expiry {:?}", state.next_expiry()));
    for clock in [0, 15, u64::MAX] {
        seen.push(format!("due@{clock}: {:?}", state.due(clock, 2)));
        seen.push(format!("all due@{clock}: {:?}", state.due(clock, 100)));
    }
    seen.push(format!("snapshot {:?}", state.snapshot()));
    seen.join("\n")
}

/// Applies `before`, cuts, then applies `during`. Throughout, the layered
/// state answers like a plain one that applied the same, the cut stays
/// the state as of its entry, and once the cut is let go the writes made
/// during it fold back without loss. Returns the folded state.
fn layered_like_plain(before: &[CacheCommand], during: &[CacheCommand]) -> CacheState {
    let (mut layered, mut plain) = (CacheState::default(), CacheState::default());
    let mut index = 0;
    for command in before {
        index += 1;
        layered.apply(index, 10, command);
        plain.apply(index, 10, command);
    }
    let cut = layered.cut();
    let at_cut = plain.snapshot();
    assert!(layered.layered());
    for command in during {
        index += 1;
        let answer = layered.apply(index, 10, command);
        assert_eq!(answer, plain.apply(index, 10, command), "{command:?}");
        assert_eq!(seen(&layered), seen(&plain), "after {command:?}");
        assert!(layered == plain);
    }
    assert_eq!(cut.snapshot(), at_cut, "the cut holds later writes");
    assert!(
        !layered.fold(usize::MAX),
        "folded while a cut holds the base"
    );
    assert!(layered.layered());
    drop(cut);
    while layered.fold(1) {
        assert_eq!(seen(&layered), seen(&plain), "part way through the fold");
    }
    assert!(!layered.layered(), "the fold did not finish");
    assert_eq!(seen(&layered), seen(&plain), "after the fold");
    layered
}

/// A put during a build is read back; the cut does not hold it.
#[test]
fn a_put_during_a_build_is_read_from_the_delta() {
    let state = layered_like_plain(
        &[put("a", "v", request(1, 1))],
        &[put("b", "w", request(1, 2))],
    );
    assert_eq!(value(&state, "b", 0), Some(("w".into(), 2)));
}

/// An overwrite during a build hides the base's value.
#[test]
fn an_overwrite_during_a_build_hides_the_base_value() {
    let state = layered_like_plain(
        &[put("a", "v", request(1, 1))],
        &[put("a", "w", request(1, 2))],
    );
    assert_eq!(value(&state, "a", 0), Some(("w".into(), 2)));
}

/// A delete during a build of a key the base holds leaves it absent.
#[test]
fn a_delete_during_a_build_hides_the_base_key() {
    let state = layered_like_plain(
        &[put_with("a", "v", 30, Condition::None, request(1, 1))],
        &[delete("a", None, request(1, 2))],
    );
    assert_eq!(state.read("a", 0), Read::Absent);
    assert_eq!(state.next_expiry(), None);
}

/// A key deleted and put again during a build holds the new value.
#[test]
fn a_delete_then_put_during_a_build_holds_the_new_value() {
    let state = layered_like_plain(
        &[put_with("a", "v", 30, Condition::None, request(1, 1))],
        &[
            delete("a", None, request(1, 2)),
            put_with("a", "w", 0, Condition::Absent, request(1, 3)),
            delete("a", None, request(1, 4)),
            put_with("a", "x", 0, Condition::Absent, request(1, 5)),
        ],
    );
    assert_eq!(value(&state, "a", u64::MAX), Some(("x".into(), 5)));
    assert_eq!(state.next_expiry(), None);
}

/// A floor raised during a build drops the base's answers below it, and a
/// retry from below it is forgotten, not applied again.
#[test]
fn a_floor_raised_during_a_build_drops_the_base_answers_below_it() {
    let before: Vec<_> = (1..=5)
        .map(|sequence| add("a", 1, request(1, sequence)))
        .collect();
    let state = layered_like_plain(
        &before,
        &[
            CacheCommand::Retire {
                producer: 1,
                floor: 4,
            },
            add("a", 1, request(1, 2)),
            add("a", 1, request(1, 6)),
        ],
    );
    assert_eq!(state.answer(1, 2), None);
    assert_eq!(state.answer(1, 4), Some(Answer::Sum(4)));
    assert_eq!(state.answer(1, 6), Some(Answer::Sum(6)));
    assert_eq!(state.sum("a"), Some(6));
}

/// Expiry walks the base and the delta as one order.
#[test]
fn expiry_during_a_build_walks_both_layers() {
    let state = layered_like_plain(
        &[
            put_with("a", "v", 10, Condition::None, request(1, 1)),
            put_with("c", "v", 30, Condition::None, request(1, 2)),
        ],
        &[
            put_with("b", "v", 20, Condition::None, request(1, 3)),
            delete("c", None, request(1, 4)),
            put_with("d", "v", 5, Condition::None, request(1, 5)),
        ],
    );
    let due: Vec<_> = state
        .due(u64::MAX, 10)
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(due, ["d", "a", "b"]);
    assert_eq!(state.next_expiry(), Some(5));
}

/// Expiry skips the base members a build removed from the front, and
/// still finds one that is put back with the same expiry.
#[test]
fn expiry_during_a_build_finds_a_front_member_put_back() {
    let before: Vec<_> = ["a", "b", "c", "d"]
        .iter()
        .enumerate()
        .map(|(at, key)| {
            put_with(
                key,
                "v",
                10 + at as u64,
                Condition::None,
                request(1, at as u64 + 1),
            )
        })
        .collect();
    let state = layered_like_plain(
        &before,
        &[
            delete("a", None, request(1, 5)),
            delete("b", None, request(1, 6)),
            delete("c", None, request(1, 7)),
            put_with("a", "w", 10, Condition::None, request(1, 8)),
            delete("d", None, request(1, 9)),
            put_with("c", "w", 12, Condition::None, request(1, 10)),
        ],
    );
    let due: Vec<_> = state
        .due(u64::MAX, 10)
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(due, ["a", "c"]);
}

/// A cut taken while an earlier one still holds the base shares the base
/// and copies the delta; one taken while a fold is part way finishes the
/// fold first. Each stays as of its own entry.
#[test]
fn a_second_cut_is_exact_while_the_first_is_held_or_folding() {
    let (mut layered, mut plain) = (CacheState::default(), CacheState::default());
    let apply = |layered: &mut CacheState, plain: &mut CacheState, index: u64| {
        let command = put(
            ["a", "b", "c", "d"][index as usize % 4],
            "v",
            request(1, index),
        );
        layered.apply(index, 10, &command);
        plain.apply(index, 10, &command);
    };
    for index in 1..=8 {
        apply(&mut layered, &mut plain, index);
    }
    let first = layered.cut();
    let at_first = plain.snapshot();
    for index in 9..=12 {
        apply(&mut layered, &mut plain, index);
    }
    let second = layered.cut();
    let at_second = plain.snapshot();
    for index in 13..=16 {
        apply(&mut layered, &mut plain, index);
    }
    assert_eq!(first.snapshot(), at_first);
    drop(first);
    assert!(
        !layered.fold(usize::MAX),
        "folded while the second cut holds the base"
    );
    assert_eq!(second.snapshot(), at_second);
    drop(second);
    assert!(layered.fold(1), "a fold of one should leave more");
    let third = layered.cut();
    let at_third = plain.snapshot();
    for index in 17..=20 {
        apply(&mut layered, &mut plain, index);
    }
    assert_eq!(third.snapshot(), at_third);
    drop(third);
    while layered.fold(1) {}
    assert!(!layered.layered());
    assert!(layered == plain);
    assert_eq!(seen(&layered), seen(&plain));
}

/// MembersAgree across snapshot builds: with cuts taken, held, let go and
/// folded back at random points, a member answers like one that never
/// built a snapshot, and each cut holds exactly the state at its entry.
#[test]
fn a_member_building_snapshots_answers_like_one_that_does_not() {
    for run in 0..300u64 {
        let mut seed = run;
        let (mut layered, mut plain) = (CacheState::default(), CacheState::default());
        let mut cuts = Vec::new();
        let mut stamp = 0;
        for index in 1..=80 {
            stamp += next(&mut seed) % 2;
            let command = random_command(&mut seed, index, stamp);
            let answer = layered.apply(index, stamp, &command);
            assert_eq!(
                answer,
                plain.apply(index, stamp, &command),
                "run {run} index {index}"
            );
            match next(&mut seed) % 6 {
                0 if cuts.len() < 2 => cuts.push((layered.cut(), plain.snapshot())),
                1 if !cuts.is_empty() => {
                    let (cut, at): (super::Cut, Bytes) = cuts.remove(0);
                    assert_eq!(cut.snapshot(), at, "run {run} index {index}");
                }
                2 => {
                    layered.fold(1 + next(&mut seed) as usize % 3);
                }
                _ => {}
            }
            assert_eq!(seen(&layered), seen(&plain), "run {run} index {index}");
        }
        for (cut, at) in cuts {
            assert_eq!(cut.snapshot(), at, "run {run}");
        }
        while layered.fold(2) {}
        assert!(!layered.layered(), "run {run}");
        assert!(layered == plain, "run {run}");
    }
}
