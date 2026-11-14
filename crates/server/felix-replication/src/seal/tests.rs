//! The seal leader's count: the start, the quiesce before the claim, the
//! majority and the one report.

use felix_storage::disk_log::switch::{RangeDigest, SideRange};

use super::*;

const SEAL: u64 = 5;

fn id(byte: u8) -> StorageId {
    StorageId::from_bytes([byte; 16])
}

fn starts(log: Offset) -> LogStarts {
    LogStarts {
        log,
        dead_letters: log / 2,
        cursors: 0,
    }
}

fn round() -> SealRound {
    SealRound::new(
        SEAL,
        ["a", "b", "c"].map(String::from),
        ("a".to_string(), id(1)),
        starts(0),
    )
}

fn base(start: LogStarts) -> SwitchBase {
    let side = |start| SideRange {
        start,
        end: start,
        digest: RangeDigest([0; 32]),
    };
    SwitchBase {
        generation: SEAL,
        offset: 10,
        start: start.log,
        digest: RangeDigest([1; 32]),
        dead_letters: side(start.dead_letters),
        cursors: side(start.cursors),
    }
}

#[test]
fn the_start_is_the_highest_among_the_copies_that_answered() {
    let mut round = round();
    round.answered("b", starts(4));
    round.answered("c", starts(2));
    // Not in the set.
    round.answered("d", starts(9));
    assert_eq!(round.starts(), starts(4));
    round.claimed(base(round.starts()));
    // Fixed once claimed.
    round.answered("c", starts(8));
    assert_eq!(round.starts(), starts(4));
}

#[test]
fn the_claim_waits_for_a_majority_and_the_quiesce() {
    let mut round = round();
    assert!(!round.may_claim(true), "the leader alone is no majority");
    round.answered("b", starts(0));
    assert!(!round.may_claim(false), "a write is still in flight");
    assert!(round.may_claim(true));
    round.claimed(base(round.starts()));
    assert!(!round.may_claim(true), "one claim per round");
}

#[test]
fn the_report_needs_a_majority_holding_n_and_is_given_once() {
    let mut round = round();
    round.answered("b", starts(0));
    // An acknowledgement before the claim counts for nothing.
    round.acked("b", id(2));
    round.claimed(base(round.starts()));
    assert_eq!(round.report(), None, "only the leader holds N");
    round.acked("d", id(4));
    assert_eq!(round.report(), None, "d is not in the set");
    round.acked("b", id(2));
    let report = round.report().expect("a majority holds N");
    assert_eq!(report.base, base(starts(0)));
    assert_eq!(
        report.voters,
        BTreeMap::from([("a".to_string(), id(1)), ("b".to_string(), id(2))])
    );
    round.acked("c", id(3));
    assert_eq!(round.report(), None, "reported once");
}

#[test]
fn an_abandoned_seal_reports_nothing() {
    let mut round = round();
    round.answered("b", starts(0));
    round.claimed(base(round.starts()));
    round.abandon();
    round.acked("b", id(2));
    round.acked("c", id(3));
    assert_eq!(round.report(), None);
    assert!(!round.may_claim(true));
}

#[test]
fn a_seal_record_starts_its_generation() {
    let record = |payload: Vec<u8>| felix_storage::log::LogRecord {
        offset: 10,
        timestamp_micros: 1,
        checksum: 0,
        payload: bytes::Bytes::from(payload),
        mark: felix_storage::log::RecordMark::GenerationStart,
        publisher: None,
        headers: None,
        generation: Some(SEAL),
    };
    let appended = base(starts(0)).record(1);
    assert!(crate::quorum::starts_generation(
        &record(appended.payload.to_vec()),
        10,
        SEAL
    ));
    assert!(crate::quorum::starts_generation(
        &record(SEAL.to_be_bytes().to_vec()),
        10,
        SEAL
    ));
    assert!(!crate::quorum::starts_generation(
        &record((SEAL + 1).to_be_bytes().to_vec()),
        10,
        SEAL
    ));
}
