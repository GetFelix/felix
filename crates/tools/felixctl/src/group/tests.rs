use clap::Parser;

use super::*;
use crate::cli::{Cli, Command};
use crate::counter::CounterCommand;
use crate::error::exit_for;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("felixctl").chain(args.iter().copied()))
}

fn group(args: &[&str]) -> GroupCommand {
    match parse(args).expect("parses").command {
        Some(Command::Group(command)) => command,
        other => panic!("not a group command: {other:?}"),
    }
}

fn info(committed: Option<u64>) -> GroupInfo {
    GroupInfo {
        committed,
        tail: 20,
        in_flight: 2,
        owed: 1,
        dead_letters: 3,
    }
}

fn target() -> Target {
    Target {
        stream: "orders".into(),
        group: "billing".into(),
        shard: None,
    }
}

#[test]
fn a_claim_is_shard_offset_and_maybe_attempts() {
    let claim: Claim = "2:15:3".parse().unwrap();
    assert_eq!(
        claim,
        Claim {
            shard: 2,
            offset: 15,
            attempts: Some(3)
        }
    );
    assert_eq!(claim.to_string(), "2:15:3");
    let claim: Claim = "0:7".parse().unwrap();
    assert_eq!(claim.attempts, None);
    assert_eq!(claim.to_string(), "0:7");
    for bad in ["7", "", "a:1", "0:x", "0:1:2:3", "0:1:", "-1:4"] {
        assert!(bad.parse::<Claim>().is_err(), "{bad:?} parsed");
    }
}

#[test]
fn poll_defaults_and_bounds() {
    let GroupCommand::Poll {
        target,
        max,
        wait_ms,
        visibility_ms,
    } = group(&["group", "poll", "orders", "billing"])
    else {
        panic!("not poll");
    };
    assert_eq!(
        (target.shard, max, wait_ms, visibility_ms),
        (None, 10, 0, None)
    );
    assert!(parse(&["group", "poll", "orders", "billing", "--max", "0"]).is_err());
}

#[test]
fn settling_needs_at_least_one_claim_and_checks_its_shape() {
    assert!(parse(&["group", "ack", "orders", "billing"]).is_err());
    assert!(parse(&["group", "ack", "orders", "billing", "15"]).is_err());
    let GroupCommand::Nack { claims, delay_ms } = group(&[
        "group",
        "nack",
        "orders",
        "billing",
        "0:15:1",
        "1:4",
        "--delay-ms",
        "500",
    ]) else {
        panic!("not nack");
    };
    assert_eq!(claims.claims.len(), 2);
    assert_eq!(delay_ms, Some(500));
}

#[test]
fn destructive_commands_take_yes() {
    let GroupCommand::Rm { confirm, .. } = group(&["group", "rm", "orders", "billing", "-y"])
    else {
        panic!("not rm");
    };
    assert!(confirm.yes);
    let GroupCommand::Seek { to, confirm, .. } =
        group(&["group", "seek", "orders", "billing", "earliest"])
    else {
        panic!("not seek");
    };
    assert_eq!(to, StartArg::Earliest);
    assert!(!confirm.yes);
    let GroupCommand::DeadLetters(DeadLetterCommand::Discard { confirm, .. }) = group(&[
        "group",
        "dead-letters",
        "discard",
        "orders",
        "billing",
        "0:3",
        "--yes",
    ]) else {
        panic!("not discard");
    };
    assert!(confirm.yes);
}

#[test]
fn a_counter_delta_may_be_negative() {
    let Some(Command::Counter(CounterCommand::Add { delta, .. })) =
        parse(&["counter", "add", "stats", "stock", "-3"])
            .expect("parses")
            .command
    else {
        panic!("not counter add");
    };
    assert_eq!(delta, -3);
}

#[test]
fn only_a_seek_behind_the_cursor_asks() {
    let finished = [(0, info(Some(10))), (1, info(None))];
    assert!(seeks_backwards(StartPosition::Earliest, &finished));
    assert!(seeks_backwards(StartPosition::Offset(9), &finished));
    assert!(!seeks_backwards(StartPosition::Offset(10), &finished));
    assert!(!seeks_backwards(StartPosition::Latest, &finished));
    // Nothing finished yet: nothing to replay.
    let fresh = [(0, info(None)), (1, info(Some(0)))];
    assert!(!seeks_backwards(StartPosition::Earliest, &fresh));
}

#[test]
fn an_extension_needs_the_attempt_count() {
    let err = claimed_record(&"0:15".parse().unwrap()).unwrap_err();
    assert_eq!(exit_for(&err), Exit::Usage);
    let record = claimed_record(&"0:15:2".parse().unwrap()).unwrap();
    assert_eq!((record.offset, record.attempts), (15, 2));
}

#[test]
fn a_polled_record_prints_its_claim() {
    let record = GroupRecord {
        offset: 15,
        payload: bytes::Bytes::from_static(b"hello"),
        attempts: 2,
        skipped_before: 0,
        publisher: None,
        timestamp_micros: Some(1_700_000),
    };
    assert_eq!(record_line(false, &target(), 1, &record), b"1:15:2\thello");
    let line = record_line(true, &target(), 1, &record);
    let value: Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(
        value,
        json!({
            "stream": "orders", "group": "billing", "shard": 1, "offset": 15,
            "attempts": 2, "claim": "1:15:2", "payload": "hello",
            "timestamp_micros": 1_700_000,
        })
    );
}

#[test]
fn describe_shows_lag_and_an_unset_cursor() {
    let text = describe_table(&[(0, info(Some(12))), (1, info(None))]);
    let lines: Vec<&str> = text.lines().collect();
    assert!(lines[0].starts_with("SHARD"), "{text}");
    assert!(
        lines[1]
            .split_whitespace()
            .eq(["0", "12", "20", "8", "2", "1", "3"])
    );
    assert!(
        lines[2]
            .split_whitespace()
            .eq(["1", "-", "20", "20", "2", "1", "3"])
    );
    assert_eq!(info_json(1, &info(None))["committed"], Value::Null);
}

#[test]
fn settle_names_its_json_field() {
    assert_eq!(Settle::Ack.verb(), "acked");
    assert_eq!(Settle::NackAfter(Duration::from_secs(1)).verb(), "nacked");
    assert_eq!(Settle::DeadLetter.verb(), "dead_lettered");
}
