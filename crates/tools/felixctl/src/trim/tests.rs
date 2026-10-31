use clap::Parser;

use super::*;
use crate::cli::{Cli, Command, StreamCommand};

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(
        ["felixctl", "stream", "trim"]
            .into_iter()
            .chain(args.iter().copied()),
    )
}

fn trim_args(args: &[&str]) -> StreamTrimArgs {
    match parse(args).expect("parse").command {
        Some(Command::Stream(StreamCommand::Trim(args))) => args,
        other => panic!("not stream trim: {other:?}"),
    }
}

#[test]
fn every_flag_but_the_headers_is_required() {
    let args = trim_args(&[
        "orders",
        "--shard",
        "2",
        "--before",
        "400",
        "--expected-offset",
        "410",
        "--headers",
        "h",
    ]);
    assert_eq!(
        (args.shard, args.before, args.expected_offset),
        (2, 400, 410)
    );
    assert_eq!(args.headers.as_deref(), Some("h"));
    for missing in [
        ["orders", "--before", "4", "--expected-offset", "5"],
        ["orders", "--shard", "0", "--expected-offset", "5"],
        ["orders", "--shard", "0", "--before", "4"],
    ] {
        assert!(parse(&missing).is_err());
    }
}

#[test]
fn the_outcome_says_where_the_trim_landed_or_that_it_was_a_no_op() {
    assert_eq!(
        trimmed_line("orders", 2, Some(410), 400),
        "trimmed orders shard 2: trim record at offset 410, reads now start at 400"
    );
    assert_eq!(
        trimmed_line("orders", 2, None, 400),
        "nothing to trim on orders shard 2: reads already start at 400"
    );
}

#[test]
fn a_stale_tail_names_the_tail() {
    let args = trim_args(&[
        "orders",
        "--shard",
        "1",
        "--before",
        "4",
        "--expected-offset",
        "5",
    ]);
    assert_eq!(
        refused_line(&args, 9),
        "orders shard 1 is at offset 9, not 5; nothing was written"
    );
}
