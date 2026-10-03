use clap::Parser;

use super::*;
use crate::cli::{Cli, Command};

fn pub_args(args: &[&str]) -> PubArgs {
    let cli = Cli::try_parse_from(["felixctl", "pub"].into_iter().chain(args.iter().copied()))
        .expect("parse");
    match cli.command {
        Some(Command::Pub(args)) => args,
        other => panic!("not pub: {other:?}"),
    }
}

#[test]
fn the_message_source_follows_the_flags() {
    assert_eq!(
        Source::of(&pub_args(&["s", "hello"])),
        Source::Literal(b"hello".to_vec())
    );
    assert_eq!(
        Source::of(&pub_args(&["s", "--file", "msg.bin"])),
        Source::File("msg.bin".into())
    );
    assert_eq!(Source::of(&pub_args(&["s", "--whole"])), Source::StdinWhole);
    assert_eq!(Source::of(&pub_args(&["s"])), Source::StdinLines);
}

#[test]
fn an_acked_publish_without_an_offset_says_why() {
    assert_eq!(
        summary("orders", &[None], false),
        "published 1 to orders; 1 without an offset \
         (acknowledged before the write, or the stream has no log)"
    );
    assert_eq!(
        summary("orders", &[Some(5), None, Some(7)], false),
        "published 3 to orders, offsets 5..=7; 1 without an offset \
         (acknowledged before the write, or the stream has no log)"
    );
    assert_eq!(
        summary("orders", &[Some(4)], false),
        "published 1 to orders at offset 4"
    );
}

#[test]
fn an_unacked_publish_has_no_offsets_to_explain() {
    assert_eq!(
        summary("orders", &[None, None], true),
        "published 2 to orders"
    );
}
