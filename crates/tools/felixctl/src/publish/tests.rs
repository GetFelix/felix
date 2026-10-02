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
