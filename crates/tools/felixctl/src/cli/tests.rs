use clap::{CommandFactory, Parser};

use super::*;

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    Cli::try_parse_from(std::iter::once("felixctl").chain(args.iter().copied()))
}

/// Every command, visible or not, with its full name.
fn all_commands() -> Vec<(String, clap::Command)> {
    fn walk(name: String, command: &clap::Command, out: &mut Vec<(String, clap::Command)>) {
        out.push((name.clone(), command.clone()));
        for sub in command.get_subcommands() {
            walk(format!("{name} {}", sub.get_name()), sub, out);
        }
    }
    let mut root = Cli::command();
    root.build();
    let mut out = Vec::new();
    walk("felixctl".to_string(), &root, &mut out);
    out
}

#[test]
fn the_command_definition_is_consistent() {
    Cli::command().debug_assert();
}

#[test]
fn every_command_has_help_text_and_examples() {
    for (name, mut command) in all_commands() {
        if name.ends_with(" help") || name.contains(" help ") {
            continue;
        }
        let about = command
            .get_about()
            .map(|s| s.to_string())
            .unwrap_or_default();
        assert!(!about.trim().is_empty(), "{name} has no about");
        let long = command
            .get_long_about()
            .map(|s| s.to_string())
            .unwrap_or_default();
        assert!(!long.trim().is_empty(), "{name} has no long_about");
        let help = command.render_long_help().to_string();
        assert!(
            help.contains("Usage:"),
            "{name}'s help did not render:\n{help}"
        );
        if !command.is_hide_set() {
            assert!(
                help.contains("Examples:\n  felixctl"),
                "{name}'s --help has no Examples section:\n{help}"
            );
        }
    }
}

#[test]
fn every_argument_has_help_text() {
    for (name, command) in all_commands() {
        for arg in command.get_arguments() {
            let id = arg.get_id().as_str();
            if id == "help" || id == "version" {
                continue;
            }
            let help = arg.get_help().map(|s| s.to_string()).unwrap_or_default();
            assert!(!help.trim().is_empty(), "{name} argument {id} has no help");
        }
    }
}

#[test]
fn defaults_are_shown_in_help() {
    let mut command = Cli::command();
    command.build();
    let mut bench = command
        .find_subcommand("bench")
        .and_then(|bench| bench.find_subcommand("ingest"))
        .expect("bench ingest")
        .clone();
    let help = bench.render_long_help().to_string();
    assert!(help.contains("[default: 100000]"), "{help}");
}

#[test]
fn no_command_parses_so_the_overview_can_be_printed() {
    let cli = parse(&[]).expect("parse");
    assert!(cli.command.is_none());
    assert!(OVERVIEW.contains("felixctl context add"));
}

#[test]
fn global_flags_work_after_the_command() {
    let cli = parse(&[
        "pub",
        "orders",
        "hello",
        "--brokers",
        "a:1,b:2",
        "--tenant",
        "t1",
        "-n",
        "ns",
        "--json",
    ])
    .expect("parse");
    assert!(cli.json);
    assert_eq!(
        cli.connection.brokers,
        Some(vec!["a:1".to_string(), "b:2".to_string()])
    );
    assert_eq!(cli.connection.tenant.as_deref(), Some("t1"));
    assert_eq!(cli.connection.namespace.as_deref(), Some("ns"));
}

#[test]
fn every_command_takes_every_global_flag() {
    // A command argument sharing a global flag's id replaces it, and the
    // flag is then refused on that command.
    let root = Cli::command();
    let globals: Vec<&str> = root
        .get_arguments()
        .filter(|arg| arg.is_global_set())
        .filter_map(|arg| arg.get_long())
        .collect();
    let mut missing = Vec::new();
    for (name, command) in all_commands() {
        // clap's generated `help` subcommands take no flags at all.
        if name.split(' ').any(|word| word == "help") {
            continue;
        }
        for long in &globals {
            if !command
                .get_arguments()
                .any(|arg| arg.get_long() == Some(long))
            {
                missing.push(format!("{name} --{long}"));
            }
        }
    }
    assert!(missing.is_empty(), "refused: {missing:?}");
}

#[test]
fn connection_flags_parse_on_commands_that_ignore_them() {
    for args in [
        &["node", "ls", "--tenant", "t", "-n", "ns", "--token", "x"][..],
        &["shard", "ls", "--tenant", "t"],
        &["tenant", "info", "t1", "--tenant", "t1"],
        &["namespace", "info", "ns", "--namespace", "ns"],
    ] {
        parse(args).unwrap_or_else(|err| panic!("{args:?}: {err}"));
    }
}

#[test]
fn pub_flags_parse() {
    let cli = parse(&[
        "pub", "orders", "x", "--key", "k", "--count", "3", "--ack", "none",
    ])
    .expect("parse");
    let Some(Command::Pub(args)) = cli.command else {
        panic!("not pub");
    };
    assert_eq!(args.stream, "orders");
    assert_eq!(args.data.as_deref(), Some("x"));
    assert_eq!(args.key.as_deref(), Some("k"));
    assert_eq!(args.count, 3);
    assert_eq!(args.ack, AckArg::None);

    let cli = parse(&["pub", "orders"]).expect("parse");
    let Some(Command::Pub(args)) = cli.command else {
        panic!("not pub");
    };
    assert_eq!(args.count, 1);
    assert_eq!(args.ack, AckArg::Message);
}

#[test]
fn pub_refuses_contradictions() {
    assert!(parse(&["pub", "s", "x", "--file", "f"]).is_err());
    assert!(parse(&["pub", "s", "--file", "f", "--whole"]).is_err());
}

#[test]
fn an_idempotent_publish_may_be_keyed() {
    let Some(Command::Pub(args)) = parse(&["pub", "s", "--idempotent", "--key", "k", "x"])
        .expect("parse")
        .command
    else {
        panic!("not pub");
    };
    assert!(args.idempotent);
    assert_eq!(args.key.as_deref(), Some("k"));
}

#[test]
fn sub_from_takes_latest_earliest_or_an_offset() {
    let from = |value: &str| {
        let cli = parse(&["sub", "s", "--from", value])?;
        let Some(Command::Sub(args)) = cli.command else {
            panic!("not sub");
        };
        Ok::<_, clap::Error>(args.from)
    };
    assert_eq!(from("latest").unwrap(), StartArg::Latest);
    assert_eq!(from("earliest").unwrap(), StartArg::Earliest);
    assert_eq!(from("42").unwrap(), StartArg::Offset(42));
    assert!(from("yesterday").is_err());

    let cli = parse(&["sub", "s"]).expect("parse");
    let Some(Command::Sub(args)) = cli.command else {
        panic!("not sub");
    };
    assert_eq!(args.from, StartArg::Latest);
    assert_eq!(args.format, FormatArg::Raw);
}

#[test]
fn cache_watch_from_needs_a_key() {
    assert!(parse(&["cache", "watch", "c", "--from", "5"]).is_err());
    assert!(parse(&["cache", "watch", "c", "--key", "k", "--from", "5"]).is_ok());
    assert!(parse(&["cache", "watch", "c", "--key", "k", "--prefix", "p"]).is_err());
}

#[test]
fn bench_defaults_are_small() {
    let cli = parse(&["bench", "latency", "orders"]).expect("parse");
    let Some(Command::Bench(BenchCommand::Latency(args))) = cli.command else {
        panic!("not bench latency");
    };
    assert_eq!(args.size.total, 5000);
    assert_eq!(args.size.warmup, 500);
    assert_eq!(args.size.payload_bytes, 256);
}

#[test]
fn completions_take_a_shell() {
    assert!(parse(&["completions", "zsh"]).is_ok());
    assert!(parse(&["completions", "cmd.exe"]).is_err());
}
