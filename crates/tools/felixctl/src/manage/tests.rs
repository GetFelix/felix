use clap::Parser;
use serde_json::json;

use super::*;
use crate::cli::{
    CacheCommand, Cli, Command, ConnectionFlags, NodeCommand, ShardCommand, StreamCommand,
    TenantCommand,
};
use crate::context::{ConfigFile, resolve};
use crate::error::exit_for;

fn parse(args: &[&str]) -> Command {
    Cli::try_parse_from(std::iter::once("felixctl").chain(args.iter().copied()))
        .unwrap_or_else(|err| panic!("{args:?}: {err}"))
        .command
        .expect("a command")
}

fn refused(args: &[&str]) -> bool {
    Cli::try_parse_from(std::iter::once("felixctl").chain(args.iter().copied())).is_err()
}

fn settings() -> Settings {
    let flags = ConnectionFlags {
        tenant: Some("t1".into()),
        namespace: Some("ns".into()),
        ..ConnectionFlags::default()
    };
    resolve(&flags, &|_| None, &ConfigFile::default()).expect("resolve")
}

fn stream_create(args: &[&str]) -> Value {
    let mut all = vec!["stream", "create"];
    all.extend_from_slice(args);
    let Command::Stream(StreamCommand::Create(args)) = parse(&all) else {
        panic!("not stream create");
    };
    stream_body(&args)
}

fn stream_set(args: &[&str], current: &Value) -> Value {
    let mut all = vec!["stream", "set"];
    all.extend_from_slice(args);
    let Command::Stream(StreamCommand::Set(args)) = parse(&all) else {
        panic!("not stream set");
    };
    stream_patch(&args, current)
}

fn shard_move_args(args: &[&str]) -> ShardMoveArgs {
    let mut all = vec!["shard", "move"];
    all.extend_from_slice(args);
    let Command::Shard(ShardCommand::Move(args)) = parse(&all) else {
        panic!("not shard move");
    };
    args
}

#[test]
fn a_stream_with_no_flags_gets_the_documented_defaults() {
    assert_eq!(
        stream_create(&["orders"]),
        json!({
            "stream": "orders",
            "kind": "Stream",
            "shards": 1,
            "replication_factor": 1,
            "retention": {"max_age_seconds": null, "max_size_bytes": null},
            "consistency": "Leader",
            "delivery": "AtLeastOnce",
            "durable": true,
        })
    );
}

#[test]
fn stream_create_flags_map_onto_the_request() {
    let body = stream_create(&[
        "orders",
        "--shards",
        "4",
        "--replication",
        "3",
        "--kind",
        "queue",
        "--consistency",
        "quorum",
        "--delivery",
        "at-most-once",
        "--durable",
        "false",
        "--retention-secs",
        "3600",
        "--retention-bytes",
        "1048576",
        "--region",
        "eu-west",
        "--routing",
        "jump-hash",
    ]);
    assert_eq!(
        body,
        json!({
            "stream": "orders",
            "kind": "Queue",
            "shards": 4,
            "replication_factor": 3,
            "retention": {"max_age_seconds": 3600, "max_size_bytes": 1048576},
            "consistency": "Quorum",
            "delivery": "AtMostOnce",
            "durable": false,
            "region": "eu-west",
            "routing": "jump_hash",
        })
    );
}

#[test]
fn stream_set_needs_something_to_change() {
    assert!(refused(&["stream", "set", "orders"]));
    assert!(refused(&[
        "stream",
        "set",
        "orders",
        "--retention-secs",
        "soon"
    ]));
}

#[test]
fn stream_set_sends_only_what_was_given() {
    let current = json!({"retention": {"max_age_seconds": 60, "max_size_bytes": 100}});
    assert_eq!(
        stream_set(&["orders", "--consistency", "quorum"], &current),
        json!({"consistency": "Quorum"})
    );
    assert_eq!(
        stream_set(
            &[
                "orders",
                "--durable",
                "false",
                "--delivery",
                "at-least-once"
            ],
            &current
        ),
        json!({"durable": false, "delivery": "AtLeastOnce"})
    );
}

#[test]
fn stream_set_keeps_the_retention_bound_not_given() {
    let current = json!({"retention": {"max_age_seconds": 60, "max_size_bytes": 100}});
    assert_eq!(
        stream_set(&["orders", "--retention-secs", "86400"], &current),
        json!({"retention": {"max_age_seconds": 86400, "max_size_bytes": 100}})
    );
    assert_eq!(
        stream_set(&["orders", "--retention-bytes", "default"], &current),
        json!({"retention": {"max_age_seconds": 60, "max_size_bytes": null}})
    );
    // A stream with no retention object yet.
    assert_eq!(
        stream_set(&["orders", "--retention-secs", "5"], &json!({})),
        json!({"retention": {"max_age_seconds": 5, "max_size_bytes": null}})
    );
}

#[test]
fn cache_create_defaults_the_display_name_to_the_cache() {
    let Command::Cache(CacheCommand::Create(args)) = parse(&["cache", "create", "sessions"]) else {
        panic!("not cache create");
    };
    assert_eq!(
        cache_body(&args),
        json!({
            "cache": "sessions",
            "display_name": "sessions",
            "shards": 1,
            "replication_factor": 1,
            "consistency": "Leader",
        })
    );
    let Command::Cache(CacheCommand::Create(args)) = parse(&[
        "cache",
        "create",
        "sessions",
        "--shards",
        "4",
        "--replication",
        "3",
        "--consistency",
        "quorum",
        "--display-name",
        "Sessions",
    ]) else {
        panic!("not cache create");
    };
    let body = cache_body(&args);
    assert_eq!(body["display_name"], "Sessions");
    assert_eq!(body["shards"], 4);
    assert_eq!(body["replication_factor"], 3);
    assert_eq!(body["consistency"], "Quorum");
}

#[test]
fn cache_set_needs_a_display_name() {
    assert!(refused(&["cache", "set", "sessions"]));
    assert!(matches!(
        parse(&["cache", "set", "sessions", "--display-name", "x"]),
        Command::Cache(CacheCommand::Set { .. })
    ));
}

#[test]
fn tenant_and_namespace_names_default_their_display_name() {
    assert_eq!(
        named_body("tenant_id", "acme", None),
        json!({"tenant_id": "acme", "display_name": "acme"})
    );
    assert_eq!(
        named_body("namespace", "payments", Some("Payments")),
        json!({"namespace": "payments", "display_name": "Payments"})
    );
    // The positional has its own id, so the global --tenant still works.
    let Command::Tenant(TenantCommand::Create {
        tenant,
        display_name,
    }) = parse(&["tenant", "create", "acme", "--tenant", "t1"])
    else {
        panic!("not tenant create");
    };
    assert_eq!(tenant, "acme");
    assert_eq!(display_name, None);
}

#[test]
fn destructive_commands_take_yes() {
    for args in [
        &["tenant", "rm", "acme", "--yes"][..],
        &["namespace", "rm", "payments", "-y"],
        &["stream", "rm", "orders", "--yes"],
        &["cache", "rm", "sessions", "--yes"],
        &["node", "drain", "broker-2", "--yes"],
        &["node", "deregister", "broker-2", "--yes"],
        &["placement", "abandon", "orders", "2", "--yes"],
    ] {
        parse(args);
    }
    let Command::Node(NodeCommand::Drain { confirm, .. }) = parse(&["node", "drain", "b"]) else {
        panic!("not node drain");
    };
    assert!(!confirm.yes);
}

#[test]
fn a_shard_move_names_the_shard_and_destination() {
    let args = shard_move_args(&["orders", "2", "--to", "broker-3"]);
    assert!(args.cancel.is_none());
    assert_eq!(
        move_body("t1", "ns", &args).expect("body"),
        json!({
            "tenant_id": "t1",
            "namespace": "ns",
            "stream": "orders",
            "shard": 2,
            "kind": "stream",
            "destination": "broker-3",
            "dry_run": false,
        })
    );
    let args = shard_move_args(&["sessions", "0", "--to", "b", "--cache", "--dry-run"]);
    let body = move_body("t1", "ns", &args).expect("body");
    assert_eq!(body["kind"], "cache");
    assert_eq!(body["dry_run"], true);
}

#[test]
fn a_shard_move_without_a_destination_is_refused() {
    assert!(refused(&["shard", "move", "orders", "2"]));
    assert!(refused(&["shard", "move", "orders"]));
    assert!(refused(&["shard", "move", "orders", "two", "--to", "b"]));
}

#[test]
fn shard_move_cancel_is_a_subcommand() {
    let args = shard_move_args(&["cancel", "orders", "2"]);
    let Some(ShardMoveCommand::Cancel(shard)) = &args.cancel else {
        panic!("not cancel");
    };
    assert_eq!(
        (shard.name.as_str(), shard.shard, shard.cache),
        ("orders", 2, false)
    );
    let (path, query) = shard_path("/v1/shard-moves", &settings(), shard).expect("path");
    assert_eq!(path, "/v1/shard-moves/t1/ns/orders/2");
    assert_eq!(query, [("kind", "stream")]);

    // Cancel takes no --to.
    assert!(refused(&[
        "shard", "move", "cancel", "orders", "2", "--to", "b"
    ]));
}

#[test]
fn shard_paths_encode_names_and_carry_the_kind() {
    let Command::Placement(PlacementCommand::Abandon { shard, .. }) =
        parse(&["placement", "abandon", "my cache", "1", "--cache", "--yes"])
    else {
        panic!("not abandon");
    };
    let (path, query) = shard_path("/v1/placement/abandon", &settings(), &shard).expect("path");
    assert_eq!(path, "/v1/placement/abandon/t1/ns/my%20cache/1");
    assert_eq!(query, [("kind", "cache")]);
}

#[test]
fn changes_list_each_field_that_differs() {
    let before = json!({"stream": "orders", "durable": true, "retention": {"max_age_seconds": 60}});
    let after =
        json!({"stream": "orders", "durable": false, "retention": {"max_age_seconds": 120}});
    assert_eq!(
        changes(&before, &after),
        [
            "durable: true -> false",
            r#"retention: {"max_age_seconds":60} -> {"max_age_seconds":120}"#,
        ]
    );
    assert!(changes(&before, &before).is_empty());
}

#[test]
fn yes_skips_the_prompt() {
    let yes = Confirm { yes: true };
    confirm_with("Delete?", yes, false, || panic!("asked")).expect("confirmed");
    confirm_with("Delete?", yes, true, || panic!("asked")).expect("confirmed");
}

#[test]
fn without_a_terminal_or_yes_nothing_happens() {
    let err = confirm_with("Delete?", Confirm { yes: false }, false, || {
        panic!("asked without a terminal")
    })
    .unwrap_err();
    assert_eq!(exit_for(&err), Exit::Usage);
    assert!(format!("{err:#}").contains("--yes"), "{err:#}");
}

#[test]
fn only_yes_at_the_prompt_confirms() {
    let no = Confirm { yes: false };
    for answer in ["y\n", "YES\n", " yes "] {
        confirm_with("Delete?", no, true, || Ok(answer.to_string()))
            .unwrap_or_else(|err| panic!("{answer:?}: {err:#}"));
    }
    for answer in ["\n", "n\n", "yep\n", ""] {
        let err = confirm_with("Delete?", no, true, || Ok(answer.to_string())).unwrap_err();
        assert_eq!(exit_for(&err), Exit::Failure, "{answer:?}");
    }
}
