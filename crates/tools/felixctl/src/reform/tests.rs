use clap::Parser;
use serde_json::json;

use super::*;
use crate::cli::{Cli, Command};

fn args(argv: &[&str]) -> anyhow::Result<Value> {
    let cli = Cli::try_parse_from(
        ["felixctl", "raft", "reform", "plan"]
            .into_iter()
            .chain(argv.iter().copied()),
    )
    .unwrap_or_else(|err| panic!("{argv:?}: {err}"));
    let Some(Command::Raft(RaftCommand::Reform(ReformCommand::Plan(args)))) = cli.command else {
        panic!("not raft reform plan");
    };
    plan_body(&args)
}

#[test]
fn the_scope_is_the_cluster_or_one_node() {
    assert_eq!(args(&[]).expect("body"), json!({ "scope": "cluster" }));
    assert_eq!(
        args(&["--scope", "cluster"]).expect("body"),
        json!({ "scope": "cluster" })
    );
    assert_eq!(
        args(&["--scope", "node", "broker-2"]).expect("body"),
        json!({ "scope": { "node": "broker-2" } })
    );
    for bad in [&["--scope", "node"][..], &["--scope", "cluster", "x"]] {
        assert!(args(bad).is_err(), "{bad:?}");
    }
}

fn shard(stream: &str, state: &str) -> Value {
    json!({
        "key": {"tenant_id": "t1", "namespace": "ns", "stream": stream, "shard": 0, "kind": "stream"},
        "incarnation": 1, "epoch": 0, "state": state, "round": 2,
    })
}

#[test]
fn a_plan_prints_one_line_per_shard_and_flags_what_could_be_lost() {
    let mut safe = shard("orders", "reform");
    safe["plan"] = json!({
        "epoch": 1, "source_broker": "broker-1", "source_kept": null,
        "base_last": [9, 0, 3], "first_index": 10,
        "voters": [["broker-1", 7], ["broker-3", 0]], "learners": [],
        "marked": null, "cut_by_vote": false, "needs_accept_loss": false,
        "dropped": [["broker-1", 10, 12]],
    });
    let mut lossy = shard("payments", "reform");
    lossy["plan"] = json!({
        "epoch": 1, "source_broker": "broker-1", "source_kept": null,
        "base_last": [4, 0, 2], "first_index": 5,
        "voters": [["broker-1", 7]], "learners": [],
        "marked": "the votes of broker-2, broker-3 are not known, a majority of the epoch's first voters (broker-1, broker-2, broker-3)",
        "cut_by_vote": false, "needs_accept_loss": true, "dropped": [],
    });
    let mut blocked = shard("audit", "blocked");
    blocked["reason"] = json!("broker broker-2 did not answer");
    let mut led = shard("logs", "left_alone");
    led["reason"] = json!("broker broker-1 leads the current epoch");
    let plan = json!({
        "plan_id": "p1", "scope": {"node": "broker-2"}, "state": "done",
        "shards": [safe, lossy, blocked, led],
    });
    let text = render_plan(&plan);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines[0],
        "plan p1 (node broker-2): done; 2 to re-form, 1 blocked, 1 left alone"
    );
    assert!(lines[1].starts_with("SHARD"), "{text}");
    let orders = lines.iter().find(|l| l.contains("orders")).expect(&text);
    assert!(orders.contains("broker-1, broker-3"), "{orders}");
    assert!(
        orders.contains(
            "keeps every acknowledged record; drops broker-1's uncommitted entries 10-12"
        ),
        "{orders}"
    );
    let payments = lines.iter().find(|l| l.contains("payments")).expect(&text);
    assert!(
        payments.contains("MAY LOSE ACKNOWLEDGED RECORDS: the votes of broker-2, broker-3"),
        "{payments}"
    );
    assert!(
        lines
            .iter()
            .any(|l| l.contains("audit") && l.contains("did not answer"))
    );
    assert_eq!(
        lines.last().copied(),
        Some("1 of these could lose acknowledged records; applying them will need --accept-loss")
    );
}

/// A cut with no mark still loses records, and the line says so.
#[test]
fn a_plan_cut_by_a_vote_is_flagged_too() {
    let mut cut = shard("orders", "reform");
    cut["plan"] = json!({
        "epoch": 1, "source_broker": "broker-1", "voters": [["broker-1", 7]],
        "marked": null, "cut_by_vote": true, "needs_accept_loss": true,
        "dropped": [["broker-2", 5, 8]],
    });
    let text = render_plan(&json!({
        "plan_id": "p1", "scope": "cluster", "state": "done", "shards": [cut],
    }));
    assert!(text.contains("MAY LOSE ACKNOWLEDGED RECORDS"), "{text}");
    assert!(text.contains("drops broker-2's entries 5-8"), "{text}");
    assert!(!text.contains("uncommitted"), "{text}");
}

#[test]
fn a_surveying_plan_says_who_it_waits_on() {
    let mut waiting = shard("orders", "surveying");
    waiting["waiting_on"] = json!(["broker-2", "broker-3"]);
    let text = render_plan(&json!({
        "plan_id": "p1", "scope": "cluster", "state": "surveying", "shards": [waiting],
    }));
    assert!(
        text.starts_with("plan p1 (cluster): still surveying;"),
        "{text}"
    );
    assert!(text.contains("waiting on broker-2, broker-3"), "{text}");
    let empty = render_plan(&json!({
        "plan_id": "p2", "scope": "cluster", "state": "done", "shards": [],
    }));
    assert!(empty.ends_with("no Raft shard in scope"), "{empty}");
}

fn apply_args(argv: &[&str]) -> anyhow::Result<Value> {
    let cli = Cli::try_parse_from(
        ["felixctl", "raft", "reform", "apply"]
            .into_iter()
            .chain(argv.iter().copied()),
    )
    .unwrap_or_else(|err| panic!("{argv:?}: {err}"));
    let Some(Command::Raft(RaftCommand::Reform(ReformCommand::Apply(args)))) = cli.command else {
        panic!("not raft reform apply");
    };
    apply_body(&args)
}

/// `--accept-loss` names shards the way the plan prints them, and a name
/// that is not one is a usage error, never a shard nobody meant.
#[test]
fn accept_loss_names_stream_and_cache_shards() {
    assert_eq!(
        apply_args(&["p1"]).expect("body"),
        json!({ "accept_loss": [] })
    );
    assert_eq!(
        apply_args(&[
            "p1",
            "--accept-loss",
            "t1/ns/orders/2",
            "--accept-loss",
            "cache:t1/ns/sessions/0",
        ])
        .expect("body"),
        json!({ "accept_loss": [
            {"tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 2, "kind": "stream"},
            {"tenant_id": "t1", "namespace": "ns", "stream": "sessions", "shard": 0, "kind": "cache"},
        ]})
    );
    for bad in [
        "t1/ns/orders",
        "t1/ns/orders/x",
        "t1//orders/0",
        "t1/ns/orders/0/1",
    ] {
        assert!(apply_args(&["p1", "--accept-loss", bad]).is_err(), "{bad}");
    }
}

#[test]
fn an_apply_prints_each_applied_shard_with_its_state() {
    let mut started = shard("orders", "reform");
    started["apply"] = json!({"state": "started", "epoch": 1});
    let mut skipped = shard("refunds", "reform");
    skipped["apply"] = json!({"state": "skipped", "reason": "name the shard with --accept-loss"});
    let mut stopping = shard("audit", "reform");
    stopping["apply"] = json!({"state": "stopping", "waiting_on": ["b"]});
    let plan = json!({
        "plan_id": "p1", "state": "applying",
        "shards": [started, skipped, stopping, shard("left", "left_alone")],
    });
    let text = render_apply(&plan);
    assert!(text.starts_with("plan p1: 3 shards applied"), "{text}");
    assert!(
        text.contains("t1/ns/orders/0") && text.contains("started"),
        "{text}"
    );
    assert!(text.contains("--accept-loss"), "{text}");
    assert!(text.contains("waiting on b"), "{text}");
    assert!(!text.contains("t1/ns/left/0"), "{text}");
    assert!(applying(&plan));
}
