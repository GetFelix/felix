//! The parts of a backup point that do not need a cluster: finding a broker,
//! matching its answer to the assignments, and noticing leadership move.

use super::*;

fn assignment(stream: &str, shard: u32, leader: &str, generation: u64) -> Assignment {
    Assignment {
        tenant_id: "t".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind: "stream".to_string(),
        leader: leader.to_string(),
        generation,
    }
}

fn answer(body: serde_json::Value) -> BrokerOffsets {
    serde_json::from_value(body).expect("broker answer")
}

fn node(id: &str, client: Option<&str>, internal: &str) -> NodeAddress {
    NodeAddress {
        node_id: id.to_string(),
        client_addr: client.map(str::to_string),
        advertise_addr: internal.to_string(),
    }
}

#[test]
fn a_broker_is_reached_on_its_client_host_at_the_metrics_port() {
    let nodes = vec![
        node("a", Some("10.0.0.1:5000"), "10.1.0.1:6000"),
        node("b", None, "broker-b.internal:6000"),
        node("c", Some("[fd00::3]:5000"), "[fd00::3]:6000"),
    ];
    let overrides = HashMap::from([("d".to_string(), "http://proxy:9000".to_string())]);
    let url = |id| broker_url(id, &nodes, &overrides, 8080);

    assert_eq!(url("a").expect("a"), "http://10.0.0.1:8080");
    // No client address: the internal listener's host is the machine.
    assert_eq!(url("b").expect("b"), "http://broker-b.internal:8080");
    assert_eq!(url("c").expect("c"), "http://[fd00::3]:8080");
    assert_eq!(url("d").expect("d"), "http://proxy:9000");
    assert!(url("e").is_err(), "an unknown leader has no address");
}

#[test]
fn every_assigned_shard_at_its_generation_makes_a_point() {
    let assigned = [
        assignment("orders", 0, "a", 3),
        assignment("orders", 1, "a", 2),
    ];
    let refs: Vec<&Assignment> = assigned.iter().collect();
    let offsets = answer(serde_json::json!({
        "node_id": "a",
        "shards": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0, "kind": "stream",
              "generation": 3, "logs": { "records": 41, "group_cursors": 2, "group_dead_letters": 0 } },
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 1, "kind": "stream",
              "generation": 2, "logs": { "records": 7 } }
        ],
        "skipped": []
    }));

    let shards = match_leader("a", &refs, &offsets).expect("complete");
    assert_eq!(shards.len(), 2);
    assert_eq!(shards[0].logs.records, 41);
    assert_eq!(shards[0].logs.group_cursors, Some(2));
    assert_eq!(shards[1].leader, "a");
    assert_eq!(shards[1].generation, 2);
}

#[test]
fn a_shard_at_another_generation_or_without_an_answer_is_missing() {
    let assigned = [
        assignment("orders", 0, "a", 4),
        assignment("orders", 1, "a", 2),
        assignment("orders", 2, "a", 2),
        assignment("scratch", 0, "a", 1),
    ];
    let refs: Vec<&Assignment> = assigned.iter().collect();
    let offsets = answer(serde_json::json!({
        "shards": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0, "kind": "stream",
              "generation": 3, "logs": { "records": 41 } }
        ],
        "skipped": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 1, "kind": "stream",
              "reason": "settling" },
            { "tenant_id": "t", "namespace": "ns", "name": "scratch", "shard": 0, "kind": "stream",
              "reason": "not_durable" }
        ]
    }));

    let missing = match_leader("a", &refs, &offsets).expect_err("incomplete");
    assert_eq!(missing.len(), 3, "{missing:?}");
    assert!(
        missing[0].contains("generation 3 rather than 4"),
        "{missing:?}"
    );
    assert!(missing[1].contains("settling"), "{missing:?}");
    assert!(missing[2].contains("not led there"), "{missing:?}");
}

#[test]
fn an_in_memory_stream_is_left_out_rather_than_missing() {
    let assigned = [assignment("scratch", 0, "a", 1)];
    let refs: Vec<&Assignment> = assigned.iter().collect();
    let offsets = answer(serde_json::json!({
        "shards": [],
        "skipped": [
            { "tenant_id": "t", "namespace": "ns", "name": "scratch", "shard": 0, "kind": "stream",
              "reason": "not_durable" }
        ]
    }));
    assert_eq!(
        match_leader("a", &refs, &offsets).expect("nothing to back up"),
        vec![]
    );
}

#[test]
fn a_new_leader_or_generation_between_the_reads_starts_the_collection_over() {
    let before = vec![
        assignment("orders", 0, "a", 3),
        assignment("orders", 1, "b", 2),
    ];
    assert!(!leadership_changed(&before, &before.clone()));

    let mut moved = before.clone();
    moved[1].leader = "c".to_string();
    assert!(leadership_changed(&before, &moved));

    let mut bumped = before.clone();
    bumped[0].generation = 4;
    assert!(leadership_changed(&before, &bumped));

    let mut added = before.clone();
    added.push(assignment("orders", 2, "a", 1));
    assert!(leadership_changed(&before, &added));
}

#[test]
fn the_manifest_round_trips_and_leaves_out_logs_a_shard_does_not_have() {
    let manifest = Manifest {
        format_version: FORMAT_VERSION,
        name: "nightly".to_string(),
        taken_at_millis: 1_700_000_000_000,
        metadata_version: 12,
        shards: vec![PointShard {
            tenant_id: "t".to_string(),
            namespace: "ns".to_string(),
            name: "prices".to_string(),
            shard: 0,
            kind: "cache".to_string(),
            leader: "a".to_string(),
            generation: 5,
            logs: PointLogs {
                records: 9,
                group_cursors: None,
                group_dead_letters: None,
                counters: Some(3),
            },
            incarnation: 0,
            end_term: None,
        }],
    };
    let json = serde_json::to_value(&manifest).expect("json");
    assert_eq!(json["format_version"], 1);
    assert_eq!(
        json["shards"][0]["logs"],
        serde_json::json!({ "records": 9, "counters": 3 })
    );
    let back: Manifest = serde_json::from_value(json).expect("parse");
    assert_eq!(back, manifest);
    assert!(render_summary(&back).contains("t/ns/prices/0 (cache)"));
}

/// `broker`'s answer for `orders/0`, as a voter of the membership
/// `{a, b, c}`, at `term`, leading with `leads` records or not.
fn raft_answer(broker: &str, term: u64, leads: Option<u64>) -> BrokerOffsets {
    member_answer((broker, 1), term, leads, &[&["a", "b", "c"]], [1, 1])
}

/// The answer of member `(broker, copy)` for `orders/0`, at `term`, leading
/// with `leads` records or not, holding the voter sets `voters` (every voter
/// at copy 1) from the entry `at`.
fn member_answer(
    (broker, copy): (&str, u64),
    term: u64,
    leads: Option<u64>,
    voters: &[&[&str]],
    at: [u64; 2],
) -> BrokerOffsets {
    let voter = |broker: &str, copy: u64| serde_json::json!({ "broker": broker, "copy": copy });
    let voters: Vec<Vec<serde_json::Value>> = voters
        .iter()
        .map(|set| set.iter().map(|b| voter(b, 1)).collect())
        .collect();
    let member = serde_json::json!({ "tenant_id": "t", "namespace": "ns", "name": "orders",
        "shard": 0, "kind": "stream", "term": term, "member": voter(broker, copy),
        "voters": voters, "membership_at": at });
    let shards: Vec<serde_json::Value> = leads
        .map(|records| {
            serde_json::json!({ "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0,
                "kind": "stream", "generation": term, "incarnation": 4, "engine": "raft",
                "end_term": term - 1, "logs": { "records": records } })
        })
        .into_iter()
        .collect();
    answer(serde_json::json!({ "shards": shards, "raft_members": [member] }))
}

/// **A Raft shard is read on the member that leads it, wherever placement
/// put the leader (#2995).** Placement names `a`; `b` won the election.
#[test]
fn a_raft_shard_is_read_on_its_elected_leader() {
    let assigned = [
        assignment("orders", 0, "a", 3),
        assignment("other", 0, "a", 1),
    ];
    let (a, b) = (raft_answer("a", 7, None), raft_answer("b", 7, Some(42)));
    let answers = vec![("a".to_string(), &a), ("b".to_string(), &b)];
    let raft = raft_keys(&assigned, &answers);
    assert!(raft.contains(&assigned[0].key()));
    assert!(!raft.contains(&assigned[1].key()), "not a Raft shard");

    let shards = match_raft(&[&assigned[0]], &answers).expect("point");
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].leader, "b");
    assert_eq!(shards[0].logs.records, 42);
    assert_eq!(shards[0].incarnation, 4);
    assert_eq!(shards[0].end_term, Some(6), "the term below the point");
    assert_eq!(
        shards[0].generation, 3,
        "placement's generation, not the term"
    );
}

/// **A leader at an older term than some member reports is not read.** It
/// may be deposed without knowing it, its commit index below a write the
/// newer leader acknowledged.
#[test]
fn a_leader_behind_a_newer_term_is_not_read() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let (a, b) = (raft_answer("a", 7, Some(40)), raft_answer("b", 8, None));
    let answers = vec![("a".to_string(), &a), ("b".to_string(), &b)];
    let missing = match_raft(&[&assigned[0]], &answers).expect_err("no leader at term 8");
    assert!(missing[0].contains("term 8"), "{missing:?}");
}

/// **A shard no broker lists a Raft member of yet is asked about again,
/// not read on placement's leader for good (#2995).** In the first round
/// no broker lists `orders/0` (its members are not open yet), and `a`,
/// which placement names, does not lead it. In the second, `b` leads it.
#[tokio::test]
async fn a_raft_shard_unlisted_in_one_round_is_read_on_its_leader_in_the_next() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let mut rounds = 0;
    let round = || {
        rounds += 1;
        let first = rounds == 1;
        async move {
            let empty = || answer(serde_json::json!({ "shards": [] }));
            if first {
                vec![
                    ("a".to_string(), Ok(empty())),
                    ("b".to_string(), Ok(empty())),
                ]
            } else {
                vec![
                    ("a".to_string(), Ok(raft_answer("a", 7, None))),
                    ("b".to_string(), Ok(raft_answer("b", 7, Some(42)))),
                ]
            }
        }
    };
    let shards = collect_rounds(round, &assigned, Duration::ZERO)
        .await
        .expect("the second round has a leader");
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].leader, "b");
    assert_eq!(shards[0].logs.records, 42);
}

/// **A hybrid shard is read on placement's leader in the same round.** An
/// unreachable leader leaves its shards missing, so the round is asked
/// again rather than giving a partial point.
#[test]
fn a_hybrid_shard_whose_leader_did_not_answer_is_missing() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let round: Round = vec![("a".to_string(), Err(anyhow::anyhow!("refused")))];
    let missing = match_round(&assigned, &round).expect_err("no answer");
    assert!(missing[0].contains("refused"), "{missing:?}");

    let round: Round = vec![(
        "a".to_string(),
        Ok(answer(serde_json::json!({ "shards": [
            { "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0, "kind": "stream",
              "generation": 3, "logs": { "records": 9 } }
        ] }))),
    )];
    let shards = match_round(&assigned, &round).expect("point");
    assert_eq!(shards[0].leader, "a");
    assert_eq!(shards[0].logs.records, 9);
}

/// A round where `answering` answered and every other broker in `cut_off`
/// could not be reached.
fn round_of(answering: Vec<(&str, BrokerOffsets)>, cut_off: &[&str]) -> Round {
    let mut round: Round = answering
        .into_iter()
        .map(|(id, offsets)| (id.to_string(), Ok(offsets)))
        .collect();
    for id in cut_off {
        round.push((id.to_string(), Err(anyhow::anyhow!("unreachable"))));
    }
    round
}

/// **A deposed leader that answers alone gives no point, and the command
/// fails naming the shard (#3179).** `a` led term 4 and is cut off; `b` and
/// `c` elected `b` in term 5 and acknowledged writes, and felixctl reaches
/// neither. `a`'s commit index would drop every one of them.
#[tokio::test]
async fn a_deposed_leader_answering_alone_gives_no_point() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let round = || async { round_of(vec![("a", raft_answer("a", 4, Some(40)))], &["b", "c"]) };
    let err = collect_rounds(round, &assigned, Duration::ZERO)
        .await
        .expect_err("one voter of three is no majority");
    let err = format!("{err:#}");
    assert!(err.contains("t/ns/orders/0 (stream)"), "{err}");
    assert!(err.contains("1 of its 3 voters answered"), "{err}");
}

/// **With a majority answering, the newest leader's point is taken, not
/// the deposed one's.**
#[test]
fn a_majority_with_both_leaders_reads_the_newest() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let round = round_of(
        vec![
            ("a", raft_answer("a", 4, Some(40))),
            ("b", raft_answer("b", 5, Some(55))),
        ],
        &["c"],
    );
    let shards = match_round(&assigned, &round).expect("point");
    assert_eq!(shards[0].leader, "b");
    assert_eq!(shards[0].logs.records, 55);
}

/// **A majority that does not include the new leader still sees its term,
/// so the deposed leader is not read.** The shard is missing this round.
#[test]
fn a_majority_without_the_new_leader_gives_no_point() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let round = round_of(
        vec![
            ("a", raft_answer("a", 4, Some(40))),
            ("c", raft_answer("c", 5, None)),
        ],
        &["b"],
    );
    let missing = match_round(&assigned, &round).expect_err("no leader at term 5");
    assert!(missing[0].contains("term 5"), "{missing:?}");
}

/// **A joint membership needs a majority of both voter sets.** `{a, b, c}`
/// is being replaced by `{c, d, e}`: `a` and `b` alone are a majority of
/// the old set and none of the new one.
#[test]
fn a_joint_membership_needs_both_majorities() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let joint: &[&[&str]] = &[&["a", "b", "c"], &["c", "d", "e"]];
    let at = |broker, leads| member_answer((broker, 1), 6, leads, joint, [6, 30]);
    let round = round_of(
        vec![("a", at("a", None)), ("b", at("b", Some(70)))],
        &["c", "d", "e"],
    );
    let missing = match_round(&assigned, &round).expect_err("old majority only");
    assert!(missing[0].contains("0 of its 3 voters"), "{missing:?}");

    let round = round_of(
        vec![
            ("a", at("a", None)),
            ("b", at("b", Some(70))),
            ("d", at("d", None)),
            ("e", at("e", None)),
        ],
        &["c"],
    );
    let shards = match_round(&assigned, &round).expect("both majorities");
    assert_eq!(shards[0].leader, "b");
    assert_eq!(shards[0].logs.records, 70);
}

/// **The membership from the latest entry is the one counted.** `c` was
/// replaced by `d` at `[5, 20]`; `c`, removed, still holds `{a, b, c}` from
/// `[2, 4]`. Counted against `c`'s stale set, `a` and `c` would pass.
#[test]
fn a_stale_membership_is_not_counted_against() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let round = round_of(
        vec![
            (
                "a",
                member_answer(("a", 1), 5, Some(30), &[&["a", "b", "d"]], [5, 20]),
            ),
            (
                "c",
                member_answer(("c", 1), 4, None, &[&["a", "b", "c"]], [2, 4]),
            ),
        ],
        &["b", "d"],
    );
    let missing = match_round(&assigned, &round).expect_err("one voter of {a, b, d}");
    assert!(missing[0].contains("1 of its 3 voters"), "{missing:?}");
}

/// **A single-voter group is its own majority.**
#[test]
fn a_single_voter_group_makes_a_point() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let round = round_of(
        vec![("a", member_answer(("a", 1), 2, Some(9), &[&["a"]], [1, 1]))],
        &[],
    );
    let shards = match_round(&assigned, &round).expect("point");
    assert_eq!(shards[0].leader, "a");
    assert_eq!(shards[0].logs.records, 9);
}

/// **A learner, or a copy that came back under a new id, counts toward no
/// majority.** `d` is a learner and `b#2` is not the voter `b#1`, so only
/// `a` of `{a, b, c}` answered.
#[test]
fn a_learner_or_a_new_copy_does_not_count() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let voters: &[&[&str]] = &[&["a", "b", "c"]];
    let round = round_of(
        vec![
            ("a", member_answer(("a", 1), 4, Some(40), voters, [1, 1])),
            ("b", member_answer(("b", 2), 4, None, voters, [1, 1])),
            ("d", member_answer(("d", 1), 4, None, voters, [1, 1])),
        ],
        &["c"],
    );
    let missing = match_round(&assigned, &round).expect_err("one voter answered");
    assert!(missing[0].contains("1 of its 3 voters"), "{missing:?}");
}

/// **A member that reports no voters, as a broker from before they were
/// reported does, gives no point.**
#[test]
fn an_answer_without_voters_gives_no_point() {
    let assigned = [assignment("orders", 0, "a", 3)];
    let old = answer(serde_json::json!({
        "shards": [{ "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0,
            "kind": "stream", "generation": 4, "engine": "raft", "logs": { "records": 40 } }],
        "raft_members": [{ "tenant_id": "t", "namespace": "ns", "name": "orders", "shard": 0,
            "kind": "stream", "term": 4 }]
    }));
    let round = round_of(vec![("a", old)], &[]);
    let missing = match_round(&assigned, &round).expect_err("no voters known");
    assert!(missing[0].contains("reports its voters"), "{missing:?}");
}
