//! What the control plane believes about replicas, and for how long.
//!
//! The reading side only. Which report the store keeps when two arrive --
//! generations, updates, a deleted assignment -- is the store's contract, in
//! `store::contract::replica_reports`, and holds on every backend.
use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::config::NodeLivenessConfig;

const HEARTBEAT_MS: u64 = 500;
const EXPIRY_MS: u64 = 1_500;
/// Reports are believed for twice the expiry plus one heartbeat.
const TTL_MS: u64 = EXPIRY_MS * 2 + HEARTBEAT_MS;

fn liveness() -> NodeLivenessConfig {
    NodeLivenessConfig {
        heartbeat_interval_ms: HEARTBEAT_MS,
        expiry_timeout_ms: EXPIRY_MS,
        regrant_margin_ms: None,
        ..Default::default()
    }
}

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: crate::model::ShardKind::Stream,
    }
}

/// Every caught-up node at the same offset, for tests that are about freshness
/// rather than about which replica is furthest ahead.
fn report(stream: &str, caught_up: &[&str], reported_at_millis: u64) -> ReplicaReport {
    let caught_up: BTreeSet<String> = caught_up.iter().map(|n| n.to_string()).collect();
    ReplicaReport {
        counter_offsets: Default::default(),
        stream_incarnation: None,
        by_round: false,
        leader_incarnation: None,
        key: key(stream),
        generation: 4,
        offsets: caught_up
            .iter()
            .map(|n| (n.clone(), 10))
            .collect::<BTreeMap<_, _>>(),
        caught_up,
        reported_at_millis,
        drained: false,
        leader_offset: None,
        halted: Default::default(),
    }
}

fn at(reports: Vec<ReplicaReport>, now_millis: u64) -> ReplicaPositions {
    ReplicaPositions::new(reports, &liveness(), now_millis)
}

/// A reported follower can take over; one that was not reported cannot.
#[test]
fn a_reported_follower_is_caught_up_and_others_are_not() {
    let view = at(vec![report("orders", &["broker-b"], 1_000)], 1_000);
    assert!(view.is_caught_up(&key("orders"), "broker-b"));
    assert!(!view.is_caught_up(&key("orders"), "broker-c"));
    assert_eq!(view.reported_offset(&key("orders"), "broker-b"), Some(10));
    assert_eq!(view.reported_offset(&key("orders"), "broker-c"), None);
}

/// **Nothing is caught up until a leader says so.** The absence of a report is
/// not permission to promote.
#[test]
fn an_unreported_shard_has_nothing_caught_up() {
    assert!(!at(Vec::new(), 1_000).is_caught_up(&key("orders"), "broker-b"));
}

/// **A report expires.** It says a follower *was* caught up; the leader kept
/// writing afterwards, and promoting on a stale report silently loses whatever
/// was written since.
#[test]
fn a_report_is_not_believed_forever() {
    let reports = || vec![report("orders", &["broker-b"], 1_000)];
    assert!(
        at(reports(), 1_000 + TTL_MS).is_caught_up(&key("orders"), "broker-b"),
        "a report expired before the window it has to survive",
    );
    assert!(
        !at(reports(), 1_000 + TTL_MS + 1).is_caught_up(&key("orders"), "broker-b"),
        "a stale report was still believed",
    );
    assert_eq!(
        at(reports(), 1_000 + TTL_MS + 1).reported_offset(&key("orders"), "broker-b"),
        None,
        "a stale report's offset was still used to rank a candidate",
    );
}

/// **The window outlives the detection of a dead leader.** The last report a
/// leader makes is from just before it dies, and promotion happens only after
/// the cluster has noticed — which takes the expiry timeout. A report that
/// expired sooner could never be used for the failover it exists for.
#[test]
fn a_report_outlives_the_window_a_dead_leader_is_noticed_in() {
    let last_report = 1_000;
    // The leader dies just after reporting; the cluster notices an expiry
    // timeout later and plans then.
    let planning_at = last_report + EXPIRY_MS;
    assert!(
        at(
            vec![report("orders", &["broker-b"], last_report)],
            planning_at
        )
        .is_caught_up(&key("orders"), "broker-b"),
        "the report expired before failover could use it",
    );
}

/// **A report has to outlive the detection of the leader that made it.**
///
/// A leader is declared down an expiry timeout after its last *heartbeat*, and
/// its last *report* is older still. If the report expires first the shard
/// becomes unpromotable forever — nothing else will ever report on it, because
/// the only broker that could is the one that died.
#[test]
fn a_report_outlives_the_detection_of_the_leader_that_made_it() {
    // The worst realistic case: the leader reports, then survives a further
    // heartbeat interval before dying, and is declared down an expiry timeout
    // after that.
    let reported_at = 1_000;
    let declared_down_at = reported_at + HEARTBEAT_MS + EXPIRY_MS;
    assert!(
        at(
            vec![report("orders", &["broker-b"], reported_at)],
            declared_down_at
        )
        .is_caught_up(&key("orders"), "broker-b"),
        "the report expired before the leader was even known to be gone, which \
         leaves the shard unpromotable for good",
    );
}

/// Shards are independent: one shard's replicas say nothing about another's.
#[test]
fn shards_do_not_share_reports() {
    let view = at(vec![report("orders", &["broker-b"], 1_000)], 1_000);
    assert!(!view.is_caught_up(&key("payments"), "broker-b"));
}

/// A planning pass judges every shard against one instant, so a report cannot
/// be fresh for one shard and stale for the next within the same pass.
#[test]
fn one_pass_judges_every_shard_at_the_same_instant() {
    let view = at(
        vec![
            report("orders", &["broker-b"], 1_000),
            report("payments", &["broker-b"], 1_000),
        ],
        1_000 + TTL_MS,
    );
    assert!(view.is_caught_up(&key("orders"), "broker-b"));
    assert!(view.is_caught_up(&key("payments"), "broker-b"));
}

/// **Both sides have to read the same clock.**
///
/// Freshness is a subtraction: the stamp a report was recorded with, against
/// the instant the placement pass reads. `ReplicaPositions::load` takes both
/// from the store, which is what makes the subtraction single-clock under
/// Postgres, where the instance that recorded the report and the one planning
/// may be different hosts. This is what a skew would cost, in the direction
/// that matters: a report stamped by a clock behind the reader's reads as older
/// than it is, so a replica that is level with its leader is called stale and
/// is not considered for promotion.
#[test]
fn a_stamp_from_a_clock_behind_the_readers_looks_stale_while_it_is_fresh() {
    let reader_now = 10_000_000;
    // Well inside the TTL; entirely outside it once skewed — which the compiler
    // holds to, so the test cannot quietly stop demonstrating anything if the
    // window is widened.
    const SKEW_MS: u64 = 60_000;
    const _: () = assert!(SKEW_MS > TTL_MS);
    let writer_now = reader_now - SKEW_MS;
    let reports = || vec![report("orders", &["broker-b"], writer_now)];

    assert!(
        !at(reports(), reader_now).is_caught_up(&key("orders"), "broker-b"),
        "a skew this large has to be visible, or the test proves nothing",
    );
    // The same report, judged on the clock that wrote it.
    assert!(at(reports(), writer_now).is_caught_up(&key("orders"), "broker-b"));
}

/// Every report the store holds is read, and the store's clock is the instant
/// they are judged at.
#[tokio::test]
async fn load_reads_the_store_on_the_stores_clock() {
    use crate::store::ControlPlaneStore;

    let store = crate::store::memory::InMemoryStore::new(crate::store::StoreConfig {
        changes_limit: 100,
        change_retention_max_rows: Some(100),
    });
    crate::store::contract::shards::seed(&store).await;
    let key = crate::store::contract::shards::key(0);
    store
        .put_shard_assignment(crate::store::contract::shards::assignment(0, "broker-x"))
        .await
        .expect("assign");
    let now = store.now_millis().await.expect("clock");
    let generation = store
        .get_shard_assignment(&key)
        .await
        .expect("get")
        .generation;
    store
        .record_replica_report(
            ReplicaReport {
                counter_offsets: Default::default(),
                stream_incarnation: None,
                by_round: false,
                leader_incarnation: None,
                key: key.clone(),
                generation,
                caught_up: ["broker-b".to_string()].into_iter().collect(),
                offsets: [("broker-b".to_string(), 7)].into_iter().collect(),
                reported_at_millis: now,
                drained: false,
                leader_offset: None,
                halted: Default::default(),
            },
            "broker-x",
        )
        .await
        .expect("record");

    let view = ReplicaPositions::load(&store, &liveness())
        .await
        .expect("load");
    assert!(view.is_caught_up(&key, "broker-b"));
    assert_eq!(view.reported_offset(&key, "broker-b"), Some(7));
}

/// A drained report counts only at the generation it was made for, and only
/// while it is fresh: the fence is a new generation, and a report from before
/// it describes a leader that was still writing.
#[test]
fn a_drained_report_is_believed_at_its_generation_while_fresh() {
    let liveness = liveness();
    let mut drained = report("orders", &["broker-b"], 1_000);
    drained.drained = true;
    let positions = at(vec![drained.clone()], 1_000);
    assert!(positions.is_drained(&key("orders"), drained.generation));
    assert!(!positions.is_drained(&key("orders"), drained.generation + 1));
    assert!(!positions.is_drained(&key("orders"), drained.generation - 1));

    let stale = at(
        vec![drained.clone()],
        1_000 + report_ttl_millis(&liveness) + 1,
    );
    assert!(!stale.is_drained(&key("orders"), drained.generation));

    let not_drained = at(vec![report("orders", &["broker-b"], 1_000)], 1_000);
    assert!(!not_drained.is_drained(&key("orders"), drained.generation));
}

/// How far behind a follower is, measured against the tail the leader
/// reported beside it; nothing without that tail or once the report is
/// stale.
#[test]
fn lag_is_measured_against_the_reported_tail() {
    let mut with_tail = report("orders", &["broker-b"], 1_000);
    with_tail.leader_offset = Some(25);
    let view = at(vec![with_tail.clone()], 1_000);
    assert_eq!(view.lag_records(&key("orders"), "broker-b"), Some(15));
    assert_eq!(view.lag_records(&key("orders"), "broker-c"), None);
    assert_eq!(view.as_of_millis(), Some(1_000));

    let view = at(vec![report("orders", &["broker-b"], 1_000)], 1_000);
    assert_eq!(view.lag_records(&key("orders"), "broker-b"), None);

    let stale = 1_000 + report_ttl_millis(&liveness()) + 1;
    assert_eq!(
        at(vec![with_tail], stale).lag_records(&key("orders"), "broker-b"),
        None
    );
}

fn suspicion(node_id: &str, leader: &str, reported_at_millis: u64) -> crate::model::NodeSuspicion {
    crate::model::NodeSuspicion {
        node_id: node_id.to_string(),
        incarnation: 1,
        suspects: [leader.to_string()].into_iter().collect(),
        reported_at_millis,
    }
}

/// The leader, `broker-a`, at the incarnation its reports came from.
fn incarnations() -> HashMap<String, u64> {
    HashMap::from([("broker-a".to_string(), 1)])
}

fn features(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// A report from the leader of `orders` at generation 4, saying whether it
/// serves by round.
fn leader_report(by_round: bool, reported_at_millis: u64) -> ReplicaReport {
    ReplicaReport {
        by_round,
        leader_incarnation: by_round.then_some(1),
        ..report("orders", &["broker-b"], reported_at_millis)
    }
}

const ALL: &[&str] = &["generation_start", "majority_ack", "lease_free_reads"];

/// **A suspicion counts for two heartbeats.** A broker that still suspects
/// says so on every beat; one that stopped says nothing, so an old one must
/// stop counting on its own.
#[test]
fn a_suspicion_counts_for_two_heartbeats() {
    let now = 100_000;
    let held = |stamped| {
        at(vec![leader_report(true, now)], now)
            .with_suspicions(
                vec![suspicion("broker-b", "broker-a", stamped)],
                incarnations(),
                &liveness(),
                &features(ALL),
            )
            .suspects(&key("orders"), 4, "broker-b", "broker-a")
    };
    assert!(held(now - 2 * HEARTBEAT_MS));
    assert!(!held(now - 2 * HEARTBEAT_MS - 1));
}

/// **Only once the fleet keeps a deposed leader out without its lease.**
/// `majority_ack` keeps its writes from being acknowledged; without
/// `lease_free_reads` it still answers reads on the lease. Brokers use
/// either only alongside `generation_start`, so without it both stay on the
/// lease, whatever else is enabled.
#[test]
fn a_suspicion_counts_only_with_all_three_features() {
    let now = 100_000;
    for (enabled, counts) in [
        (
            features(&["generation_start", "majority_ack", "lease_free_reads"]),
            true,
        ),
        (features(&["majority_ack", "lease_free_reads"]), false),
        (features(&["generation_start", "majority_ack"]), false),
        (features(&["generation_start", "lease_free_reads"]), false),
        (features(&["majority_ack"]), false),
        (features(&["lease_free_reads"]), false),
        (features(&[]), false),
    ] {
        let positions = at(vec![leader_report(true, now)], now).with_suspicions(
            vec![suspicion("broker-b", "broker-a", now)],
            incarnations(),
            &liveness(),
            &enabled,
        );
        assert_eq!(
            positions.suspects(&key("orders"), 4, "broker-b", "broker-a"),
            counts,
            "{enabled:?}"
        );
        assert!(!positions.suspects(&key("orders"), 4, "broker-b", "broker-c"));
    }
}

/// **Not for a cache.** A `Quorum` cache still fails over on the lease.
#[test]
fn a_suspicion_does_not_move_a_cache() {
    let now = 100_000;
    let cache = ShardKey {
        kind: crate::model::ShardKind::Cache,
        ..key("profiles")
    };
    let positions = at(
        vec![ReplicaReport {
            key: cache.clone(),
            ..leader_report(true, now)
        }],
        now,
    )
    .with_suspicions(
        vec![suspicion("broker-b", "broker-a", now)],
        incarnations(),
        &liveness(),
        &features(&[
            "generation_start",
            "majority_ack",
            "lease_free_reads",
            "fenced_caches",
        ]),
    );
    assert!(!positions.suspects(&cache, 4, "broker-b", "broker-a"));
}

/// **Only against a leader that reported it serves nothing on its lease.**
/// The enabled set says what brokers may do. A leader still reading on its
/// lease, kept there by `FELIX_QUORUM_READS=lease` or not yet told of the
/// finalize, would go on serving after it was replaced. A broker that
/// predates the field never says so.
#[test]
fn a_suspicion_counts_only_against_a_leader_off_its_lease() {
    let now = 100_000;
    let suspects = |reports: Vec<ReplicaReport>, generation| {
        at(reports, now)
            .with_suspicions(
                vec![suspicion("broker-b", "broker-a", now)],
                incarnations(),
                &liveness(),
                &features(ALL),
            )
            .suspects(&key("orders"), generation, "broker-b", "broker-a")
    };
    assert!(suspects(vec![leader_report(true, now)], 4));
    // On its lease, or silent about it.
    assert!(!suspects(vec![leader_report(false, now)], 4));
    let older: ReplicaReport = serde_json::from_value(serde_json::json!({
        "key": key("orders"),
        "generation": 4,
        "caught_up": ["broker-b"],
        "offsets": {"broker-b": 10},
        "reported_at_millis": now,
    }))
    .expect("a report without the field");
    assert!(!suspects(vec![older], 4));
    // No report, or one too old to believe.
    assert!(!suspects(Vec::new(), 4));
    assert!(!suspects(vec![leader_report(true, now - TTL_MS - 1)], 4));
    // Reported at another generation: not this leader's word.
    assert!(!suspects(vec![leader_report(true, now)], 5));
}

/// **Not on a report from before the leader restarted.** A broker that
/// restarts within the report's lifetime can lead again at the same
/// generation, and serves on its lease until its first replication pass. Its
/// last report, from the process that is gone, still says `by_round`; the
/// incarnation its registration took tells the two apart.
#[test]
fn a_suspicion_does_not_count_on_a_report_from_before_a_restart() {
    let now = 100_000;
    let suspects = |incarnations: HashMap<String, u64>| {
        at(vec![leader_report(true, now)], now)
            .with_suspicions(
                vec![suspicion("broker-b", "broker-a", now)],
                incarnations,
                &liveness(),
                &features(ALL),
            )
            .suspects(&key("orders"), 4, "broker-b", "broker-a")
    };
    assert!(suspects(incarnations()));
    assert!(!suspects(HashMap::from([("broker-a".to_string(), 2)])));
    assert!(!suspects(HashMap::new()));
    // Nor one that says `by_round` without naming the incarnation.
    let unnamed = ReplicaReport {
        leader_incarnation: None,
        ..leader_report(true, now)
    };
    assert!(
        !at(vec![unnamed], now)
            .with_suspicions(
                vec![suspicion("broker-b", "broker-a", now)],
                incarnations(),
                &liveness(),
                &features(ALL),
            )
            .suspects(&key("orders"), 4, "broker-b", "broker-a")
    );
}

/// **Finalizing does not move a leader that has not heard of it.** Brokers
/// learn the enabled set on their next heartbeat, and until then a leader
/// reads on its lease and reports so. Placement waits for the report that
/// says otherwise.
#[test]
fn a_finalize_waits_for_the_leader_to_report_by_round() {
    let now = 100_000;
    let positions = |reports| {
        at(reports, now).with_suspicions(
            vec![suspicion("broker-b", "broker-a", now)],
            incarnations(),
            &liveness(),
            &features(ALL),
        )
    };
    // The last feature just committed; the leader's report predates it.
    let before = positions(vec![leader_report(false, now - HEARTBEAT_MS)]);
    assert!(!before.suspects(&key("orders"), 4, "broker-b", "broker-a"));
    // Its next report, after the heartbeat that told it.
    let after = positions(vec![leader_report(true, now)]);
    assert!(after.suspects(&key("orders"), 4, "broker-b", "broker-a"));
}

fn reporter(id: &str, last_heartbeat_at_millis: u64) -> crate::model::Node {
    crate::model::Node {
        node_id: id.to_string(),
        spec: crate::model::NodeSpec {
            advertise_addr: "10.0.0.4:7000".to_string(),
            client_addr: None,
            kafka_addr: None,
            region: "us-west-2".to_string(),
            zone: None,
            labels: Default::default(),
            capacity: crate::model::NodeCapacity {
                max_shards: None,
                weight: 1,
            },
        },
        status: crate::model::NodeStatus {
            lifecycle: crate::model::NodeLifecycle::Down,
            last_heartbeat_at_millis,
            registered_at_millis: 1,
            incarnation: 0,
            features: Default::default(),
        },
    }
}

fn led_by(stream: &str, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(stream),
        leader: leader.to_string(),
        replicas: vec!["broker-b".to_string()],
        generation,
        state: crate::model::ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
    }
}

/// **A report ages only while its leader is heard from.** Measured against
/// now, a dead leader's last report would expire and leave its shard with
/// nothing to promote, since nobody else reports on it.
#[test]
fn a_dead_leaders_report_stops_ageing_at_its_last_heartbeat() {
    let reported_at = 1_000;
    let now = reported_at + TTL_MS * 100;
    let report = report("orders", &["broker-b"], reported_at);
    let existing = [led_by("orders", "broker-a", report.generation)];

    let heard_within = at(vec![report.clone()], now)
        .with_reporters(&existing, &[reporter("broker-a", reported_at + TTL_MS)]);
    assert!(heard_within.is_caught_up(&key("orders"), "broker-b"));

    let heard_after = at(vec![report], now)
        .with_reporters(&existing, &[reporter("broker-a", reported_at + TTL_MS + 1)]);
    assert!(
        !heard_after.is_caught_up(&key("orders"), "broker-b"),
        "a report made a TTL before the leader's last heartbeat was believed",
    );
}

/// **A leader that heartbeats but stops reporting still ages its report out.**
/// Its heartbeat keeps up with now, so the report is measured as before.
#[test]
fn a_live_leaders_old_report_still_expires() {
    let reported_at = 1_000;
    let now = reported_at + TTL_MS + 1;
    let report = report("orders", &["broker-b"], reported_at);
    let existing = [led_by("orders", "broker-a", report.generation)];

    let view = at(vec![report], now).with_reporters(&existing, &[reporter("broker-a", now)]);
    assert!(!view.is_caught_up(&key("orders"), "broker-b"));
}

/// A report from another generation may be from another leader, so it is not
/// aged against this one's heartbeat.
#[test]
fn a_report_from_another_generation_ages_against_now() {
    let reported_at = 1_000;
    let now = reported_at + TTL_MS + 1;
    let report = report("orders", &["broker-b"], reported_at);
    let existing = [led_by("orders", "broker-a", report.generation + 1)];

    let view =
        at(vec![report], now).with_reporters(&existing, &[reporter("broker-a", reported_at)]);
    assert!(!view.is_caught_up(&key("orders"), "broker-b"));
}
