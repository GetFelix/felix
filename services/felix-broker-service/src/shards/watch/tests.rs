//! Continuity and apply rules: every branch here is a way to silently lose an
//! ownership change.

use super::*;
use crate::shards::ShardKind;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: crate::shards::ShardKind::Stream,
    }
}

fn assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
    }
}

#[test]
fn a_contiguous_page_needs_no_resync() {
    // Asked from 5, got 5 onwards.
    assert_eq!(check_continuity(5, Some(5), 9), None);
    // Asked from 5, nothing new yet.
    assert_eq!(check_continuity(5, None, 5), None);
    // First poll of an empty cluster.
    assert_eq!(check_continuity(0, None, 0), None);
}

/// The change we asked for is gone from the window, so everything between our
/// checkpoint and the first returned change was evicted.
#[test]
fn a_first_seq_above_the_checkpoint_is_a_gap() {
    assert_eq!(check_continuity(5, Some(9), 20), Some(Resync::GapInHistory),);
}

/// The subtle one: an empty page is only safe when the log has not moved. If it
/// has, the whole span was evicted rather than being empty, and resuming would
/// skip it in silence.
#[test]
fn an_empty_page_with_a_moved_log_is_a_gap() {
    assert_eq!(check_continuity(5, None, 12), Some(Resync::GapInHistory));
    assert_eq!(
        check_continuity(5, None, 5),
        None,
        "not moved: genuinely empty"
    );
}

/// A control plane restarted onto a store that does not persist its sequence,
/// so the log is now behind our checkpoint. Continuing would apply old changes
/// as if they were new.
#[test]
fn a_log_behind_the_checkpoint_is_a_reset() {
    assert_eq!(check_continuity(40, None, 0), Some(Resync::SequenceReset));
    assert_eq!(
        check_continuity(40, Some(0), 3),
        Some(Resync::SequenceReset),
        "a reset is checked before a gap, since the gap test is meaningless here",
    );
}

#[test]
fn applying_a_newer_generation_takes_effect() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 0))));
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-b", 1))));
    assert_eq!(owned.get(&key(0)).expect("present").leader, "broker-b");
}

/// Duplicate delivery has to be harmless, or an at-least-once poll would need
/// exactly-once plumbing it does not have.
#[test]
fn a_duplicate_change_is_ignored() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 3))));
    assert!(!owned.apply(&key(0), Some(assignment(0, "broker-a", 3))));
    assert_eq!(owned.len(), 1);
}

/// The property that makes reordering safe: a change that would move ownership
/// backwards is dropped, not applied.
#[test]
fn a_stale_generation_cannot_roll_ownership_back() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-b", 5)));
    assert!(!owned.apply(&key(0), Some(assignment(0, "broker-a", 4))));
    assert_eq!(
        owned.get(&key(0)).expect("present").leader,
        "broker-b",
        "an older generation must not take the shard back",
    );
}

/// A recreated stream's shard can start below the deleted stream's generation
/// (a control-plane restore rewinds the change seq it starts at). The newer
/// incarnation wins anyway, and the deleted one's assignment, delivered late,
/// does not take the shard back.
#[test]
fn a_newer_incarnation_wins_at_a_lower_generation() {
    let at = |leader: &str, generation, incarnation| ShardAssignment {
        incarnation,
        ..assignment(0, leader, generation)
    };
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(at("broker-a", 40, 7)));
    assert!(owned.apply(&key(0), Some(at("broker-b", 12, 9))));
    assert_eq!(owned.get(&key(0)).expect("present").leader, "broker-b");
    assert!(!owned.apply(&key(0), Some(at("broker-a", 41, 7))));
    assert_eq!(owned.get(&key(0)).expect("present").incarnation, 9);

    // A stream from before incarnations is at 0, below any incarnated one.
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(at("broker-a", 40, 0)));
    assert!(owned.apply(&key(0), Some(at("broker-b", 3, 9))));
}

/// A stream deleted and created again without incarnations (#1435). The
/// delete sends no change per shard, so the moved shard's assignment at
/// generation 2 is still held when the new stream's first one arrives at
/// generation 0. It must replace it: otherwise this broker keeps leading, and
/// replicating, a shard of a stream that is gone.
#[test]
fn a_recreated_streams_first_assignment_replaces_a_moved_shard() {
    let change = |seq: u64, op: &str, leader: &str, generation: u64| {
        serde_json::json!({
            "seq": seq,
            "op": op,
            "key": { "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0 },
            "assignment": {
                "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
                "leader": leader, "replicas": [], "generation": generation, "state": "active",
            },
        })
    };
    let page = |items: Vec<serde_json::Value>| -> ChangesResponse {
        serde_json::from_value(serde_json::json!({ "items": items, "next_seq": 0 }))
            .expect("changes page")
    };
    let mut owned = ShardOwnership::default();
    owned.apply_changes(
        &page(vec![
            change(1, "assigned", "broker-c", 0),
            change(2, "updated", "broker-a", 2),
        ])
        .items,
    );
    assert_eq!(owned.get(&key(0)).expect("present").leader, "broker-a");

    assert!(owned.apply_changes(&page(vec![change(3, "assigned", "broker-c", 0)]).items));
    let held = owned.get(&key(0)).expect("present");
    assert_eq!((held.leader.as_str(), held.generation), ("broker-c", 0));
    assert_eq!(owned.renewals().get(&key(0)), Some(&1));

    // An update below what is held is still stale.
    assert!(!owned.apply_changes(&page(vec![change(4, "updated", "broker-a", 0)]).items));
    assert_eq!(owned.get(&key(0)).expect("present").leader, "broker-c");
}

/// A snapshot at a lower generation of the same incarnation may come from a
/// restored or lagging control plane, not a recreate. Counting it as a renewal
/// would reopen the shard here at a generation a newer leader wrote past.
#[test]
fn a_lower_generation_snapshot_is_not_a_renewal() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-b", 2)));
    owned.reset(vec![assignment(0, "broker-a", 1)]);
    assert!(owned.renewals().get(&key(0)).is_none());
}

#[test]
fn an_unassignment_removes_the_shard() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    assert!(owned.apply(&key(0), None));
    assert!(owned.get(&key(0)).is_none());
    // Removing it twice is harmless, and reports that nothing changed.
    assert!(!owned.apply(&key(0), None));
}

#[test]
fn a_snapshot_replaces_everything() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    owned.apply(&key(1), Some(assignment(1, "broker-a", 0)));

    owned.reset(vec![assignment(2, "broker-c", 7)]);
    assert_eq!(owned.len(), 1);
    assert!(
        owned.get(&key(0)).is_none(),
        "stale ownership must not survive"
    );
    assert_eq!(owned.get(&key(2)).expect("present").leader, "broker-c");
}

#[test]
fn leadership_is_reported_for_this_node_only() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 0)));
    assert!(owned.is_leader(&key(0), "broker-a"));
    assert!(!owned.is_leader(&key(0), "broker-b"));
    assert!(!owned.is_leader(&key(9), "broker-a"), "unknown shard");
}

#[test]
fn backoff_grows_and_then_stops_growing() {
    let interval = Duration::from_millis(100);
    assert_eq!(backoff(interval, 1), interval);
    assert_eq!(backoff(interval, 2), interval * 2);
    assert_eq!(backoff(interval, 3), interval * 4);
    assert_eq!(backoff(interval, 40), MAX_BACKOFF);
}

// --- Cache shards are placed but not yet routed ------------------------------

fn cache_assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: ShardKey {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            // Deliberately the stream's name: the two are told apart by kind
            // alone, and a broker that ignored it would file one under the
            // other and serve requests against the wrong log.
            stream: "orders".to_string(),
            shard,
            kind: ShardKind::Cache,
        },
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
    }
}

/// A snapshot carrying cache shards keeps them. They were dropped while nothing
/// routed a cache request; now that the data path resolves cache keys through
/// this table, dropping one would make every key in that shard unroutable.
#[test]
fn a_snapshot_keeps_cache_shards() {
    let mut owned = ShardOwnership::default();
    owned.reset(vec![
        assignment(0, "broker-a", 1),
        cache_assignment(0, "broker-b", 1),
        assignment(1, "broker-a", 1),
    ]);

    assert_eq!(owned.len(), 3);
    assert_eq!(
        owned.get(&key(0)).map(|a| a.leader.as_str()),
        Some("broker-a"),
        "the cache shard must not have taken the stream shard's row",
    );
    assert_eq!(
        owned
            .get(&cache_assignment(0, "broker-b", 1).key)
            .map(|a| a.leader.as_str()),
        Some("broker-b"),
    );
}

/// The key includes the kind, so the two are distinct entries rather than one
/// overwriting the other — which is what lets a broker hold a cache shard and a
/// stream shard of the same name and route each to its own log.
#[test]
fn a_cache_key_is_not_the_stream_key_of_the_same_name() {
    let mut owned = ShardOwnership::default();
    assert!(owned.apply(&key(0), Some(assignment(0, "broker-a", 1))));

    let cache = cache_assignment(0, "broker-b", 1);
    assert!(owned.apply(&cache.key.clone(), Some(cache)));

    assert_eq!(owned.len(), 2);
    assert_eq!(
        owned.get(&key(0)).map(|a| a.leader.as_str()),
        Some("broker-a")
    );
}

/// A control plane that predates cache placement sends no kind at all, and the
/// only safe reading of that silence is the kind everything used to be.
#[test]
fn an_assignment_without_a_kind_is_a_stream() {
    let json = r#"{
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "shard": 3,
        "leader": "broker-a",
        "generation": 7,
        "state": "active"
    }"#;
    let assignment: ShardAssignment = serde_json::from_str(json).expect("legacy assignment");

    assert_eq!(assignment.key.kind, ShardKind::Stream);
    assert_eq!(assignment.key.shard, 3);
    assert_eq!(
        assignment.routing,
        felix_wire::routing::ShardRouting::Modulo,
        "a control plane that predates routing only ever had modulo streams"
    );
}

#[test]
fn an_assignment_carries_its_streams_routing() {
    let json = r#"{
        "tenant_id": "t1",
        "namespace": "ns",
        "stream": "orders",
        "shard": 3,
        "leader": "broker-a",
        "generation": 7,
        "state": "active",
        "routing": "jump_hash"
    }"#;
    let assignment: ShardAssignment = serde_json::from_str(json).expect("assignment");
    assert_eq!(
        assignment.routing,
        felix_wire::routing::ShardRouting::JumpHash
    );
}

/// A changes page and then a snapshot from behind what the broker applied, as
/// a lagging Raft member answers, roll nothing back. Resnapshotting at once
/// would replace the broker's ownership with the member's older view.
#[tokio::test]
async fn an_answer_from_behind_the_checkpoint_rolls_nothing_back() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{Json, Router, routing::get};

    let snapshots = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(
            "/v1/shard-assignments/snapshot",
            get({
                let snapshots = Arc::clone(&snapshots);
                move || async move {
                    // The first answer is current; every later one is a
                    // member that has not applied the assignment yet.
                    if snapshots.fetch_add(1, Ordering::SeqCst) == 0 {
                        Json(serde_json::json!({
                            "items": [{
                                "tenant_id": "t1", "namespace": "ns", "stream": "orders",
                                "shard": 0, "leader": "b1", "generation": 2, "state": "active"
                            }],
                            "next_seq": 10
                        }))
                    } else {
                        Json(serde_json::json!({ "items": [], "next_seq": 3 }))
                    }
                }
            }),
        )
        .route(
            "/v1/shard-assignments/changes",
            get(|| async { Json(serde_json::json!({ "items": [], "next_seq": 4 })) }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = tokio::spawn(run(
        felix_replication::controlplane_link::HttpControlPlaneLink::new(
            reqwest::Client::new(),
            &format!("http://{addr}"),
        ),
        None,
        Arc::clone(&ownership),
        Arc::new(Notify::new()),
        Duration::from_millis(10),
        None,
        shutdown.clone(),
    ));

    // Many polls' worth: each one answers from behind the checkpoint.
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    watch.await.expect("watch");
    server.abort();

    assert!(
        snapshots.load(Ordering::SeqCst) >= 1,
        "the watch never seeded"
    );
    assert_eq!(
        ownership.read().await.get(&key(0)).map(|a| a.generation),
        Some(2),
        "an answer from behind the checkpoint replaced the broker's ownership",
    );
}

/// The set the watch says came right before shard 0's current assignment.
fn prior_leader(owned: &ShardOwnership) -> Option<Option<String>> {
    owned
        .priors()
        .get(&crate::shards::routing::to_router_key(&key(0)))
        .map(|prior| prior.before.as_ref().map(|set| set.leader.clone()))
}

/// **The set before a route is known only from the step right before it.**
/// A joint fence leaves that set, so a generation the watch skipped, or a
/// snapshot it did not see the step into, leaves it unknown rather than
/// guessed from an older one.
#[test]
fn the_set_before_is_known_only_from_the_generation_right_before() {
    let mut owned = ShardOwnership::default();
    owned.apply(&key(0), Some(assignment(0, "broker-a", 4)));
    owned.apply(&key(0), Some(assignment(0, "broker-b", 5)));
    assert_eq!(prior_leader(&owned), Some(Some("broker-a".into())));

    // The same snapshot again keeps it; a later one it skipped into drops it.
    owned.reset(vec![assignment(0, "broker-b", 5)]);
    assert_eq!(prior_leader(&owned), Some(Some("broker-a".into())));
    owned.reset(vec![assignment(0, "broker-c", 7)]);
    assert_eq!(prior_leader(&owned), None);

    owned.apply(&key(0), Some(assignment(0, "broker-a", 9)));
    assert_eq!(prior_leader(&owned), None);
}

/// **A rename past a ballot keeps the set before when the control plane names
/// the generation it replaced** (#2024). The control plane adopted broker-a
/// at 23, a withdrawn standing left ballot 24, and placement named broker-a
/// again at 25. Nothing was recorded at 24, so the set before 25 is the one
/// at 23. Without the name the jump reads as an adoption and stays unknown,
/// and a name that is not the held generation proves nothing.
#[test]
fn a_rename_past_a_ballot_keeps_the_set_it_replaced() {
    let renamed = |previous: Option<u64>| ShardAssignment {
        previous_generation: previous,
        ..assignment(0, "broker-b", 25)
    };
    for (previous, expected) in [
        (Some(23), Some(Some("broker-a".to_string()))),
        (None, None),
        (Some(22), None),
    ] {
        let mut owned = ShardOwnership::default();
        owned.apply(&key(0), Some(assignment(0, "broker-a", 23)));
        owned.apply(&key(0), Some(renamed(previous)));
        assert_eq!(prior_leader(&owned), expected, "named {previous:?}");

        let mut owned = ShardOwnership::default();
        owned.apply(&key(0), Some(assignment(0, "broker-a", 23)));
        owned.reset(vec![renamed(previous)]);
        assert_eq!(
            prior_leader(&owned),
            expected,
            "a snapshot, named {previous:?}"
        );
    }

    let json = serde_json::json!({
        "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
        "leader": "broker-b", "generation": 25, "state": "active",
        "previous_generation": 23,
    });
    let parsed: ShardAssignment = serde_json::from_value(json).expect("assignment");
    assert_eq!(parsed.previous_generation, Some(23));
}

/// **A broker reseeded from a snapshot takes the set before from the item**
/// (#2024). The control plane cut shard 0 over from broker-a at 1 to broker-b
/// at 2, and broker-b restarted: its watch holds nothing, so only the
/// snapshot can say what came before. An item without `previous`, from an
/// older control plane or after the change left the log, leaves it unknown,
/// and a `previous` that is not right before the item proves nothing.
#[test]
fn a_reseeded_watch_takes_the_set_before_from_the_snapshot() {
    let item = |previous: Option<serde_json::Value>| {
        let mut json = serde_json::json!({
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
            "leader": "broker-b", "replicas": ["broker-a", "broker-c"],
            "generation": 2, "state": "active",
        });
        if let Some(previous) = previous {
            json["previous"] = previous;
        }
        serde_json::from_value::<super::SnapshotItem>(json).expect("item")
    };
    let before = |generation: u64| {
        serde_json::json!({
            "generation": generation, "leader": "broker-a",
            "replicas": ["broker-b", "broker-c"],
        })
    };
    for (previous, expected) in [
        (Some(before(1)), Some(Some("broker-a".to_string()))),
        (None, None),
        (Some(before(0)), None),
    ] {
        let mut owned = ShardOwnership::default();
        let (items, first, previous) = super::seed_of(vec![item(previous)]);
        owned.reset_from_control_plane(items, &first, &previous);
        assert_eq!(prior_leader(&owned), expected);
    }
}

/// **A first assignment has nothing before it**, unless the same incarnation
/// carries on from a deleted stream's logs.
#[test]
fn a_first_assignment_has_no_set_before() {
    let mut first = assignment(0, "broker-a", 12);
    first.incarnation = 3;
    let mut owned = ShardOwnership::default();
    owned.assign_afresh(&key(0), first.clone());
    assert_eq!(prior_leader(&owned), Some(None));

    let mut again = first;
    again.generation = 20;
    owned.assign_afresh(&key(0), again);
    assert_eq!(prior_leader(&owned), None);
}

/// **A recreate at incarnation 0 is never first**, even when this broker no
/// longer holds the deleted stream's assignment (a restart or snapshot between
/// the delete and the recreate). Its logs carry on from the deleted stream's.
#[test]
fn an_incarnation_zero_assignment_is_never_first() {
    let mut owned = ShardOwnership::default();
    owned.assign_afresh(&key(0), assignment(0, "broker-a", 0));
    assert_eq!(prior_leader(&owned), None);
}

/// **A snapshot item marked as a shard's first assignment has nothing before
/// it**, on the terms an `assigned` change has: not at incarnation 0, and not
/// at an incarnation this broker already held (#1872).
#[test]
fn a_snapshot_marked_first_has_no_set_before() {
    let marked = felix_common::random::HashSet::from_iter([key(0)]);
    let mut first = assignment(0, "broker-a", 12);
    first.incarnation = 3;

    let mut owned = ShardOwnership::default();
    owned.reset_marked(vec![first.clone()], &marked);
    assert_eq!(prior_leader(&owned), Some(None));

    let mut owned = ShardOwnership::default();
    owned.reset_marked(vec![assignment(0, "broker-a", 0)], &marked);
    assert_eq!(prior_leader(&owned), None, "incarnation 0 is never first");

    let mut owned = ShardOwnership::default();
    let mut held = first.clone();
    held.generation = 11;
    held.leader = "broker-z".to_string();
    owned.apply(&key(0), Some(held));
    owned.reset_marked(vec![first], &marked);
    assert_eq!(
        prior_leader(&owned),
        Some(Some("broker-z".into())),
        "the set this broker held at the same incarnation came before"
    );
}

/// **An unmarked snapshot item records nothing before it** (#1872). A
/// failover's new leader that learns its shard from a snapshot has no record
/// of the set it leaves; reading the item as a first assignment would fence
/// the new set alone while a majority of the old one still acknowledges.
/// What an older control plane sends is unmarked.
#[test]
fn an_unmarked_snapshot_item_is_not_first() {
    let item = |marked: Option<bool>| {
        let mut json = serde_json::json!({
            "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0,
            "leader": "broker-b", "replicas": ["broker-c", "broker-d"],
            "generation": 12, "state": "active", "incarnation": 3,
        });
        if let Some(marked) = marked {
            json["first_assignment"] = marked.into();
        }
        serde_json::from_value::<super::SnapshotItem>(json).expect("item")
    };
    assert!(!item(None).first_assignment);
    assert!(!item(Some(false)).first_assignment);
    let marked = item(Some(true));
    assert!(marked.first_assignment);
    assert_eq!(marked.assignment.generation, 12);

    let mut owned = ShardOwnership::default();
    owned.reset(vec![item(None).assignment]);
    assert_eq!(prior_leader(&owned), None);
}

/// The watch needs nothing from the control plane but the link: a link that
/// answers a snapshot seeds ownership with no socket involved.
#[tokio::test]
async fn a_fake_link_drives_the_watch() {
    use crate::test_support::fake_link::FakeLink;

    let link = FakeLink::default()
        .reply(
            "assignment_snapshot",
            serde_json::json!({
                "items": [{
                    "tenant_id": "t1", "namespace": "ns", "stream": "orders",
                    "shard": 0, "leader": "b1", "generation": 2, "state": "active"
                }],
                "next_seq": 10
            }),
        )
        .reply(
            "assignment_changes",
            serde_json::json!({ "items": [], "next_seq": 10 }),
        );
    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = tokio::spawn(run(
        link.clone(),
        None,
        Arc::clone(&ownership),
        Arc::new(Notify::new()),
        Duration::from_millis(10),
        None,
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    shutdown.cancel();
    watch.await.expect("watch");

    assert_eq!(
        ownership.read().await.get(&key(0)).map(|a| a.generation),
        Some(2),
    );
    assert_eq!(link.calls()[0], "assignment_snapshot");
}

/// A renewal after a surrender waits for routes that reach the feed head it
/// was accepted at (#1955). The watch is mid long-poll, which the control
/// plane holds as one lost in a partition would be; it asks again at once
/// without the hold. The first answers come from a member that has not
/// applied the move: newer than the renewal, and short of its head, they
/// confirm nothing. The caught-up answer moves the shard and confirms it.
#[tokio::test]
async fn a_renewal_after_a_surrender_waits_for_the_feed_head_it_was_accepted_at() {
    use axum::extract::Query;
    use axum::{Json, Router, routing::get};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    let caught_up = Arc::new(AtomicBool::new(false));
    let lagging_answers = Arc::new(AtomicUsize::new(0));
    let (up, lagging) = (Arc::clone(&caught_up), Arc::clone(&lagging_answers));
    let router = Router::new()
        .route(
            "/v1/shard-assignments/snapshot",
            get(|| async {
                Json(serde_json::json!({
                    "items": [{
                        "tenant_id": "t1", "namespace": "ns", "stream": "orders",
                        "shard": 0, "leader": "b1", "generation": 2, "state": "active"
                    }],
                    "next_seq": 10
                }))
            }),
        )
        .route(
            "/v1/shard-assignments/changes",
            get(move |Query(query): Query<HashMap<String, String>>| {
                let (up, lagging) = (Arc::clone(&up), Arc::clone(&lagging));
                async move {
                    if query.get("wait_ms").map(String::as_str) != Some("0") {
                        std::future::pending::<()>().await;
                    }
                    if !up.load(Ordering::Acquire) {
                        lagging.fetch_add(1, Ordering::AcqRel);
                        return Json(serde_json::json!({ "items": [], "next_seq": 10 }));
                    }
                    Json(serde_json::json!({
                        "items": [{
                            "seq": 10,
                            "key": { "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0 },
                            "assignment": {
                                "tenant_id": "t1", "namespace": "ns", "stream": "orders",
                                "shard": 0, "leader": "b2", "generation": 3, "state": "active"
                            }
                        }],
                        "next_seq": 11
                    }))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let lease = Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
        60,
    )));
    let ownership = Arc::new(RwLock::new(ShardOwnership::default()));
    let shutdown = CancellationToken::new();
    let watch = tokio::spawn(run(
        felix_replication::controlplane_link::HttpControlPlaneLink::new(
            reqwest::Client::new(),
            &format!("http://{addr}"),
        ),
        None,
        Arc::clone(&ownership),
        Arc::new(Notify::new()),
        Duration::from_millis(10),
        Some(Arc::clone(&lease)),
        shutdown.clone(),
    ));
    let seeded = async {
        while ownership.read().await.get(&key(0)).is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), seeded)
        .await
        .expect("the watch never seeded");
    // Let the watch settle into its held long-poll.
    tokio::time::sleep(Duration::from_millis(50)).await;

    lease.renew();
    lease.surrender();
    lease.renew_at(lease.now(), Some(11));
    let current = |owned: &ShardOwnership| {
        lease.routes_current(owned.as_of().expect("answered"), owned.checkpoint());
    };
    current(&*ownership.read().await);
    assert!(
        !lease.is_valid_now(),
        "routes from before the renewal confirmed it"
    );

    let asked = async {
        while lagging_answers.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), asked)
        .await
        .expect("the watch kept waiting on its held poll");
    current(&*ownership.read().await);
    assert!(
        !lease.is_valid_now(),
        "a member that has not applied the move confirmed the renewal"
    );

    caught_up.store(true, Ordering::Release);
    let moved = async {
        while ownership.read().await.get(&key(0)).map(|a| a.generation) != Some(3) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), moved)
        .await
        .expect("the watch never caught up");
    current(&*ownership.read().await);
    assert!(
        lease.is_valid_now(),
        "the caught-up routes did not confirm the lease"
    );

    shutdown.cancel();
    watch.await.expect("watch");
    server.abort();
}
