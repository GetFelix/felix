//! A broker is added or drained and its shards move without losing anything
//! acknowledged.
//!
//! These start real broker processes and move shards between them, so they
//! are slow and deliberately few. Each drives placement by hand -- the
//! harness's control plane does not run the reconciler on a timer -- and
//! reads back through the new owner what was acknowledged through the old.
//!
//! Run with `cargo test -p felix-cluster --test routing rebalance::`.
use std::collections::BTreeSet;
use std::time::Duration;

use felix_cluster::{Cluster, ClusterConfig, StreamSpec};
use serial_test::serial;

const STREAM: &str = "orders";

/// Everything `node_id` replays from the start of the stream, retried until
/// it yields something: a promoted broker learns of its own promotion through
/// the same watch as everything else and has to open the shard first.
///
/// A replay the broker resets part way (it stopped serving the shard, or its
/// leadership is not open yet) is read again from the start, never taken as
/// the log: see [`Cluster::whole_replay_on`].
async fn replay_from(cluster: &Cluster, node_id: &str, budget: Duration) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(budget);
    loop {
        let events = cluster
            .whole_replay_on(node_id, STREAM, Duration::from_secs(2), budget)
            .await
            .unwrap_or_else(|err| panic!("no whole replay from {node_id}: {err:#}"));
        if !events.is_empty() {
            return events.iter().map(|e| e.payload.to_vec()).collect();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "nothing replayed from {node_id}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Everything the shard's leader holds for it, replayed from the start.
///
/// The leader is asked by the control plane's assignment and retried: the
/// broker learns of its own assignment through its watch, so for a moment it
/// redirects rather than serves. A reset part way is retried as in
/// [`replay_from`].
async fn replay_shard_from(cluster: &Cluster, shard: u32) -> Vec<Vec<u8>> {
    let budget = Duration::from_secs(30);
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(budget);
    let mut last;
    loop {
        let leader = cluster
            .shard_owner_of("stream", STREAM, shard)
            .await
            .expect("owner");
        match cluster
            .whole_replay_of_shard(&leader, STREAM, shard, Duration::from_secs(2), budget)
            .await
        {
            Ok(events) if !events.is_empty() => {
                return events.iter().map(|e| e.payload.to_vec()).collect();
            }
            Ok(_) => last = format!("{leader} replayed nothing for shard {shard}"),
            Err(err) => last = format!("replay shard {shard} on {leader}: {err:#}"),
        }
        if std::time::Instant::now() >= deadline {
            panic!("shard {shard} never replayed; last: {last}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Publish `count` records through `via`, retrying a refusal until the shard
/// is served again: the new owner opens the shard a watch interval after it
/// is named.
async fn publish_until_served(
    cluster: &Cluster,
    via: &str,
    prefix: &str,
    count: usize,
) -> Vec<Vec<u8>> {
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    let mut sent = Vec::with_capacity(count);
    let mut i = 0usize;
    while sent.len() < count {
        let payload = format!("{prefix}-{i}").into_bytes();
        i += 1;
        match cluster.publish_via(via, STREAM, payload.clone()).await {
            Ok(()) => sent.push(payload),
            Err(err) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the shard was never served again after the move: {err}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    sent
}

/// Publish `count` records through `via`, acknowledged, and return them.
async fn publish_batch(cluster: &Cluster, via: &str, prefix: &str, count: usize) -> Vec<Vec<u8>> {
    let mut sent = Vec::with_capacity(count);
    for i in 0..count {
        let payload = format!("{prefix}-{i}").into_bytes();
        cluster
            .publish_via(via, STREAM, payload.clone())
            .await
            .expect("publish");
        sent.push(payload);
    }
    sent
}

fn missing(sent: &[Vec<u8>], got: &[Vec<u8>]) -> Vec<String> {
    let got: BTreeSet<&[u8]> = got.iter().map(Vec::as_slice).collect();
    sent.iter()
        .filter(|payload| !got.contains(payload.as_slice()))
        .map(|payload| String::from_utf8_lossy(payload).into_owned())
        .collect()
}

/// **Drain.** An unreplicated durable shard --
/// the case where nothing else holds the log -- is handed to another broker
/// when its owner drains, and every record acknowledged before the drain is
/// readable from the new owner.
#[serial]
#[tokio::test]
async fn a_drained_broker_hands_its_shard_over_with_every_record() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let sent = publish_batch(&cluster, &owner, "before", 20).await;

    cluster.drain_node(&owner).await.expect("drain");
    cluster
        .drain_until_empty(&owner, 1, Duration::from_secs(60))
        .await
        .expect("the shard should move off the draining broker");

    let new_owner = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_owner, owner);
    let got = replay_from(&cluster, &new_owner, Duration::from_secs(30)).await;
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records acknowledged before the drain are missing from {new_owner}: {lost:?}",
    );

    // The drained broker leads nothing and follows nothing: it can leave.
    let owners = cluster.shard_owners().await.expect("owners");
    assert!(owners.values().all(|leader| leader != &owner));
    cluster.shutdown().await;
}

/// **Nothing acknowledged during a move is lost.** Publishes keep arriving
/// while the shard is staged, fenced, and cut over. Some are refused in the
/// window between the fence and the new owner opening -- that is the
/// handoff, and the client sees an error rather than a silent drop -- but
/// every publish that was acknowledged is on the new owner.
#[serial]
#[tokio::test]
async fn records_acknowledged_during_a_move_survive_it() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");

    cluster.drain_node(&owner).await.expect("drain");

    // Publish by turns through the draining owner and through the other
    // broker. The owner is the path the fence exists for: a client connected
    // to it keeps sending until the owner has been told to stop, and every
    // publish it acknowledges after the successor takes over would be lost.
    // The other broker forwards to whichever leader its routing view names,
    // which is the path a client with a seed list takes.
    let mut acknowledged = Vec::new();
    let mut refused = 0usize;
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
    let mut i = 0usize;
    loop {
        cluster.place_shards().await;
        let payload = format!("during-{i}").into_bytes();
        let via = if i.is_multiple_of(2) { &owner } else { &other };
        i += 1;
        match cluster.publish_via(via, STREAM, payload.clone()).await {
            Ok(()) => acknowledged.push(payload),
            Err(_) => refused += 1,
        }
        let moved = cluster
            .owner(STREAM)
            .await
            .is_ok_and(|leader| leader != owner);
        if moved {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the shard never left {owner}; {} acknowledged, {refused} refused",
            acknowledged.len()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Placement names a Raft shard's new leader before the group hands
    // leadership over, so wait for the hand-over before checking the old
    // owner stopped writing.
    cluster
        .await_raft_leaders_placed(Duration::from_secs(60))
        .await
        .expect("the group to elect the new leader");
    // The fence itself: the old owner was told to stop and stopped before
    // the new owner was named, so nothing it is sent now is written locally.
    // It is refused until the owner's watch catches up, then forwarded.
    // Checked at once, while the owner's watch is still likely to be behind
    // the cut-over -- the window in which a leader that had not been fenced
    // would still be accepting.
    for _ in 0..10 {
        let forwarded_before = cluster
            .metric(&owner, "felix_broker_forwards_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
        let accepted = cluster
            .publish_via(&owner, STREAM, b"after-the-fence".to_vec())
            .await
            .is_ok();
        let forwarded_after = cluster
            .metric(&owner, "felix_broker_forwards_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
        assert!(
            !accepted || forwarded_after > forwarded_before,
            "{owner} wrote a publish locally after {} was named leader: the fence did not hold",
            cluster.owner(STREAM).await.unwrap_or_default(),
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // A few more after the cut-over, to show the new owner is serving.
    let after = publish_until_served(&cluster, &other, "after", 5).await;
    acknowledged.extend(after);
    println!(
        "{} acknowledged, {refused} refused across the move",
        acknowledged.len()
    );

    let new_owner = cluster.owner(STREAM).await.expect("owner");
    let got = replay_from(&cluster, &new_owner, Duration::from_secs(30)).await;
    let lost = missing(&acknowledged, &got);
    assert!(
        lost.is_empty(),
        "records acknowledged during the move are missing from {new_owner}: {lost:?}",
    );
    cluster.shutdown().await;
}

/// **Join.** A broker added to a running cluster
/// takes shards from the one carrying more than its share, without losing
/// what was already on them -- the 49/0 case from the perf session, on a
/// smaller scale.
#[serial]
#[tokio::test]
async fn a_broker_that_joins_takes_shards_from_an_overloaded_one() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 4)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();
    // Keyed publishes so every shard holds something.
    let mut sent = Vec::new();
    for i in 0..40 {
        let payload = format!("keyed-{i}").into_bytes();
        // Settled: a broker's lease is valid only after its first heartbeat,
        // which can land after its shards open, and a refusal until then is
        // right.
        cluster
            .publish_keyed_via_settled(
                &first,
                STREAM,
                format!("k{i}").as_bytes(),
                payload.clone(),
                felix_cluster::wait::budget(Duration::from_secs(30)),
            )
            .await
            .expect("publish");
        sent.push(payload);
    }

    let joined = cluster.add_node().await.expect("add a broker");

    // Two shards each is balanced; placement stops there.
    felix_cluster::wait::until(
        Duration::from_secs(90),
        "the joiner to lead half the shards",
        || async {
            cluster.place_shards_moving(2).await;
            match cluster.shard_owners().await {
                Ok(owners) => {
                    owners.values().filter(|leader| **leader == joined).count() == 2
                        && owners.values().filter(|leader| **leader == first).count() == 2
                }
                Err(_) => false,
            }
        },
    )
    .await
    .expect("rebalance");

    // And it stays there: no further moves once balanced.
    for _ in 0..5 {
        let outcome = cluster.place_shards_moving(2).await;
        assert_eq!(
            outcome.moved, 0,
            "placement kept moving after balance: {outcome:?}"
        );
    }

    // Every record is readable through the cluster, whichever broker leads
    // its shard now.
    let mut got = Vec::new();
    for shard in 0..4 {
        got.extend(replay_shard_from(&cluster, shard).await);
    }
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost across the rebalance: {lost:?}"
    );
    cluster.shutdown().await;
}

/// **A destination that dies mid-transfer never takes the shard.** The move
/// is staged onto it, it is killed before the cut-over, and the shard ends up
/// on a broker that holds the log -- with every record still there.
#[serial]
#[tokio::test]
async fn a_destination_that_dies_mid_transfer_does_not_take_the_shard() {
    if felix_cluster::skip_on_raft_retire_with_hybrid(
        "hybrid-only: stages a successor replica and kills it before the cut-over; a Raft add has no staged successor to lose, and a dead joiner leaves the grown set without a majority",
    ) {
        return;
    }
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 3,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let sent = publish_batch(&cluster, &owner, "held", 10).await;

    cluster.drain_node(&owner).await.expect("drain");

    // Step once: the successor is staged as a replica.
    let key = format!(
        "stream/{}/{}/{STREAM}/0",
        cluster.tenant_id, cluster.namespace
    );
    let successor = felix_cluster::wait::until_some(
        Duration::from_secs(30),
        "a successor to be staged",
        || async {
            cluster.place_shards().await;
            cluster
                .shard_successors()
                .await
                .ok()
                .and_then(|successors| successors.get(&key).cloned().flatten())
        },
    )
    .await
    .expect("staged");
    assert_ne!(successor, owner);

    // The destination dies before it leads.
    cluster
        .stop_node(&successor)
        .await
        .expect("stop the successor");

    cluster
        .drain_until_empty(&owner, 1, Duration::from_secs(90))
        .await
        .expect("the shard should still leave the draining broker");
    let new_owner = cluster.owner(STREAM).await.expect("owner");
    assert_ne!(new_owner, owner);
    assert_ne!(new_owner, successor, "a dead destination must not lead");

    let got = replay_from(&cluster, &new_owner, Duration::from_secs(30)).await;
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost when the destination died: {lost:?}"
    );
    cluster.shutdown().await;
}

/// **Concurrent placement changes converge.** One broker drains while another
/// joins, with several moves in flight at once; everything ends up on live
/// brokers, nothing on the drained one, and every record is still readable.
#[serial]
#[tokio::test]
async fn a_drain_and_a_join_at_the_same_time_converge() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 4)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();
    let mut sent = Vec::new();
    for i in 0..40 {
        let payload = format!("keyed-{i}").into_bytes();
        // Settled: a broker's lease is valid only after its first heartbeat,
        // which can land after its shards open, and a refusal until then is
        // right.
        cluster
            .publish_keyed_via_settled(
                &first,
                STREAM,
                format!("k{i}").as_bytes(),
                payload.clone(),
                felix_cluster::wait::budget(Duration::from_secs(30)),
            )
            .await
            .expect("publish");
        sent.push(payload);
    }

    let draining = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &first)
        .expect("two brokers");
    cluster.drain_node(&draining).await.expect("drain");
    let joined = cluster.add_node().await.expect("add a broker");

    cluster
        .drain_until_empty(&draining, 3, Duration::from_secs(120))
        .await
        .expect("the draining broker should end up leading nothing");
    let owners = cluster.shard_owners().await.expect("owners");
    assert_eq!(owners.len(), 4);
    assert!(owners.values().all(|leader| leader != &draining));
    assert!(
        owners.values().any(|leader| leader == &joined),
        "the joiner took nothing: {owners:?}"
    );

    let mut got = Vec::new();
    for shard in 0..4 {
        got.extend(replay_shard_from(&cluster, shard).await);
    }
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost across the concurrent changes: {lost:?}"
    );
    cluster.shutdown().await;
}

/// **The switch-over is quick.** From the fence to the destination accepting a
/// publish is well under a second, with every broker loop on the production
/// 2 s interval and placement on a timer too slow to be what moves it. Only
/// the wakes -- the long-polled assignment feed, the feed and replication
/// woken on a change, placement woken by the reports -- can make it that fast.
#[serial]
#[tokio::test]
async fn a_move_switches_over_in_well_under_a_second() {
    if felix_cluster::skip_on_raft_retire_with_hybrid(
        "hybrid-only: times the fence-to-serve switch-over, which a Raft move does not have; it hands over by election",
    ) {
        return;
    }
    use felix_controlplane_service::model::ShardState;

    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        sync_interval_ms: 2_000,
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let sent = publish_batch(&cluster, &owner, "before", 20).await;

    cluster.run_placement(Duration::from_secs(60));
    cluster.drain_node(&owner).await.expect("drain");
    // Stages the move. Everything after is driven by the reports.
    cluster.place_shards().await;

    // Read straight from the store: an HTTP round trip per check would blur
    // the moment the fence lands.
    let store = &cluster.control_plane.as_ref().expect("control plane").store;
    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    let fenced_at = loop {
        let draining = store
            .list_shard_assignments()
            .await
            .expect("assignments")
            .into_iter()
            .any(|a| a.key.stream == STREAM && matches!(a.state, ShardState::Draining));
        if draining {
            break std::time::Instant::now();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the move never reached its fence"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    };

    let mut attempt = 0usize;
    let switch_over = loop {
        attempt += 1;
        let payload = format!("after-{attempt}").into_bytes();
        if cluster
            .publish_via(&destination, STREAM, payload)
            .await
            .is_ok()
            && cluster
                .owner(STREAM)
                .await
                .is_ok_and(|now| now == destination)
        {
            break fenced_at.elapsed();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{destination} never took the shard over"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let measured = cluster
        .metric(&destination, "felix_broker_shard_switchover_seconds_sum")
        .await
        .ok()
        .flatten();
    println!(
        "switch-over: {} ms from the fence to a publish accepted by {destination}; \
         the destination measured {measured:?} s",
        switch_over.as_millis()
    );
    assert!(
        switch_over < felix_cluster::wait::budget(Duration::from_millis(1_000)),
        "the switch-over took {} ms",
        switch_over.as_millis()
    );
    assert!(
        measured.is_some(),
        "{destination} did not record the switch-over it served"
    );

    let got = replay_from(&cluster, &destination, Duration::from_secs(30)).await;
    let lost = missing(&sent, &got);
    assert!(lost.is_empty(), "records lost across the move: {lost:?}");
    cluster.shutdown().await;
}

/// Publish through `via` on one long-lived connection until `stop` is set, and
/// say what was acknowledged and what was refused.
///
/// One connection rather than one per publish: the switch-over lasts tens of
/// milliseconds, and a publisher that spends most of its time connecting would
/// rarely have a publish in flight during it.
async fn publish_until(
    cluster: &Cluster,
    via: &str,
    prefix: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> (Vec<Vec<u8>>, Vec<String>) {
    let node = cluster.node(via).expect("node");
    let client = felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");
    let publisher = client.publisher().await.expect("publisher");
    let mut acknowledged = Vec::new();
    let mut refused = Vec::new();
    let mut i = 0usize;
    while !stop.load(std::sync::atomic::Ordering::Acquire) {
        let payload = format!("{prefix}-{i}").into_bytes();
        i += 1;
        match publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                payload.clone(),
                felix_wire::AckMode::PerMessage,
            )
            .await
        {
            Ok(_) => acknowledged.push(payload),
            Err(err) => refused.push(format!("{err:#}")),
        }
        // Paced, so eight publishers stay under what a debug broker's publish
        // queue takes and every refusal left is about the move.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    (acknowledged, refused)
}

/// Every record `node_id` holds for the stream, replayed from the start.
///
/// The replay starts right after a move, while the new leader may still be
/// opening its term, and a broker that cannot serve the history yet resets
/// the event stream (`NOT_SERVING_HERE`). That reset asks for a fresh
/// subscribe, so the whole replay is read again from offset 0 rather than
/// resumed, and the offsets of the replay that is kept are checked so that a
/// hole fails here rather than showing up as a lost record.
async fn read_whole_log(cluster: &Cluster, node_id: &str) -> Vec<Vec<u8>> {
    let events = cluster
        .whole_replay_on(
            node_id,
            STREAM,
            Duration::from_secs(2),
            Duration::from_secs(60),
        )
        .await
        .unwrap_or_else(|err| panic!("no whole replay from {node_id}: {err:#}"));
    let offsets: Vec<(u64, u64)> = events
        .iter()
        .map(|event| {
            let offset = event.offset.expect("a durable stream delivers offsets");
            (offset, event.skipped_before)
        })
        .collect();
    felix_cluster::replay::assert_offsets_account_for_skips(
        &offsets,
        &format!("{node_id}'s replay"),
    );
    events.iter().map(|e| e.payload.to_vec()).collect()
}

/// Four publishers through `via` at once, so some are always in flight.
async fn publish_through(
    cluster: &Cluster,
    via: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> (Vec<Vec<u8>>, Vec<String>) {
    let prefixes: Vec<String> = (0..4).map(|n| format!("via-{via}-{n}")).collect();
    let (a, b, c, d) = tokio::join!(
        publish_until(cluster, via, &prefixes[0], stop),
        publish_until(cluster, via, &prefixes[1], stop),
        publish_until(cluster, via, &prefixes[2], stop),
        publish_until(cluster, via, &prefixes[3], stop),
    );
    let mut acknowledged = Vec::new();
    let mut refused = Vec::new();
    for (sent, failed) in [a, b, c, d] {
        acknowledged.extend(sent);
        refused.extend(failed);
    }
    (acknowledged, refused)
}

/// **No publish is refused during a move.** Publishers keep going through a
/// whole move, through the old owner and through the destination.
/// Between the fence and the cut-over nobody serves the shard, and a publish
/// that lands then is held and sent on to the new owner rather than refused.
/// Every acknowledged record is on the new owner exactly once.
///
/// On Raft a hand-over closes the shard until the next term opens, so a
/// publish may be refused there, but only as retryable; the Raft contract is
/// checked by [`publishing_through_raft_moves`].
#[serial]
#[tokio::test]
async fn continuous_publishing_through_a_move_is_never_refused() {
    let config = ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        sync_interval_ms: 2_000,
        ..Default::default()
    };
    if felix_cluster::suite_runs_on_raft() {
        publishing_through_raft_moves(ClusterConfig {
            broker_env: vec![(
                "FELIX_DURABLE_FSYNC_MODE".to_string(),
                "periodic".to_string(),
            )],
            ..config
        })
        .await;
        return;
    }
    let cluster = Cluster::start(config).await.expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let stop = std::sync::atomic::AtomicBool::new(false);

    let mover = async {
        // Let both publishers get going before the move starts.
        tokio::time::sleep(Duration::from_millis(300)).await;
        cluster.run_placement(Duration::from_secs(60));
        cluster.drain_node(&owner).await.expect("drain");
        cluster.place_shards().await;
        let deadline =
            std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
        while !cluster
            .owner(STREAM)
            .await
            .is_ok_and(|now| now == destination)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the shard never moved"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Keep publishing past the cut-over, while the old owner's routes
        // may still be catching up.
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.store(true, std::sync::atomic::Ordering::Release);
    };
    let (_, (through_owner, refused_owner), (through_destination, refused_destination)) = tokio::join!(
        mover,
        publish_through(&cluster, &owner, &stop),
        publish_through(&cluster, &destination, &stop),
    );
    let refused: Vec<String> = refused_owner
        .into_iter()
        .chain(refused_destination)
        .collect();

    let held = cluster
        .metric(&owner, "felix_broker_shard_move_held_total")
        .await
        .ok()
        .flatten()
        .unwrap_or(0.0)
        + cluster
            .metric(&destination, "felix_broker_shard_move_held_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
    println!(
        "{} acknowledged through {owner}, {} through {destination}; {held} held across the move",
        through_owner.len(),
        through_destination.len(),
    );

    let acknowledged: Vec<Vec<u8>> = through_owner
        .into_iter()
        .chain(through_destination)
        .collect();
    let got = read_whole_log(&cluster, &destination).await;
    let lost = missing(&acknowledged, &got);
    assert!(
        lost.is_empty(),
        "{} acknowledged records lost, first {:?}",
        lost.len(),
        lost.iter().take(5).collect::<Vec<_>>(),
    );
    let mut seen = std::collections::BTreeMap::<&[u8], usize>::new();
    for payload in &got {
        *seen.entry(payload.as_slice()).or_default() += 1;
    }
    let duplicated: Vec<String> = seen
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(payload, count)| format!("{} x{count}", String::from_utf8_lossy(payload)))
        .collect();
    assert!(
        duplicated.is_empty(),
        "records stored twice: {duplicated:?}"
    );
    assert!(
        refused.is_empty(),
        "{} publishes were refused during the move: {:?}",
        refused.len(),
        refused.iter().take(5).collect::<Vec<_>>(),
    );
    cluster.shutdown().await;
}

/// [`continuous_publishing_through_a_move_is_never_refused`] for a `Quorum`
/// stream fsynced on commit, at RF 3 on four brokers: the leader's seat
/// moves to the fourth broker and back, so each hand-over meets a real
/// majority. RF 1 adds nothing here the `Leader` case does not cover: its
/// grown set is two voters either way.
#[serial]
#[tokio::test]
async fn continuous_quorum_publishing_through_a_raft_move_is_never_refused() {
    if !felix_cluster::suite_runs_on_raft() {
        return;
    }
    publishing_through_raft_moves(ClusterConfig {
        nodes: 4,
        streams: vec![StreamSpec::quorum(STREAM, 1, 3)],
        sync_interval_ms: 2_000,
        broker_env: vec![(
            "FELIX_DURABLE_FSYNC_MODE".to_string(),
            "on_commit".to_string(),
        )],
        ..Default::default()
    })
    .await;
}

/// **A planned Raft move never answers a publish with a final refusal and
/// loses or doubles nothing.** Eight cluster clients publish through the
/// leader and a broker outside the set while the leader's seat moves to that
/// broker and back, each move a membership change and a leadership hand-over.
///
/// The contract (#3330): a refusal the client is meant to retry is retried
/// and lands; an outcome-unknown answer is allowed and its record is in the
/// log at most once; no refusal is final. Every acknowledged record is in
/// the final leader's whole replay exactly once, and the offsets there
/// account for every entry Raft skipped.
async fn publishing_through_raft_moves(config: ClusterConfig) {
    let cluster = Cluster::start(config).await.expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let assignment = cluster
        .shard_assignments()
        .await
        .expect("assignments")
        .into_values()
        .find(|a| a.leader == owner)
        .expect("the shard's assignment");
    // `replicas` never holds the leader.
    let set = assignment.replicas.len() + 1;
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| *id != owner && !assignment.replicas.contains(id))
        .expect("a broker outside the set");
    let stop = std::sync::atomic::AtomicBool::new(false);

    let mover = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        // The leader's seat moves out of the set and back.
        for (to, leaving) in [(&destination, &owner), (&owner, &destination)] {
            raft_move_to(&cluster, to, leaving, set).await;
            // Keep publishing past the hand-over, while routes catch up.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        stop.store(true, std::sync::atomic::Ordering::Release);
    };
    let (_, through_owner, through_destination) = tokio::join!(
        mover,
        publish_through_on_raft(&cluster, &owner, &stop),
        publish_through_on_raft(&cluster, &destination, &stop),
    );
    let tally = through_owner.merge(through_destination);
    println!(
        "{} acknowledged, {} retryable refusals retried, {} outcome unknown, {} refused",
        tally.acknowledged.len(),
        tally.retried,
        tally.unknown.len(),
        tally.refused.len(),
    );

    // The shard is back on `owner`, so its whole log is there.
    let got = read_whole_log(&cluster, &owner).await;
    let mut copies = std::collections::BTreeMap::<&[u8], usize>::new();
    for payload in &got {
        *copies.entry(payload.as_slice()).or_default() += 1;
    }
    let count = |payload: &[u8]| copies.get(payload).copied().unwrap_or(0);
    let lost: Vec<String> = tally
        .acknowledged
        .iter()
        .filter(|payload| count(payload) == 0)
        .map(|payload| String::from_utf8_lossy(payload).into_owned())
        .collect();
    assert!(
        lost.is_empty(),
        "{} acknowledged records lost, first {:?}",
        lost.len(),
        lost.iter().take(5).collect::<Vec<_>>(),
    );
    let duplicated: Vec<String> = copies
        .iter()
        .filter(|(_, n)| **n > 1)
        .map(|(payload, n)| format!("{} x{n}", String::from_utf8_lossy(payload)))
        .collect();
    assert!(
        duplicated.is_empty(),
        "records stored twice (acknowledged, retried or outcome unknown): {duplicated:?}"
    );
    let unknown_landed = tally.unknown.iter().filter(|p| count(p) == 1).count();
    println!(
        "{unknown_landed} of {} outcome-unknown records are in the log",
        tally.unknown.len()
    );
    assert!(
        tally.refused.is_empty(),
        "{} publishes were refused for good during a planned move: {:?}",
        tally.refused.len(),
        tally.refused.iter().take(5).collect::<Vec<_>>(),
    );
    cluster.shutdown().await;
}

/// Move the leader's seat of the stream's one shard from `leaving` to `to`
/// through the operator API, and wait until `to` leads, the set is back to
/// `set` members without `leaving`, and the move has ended.
async fn raft_move_to(cluster: &Cluster, to: &str, leaving: &str, set: usize) {
    let step = cluster
        .start_move(STREAM, 0, to)
        .await
        .unwrap_or_else(|err| panic!("start the move to {to}: {err:#}"));
    assert_eq!(step, "raft_replace", "the move to {to}");
    felix_cluster::wait::until(
        Duration::from_secs(60),
        &format!("{to} to lead a set without {leaving}"),
        || async {
            cluster.place_shards().await;
            cluster.owner(STREAM).await.is_ok_and(|now| now == to)
                && cluster
                    .raft_target(STREAM, 0)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|members| {
                        members.len() == set
                            && members.contains_key(to)
                            && !members.contains_key(leaving)
                    })
        },
    )
    .await
    .expect("the move to hand over");
    cluster
        .await_raft_leaders_placed(Duration::from_secs(60))
        .await
        .expect("the group to elect the new leader");
    felix_cluster::wait::until(Duration::from_secs(30), "the move to end", || async {
        cluster.place_shards().await;
        cluster
            .shard_moves()
            .await
            .is_ok_and(|moves| moves["items"] == serde_json::json!([]))
    })
    .await
    .expect("the moves listing to empty");
}

/// What publishers saw during Raft moves.
#[derive(Default)]
struct Tally {
    acknowledged: Vec<Vec<u8>>,
    /// Answered as possibly applied; never sent again.
    unknown: Vec<Vec<u8>>,
    /// Final refusals, and retryable ones that never landed, described.
    refused: Vec<String>,
    /// Retryable refusals that were sent again.
    retried: usize,
}

impl Tally {
    fn merge(mut self, other: Tally) -> Tally {
        self.acknowledged.extend(other.acknowledged);
        self.unknown.extend(other.unknown);
        self.refused.extend(other.refused);
        self.retried += other.retried;
        self
    }
}

/// What a failed publish means for its record, as the client classifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Answer {
    /// Nothing was applied, and the client is meant to send it again.
    Retryable,
    /// It may have been applied, so sending it again could store it twice.
    OutcomeUnknown,
    /// The client is told sending it again fails the same way.
    Final,
}

/// The refusals a Raft hand-over answers with reach the client classed as
/// the contract needs, with the class a broker sends by default.
#[test]
fn hand_over_refusals_are_retryable_and_lost_terms_outcome_unknown() {
    use felix_wire::ErrorCode;
    let refusal = |code: ErrorCode| {
        let retry = code.default_retry();
        anyhow::Error::from(felix_client::BrokerError::new(code, retry, "refused"))
    };
    for code in [
        ErrorCode::ShardUnavailable,
        ErrorCode::NotLeader,
        ErrorCode::Draining,
        ErrorCode::StaleWidth,
        ErrorCode::NotFound,
        ErrorCode::Overloaded,
    ] {
        assert_eq!(answer(&refusal(code.clone())), Answer::Retryable, "{code}");
    }
    for code in [
        ErrorCode::LeadershipLost,
        ErrorCode::Unacknowledged,
        ErrorCode::QuorumTimeout,
    ] {
        assert_eq!(
            answer(&refusal(code.clone())),
            Answer::OutcomeUnknown,
            "{code}"
        );
    }
    assert_eq!(answer(&refusal(ErrorCode::Forbidden)), Answer::Final);
    let lost = anyhow::Error::from(felix_client::AckLost {
        reason: "connection closed".to_string(),
    });
    assert_eq!(answer(&lost), Answer::OutcomeUnknown);
    let uncoded = anyhow::anyhow!("publisher closed").context("publish");
    assert_eq!(answer(&uncoded), Answer::Final);
}

fn answer(err: &anyhow::Error) -> Answer {
    if felix_client::outcome_unknown(err) {
        return Answer::OutcomeUnknown;
    }
    match err
        .chain()
        .find_map(|cause| cause.downcast_ref::<felix_client::BrokerError>())
    {
        Some(broker) => match broker.retry {
            felix_wire::RetryClass::Retry
            | felix_wire::RetryClass::RetryAfter
            | felix_wire::RetryClass::Redirect => Answer::Retryable,
            felix_wire::RetryClass::OutcomeUnknown => Answer::OutcomeUnknown,
            felix_wire::RetryClass::Fatal => Answer::Final,
        },
        // A write lost after the send is an `AckLost`, caught above; the
        // client documents no uncoded error as safe to send again.
        None => Answer::Final,
    }
}

fn describe(err: &anyhow::Error) -> String {
    match err
        .chain()
        .find_map(|cause| cause.downcast_ref::<felix_client::BrokerError>())
    {
        Some(broker) => format!(
            "{} ({}, reason {:?}): {err:#}",
            broker.code,
            broker.retry.as_str(),
            broker.reason(),
        ),
        None => format!("{err:#}"),
    }
}

/// Publish through a cluster client whose entry broker is `via` until `stop`
/// is set. A retryable refusal is sent again, past `stop` if need be, until
/// it lands or a minute has passed.
async fn publish_on_raft_until(
    cluster: &Cluster,
    via: &str,
    prefix: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> Tally {
    let node = cluster.node(via).expect("node");
    let client = felix_cluster::client::connect_cluster(
        &[node.client_addr],
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect");
    let mut tally = Tally::default();
    let mut i = 0usize;
    while !stop.load(std::sync::atomic::Ordering::Acquire) {
        let payload = format!("{prefix}-{i}").into_bytes();
        i += 1;
        let gives_up =
            std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
        loop {
            let err = match client
                .publish(
                    &cluster.tenant_id,
                    &cluster.namespace,
                    STREAM,
                    payload.clone(),
                    felix_wire::AckMode::PerMessage,
                )
                .await
            {
                Ok(_) => {
                    tally.acknowledged.push(payload);
                    break;
                }
                Err(err) => err,
            };
            match answer(&err) {
                Answer::Retryable if std::time::Instant::now() < gives_up => {
                    tally.retried += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Answer::Retryable => {
                    tally.refused.push(format!(
                        "retried for a minute, never landed: {}",
                        describe(&err)
                    ));
                    break;
                }
                Answer::OutcomeUnknown => {
                    tally.unknown.push(payload);
                    break;
                }
                Answer::Final => {
                    tally.refused.push(describe(&err));
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    tally
}

/// Four Raft publishers through `via` at once, so some are always in flight.
async fn publish_through_on_raft(
    cluster: &Cluster,
    via: &str,
    stop: &std::sync::atomic::AtomicBool,
) -> Tally {
    let prefixes: Vec<String> = (0..4).map(|n| format!("via-{via}-{n}")).collect();
    let (a, b, c, d) = tokio::join!(
        publish_on_raft_until(cluster, via, &prefixes[0], stop),
        publish_on_raft_until(cluster, via, &prefixes[1], stop),
        publish_on_raft_until(cluster, via, &prefixes[2], stop),
        publish_on_raft_until(cluster, via, &prefixes[3], stop),
    );
    a.merge(b).merge(c).merge(d)
}

/// **A `Quorum` publish is not held by the destination's copy.** A stream with
/// one replica asked for is moving, and its destination has stopped answering
/// mid-copy. The destination is not part of the replica set the stream asked
/// for, so publishes keep being acknowledged by the leader, which is that
/// whole set, rather than waiting on a copy that is not moving.
#[serial]
#[tokio::test]
async fn a_quorum_publish_during_a_copy_is_not_held_by_it() {
    if felix_cluster::skip_on_raft_retire_with_hybrid(
        "hybrid-only: a staged copy that stalls while a Quorum publish is acknowledged by the leader alone; a Raft add votes in the group",
    ) {
        return;
    }
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::quorum(STREAM, 1, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let mut acknowledged = publish_batch(&cluster, &owner, "before", 5).await;

    // The copy stalls: the destination is suspended before it is staged.
    cluster.pause_node(&destination).expect("pause");
    cluster.drain_node(&owner).await.expect("drain");
    cluster.place_shards().await;
    let store = &cluster.control_plane.as_ref().expect("control plane").store;
    let staged = store
        .list_shard_assignments()
        .await
        .expect("assignments")
        .into_iter()
        .any(|a| a.key.stream == STREAM && a.successor.as_deref() == Some(destination.as_str()));
    assert!(staged, "the move was not staged toward {destination}");

    // Under the peer pool's 2 s handshake timeout: a publish held by the
    // dial to the suspended destination takes at least that.
    let budget = felix_cluster::wait::budget(Duration::from_millis(1500));
    let mut slowest = Duration::ZERO;
    for i in 0..20 {
        let payload = format!("during-{i}").into_bytes();
        let started = std::time::Instant::now();
        cluster
            .publish_via(&owner, STREAM, payload.clone())
            .await
            .expect("a Quorum publish during the copy");
        slowest = slowest.max(started.elapsed());
        acknowledged.push(payload);
    }
    println!("slowest Quorum publish during the stalled copy: {slowest:?}");
    // The quorum timeout is 5 s. A publish that waited for the copy would
    // have run it out, and one that waited for the pass dialling the
    // destination would have taken the dial's timeout.
    assert!(
        slowest < budget,
        "a Quorum publish took {slowest:?} while the destination was copying",
    );

    // Suspended this long, the destination has dropped out of the control
    // plane's view and the move is abandoned; finishing moves is what the
    // tests above cover. What matters here is that every acknowledged record
    // is on whoever leads the shard.
    cluster.resume_node(&destination).expect("resume");
    let got = replay_shard_from(&cluster, 0).await;
    let lost = missing(&acknowledged, &got);
    assert!(lost.is_empty(), "acknowledged records lost: {lost:?}");
    cluster.shutdown().await;
}

/// **A move finishes under steady writes.** The destination of a busy shard
/// is almost never exactly level with the leader, so a move that waited for
/// that could wait forever. Placement fences once the destination is within
/// the lag bound, the leader stops, and the drained report waits for the
/// rest; every acknowledged write is on the new owner.
#[serial]
#[tokio::test]
async fn a_move_completes_while_a_publisher_keeps_writing() {
    use std::sync::atomic::{AtomicBool, Ordering};

    use felix_controlplane_service::cluster::placement::MovePolicy;

    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let other = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let mut acknowledged = publish_batch(&cluster, &owner, "before", 200).await;

    let moved = AtomicBool::new(false);
    // Four writers with no pause between publishes, so the log grows between
    // any two reports.
    // Four writers on their own connections, batches back to back, so the
    // log grows between any two reports. Half write through the owner, half
    // through the other broker, which forwards.
    let writer = |writer: usize| {
        let (cluster, moved) = (&cluster, &moved);
        let via = if writer.is_multiple_of(2) {
            owner.clone()
        } else {
            other.clone()
        };
        async move {
            let mut acknowledged = Vec::new();
            let mut i = 0usize;
            while !moved.load(Ordering::Relaxed) {
                let Ok(client) = cluster.client_on(&via).await else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                };
                let Ok(publisher) = client.publisher().await else {
                    continue;
                };
                while !moved.load(Ordering::Relaxed) {
                    let batch: Vec<Vec<u8>> = (0..20)
                        .map(|n| format!("steady-{writer}-{i}-{n}").into_bytes())
                        .collect();
                    i += 1;
                    let published = publisher
                        .publish_batch(
                            &cluster.tenant_id,
                            &cluster.namespace,
                            STREAM,
                            batch.clone(),
                            felix_wire::AckMode::PerMessage,
                        )
                        .await;
                    match published {
                        Ok(_) => acknowledged.extend(batch),
                        // Refused across the switch-over: a new connection
                        // finds the new route.
                        Err(_) => break,
                    }
                }
            }
            acknowledged
        }
    };
    let mover = async {
        cluster.drain_node(&owner).await.expect("drain");
        let deadline =
            std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(60));
        loop {
            cluster
                .control_plane
                .as_ref()
                .expect("control plane")
                .place_shards_with(MovePolicy::default())
                .await;
            if cluster
                .owner(STREAM)
                .await
                .is_ok_and(|leader| leader != owner)
            {
                break;
            }
            if std::time::Instant::now() >= deadline {
                moved.store(true, Ordering::Relaxed);
                panic!("the shard never left {owner} while writes kept arriving");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // A little more writing on the new owner before the writers stop.
        tokio::time::sleep(Duration::from_millis(500)).await;
        moved.store(true, Ordering::Relaxed);
    };
    let (a, b, c, d, ()) = tokio::join!(writer(0), writer(1), writer(2), writer(3), mover);
    for batch in [a, b, c, d] {
        acknowledged.extend(batch);
    }
    println!("{} acknowledged across the move", acknowledged.len());

    let new_owner = cluster.owner(STREAM).await.expect("owner");
    assert_eq!(new_owner, other);
    let got = read_whole_log(&cluster, &new_owner).await;
    let lost = missing(&acknowledged, &got);
    assert!(
        lost.is_empty(),
        "records acknowledged during the move are missing from {new_owner}: {} of {}",
        lost.len(),
        acknowledged.len()
    );
    cluster.shutdown().await;
}

/// **A move that cannot copy gives its slot back.** The destination stays
/// live -- it heartbeats -- but the leader cannot reach it, so it never gets
/// close. Past the move timeout the staging is undone in one write, the
/// leader keeps serving throughout, and once the destination is reachable
/// again the next attempt finishes.
#[serial]
#[tokio::test]
async fn a_move_that_cannot_copy_is_abandoned_after_its_timeout() {
    if felix_cluster::skip_on_raft_retire_with_hybrid(
        "hybrid-only: abandons a staged move after its timeout; a Raft move has no staging to undo by timeout",
    ) {
        return;
    }
    use felix_controlplane_service::cluster::placement::MovePolicy;

    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");
    let mut sent = publish_batch(&cluster, &owner, "before", 20).await;

    cluster.partition_node(&destination).expect("partition");
    cluster.drain_node(&owner).await.expect("drain");
    let policy = MovePolicy {
        timeout_millis: Some(3_000),
        ..MovePolicy::default()
    };
    let store = &cluster.control_plane.as_ref().expect("control plane").store;
    let assignment = || async {
        store
            .list_shard_assignments()
            .await
            .expect("assignments")
            .into_iter()
            .find(|a| a.key.stream == STREAM)
            .expect("assigned")
    };

    let deadline = std::time::Instant::now() + felix_cluster::wait::budget(Duration::from_secs(30));
    let mut staged_at = None;
    let abandoned = loop {
        cluster
            .control_plane
            .as_ref()
            .expect("control plane")
            .place_shards_with(policy.clone())
            .await;
        let now = assignment().await;
        if now.successor.is_some() {
            staged_at.get_or_insert(now.generation);
        } else if let Some(generation) = staged_at {
            break (generation, now);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the move was never abandoned: {now:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let (staged_generation, undone) = abandoned;
    assert_eq!(undone.leader, owner, "the leader never changed");
    assert!(
        undone.replicas.is_empty(),
        "the partial copy is dropped: {undone:?}"
    );
    assert_eq!(
        undone.generation,
        staged_generation + 1,
        "undone in one write, without a fence"
    );
    assert!(
        undone.move_started_at_millis.is_some(),
        "the shard keeps its place at the back of the queue"
    );
    sent.extend(publish_batch(&cluster, &owner, "while-stuck", 5).await);

    cluster.heal_partitions().expect("heal");
    cluster
        .drain_until_empty(&owner, 1, Duration::from_secs(60))
        .await
        .expect("once reachable, the shard moves");
    let got = replay_from(&cluster, &destination, Duration::from_secs(30)).await;
    let lost = missing(&sent, &got);
    assert!(
        lost.is_empty(),
        "records lost across the retried move: {lost:?}"
    );
    cluster.shutdown().await;
}

/// **A move onto a broker that just restarted does not wait for a catalog
/// refresh.** The broker comes back under the same id on new ports, so the
/// leader already knows the id and only a refresh tells it the new address.
/// The interval is ten seconds here and the leader has just refreshed, so the
/// move is quick only if the leader refreshes when the move wakes it.
#[serial]
#[tokio::test]
async fn a_move_onto_a_restarted_broker_does_not_wait_for_the_catalog_tick() {
    const SYNC_INTERVAL: Duration = Duration::from_secs(10);
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        sync_interval_ms: SYNC_INTERVAL.as_millis() as u64,
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let owner = cluster.owner(STREAM).await.expect("owner");
    let destination = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &owner)
        .expect("two brokers");

    // Restart once and wait for the owner's tick to pick up the new address,
    // so the second restart lands just after a refresh with most of an
    // interval before the next one.
    cluster.kill_node(&destination).expect("kill");
    cluster.restart_node(&destination).await.expect("restart");
    let first = cluster.node(&destination).expect("node").client_addr;
    felix_cluster::wait::until(
        SYNC_INTERVAL * 3,
        "the owner to learn the restarted broker's address",
        || advertises(&cluster, &owner, first),
    )
    .await
    .expect("the owner's tick refreshes its catalog");
    cluster.kill_node(&destination).expect("kill");
    cluster.restart_node(&destination).await.expect("restart");
    let second = cluster.node(&destination).expect("node").client_addr;
    assert!(
        !advertises(&cluster, &owner, second).await,
        "the owner refreshed during the restart, so this run proves nothing"
    );

    cluster.run_placement(Duration::from_secs(60));
    let started = std::time::Instant::now();
    cluster
        .start_move(STREAM, 0, &destination)
        .await
        .expect("start move");
    let deadline = started + felix_cluster::wait::budget(Duration::from_secs(30));
    while !cluster
        .owner(STREAM)
        .await
        .is_ok_and(|now| now == destination)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "{destination} never took the shard"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let took = started.elapsed();
    println!("move onto the restarted broker: {} ms", took.as_millis());
    assert!(
        took < felix_cluster::wait::budget(Duration::from_secs(3)),
        "the move took {} ms, as if it waited for the owner's catalog tick",
        took.as_millis()
    );
    cluster.shutdown().await;
}

/// Whether `via` tells clients where to reach `addr`: that list comes from
/// the same catalog fetch the broker forwards and ships with.
async fn advertises(cluster: &Cluster, via: &str, addr: std::net::SocketAddr) -> bool {
    let node = cluster.node(via).expect("node");
    let Ok(client) = felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    else {
        return false;
    };
    client
        .topology()
        .await
        .is_ok_and(|brokers| brokers.iter().any(|broker| broker.addr.parse() == Ok(addr)))
}
