//! A planned move carries the state that rides a shard: its consumer groups'
//! positions and dead letters, and its cache's counters.
//!
//! Those logs are shipped beside the shard's own log, and the control plane
//! cuts over on the leader's drained report. So the report must wait for them
//! too: a dead letter left behind is a record the new leader's group skips,
//! and a counter add left behind is an acknowledged add gone from the sum.
//!
//! Run with `cargo test -p felix-cluster --test queues handoff::`.
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use felix_cluster::{CacheSpec, Cluster, ClusterConfig, StreamSpec, wait};
use serial_test::serial;

const STREAM: &str = "jobs";
const CACHE: &str = "tallies";
const GROUP: &str = "workers";
const COUNTER: &str = "hits";

/// **A move off a live leader loses no group state and no counter add.** Group
/// acks and counter adds keep arriving while placement stages, fences and cuts
/// the shards over, so the last of them land right before the fence. On the
/// new owner no acknowledged record comes back, the dead-letter list is the
/// one the old leader had, and the counter is the sum of every acknowledged
/// add.
#[tokio::test]
#[serial]
async fn a_moved_shard_keeps_its_group_state_and_counters() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        caches: vec![CacheSpec::new(CACHE, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();

    wait::until(
        Duration::from_secs(30),
        "the first publish to land",
        || async {
            cluster
                .publish_via(&first, STREAM, b"poison".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("publish");
    for i in 0..40 {
        cluster
            .publish_via(&first, STREAM, format!("job-{i}").into_bytes())
            .await
            .expect("publish");
    }

    // Give the first record up. Everything else claimed on the way is
    // acknowledged, so the group's position moves too.
    let mut acked: HashSet<u64> = HashSet::new();
    let mut rounds = 0;
    let poison = loop {
        rounds += 1;
        assert!(rounds <= 20, "the record was never dead-lettered");
        let claimed = cluster
            .group_poll_records_via(&first, STREAM, 0, GROUP, 5)
            .await
            .expect("poll");
        for record in claimed {
            if record.payload.as_ref() == b"poison" {
                cluster
                    .group_nack_via(&first, STREAM, 0, GROUP, &record)
                    .await
                    .expect("nack");
            } else {
                cluster
                    .group_ack_via(&first, STREAM, 0, GROUP, record.offset)
                    .await
                    .expect("ack");
                acked.insert(record.offset);
            }
        }
        let dead = cluster
            .group_dead_letters_via(&first, STREAM, 0, GROUP)
            .await
            .expect("dead letters");
        if let [offset] = dead[..] {
            break offset;
        }
    };
    let dead_before = cluster
        .group_dead_letters_via(&first, STREAM, 0, GROUP)
        .await
        .expect("dead letters");
    assert_eq!(dead_before, vec![poison]);

    let mut added: i64 = 0;
    // An add that failed may still have landed (a timeout after the write),
    // so the sum is checked against both bounds.
    let mut unknown: i64 = 0;
    for _ in 0..10 {
        cluster
            .counter_add_via(&first, CACHE, COUNTER, 1)
            .await
            .expect("counter add");
        added += 1;
    }

    let joined = cluster.add_node().await.expect("add a broker");
    cluster.drain_node(&first).await.expect("drain");

    // Keep writing between placement steps, through the old leader, until it
    // leads nothing. Writes refused once the fence closes are fine; the ones
    // acknowledged before it are the ones the move has to carry.
    let deadline = tokio::time::Instant::now() + wait::budget(Duration::from_secs(120));
    loop {
        cluster.place_shards_moving(2).await;
        match cluster.counter_add_via(&first, CACHE, COUNTER, 1).await {
            Ok(_) => added += 1,
            Err(_) => unknown += 1,
        }
        if let Ok(claimed) = cluster
            .group_poll_records_via(&first, STREAM, 0, GROUP, 1)
            .await
        {
            for record in claimed {
                if cluster
                    .group_ack_via(&first, STREAM, 0, GROUP, record.offset)
                    .await
                    .is_ok()
                {
                    acked.insert(record.offset);
                }
            }
        }
        let owners = cluster.shard_owners().await.expect("owners");
        if owners.values().all(|leader| *leader == joined) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the shards never moved off {first}: {owners:?}"
        );
    }

    // Named owner and serving are different moments.
    let listed = wait::until(
        Duration::from_secs(30),
        "the new owner to serve the group",
        || async {
            cluster
                .group_dead_letters_via(&joined, STREAM, 0, GROUP)
                .await
                .is_ok()
        },
    )
    .await;
    assert!(listed.is_ok(), "{joined} never served the group");
    assert_eq!(
        cluster
            .group_dead_letters_via(&joined, STREAM, 0, GROUP)
            .await
            .expect("dead letters on the new owner"),
        dead_before,
        "the dead-letter list changed across the move",
    );

    let mut delivered = Vec::new();
    loop {
        let claimed = cluster
            .group_poll_records_via(&joined, STREAM, 0, GROUP, 100)
            .await
            .expect("poll the new owner");
        if claimed.is_empty() {
            break;
        }
        for record in claimed {
            cluster
                .group_ack_via(&joined, STREAM, 0, GROUP, record.offset)
                .await
                .expect("ack on the new owner");
            delivered.push(record.offset);
        }
    }
    let redelivered: Vec<u64> = delivered
        .iter()
        .copied()
        .filter(|offset| acked.contains(offset) || *offset == poison)
        .collect();
    assert!(
        redelivered.is_empty(),
        "the new owner handed out records the group had finished: {redelivered:?}",
    );

    let sum = cluster
        .counter_get_via(&joined, CACHE, COUNTER)
        .await
        .expect("counter on the new owner")
        .unwrap_or(0);
    assert!(
        (added..=added + unknown).contains(&sum),
        "the counter reads {sum}, but {added} adds were acknowledged ({unknown} unanswered)",
    );
    cluster.shutdown().await;
}

/// What one writer saw: how many of its writes were acknowledged, and the
/// errors of the ones that were not.
#[derive(Default)]
struct Tally {
    ok: usize,
    refused: Vec<String>,
}

async fn connect(cluster: &Cluster, via: &str) -> felix_client::Client {
    let node = cluster.node(via).expect("node");
    felix_cluster::client::connect(
        node.client_addr,
        &cluster.tenant_id,
        &cluster.client_token(),
    )
    .await
    .expect("connect")
}

/// Put fresh keys through `via` until `stop`, deleting every fourth one again.
/// Returns what each surviving key should read and the keys deleted.
async fn cache_writes_until(
    cluster: &Cluster,
    via: &str,
    stop: &AtomicBool,
) -> (Tally, Vec<(String, Vec<u8>)>, Vec<String>) {
    let client = connect(cluster, via).await;
    let (tenant, namespace) = (&cluster.tenant_id, &cluster.namespace);
    let mut tally = Tally::default();
    let mut kept = Vec::new();
    let mut deleted = Vec::new();
    let mut i = 0usize;
    while !stop.load(Ordering::Acquire) {
        let key = format!("{via}-{i}");
        let value = format!("value-{i}").into_bytes();
        i += 1;
        match client
            .cache_put(tenant, namespace, CACHE, &key, value.clone().into(), None)
            .await
        {
            Ok(()) => tally.ok += 1,
            Err(err) => {
                tally.refused.push(format!("put {key}: {err:#}"));
                tokio::time::sleep(Duration::from_millis(2)).await;
                continue;
            }
        }
        if i.is_multiple_of(4) {
            match client.cache_delete(tenant, namespace, CACHE, &key).await {
                Ok(_) => {
                    tally.ok += 1;
                    deleted.push(key);
                }
                Err(err) => tally.refused.push(format!("delete {key}: {err:#}")),
            }
        } else {
            kept.push((key, value));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    (tally, kept, deleted)
}

async fn counter_adds_until(cluster: &Cluster, via: &str, stop: &AtomicBool) -> Tally {
    let client = connect(cluster, via).await;
    let mut tally = Tally::default();
    while !stop.load(Ordering::Acquire) {
        match client
            .counter_add(&cluster.tenant_id, &cluster.namespace, CACHE, COUNTER, 1)
            .await
        {
            Ok(_) => tally.ok += 1,
            Err(err) => tally.refused.push(format!("counter add: {err:#}")),
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    tally
}

async fn publish_jobs_until(
    cluster: &Cluster,
    via: &str,
    stop: &AtomicBool,
) -> (Tally, Vec<String>) {
    let client = connect(cluster, via).await;
    let publisher = client.publisher().await.expect("publisher");
    let mut tally = Tally::default();
    let mut published = Vec::new();
    let mut i = 0usize;
    while !stop.load(Ordering::Acquire) {
        let payload = format!("job-{i}");
        i += 1;
        match publisher
            .publish(
                &cluster.tenant_id,
                &cluster.namespace,
                STREAM,
                payload.clone().into_bytes(),
                felix_wire::AckMode::PerMessage,
            )
            .await
        {
            Ok(_) => {
                tally.ok += 1;
                published.push(payload);
            }
            Err(err) => tally.refused.push(format!("publish: {err:#}")),
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    (tally, published)
}

/// Poll and ack through a cluster client until `stop`, then say which
/// payloads were finished. A record handed out again after its ack fails the
/// test on the spot.
async fn consume_until(
    group: &felix_client::ShardedGroup,
    stop: &AtomicBool,
) -> (Tally, HashSet<String>) {
    let mut tally = Tally::default();
    let mut acked: HashSet<u64> = HashSet::new();
    let mut finished = HashSet::new();
    while !stop.load(Ordering::Acquire) {
        let batch = match group.poll(5).await {
            Ok(batch) => {
                tally.ok += 1;
                batch
            }
            Err(err) => {
                tally.refused.push(format!("poll: {err:#}"));
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
        };
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for claimed in batch {
            let offset = claimed.record.offset;
            assert!(
                !acked.contains(&offset),
                "offset {offset} was handed out again after its ack"
            );
            match group.ack(&claimed).await {
                Ok(()) => {
                    tally.ok += 1;
                    acked.insert(offset);
                    finished.insert(String::from_utf8_lossy(&claimed.record.payload).to_string());
                }
                Err(err) => tally.refused.push(format!("ack {offset}: {err:#}")),
            }
        }
    }
    (tally, finished)
}

/// **No write of any kind is refused during a move.** Cache puts and deletes,
/// counter adds, and a consumer group's polls and acks keep going through both
/// brokers while the stream's and the cache's shards move. Between a fence and
/// its cut-over nobody serves the shard, and each of those writes is held and
/// sent on to the new owner, as a publish is. Afterwards every acknowledged
/// cache write reads back, the counter is exactly the number of acknowledged
/// adds, and the group finishes every record once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn writes_of_every_kind_through_a_move_are_never_refused() {
    let mut cluster = Cluster::start(ClusterConfig {
        nodes: 1,
        streams: vec![StreamSpec::new(STREAM, 1)],
        caches: vec![CacheSpec::new(CACHE, 1)],
        sync_interval_ms: 2_000,
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.node_ids()[0].clone();
    wait::until(
        Duration::from_secs(30),
        "the stream to take a publish",
        || async {
            cluster
                .publish_via(&first, STREAM, b"warm-up".to_vec())
                .await
                .is_ok()
        },
    )
    .await
    .expect("publish");
    let joined = cluster.add_node().await.expect("add a broker");

    let client = Arc::new(
        felix_cluster::client::connect_cluster(
            &cluster.broker_addrs(),
            &cluster.tenant_id,
            &cluster.client_token(),
        )
        .await
        .expect("connect a cluster client"),
    );
    let group = client
        .group_sharded(&cluster.tenant_id, &cluster.namespace, STREAM, GROUP)
        .await
        .expect("sharded group");

    let stop = AtomicBool::new(false);
    let mover = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        // Two shards move one after the other; the tick starts the second.
        cluster.run_placement(Duration::from_secs(1));
        cluster.drain_node(&first).await.expect("drain");
        cluster.place_shards().await;
        let deadline = std::time::Instant::now() + wait::budget(Duration::from_secs(60));
        loop {
            let owners = cluster.shard_owners().await.expect("owners");
            if owners.values().all(|leader| *leader == joined) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the shards never moved: {owners:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Past the cut-over too, while the old owner's routes catch up.
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop.store(true, Ordering::Release);
    };
    let (
        (),
        (puts_first, kept_first, deleted_first),
        (puts_joined, kept_joined, deleted_joined),
        adds_first,
        adds_joined,
        (published, jobs),
        (consumed, finished),
    ) = tokio::join!(
        mover,
        cache_writes_until(&cluster, &first, &stop),
        cache_writes_until(&cluster, &joined, &stop),
        counter_adds_until(&cluster, &first, &stop),
        counter_adds_until(&cluster, &joined, &stop),
        publish_jobs_until(&cluster, &first, &stop),
        consume_until(&group, &stop),
    );

    let refused: Vec<&String> = [
        &puts_first,
        &puts_joined,
        &adds_first,
        &adds_joined,
        &published,
        &consumed,
    ]
    .iter()
    .flat_map(|tally| &tally.refused)
    .collect();
    let mut held = 0.0;
    for node in [&first, &joined] {
        held += cluster
            .metric(node, "felix_broker_shard_move_held_total")
            .await
            .ok()
            .flatten()
            .unwrap_or(0.0);
    }
    println!(
        "{} cache writes, {} counter adds, {} publishes, {} group operations acknowledged; \
         {held} held across the move",
        puts_first.ok + puts_joined.ok,
        adds_first.ok + adds_joined.ok,
        published.ok,
        consumed.ok,
    );
    let refused_by_kind = [
        (
            "cache",
            puts_first.refused.len() + puts_joined.refused.len(),
        ),
        (
            "counter",
            adds_first.refused.len() + adds_joined.refused.len(),
        ),
        ("publish", published.refused.len()),
        ("group", consumed.refused.len()),
    ];
    assert!(
        refused.is_empty(),
        "writes were refused during the move ({refused_by_kind:?}), first {:?}",
        refused.iter().take(8).collect::<Vec<_>>(),
    );

    let reader = connect(&cluster, &joined).await;
    let get = |key: String| {
        let reader = &reader;
        let cluster = &cluster;
        async move {
            reader
                .cache_get(&cluster.tenant_id, &cluster.namespace, CACHE, &key)
                .await
                .expect("cache get")
                .map(|value| value.to_vec())
        }
    };
    for (key, value) in kept_first.iter().chain(&kept_joined) {
        let read = get(key.clone()).await;
        assert_eq!(read.as_ref(), Some(value), "{key} does not read back");
    }
    for key in deleted_first.iter().chain(&deleted_joined) {
        assert_eq!(
            get(key.clone()).await,
            None,
            "{key} was deleted and reads back"
        );
    }

    let sum = cluster
        .counter_get_via(&joined, CACHE, COUNTER)
        .await
        .expect("counter get")
        .unwrap_or(0);
    assert_eq!(
        sum,
        (adds_first.ok + adds_joined.ok) as i64,
        "the counter is not the number of acknowledged adds"
    );

    // Finish what the consumer had not got to. Nothing it finished may come
    // back, and every acknowledged publish must be finished once.
    let mut finished = finished;
    loop {
        let batch = group.poll(100).await.expect("poll after the move");
        if batch.is_empty() {
            break;
        }
        for claimed in batch {
            let payload = String::from_utf8_lossy(&claimed.record.payload).to_string();
            assert!(
                finished.insert(payload.clone()),
                "{payload} was handed out again after its ack"
            );
            group.ack(&claimed).await.expect("ack after the move");
        }
    }
    let lost: Vec<&String> = jobs.iter().filter(|job| !finished.contains(*job)).collect();
    assert!(
        lost.is_empty(),
        "{} acknowledged records never reached the group, first {:?}",
        lost.len(),
        lost.iter().take(5).collect::<Vec<_>>(),
    );
    cluster.shutdown().await;
}

/// Poll and ack through `via` until it has nothing left, once it serves the
/// group. Every offset it hands out is checked against `acked` first.
async fn finish_via(cluster: &Cluster, via: &str, acked: &mut HashSet<u64>) -> usize {
    let mut finished = 0;
    let deadline = tokio::time::Instant::now() + wait::budget(Duration::from_secs(30));
    loop {
        let claimed = match cluster
            .group_poll_records_via(via, STREAM, 0, GROUP, 10)
            .await
        {
            Ok(claimed) => claimed,
            // Named owner and serving are different moments.
            Err(err) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{via} never served the group: {err:#}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        if claimed.is_empty() {
            return finished;
        }
        for record in claimed {
            assert!(
                !acked.contains(&record.offset),
                "{via} handed out offset {} after the group acked it",
                record.offset,
            );
            cluster
                .group_ack_via(via, STREAM, 0, GROUP, record.offset)
                .await
                .expect("ack");
            acked.insert(record.offset);
            finished += 1;
        }
    }
}

/// Move the stream's shard to `to` by hand and wait for the cut-over.
async fn move_to(cluster: &Cluster, to: &str) {
    cluster.start_move(STREAM, 0, to).await.expect("start move");
    wait::until(Duration::from_secs(60), "the move to cut over", || async {
        cluster.place_shards().await;
        cluster.owner(STREAM).await.is_ok_and(|owner| owner == to)
    })
    .await
    .expect("move");
}

async fn publish_jobs(cluster: &Cluster, via: &str, from: usize, count: usize) {
    for i in from..from + count {
        cluster
            .publish_keyed_via_settled(
                via,
                STREAM,
                b"k",
                format!("job-{i}").into_bytes(),
                Duration::from_secs(30),
            )
            .await
            .expect("publish");
    }
}

/// **A shard moved away and back hands out nothing the group finished while
/// it was gone.** The group acks records on the first leader, the shard moves
/// to a second broker where the group finishes everything, and then moves
/// back. What the first leader remembered handing out is from before it let
/// the shard go; the group's cursor, copied back with the shard, is the truth.
#[tokio::test]
#[serial]
async fn a_shard_moved_away_and_back_hands_out_nothing_already_acked() {
    let cluster = Cluster::start(ClusterConfig {
        nodes: 2,
        streams: vec![StreamSpec::new(STREAM, 1)],
        ..Default::default()
    })
    .await
    .expect("start cluster");
    let first = cluster.owner(STREAM).await.expect("owner");
    let second = cluster
        .node_ids()
        .into_iter()
        .find(|id| id != &first)
        .expect("two brokers");

    let mut acked = HashSet::new();
    publish_jobs(&cluster, &first, 0, 10).await;
    assert!(finish_via(&cluster, &first, &mut acked).await > 0);

    move_to(&cluster, &second).await;
    publish_jobs(&cluster, &second, 10, 10).await;
    assert!(finish_via(&cluster, &second, &mut acked).await > 0);

    move_to(&cluster, &first).await;
    publish_jobs(&cluster, &first, 20, 5).await;
    // Only the records published since the move back are owed.
    assert_eq!(finish_via(&cluster, &first, &mut acked).await, 5);
    cluster.shutdown().await;
}
