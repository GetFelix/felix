//! A promoted stream shard's group logs: both fenced in one round beside its
//! records, a replica counting once it took both; the dead-letter log taken
//! from the replica whose pair is furthest ahead, and that replica's cursors
//! merged in, each checked against the dead letters the leader ends up with.

use felix_broker::LogKind;

use super::*;

/// What a replica offers once it fences group logs, with the labels every
/// build that fences sends.
const WITH_GROUPS: PeerCapabilities = REQUIRED
    .union(PeerCapabilities::GROUP_FENCE)
    .union(PeerCapabilities::GENERATION_LABELS);

const FOLLOWERS: [&str; 4] = ["broker-b", "broker-c", "broker-d", "broker-e"];

/// [`broker_on`], keeping consumer groups.
fn group_broker_on(dir: &std::path::Path) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage =
        felix_broker::DurableStorage::open(dir.join("streams"), config.clone()).expect("storage");
    Arc::new(
        Broker::new(felix_storage::EphemeralCache::new().into())
            .with_durable_storage(storage)
            .with_consumer_groups(
                Arc::new(
                    felix_broker::ConsumerGroups::open(dir.join("groups"), config.clone())
                        .expect("groups"),
                ),
                Arc::new(
                    felix_broker::DeadLetters::open(dir.join("dead-letters"), config)
                        .expect("dead letters"),
                ),
                std::time::Duration::from_secs(30),
                3,
            ),
    )
}

/// The shard led by this broker with `followers` behind it, every node in
/// the table.
fn table(leader: &str, followers: &[&str], generation: u64) -> RoutingTable {
    let nodes: HashMap<String, NodeRef> = std::iter::once(LEADER)
        .chain(FOLLOWERS)
        .enumerate()
        .map(|(i, id)| (id.to_string(), node(id, 7001 + i as u16)))
        .collect();
    RoutingTable::build(
        [(
            key(),
            leader.to_string(),
            followers
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>(),
            generation,
        )],
        &nodes,
    )
}

fn nodes_of(table: &RoutingTable) -> HashMap<String, NodeRef> {
    let route = table.get(&key()).expect("route");
    route
        .replicas
        .iter()
        .chain(std::iter::once(&route.leader))
        .map(|node| (node.node_id.clone(), node.clone()))
        .collect()
}

/// A router for replica `local` of a set of `size`, still at `generation`.
fn replica_router(local: &str, size: usize, generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        local,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let mut members = vec![LEADER];
    members.extend(&FOLLOWERS[..size - 1]);
    let table = table("broker-old", &members, generation);
    router.publish(table.clone(), &nodes_of(&table));
    router
}

/// The route the promoted leader sees, `size` replicas in all.
fn route_of_size(size: usize) -> Route {
    table(LEADER, &FOLLOWERS[..size - 1], PROMOTED)
        .get(&key())
        .expect("route")
        .clone()
}

/// The followers of a set of `size`, keeping consumer groups and offering
/// `capabilities`.
fn group_replicas(size: usize, capabilities: PeerCapabilities) -> Replicas {
    let mut replicas = Replicas::new();
    replicas.replicas.clear();
    for id in &FOLLOWERS[..size - 1] {
        let dir = tempfile::tempdir().expect("tempdir");
        let broker = group_broker_on(dir.path());
        replicas.replicas.insert(
            id.to_string(),
            Replica {
                handler: ReplicaHandler::new(Arc::clone(&broker), replica_router(id, size, 3)),
                broker,
                capabilities,
                reachable: true,
                unreachable_logs: Vec::new(),
                _dir: dir,
            },
        );
    }
    replicas
}

fn put(key: &str, value: &[u8]) -> Bytes {
    felix_storage::cache::CacheOp::Put {
        key: key.to_string(),
        value: Bytes::copy_from_slice(value),
        expires_at_millis: 0,
        version: None,
    }
    .encode()
}

/// `group`'s dead letter for `offset`, as the dead-letter log stores it.
fn dead(group: &str, offset: u64) -> Bytes {
    put(&format!("{group}\u{1f}{offset}"), b"")
}

/// `group`'s cursor at `offset`, needing the dead-letter log up to `end` as
/// generation `generation` wrote it, as the cursor log stores it.
fn cursor(group: &str, offset: u64, end: u64, generation: u64, origin: u64) -> Bytes {
    let mut value = vec![2];
    for field in [offset, end, generation, origin] {
        value.extend_from_slice(&field.to_be_bytes());
    }
    put(group, &value)
}

/// Store `payloads` in `node_id`'s `log` as a leader at `generation` shipped
/// them, in a set of `size`.
async fn holds(
    replicas: &Replicas,
    size: usize,
    node_id: &str,
    log: LogKind,
    generation: u64,
    payloads: &[Bytes],
) {
    let handler = ReplicaHandler::new(
        Arc::clone(&replicas.get(node_id).broker),
        replica_router(node_id, size, generation),
    );
    let batch = ReplicateRecords {
        checksum: batch_checksum(payloads, &[], &[]),
        payloads: payloads.to_vec(),
        ..batch(generation, 0, &[])
    };
    let answer = handler.apply(None, batch, log).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{node_id} did not store the setup batch: {answer:?}"
    );
}

/// Write `payloads` to the leader's own `log`, as it wrote them leading at
/// `generation`.
async fn wrote(broker: &Broker, log: LogKind, generation: u64, payloads: &[Bytes]) {
    let log = broker
        .shard_log(log, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let tail = log.tail_offset().await.expect("tail");
    log.record_generation(generation, tail).expect("generation");
    log.append(payloads).await.expect("append");
}

/// The promoted leader, holding the shard's record from generation 4.
async fn group_leader() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = group_broker_on(dir.path());
    holding(&broker, LogKind::Stream, 4, &["a"]).await;
    (broker, dir)
}

fn group(name: &str) -> felix_broker::GroupKey {
    felix_broker::GroupKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        group: name.to_string(),
    }
}

/// Where the leader serves `name` from, and what it lists as dead, once its
/// cursors are checked against its dead letters.
async fn serves(broker: &Broker, name: &str) -> (Option<u64>, Vec<u64>) {
    let reader = broker.group_reader().expect("groups");
    (
        reader.committed(&group(name)).await.expect("cursor"),
        reader.dead_lettered(&group(name)).await.expect("dead"),
    )
}

/// b holds the shard's record, `workers`' dead letter for offset 1 and its
/// cursor past it, from generation 4.
async fn b_gave_up_on_one(replicas: &Replicas, size: usize) {
    holds(
        replicas,
        size,
        "broker-b",
        LogKind::Stream,
        4,
        &[Bytes::from_static(b"a")],
    )
    .await;
    holds(
        replicas,
        size,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &[dead("workers", 1)],
    )
    .await;
    holds(
        replicas,
        size,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &[cursor("workers", 2, 1, 4, 0)],
    )
    .await;
}

/// **A promoted stream leader takes the group state furthest ahead, as well
/// as the records.** b holds a dead letter and the cursor past it that the
/// leader never got. Opening without them puts the group back before work it
/// finished, and redrives nothing it gave up on.
#[tokio::test]
async fn the_group_logs_furthest_ahead_are_taken_too() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    b_gave_up_on_one(&replicas, 3).await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(serves(&leader, "workers").await, (Some(2), vec![1]));
    // The records' log and both group logs, in one round.
    assert_eq!(replicas.sent(Kind::Fence).len(), 3);
}

/// **A replica counts toward the group logs' majority only once it took
/// both fences.** Of five, b's dead-letter fence does not reach it. c and d
/// make the majority, and offset 1 is still owed rather than passed.
#[tokio::test]
async fn a_replica_counts_once_it_took_both_group_fences() {
    let mut replicas = group_replicas(5, WITH_GROUPS);
    b_gave_up_on_one(&replicas, 5).await;
    for id in ["broker-c", "broker-d"] {
        holds(
            &replicas,
            5,
            id,
            LogKind::Stream,
            4,
            &[Bytes::from_static(b"a")],
        )
        .await;
    }
    replicas.get_mut("broker-b").unreachable_logs = vec![ReplicaLog::GroupDeadLetters];
    replicas.get_mut("broker-e").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(5), true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(serves(&leader, "workers").await, (None, Vec::new()));
}

/// **A cursor is checked against the generation that wrote its dead
/// letters, not only the log's length.** The leader's cursor passed offset 1
/// on a dead letter generation 3 wrote; b's newer log replaced it with
/// another group's at the same offset. The log is as long as the cursor
/// needs, but its dead letter is gone, so the group goes back to where it
/// was placed.
#[tokio::test]
async fn a_cursor_is_checked_against_its_dead_letters_generation() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::Stream,
        4,
        &[Bytes::from_static(b"a")],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &[dead("others", 0)],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;
    wrote(&leader, LogKind::GroupDeadLetters, 3, &[dead("workers", 1)]).await;
    wrote(
        &leader,
        LogKind::GroupCursors,
        3,
        &[cursor("workers", 2, 1, 3, 0)],
    )
    .await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(serves(&leader, "workers").await, (Some(0), Vec::new()));
}

/// **This broker's own further cursor is kept while it still stands.** b's
/// dead-letter and cursor logs are both longer and are taken, but b's
/// cursor for `workers` is behind the leader's, which needs only the dead
/// letter both have.
#[tokio::test]
async fn an_own_further_cursor_is_kept_while_it_stands() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::Stream,
        4,
        &[Bytes::from_static(b"a")],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &[dead("workers", 1), dead("others", 0)],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &[cursor("workers", 1, 0, 4, 1), cursor("workers", 2, 1, 4, 0)],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;
    wrote(&leader, LogKind::GroupDeadLetters, 4, &[dead("workers", 1)]).await;
    wrote(
        &leader,
        LogKind::GroupCursors,
        4,
        &[cursor("workers", 3, 1, 4, 0)],
    )
    .await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(serves(&leader, "workers").await, (Some(3), vec![1]));
    assert_eq!(serves(&leader, "others").await, (None, vec![0]));
}

/// **A group only this broker had is kept**: the replica's cursors are
/// merged in, never put in place of this broker's.
#[tokio::test]
async fn a_group_only_this_broker_had_is_kept() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    b_gave_up_on_one(&replicas, 3).await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;
    wrote(
        &leader,
        LogKind::GroupCursors,
        3,
        &[cursor("solo", 5, 0, 0, 5)],
    )
    .await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(serves(&leader, "solo").await, (Some(5), Vec::new()));
    assert_eq!(serves(&leader, "workers").await, (Some(2), vec![1]));
}

/// **A newer generation's shorter group state beats an older generation's
/// longer one.** This broker led generation 6 and wrote there; b holds more
/// of generation 4. Ordered by length alone, the leader would take b's stale
/// dead letters. The generation labels that order them are what a leader
/// records on its group logs at takeover.
#[tokio::test]
async fn a_newer_shorter_pair_is_kept_over_an_older_longer_one() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::Stream,
        4,
        &[Bytes::from_static(b"a")],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &[dead("workers", 0), dead("others", 0)],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;
    wrote(&leader, LogKind::GroupDeadLetters, 6, &[dead("workers", 1)]).await;
    wrote(
        &leader,
        LogKind::GroupCursors,
        6,
        &[cursor("workers", 2, 1, 6, 0)],
    )
    .await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(serves(&leader, "workers").await, (Some(2), vec![1]));
}

/// **Too few replicas that fence group logs leaves them as they are**, and
/// the shard opens on its fenced records: a fleet part way through an
/// upgrade behaves as before.
#[tokio::test]
async fn without_the_group_fence_the_group_logs_stay_as_they_are() {
    let mut replicas = group_replicas(3, REQUIRED);
    b_gave_up_on_one(&replicas, 3).await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), false).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(serves(&leader, "workers").await, (None, Vec::new()));
    assert_eq!(replicas.sent(Kind::Fence), vec!["broker-b"]);
}

/// **A replica without consumer groups answers the group fence as an empty
/// log**, so it counts toward the majority rather than keeping the shard
/// closed for good.
#[tokio::test]
async fn a_replica_without_consumer_groups_counts_as_empty() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    let mut plain = Replica::new("broker-b");
    plain.capabilities = WITH_GROUPS;
    replicas.replicas.insert("broker-b".to_string(), plain);
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a"])
        .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(replicas.sent(Kind::Fence).len(), 3);
}
