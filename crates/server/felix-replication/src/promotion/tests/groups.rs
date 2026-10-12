//! A promoted stream shard's group logs: both fenced in one round beside its
//! records, a replica counting once it took both, and taken together from the
//! one replica whose pair is furthest ahead.

use felix_broker::LogKind;

use super::*;

/// What a replica offers once it fences group logs.
const WITH_GROUPS: PeerCapabilities = REQUIRED.union(PeerCapabilities::GROUP_FENCE);

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

/// Store `values` in `node_id`'s `log` as a leader at `generation` shipped
/// them, in a set of `size`.
async fn holds(
    replicas: &Replicas,
    size: usize,
    node_id: &str,
    log: LogKind,
    generation: u64,
    values: &[&str],
) {
    let handler = ReplicaHandler::new(
        Arc::clone(&replicas.get(node_id).broker),
        replica_router(node_id, size, generation),
    );
    let answer = handler.apply(None, batch(generation, 0, values), log).await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{node_id} did not store the setup batch: {answer:?}"
    );
}

/// The promoted leader, holding the shard's record from generation 4.
async fn group_leader() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = group_broker_on(dir.path());
    holding(&broker, LogKind::Stream, 4, &["a"]).await;
    (broker, dir)
}

/// The leader's dead letters and cursors after the fence.
async fn pair(broker: &Broker) -> (Vec<String>, Vec<String>) {
    (
        held_in(broker, LogKind::GroupDeadLetters).await,
        held_in(broker, LogKind::GroupCursors).await,
    )
}

/// **A promoted stream leader takes the group logs furthest ahead, as well
/// as the records.** b holds a dead letter and the cursor past it that the
/// leader never got. Opening without them puts the group back before work it
/// finished, and redrives nothing it gave up on.
#[tokio::test]
async fn the_group_logs_furthest_ahead_are_taken_too() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    holds(&replicas, 3, "broker-b", LogKind::Stream, 4, &["a"]).await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &["dead 1"],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &["at 2"],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(
        pair(&leader).await,
        (vec!["dead 1".to_string()], vec!["at 2".to_string()])
    );
    // The records' log and both group logs, in one round.
    assert_eq!(replicas.sent(Kind::Fence).len(), 3);
}

/// **A replica counts toward the group logs' majority only once it took
/// both fences.** Of five, b's dead-letter fence does not reach it but its
/// cursor fence does. Counted on its cursors alone, the leader would take
/// b's cursor past offset 1 and keep a dead-letter list without it: the
/// record neither redelivered nor dead. c and d make the majority instead.
#[tokio::test]
async fn a_cursor_is_never_taken_without_the_dead_letters_beside_it() {
    let mut replicas = group_replicas(5, WITH_GROUPS);
    for id in ["broker-b", "broker-c", "broker-d"] {
        holds(&replicas, 5, id, LogKind::Stream, 4, &["a"]).await;
    }
    holds(
        &replicas,
        5,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &["dead 1"],
    )
    .await;
    holds(
        &replicas,
        5,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &["at 2"],
    )
    .await;
    replicas.get_mut("broker-b").unreachable_logs = vec![ReplicaLog::GroupDeadLetters];
    replicas.get_mut("broker-e").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(5), true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(pair(&leader).await, (Vec::new(), Vec::new()));
}

/// **Both logs come from one replica, chosen by its dead letters first.** b
/// has a dead letter c lacks, c has cursors further than b's. Each log's
/// furthest copy on its own would be b's dead letters with c's cursors,
/// which may pass b's dead-lettered record in c's history without listing it.
#[tokio::test]
async fn the_pair_is_taken_from_one_replica() {
    let mut replicas = group_replicas(5, WITH_GROUPS);
    for id in ["broker-b", "broker-c"] {
        holds(&replicas, 5, id, LogKind::Stream, 4, &["a"]).await;
    }
    holds(
        &replicas,
        5,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &["dead 1"],
    )
    .await;
    holds(
        &replicas,
        5,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &["b at 2"],
    )
    .await;
    holds(
        &replicas,
        5,
        "broker-c",
        LogKind::GroupCursors,
        4,
        &["c at 1", "c at 3"],
    )
    .await;
    replicas.get_mut("broker-d").reachable = false;
    replicas.get_mut("broker-e").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(5), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(
        pair(&leader).await,
        (vec!["dead 1".to_string()], vec!["b at 2".to_string()])
    );
}

/// **A dead-letter suffix the leader drops takes its cursors with it.** The
/// leader's dead letter and cursors are generation 3's; b's newer dead
/// letters supersede that dead letter, though b's cursors are older and
/// shorter than the leader's. Kept, the leader's cursors would pass the
/// record of a dead letter it no longer has.
#[tokio::test]
async fn a_superseded_dead_letter_takes_the_cursors_past_it() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    // Shipped by generation 2's leader, before generation 4's.
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupCursors,
        2,
        &["b at 1"],
    )
    .await;
    holds(&replicas, 3, "broker-b", LogKind::Stream, 4, &["a"]).await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &["new dead"],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;
    holding(&leader, LogKind::GroupDeadLetters, 3, &["old dead"]).await;
    holding(&leader, LogKind::GroupCursors, 3, &["p1", "p2", "p3"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(
        pair(&leader).await,
        (vec!["new dead".to_string()], vec!["b at 1".to_string()])
    );
}

/// **A newer generation's shorter group logs beat an older generation's
/// longer ones.** This broker led generation 6 and wrote there; b holds more
/// of generation 4. Ordered by length alone, the leader would replace what
/// it wrote with b's stale copy. The generation labels that order them are
/// what a leader records on its group logs at takeover.
#[tokio::test]
async fn a_newer_shorter_pair_is_kept_over_an_older_longer_one() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    holds(&replicas, 3, "broker-b", LogKind::Stream, 4, &["a"]).await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &["l1", "l2"],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &["l at 1", "l at 2"],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;
    holding(&leader, LogKind::GroupDeadLetters, 6, &["q dead"]).await;
    holding(&leader, LogKind::GroupCursors, 6, &["q at 1"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(
        pair(&leader).await,
        (vec!["q dead".to_string()], vec!["q at 1".to_string()])
    );
}

/// **Too few replicas that fence group logs leaves them as they are**, and
/// the shard opens on its fenced records: a fleet part way through an
/// upgrade behaves as before.
#[tokio::test]
async fn without_the_group_fence_the_group_logs_stay_as_they_are() {
    let mut replicas = group_replicas(3, REQUIRED);
    holds(&replicas, 3, "broker-b", LogKind::Stream, 4, &["a"]).await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupDeadLetters,
        4,
        &["dead 1"],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-b",
        LogKind::GroupCursors,
        4,
        &["at 2"],
    )
    .await;
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route_of_size(3), false).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(pair(&leader).await, (Vec::new(), Vec::new()));
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
