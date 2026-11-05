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
pub(super) fn route_of_size(size: usize) -> Route {
    table(LEADER, &FOLLOWERS[..size - 1], PROMOTED)
        .get(&key())
        .expect("route")
        .clone()
}

/// The followers of a set of `size`, keeping consumer groups and offering
/// `capabilities`.
pub(super) fn group_replicas(size: usize, capabilities: PeerCapabilities) -> Replicas {
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
                unfetchable_logs: Vec::new(),
                storage: Some(storage_of(id)),
                group_storage: None,
                refuses: None,
                refuses_with: ErrorCode::Unavailable,
                recorded: true,
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
pub(super) async fn holds(
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
        checksum: batch_checksum(payloads, &[], &[], &[]),
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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await;

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

/// **A failover's group round counts a replica only from the copy the report
/// named**, as the records' round does. b's group answers come from another
/// copy, so with c unreachable too few took both and the leader serves its
/// own pair rather than take b's.
#[tokio::test]
async fn a_group_answer_from_another_copy_does_not_count() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    b_gave_up_on_one(&replicas, 3).await;
    replicas.get_mut("broker-c").reachable = false;
    replicas.get_mut("broker-b").group_storage = Some(StorageId::from_bytes([0xee; 16]));
    let (leader, _dir) = group_leader().await;
    let mut route = route_of_size(3);
    route.fence_storage_ids = Some(Arc::new(
        ["broker-b", "broker-c"]
            .into_iter()
            .map(|id| (id.to_string(), storage_of(id).to_hex()))
            .collect(),
    ));

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, true, None).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
    assert_eq!(serves(&leader, "workers").await, (None, Vec::new()));
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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(5),
        true,
        None,
    )
    .await;

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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await;

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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await;

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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await;

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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await;

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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        false,
        None,
    )
    .await;

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

    let outcome = fence_shard(
        &replicas,
        &leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert_eq!(replicas.sent(Kind::Fence).len(), 3);
}

/// **A leader whose copy is marked wiped does not count its own group logs**
/// toward the group majority: re-admitted on its stream log, they may still
/// lack a cursor commit a majority took. Of three it waits for both others.
#[test]
fn a_wiped_leaders_own_group_logs_do_not_count() {
    assert_eq!(super::super::group_answers_needed(2, true), 1);
    assert_eq!(super::super::group_answers_needed(2, false), 2);
    assert_eq!(super::super::group_answers_needed(4, false), 3);
}

/// Promote this broker on a set of three.
async fn promote(replicas: &Replicas, leader: &Arc<Broker>) -> Outcome {
    fence_shard(
        replicas,
        leader,
        LEADER,
        &key(),
        &route_of_size(3),
        true,
        None,
    )
    .await
}

/// The group leader, its copy re-admitted on the stream log at generation 4
/// but still marked wiped: its group logs have not caught up.
async fn wiped_group_leader() -> (Arc<Broker>, TempDir) {
    let (leader, dir) = group_leader().await;
    let log = leader
        .shard_log(LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    log.accept_generation(4, Some("broker-old"))
        .await
        .expect("ballot");
    let members = [LEADER, "broker-b", "broker-c"]
        .into_iter()
        .map(|node| felix_storage::disk_log::Member {
            node: node.to_string(),
            storage: None,
            learner: false,
            wiped: node == LEADER,
        })
        .collect();
    log.accept_set(felix_storage::disk_log::ReplicaSet {
        current: felix_storage::disk_log::Membership {
            generation: 4,
            members,
        },
        leaving: None,
        earlier: Vec::new(),
    })
    .await
    .expect("set");
    (leader, dir)
}

/// **A wiped leader keeps its mark through a promotion that fails, and the
/// retry still waits for both other copies' group logs.** c holds a dead
/// letter and the cursor past it; b lags. The first try takes c's group logs
/// and the fetch fails after the leader kept its route's set. Had that set
/// write dropped the mark, the retry would count the leader's empty group
/// logs and could open on b's alone (`GroupLogs` in
/// `docs/formal/FelixShard.tla`).
#[tokio::test]
async fn a_wiped_leader_keeps_its_mark_through_a_failed_group_take() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.electing = true;
    for node in ["broker-b", "broker-c"] {
        holds(
            &replicas,
            3,
            node,
            LogKind::Stream,
            4,
            &[Bytes::from_static(b"a")],
        )
        .await;
    }
    holds(
        &replicas,
        3,
        "broker-c",
        LogKind::GroupDeadLetters,
        4,
        &[dead("workers", 1)],
    )
    .await;
    holds(
        &replicas,
        3,
        "broker-c",
        LogKind::GroupCursors,
        4,
        &[cursor("workers", 2, 1, 4, 0)],
    )
    .await;
    replicas.get_mut("broker-c").unfetchable_logs = vec![ReplicaLog::GroupDeadLetters];
    let (leader, _dir) = wiped_group_leader().await;

    let outcome = promote(&replicas, &leader).await;
    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
    let log = leader
        .shard_log(LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.set_generation(), PROMOTED, "the route's set was kept");
    assert!(
        log.member_is_wiped(LEADER),
        "the set write dropped the mark"
    );

    replicas.get_mut("broker-c").unfetchable_logs.clear();
    let outcome = promote(&replicas, &leader).await;
    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-c".to_string())
        }
    );
    assert_eq!(serves(&leader, "workers").await, (Some(2), vec![1]));
}

/// **A wiped leader that no replica answers on its group logs stays
/// pending** rather than serve its own, which may hold none of what a
/// majority took. One whose copy is not marked serves its own pair.
#[tokio::test]
async fn a_wiped_leader_with_no_group_answer_stays_pending() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    for node in ["broker-b", "broker-c"] {
        replicas.get_mut(node).unreachable_logs = vec![ReplicaLog::GroupDeadLetters];
    }

    let (wiped, _dir) = wiped_group_leader().await;
    let outcome = promote(&replicas, &wiped).await;
    let Outcome::Pending { why, .. } = &outcome else {
        panic!("a wiped leader served its own group logs: {outcome:?}");
    };
    assert!(why.contains("wiped"), "{why}");

    let (clean, _dir) = group_leader().await;
    assert_eq!(
        promote(&replicas, &clean).await,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
}

/// Hold `node_id`'s group cursor log at `generation` for `to`, as a
/// candidate's fence there leaves it.
async fn group_ballot(replicas: &Replicas, node_id: &str, generation: u64, to: &str) {
    replicas
        .get(node_id)
        .broker
        .shard_log(LogKind::GroupCursors, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_generation(generation, Some(to))
        .await
        .expect("ballot");
}

/// **A group-log fence both replicas promised to another node is refused,
/// naming it**, as the records' fence is. Left pending, nothing reports it,
/// and the control plane never names the shard past the generation (#1914).
#[tokio::test]
async fn a_group_fence_promised_away_is_refused() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.ballots = true;
    for id in ["broker-b", "broker-c"] {
        group_ballot(&replicas, id, PROMOTED, "broker-d").await;
    }
    let (leader, _dir) = group_leader().await;

    let outcome = promote(&replicas, &leader).await;

    let Outcome::Refused {
        promised_to, why, ..
    } = outcome
    else {
        panic!("{outcome:?}");
    };
    assert_eq!(promised_to, "broker-d");
    assert!(why.contains("promised generation 5"), "{why}");
}

/// **One replica's promise away does not hold the group fence** when the
/// other still makes the majority, as on the records' fence.
#[tokio::test]
async fn a_group_fence_one_replica_promised_away_opens() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.ballots = true;
    group_ballot(&replicas, "broker-b", PROMOTED, "broker-d").await;
    let (leader, _dir) = group_leader().await;

    let outcome = promote(&replicas, &leader).await;

    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A newer ballot on a replica's group log has the fence refused**,
/// naming no node, as a newer ballot on its records does.
#[tokio::test]
async fn a_newer_group_ballot_has_the_fence_refused() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.ballots = true;
    group_ballot(&replicas, "broker-b", PROMOTED + 1, "broker-d").await;
    replicas.get_mut("broker-c").unreachable_logs = vec![ReplicaLog::GroupCursors];
    let (leader, _dir) = group_leader().await;

    let outcome = promote(&replicas, &leader).await;

    let Outcome::Refused {
        promised_to, why, ..
    } = outcome
    else {
        panic!("{outcome:?}");
    };
    assert_eq!(promised_to, "");
    assert!(why.contains("accepted a newer generation"), "{why}");
}

/// **A promise away with the other replica silent refuses the group fence**:
/// the promise never comes back, so waiting on the silent one could be for
/// good.
#[tokio::test]
async fn a_promise_away_and_silence_refuse_the_group_fence() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.ballots = true;
    group_ballot(&replicas, "broker-b", PROMOTED, "broker-d").await;
    replicas.get_mut("broker-c").unreachable_logs = vec![ReplicaLog::GroupCursors];
    let (leader, _dir) = group_leader().await;

    let outcome = promote(&replicas, &leader).await;

    let Outcome::Refused { promised_to, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(promised_to, "broker-d");
}

/// **A promise away with the other replica declining keeps the group fence
/// pending**, as on the records' fence: the decline may clear on a retry.
/// Serving the leader's own pair would leave a replica refusing it.
#[tokio::test]
async fn a_promise_away_and_a_decline_keep_the_group_fence_pending() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.ballots = true;
    group_ballot(&replicas, "broker-b", PROMOTED, "broker-d").await;
    // c answers the dead letters from another copy than the report named,
    // and its cursor fence never arrives.
    replicas.get_mut("broker-c").group_storage = Some(StorageId::from_bytes([0xee; 16]));
    replicas.get_mut("broker-c").unreachable_logs = vec![ReplicaLog::GroupCursors];
    let (leader, _dir) = group_leader().await;
    let mut route = route_of_size(3);
    route.fence_storage_ids = Some(Arc::new(
        ["broker-b", "broker-c"]
            .into_iter()
            .map(|id| (id.to_string(), storage_of(id).to_hex()))
            .collect(),
    ));

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route, true, None).await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// **A wiped leader's group fence is refused on one promise away**: its own
/// pair does not count, so the other replica alone is no majority.
#[tokio::test]
async fn a_wiped_leaders_group_fence_is_refused_on_one_promise_away() {
    let mut replicas = group_replicas(3, WITH_GROUPS);
    replicas.ballots = true;
    group_ballot(&replicas, "broker-b", PROMOTED, "broker-d").await;
    let (wiped, _dir) = wiped_group_leader().await;

    let outcome = promote(&replicas, &wiped).await;

    let Outcome::Refused { promised_to, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert_eq!(promised_to, "broker-d");
}
