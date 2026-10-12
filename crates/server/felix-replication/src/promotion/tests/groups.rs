//! A promoted stream shard's group logs: fenced after its records, dead
//! letters first, each taking the copy furthest ahead among a majority's
//! answers.

use felix_broker::LogKind;

use super::*;

/// What a replica offers once it fences group logs.
const WITH_GROUPS: PeerCapabilities = REQUIRED.union(PeerCapabilities::GROUP_FENCE);

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

/// b and c keep consumer groups and offer `capabilities`; c is unreachable.
fn group_replicas(capabilities: PeerCapabilities) -> Replicas {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let broker = group_broker_on(dir.path());
        replicas.replicas.insert(
            id.to_string(),
            Replica {
                handler: ReplicaHandler::new(Arc::clone(&broker), router_for(id, 3)),
                broker,
                capabilities,
                reachable: true,
                _dir: dir,
            },
        );
    }
    replicas.get_mut("broker-c").reachable = false;
    replicas
}

/// b holds the shard's record, a dead letter, and the cursor that moved past
/// it, from the leader before.
async fn b_holds_group_state(replicas: &Replicas) {
    let b = replicas.get("broker-b");
    b.holds("broker-b", 4, 0, &["a"]).await;
    b.holds_in(LogKind::GroupDeadLetters, "broker-b", 4, 0, &["dead 1"])
        .await;
    b.holds_in(LogKind::GroupCursors, "broker-b", 4, 0, &["at 2"])
        .await;
}

/// The promoted leader: level with b on the records, without its group state.
async fn group_leader() -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = group_broker_on(dir.path());
    holding(&broker, LogKind::Stream, 4, &["a"]).await;
    (broker, dir)
}

/// **A promoted stream leader takes the group logs furthest ahead, as well
/// as the records.** b holds a dead letter and the cursor past it that the
/// leader never got. Opening without them puts the group back before work it
/// finished, and redrives nothing it gave up on.
#[tokio::test]
async fn the_group_logs_furthest_ahead_are_taken_too() {
    let replicas = group_replicas(WITH_GROUPS);
    b_holds_group_state(&replicas).await;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), true).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: Some("broker-b".to_string())
        }
    );
    assert_eq!(
        held_in(&leader, LogKind::GroupDeadLetters).await,
        vec!["dead 1"]
    );
    assert_eq!(held_in(&leader, LogKind::GroupCursors).await, vec!["at 2"]);
    // The records' log, then the dead letters, then the cursors.
    assert_eq!(
        replicas.sent(Kind::Fence),
        vec!["broker-b", "broker-b", "broker-b"]
    );
}

/// **Too few replicas that fence group logs leaves them as they are**, and
/// the shard opens on its fenced records: a fleet part way through an
/// upgrade behaves as before.
#[tokio::test]
async fn without_the_group_fence_the_group_logs_stay_as_they_are() {
    let replicas = group_replicas(REQUIRED);
    b_holds_group_state(&replicas).await;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), false).await;

    assert_eq!(
        outcome,
        Outcome::Fenced {
            caught_up_from: None
        }
    );
    assert!(held_in(&leader, LogKind::GroupCursors).await.is_empty());
    assert_eq!(replicas.sent(Kind::Fence), vec!["broker-b"]);
}

/// **Where a majority offers the group fence, the shard waits for one to
/// take it.** b offers it but has no group logs to keep the promise in.
#[tokio::test]
async fn a_group_fence_without_a_majority_keeps_the_shard_closed() {
    let mut replicas = group_replicas(WITH_GROUPS);
    let b = Replica::new("broker-b");
    replicas.replicas.insert(
        "broker-b".to_string(),
        Replica {
            capabilities: WITH_GROUPS,
            ..b
        },
    );
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a"])
        .await;
    let (leader, _dir) = group_leader().await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), true).await;

    let Outcome::Pending { why, .. } = outcome else {
        panic!("{outcome:?}");
    };
    assert!(why.contains("GroupDeadLetters"), "{why}");
}
