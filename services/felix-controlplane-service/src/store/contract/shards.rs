//! Shard assignment behaviour every backend must satisfy.
//!
//! The replica-report cases live in `replica_reports` but run from here,
//! since they need this suite's catalog.
use std::sync::Arc;

use super::nodes::node;
use super::replica_reports;
use crate::model::{
    Cache, CacheKey, ConsistencyLevel, DeliveryGuarantee, MoveReason, Namespace, RetentionPolicy,
    ShardAssignment, ShardKey, ShardKind, ShardState, Stream, StreamKey, StreamKind, Tenant,
};
use crate::store::{AssignmentWrite, ControlPlaneStore, StoreError};

const TENANT: &str = "shard-t";
const NAMESPACE: &str = "shard-ns";
const STREAM: &str = "orders";
const SHARDS: u32 = 4;
/// Deliberately the stream's name, and deliberately a different shard count:
/// the two must be distinguished by kind and nothing else.
const CACHE: &str = "orders";
const CACHE_SHARDS: u32 = 2;
const _: () = assert!(
    SHARDS > CACHE_SHARDS,
    "the cache must have fewer shards than the stream, or the bound test proves nothing",
);

pub(crate) fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

pub(crate) fn assignment(shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        // Deliberately non-zero: the store owns this, and every test that checks
        // a generation is checking the store ignored what the caller sent.
        generation: 999,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
    }
}

/// Build the tenant, namespace, stream, and two nodes every case needs.
pub(crate) async fn seed(store: &dyn ControlPlaneStore) {
    let _ = store
        .create_tenant(Tenant {
            tenant_id: TENANT.to_string(),
            display_name: "Shards".to_string(),
        })
        .await;
    let _ = store
        .create_namespace(Namespace {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            display_name: "Shards".to_string(),
        })
        .await;
    let _ = store
        .create_stream(Stream {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            kind: StreamKind::Stream,
            shards: SHARDS,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
        })
        .await;

    let _ = store
        .create_cache(Cache {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            cache: CACHE.to_string(),
            display_name: "Orders cache".to_string(),
            shards: CACHE_SHARDS,
            replication_factor: 1,
            consistency: crate::model::ConsistencyLevel::Leader,
        })
        .await;

    for (i, id) in ["broker-x", "broker-y"].iter().enumerate() {
        let _ = store.register_node(node(id, 7500 + i as u16)).await;
    }
}

/// The placement token now, for a write whose fencing is not what the case is
/// about.
pub(crate) async fn token(store: &dyn ControlPlaneStore) -> u64 {
    store.placement_token().await.expect("placement token")
}

pub(crate) async fn clear(store: &dyn ControlPlaneStore) {
    for assignment in store.list_shard_assignments().await.expect("list") {
        store
            .delete_shard_assignment(&assignment.key)
            .await
            .expect("delete");
    }
}

pub(crate) async fn run_shard_contract(store: Arc<dyn ControlPlaneStore>) {
    let store: &dyn ControlPlaneStore = store.as_ref();
    seed(store).await;

    an_assignment_is_readable(store).await;
    the_store_owns_the_generation(store).await;
    one_assignment_per_shard(store).await;
    a_shard_outside_the_stream_is_rejected(store).await;
    an_unknown_stream_is_rejected(store).await;
    an_unknown_node_is_rejected(store).await;
    an_unsupported_state_transition_is_rejected(store).await;
    assignments_can_be_listed_by_leader(store).await;
    missing_assignments_report_not_found(store).await;
    deleting_leaves_the_shard_unowned(store).await;
    a_node_leading_a_shard_cannot_be_deleted(store).await;
    a_node_replicating_a_shard_cannot_be_deleted(store).await;
    a_snapshot_and_the_changes_after_it_lose_nothing(store).await;
    a_cache_shard_and_a_stream_shard_of_the_same_name_coexist(store).await;
    a_cache_shard_is_bounded_by_the_cache_not_the_stream(store).await;
    an_unknown_cache_is_rejected(store).await;
    deleting_a_stream_or_cache_takes_its_shard_assignments_with_it(store).await;
    replica_reports::a_replica_report_is_kept_and_read_back(store).await;
    replica_reports::a_report_from_a_superseded_leader_is_dropped(store).await;
    replica_reports::a_report_at_the_same_generation_is_an_update(store).await;
    replica_reports::a_report_behind_the_held_one_is_dropped(store).await;
    replica_reports::a_report_needs_an_assignment_and_goes_with_it(store).await;
    a_move_in_progress_is_persisted(store).await;
    a_replacement_in_progress_is_persisted(store).await;
    replica_reports::a_drained_report_is_kept(store).await;
    replica_reports::the_leader_offset_is_kept(store).await;
    a_conditional_write_lands_only_at_the_expected_generation(store).await;
    a_fence_planned_before_a_cut_over_is_refused_after_it(store).await;
    why_a_move_started_is_persisted(store).await;
    pausing_moves_is_persisted(store).await;
}

fn cache_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: CACHE.to_string(),
        shard,
        kind: ShardKind::Cache,
    }
}

fn cache_assignment(shard: u32, leader: &str) -> ShardAssignment {
    ShardAssignment {
        key: cache_key(shard),
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation: 999,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
    }
}

/// The collision the kind exists to prevent. Without it these two writes are
/// the same primary key, and the second silently takes the first's ownership --
/// so a client would be routed to a broker holding the wrong log entirely.
async fn a_cache_shard_and_a_stream_shard_of_the_same_name_coexist(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("stream shard");
    store
        .put_shard_assignment(cache_assignment(0, "broker-y"))
        .await
        .expect("cache shard");

    let stream_shard = store.get_shard_assignment(&key(0)).await.expect("stream");
    let cache_shard = store
        .get_shard_assignment(&cache_key(0))
        .await
        .expect("cache");

    assert_eq!(stream_shard.leader, "broker-x");
    assert_eq!(cache_shard.leader, "broker-y");
    assert_eq!(store.list_shard_assignments().await.expect("list").len(), 2);
}

/// The shard bound comes from whichever of the two the key names. Shard 3 is
/// inside the stream and outside the cache, so resolving the bound against the
/// wrong one would let this through.
async fn a_cache_shard_is_bounded_by_the_cache_not_the_stream(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(SHARDS - 1, "broker-x"))
        .await
        .expect("the stream has this shard");

    let err = store
        .put_shard_assignment(cache_assignment(SHARDS - 1, "broker-x"))
        .await
        .expect_err("the cache does not");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
}

async fn an_unknown_cache_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut orphan = cache_assignment(0, "broker-x");
    orphan.key.stream = "no-such-cache".to_string();
    let err = store
        .put_shard_assignment(orphan)
        .await
        .expect_err("unknown cache");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");
}

/// Cases needing an owned handle to share across tasks.
pub(crate) async fn run_shard_concurrency_contract(store: Arc<dyn ControlPlaneStore>) {
    seed(store.as_ref()).await;
    concurrent_writes_to_one_shard_serialise(Arc::clone(&store)).await;
    concurrent_conditional_writes_have_one_winner(store).await;
}

/// Deleting a node while an assignment to it is being written: either the
/// delete is refused or the write is, never both through. Both checks read
/// the other side's table, so without a lock between them each can pass on a
/// read the other is about to invalidate, leaving a shard led by a node that
/// no longer exists.
///
/// `rounds` is per backend: the window is narrow, so an in-process store
/// needs thousands to hit it, and a replicated one pays a commit per write.
pub(crate) async fn run_node_delete_race_contract(store: Arc<dyn ControlPlaneStore>, rounds: u16) {
    seed(store.as_ref()).await;
    for round in 0..rounds {
        clear(store.as_ref()).await;
        let node_id = format!("broker-race-{round}");
        store
            .register_node(node(&node_id, 9000 + round))
            .await
            .expect("register");

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let deleter = {
            let (store, barrier, node_id) =
                (Arc::clone(&store), Arc::clone(&barrier), node_id.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                store.delete_node(&node_id).await
            })
        };
        let writer = {
            let (store, barrier, node_id) =
                (Arc::clone(&store), Arc::clone(&barrier), node_id.clone());
            tokio::spawn(async move {
                barrier.wait().await;
                store.put_shard_assignment(assignment(0, &node_id)).await
            })
        };
        let deleted = deleter.await.expect("deleter").is_ok();
        let written = writer.await.expect("writer").is_ok();

        assert!(
            !(deleted && written),
            "round {round}: the node was deleted and an assignment to it written",
        );
        if written {
            let stored = store.get_shard_assignment(&key(0)).await.expect("get");
            assert!(
                store.get_node(&stored.leader).await.is_ok(),
                "round {round}: the stored assignment names a deleted node",
            );
        }
    }
    clear(store.as_ref()).await;
}

/// Placement instances racing on one shard, each writing against the
/// generation it read: exactly one lands, whether the shard existed or not.
async fn concurrent_conditional_writes_have_one_winner(store: Arc<dyn ControlPlaneStore>) {
    const WRITERS: u64 = 8;

    for expected in [None, Some(0)] {
        if expected.is_none() {
            clear(store.as_ref()).await;
        }
        let fence = token(store.as_ref()).await;
        let writers: Vec<_> = (0..WRITERS)
            .map(|i| {
                let store = Arc::clone(&store);
                tokio::spawn(async move {
                    let leader = if i % 2 == 0 { "broker-x" } else { "broker-y" };
                    store
                        .put_shard_assignment_if(assignment(0, leader), expected, fence)
                        .await
                        .expect("conditional put")
                })
            })
            .collect();
        let mut written = Vec::new();
        for writer in writers {
            if let AssignmentWrite::Written(assignment) = writer.await.expect("writer") {
                written.push(assignment);
            }
        }
        assert_eq!(written.len(), 1, "expecting {expected:?}: one writer wins");
        assert_eq!(
            store.get_shard_assignment(&key(0)).await.expect("get"),
            written[0],
            "the winner's write is what is stored",
        );
    }
}

/// Two placement passes can write the same shard at once. Whatever order they
/// land in, the shard must end with one assignment, and the generation must
/// count every write rather than two writers both reading generation 0 and both
/// storing 1 -- which is how a broker gets told its current view is stale.
async fn concurrent_writes_to_one_shard_serialise(store: Arc<dyn ControlPlaneStore>) {
    const WRITERS: u64 = 8;

    clear(store.as_ref()).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("initial");

    let writers: Vec<_> = (0..WRITERS)
        .map(|i| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                let leader = if i % 2 == 0 { "broker-x" } else { "broker-y" };
                store
                    .put_shard_assignment(assignment(0, leader))
                    .await
                    .expect("concurrent put")
                    .generation
            })
        })
        .collect();

    let mut generations = Vec::new();
    for writer in writers {
        generations.push(writer.await.expect("writer"));
    }
    generations.sort_unstable();

    assert_eq!(
        store.list_shard_assignments().await.expect("list").len(),
        1,
        "concurrent writes must not create a second assignment",
    );
    assert_eq!(
        generations,
        (1..=WRITERS).collect::<Vec<_>>(),
        "every write must take a distinct, consecutive generation",
    );
    assert_eq!(
        store
            .get_shard_assignment(&key(0))
            .await
            .expect("get")
            .generation,
        WRITERS,
        "the stored generation must be the last one handed out",
    );
}

async fn an_assignment_is_readable(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let stored = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    assert_eq!(stored.key, key(0));
    assert_eq!(stored.leader, "broker-x");
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        stored
    );
    assert_eq!(
        store.list_shard_assignments().await.expect("list"),
        vec![stored]
    );
}

/// A broker reports against the generation it read. If a caller could choose
/// one, a stale report could be made to look current.
async fn the_store_owns_the_generation(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let first = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    assert_eq!(first.generation, 0, "the caller's 999 must be ignored");

    let second = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put again");
    assert_eq!(second.generation, 1, "every write moves the generation on");
}

/// The invariant placement depends on: writing a shard replaces its assignment
/// rather than adding a second one.
async fn one_assignment_per_shard(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(1, "broker-x"))
        .await
        .expect("put");
    let mut moved = assignment(1, "broker-y");
    moved.state = ShardState::Assigning;
    store.put_shard_assignment(moved).await.expect("reassign");

    let all = store.list_shard_assignments().await.expect("list");
    assert_eq!(all.len(), 1, "one assignment per shard");
    assert_eq!(all[0].leader, "broker-y");
}

async fn a_shard_outside_the_stream_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let err = store
        .put_shard_assignment(assignment(SHARDS, "broker-x"))
        .await
        .expect_err("out of bounds");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(
        store
            .list_shard_assignments()
            .await
            .expect("list")
            .is_empty()
    );
}

async fn an_unknown_stream_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut orphan = assignment(0, "broker-x");
    orphan.key.stream = "no-such-stream".to_string();
    let err = store
        .put_shard_assignment(orphan)
        .await
        .expect_err("unknown stream");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");
}

/// Checked in the store rather than by a foreign key, because the node
/// reference deliberately has none.
async fn an_unknown_node_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let err = store
        .put_shard_assignment(assignment(0, "broker-ghost"))
        .await
        .expect_err("unknown leader");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");

    let mut with_ghost_replica = assignment(0, "broker-x");
    with_ghost_replica.replicas = vec!["broker-ghost".to_string()];
    let err = store
        .put_shard_assignment(with_ghost_replica)
        .await
        .expect_err("unknown replica");
    assert!(matches!(err, StoreError::NotFound(_)), "got {err:?}");
}

async fn an_unsupported_state_transition_is_rejected(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("assigning");

    // Assigning -> Active is fine; Active -> Assigning is not a move.
    let mut active = assignment(0, "broker-x");
    active.state = ShardState::Active;
    store.put_shard_assignment(active).await.expect("active");
    let err = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect_err("bad transition");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let unchanged = store.get_shard_assignment(&key(0)).await.expect("get");
    assert_eq!(unchanged.state, ShardState::Active);
    assert_eq!(unchanged.generation, 1, "a rejected write moves nothing");
}

/// The steps of a planned move, as the store must carry them: the successor
/// survives a round trip, a drain at any serving state is accepted, and the
/// cut-over is a fresh `Assigning` naming the successor.
async fn a_move_in_progress_is_persisted(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("assigning");

    let mut staged = assignment(0, "broker-x");
    staged.replicas = vec!["broker-y".to_string()];
    staged.successor = Some("broker-y".to_string());
    let written = store.put_shard_assignment(staged).await.expect("stage");
    assert_eq!(written.successor.as_deref(), Some("broker-y"));
    let read = store.get_shard_assignment(&key(0)).await.expect("get");
    assert_eq!(read.successor.as_deref(), Some("broker-y"));

    let mut not_a_replica = read.clone();
    not_a_replica.successor = Some("broker-z".to_string());
    let err = store
        .put_shard_assignment(not_a_replica)
        .await
        .expect_err("a successor outside the replica set");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let mut fenced = read.clone();
    fenced.state = ShardState::Draining;
    store.put_shard_assignment(fenced).await.expect("fence");

    let mut cut_over = assignment(0, "broker-y");
    cut_over.state = ShardState::Assigning;
    let written = store
        .put_shard_assignment(cut_over)
        .await
        .expect("cut over");
    assert_eq!(written.leader, "broker-y");
    assert_eq!(written.successor, None);
    assert_eq!(written.generation, 3);
}

/// A follower being copied in, and when the move started, survive a round
/// trip; a joining follower must be one of the replicas.
async fn a_replacement_in_progress_is_persisted(store: &dyn ControlPlaneStore) {
    clear(store).await;
    let mut replacing = assignment(0, "broker-x");
    replacing.replicas = vec!["broker-y".to_string()];
    replacing.joining = Some("broker-y".to_string());
    replacing.move_started_at_millis = Some(42_000);
    store
        .put_shard_assignment(replacing)
        .await
        .expect("replace");
    let read = store.get_shard_assignment(&key(0)).await.expect("get");
    assert_eq!(read.joining.as_deref(), Some("broker-y"));
    assert_eq!(read.move_started_at_millis, Some(42_000));

    let mut not_a_replica = read.clone();
    not_a_replica.replicas = Vec::new();
    let err = store
        .put_shard_assignment(not_a_replica)
        .await
        .expect_err("a joining follower outside the replica set");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");

    let mut seated = read;
    seated.joining = None;
    seated.move_started_at_millis = None;
    let written = store.put_shard_assignment(seated).await.expect("seat");
    assert_eq!(written.joining, None);
    assert_eq!(written.move_started_at_millis, None);
}

/// Why a move started survives a round trip, and clearing it does too.
async fn why_a_move_started_is_persisted(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("assigning");
    for reason in [
        MoveReason::Drain,
        MoveReason::Balance,
        MoveReason::Operator,
        MoveReason::Replace,
    ] {
        let mut staged = assignment(0, "broker-x");
        staged.replicas = vec!["broker-y".to_string()];
        staged.successor = Some("broker-y".to_string());
        staged.move_reason = Some(reason);
        let written = store.put_shard_assignment(staged).await.expect("stage");
        assert_eq!(written.move_reason, Some(reason));
        let read = store.get_shard_assignment(&key(0)).await.expect("get");
        assert_eq!(read.move_reason, Some(reason));
    }
    let undone = assignment(0, "broker-x");
    let written = store.put_shard_assignment(undone).await.expect("undo");
    assert_eq!(written.move_reason, None);
    let read = store.get_shard_assignment(&key(0)).await.expect("get");
    assert_eq!(read.move_reason, None);
}

/// Moves start unpaused, and pausing and resuming are read back by the next
/// reader, repeated or not.
async fn pausing_moves_is_persisted(store: &dyn ControlPlaneStore) {
    assert!(
        !store.moves_paused().await.expect("read"),
        "starts unpaused"
    );
    store.set_moves_paused(true).await.expect("pause");
    assert!(store.moves_paused().await.expect("read"));
    store.set_moves_paused(true).await.expect("pause again");
    assert!(store.moves_paused().await.expect("read"));
    store.set_moves_paused(false).await.expect("resume");
    assert!(!store.moves_paused().await.expect("read"));
}

/// A conditional write lands only over the generation it names, or where
/// there is no assignment when it names none; anything else writes nothing
/// and publishes nothing.
async fn a_conditional_write_lands_only_at_the_expected_generation(store: &dyn ControlPlaneStore) {
    clear(store).await;

    let first = store
        .put_shard_assignment_if(assignment(0, "broker-x"), None, token(store).await)
        .await
        .expect("create");
    let AssignmentWrite::Written(first) = first else {
        panic!("a write expecting no assignment lands on an empty shard: {first:?}");
    };
    assert_eq!(first.generation, 0);
    let seq = store
        .shard_assignment_snapshot()
        .await
        .expect("snapshot")
        .next_seq;

    assert_eq!(
        store
            .put_shard_assignment_if(assignment(0, "broker-y"), None, token(store).await)
            .await
            .expect("create again"),
        AssignmentWrite::Stale { current: Some(0) },
    );
    assert_eq!(
        store
            .put_shard_assignment_if(assignment(0, "broker-y"), Some(7), token(store).await)
            .await
            .expect("wrong generation"),
        AssignmentWrite::Stale { current: Some(0) },
    );
    assert_eq!(
        store
            .put_shard_assignment_if(assignment(1, "broker-y"), Some(0), token(store).await)
            .await
            .expect("no assignment"),
        AssignmentWrite::Stale { current: None },
    );
    assert!(matches!(
        store.get_shard_assignment(&key(1)).await,
        Err(StoreError::NotFound(_))
    ));
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        first,
        "a stale write leaves the assignment alone",
    );
    assert_eq!(
        store
            .shard_assignment_snapshot()
            .await
            .expect("snapshot")
            .next_seq,
        seq,
        "a stale write publishes no change",
    );

    let second = store
        .put_shard_assignment_if(assignment(0, "broker-y"), Some(0), token(store).await)
        .await
        .expect("update");
    let AssignmentWrite::Written(second) = second else {
        panic!("a write at the current generation lands: {second:?}");
    };
    assert_eq!(second.generation, 1);
    assert_eq!(second.leader, "broker-y");

    // Stale is reported ahead of an invalid transition: the caller planned
    // from an old state, and the transition is judged against a newer one.
    let mut draining = second.clone();
    draining.state = ShardState::Draining;
    store
        .put_shard_assignment_if(draining, Some(1), token(store).await)
        .await
        .expect("drain");
    let mut active = second;
    active.state = ShardState::Active;
    assert_eq!(
        store
            .put_shard_assignment_if(active, Some(1), token(store).await)
            .await
            .expect("stale and disallowed"),
        AssignmentWrite::Stale { current: Some(2) },
    );
}

/// The race placement's conditional writes exist for: two instances plan a
/// fence from the same read, one fences and then cuts over, and the other's
/// fence arrives last. Unconditionally it would hand the shard back to the
/// old leader after the new one may have acknowledged writes.
async fn a_fence_planned_before_a_cut_over_is_refused_after_it(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("assigning");
    let mut staged = assignment(0, "broker-x");
    staged.replicas = vec!["broker-y".to_string()];
    staged.successor = Some("broker-y".to_string());
    let read = store.put_shard_assignment(staged).await.expect("stage");

    let mut fence = read.clone();
    fence.state = ShardState::Draining;
    let fenced = store
        .put_shard_assignment_if(fence.clone(), Some(read.generation), token(store).await)
        .await
        .expect("fence");
    let AssignmentWrite::Written(fenced) = fenced else {
        panic!("the first fence lands: {fenced:?}");
    };
    let cut_over = store
        .put_shard_assignment_if(
            assignment(0, "broker-y"),
            Some(fenced.generation),
            token(store).await,
        )
        .await
        .expect("cut over");
    let AssignmentWrite::Written(cut_over) = cut_over else {
        panic!("the cut-over lands: {cut_over:?}");
    };

    assert_eq!(
        store
            .put_shard_assignment_if(fence, Some(read.generation), token(store).await)
            .await
            .expect("late fence"),
        AssignmentWrite::Stale {
            current: Some(cut_over.generation)
        },
    );
    assert_eq!(
        store.get_shard_assignment(&key(0)).await.expect("get"),
        cut_over
    );
}

/// The question placement asks when a node fails or is drained.
async fn assignments_can_be_listed_by_leader(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    store
        .put_shard_assignment(assignment(1, "broker-y"))
        .await
        .expect("put");
    store
        .put_shard_assignment(assignment(2, "broker-x"))
        .await
        .expect("put");

    let owned = store
        .list_shard_assignments_for_node("broker-x")
        .await
        .expect("by node");
    assert_eq!(
        owned.iter().map(|a| a.key.shard).collect::<Vec<_>>(),
        vec![0, 2],
    );
    assert!(
        store
            .list_shard_assignments_for_node("broker-ghost")
            .await
            .expect("by node")
            .is_empty()
    );
}

async fn missing_assignments_report_not_found(store: &dyn ControlPlaneStore) {
    clear(store).await;
    assert!(matches!(
        store.get_shard_assignment(&key(3)).await.expect_err("get"),
        StoreError::NotFound(_)
    ));
    assert!(matches!(
        store
            .delete_shard_assignment(&key(3))
            .await
            .expect_err("delete"),
        StoreError::NotFound(_)
    ));
}

async fn deleting_leaves_the_shard_unowned(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    let since = store
        .shard_assignment_snapshot()
        .await
        .expect("snapshot")
        .next_seq;

    store
        .delete_shard_assignment(&key(0))
        .await
        .expect("delete");

    assert!(store.get_shard_assignment(&key(0)).await.is_err());
    let changes = store
        .shard_assignment_changes(since)
        .await
        .expect("changes");
    assert_eq!(changes.items.len(), 1);
    assert!(
        changes.items[0].assignment.is_none(),
        "an unassignment carries no body",
    );
}

/// A replica is as much a reference as a leader: an assignment naming a
/// deleted replica could never be rewritten unchanged, since every write
/// checks its nodes exist.
async fn a_node_replicating_a_shard_cannot_be_deleted(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(ShardAssignment {
            replicas: vec!["broker-y".to_string()],
            ..assignment(0, "broker-x")
        })
        .await
        .expect("put");
    let err = store
        .delete_node("broker-y")
        .await
        .expect_err("should refuse");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    clear(store).await;
}

/// Cascading would delete the only record of where that shard's data lives.
async fn a_node_leading_a_shard_cannot_be_deleted(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");

    let err = store
        .delete_node("broker-x")
        .await
        .expect_err("should refuse");
    assert!(matches!(err, StoreError::Conflict(_)), "got {err:?}");
    assert!(
        store.get_node("broker-x").await.is_ok(),
        "node must survive"
    );

    // Reassigning frees it.
    store
        .delete_shard_assignment(&key(0))
        .await
        .expect("delete");
    assert!(store.delete_node("broker-x").await.is_ok());
    store
        .register_node(node("broker-x", 7500))
        .await
        .expect("re-register for later cases");
}

async fn a_snapshot_and_the_changes_after_it_lose_nothing(store: &dyn ControlPlaneStore) {
    clear(store).await;
    store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");

    let snapshot = store.shard_assignment_snapshot().await.expect("snapshot");
    assert_eq!(snapshot.items.len(), 1);

    store
        .put_shard_assignment(assignment(1, "broker-y"))
        .await
        .expect("put");
    store
        .delete_shard_assignment(&key(0))
        .await
        .expect("delete");

    let mut reconstructed: std::collections::BTreeMap<(String, u32), ShardAssignment> = snapshot
        .items
        .into_iter()
        .map(|a| ((a.key.stream.clone(), a.key.shard), a))
        .collect();
    for change in store
        .shard_assignment_changes(snapshot.next_seq)
        .await
        .expect("changes")
        .items
    {
        let id = (change.key.stream.clone(), change.key.shard);
        match change.assignment {
            Some(assignment) => {
                reconstructed.insert(id, assignment);
            }
            None => {
                reconstructed.remove(&id);
            }
        }
    }

    let actual: std::collections::BTreeMap<(String, u32), ShardAssignment> = store
        .list_shard_assignments()
        .await
        .expect("list")
        .into_iter()
        .map(|a| ((a.key.stream.clone(), a.key.shard), a))
        .collect();
    assert_eq!(reconstructed, actual);
}

/// Seeding exposed for the restart test, which needs the catalog in place
/// across two store handles.
///
/// Gated to match its only caller: `postgres::tests::database` is `pg-tests`-only, so
/// without that feature this has no users and is dead code.
#[cfg(feature = "pg-tests")]
pub(crate) async fn seed_for_restart(store: &dyn ControlPlaneStore) {
    seed(store).await;
}

/// Write one assignment and hand it back, for the restart test to compare.
///
/// `pg-tests`-only for the same reason as [`seed_for_restart`].
#[cfg(feature = "pg-tests")]
pub(crate) async fn assign_for_restart(store: &dyn ControlPlaneStore) -> ShardAssignment {
    clear(store).await;
    let first = store
        .put_shard_assignment(assignment(0, "broker-x"))
        .await
        .expect("put");
    let mut active = assignment(0, "broker-x");
    active.state = ShardState::Active;
    let _ = first;
    store.put_shard_assignment(active).await.expect("activate")
}

/// Deleting a stream or a cache must not leave its shards owned.
///
/// Postgres enforces this with `ON DELETE CASCADE`; the point of running it
/// here is that the in-memory store must do the same thing, and the divergence
/// runs the dangerous way — a suite in memory that kept the rows would be
/// asserting against ghosts the deployed system has already cleaned up.
///
/// Uses names of its own rather than the shared fixtures, because the cases
/// after it still need those to exist.
async fn deleting_a_stream_or_cache_takes_its_shard_assignments_with_it(
    store: &dyn ControlPlaneStore,
) {
    const DOOMED: &str = "doomed";

    let stream_key = StreamKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: DOOMED.to_string(),
    };
    let cache_key = CacheKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        cache: DOOMED.to_string(),
    };

    store
        .create_stream(Stream {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: DOOMED.to_string(),
            kind: StreamKind::Stream,
            shards: 2,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
        })
        .await
        .expect("create the stream to delete");
    store
        .create_cache(Cache {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            cache: DOOMED.to_string(),
            display_name: "Doomed".to_string(),
            shards: 2,
            replication_factor: 1,
            consistency: crate::model::ConsistencyLevel::Leader,
        })
        .await
        .expect("create the cache to delete");

    let doomed = |shard: u32, kind: ShardKind| ShardAssignment {
        key: ShardKey {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: DOOMED.to_string(),
            shard,
            kind,
        },
        leader: "broker-x".to_string(),
        replicas: Vec::new(),
        generation: 0,
        state: ShardState::Assigning,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
    };
    for shard in 0..2 {
        store
            .put_shard_assignment(doomed(shard, ShardKind::Stream))
            .await
            .expect("assign a stream shard");
        store
            .put_shard_assignment(doomed(shard, ShardKind::Cache))
            .await
            .expect("assign a cache shard");
    }

    let owned = |assignments: &[ShardAssignment], kind: ShardKind| -> usize {
        assignments
            .iter()
            .filter(|a| a.key.stream == DOOMED && a.key.kind == kind)
            .count()
    };
    let before = store.list_shard_assignments().await.expect("list");
    assert_eq!(owned(&before, ShardKind::Stream), 2);
    assert_eq!(owned(&before, ShardKind::Cache), 2);

    store
        .delete_stream(&stream_key)
        .await
        .expect("delete the stream");
    let after_stream = store.list_shard_assignments().await.expect("list");
    assert_eq!(
        owned(&after_stream, ShardKind::Stream),
        0,
        "the deleted stream's shards are still owned, so placement is chasing \
         a stream that no longer exists",
    );
    assert_eq!(
        owned(&after_stream, ShardKind::Cache),
        2,
        "deleting the stream took the same-named cache's shards with it; the \
         two are distinguished by kind and nothing else",
    );

    store
        .delete_cache(&cache_key)
        .await
        .expect("delete the cache");
    let after_cache = store.list_shard_assignments().await.expect("list");
    assert_eq!(
        owned(&after_cache, ShardKind::Cache),
        0,
        "the deleted cache's shards are still owned",
    );
}
