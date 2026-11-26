use std::collections::BTreeSet;

use super::*;
use crate::model::NodeLifecycle;
use crate::store::contract::nodes::node;

fn at(id: &str, lifecycle: NodeLifecycle, features: &[&str]) -> Node {
    let mut node = node(id, 7000);
    node.status.lifecycle = lifecycle;
    node.status.features = features.iter().map(|f| f.to_string()).collect();
    node
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|f| f.to_string()).collect()
}

#[test]
fn down_and_departed_nodes_leave_the_supported_set() {
    let nodes = [
        at("a", NodeLifecycle::Live, &["x", "y"]),
        at("b", NodeLifecycle::Draining, &["x", "y"]),
        at("c", NodeLifecycle::Down, &[]),
        at("d", NodeLifecycle::Left, &["x"]),
    ];
    assert_eq!(supported(&nodes), set(&["x", "y"]));
}

#[test]
fn an_old_broker_that_reports_nothing_holds_everything_back() {
    let nodes = [
        at("a", NodeLifecycle::Live, &["x"]),
        at("b", NodeLifecycle::Live, &[]),
    ];
    assert!(supported(&nodes).is_empty());
    assert_eq!(lacking(&nodes, "x"), vec!["b".to_string()]);
}

#[test]
fn a_joiner_lacking_an_enabled_feature_is_refused() {
    let refused = admit(&set(&["x"]), &at("c", NodeLifecycle::Live, &["y"]));
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("feature(s) x that")),
        "{refused:?}"
    );
    assert!(admit(&set(&["x"]), &at("c", NodeLifecycle::Live, &["x", "y"])).is_ok());
}

#[test]
fn a_joiner_reading_by_lease_is_told_to_unset_it() {
    let refused = admit(
        &set(&["lease_free_reads"]),
        &at("c", NodeLifecycle::Live, &["majority_ack"]),
    );
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message))
            if message.contains("unset FELIX_QUORUM_READS") && message.contains("restart the broker")),
        "{refused:?}"
    );
    let other = admit(&set(&["x"]), &at("c", NodeLifecycle::Live, &[]));
    assert!(
        matches!(&other, Err(StoreError::Conflict(message)) if !message.contains("FELIX_QUORUM_READS")),
        "{other:?}"
    );
}

#[test]
fn a_joiner_without_the_fence_is_told_to_unset_it() {
    let refused = admit(
        &set(&["generation_start", "majority_ack"]),
        &at("c", NodeLifecycle::Live, &["generation_start"]),
    );
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message))
            if message.contains("unset FELIX_INTERNAL_FENCE")
                && !message.contains("FELIX_QUORUM_READS")),
        "{refused:?}"
    );
}

#[test]
fn nothing_is_refused_before_a_finalize() {
    assert!(admit(&set(&[]), &at("c", NodeLifecycle::Live, &[])).is_ok());
}

#[test]
fn finalizing_needs_every_serving_broker() {
    let nodes = [
        at("a", NodeLifecycle::Live, &["x"]),
        at("b", NodeLifecycle::Draining, &[]),
        at("c", NodeLifecycle::Down, &[]),
    ];
    let refused = check_finalize(&nodes, "x");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("broker(s) b do")),
        "{refused:?}"
    );
    // A down broker does not hold it back; it is refused when it returns.
    assert!(check_finalize(&nodes[..1], "x").is_ok());
    assert!(check_finalize(&nodes[2..], "x").is_err(), "nothing serving");
}

#[test]
fn follower_features_wait_for_generation_start() {
    let refused = check_prerequisites(&set(&[]), "majority_ack");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("finalize generation_start first")),
        "{refused:?}"
    );
    let refused = check_prerequisites(&set(&["majority_ack"]), "lease_free_reads");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("finalize generation_start first")),
        "{refused:?}"
    );
    assert_eq!(
        missing_prerequisites(&set(&["generation_start"]), "lease_free_reads"),
        vec!["majority_ack".to_string()]
    );
    assert!(check_prerequisites(&set(&[]), "generation_start").is_ok());
    assert!(check_prerequisites(&set(&["generation_start"]), "majority_ack").is_ok());
    assert!(
        check_prerequisites(
            &set(&["generation_start", "majority_ack"]),
            "lease_free_reads"
        )
        .is_ok()
    );
    // A fleet that finalized out of order is not refused a repeat.
    assert!(check_prerequisites(&set(&["majority_ack"]), "majority_ack").is_ok());
}

/// A new cluster starts with every feature this build implements but
/// `publisher_principal` and `record_generations`; a feature added later
/// fails this until it picks.
#[test]
fn a_new_cluster_leaves_publisher_principal_to_the_operator() {
    let manual: Vec<_> = felix_common::fleet::IMPLEMENTED
        .iter()
        .filter(|feature| !NEW_CLUSTER.contains(feature))
        .collect();
    assert_eq!(
        manual,
        [
            &felix_common::fleet::PUBLISHER_PRINCIPAL,
            &felix_common::fleet::RECORD_GENERATIONS
        ]
    );
    assert_eq!(
        NEW_CLUSTER.len() + 2,
        felix_common::fleet::IMPLEMENTED.len()
    );
}

/// `finalize_in_order` skips the election gate, which only the election
/// features meet. Holds for a fleet that reports nothing and a level that
/// is not ready.
#[test]
fn a_new_cluster_finalizes_nothing_the_election_gate_checks() {
    let nodes = [at("a", NodeLifecycle::Live, &[])];
    for feature in NEW_CLUSTER {
        assert!(
            check_election(&nodes, feature.name(), false).is_ok(),
            "{feature} is gated by check_election; finalize_in_order must check it"
        );
    }
}

/// Finalized in order, a feature never meets a prerequisite still off.
#[test]
fn a_new_cluster_finalizes_prerequisites_first() {
    let mut enabled = BTreeSet::new();
    for feature in NEW_CLUSTER {
        assert!(
            check_prerequisites(&enabled, feature.name()).is_ok(),
            "{feature} before what it needs"
        );
        enabled.insert(feature.name().to_string());
    }
}

/// Only a broker under `FELIX_INTERNAL_ELECTION` reports the election features,
/// so a fleet without it cannot finalize them even with every prerequisite
/// enabled, and a new cluster leaves them to the operator.
#[test]
fn the_election_features_cannot_be_finalized_yet() {
    let reported: Vec<&str> = felix_common::fleet::IMPLEMENTED
        .iter()
        .map(|feature| feature.name())
        .collect();
    let nodes = [
        at("a", NodeLifecycle::Live, &reported),
        at("b", NodeLifecycle::Live, &reported),
    ];
    for feature in ["joint_fence", "replica_election"] {
        let refused = check_finalize(&nodes, feature);
        assert!(
            matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("do not support it")),
            "{feature}: {refused:?}"
        );
        assert!(!NEW_CLUSTER.iter().any(|f| f.name() == feature));
    }
    // The prerequisites are still enforced in order.
    let refused = check_prerequisites(&set(&["generation_start"]), "replica_election");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("majority_ack")),
        "{refused:?}"
    );
    // A learner holds the start record only where leaders write one.
    let refused = check_prerequisites(&set(&[]), "joint_fence");
    assert!(
        matches!(&refused, Err(StoreError::Conflict(message)) if message.contains("generation_start")),
        "{refused:?}"
    );
    assert!(
        check_prerequisites(
            &set(&[
                "generation_start",
                "stream_incarnations",
                "cache_incarnations"
            ]),
            "joint_fence"
        )
        .is_ok()
    );
    assert!(
        check_prerequisites(
            &set(&[
                "generation_start",
                "majority_ack",
                "lease_free_reads",
                "stream_incarnations",
                "cache_incarnations",
                "joint_fence"
            ]),
            "replica_election"
        )
        .is_ok()
    );
}

/// A broker of this build that offers `ELECTION`: it also names start
/// holders and keeps a wiped copy from voting.
fn offering_election(id: &str, features: &[&str]) -> Node {
    let mut reported = features.to_vec();
    reported.extend(["peer_election", "start_holders", "non_voting"]);
    at(id, NodeLifecycle::Live, &reported)
}

fn conflict(result: Result<(), StoreError>) -> String {
    match result {
        Err(StoreError::Conflict(message)) => message,
        other => panic!("expected a conflict, got {other:?}"),
    }
}

#[test]
fn joint_fence_is_refused_while_a_serving_broker_lacks_election() {
    let nodes = [
        offering_election("a", &["joint_fence"]),
        at("b", NodeLifecycle::Live, &["joint_fence"]),
        // Down brokers do not count.
        at("c", NodeLifecycle::Down, &[]),
    ];
    let message = conflict(check_election(&nodes, "joint_fence", false));
    assert!(
        message.contains("broker(s) b do not offer the ELECTION"),
        "{message}"
    );
    assert!(check_election(&nodes[..1], "joint_fence", false).is_ok());
}

/// A broker that never names start holders would keep a replaced copy a
/// learner for good, so joint fences wait for every serving broker to.
#[test]
fn joint_fence_is_refused_while_a_serving_broker_names_no_start_holders() {
    let nodes = [
        offering_election("a", &["joint_fence"]),
        at("b", NodeLifecycle::Live, &["joint_fence", "peer_election"]),
    ];
    let message = conflict(check_election(&nodes, "joint_fence", false));
    assert!(
        message.contains("broker(s) b do not report which followers hold"),
        "{message}"
    );
    assert!(check_election(&nodes[..1], "joint_fence", false).is_ok());
    // Election itself does not ask for them.
    assert!(check_election(&nodes, "replica_election", true).is_ok());
}

/// A broker that lets a wiped copy vote could elect it or count its acks
/// before it holds them, so joint fences wait for every serving broker to
/// report `non_voting`, whatever order builds were released in.
#[test]
fn joint_fence_is_refused_while_a_serving_broker_lets_a_wiped_copy_vote() {
    let nodes = [
        offering_election("a", &["joint_fence"]),
        at(
            "b",
            NodeLifecycle::Live,
            &["joint_fence", "peer_election", "start_holders"],
        ),
    ];
    let message = conflict(check_election(&nodes, "joint_fence", false));
    assert!(
        message.contains("broker(s) b let a wiped copy vote"),
        "{message}"
    );
    assert!(check_election(&nodes[..1], "joint_fence", false).is_ok());
    // Election itself does not ask for it.
    assert!(check_election(&nodes, "replica_election", true).is_ok());
}

#[test]
fn replica_election_is_refused_while_a_serving_broker_lacks_election() {
    let nodes = [
        offering_election("a", &["replica_election"]),
        at("b", NodeLifecycle::Draining, &["replica_election"]),
    ];
    let message = conflict(check_election(&nodes, "replica_election", true));
    assert!(
        message.contains("broker(s) b do not offer the ELECTION"),
        "{message}"
    );
    assert!(check_election(&nodes[..1], "replica_election", true).is_ok());
}

#[test]
fn replica_election_is_refused_below_the_metadata_level() {
    let nodes = [offering_election("a", &["replica_election"])];
    let message = conflict(check_election(&nodes, "replica_election", false));
    assert!(
        message.contains("metadata level") && message.contains("does not have"),
        "{message}"
    );
    // joint_fence needs no metadata level.
    assert!(check_election(&nodes, "joint_fence", false).is_ok());
}

/// Under joint_fence a broker opens a new stream or cache only once its
/// shard's incarnation says nothing came before it (#1862).
#[test]
fn joint_fence_is_refused_until_incarnations_are_finalized() {
    for missing in ["stream_incarnations", "cache_incarnations"] {
        let have: Vec<&str> = [
            "generation_start",
            "stream_incarnations",
            "cache_incarnations",
        ]
        .into_iter()
        .filter(|name| *name != missing)
        .collect();
        let message = conflict(check_prerequisites(&set(&have), "joint_fence"));
        assert!(message.contains(missing), "{message}");
    }
}

#[test]
fn replica_election_is_refused_until_joint_fence_and_the_rest_are_finalized() {
    let all = [
        "generation_start",
        "majority_ack",
        "lease_free_reads",
        "stream_incarnations",
        "cache_incarnations",
    ];
    let message = conflict(check_prerequisites(&set(&all), "replica_election"));
    assert!(message.contains("joint_fence"), "{message}");
    for missing in all {
        let have: Vec<&str> = all
            .iter()
            .copied()
            .filter(|name| *name != missing)
            .chain(["joint_fence"])
            .collect();
        let message = conflict(check_prerequisites(&set(&have), "replica_election"));
        assert!(message.contains(missing), "{message}");
    }
    let mut ready = all.to_vec();
    ready.push("joint_fence");
    assert!(check_prerequisites(&set(&ready), "replica_election").is_ok());
}

#[test]
fn other_features_pass_the_election_check() {
    let nodes = [at("a", NodeLifecycle::Live, &["majority_ack"])];
    assert!(check_election(&nodes, "majority_ack", false).is_ok());
}

/// Replication carries the writer's generation, so the operator may finalize
/// `record_generations`.
#[test]
fn record_generations_can_be_finalized() {
    check_operator_finalizable("record_generations").expect("finalizable");
    assert!(check_operator_finalizable("head_trim").is_ok());
}

type Shared = Arc<dyn crate::store::ControlPlaneStore + Send + Sync>;

fn memory() -> Arc<crate::store::memory::InMemoryStore> {
    Arc::new(crate::store::memory::InMemoryStore::new(
        crate::store::StoreConfig {
            changes_limit: crate::config::DEFAULT_CHANGES_LIMIT,
            change_retention_max_rows: Some(1_000),
        },
    ))
}

/// What an operator's seed job creates before any broker is up: a stream
/// and a cache, named as `contract::shards::assignment` expects.
async fn seed_job(store: &dyn crate::store::ControlPlaneStore) {
    use crate::model::{
        Cache, ConsistencyLevel, DeliveryGuarantee, Namespace, RetentionPolicy, Stream, StreamKind,
        Tenant,
    };
    store
        .create_tenant(Tenant {
            tenant_id: "shard-t".to_string(),
            display_name: "Seed".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: "shard-t".to_string(),
            namespace: "shard-ns".to_string(),
            display_name: "Seed".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "shard-t".to_string(),
            namespace: "shard-ns".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 1,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
            routing: Default::default(),
            incarnation: 0,
            mount: None,
        })
        .await
        .expect("stream");
    store
        .create_cache(Cache {
            tenant_id: "shard-t".to_string(),
            namespace: "shard-ns".to_string(),
            cache: "sessions".to_string(),
            display_name: "Sessions".to_string(),
            shards: 1,
            replication_factor: 1,
            consistency: ConsistencyLevel::Leader,
            incarnation: 0,
        })
        .await
        .expect("cache");
}

fn current(id: &str, port: u16) -> Node {
    let all: Vec<&str> = felix_common::fleet::IMPLEMENTED
        .iter()
        .map(|feature| feature.name())
        .collect();
    crate::store::contract::nodes::with_features(node(id, port), &all)
}

/// What the registration handler does, in its order: check placement,
/// register, then finalize. `between` runs after the registration landed,
/// where the placement pass it wakes may place a shard.
async fn register<F, Fut>(store: &Shared, node: Node, between: F) -> BTreeSet<String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let never_placed = never_placed(store.as_ref()).await;
    let (_, enabled) = store.register_node_in_fleet(node).await.expect("register");
    between().await;
    finalize_new_cluster(
        store,
        &Arc::new(NewClusterFinalize::default()),
        never_placed,
        enabled,
    )
    .await
}

fn new_cluster_names() -> BTreeSet<String> {
    NEW_CLUSTER
        .iter()
        .map(|feature| feature.name().to_string())
        .collect()
}

/// **Streams created before the first broker do not make a cluster old.**
/// The first registration answers with the whole new-cluster set, including
/// `raft_written_commit`, even when placement puts the stream's shard on the
/// broker before the finalize runs. A broker opens its Raft members only
/// after that answer, from it, so they open relaxed.
#[tokio::test]
async fn streams_created_before_the_first_broker_are_finalized_before_it_opens_a_member() {
    let store = memory();
    seed_job(store.as_ref()).await;
    let shared = Arc::clone(&store) as Shared;

    let placing = Arc::clone(&shared);
    let enabled = register(&shared, current("broker-a", 7001), || async move {
        placing
            .put_shard_assignment(crate::store::contract::shards::assignment(0, "broker-a"))
            .await
            .expect("placed on the registering broker");
    })
    .await;
    assert!(
        enabled.contains(felix_common::fleet::RAFT_WRITTEN_COMMIT.name()),
        "{enabled:?}"
    );
    assert_eq!(enabled, new_cluster_names());

    let enabled = register(&shared, current("broker-b", 7002), || async {}).await;
    assert_eq!(enabled, new_cluster_names());
}

/// **A restored store is not new**, though its backup named no broker and
/// no placement: brokers that ran after the backup hold data it does not
/// know of.
#[tokio::test]
async fn a_restored_store_is_not_new() {
    let store = memory();
    seed_job(store.as_ref()).await;
    let mut backup = serde_json::to_value(store.export_state().await).expect("export");
    backup["placement_incarnation"] = 1.into();
    store
        .import_state(serde_json::from_value(backup).expect("state"))
        .await
        .expect("import");
    let shared = Arc::clone(&store) as Shared;

    let enabled = register(&shared, current("broker-a", 7001), || async {}).await;
    assert!(enabled.is_empty(), "{enabled:?}");
}

/// **A store that ever placed a shard is not new**, even once the shard,
/// its stream and the broker's record are gone: that broker, here one that
/// predates every feature, may still hold the shard's data and come back.
#[tokio::test]
async fn a_store_that_placed_a_shard_on_a_broker_since_gone_is_not_new() {
    let store = memory();
    seed_job(store.as_ref()).await;
    let shared = Arc::clone(&store) as Shared;

    let enabled = register(&shared, node("broker-old", 7000), || async {}).await;
    assert!(enabled.is_empty(), "{enabled:?}");
    shared
        .put_shard_assignment(crate::store::contract::shards::assignment(0, "broker-old"))
        .await
        .expect("placed");
    shared
        .delete_stream(&crate::model::StreamKey {
            tenant_id: "shard-t".to_string(),
            namespace: "shard-ns".to_string(),
            stream: "orders".to_string(),
        })
        .await
        .expect("delete stream");
    shared
        .set_node_lifecycle("broker-old", NodeLifecycle::Down)
        .await
        .expect("expired");
    shared.delete_node("broker-old").await.expect("forgotten");
    assert!(shared.list_nodes().await.expect("nodes").is_empty());

    assert!(!never_placed(shared.as_ref()).await.expect("placed"));
    let enabled = register(&shared, current("broker-a", 7001), || async {}).await;
    assert!(enabled.is_empty(), "{enabled:?}");
}

/// **A broker that registered before and expired is not new** when it comes
/// back: it takes the next node incarnation.
#[tokio::test]
async fn a_broker_that_registers_again_is_not_new() {
    let store = memory();
    seed_job(store.as_ref()).await;
    let shared = Arc::clone(&store) as Shared;
    shared
        .register_node(current("broker-a", 7001))
        .await
        .expect("earlier run");
    shared
        .set_node_lifecycle("broker-a", NodeLifecycle::Down)
        .await
        .expect("expired");

    let enabled = register(&shared, current("broker-a", 7001), || async {}).await;
    assert!(enabled.is_empty(), "{enabled:?}");
}

/// **An older broker registering first keeps what it lacks off**, streams
/// or not, and a current broker after it does not turn them on over it.
#[tokio::test]
async fn an_older_first_broker_keeps_a_seeded_cluster_unfinalized() {
    let store = memory();
    seed_job(store.as_ref()).await;
    let shared = Arc::clone(&store) as Shared;

    let enabled = register(&shared, node("broker-old", 7000), || async {}).await;
    assert!(enabled.is_empty(), "{enabled:?}");
    let enabled = register(&shared, current("broker-a", 7001), || async {}).await;
    assert!(enabled.is_empty(), "{enabled:?}");
}
