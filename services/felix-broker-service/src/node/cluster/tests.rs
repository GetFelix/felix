//! Every user of the broker's engine holds the one the node built.
use std::time::Duration;

use super::*;
use crate::config::MembershipConfig;
use crate::serving::auth::BrokerAuth;
use crate::serving::kafka::BrokerCluster;
use crate::serving::mqtt::BrokerWrites;
use crate::serving::quic::ClusterContext;
use felix_broker::Broker;
use felix_storage::EphemeralCache;

/// **The client listeners, the Kafka and MQTT writes, forwarded writes, the
/// peer handler and the read bounds share one engine.** A second engine would
/// answer from different routing state.
#[tokio::test]
async fn every_component_holds_the_one_engine() {
    let config = BrokerConfig {
        membership: Some(MembershipConfig {
            node_id: "broker-a".to_string(),
            advertise_addr: "127.0.0.1:7001".to_string(),
            client_advertise_addr: None,
            kafka_advertise_addr: None,
            region: "local".to_string(),
            zone: None,
            internal: false,
            durable_storage: true,
            region_bridges: Vec::new(),
            features: Default::default(),
        }),
        ..BrokerConfig::default()
    };
    let cluster = shard_state(&config);
    let (router, ingress, _, _) = cluster.as_ref().expect("shard state");
    let engine = Arc::new(shard_routing::StreamEngine::new(Some(Arc::clone(ingress))));
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let auth = Arc::new(BrokerAuth::new("http://localhost".to_string()));

    let handler = peer_handler(&config, router, &engine, &broker, &auth);
    assert!(Arc::ptr_eq(handler.engine(), &engine), "peer handler");
    assert!(
        Arc::ptr_eq(handler.forwarding().engine(), &engine),
        "forwarding handler"
    );

    let context = ClusterContext {
        engine: Arc::clone(&engine),
        ..ClusterContext::default()
    };
    assert!(Arc::ptr_eq(&context.engine, &engine), "cluster context");

    let writes = BrokerWrites::new(
        Arc::clone(&broker),
        Arc::clone(&engine),
        Duration::from_secs(1),
    );
    assert!(Arc::ptr_eq(writes.engine(), &engine), "mqtt writes");

    let kafka = BrokerCluster::new(
        auth,
        Arc::clone(&engine),
        None,
        "broker-a",
        "127.0.0.1:9092",
    )
    .expect("kafka cluster");
    assert!(Arc::ptr_eq(kafka.engine(), &engine), "kafka");

    // The same shard routing as the one the engine was built from.
    assert!(Arc::ptr_eq(engine.serving().expect("ingress"), ingress));
}

/// **The restore gate runs only after the start's restore check has
/// answered, and its result reaches the members, the heartbeat and the
/// feed.** That check may rotate the storage id, which removes the mark log,
/// so the gate must not have it open before. Once the root passes, the
/// heartbeat reports it, the routing feed is woken and the member opens.
#[cfg(feature = "raft")]
#[tokio::test]
async fn the_raft_root_opens_after_the_restore_check_and_reaches_the_heartbeat() {
    use crate::cluster::membership::RootSequenceSlot;
    use crate::shards::raft::tests as raft;
    use crate::shards::routing::EngineShards;
    use crate::test_support::fake_link::FakeLink;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = raft::broker(root.path()).await;
    let (storage_id, _) =
        felix_storage::storage_identity::load_or_create(root.path(), || [7; 16]).expect("id");
    let members = raft::held_shards(&broker);
    let wake = Arc::new(tokio::sync::Notify::new());
    members.wake_with(Arc::clone(&wake));
    let shards = RaftShards {
        engine_shards: None,
        handler: None,
        members: Some(Arc::clone(&members)),
    };
    let (restore_checked, stop) = (CancellationToken::new(), CancellationToken::new());
    let slot = RootSequenceSlot::default();
    open_raft_root(
        &shards,
        RaftRoot {
            root: root.path().to_path_buf(),
            witness: None,
            restore_checked: restore_checked.clone(),
            votes_unchecked: None,
            // Answers 503: the control plane has no copy to give.
            link: Some(FakeLink::default()),
            node_id: "broker-a".to_string(),
            storage_id,
            credential: Some(NodeCredential::new("token".to_string())),
            slot: slot.clone(),
            stop: stop.clone(),
            rotated: CancellationToken::new(),
        },
        &tokio_util::task::TaskTracker::new(),
    );
    let orders = raft::key("orders", 0);
    let fed = || raft::assignments([raft::assigned(orders.clone(), raft::INCARNATION)]);

    tokio::time::sleep(Duration::from_millis(200)).await;
    members.take(&mut fed()).await;
    assert_eq!(slot.read(), None, "the gate ran before the restore check");
    assert!(members.engine_for(&orders).is_none());
    assert!(
        !root.path().join("raft-marks").exists(),
        "the mark log was opened before the restore check"
    );

    restore_checked.cancel();
    tokio::time::timeout(Duration::from_secs(10), wake.notified())
        .await
        .expect("the feed was not woken");
    assert_eq!(slot.read(), Some((storage_id, 0)));
    members.take(&mut fed()).await;
    assert!(members.engine_for(&orders).is_some());
    raft::until("the heartbeat to report the member's marks", || {
        slot.read().is_some_and(|(_, sequence)| sequence > 0)
    })
    .await;
    stop.cancel();
    members.shutdown().await;
}

/// **A start from the view under the pin rule opens its Raft members before
/// the restore check.** The control plane is down, so the check has not
/// answered: the run witness stands in for it, and the member votes so that
/// the copies still up can elect (#3102). It leads nothing before the
/// broker registers (`raft::tests::a_member_of_an_unregistered_broker_votes_but_does_not_lead`).
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_view_start_under_the_pin_rule_opens_its_raft_root_before_the_restore_check() {
    use crate::cluster::membership::RootSequenceSlot;
    use crate::shards::raft::tests as raft;
    use crate::shards::routing::EngineShards;
    use crate::test_support::fake_link::FakeLink;

    let root = tempfile::tempdir().expect("tempdir");
    let witness = tempfile::tempdir().expect("witness");
    let broker = raft::broker(root.path()).await;
    let (storage_id, _) =
        felix_storage::storage_identity::load_or_create(root.path(), || [7; 16]).expect("id");
    let members = raft::held_shards(&broker);
    let wake = Arc::new(tokio::sync::Notify::new());
    members.wake_with(Arc::clone(&wake));
    let shards = RaftShards {
        engine_shards: None,
        handler: None,
        members: Some(Arc::clone(&members)),
    };
    let (restore_checked, registered, stop) = (
        CancellationToken::new(),
        CancellationToken::new(),
        CancellationToken::new(),
    );
    let slot = RootSequenceSlot::default();
    open_raft_root(
        &shards,
        RaftRoot {
            root: root.path().to_path_buf(),
            witness: Some(witness.path().to_path_buf()),
            restore_checked: restore_checked.clone(),
            votes_unchecked: Some(registered.clone()),
            // Answers 503, as a control plane that is down.
            link: Some(FakeLink::default()),
            node_id: "broker-a".to_string(),
            storage_id,
            credential: Some(NodeCredential::new("token".to_string())),
            slot: slot.clone(),
            stop: stop.clone(),
            rotated: CancellationToken::new(),
        },
        &tokio_util::task::TaskTracker::new(),
    );
    let orders = raft::key("orders", 0);
    let fed = || raft::assignments([raft::assigned(orders.clone(), raft::INCARNATION)]);

    tokio::time::timeout(Duration::from_secs(10), wake.notified())
        .await
        .expect("the root did not open while the restore check was unanswered");
    assert!(!restore_checked.is_cancelled());
    assert_eq!(slot.read(), Some((storage_id, 0)));
    members.take(&mut fed()).await;
    assert!(members.engine_for(&orders).is_some());

    // Once the broker registers the member may lead, so its group forms.
    registered.cancel();
    raft::take_formed(&members, raft::assigned(orders.clone(), raft::INCARNATION)).await;
    stop.cancel();
    members.shutdown().await;
}

/// **A root found behind the control plane's copy at open is given a new
/// storage id and the broker stops.** This run registered under the old id,
/// so nothing opens on the new one until the restart registers it.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_root_behind_the_control_plane_rotates_and_stops_the_broker() {
    use crate::cluster::membership::RootSequenceSlot;
    use crate::shards::raft::tests as raft;
    use crate::shards::routing::EngineShards;
    use crate::test_support::fake_link::FakeLink;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = raft::broker(root.path()).await;
    let (storage_id, _) =
        felix_storage::storage_identity::load_or_create(root.path(), || [7; 16]).expect("id");
    let members = raft::held_shards(&broker);
    let shards = RaftShards {
        engine_shards: None,
        handler: None,
        members: Some(Arc::clone(&members)),
    };
    // The control plane heard sequence 5 under this id; the root has none.
    let link = FakeLink::default().reply(
        "node",
        serde_json::json!({
            "node": {
                "node_id": "broker-a",
                "status": {
                    "incarnation": 1,
                    "lifecycle": "live",
                    "storage_id": storage_id.to_string(),
                },
            },
            "placement": { "eligible": true },
            "root_sequence": { "storage_id": storage_id.to_string(), "sequence": 5 },
        }),
    );
    let (restore_checked, stop, rotated) = (
        CancellationToken::new(),
        CancellationToken::new(),
        CancellationToken::new(),
    );
    restore_checked.cancel();
    let slot = RootSequenceSlot::default();
    open_raft_root(
        &shards,
        RaftRoot {
            root: root.path().to_path_buf(),
            witness: None,
            restore_checked,
            votes_unchecked: None,
            link: Some(link),
            node_id: "broker-a".to_string(),
            storage_id,
            credential: Some(NodeCredential::new("token".to_string())),
            slot: slot.clone(),
            stop: stop.clone(),
            rotated: rotated.clone(),
        },
        &tokio_util::task::TaskTracker::new(),
    );
    tokio::time::timeout(Duration::from_secs(10), rotated.cancelled())
        .await
        .expect("the broker was not told to stop");
    let now = felix_storage::storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    assert_ne!(now, storage_id, "the root kept its storage id");
    assert_eq!(slot.read(), None, "the rotated root reached the heartbeat");
    let orders = raft::key("orders", 0);
    members
        .take(&mut raft::assignments([raft::assigned(
            orders.clone(),
            raft::INCARNATION,
        )]))
        .await;
    assert!(members.engine_for(&orders).is_none(), "a member opened");
    stop.cancel();
    members.shutdown().await;
}

/// **A root under a storage id this run did not register under stops the
/// broker instead of opening.** That is the root a rotation leaves when it
/// fails after writing the new id: the gate passes it at sequence 0, and the
/// retry must not open members the control plane never heard of.
#[cfg(feature = "raft")]
#[tokio::test]
async fn a_raft_root_under_an_unregistered_storage_id_stops_the_broker() {
    use crate::cluster::membership::RootSequenceSlot;
    use crate::shards::raft::tests as raft;
    use crate::shards::routing::EngineShards;
    use crate::test_support::fake_link::FakeLink;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = raft::broker(root.path()).await;
    let (registered, _) =
        felix_storage::storage_identity::load_or_create(root.path(), || [7; 16]).expect("id");
    // The rotation's id write, with nothing after it.
    let on_disk = felix_storage::run_state::copy_trust::rotate_storage_id(root.path(), [8; 16])
        .expect("rotate");
    let members = raft::held_shards(&broker);
    let shards = RaftShards {
        engine_shards: None,
        handler: None,
        members: Some(Arc::clone(&members)),
    };
    let (restore_checked, stop, rotated) = (
        CancellationToken::new(),
        CancellationToken::new(),
        CancellationToken::new(),
    );
    restore_checked.cancel();
    let slot = RootSequenceSlot::default();
    open_raft_root(
        &shards,
        RaftRoot {
            root: root.path().to_path_buf(),
            witness: None,
            restore_checked,
            votes_unchecked: None,
            // Answers 503: no copy, so the gate alone would pass the root.
            link: Some(FakeLink::default()),
            node_id: "broker-a".to_string(),
            storage_id: registered,
            credential: Some(NodeCredential::new("token".to_string())),
            slot: slot.clone(),
            stop: stop.clone(),
            rotated: rotated.clone(),
        },
        &tokio_util::task::TaskTracker::new(),
    );
    tokio::time::timeout(Duration::from_secs(10), rotated.cancelled())
        .await
        .expect("the broker was not told to stop");
    assert_eq!(
        felix_storage::storage_identity::load(root.path()).expect("id"),
        Some(on_disk)
    );
    assert_eq!(slot.read(), None, "the root reached the heartbeat");
    let orders = raft::key("orders", 0);
    members
        .take(&mut raft::assignments([raft::assigned(
            orders.clone(),
            raft::INCARNATION,
        )]))
        .await;
    assert!(members.engine_for(&orders).is_none(), "a member opened");
    stop.cancel();
    members.shutdown().await;
}

/// Another engine that holds, leads and serves one shard, and names it as a
/// backup member.
struct HoldsOne(crate::shards::ShardKey);

impl shard_routing::EngineShards for HoldsOne {
    fn take<'a>(
        &'a self,
        _assignments: &'a mut felix_common::random::HashMap<
            crate::shards::ShardKey,
            shard_watch::ShardAssignment,
        >,
    ) -> std::pin::Pin<Box<dyn Future<Output = shard_routing::Taken> + Send + 'a>> {
        Box::pin(async { shard_routing::Taken::new() })
    }

    fn dispatch(
        &self,
        _key: &crate::shards::ShardKey,
        _nodes: &dyn Fn(&str) -> Option<std::net::SocketAddr>,
    ) -> Option<shard_routing::Dispatch> {
        Some(shard_routing::Dispatch::Local { generation: 7 })
    }

    fn elected_leader(&self, key: &crate::shards::ShardKey) -> Option<String> {
        (*key == self.0).then(|| "broker-b".to_string())
    }

    fn serves_reads(&self, key: &crate::shards::ShardKey) -> bool {
        *key == self.0
    }

    fn holds(&self, key: &crate::shards::ShardKey) -> bool {
        *key == self.0
    }

    fn leads_unheard(&self, key: &crate::shards::ShardKey) -> bool {
        *key == self.0
    }

    fn placed_elsewhere(&self, key: &crate::shards::ShardKey) -> bool {
        *key == self.0
    }

    fn backup_members(&self) -> Vec<shard_routing::BackupMember> {
        vec![shard_routing::BackupMember {
            key: self.0.clone(),
            incarnation: 1,
            member: felix_replication::raft::MemberId::new("broker-a", 1),
            term: 7,
            voter_sets: vec![
                ["broker-a", "broker-b", "broker-c"]
                    .map(|broker| felix_replication::raft::MemberId::new(broker, 1))
                    .into(),
            ],
            membership_at: Some(felix_replication::raft::LogPoint { term: 6, index: 3 }),
            committed_end: None,
            end_term: None,
        }]
    }

    fn wake_with(&self, _wake: Arc<tokio::sync::Notify>) {}

    fn follow_lease(&self, _lease: Arc<crate::cluster::lease::LeaseState>) {}

    fn answered(&self, _as_of: Option<tokio::time::Instant>, _checkpoint: Option<u64>) {}

    fn shutdown(&self) -> std::pin::Pin<Box<dyn Future<Output = bool> + Send + '_>> {
        Box::pin(async { true })
    }
}

/// **`GET /backup/offsets` on a clustered broker lists the members its
/// other engine holds, through the engine check the node puts in front of
/// it.** Built the way `node::run` builds it: `match_engines` wraps the
/// engine in `EngineMatch` and hands the wrapper to the ingress router the
/// endpoint reads. The wrapper once left `backup_members` to the trait's
/// empty default, so every broker in a Raft cluster listed no member and
/// `felixctl backup-point` fell back to asking placement's leader, which
/// answered "not led there". The unit tests on the engine alone passed.
#[tokio::test]
async fn the_backup_endpoint_sees_the_members_behind_the_engine_check() {
    let config = BrokerConfig {
        membership: Some(MembershipConfig {
            node_id: "broker-a".to_string(),
            advertise_addr: "127.0.0.1:7001".to_string(),
            client_advertise_addr: None,
            kafka_advertise_addr: None,
            region: "local".to_string(),
            zone: None,
            internal: false,
            durable_storage: true,
            region_bridges: Vec::new(),
            features: Default::default(),
        }),
        peer_transport: Some(peer::PeerTransportConfig::default()),
        ..BrokerConfig::default()
    };
    let shutdown = CancellationToken::new();
    let peers = connect_peers(&config, &shutdown).expect("peers").pool;
    assert!(peers.is_some(), "the engine check needs a peer pool");
    let cluster = shard_state(&config);
    let ingress = cluster
        .as_ref()
        .map(|(_, ingress, _, _)| Arc::clone(ingress));
    let engine = Arc::new(shard_routing::StreamEngine::new(ingress.clone()));
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()));
    let key = crate::shards::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Stream,
    };
    let shards = RaftShards {
        engine_shards: Some(Arc::new(HoldsOne(key.clone()))),
        ..RaftShards::default()
    };
    let shards = match_engines(&config, &cluster, &peers, &broker, &engine, true, shards);
    assert!(shards.engine_shards.is_some());

    // Every question the routes ask reaches the engine through the check.
    let router = ingress.as_ref().expect("ingress");
    assert!(router.engine_holds(&key), "holds");
    assert!(router.engine_serves_reads(&key), "serves_reads");
    assert!(router.engine_leads_unheard(&key), "leads_unheard");
    assert!(router.engine_placed_elsewhere(&key), "placed_elsewhere");
    assert_eq!(
        router.engine_elected_leader(&key).as_deref(),
        Some("broker-b"),
        "elected_leader"
    );
    assert_eq!(router.engine_backup_members().len(), 1, "backup_members");

    let offsets = crate::observability::backup::BackupOffsets {
        node_id: Some("broker-a".to_string()),
        broker,
        ingress,
    };
    let answer = serde_json::to_value(offsets.collect().await).expect("json");
    let members = answer["raft_members"].as_array().expect("raft_members");
    assert_eq!(members.len(), 1, "{answer}");
    assert_eq!(members[0]["name"], "orders");
    assert_eq!(members[0]["term"], 7);
    assert_eq!(
        members[0]["member"],
        serde_json::json!({ "broker": "broker-a", "copy": 1 })
    );
    assert_eq!(members[0]["voters"][0].as_array().map(Vec::len), Some(3));
    assert_eq!(members[0]["membership_at"], serde_json::json!([6, 3]));
    shutdown.cancel();
}
