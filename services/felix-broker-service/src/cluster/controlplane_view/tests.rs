//! The view holds only what the control plane backed, is rewritten only when
//! it changed, at most once per interval, and reads back as written or not
//! at all.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use felix_broker::{CacheMetadata, ConsistencyLevel, StreamMetadata};
use felix_router::{NodeRef, RegionRouter, RoutingTable};

use super::*;
use crate::shards::watch::PendingSet;

const ME: &str = "broker-a";

fn key(stream: &str, shard: u32, kind: ShardKind) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_owned(),
        namespace: "ns".to_owned(),
        stream: stream.to_owned(),
        shard,
        kind,
    }
}

fn assignment(key: ShardKey, leader: &str, replicas: &[&str]) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: leader.to_owned(),
        replicas: replicas.iter().map(|r| (*r).to_owned()).collect(),
        generation: 3,
        state: "active".to_owned(),
        successor: None,
        routing: Default::default(),
        incarnation: 7,
        fence_storage_ids: Default::default(),
        member_storage_ids: [("broker-a".to_owned(), "aa".to_owned())].into(),
        pending_set: None,
        previous_generation: None,
    }
}

fn node(id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: id.to_owned(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
        region: "us-west-2".to_owned(),
        live: true,
    }
}

fn view(incarnation: u64) -> View {
    View {
        node_id: ME.to_owned(),
        storage_id: "00".repeat(16),
        incarnation,
        fleet: vec!["generation_start".to_owned()],
        routes: vec![assignment(
            key("orders", 0, ShardKind::Stream),
            ME,
            &[ME, "broker-b"],
        )],
        nodes: vec![Node {
            node_id: "broker-b".to_owned(),
            advertise_addr: SocketAddr::from(([10, 0, 0, 5], 7002)),
            region: "us-west-2".to_owned(),
        }],
        catalog: Catalog::default(),
    }
}

struct Fixture {
    sources: Sources,
    registered: watch::Sender<Option<u64>>,
}

/// Sources with every part ready but the ones a test leaves out.
async fn fixture() -> Fixture {
    let (registered, registered_rx) = watch::channel(None);
    let fleet = Arc::new(FleetGate::new(["generation_start", "majority_ack"]));
    let router = Arc::new(ShardRouter::new(
        ME,
        "us-west-2",
        RegionRouter::new("us-west-2".to_owned()),
    ));
    let nodes: HashMap<String, NodeRef> = [
        node(ME, 7001),
        node("broker-b", 7002),
        node("broker-c", 7003),
        node("broker-d", 7004),
        // Named by no route of this node's.
        node("broker-x", 7009),
        // The catalog has no address for it.
    ]
    .into_iter()
    .map(|node| (node.node_id.clone(), node))
    .collect();
    router.publish(RoutingTable::default(), &nodes);
    let broker = Arc::new(Broker::new(felix_storage::EphemeralCache::new().into()));
    broker.register_tenant("t1").await.unwrap();
    broker.register_namespace("t1", "ns").await.unwrap();
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                shards: 2,
                consistency: ConsistencyLevel::Quorum,
                incarnation: 7,
                retention: felix_storage::log::Retention {
                    bytes: Some(1 << 20),
                    age: Some(Duration::from_secs(60)),
                },
                ..StreamMetadata::default()
            },
        )
        .await
        .unwrap();
    broker
        .register_cache(
            "t1",
            "ns",
            "sessions",
            CacheMetadata {
                consistency: ConsistencyLevel::Quorum,
                incarnation: 4,
            },
        )
        .await
        .unwrap();
    let mut ownership = ShardOwnership::default();
    let mut moving = assignment(
        key("orders", 1, ShardKind::Stream),
        "broker-b",
        &["broker-b"],
    );
    moving.pending_set = Some(PendingSet {
        replicas: vec!["broker-b".to_owned(), ME.to_owned(), "broker-e".to_owned()],
        generation: 4,
    });
    ownership.reset_from_control_plane(
        vec![
            assignment(key("orders", 0, ShardKind::Stream), ME, &[ME, "broker-c"]),
            moving,
            assignment(
                key("sessions", 0, ShardKind::Cache),
                "broker-d",
                &["broker-d", ME],
            ),
            // Another node's shard, and one of a stream the catalog lacks.
            assignment(
                key("orders", 2, ShardKind::Stream),
                "broker-x",
                &["broker-x"],
            ),
            assignment(key("unknown", 0, ShardKind::Stream), ME, &[ME]),
        ],
        &std::collections::HashSet::new(),
    );
    let catalog_seeded = CancellationToken::new();
    catalog_seeded.cancel();
    fleet.restart(["generation_start"]);
    registered.send_replace(Some(9));
    Fixture {
        sources: Sources {
            node_id: ME.to_owned(),
            storage_id: felix_storage::StorageId::from_bytes([0xab; 16]),
            registered: registered_rx,
            fleet,
            catalog_seeded,
            ownership: Arc::new(RwLock::new(ownership)),
            router,
            broker,
        },
        registered,
    }
}

#[tokio::test]
async fn the_view_holds_this_nodes_shards_and_what_they_need() {
    let fixture = fixture().await;
    let view = fixture.sources.build().await.expect("every part is ready");
    assert_eq!(view.node_id, ME);
    assert_eq!(view.storage_id, "ab".repeat(16));
    assert_eq!(view.incarnation, 9);
    assert_eq!(view.fleet, ["generation_start"]);
    let keys: Vec<_> = view.routes.iter().map(|route| route.key.clone()).collect();
    assert_eq!(
        keys,
        [
            key("orders", 0, ShardKind::Stream),
            // Named only by the set it is moving to.
            key("orders", 1, ShardKind::Stream),
            key("sessions", 0, ShardKind::Cache),
            key("unknown", 0, ShardKind::Stream),
        ]
    );
    let nodes: Vec<_> = view
        .nodes
        .iter()
        .map(|node| node.node_id.as_str())
        .collect();
    // Not broker-x, which no route of this node's names, and not broker-e,
    // which the catalog does not know.
    assert_eq!(nodes, [ME, "broker-b", "broker-c", "broker-d"]);
    // The entries apply back to what was registered, and a stream the
    // catalog has not registered is left out.
    assert_eq!(view.catalog.streams.len(), 1);
    let stream = view.catalog.streams[0].metadata().unwrap();
    assert_eq!(stream.consistency, ConsistencyLevel::Quorum);
    assert_eq!(stream.incarnation, 7);
    assert_eq!(stream.shards, 2);
    assert_eq!(stream.retention.bytes, Some(1 << 20));
    assert_eq!(stream.retention.age, Some(Duration::from_secs(60)));
    assert_eq!(view.catalog.caches.len(), 1);
    assert_eq!(view.catalog.caches[0].cache, "sessions");
    let cache = view.catalog.caches[0].metadata().unwrap();
    assert_eq!(cache.consistency, ConsistencyLevel::Quorum);
    assert_eq!(cache.incarnation, 4);
}

#[tokio::test]
async fn nothing_is_built_before_registration() {
    let fixture = fixture().await;
    fixture.registered.send_replace(None);
    assert_eq!(fixture.sources.build().await, None);
}

#[tokio::test]
async fn nothing_is_built_before_the_fleet_is_adopted() {
    let mut fixture = fixture().await;
    // Seeded from an earlier view but not yet answered by a registration.
    fixture.sources.fleet = Arc::new(FleetGate::seeded(
        ["generation_start"],
        ["generation_start"],
    ));
    assert_eq!(fixture.sources.build().await, None);
}

#[tokio::test]
async fn nothing_is_built_before_the_catalog_is_seeded() {
    let mut fixture = fixture().await;
    fixture.sources.catalog_seeded = CancellationToken::new();
    assert_eq!(fixture.sources.build().await, None);
}

#[tokio::test]
async fn nothing_is_built_before_a_routes_snapshot() {
    let mut fixture = fixture().await;
    let mut ownership = ShardOwnership::default();
    ownership.apply(
        &key("orders", 0, ShardKind::Stream),
        Some(assignment(key("orders", 0, ShardKind::Stream), ME, &[ME])),
    );
    fixture.sources.ownership = Arc::new(RwLock::new(ownership));
    assert_eq!(fixture.sources.build().await, None);
}

/// Routes a start took from the view are not a snapshot this process got
/// from the control plane, so a registration alone does not let them be
/// written back as if it had answered them.
#[tokio::test]
async fn routes_seeded_from_the_view_are_not_a_snapshot() {
    let mut fixture = fixture().await;
    let mut ownership = ShardOwnership::default();
    ownership.reset(vec![assignment(
        key("orders", 0, ShardKind::Stream),
        ME,
        &[ME],
    )]);
    fixture.sources.ownership = Arc::new(RwLock::new(ownership));
    assert_eq!(fixture.sources.build().await, None);
    fixture
        .sources
        .ownership
        .write()
        .await
        .reset_from_control_plane(
            vec![assignment(key("orders", 0, ShardKind::Stream), ME, &[ME])],
            &std::collections::HashSet::new(),
        );
    assert!(fixture.sources.build().await.is_some());
}

#[tokio::test]
async fn a_written_view_reads_back_for_its_node_only() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(ViewFile::new(dir.path().to_path_buf()));
    assert!(writer.write(Some(view(1))).await);
    assert_eq!(read(dir.path(), ME), Some(view(1)));
    assert_eq!(read(dir.path(), "broker-b"), None);
}

#[tokio::test]
async fn a_body_that_does_not_parse_reads_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    felix_storage::controlplane_view::record(dir.path(), b"{\"node_id\":").unwrap();
    assert_eq!(read(dir.path(), ME), None);
}

#[tokio::test]
async fn a_field_a_later_build_adds_is_ignored() {
    let mut body = serde_json::to_value(view(1)).unwrap();
    body["added_later"] = serde_json::json!({"anything": 1});
    let body = serde_json::to_vec(&body).unwrap();
    assert_eq!(View::decode(&body, ME), Some(view(1)));
}

#[tokio::test]
async fn an_unchanged_view_is_not_written_again() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(felix_storage::controlplane_view::FILE_NAME);
    let mut writer = Writer::new(ViewFile::new(dir.path().to_path_buf()));
    assert!(!writer.write(None).await);
    assert!(!path.exists());
    assert!(writer.write(Some(view(1))).await);
    // Removed behind the writer's back: a write would put it back.
    std::fs::remove_file(&path).unwrap();
    assert!(!writer.write(Some(view(1))).await);
    assert!(!path.exists());
    assert!(writer.write(Some(view(2))).await);
    assert_eq!(read(dir.path(), ME), Some(view(2)));
}

#[tokio::test]
async fn a_failed_write_is_tried_again_with_the_next_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(ViewFile::new(dir.path().to_path_buf()));
    assert!(writer.write(Some(view(1))).await);
    let blocker = dir
        .path()
        .join(felix_storage::controlplane_view::FILE_NAME)
        .with_extension("tmp");
    std::fs::create_dir(&blocker).unwrap();
    assert!(!writer.write(Some(view(2))).await);
    assert_eq!(read(dir.path(), ME), Some(view(1)));
    std::fs::remove_dir(&blocker).unwrap();
    // The same view again: it was never written, so it is not "unchanged".
    assert!(writer.write(Some(view(2))).await);
    assert_eq!(read(dir.path(), ME), Some(view(2)));
}

#[tokio::test(start_paused = true)]
async fn writes_come_at_most_once_an_interval_and_once_more_at_a_stop() {
    let dir = tempfile::tempdir().unwrap();
    let builds = Arc::new(AtomicU64::new(0));
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(
        {
            let builds = Arc::clone(&builds);
            // A view that changes every time it is built, so every build is
            // written.
            move || {
                let n = builds.fetch_add(1, Ordering::SeqCst) + 1;
                async move { Some(view(n)) }
            }
        },
        ViewFile::new(dir.path().to_path_buf()),
        shutdown.clone(),
    ));
    tokio::time::sleep(WRITE_EVERY * 5 + WRITE_EVERY / 2).await;
    // The first tick is at once, then one per interval.
    assert_eq!(builds.load(Ordering::SeqCst), 6);
    shutdown.cancel();
    let last = task.await.unwrap();
    assert_eq!(builds.load(Ordering::SeqCst), 7);
    assert_eq!(read(dir.path(), ME), Some(view(7)));
    assert_eq!(last, Some(serde_json::to_vec(&view(7)).unwrap()));
}

/// The body a stopped writer hands back for `n`.
fn body(n: u64) -> Option<Vec<u8>> {
    Some(serde_json::to_vec(&view(n)).unwrap())
}

#[tokio::test]
async fn a_deregistration_that_may_have_reached_the_control_plane_leaves_no_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(ViewFile::new(dir.path().to_path_buf()));
    writer.write(Some(view(1))).await;
    let seen = Arc::new(AtomicU64::new(0));
    let deregister = {
        let seen = Arc::clone(&seen);
        let root = dir.path().to_path_buf();
        async move {
            // Gone before the request goes out, so a crash during it
            // cannot leave a view of sets the control plane has moved on.
            if read(&root, ME).is_none() {
                seen.store(1, Ordering::SeqCst);
            }
            true
        }
    };
    assert!(
        ViewFile::new(dir.path().to_path_buf())
            .forget_while(body(1), deregister)
            .await
    );
    assert_eq!(seen.load(Ordering::SeqCst), 1);
    assert_eq!(read(dir.path(), ME), None);
}

#[tokio::test]
async fn a_forgotten_view_is_not_written_again() {
    let dir = tempfile::tempdir().unwrap();
    let file = ViewFile::new(dir.path().to_path_buf());
    let mut writer = Writer::new(file.clone());
    writer.write(Some(view(1))).await;
    assert!(file.forget_while(body(1), async { true }).await);
    // A write still on its way when the stop gave up on the writer.
    writer.write(Some(view(2))).await;
    assert_eq!(read(dir.path(), ME), None);
}

#[tokio::test]
async fn a_deregistration_that_never_left_keeps_the_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = Writer::new(ViewFile::new(dir.path().to_path_buf()));
    writer.write(Some(view(1))).await;
    assert!(
        ViewFile::new(dir.path().to_path_buf())
            .forget_while(body(1), async { false })
            .await
    );
    assert_eq!(read(dir.path(), ME), Some(view(1)));
}

#[tokio::test]
async fn a_view_that_cannot_be_removed_is_not_deregistered_over() {
    let dir = tempfile::tempdir().unwrap();
    // A directory in the file's place cannot be removed as a file.
    std::fs::create_dir(dir.path().join(felix_storage::controlplane_view::FILE_NAME)).unwrap();
    let asked = Arc::new(AtomicU64::new(0));
    let deregister = {
        let asked = Arc::clone(&asked);
        async move {
            asked.store(1, Ordering::SeqCst);
            true
        }
    };
    assert!(
        !ViewFile::new(dir.path().to_path_buf())
            .forget_while(body(1), deregister)
            .await
    );
    assert_eq!(
        asked.load(Ordering::SeqCst),
        0,
        "deregistered with a view left behind"
    );
}
