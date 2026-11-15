use std::path::Path;

use felix_broker::{ConsistencyLevel, StreamMetadata};
use felix_replication::raft::{
    AgreeingPeers, RaftReply, RaftRequest, RaftTransport, Refused, Role, RuntimeConfig,
};

use super::*;

pub(crate) const INCARNATION: u64 = 3;

pub(crate) fn key(stream: &str, shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard,
        kind: ShardKind::Stream,
    }
}

pub(crate) fn assigned(key: ShardKey, incarnation: u64) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: "broker-a".to_string(),
        replicas: vec!["broker-a".to_string()],
        generation: 1,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
    }
}

pub(crate) fn assignments(
    all: impl IntoIterator<Item = ShardAssignment>,
) -> HashMap<ShardKey, ShardAssignment> {
    let mut map = HashMap::default();
    for assignment in all {
        map.insert(assignment.key.clone(), assignment);
    }
    map
}

/// A broker that leaves durable streams to their engines, with the durable
/// stream `orders` at [`INCARNATION`] and the in-memory stream `scratch`.
pub(crate) async fn broker(root: &Path) -> Arc<Broker> {
    broker_of(root, 1).await
}

/// [`broker`], with `orders` asking for `replicas` brokers a shard.
pub(crate) async fn broker_of(root: &Path, replicas: u32) -> Arc<Broker> {
    broker_at(
        root,
        replicas,
        INCARNATION,
        ConsistencyLevel::Leader,
        FsyncMode::None,
    )
    .await
}

/// [`broker_of`], with `orders` at `incarnation` and `consistency`, and
/// the storage at `fsync`.
pub(crate) async fn broker_at(
    root: &Path,
    replicas: u32,
    incarnation: u64,
    consistency: ConsistencyLevel,
    fsync: FsyncMode,
) -> Arc<Broker> {
    let config = LogConfig {
        fsync_mode: fsync,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    broker_with_config(root, replicas, incarnation, consistency, config).await
}

/// [`broker_at`], with the storage opened on `config`.
pub(crate) async fn broker_with_config(
    root: &Path,
    replicas: u32,
    incarnation: u64,
    consistency: ConsistencyLevel,
    config: LogConfig,
) -> Arc<Broker> {
    let broker = Broker::new(felix_storage::EphemeralCache::new().into());
    registered(broker, root, replicas, incarnation, consistency, config).await
}

/// [`broker_with_config`] on `broker`, for a test that needs it built
/// another way.
pub(crate) async fn registered(
    broker: Broker,
    root: &Path,
    replicas: u32,
    incarnation: u64,
    consistency: ConsistencyLevel,
    config: LogConfig,
) -> Arc<Broker> {
    let storage = felix_broker::DurableStorage::open(root, config).expect("storage");
    let broker = broker.with_durable_storage(storage);
    broker.require_registered_streams();
    broker.leave_durable_streams_to_engines();
    broker.register_tenant("t1").await.expect("tenant");
    broker.register_namespace("t1", "ns").await.expect("ns");
    broker
        .register_stream(
            "t1",
            "ns",
            "orders",
            StreamMetadata {
                durable: true,
                incarnation,
                replication_factor: Some(replicas),
                consistency,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("orders");
    broker
        .register_stream(
            "t1",
            "ns",
            "scratch",
            StreamMetadata {
                durable: false,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("scratch");
    Arc::new(broker)
}

/// `broker`'s Raft shards, with its readers bound by them as startup does.
pub(crate) fn shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
    let shards = unbound_shards(broker);
    broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
        Arc::new(engine(&shards)),
    )));
    shards
}

/// [`shards`] on a broker nothing tells what its groups committed.
fn unbound_shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
    let shards = held_shards(broker);
    open_root(broker, &shards);
    shards
}

/// A fleet that has finalized nothing: every member fsyncs on commit.
pub(crate) fn no_fleet() -> Arc<felix_common::fleet::FleetGate> {
    Arc::new(felix_common::fleet::FleetGate::new(Vec::<String>::new()))
}

/// [`unbound_shards`] whose data root has not passed the restore gate.
pub(crate) fn held_shards(broker: &Arc<Broker>) -> Arc<RaftShards> {
    let runtime =
        GroupRuntime::start("broker-a", timers(), Arc::new(AgreeingPeers)).expect("runtime");
    RaftShards::new(
        Arc::clone(broker),
        runtime,
        "broker-a".to_string(),
        no_fleet(),
    )
}

/// Pass `broker`'s data root through the restore gate and let `shards` open
/// members on it, as a start does. The control plane's copy is one from
/// before any mark, so a root a test opens a second time passes too.
pub(crate) fn open_root(broker: &Arc<Broker>, shards: &RaftShards) {
    use felix_storage::raft_copy::{ControlPlaneCopy, Evidence, Gate, check, random_copy_id};
    let root = broker
        .durable_storage()
        .expect("storage")
        .root()
        .to_path_buf();
    let (storage_id, _) = felix_storage::storage_identity::load_or_create(&root, || {
        felix_common::random::uuid_v4().into_bytes()
    })
    .expect("storage id");
    let gate = check(
        &root,
        Evidence {
            witness_dir: None,
            control_plane: Some(ControlPlaneCopy {
                storage_id,
                sequence: 0,
            }),
            restored: false,
        },
    )
    .expect("gate");
    let Gate::Passed(passed) = gate else {
        panic!("the root did not pass the gate: {gate:?}");
    };
    shards.set_root(Arc::new(CopyRoot::open(root, passed, random_copy_id)));
}

/// The directory `orders` shard 0 opens on.
fn orders_dir(broker: &Arc<Broker>) -> std::path::PathBuf {
    let storage = broker.durable_storage().expect("storage");
    storage
        .claim_for_raft("t1", "ns", "orders", 0, INCARNATION)
        .expect("directory")
        .0
}

/// Take `orders` and wait until its group of one is led and served.
async fn lead_orders(shards: &Arc<RaftShards>) {
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;
}

/// **No member opens on a data root the restore gate has not passed.** A
/// held root may be a restored one, whose copies would vote a second time
/// in a term they already voted in. The shard answers "not ready", and the
/// pass after the gate passes the root opens the member.
#[tokio::test]
async fn no_member_opens_before_the_restore_gate_passes_the_root() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = held_shards(&broker);
    let orders = key("orders", 0);
    let fed = || assignments([assigned(orders.clone(), INCARNATION)]);

    let mut left = fed();
    shards.take(&mut left).await;
    assert!(left.is_empty(), "a held Raft shard was left to the hybrid");
    assert!(shards.engine_for(&orders).is_none());
    assert!(matches!(
        shards.dispatch(&orders, &|_| None),
        Some(Dispatch::Unavailable(Reason::NotReady))
    ));

    // The feed is woken, so the members do not wait for a placement change.
    let wake = Arc::new(tokio::sync::Notify::new());
    shards.wake_with(Arc::clone(&wake));
    open_root(&broker, &shards);
    tokio::time::timeout(Duration::from_secs(5), wake.notified())
        .await
        .expect("the feed was not woken");
    shards.take(&mut fed()).await;
    assert!(shards.engine_for(&orders).is_some());
    shards.runtime.shutdown().await;
}

/// **A copy the restore check refuses does not open as the member it was.**
/// A data root that got a new storage id (a restore, an unsafe crash, the
/// operator command) may hold older votes and logs than its copies gave
/// out. The directory is moved aside and the shard opens as a member with
/// a new copy id, no vote and no entry, which the old group does not know.
#[tokio::test]
async fn a_copy_the_restore_check_refuses_opens_as_a_new_member() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let (old, dir) = {
        let broker = broker(root.path()).await;
        let shards = shards(&broker);
        lead_orders(&shards).await;
        let old = shards.runtime.status(&orders).expect("status").member;
        assert_ne!(old.copy, 0);
        shards.shutdown().await;
        (old, orders_dir(&broker))
    };

    felix_storage::run_state::copy_trust::rotate_storage_id(
        root.path(),
        felix_common::random::uuid_v4().into_bytes(),
    )
    .expect("rotate");
    let broker = broker(root.path()).await;
    // Held, so the new member forms no group before it is looked at.
    let shards = held_shards(&broker);
    open_root(&broker, &shards);
    let mut alone = assigned(orders.clone(), INCARNATION);
    alone.replicas.push("broker-b".to_string());
    shards.take(&mut assignments([alone])).await;

    let status = shards.runtime.status(&orders).expect("a member");
    assert_eq!(status.member.broker, old.broker);
    assert_ne!(status.member.copy, old.copy);
    assert_eq!((status.term, status.last_index), (0, None));
    let kept = felix_storage::raft_copy::kept_copies(&dir).expect("kept");
    assert_eq!(
        kept.iter().map(|kept| kept.copy).collect::<Vec<_>>(),
        vec![Some(old.copy)],
        "the refused copy's records are kept"
    );
    shards.shutdown().await;
}

/// **A shard directory written before members had copy ids is refused.**
/// It holds a Raft log and no copy file. Opened as a copy it would rotate,
/// on every broker of its group, and the brokers would then form the group
/// again on empty logs. It stays closed and untouched until an operator
/// removes it.
#[tokio::test]
async fn a_directory_from_before_copy_ids_is_not_opened() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let dir = {
        let broker = broker(root.path()).await;
        let shards = shards(&broker);
        lead_orders(&shards).await;
        shards.shutdown().await;
        orders_dir(&broker)
    };
    std::fs::remove_file(dir.join("raft-copy")).expect("no copy file");
    let listing = || {
        let mut names: Vec<_> = std::fs::read_dir(&dir)
            .expect("directory")
            .map(|entry| entry.expect("entry").file_name())
            .collect();
        names.sort();
        names
    };
    let before = listing();

    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let mut left = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut left).await;
    assert!(left.is_empty(), "the refused shard was left to the hybrid");
    assert!(shards.engine_for(&orders).is_none());
    assert_eq!(listing(), before);
    assert_eq!(
        felix_storage::raft_copy::kept_copies(&dir).expect("kept"),
        Vec::new()
    );

    // Removed by the operator, the shard opens as a new member.
    std::fs::remove_dir_all(&dir).expect("remove");
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.engine_for(&orders).is_some());
    shards.shutdown().await;
}

fn timers() -> RuntimeConfig {
    RuntimeConfig {
        election_min: Duration::from_millis(150),
        election_max: Duration::from_millis(300),
        heartbeat: Duration::from_millis(50),
        tick: Duration::from_millis(10),
        // Inside the shortest election timeout, as the runtime requires.
        lease: Duration::from_millis(100),
        ..RuntimeConfig::default()
    }
}

/// The index of the last entry `shard`'s log on `shards` has written.
pub(crate) fn written(shards: &RaftShards, shard: &ShardKey) -> u64 {
    let status = shards.runtime.status(shard).expect("a member");
    status.written.map_or(0, |point| point.index)
}

pub(crate) async fn until(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// **A durable stream shard is taken from the hybrid and opened as a Raft
/// member on its incarnation's directory**, which the storage provider then
/// refuses to open: one shard never has two engines on its log.
#[tokio::test]
async fn a_durable_stream_shard_opens_as_a_raft_member() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut cache = key("orders", 0);
    cache.kind = ShardKind::Cache;
    let mut left = assignments([
        assigned(orders.clone(), INCARNATION),
        assigned(key("scratch", 0), 0),
        assigned(key("unknown", 0), 0),
        assigned(cache.clone(), 0),
    ]);

    shards.take(&mut left).await;

    // The in-memory stream and the cache are the hybrid's. The stream not
    // registered yet is nobody's this pass.
    let mut kept: Vec<_> = left.into_keys().collect();
    kept.sort();
    let mut expected = vec![key("scratch", 0), cache];
    expected.sort();
    assert_eq!(kept, expected);
    assert!(shards.engine_for(&key("unknown", 0)).is_none());
    assert!(shards.engine_for(&key("scratch", 0)).is_none());

    until("the member to lead its group of one", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;
    assert_eq!(shards.leading(&orders), shards.engine.open_term(&orders));
    assert!(shards.engine_for(&orders).is_some());

    let storage = broker.durable_storage().expect("storage");
    assert!(
        storage
            .open_stream_for("t1", "ns", "orders", 0, None, INCARNATION)
            .is_err(),
        "the provider opened a Raft member's directory"
    );
    assert!(
        broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .is_ok(),
        "the stream is not open on the member's log"
    );
    shards.runtime.shutdown().await;
}

/// **Broker-wide retention does not stop a Raft shard from opening.** A Raft
/// log refuses retention bounds, and the member's config is the broker's.
#[tokio::test]
async fn broker_wide_retention_does_not_stop_a_shard_opening() {
    let root = tempfile::tempdir().expect("tempdir");
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        retention_bytes: Some(1 << 30),
        retention_age: Some(Duration::from_secs(3600)),
        ..LogConfig::default()
    };
    let broker = broker_with_config(
        root.path(),
        1,
        INCARNATION,
        ConsistencyLevel::Leader,
        config,
    )
    .await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut left = assignments([assigned(orders.clone(), INCARNATION)]);

    shards.take(&mut left).await;

    until("the member to lead its group of one", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;
    shards.runtime.shutdown().await;
}

/// **A deleted stream's Raft member is stopped and its log closed before
/// the directory is retired.** The provider never opened that log, so its
/// own close misses it, and the member would go on writing deleted files.
#[tokio::test]
async fn a_deleted_stream_closes_its_raft_log_before_the_directory_goes() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let log = shards.log_of(&orders).await.expect("open member");
    assert!(!log.is_closed());

    let removed = broker
        .remove_stream_from_catalog("t1", "ns", "orders", Some(INCARNATION))
        .await
        .expect("delete");

    assert!(removed);
    assert!(log.is_closed(), "the member's log outlived its directory");
    assert!(shards.runtime.status(&orders).is_none());
    assert!(shards.log_of(&orders).await.is_none());
    // The assignment is still in the feed: nothing is opened for a stream
    // that is gone.
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.runtime.status(&orders).is_none());
    shards.runtime.shutdown().await;
}

/// **A member placement drops keeps running.** Its group counts it as a
/// voter until a membership change says otherwise, and placement cannot make
/// one: closing it would take a vote from a group that still needs it.
#[tokio::test]
async fn a_member_placement_drops_keeps_serving() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let log = shards.log_of(&orders).await.expect("open member");
    until("the led term to be served", || {
        shards.leading(&orders).is_some()
    })
    .await;
    let term = shards.leading(&orders);

    // Assigned to another broker only.
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    let mut left = assignments([elsewhere.clone()]);
    shards.take(&mut left).await;

    assert!(
        left.is_empty(),
        "a Raft shard held elsewhere went to the hybrid"
    );
    assert!(!log.is_closed(), "the dropped member's log was closed");
    assert_eq!(shards.leading(&orders), term);
    assert!(matches!(
        route(&shards, &orders),
        Some(Dispatch::Local { .. })
    ));
    let placed = BTreeSet::from(["broker-b"]);
    assert!(
        shards.off_placement(&orders, &placed),
        "a group whose voters placement does not name was not reported"
    );
    shards.runtime.shutdown().await;
}

/// **A member opened by an earlier run is opened again, whatever placement
/// says now.** A dropped voter that restarted would otherwise leave its
/// group one short for good.
#[tokio::test]
async fn a_dropped_member_reopens_after_a_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    {
        let broker = broker(root.path()).await;
        let shards = shards(&broker);
        // Never held here: nothing opens.
        shards.take(&mut assignments([elsewhere.clone()])).await;
        assert!(shards.runtime.status(&orders).is_none());
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
        until("the led term to be served", || {
            shards.leading(&orders).is_some()
        })
        .await;
        EngineShards::shutdown(&*shards).await;
    }
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    shards.take(&mut assignments([elsewhere])).await;
    until("the member to lead again", || {
        shards.leading(&orders).is_some()
    })
    .await;
    shards.runtime.shutdown().await;
}

/// **A failed read of whether a member was opened here, or a stream not yet
/// at the assignment's incarnation, is read again on the next pass.** Taken
/// for "never opened" for good, either would keep a dropped voter closed.
#[tokio::test]
async fn a_dropped_member_reopens_after_a_failed_read() {
    let root = tempfile::tempdir().expect("tempdir");
    let orders = key("orders", 0);
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];
    {
        let broker = broker(root.path()).await;
        let shards = shards(&broker);
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
        until("the led term to be served", || {
            shards.leading(&orders).is_some()
        })
        .await;
        EngineShards::shutdown(&*shards).await;
    }
    let broker = broker(root.path()).await;
    let storage = broker.durable_storage().expect("storage");
    let (dir, _) = storage
        .claim_for_raft("t1", "ns", "orders", 0, INCARNATION)
        .expect("claimed");
    // A directory where the marker file should be fails the read.
    let marker = dir.join("engine");
    let raft = std::fs::read(&marker).expect("marker");
    std::fs::remove_file(&marker).expect("remove the marker");
    std::fs::create_dir(&marker).expect("unreadable marker");
    let shards = shards(&broker);
    shards.take(&mut assignments([elsewhere.clone()])).await;
    assert!(shards.runtime.status(&orders).is_none());
    std::fs::remove_dir(&marker).expect("readable again");
    std::fs::write(&marker, raft).expect("marker");

    storage.set_stream_incarnation("t1", "ns", "orders", Some(INCARNATION - 1));
    shards.take(&mut assignments([elsewhere.clone()])).await;
    assert!(shards.runtime.status(&orders).is_none());
    storage.set_stream_incarnation("t1", "ns", "orders", Some(INCARNATION));

    shards.take(&mut assignments([elsewhere])).await;
    until("the member to lead again", || {
        shards.leading(&orders).is_some()
    })
    .await;
    shards.runtime.shutdown().await;
}

/// **A slow marker read holds up no other shard.** A pass reads markers
/// before it takes the lock that other passes and a deleted stream's close
/// wait on.
#[tokio::test]
async fn a_slow_marker_read_does_not_hold_up_other_shards() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    broker
        .register_stream(
            "t1",
            "ns",
            "invoices",
            StreamMetadata {
                durable: true,
                incarnation: INCARNATION,
                replication_factor: Some(1),
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("invoices");
    let shards = shards(&broker);
    let (orders, invoices) = (key("orders", 0), key("invoices", 0));
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];

    shards.stall_reads(true);
    let slow = tokio::spawn({
        let shards = Arc::clone(&shards);
        async move {
            shards.take(&mut assignments([elsewhere])).await;
        }
    });
    until("the marker read to start", || {
        shards.stall_reads.receiver_count() > 0
    })
    .await;
    tokio::time::timeout(
        Duration::from_secs(5),
        shards.take(&mut assignments([assigned(invoices.clone(), INCARNATION)])),
    )
    .await
    .expect("a pass waited on another shard's marker read");
    assert!(shards.runtime.status(&invoices).is_some());
    tokio::time::timeout(
        Duration::from_secs(5),
        felix_broker::EngineLogs::close_through(&*shards, "t1", "ns", "invoices", INCARNATION),
    )
    .await
    .expect("a close waited on another shard's marker read");
    assert!(shards.runtime.status(&invoices).is_none());
    assert!(!slow.is_finished());

    shards.stall_reads(false);
    slow.await.expect("the slow pass");
    assert!(shards.runtime.status(&orders).is_none());
    shards.runtime.shutdown().await;
}

/// **A pass's own opens keep its marker reads.** Otherwise a restart that
/// opens members placed here reads every other marker under the lock.
#[tokio::test]
async fn a_pass_that_opens_members_reads_no_marker_under_the_lock() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    // Several on each side, so the pass's map order almost always puts an
    // open before a check of a shard placed elsewhere.
    let here: Vec<String> = (0..4).map(|n| format!("here-{n}")).collect();
    let away: Vec<String> = (0..8).map(|n| format!("away-{n}")).collect();
    for stream in here.iter().chain(&away) {
        broker
            .register_stream(
                "t1",
                "ns",
                stream,
                StreamMetadata {
                    durable: true,
                    incarnation: INCARNATION,
                    replication_factor: Some(1),
                    ..StreamMetadata::default()
                },
            )
            .await
            .expect("stream");
    }
    let shards = shards(&broker);
    let placed = here
        .iter()
        .map(|stream| assigned(key(stream, 0), INCARNATION))
        .chain(away.iter().map(|stream| {
            let mut elsewhere = assigned(key(stream, 0), INCARNATION);
            elsewhere.leader = "broker-b".to_string();
            elsewhere.replicas = vec!["broker-b".to_string()];
            elsewhere
        }));

    shards.take(&mut assignments(placed)).await;
    for stream in &here {
        assert!(shards.runtime.status(&key(stream, 0)).is_some());
    }
    for stream in &away {
        assert!(shards.runtime.status(&key(stream, 0)).is_none());
    }
    assert_eq!(shards.reads_under_lock.load(Ordering::Relaxed), 0);
    EngineShards::shutdown(&*shards).await;
}

/// **Only a durable registered stream's marker is read.** No other shard
/// reaches the check, so its read would be made again on every pass.
#[tokio::test]
async fn no_marker_is_read_for_a_stream_that_cannot_be_a_member() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    broker
        .register_stream(
            "t1",
            "ns",
            "memory",
            StreamMetadata {
                durable: false,
                incarnation: INCARNATION,
                ..StreamMetadata::default()
            },
        )
        .await
        .expect("memory");
    let shards = shards(&broker);
    let elsewhere = |stream: &str| {
        let mut assignment = assigned(key(stream, 0), INCARNATION);
        assignment.leader = "broker-b".to_string();
        assignment.replicas = vec!["broker-b".to_string()];
        assignment
    };

    for _ in 0..2 {
        shards
            .take(&mut assignments([
                elsewhere("memory"),
                elsewhere("unknown"),
            ]))
            .await;
    }
    assert_eq!(shards.marker_reads.load(Ordering::Relaxed), 0);
    assert_eq!(shards.reads_under_lock.load(Ordering::Relaxed), 0);
    EngineShards::shutdown(&*shards).await;
}

/// **A marker read as unmarked before a claim is not trusted after it.**
/// Remembered, it would keep the claimed member closed for good once its
/// placement dropped this broker.
#[tokio::test]
async fn a_marker_claimed_during_a_read_is_read_again() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut elsewhere = assigned(orders.clone(), INCARNATION);
    elsewhere.leader = "broker-b".to_string();
    elsewhere.replicas = vec!["broker-b".to_string()];

    shards.stall_reads(true);
    let slow = tokio::spawn({
        let shards = Arc::clone(&shards);
        async move {
            shards.take(&mut assignments([elsewhere])).await;
        }
    });
    // Read as unmarked; held there while another pass claims the directory.
    until("the marker to be read", || {
        shards.stall_reads.receiver_count() > 0
    })
    .await;
    tokio::time::timeout(
        Duration::from_secs(5),
        shards.take(&mut assignments([assigned(orders.clone(), INCARNATION)])),
    )
    .await
    .expect("a pass waited on another pass's marker read");
    felix_broker::EngineLogs::close_through(&*shards, "t1", "ns", "orders", INCARNATION).await;
    broker
        .close_stream_shard("t1", "ns", "orders", 0, None)
        .await
        .expect("close the stream");
    assert!(shards.runtime.status(&orders).is_none());

    shards.stall_reads(false);
    slow.await.expect("the slow pass");
    assert!(
        shards.runtime.status(&orders).is_some(),
        "a stale unmarked read kept the claimed member closed"
    );
    EngineShards::shutdown(&*shards).await;
}

/// **A led term is not served while the stream could not be moved to the
/// committed tail.** Here nothing tells the broker what the group committed,
/// so the move does nothing, and that is not taken for a stream at its tail.
#[tokio::test]
async fn a_term_is_not_served_while_nothing_says_what_committed() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = unbound_shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the member to lead its group of one", || {
        shards.engine.open_term(&orders).is_some()
    })
    .await;
    tokio::time::sleep(LEAD_RETRY * 10).await;
    assert_eq!(shards.leading(&orders), None);
    shards.runtime.shutdown().await;
}

/// How long a group that must not form is watched.
const NOT_FORMED_FOR: Duration = Duration::from_secs(1);

fn formed(member: &Member) -> bool {
    member
        .shards
        .runtime
        .status(&key("orders", 0))
        .is_some_and(|status| status.term > 0 || status.last_index.is_some())
}

/// **A set short of the stream's replica count forms no group, and the
/// full set forms one.** A group of the one broker placed first would
/// acknowledge on its own, and the brokers placed later would form the set
/// again on empty logs and overwrite what it acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_placement_forms_no_group() {
    let net = Arc::new(Loopback::default());
    let first = Member::start(&net, "broker-a").await;
    let orders = key("orders", 0);
    first
        .shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    tokio::time::sleep(NOT_FORMED_FOR).await;
    assert!(!formed(&first), "one broker of three formed a group");
    assert_eq!(first.shards.leading(&orders), None);
    assert_eq!(
        route(&first.shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );

    let mut members = vec![first];
    for name in ["broker-b", "broker-c"] {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    members[0]
        .shards
        .take(&mut assignments([replicated()]))
        .await;
    let leader = served(&members).await;
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"after the set filled").await;
    stop(&members).await;
}

/// **Only the lowest broker of the set forms the group, and only once every
/// other member has answered.** The others never form one, even when every
/// member answers them: two brokers forming at once could each be the lowest
/// of a different set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_forms_only_when_every_member_answers_the_lowest() {
    let net = Arc::new(Loopback::default());
    let orders = key("orders", 0);
    // The lowest broker still sees the short set, so it is open, answers,
    // and forms nothing itself.
    let lowest = Member::start(&net, "broker-a").await;
    lowest
        .shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    let mut members = Vec::new();
    for name in ["broker-b", "broker-c"] {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    tokio::time::sleep(NOT_FORMED_FOR).await;
    assert!(
        !members.iter().any(formed) && !formed(&lowest),
        "a broker other than the lowest formed the group"
    );

    // The lowest broker with one member silent forms nothing either.
    let silent = net.runtimes.lock().remove("broker-c").expect("runtime");
    lowest.shards.take(&mut assignments([replicated()])).await;
    tokio::time::sleep(NOT_FORMED_FOR).await;
    assert!(!formed(&lowest), "formed with a member that never answered");
    assert!(!members.iter().any(formed));

    net.runtimes.lock().insert("broker-c".to_string(), silent);
    lowest.shards.take(&mut assignments([replicated()])).await;
    members.insert(0, lowest);
    served(&members).await;
    stop(&members).await;
}

/// **A broker placed on a shard whose group has formed does not form it
/// again.** It is the lowest broker of its set and its log is empty, but
/// the members it asks hold the group's entries. It serves nothing until a
/// membership change adds it, and the group keeps the members it formed with.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_broker_placed_on_a_formed_group_does_not_form_it_again() {
    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"acknowledged").await;

    // Placement replaces broker-a with a broker that sorts before the rest.
    let mut replaced = replicated();
    replaced.leader = "broker-0".to_string();
    replaced.replicas = vec![
        "broker-0".to_string(),
        "broker-b".to_string(),
        "broker-c".to_string(),
    ];
    let late = Member::start(&net, "broker-0").await;
    late.shards.take(&mut assignments([replaced.clone()])).await;
    for member in &members {
        member
            .shards
            .take(&mut assignments([replaced.clone()]))
            .await;
    }
    tokio::time::sleep(NOT_FORMED_FOR).await;

    assert!(!formed(&late), "a late broker formed the group again");
    assert_eq!(
        route(&late.shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    for member in &members {
        assert!(
            member.shards.runtime.status(&orders).is_some(),
            "{} closed its member",
            member.name
        );
    }
    let leader = served(&members).await;
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"still one group").await;
    members.push(late);
    stop(&members).await;
}

/// **The broker that forms a group hands its first leadership to the
/// broker placement names as leader.** `broker-a` forms every set it is
/// lowest in, so without the hand-over it would lead them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_formed_group_is_led_by_the_placed_leader() {
    let net = Arc::new(Loopback::default());
    let orders = key("orders", 0);
    let mut placed = replicated();
    placed.leader = "broker-b".to_string();
    let mut members = Vec::new();
    // The broker that forms the group last, so every member it asks is up.
    for name in BROKERS.iter().rev() {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([placed.clone()])).await;
        members.push(member);
    }
    members.reverse();
    until("placement's leader serves its term", || {
        members[1].shards.leading(&orders).is_some()
    })
    .await;
    let term = members[1].shards.leading(&orders).expect("term");
    assert_eq!(members[0].shards.leading(&orders), None);
    members[1].publish(term, b"on the placed leader").await;
    stop(&members).await;
}

/// **A shard whose every copy rotated is not formed again.** Each copy
/// comes back with a new copy id, no vote and an empty log, as after
/// `rotate-storage-id` on every broker or a fleet-wide restore. Formed from
/// those, the group would start at index 0 and give out offsets it already
/// acknowledged. The kept copies say a group was there, so the shard waits
/// for a re-form.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_shard_whose_every_copy_rotated_is_not_formed_again() {
    not_formed_after_rotating(&[]).await;
}

/// **Nor is it formed by a broker whose disk was replaced.** The lowest
/// broker has no kept copy to go by, so the others refuse its question.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotated_copy_does_not_answer_that_it_is_blank() {
    not_formed_after_rotating(&["broker-a"]).await;
}

/// **Nor by the one rotated copy among replaced disks.** The others have
/// nothing to go by, so the lowest broker's kept copy is the only evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rotated_copy_forms_no_group_among_replaced_disks() {
    not_formed_after_rotating(&["broker-b", "broker-c"]).await;
}

/// Acknowledge a record on a trio, rotate every copy except on `wiped`,
/// which get an empty data root, and check no group forms.
async fn not_formed_after_rotating(wiped: &[&str]) {
    let orders = key("orders", 0);
    let (net, members, leader) = trio().await;
    let term = members[leader].shards.leading(&orders).expect("term");
    let acknowledged = members[leader].publish(term, b"acknowledged").await;
    stop(&members).await;
    drop(net);

    let net = Arc::new(Loopback::default());
    let mut rotated = Vec::new();
    // The lowest broker last, so every member it asks is up.
    for member in members.into_iter().rev() {
        let Member { name, mut root, .. } = member;
        if wiped.contains(&name) {
            root = tempfile::tempdir().expect("tempdir");
        } else {
            felix_storage::run_state::copy_trust::rotate_storage_id(
                root.path(),
                felix_common::random::uuid_v4().into_bytes(),
            )
            .expect("rotate");
        }
        let member = Member::start_on(
            &net,
            name,
            root,
            INCARNATION,
            ConsistencyLevel::Quorum,
            FsyncMode::None,
            false,
        )
        .await;
        member.shards.take(&mut assignments([replicated()])).await;
        rotated.push(member);
    }
    rotated.reverse();
    tokio::time::sleep(NOT_FORMED_FOR).await;
    rotated[0]
        .shards
        .take(&mut assignments([replicated()]))
        .await;
    tokio::time::sleep(NOT_FORMED_FOR).await;

    if rotated.iter().any(formed) {
        let leader = served(&rotated).await;
        let term = rotated[leader].shards.leading(&orders).expect("term");
        let offset = rotated[leader].publish(term, b"after the rotation").await;
        panic!(
            "formed again on rotated copies: offset {offset}, after {acknowledged} was acknowledged"
        );
    }
    assert_eq!(
        route(&rotated[0].shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    stop(&rotated).await;
}

/// **A member of an earlier incarnation does not answer for a later one.**
/// The stream was recreated with a full set, and one broker's feed still
/// has the old incarnation, so its member there is blank. Taken as an
/// answer, the new group would append into the old incarnation's directory
/// on that broker and count it, and the broker drops that directory when
/// its feed catches up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_of_an_earlier_incarnation_does_not_answer_for_a_later_one() {
    let net = Arc::new(Loopback::default());
    let mut members = Vec::new();
    for name in ["broker-b", "broker-c"] {
        let member = Member::start(&net, name).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    let recreated = INCARNATION + 1;
    let lowest = Member::start_at(&net, "broker-a", recreated).await;
    let mut assignment = replicated();
    assignment.incarnation = recreated;
    lowest.shards.take(&mut assignments([assignment])).await;
    tokio::time::sleep(NOT_FORMED_FOR).await;

    assert!(
        !formed(&lowest),
        "formed on blank answers from an earlier incarnation"
    );
    assert!(!members.iter().any(formed));
    members.push(lowest);
    stop(&members).await;
}

pub(crate) fn engine(shards: &Arc<RaftShards>) -> crate::shards::routing::StreamEngine {
    let engine = crate::shards::routing::StreamEngine::default();
    engine.set_raft(Arc::clone(shards));
    engine
}

/// Write `payloads` to `shard`'s group as records `position..` of
/// `producer_id`'s batch `sequence`, `len` records long, with no check
/// against the log: what a leader that stops partway leaves. Returns the
/// first record's offset.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_batch(
    shards: &RaftShards,
    shard: &ShardKey,
    producer_id: u64,
    sequence: u64,
    position: u32,
    len: u32,
    payloads: &[&'static [u8]],
) -> u64 {
    let records = payloads
        .iter()
        .zip(position..)
        .map(|(payload, at)| {
            let mark = (producer_id, sequence, at, len);
            (bytes::Bytes::from_static(payload), Some(mark))
        })
        .collect();
    let written = shards.runtime.write_unchecked(shard, records).await;
    written.expect("written")[0]
}

/// Leave `producer_id`'s batch `sequence` cut and then forgotten: its
/// first record, then the first records of one more cut batch than a log
/// keeps, each another producer's, so the oldest cut is kept by name only.
pub(crate) async fn forget_cut(
    shards: &RaftShards,
    shard: &ShardKey,
    producer_id: u64,
    sequence: u64,
) {
    write_batch(shards, shard, producer_id, sequence, 0, 2, &[b"cut"]).await;
    // The log keeps 4096 cut batches, and the last of these is still open.
    let others = (1..=4097u64)
        .map(|other| write_batch(shards, shard, producer_id + other, 0, 0, 2, &[b"other"]));
    futures::future::join_all(others).await;
}

/// Routes that ask `shards` first and know every broker's address, with a
/// broker lease that was never granted: a Raft shard must not need it.
pub(crate) fn ingress(shards: &Arc<RaftShards>) -> Arc<crate::shards::routing::IngressRouter> {
    routes(shards).1
}

/// [`ingress`], with the router under it.
pub(crate) fn routes(
    shards: &Arc<RaftShards>,
) -> (
    Arc<felix_router::ShardRouter>,
    Arc<crate::shards::routing::IngressRouter>,
) {
    use felix_router::{NodeRef, RegionRouter, RoutingTable, ShardRouter};
    let router = Arc::new(ShardRouter::new(
        shards.local.clone(),
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = BROKERS
        .iter()
        .map(|name| {
            let node = NodeRef {
                node_id: name.to_string(),
                advertise_addr: addr_of(name).expect("addr"),
                region: "us-west-2".to_string(),
                live: true,
            };
            (name.to_string(), node)
        })
        .collect();
    router.publish(RoutingTable::default(), &nodes);
    let ingress = crate::shards::routing::IngressRouter::new(Arc::clone(&router), Arc::default());
    ingress
        .fence()
        .bind_lease(Arc::new(crate::cluster::lease::LeaseState::new(
            Duration::from_secs(10),
        )));
    ingress.set_engine_shards(Arc::clone(shards) as Arc<dyn EngineShards>);
    (router, Arc::new(ingress))
}

fn route(shards: &RaftShards, key: &ShardKey) -> Option<Dispatch> {
    EngineShards::dispatch(shards, key, &|_| None)
}

fn target(
    handle: felix_broker::StreamHandle,
    key: &ShardKey,
    term: u64,
) -> crate::serving::direct_publish::LocalTarget {
    crate::serving::direct_publish::LocalTarget {
        handle,
        shard: Some(key.clone()),
        generation: term,
        fenced: None,
    }
}

/// **A led term takes no write and is not routed here until the stream is
/// at the committed tail.** A batch the last term committed and this one
/// delivered after its own would reach a resuming reader twice.
#[tokio::test]
async fn a_led_term_is_served_only_after_the_stream_is_at_the_committed_tail() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let engine = engine(&shards);
    let orders = key("orders", 0);
    shards.stall_leading(true);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    until("the member leads", || {
        shards
            .runtime
            .status(&orders)
            .is_some_and(|status| status.role == Role::Leader)
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let term = shards.runtime.status(&orders).expect("status").term;
    assert_eq!(shards.leading(&orders), None);
    assert_eq!(
        route(&shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let refused = engine
        .publish_on_raft(
            &broker,
            &handle,
            &orders,
            term,
            vec![bytes::Bytes::from_static(b"early")],
            Default::default(),
            Duration::from_secs(5),
        )
        .expect("a Raft shard");
    assert!(matches!(
        refused,
        Err(crate::shards::routing::RaftRefusal::NotLeading { .. })
    ));

    shards.stall_leading(false);
    until("the term is served", || {
        shards.leading(&orders) == Some(term)
    })
    .await;
    assert_eq!(
        route(&shards, &orders),
        Some(Dispatch::Local { generation: term })
    );
    EngineShards::shutdown(&*shards).await;
}

/// **A publish to a Raft shard is proposed to its group and answered at the
/// commit, and readers are bound by the commit index.** It never enters the
/// hybrid's fence or claims on the hybrid's log, which refuses the stream.
#[tokio::test]
async fn a_publish_on_a_raft_shard_commits_through_its_group() {
    use crate::serving::direct_publish::{DirectPublishError, publish_direct};
    use felix_replication::engine::Engine;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let engine = engine(&shards);
    let orders = key("orders", 0);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let handle = broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let payloads = [
        bytes::Bytes::from_static(b"one"),
        bytes::Bytes::from_static(b"two"),
    ];
    let timeout = Duration::from_secs(10);

    let first = publish_direct(
        &broker,
        None,
        &engine,
        target(handle.clone(), &orders, term),
        &payloads,
        None,
        timeout,
    )
    .await
    .expect("published")
    .expect("an offset");
    // `orders` is a `Leader` stream here, answered inside the lease before
    // the commit shows.
    until("the batch commits", || {
        engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(first + 2)
    })
    .await;
    assert_eq!(engine.read_bound(None, &orders).ok(), Some(Some(first + 2)));

    // Routed at a term this broker does not serve: nothing is proposed.
    let stale = publish_direct(
        &broker,
        None,
        &engine,
        target(handle, &orders, term + 1),
        &payloads,
        None,
        timeout,
    )
    .await;
    assert!(matches!(stale, Err(DirectPublishError::NotWritten(_))));
    assert_eq!(
        engine.committed_bound(&orders),
        felix_broker::ReadBound::Committed(first + 2)
    );
    EngineShards::shutdown(&*shards).await;
}

/// **Stopping the broker stops every member and closes its log**, so the
/// storage flush that follows is not racing a group still writing.
#[tokio::test]
async fn shutdown_stops_the_members_and_closes_their_logs() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    assert!(shards.log_of(&orders).await.is_some());

    assert!(EngineShards::shutdown(&*shards).await, "every log closed");

    assert!(shards.log_of(&orders).await.is_none());
    assert_eq!(shards.runtime.status(&orders), None);
    assert_eq!(shards.leading(&orders), None);
    assert_eq!(route(&shards, &orders), None);
}

/// **A member log whose close fails makes the stop unclean**: its last sync
/// may not have run, so the broker must not record a clean stop.
#[tokio::test]
async fn shutdown_reports_a_log_that_failed_to_close() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    let mut all = assignments([assigned(orders.clone(), INCARNATION)]);
    shards.take(&mut all).await;
    shards
        .log_of(&orders)
        .await
        .expect("open")
        .fail_next_flush();

    assert!(!EngineShards::shutdown(&*shards).await);
}

/// **Stopping the broker writes the last batch of log marks**, after it
/// closed every member, so a planned stop leaves each batched log mark at
/// the flushed log rather than up to a batch interval behind it.
#[tokio::test]
async fn shutdown_writes_the_last_batched_log_marks() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let config = RuntimeConfig {
        log_mark: felix_replication::raft::LogMarkMode::Batched(Duration::from_secs(3600)),
        ..timers()
    };
    let runtime =
        GroupRuntime::start("broker-a", config, Arc::new(AgreeingPeers)).expect("runtime");
    let shards = RaftShards::new(
        Arc::clone(&broker),
        runtime,
        "broker-a".to_string(),
        no_fleet(),
    );
    broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
        Arc::new(engine(&shards)),
    )));
    open_root(&broker, &shards);
    lead_orders(&shards).await;
    let orders = key("orders", 0);
    for sequence in 0..3 {
        write_batch(&shards, &orders, 1, sequence, 0, 1, &[b"one"]).await;
    }
    let last = written(&shards, &orders);
    let marks = shards.root.get().expect("root").mark_log().clone();
    let name = felix_replication::raft::mark_name(&orders);
    let mark = || {
        marks
            .marks(&name)
            .and_then(|marks| marks.log)
            .map(|at| at.index)
    };
    assert!(mark() < Some(last), "the batch ran before the stop");

    EngineShards::shutdown(&*shards).await;

    assert_eq!(mark(), Some(last));
}

/// **A member's log directory is locked while the member is open**, so a
/// second broker started on the same data directory cannot recover and
/// write a log this one is writing. Closing the member lets it go.
#[tokio::test]
async fn an_open_member_holds_its_log_directory() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    assert!(shards.log_of(&orders).await.is_some());
    let storage = broker.durable_storage().expect("storage");
    let (dir, label) = storage
        .claim_for_raft("t1", "ns", "orders", 0, INCARNATION)
        .expect("claimed");
    let config = LogConfig {
        raft_log: true,
        fsync_mode: FsyncMode::OnCommit,
        ..storage.config().clone()
    };

    let second = DiskLog::open(&dir, label.clone(), config.clone());
    assert!(
        matches!(second, Err(felix_storage::StorageError::Io(ref err))
            if err.kind() == std::io::ErrorKind::ResourceBusy),
        "a second open of an open member's directory was let in"
    );

    EngineShards::shutdown(&*shards).await;
    let reopened = DiskLog::open(&dir, label, config).expect("open after the member closed");
    reopened.close().await.expect("close");
}

/// Carries Raft messages between the runtimes of one process. A broker
/// taken out of it, or in `cut`, is unreachable.
#[derive(Default)]
pub(crate) struct Loopback {
    runtimes: parking_lot::Mutex<HashMap<String, GroupRuntime>>,
    cut: parking_lot::Mutex<std::collections::HashSet<String>>,
    /// Brokers cut off right after the next append with entries reaches
    /// them, so they hold the entries but never hear them committed.
    cut_after_entries: parking_lot::Mutex<std::collections::HashSet<String>>,
}

impl Loopback {
    /// Take `broker` out: nothing reaches it from now on.
    pub(crate) fn forget(&self, broker: &str) {
        self.runtimes.lock().remove(broker);
    }
}

#[async_trait::async_trait]
impl RaftTransport for Loopback {
    async fn send(&self, request: RaftRequest) -> Result<RaftReply, Refused> {
        let to = request.to().broker.to_string();
        if self.cut.lock().contains(&to) {
            return Err(Refused::Unreachable(format!("broker {to} is cut off")));
        }
        let cut_after = request.carries_entries() && self.cut_after_entries.lock().remove(&to);
        if cut_after {
            self.cut.lock().insert(to.clone());
        }
        let runtime = self.runtimes.lock().get(&to).cloned();
        match runtime {
            Some(runtime) => runtime.handle(request).await,
            None => Err(Refused::Unreachable(format!("broker {to} is down"))),
        }
    }
}

pub(crate) const BROKERS: [&str; 3] = ["broker-a", "broker-b", "broker-c"];

pub(crate) struct Member {
    pub(crate) name: &'static str,
    pub(crate) broker: Arc<Broker>,
    pub(crate) shards: Arc<RaftShards>,
    pub(crate) engine: Arc<crate::shards::routing::StreamEngine>,
    pub(crate) root: tempfile::TempDir,
}

impl Member {
    /// A broker on `net` whose readers are bound the way startup binds them.
    /// `orders` is a `Quorum` stream.
    async fn start(net: &Arc<Loopback>, name: &'static str) -> Self {
        Self::start_at(net, name, INCARNATION).await
    }

    /// [`Self::start`], with `orders` at `incarnation`.
    async fn start_at(net: &Arc<Loopback>, name: &'static str, incarnation: u64) -> Self {
        Self::start_with(
            net,
            name,
            incarnation,
            ConsistencyLevel::Quorum,
            FsyncMode::None,
            false,
        )
        .await
    }

    /// With `written_commit`, the fleet has finalized `raft_written_commit`
    /// and the root's run began relaxed, as a relaxed broker's start does.
    async fn start_with(
        net: &Arc<Loopback>,
        name: &'static str,
        incarnation: u64,
        consistency: ConsistencyLevel,
        fsync: FsyncMode,
        written_commit: bool,
    ) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        Self::start_on(
            net,
            name,
            root,
            incarnation,
            consistency,
            fsync,
            written_commit,
        )
        .await
    }

    /// [`Self::start_with`] on a data root an earlier member left.
    async fn start_on(
        net: &Arc<Loopback>,
        name: &'static str,
        root: tempfile::TempDir,
        incarnation: u64,
        consistency: ConsistencyLevel,
        fsync: FsyncMode,
        written_commit: bool,
    ) -> Self {
        let broker = broker_at(root.path(), 3, incarnation, consistency, fsync).await;
        let fleet = if written_commit {
            let relaxed = felix_storage::run_state::relaxed(&fsync);
            felix_storage::run_state::begin(
                root.path(),
                relaxed,
                None,
                None,
                || false,
                || felix_common::random::uuid_v4().into_bytes(),
            )
            .expect("begin the run");
            let feature = [felix_common::fleet::RAFT_WRITTEN_COMMIT.name()];
            Arc::new(felix_common::fleet::FleetGate::seeded(feature, feature))
        } else {
            no_fleet()
        };
        let runtime =
            GroupRuntime::start(name, timers(), Arc::clone(net) as Arc<dyn RaftTransport>)
                .expect("runtime");
        net.runtimes
            .lock()
            .insert(name.to_string(), runtime.clone());
        let shards = RaftShards::new(Arc::clone(&broker), runtime, name.to_string(), fleet);
        open_root(&broker, &shards);
        let engine = Arc::new(engine(&shards));
        broker.set_read_bounds(Arc::new(felix_replication::engine::CommittedReads::new(
            Arc::clone(&engine),
        )));
        Self {
            name,
            broker,
            shards,
            engine,
            root,
        }
    }

    pub(crate) async fn publish(&self, term: u64, payload: &'static [u8]) -> u64 {
        let published = self.publish_within(term, payload, Duration::from_secs(10));
        published.await.expect("published")
    }

    /// The offset, or `None` when no answer came within `timeout`.
    async fn publish_within(
        &self,
        term: u64,
        payload: &'static [u8],
        timeout: Duration,
    ) -> Option<u64> {
        use crate::serving::direct_publish::publish_direct;
        let orders = key("orders", 0);
        let handle = self
            .broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .expect("handle");
        publish_direct(
            &self.broker,
            None,
            &self.engine,
            target(handle, &orders, term),
            &[bytes::Bytes::from_static(payload)],
            None,
            timeout,
        )
        .await
        .ok()?
    }

    /// One past the last offset this member knows committed.
    fn bound(&self) -> u64 {
        use felix_replication::engine::Engine;
        match self.engine.committed_bound(&key("orders", 0)) {
            felix_broker::ReadBound::Committed(bound) => bound,
            other => panic!("no bound: {other:?}"),
        }
    }

    /// Publish `payload` as batch `sequence` of producer 5 and return its
    /// offset: where it was written, or where the batch it repeats was.
    async fn publish_idempotent(&self, term: u64, sequence: u64, payload: &'static [u8]) -> u64 {
        let orders = key("orders", 0);
        let handle = self
            .broker
            .resolve_stream_handle("t1", "ns", "orders", 0)
            .await
            .expect("handle");
        let turn = self.broker.producer_turn(&handle, 5).await;
        let proposed = self.engine.publish_idempotent_on_raft(
            &self.broker,
            &handle,
            &orders,
            term,
            turn,
            sequence,
            felix_broker::SequenceReuse::Refuse,
            vec![bytes::Bytes::from_static(payload)],
            Default::default(),
            Duration::from_secs(10),
        );
        let proposed = proposed.expect("a Raft shard").expect("proposed");
        let offsets = proposed.complete().await.expect("committed");
        offsets.expect("an offset").0
    }
}

/// `orders` on three brokers, placed with `broker-a` as leader.
pub(crate) fn replicated() -> ShardAssignment {
    let mut assignment = assigned(key("orders", 0), INCARNATION);
    assignment.replicas = BROKERS.iter().map(|name| name.to_string()).collect();
    assignment
}

/// Three brokers holding a member each, and the one whose term is served.
/// `orders` is a `Quorum` stream on `FsyncMode::None`: a publish is
/// answered on a written majority.
pub(crate) async fn trio() -> (Arc<Loopback>, Vec<Member>, usize) {
    trio_at(ConsistencyLevel::Quorum).await
}

async fn trio_at(consistency: ConsistencyLevel) -> (Arc<Loopback>, Vec<Member>, usize) {
    trio_fsync(consistency, FsyncMode::None).await
}

async fn trio_fsync(
    consistency: ConsistencyLevel,
    fsync: FsyncMode,
) -> (Arc<Loopback>, Vec<Member>, usize) {
    trio_with(consistency, fsync, false).await
}

/// [`trio_fsync`], with `written_commit` as [`Member::start_with`] takes it.
async fn trio_with(
    consistency: ConsistencyLevel,
    fsync: FsyncMode,
    written_commit: bool,
) -> (Arc<Loopback>, Vec<Member>, usize) {
    let net = Arc::new(Loopback::default());
    let mut members = Vec::new();
    // The broker that forms the group last, so every member it asks is up.
    for name in BROKERS.iter().rev() {
        let member =
            Member::start_with(&net, name, INCARNATION, consistency, fsync, written_commit).await;
        member.shards.take(&mut assignments([replicated()])).await;
        members.push(member);
    }
    members.reverse();
    let leader = served(&members).await;
    (net, members, leader)
}

async fn served(members: &[Member]) -> usize {
    let orders = key("orders", 0);
    let mut leader = None;
    until("a member serves its term", || {
        leader = members
            .iter()
            .position(|member| member.shards.leading(&orders).is_some());
        leader.is_some()
    })
    .await;
    leader.expect("a leader")
}

pub(crate) async fn stop(members: &[Member]) {
    for member in members {
        EngineShards::shutdown(&*member.shards).await;
    }
}

pub(crate) fn addr_of(name: &str) -> Option<SocketAddr> {
    let at = BROKERS.iter().position(|broker| *broker == name)?;
    Some(SocketAddr::from(([10, 0, 0, at as u8 + 1], 7001)))
}

/// **A follower sends a request to the leader its group elected, and a
/// broker with no member sends it to the placement's leader.** Neither
/// reads the hybrid's routes, which never saw the shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_request_off_the_leader_is_routed_to_it() {
    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let follower = &members[(leader + 1) % 3];
    let to_leader = Dispatch::Forward {
        node_id: members[leader].name.to_string(),
        advertise_addr: addr_of(members[leader].name).expect("addr"),
        generation: term,
        incarnation: 0,
    };
    until("the follower knows its leader", || {
        EngineShards::dispatch(&*follower.shards, &orders, &addr_of) == Some(to_leader.clone())
    })
    .await;
    assert_eq!(
        route(&follower.shards, &orders),
        Some(Dispatch::Unavailable(Reason::OwnerUnavailable(
            members[leader].name.to_string()
        )))
    );

    let outside = Member::start(&net, "broker-d").await;
    assert_eq!(route(&outside.shards, &orders), None);
    let mut all = assignments([replicated()]);
    outside.shards.take(&mut all).await;
    assert!(
        all.is_empty(),
        "a Raft shard held elsewhere went to the hybrid"
    );
    assert!(outside.shards.engine_for(&orders).is_none());
    assert_eq!(
        EngineShards::dispatch(&*outside.shards, &orders, &addr_of),
        Some(Dispatch::Forward {
            node_id: "broker-a".to_string(),
            advertise_addr: addr_of("broker-a").expect("addr"),
            generation: 0,
            incarnation: 0,
        })
    );
    assert_eq!(
        route(&outside.shards, &orders),
        Some(Dispatch::Unavailable(Reason::OwnerUnavailable(
            "broker-a".to_string()
        )))
    );
    stop(&members).await;
    EngineShards::shutdown(&*outside.shards).await;
}

/// **A restarted follower forwards to the leader its stored vote names,
/// before it hears from it.** It serves no reader until it does: the hint
/// is enough to forward to, not to read from.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_follower_forwards_before_it_hears_its_leader() {
    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let Member {
        name, root, shards, ..
    } = members.remove((leader + 1) % 3);
    let leader = members
        .iter()
        .find(|member| member.shards.leading(&orders).is_some())
        .expect("the leader");
    EngineShards::shutdown(&*shards).await;
    net.forget(name);
    // Back up, but nothing reaches it, so it takes no append.
    net.cut.lock().insert(name.to_string());
    let restarted = Member::start_on(
        &net,
        name,
        root,
        INCARNATION,
        ConsistencyLevel::Quorum,
        FsyncMode::None,
        false,
    )
    .await;
    restarted
        .shards
        .take(&mut assignments([replicated()]))
        .await;
    let to_leader = Dispatch::Forward {
        node_id: leader.name.to_string(),
        advertise_addr: addr_of(leader.name).expect("addr"),
        generation: term,
        incarnation: 0,
    };
    until("the restarted follower forwards to its leader", || {
        EngineShards::dispatch(&*restarted.shards, &orders, &addr_of) == Some(to_leader.clone())
    })
    .await;
    assert!(!EngineShards::serves_reads(&*restarted.shards, &orders));
    stop(&members).await;
    EngineShards::shutdown(&*restarted.shards).await;
}

/// **A placed member that is not open yet answers not ready**, and never
/// falls through to the hybrid's routes.
#[tokio::test]
async fn a_shard_placed_here_with_no_member_open_is_not_ready() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .placed
        .lock()
        .insert(orders.clone(), "broker-a".to_string());

    assert_eq!(
        route(&shards, &orders),
        Some(Dispatch::Unavailable(Reason::NotReady))
    );
    EngineShards::shutdown(&*shards).await;
}

pub(crate) async fn delivered(
    receiver: &mut felix_broker::SubscriptionReceiver,
) -> (u64, bytes::Bytes) {
    let got = tokio::time::timeout(Duration::from_secs(20), receiver.recv()).await;
    let envelope = got.expect("delivered in time").expect("subscription open");
    (
        envelope.base_offset().expect("an offset"),
        envelope.payloads()[0].clone(),
    )
}

/// **A Raft shard's subscribers on its leader get each record once it is
/// committed, at the offset the group gave it, and a follower's readers
/// are bound by its own member's commit index.**
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readers_of_a_raft_shard_get_what_committed() {
    use felix_replication::engine::Engine;

    let (_net, members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let follower = &members[(leader + 1) % 3];
    let (mut receiver, _guard) = {
        let subscription = members[leader].broker.subscribe("t1", "ns", "orders", 0);
        subscription.await.expect("subscribe").into_parts()
    };

    let first = members[leader].publish(term, b"one").await;
    let second = members[leader].publish(term, b"two").await;

    assert_eq!(second, first + 1);
    let one = (first, bytes::Bytes::from_static(b"one"));
    assert_eq!(delivered(&mut receiver).await, one);
    let two = (second, bytes::Bytes::from_static(b"two"));
    assert_eq!(delivered(&mut receiver).await, two);
    // What the follower's broker hands its readers, as startup wires it.
    let handle = follower
        .broker
        .resolve_stream_handle("t1", "ns", "orders", 0)
        .await
        .expect("handle");
    let committed = felix_broker::ReadBound::Committed(second + 1);
    until("the follower applies both", || {
        handle.read_bound() == committed
    })
    .await;
    assert_eq!(follower.engine.committed_bound(&orders), committed);
    assert_eq!(
        follower.engine.read_bound(None, &orders).ok(),
        Some(Some(second + 1))
    );
    stop(&members).await;
}

/// **When the leader stops, another member leads and takes publishes, and
/// nothing acknowledged before is lost**: the new leader's log holds every
/// acknowledged offset, so its first record lands above them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_leader_is_replaced_and_no_acked_record_is_lost() {
    use felix_replication::engine::Engine;

    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let mut acked = Vec::new();
    for payload in [b"a1", b"a2", b"a3"] {
        acked.push(members[leader].publish(term, payload).await);
    }
    let last = *acked.last().expect("acked");
    assert_eq!(acked, [last - 2, last - 1, last]);

    let old = members.remove(leader);
    net.runtimes.lock().remove(old.name);
    EngineShards::shutdown(&*old.shards).await;

    let next = served(&members).await;
    let next_term = members[next].shards.leading(&orders).expect("term");
    assert!(next_term > term);
    assert_eq!(
        route(&members[next].shards, &orders),
        Some(Dispatch::Local {
            generation: next_term
        })
    );
    let (mut receiver, _guard) = {
        let subscription = members[next]
            .broker
            .subscribe("t1", "ns", "orders", 0)
            .await;
        subscription.expect("subscribe").into_parts()
    };
    let after = members[next].publish(next_term, b"b1").await;
    assert!(after > last, "offset {after} reused an acknowledged one");
    assert_eq!(
        delivered(&mut receiver).await,
        (after, bytes::Bytes::from_static(b"b1"))
    );
    // Every acknowledged payload reads back from the new leader's log.
    let read = members[next]
        .broker
        .read_durable("t1", "ns", "orders", 0, acked[0], 1 << 20)
        .await
        .expect("read");
    let payloads: Vec<&[u8]> = read.iter().map(|record| &record.payload[..]).collect();
    assert_eq!(payloads, [&b"a1"[..], b"a2", b"a3", b"b1"]);
    // The other survivor holds the acknowledged records and the new one.
    let other = &members[1 - next];
    until("the follower applies the new record", || {
        other.engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(after + 1)
    })
    .await;
    stop(&members).await;
}

/// **An idempotent batch sent again after its leader stopped is answered by
/// the next leader with the offset it first got, and the producer carries
/// on in sequence.** The next leader knows the producer from the entries in
/// its own log. Known only to the leader that took it, the batch would be
/// written twice, or the producer refused as unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idempotent_batch_sent_again_after_its_leader_stopped_is_written_once() {
    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let zero = members[leader].publish_idempotent(term, 0, b"zero").await;
    let one = members[leader].publish_idempotent(term, 1, b"one").await;
    assert_eq!(one, zero + 1);

    let old = members.remove(leader);
    net.runtimes.lock().remove(old.name);
    EngineShards::shutdown(&*old.shards).await;
    let next = served(&members).await;
    let next_term = members[next].shards.leading(&orders).expect("term");
    assert!(next_term > term);

    let again = members[next].publish_idempotent(next_term, 1, b"one").await;
    assert_eq!(again, one, "the re-send was written again");
    let two = members[next].publish_idempotent(next_term, 2, b"two").await;
    assert!(two > one);
    let read = members[next]
        .broker
        .read_durable("t1", "ns", "orders", 0, zero, 1 << 20)
        .await
        .expect("read");
    let payloads: Vec<&[u8]> = read.iter().map(|record| &record.payload[..]).collect();
    assert_eq!(payloads, [&b"zero"[..], b"one", b"two"]);
    stop(&members).await;
}

/// **A leader that is deposed lets its term's subscribers go.** A batch
/// the term was handed may settle as a gap, which they would read as a
/// drop. One registering after is served from the log, as a follower's.
#[tokio::test]
async fn a_deposed_leader_lets_its_subscribers_go() {
    use crate::serving::quic::handlers::redirect::stopped_serving;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let ingress = ingress(&shards);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let (mut receiver, _guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };
    // Served on the term alone: the broker's lease was never granted.
    assert!(stopped_serving(Some(&ingress), &orders).is_none());

    shards.leading.deposed(&orders, term);

    let ended = tokio::time::timeout(Duration::from_secs(20), receiver.recv()).await;
    assert!(
        ended.expect("ended in time").is_none(),
        "a record reached a deposed leader's subscriber"
    );
    // The member still follows a leader, so a reader registering now is
    // served from the log.
    assert!(
        stopped_serving(Some(&ingress), &orders).is_none(),
        "a subscriber registered after the deposal was refused"
    );
    let (mut later, _later_guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };
    let kept = tokio::time::timeout(Duration::from_millis(300), later.recv()).await;
    assert!(
        kept.is_err(),
        "a subscriber registered after the deposal was ended"
    );
    EngineShards::shutdown(&*shards).await;
}

/// **A term led again straight after a deposal keeps its own subscribers.**
/// The deposed term's subscribers are ended by a task; the next term is not
/// served, so takes no subscriber, until that task is done.
#[tokio::test]
async fn a_term_led_again_keeps_its_subscribers() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let orders = key("orders", 0);
    shards
        .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
        .await;
    until("the term is served", || shards.leading(&orders).is_some()).await;
    let term = shards.leading(&orders).expect("term");
    let (mut old, _old_guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };

    shards.stall_ends(true);
    shards.leading.deposed(&orders, term);
    shards.leading.opened(&orders, term + 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        shards.leading(&orders),
        None,
        "a term was served before the deposed term's subscribers were ended"
    );

    shards.stall_ends(false);
    until("the next term is served", || {
        shards.leading(&orders) == Some(term + 1)
    })
    .await;
    let ended = tokio::time::timeout(Duration::from_secs(20), old.recv()).await;
    assert!(ended.expect("ended in time").is_none());
    let (mut new, _new_guard) = {
        let subscription = broker.subscribe("t1", "ns", "orders", 0).await;
        subscription.expect("subscribe").into_parts()
    };
    let kept = tokio::time::timeout(Duration::from_millis(300), new.recv()).await;
    assert!(kept.is_err(), "the next term's subscriber was ended");
    EngineShards::shutdown(&*shards).await;
}

/// How long a publish that must wait for a commit nobody can give is
/// watched before it counts as not answered.
const UNANSWERED: Duration = Duration::from_millis(400);

/// No member reaches another. A leader stays one, since it hears of no
/// election, and the others can elect nobody.
fn cut_all(net: &Loopback) {
    net.cut.lock().extend(BROKERS.map(String::from));
}

/// Publish on the leader while no member reaches another, until one is
/// answered. Returns the leader, its term, the offset and the payload
/// answered, with everyone still cut off. An attempt that falls outside the
/// lease is not answered, and may cost the leader its term once the others
/// are back. It can still commit then, so each attempt has its own payload.
async fn answered_alone(
    net: &Loopback,
    members: &[Member],
    name: &str,
) -> (usize, u64, u64, &'static [u8]) {
    let orders = key("orders", 0);
    for attempt in 0..40 {
        let payload: &'static [u8] = format!("{name}-{attempt}").into_bytes().leak();
        net.cut.lock().clear();
        // A heartbeat or two, so a majority has answered inside the lease.
        tokio::time::sleep(timers().heartbeat * 2).await;
        let leader = served(members).await;
        let Some(term) = members[leader].shards.leading(&orders) else {
            continue;
        };
        cut_all(net);
        let answered = members[leader].publish_within(term, payload, UNANSWERED);
        if let Some(offset) = answered.await {
            return (leader, term, offset, payload);
        }
    }
    panic!("no publish was answered while the leader reached nobody");
}

/// The shard takes a publish again on whichever member leads now. Until
/// the batches held through the cut settle, or a new leader is elected, a
/// publish is refused at once, so attempts are spread over a deadline
/// rather than counted.
async fn publishes_again(members: &[Member]) {
    let orders = key("orders", 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let leader = served(members).await;
        if let Some(term) = members[leader].shards.leading(&orders) {
            let answered = members[leader].publish_within(term, b"again", Duration::from_secs(2));
            if answered.await.is_some() {
                return;
            }
        }
        tokio::time::sleep(timers().heartbeat).await;
    }
    panic!("the shard took no publish once its members were back");
}

/// **A publish to a `Leader` stream is answered on its leader's log inside
/// the group's lease, before any follower holds it, and its subscribers
/// get it once it commits.** Mapped to the commit, the level would cost a
/// round to the followers on every publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_stream_is_answered_on_its_leaders_log() {
    let (net, members, _) = trio_at(ConsistencyLevel::Leader).await;
    let (leader, _, offset, payload) = answered_alone(&net, &members, "l1").await;
    let leader = &members[leader];
    assert!(leader.bound() <= offset, "committed with nobody reachable");
    let subscription = leader.broker.subscribe("t1", "ns", "orders", 0);
    let (mut receiver, _guard) = subscription.await.expect("subscribe").into_parts();
    let early = tokio::time::timeout(Duration::from_millis(20), receiver.recv()).await;
    assert!(early.is_err(), "delivered before its commit: {early:?}");

    // Back before a follower's election timeout, so the leader keeps its
    // term and commits the record.
    net.cut.lock().clear();
    let got = delivered(&mut receiver).await;
    assert_eq!(got, (offset, bytes::Bytes::from_static(payload)));
    assert!(leader.bound() > offset);
    stop(&members).await;
}

/// **A publish to a `Quorum` stream is not answered while no follower
/// has it, lease or no lease.** With every follower cut there is no
/// majority, and an answer on the leader's log alone could be lost with
/// one broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quorum_stream_is_not_answered_without_a_majority() {
    let (net, members, leader) = trio().await;
    let term = members[leader].shards.leading(&key("orders", 0));
    let term = term.expect("term");
    members[leader].publish(term, b"q0").await;

    // Straight after a commit, so the lease holds.
    cut_all(&net);
    for _ in 0..3 {
        let answered = members[leader].publish_within(term, b"q1", UNANSWERED);
        assert_eq!(answered.await, None, "answered with nobody reachable");
    }
    net.cut.lock().clear();
    publishes_again(&members).await;
    stop(&members).await;
}

/// The logs of every member but `leader`, to hold their flushes.
async fn follower_logs(members: &[Member], leader: usize) -> Vec<felix_storage::DiskLog> {
    let orders = key("orders", 0);
    let mut logs = Vec::new();
    for (at, member) in members.iter().enumerate() {
        if at != leader {
            logs.push(member.shards.log_of(&orders).await.expect("open"));
        }
    }
    logs
}

/// **A publish to a `Quorum` stream on a periodic fsync is answered once a
/// majority has written it, before any follower fsyncs it.** That is the
/// hybrid's periodic promise; answered at the commit, every publish would
/// wait for a follower's fsync the stream does not ask for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_periodic_quorum_stream_is_answered_on_a_written_majority() {
    let periodic = FsyncMode::Periodic {
        interval: Duration::from_millis(250),
    };
    let (_net, members, leader) = trio_fsync(ConsistencyLevel::Quorum, periodic).await;
    let term = members[leader].shards.leading(&key("orders", 0));
    let term = term.expect("term");
    // Asks the followers for their written positions from then on.
    members[leader].publish(term, b"q0").await;

    let logs = follower_logs(&members, leader).await;
    let held = futures::future::join_all(logs.iter().map(|log| log.hold_flushes())).await;
    let answered = members[leader].publish_within(term, b"q1", Duration::from_secs(10));
    let offset = answered.await.expect("answered before its commit");
    assert!(members[leader].bound() <= offset, "it committed");
    drop(held);
    until("the answered publish commits", || {
        members[leader].bound() > offset
    })
    .await;
    stop(&members).await;
}

/// **A relaxed broker's shard commits before any follower fsyncs once the
/// fleet finalized `raft_written_commit`.** Its Raft logs open with the
/// broker's own mode, so a written majority is a commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_relaxed_shard_commits_on_a_written_majority_once_the_fleet_allows_it() {
    let periodic = FsyncMode::Periodic {
        interval: Duration::from_secs(3600),
    };
    let (_net, members, leader) = trio_with(ConsistencyLevel::Quorum, periodic, true).await;
    let term = members[leader].shards.leading(&key("orders", 0));
    let term = term.expect("term");
    members[leader].publish(term, b"w0").await;

    let logs = follower_logs(&members, leader).await;
    let held = futures::future::join_all(logs.iter().map(|log| log.hold_flushes())).await;
    let offset = members[leader].publish(term, b"w1").await;
    until("the publish commits with every follower fsync held", || {
        members[leader].bound() > offset
    })
    .await;
    drop(held);
    stop(&members).await;
}

/// **A publish to a `Quorum` stream fsynced on commit is answered only at
/// its commit, though a majority has written it.** Answered on the written
/// count, a power cut on two brokers could take a write the stream's
/// policy promised was on disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quorum_stream_fsynced_on_commit_is_not_answered_on_a_written_majority() {
    let (_net, members, leader) = trio_fsync(ConsistencyLevel::Quorum, FsyncMode::OnCommit).await;
    let term = members[leader].shards.leading(&key("orders", 0));
    let term = term.expect("term");
    members[leader].publish(term, b"q0").await;

    let logs = follower_logs(&members, leader).await;
    let held = futures::future::join_all(logs.iter().map(|log| log.hold_flushes())).await;
    let answered = members[leader].publish_within(term, b"q1", Duration::from_secs(2));
    assert_eq!(answered.await, None, "answered before its commit");
    drop(held);
    publishes_again(&members).await;
    stop(&members).await;
}

/// **A leader nobody has answered for several leases answers no publish
/// to a `Leader` stream, and the shard stays open.** The others may have
/// elected by then. The group takes no write this late, so the lease is
/// not what refuses here: its own rules are tested where a write can be
/// held on the log, in felix-replication's `raft/tests/deliver.rs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_cut_off_past_its_lease_answers_no_leader_publish() {
    let (net, members, leader) = trio_at(ConsistencyLevel::Leader).await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    members[leader].publish(term, b"l0").await;

    cut_all(&net);
    tokio::time::sleep(timers().lease * 4).await;
    let answered = members[leader].publish_within(term, b"l1", UNANSWERED);
    assert_eq!(answered.await, None, "answered outside the lease");
    assert_eq!(members[leader].shards.leading(&orders), Some(term));

    net.cut.lock().clear();
    publishes_again(&members).await;
    stop(&members).await;
}

/// **A record acknowledged on the lease is lost with its leader if no
/// follower held it, and the next leader answers on a lease of its own.**
/// The loss is what the `Leader` level allows. The next leader must not
/// inherit a lease: its own starts with its term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_acknowledgement_does_not_outlive_its_leader() {
    let (net, mut members, _) = trio_at(ConsistencyLevel::Leader).await;
    let (leader, term, lost, lost_payload) = answered_alone(&net, &members, "lost").await;

    let old = members.remove(leader);
    net.runtimes.lock().remove(old.name);
    EngineShards::shutdown(&*old.shards).await;
    net.cut.lock().clear();

    let (next, next_term, kept, kept_payload) = answered_alone(&net, &members, "kept").await;
    assert!(next_term > term);
    net.cut.lock().clear();
    until("the new leader's record commits", || {
        members[next].bound() > kept
    })
    .await;
    let read = members[next]
        .broker
        .read_durable("t1", "ns", "orders", 0, 0, 1 << 20)
        .await
        .expect("read");
    assert!(
        read.iter()
            .any(|record| &record.payload[..] == kept_payload)
    );
    assert!(
        read.iter()
            .all(|record| &record.payload[..] != lost_payload),
        "offset {lost} was acknowledged by the old leader alone"
    );
    stop(&members).await;
}

/// **A member fsyncs on commit unless the fleet finalized
/// `raft_written_commit` and the root's run rotates on a crash.** Either
/// missing, a written entry counted toward a commit could be lost unseen.
#[test]
fn a_member_counts_written_entries_only_on_a_relaxed_run_the_fleet_allows() {
    use felix_storage::run_state;
    let none = FsyncMode::None;
    let root = tempfile::tempdir().expect("tempdir");
    let dir = root.path().join("shard");
    let fresh = || felix_common::random::uuid_v4().into_bytes();
    run_state::begin(root.path(), true, None, None, || false, fresh).expect("begin");
    assert_eq!(super::raft_fsync_mode(none, true, &dir), none);
    assert_eq!(
        super::raft_fsync_mode(none, false, &dir),
        FsyncMode::OnCommit
    );
    run_state::begin(root.path(), false, None, None, || false, fresh).expect("begin");
    assert_eq!(
        super::raft_fsync_mode(none, true, &dir),
        FsyncMode::OnCommit
    );
}

mod retention;
mod trim;
