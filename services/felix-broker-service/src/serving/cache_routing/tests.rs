//! Which broker a cache key resolves to, and what happens when it is not this one.
use felix_common::random::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use felix_router::{NodeRef, RegionRouter, ShardRouter};

use super::*;
use crate::shards::lifecycle::ShardLifecycle;
use crate::shards::routing::routing_table_from;
use crate::shards::watch::ShardAssignment;

const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
/// Deliberately shares a name with the stream in `shard_routing_tests`: a cache
/// and a stream of one name are different things, and every case here would
/// pass by accident if they were not.
const CACHE: &str = "orders";

fn cache_key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: CACHE.to_string(),
        shard,
        kind: ShardKind::Cache,
    }
}

fn stream_key(shard: u32) -> ShardKey {
    ShardKey {
        kind: ShardKind::Stream,
        ..cache_key(shard)
    }
}

fn assignment(key: ShardKey, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key,
        leader: leader.to_string(),
        replicas: Vec::new(),
        generation,
        state: "active".to_string(),
        successor: None,
        routing: Default::default(),
        incarnation: 0,
        fence_storage_ids: Default::default(),
        member_storage_ids: Default::default(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
    }
}

fn node(id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
        region: "us-west-2".to_string(),
        live: true,
    }
}

fn catalog() -> HashMap<String, NodeRef> {
    [node("broker-a", 7001), node("broker-b", 7002)]
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect()
}

/// An ingress router for `broker-a` holding `assignments`, every one of them
/// already through its opening phase.
fn ingress(assignments: &[ShardAssignment]) -> IngressRouter {
    let nodes = catalog();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let owned: HashMap<ShardKey, ShardAssignment> = assignments
        .iter()
        .map(|a| (a.key.clone(), a.clone()))
        .collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);

    let mut lifecycle = ShardLifecycle::new("broker-a");
    for assignment in assignments {
        lifecycle.observe(&assignment.key, Some(assignment));
        lifecycle.opened(&assignment.key, assignment.generation);
    }
    let ingress = IngressRouter::new(router, Arc::clone(lifecycle.fence()));
    ingress.publish_servable(lifecycle.servable());
    ingress
}

/// The property that must not regress: a broker with no cluster identity keeps
/// serving every key itself, exactly as an unsharded cache always did.
#[tokio::test]
async fn a_single_node_broker_serves_every_key_locally() {
    for key in ["a", "b", "anything at all"] {
        match resolve_cache_route(None, TENANT, NAMESPACE, CACHE, key).await {
            CacheRoute::Local { shard: 0, .. } => {}
            other => panic!("expected shard 0 locally, got {other:?}"),
        }
    }
}

/// The whole point. Two brokers resolving the same key must agree on who owns
/// it, or both will serve it and their answers will diverge.
#[tokio::test]
async fn every_key_resolves_to_exactly_one_owner() {
    let assignments: Vec<ShardAssignment> = (0..4)
        .map(|shard| {
            let leader = if shard % 2 == 0 {
                "broker-a"
            } else {
                "broker-b"
            };
            assignment(cache_key(shard), leader, 1)
        })
        .collect();
    let ours = ingress(&assignments);

    let mut local = 0;
    let mut forwarded = 0;
    for n in 0..200 {
        let key = format!("session:{n}");
        match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key).await {
            CacheRoute::Local { shard, .. } => {
                assert!(shard < 4);
                // A key served here must belong to a shard this broker leads.
                assert_eq!(shard % 2, 0, "{key} is shard {shard}, which broker-b leads");
                local += 1;
            }
            CacheRoute::Forward { key: fk, target } => {
                assert_eq!(target.node_id, "broker-b");
                assert_eq!(fk.shard % 2, 1);
                assert_eq!(fk.stream, CACHE);
                forwarded += 1;
            }
            CacheRoute::Refused(reason) => panic!("{key} was refused: {reason}"),
        }
    }

    assert!(
        local > 0 && forwarded > 0,
        "the split was {local}/{forwarded}"
    );
}

/// The collision the kind exists to stop. `orders` the stream is led here and
/// `orders` the cache is led elsewhere; a table that cannot tell them apart
/// keeps one assignment and answers both questions with it.
///
/// Both directions are asserted deliberately. Checking only the cache would
/// pass whenever the surviving entry happened to be the cache's — an accident
/// of insertion order, not a working table. With both, a collapse fails
/// whichever of the two it kept.
#[tokio::test]
async fn a_cache_and_a_stream_of_the_same_name_resolve_to_their_own_owners() {
    let ours = ingress(&[
        assignment(stream_key(0), "broker-a", 1),
        assignment(cache_key(0), "broker-b", 1),
    ]);

    match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, "any-key").await {
        CacheRoute::Forward { target, .. } => assert_eq!(target.node_id, "broker-b"),
        other => panic!("the cache should forward to broker-b, got {other:?}"),
    }

    assert_eq!(
        crate::shards::routing::dispatch(Some(&ours), &stream_key(0)),
        crate::shards::routing::Dispatch::Local { generation: 1 },
        "the stream is led here, and the cache must not have taken its row",
    );
}

/// Widths are per-kind too. The cache has four shards and the stream one, so a
/// cache key resolved against the stream's width would collapse to shard 0.
#[tokio::test]
async fn a_cache_uses_its_own_shard_count() {
    let mut assignments = vec![assignment(stream_key(0), "broker-a", 1)];
    assignments.extend((0..4).map(|shard| assignment(cache_key(shard), "broker-a", 1)));
    let ours = ingress(&assignments);

    let mut shards = std::collections::BTreeSet::new();
    for n in 0..200 {
        let key = format!("k{n}");
        if let CacheRoute::Local { shard, .. } =
            resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key).await
        {
            shards.insert(shard);
        }
    }

    assert!(
        shards.len() > 1,
        "every key landed in {shards:?}; the cache's width was not used",
    );
}

/// An unplaced cache is refused rather than served. Serving it would be the
/// old behaviour: a local write nothing else can see and nothing reconciles.
#[tokio::test]
async fn an_unassigned_cache_shard_is_refused() {
    let ours = ingress(&[assignment(stream_key(0), "broker-a", 1)]);

    match resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, "any-key").await {
        CacheRoute::Refused(_) => {}
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Where a route sends an operation, without the fence place a local one holds.
fn where_to(route: CacheRoute) -> String {
    match route {
        CacheRoute::Local {
            shard, generation, ..
        } => format!("local shard {shard} at {generation}"),
        other => format!("{other:?}"),
    }
}

/// Resolution is a pure function of the key and the view, so two brokers with
/// the same view send the same key to the same place.
#[tokio::test]
async fn resolution_is_deterministic() {
    let assignments: Vec<ShardAssignment> = (0..4)
        .map(|shard| assignment(cache_key(shard), "broker-a", 1))
        .collect();
    let ours = ingress(&assignments);

    for n in 0..50 {
        let key = format!("session:{n}");
        let first =
            where_to(resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key).await);
        for _ in 0..5 {
            let again =
                where_to(resolve_cache_route(Some(&ours), TENANT, NAMESPACE, CACHE, &key).await);
            assert_eq!(first, again);
        }
    }
}

/// The write fence, on the local cache path. Admission still says the shard is
/// served here -- the servable set has not caught up -- but the lifecycle has
/// closed the fence for a move, so every cache write is refused and the cache
/// is left as it was.
mod fence {
    use bytes::Bytes;

    use super::*;
    use crate::serving::forward::CacheRequest;
    use crate::serving::quic::client_error::ClientError;
    use crate::test_support::leader::{self, Leader};

    /// Refused at the fence: nothing was written, so the client may retry
    /// against the new owner.
    fn assert_fenced(refused: ClientError, what: &str) {
        assert_eq!(
            refused.code(),
            &felix_wire::ErrorCode::ShardUnavailable,
            "{what}"
        );
        assert_eq!(refused.retry(), felix_wire::RetryClass::Retry, "{what}");
        let felix_wire::Message::Error { detail, .. } = refused.into_message() else {
            unreachable!()
        };
        assert_eq!(
            detail.and_then(|detail| detail.reason).as_deref(),
            Some(felix_wire::shard_unavailable_reason::FENCED),
            "{what}"
        );
    }

    async fn cache_op(
        leader: &Leader,
        request: CacheRequest,
    ) -> Result<Option<Bytes>, ClientError> {
        apply_cache_op(
            &leader.broker,
            (
                &crate::shards::routing::StreamEngine::default(),
                std::time::Duration::from_secs(1),
            ),
            Some(&leader.ingress),
            None,
            "",
            leader::TENANT,
            leader::NAMESPACE,
            leader::CACHE,
            "session:abc",
            request,
        )
        .await
        .map_err(|err| ClientError::from_anyhow(&err))
    }

    async fn counter_op(
        leader: &Leader,
        request: CacheRequest,
    ) -> Result<Option<i64>, ClientError> {
        apply_counter_op(
            &leader.broker,
            (
                &crate::shards::routing::StreamEngine::default(),
                std::time::Duration::from_secs(1),
            ),
            Some(&leader.ingress),
            None,
            "",
            leader::TENANT,
            leader::NAMESPACE,
            leader::CACHE,
            "hits",
            request,
        )
        .await
        .map_err(|err| ClientError::from_anyhow(&err))
    }

    #[tokio::test]
    async fn cache_writes_after_the_fence_are_refused() {
        let mut leader = Leader::start().await;
        cache_op(&leader, put_request(Bytes::from_static(b"v1"), None))
            .await
            .expect("served before the move");

        leader.fence_move(&leader::cache_key());
        assert_fenced(
            cache_op(&leader, put_request(Bytes::from_static(b"v2"), None))
                .await
                .expect_err("a put landed after the fence closed"),
            "put",
        );
        assert_fenced(
            cache_op(&leader, CacheRequest::Delete)
                .await
                .expect_err("a delete landed after the fence closed"),
            "delete",
        );
        assert_eq!(
            cache_op(&leader, CacheRequest::Get).await,
            Ok(Some(Bytes::from_static(b"v1"))),
            "reads are not fenced, and the value is untouched"
        );
    }

    #[tokio::test]
    async fn a_counter_add_after_the_fence_is_refused() {
        let mut leader = Leader::start().await;
        assert_eq!(
            counter_op(&leader, CacheRequest::CounterAdd { delta: 5 }).await,
            Ok(Some(5))
        );

        leader.fence_move(&leader::cache_key());
        assert_fenced(
            counter_op(&leader, CacheRequest::CounterAdd { delta: 5 })
                .await
                .expect_err("a counter add landed after the fence closed"),
            "counter add",
        );
        assert_eq!(
            counter_op(&leader, CacheRequest::CounterGet).await,
            Ok(Some(5))
        );
    }

    /// A broker whose lease lapsed may already have been replaced as leader,
    /// so cache puts, deletes and counter adds are refused like a publish is,
    /// and so are reads; all are served again once the lease is renewed.
    #[tokio::test]
    async fn cache_and_counter_reads_and_writes_after_the_lease_lapses_are_refused() {
        let leader = Leader::start().await;
        let lease = leader.hold_lease();
        cache_op(&leader, put_request(Bytes::from_static(b"v1"), None))
            .await
            .expect("leased");
        assert_eq!(
            counter_op(&leader, CacheRequest::CounterAdd { delta: 5 }).await,
            Ok(Some(5))
        );

        lease.surrender();
        assert_fenced(
            cache_op(&leader, put_request(Bytes::from_static(b"v2"), None))
                .await
                .expect_err("a put landed without a lease"),
            "put",
        );
        assert_fenced(
            cache_op(&leader, CacheRequest::Delete)
                .await
                .expect_err("a delete landed without a lease"),
            "delete",
        );
        assert_fenced(
            counter_op(&leader, CacheRequest::CounterAdd { delta: 5 })
                .await
                .expect_err("a counter add landed without a lease"),
            "counter add",
        );
        // Reads follow the lease too: another broker may lead the shard now.
        assert_fenced(
            cache_op(&leader, CacheRequest::Get)
                .await
                .expect_err("a get was served without a lease"),
            "get",
        );
        assert_fenced(
            counter_op(&leader, CacheRequest::CounterGet)
                .await
                .expect_err("a counter read was served without a lease"),
            "counter get",
        );

        lease.renew();
        assert_eq!(
            cache_op(&leader, CacheRequest::Get).await,
            Ok(Some(Bytes::from_static(b"v1")))
        );
        assert_eq!(
            counter_op(&leader, CacheRequest::CounterAdd { delta: 1 }).await,
            Ok(Some(6))
        );
    }

    /// A cache write to the old owner between the fence and the cut-over is
    /// held, then sent to the new owner rather than refused.
    #[tokio::test]
    async fn a_cache_write_during_a_move_follows_the_cut_over() {
        let hold =
            crate::shards::routing::hold::MoveHold::new(std::time::Duration::from_secs(5), 16);
        let mut leader = Leader::start_holding(hold).await;
        leader.fence_move(&leader::cache_key());

        let ingress = Arc::clone(&leader.ingress);
        let route = tokio::spawn(async move {
            match resolve_cache_route(
                Some(&ingress),
                leader::TENANT,
                leader::NAMESPACE,
                leader::CACHE,
                "session:abc",
            )
            .await
            {
                CacheRoute::Forward { target, .. } => (target.node_id, target.generation),
                other => panic!("expected a forward, got {other:?}"),
            }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !route.is_finished(),
            "the write is held while the shard moves"
        );

        leader.cut_over(&leader::cache_key());
        assert_eq!(
            route.await.expect("route"),
            (leader::SUCCESSOR.to_string(), leader::GENERATION + 1)
        );
    }
}

/// A write the cache store refused. Acknowledging it would tell the client a
/// value is set that no read will ever return.
mod store_failure {
    use bytes::Bytes;

    use super::*;
    use crate::serving::forward::CacheRequest;
    use crate::serving::quic::client_error::ClientError;

    /// A log-backed cache whose shard can never open: its root was replaced
    /// by a plain file after the store was created.
    fn broken_cache(dir: &std::path::Path) -> felix_broker::Broker {
        let root = dir.join("caches");
        let cache = felix_storage::LogCache::open(&root, felix_storage::log::LogConfig::default())
            .expect("open cache");
        std::fs::remove_dir(&root).expect("remove root");
        std::fs::write(&root, b"not a directory").expect("replace root");
        felix_broker::Broker::new(Box::new(cache))
    }

    async fn cache_op(
        broker: &felix_broker::Broker,
        request: CacheRequest,
    ) -> Result<Option<Bytes>, ClientError> {
        apply_cache_op(
            broker,
            (
                &crate::shards::routing::StreamEngine::default(),
                std::time::Duration::from_secs(1),
            ),
            None,
            None,
            "",
            TENANT,
            NAMESPACE,
            CACHE,
            "k",
            request,
        )
        .await
        .map_err(|err| ClientError::from_anyhow(&err))
    }

    #[tokio::test]
    async fn a_put_the_store_refused_is_not_acknowledged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let broker = broken_cache(dir.path());
        for (what, request) in [
            ("put", put_request(Bytes::from_static(b"v"), None)),
            ("delete", CacheRequest::Delete),
            ("get", CacheRequest::Get),
        ] {
            let refused = cache_op(&broker, request).await.expect_err(&format!(
                "a {what} the store failed was answered as a success"
            ));
            assert_eq!(refused.code(), &felix_wire::ErrorCode::Storage, "{what}");
        }
    }
}

/// The cache and counter waits take their marks and timeout from the engine
/// the caller holds. An engine without marks answers each of these at once.
mod quorum_waits {
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use felix_replication::quorum::QuorumMarks;

    use super::*;
    use crate::serving::forward::CacheRequest;
    use crate::serving::quic::client_error::ClientError;
    use crate::shards::routing::StreamEngine;
    use crate::test_support::leader::{self, GENERATION, Leader};

    const TIMEOUT: Duration = Duration::from_millis(300);

    fn engine(leader: &Leader, marks: &Arc<QuorumMarks>) -> StreamEngine {
        StreamEngine::new(Some(Arc::clone(marks)), Some(Arc::clone(&leader.ingress)))
    }

    async fn cache_op(
        leader: &Leader,
        engine: &StreamEngine,
        request: CacheRequest,
    ) -> Result<Option<Bytes>, ClientError> {
        apply_cache_op(
            &leader.broker,
            (engine, TIMEOUT),
            Some(&leader.ingress),
            None,
            "",
            leader::TENANT,
            leader::NAMESPACE,
            leader::QUORUM_CACHE,
            "k",
            request,
        )
        .await
        .map_err(|err| ClientError::from_anyhow(&err))
    }

    async fn counter_op(
        leader: &Leader,
        engine: &StreamEngine,
        request: CacheRequest,
    ) -> Result<Option<i64>, ClientError> {
        apply_counter_op(
            &leader.broker,
            (engine, TIMEOUT),
            Some(&leader.ingress),
            None,
            "",
            leader::TENANT,
            leader::NAMESPACE,
            leader::QUORUM_CACHE,
            "hits",
            request,
        )
        .await
        .map_err(|err| ClientError::from_anyhow(&err))
    }

    /// Runs `op` and says how long it took.
    async fn timed<T>(op: impl std::future::Future<Output = T>) -> (T, Duration) {
        let started = Instant::now();
        let out = op.await;
        (out, started.elapsed())
    }

    fn waited_the_timeout(waited: Duration, what: &str) {
        assert!(waited >= TIMEOUT, "{what} ended early after {waited:?}");
        assert!(waited < TIMEOUT * 6, "{what} ended late after {waited:?}");
    }

    #[tokio::test]
    async fn a_quorum_cache_write_and_read_wait_their_timeout_for_the_mark() {
        let leader = Leader::start().await;
        let _lease = leader.hold_lease();
        let marks = Arc::new(QuorumMarks::new());
        let engine = engine(&leader, &marks);
        let put = || CacheRequest::Put {
            value: Bytes::from_static(b"v"),
            ttl_ms: 0,
        };

        let conditional = || {
            [
                (
                    "put if",
                    CacheRequest::PutIf {
                        value: Bytes::from_static(b"v"),
                        ttl_ms: 0,
                        condition: felix_storage::CacheCondition::Absent,
                    },
                ),
                ("delete if", CacheRequest::DeleteIf { version: 1 }),
                ("get versioned", CacheRequest::GetVersioned),
            ]
        };

        for (what, request) in [
            ("put", put()),
            ("delete", CacheRequest::Delete),
            ("get", CacheRequest::Get),
        ]
        .into_iter()
        .chain(conditional())
        {
            let (result, waited) = timed(cache_op(&leader, &engine, request)).await;
            result.expect_err(what);
            waited_the_timeout(waited, what);
        }

        marks.publish(&leader::quorum_cache_key(), GENERATION, 1 << 20);
        for (what, request) in [
            ("put", put()),
            ("get", CacheRequest::Get),
            ("delete", CacheRequest::Delete),
        ]
        .into_iter()
        .chain(conditional())
        {
            let (result, waited) = timed(cache_op(&leader, &engine, request)).await;
            result.unwrap_or_else(|err| panic!("{what}: {err:?}"));
            assert!(waited < TIMEOUT, "{what} waited though the mark passed");
        }
    }

    #[tokio::test]
    async fn a_quorum_counter_add_and_read_wait_their_timeout_for_the_mark() {
        let leader = Leader::start().await;
        let _lease = leader.hold_lease();
        let marks = Arc::new(QuorumMarks::new());
        let engine = engine(&leader, &marks);

        let (result, waited) = timed(counter_op(
            &leader,
            &engine,
            CacheRequest::CounterAdd { delta: 2 },
        ))
        .await;
        result.expect_err("add");
        waited_the_timeout(waited, "add");
        let (result, waited) = timed(counter_op(&leader, &engine, CacheRequest::CounterGet)).await;
        result.expect_err("get");
        waited_the_timeout(waited, "get");

        marks
            .counters()
            .publish(&leader::quorum_cache_key(), GENERATION, 1 << 20);
        let (result, waited) = timed(counter_op(
            &leader,
            &engine,
            CacheRequest::CounterAdd { delta: 2 },
        ))
        .await;
        assert_eq!(result, Ok(Some(4)));
        assert!(waited < TIMEOUT, "add waited though the mark passed");
        let (result, waited) = timed(counter_op(&leader, &engine, CacheRequest::CounterGet)).await;
        assert_eq!(result, Ok(Some(4)));
        assert!(waited < TIMEOUT, "get waited though the mark passed");
    }

    /// A read in a fleet that reads by round skips the lease check before its
    /// value, so a lapsed lease does not refuse it. An engine without marks
    /// would refuse each of these.
    #[tokio::test]
    async fn a_quorum_read_by_round_is_not_refused_for_a_lapsed_lease() {
        use felix_common::fleet::{FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK};
        struct Confirms;
        #[async_trait::async_trait]
        impl felix_replication::quorum::LeadershipCheck for Confirms {
            async fn confirm(
                &self,
                _key: &ShardKey,
                _generation: u64,
            ) -> Result<(), felix_replication::quorum::QuorumError> {
                Ok(())
            }
        }

        let leader = Leader::start().await;
        let lease = leader.hold_lease();
        let names = [
            GENERATION_START.name(),
            MAJORITY_ACK.name(),
            LEASE_FREE_READS.name(),
        ];
        let fleet = Arc::new(FleetGate::new(names));
        let marks = Arc::new(QuorumMarks::with_fleet(Arc::clone(&fleet)));
        marks.set_read_check(Arc::new(Confirms), false);
        fleet.observe(names);
        marks.publish(&leader::quorum_cache_key(), GENERATION, 1 << 20);
        marks
            .counters()
            .publish(&leader::quorum_cache_key(), GENERATION, 1 << 20);
        let engine = engine(&leader, &marks);

        lease.surrender();
        for (what, request) in [
            ("get", CacheRequest::Get),
            ("get versioned", CacheRequest::GetVersioned),
        ] {
            cache_op(&leader, &engine, request)
                .await
                .unwrap_or_else(|err| panic!("{what}: {err:?}"));
        }
        counter_op(&leader, &engine, CacheRequest::CounterGet)
            .await
            .expect("counter get");
    }
}
