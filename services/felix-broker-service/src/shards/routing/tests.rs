//! Ingress dispatch: what a broker does with a request for each ownership state.
use felix_common::random::HashMap;

use felix_replication::controlplane_link::HttpControlPlaneLink;
use felix_router::{NodeRef, RegionRouter};

use super::*;
use crate::shards::lifecycle::ShardLifecycle;
use crate::shards::watch::ShardAssignment;

fn no_catalog() -> Option<CatalogSource<HttpControlPlaneLink>> {
    None
}

mod hold;
mod quorum;

fn key(shard: u32) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard,
        kind: crate::shards::ShardKind::Stream,
    }
}

fn assignment(shard: u32, leader: &str, generation: u64) -> ShardAssignment {
    ShardAssignment {
        key: key(shard),
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

fn node(id: &str, port: u16, live: bool) -> NodeRef {
    NodeRef {
        node_id: id.to_string(),
        advertise_addr: SocketAddr::from(([10, 0, 0, 4], port)),
        region: "us-west-2".to_string(),
        live,
    }
}

fn catalog() -> HashMap<String, NodeRef> {
    [node("broker-a", 7001, true), node("broker-b", 7002, true)]
        .into_iter()
        .map(|n| (n.node_id.clone(), n))
        .collect()
}

/// Build an ingress router whose view is `assignments`, with `opened` shards
/// already through their opening phase locally.
fn ingress(assignments: &[ShardAssignment], opened: &[u32]) -> IngressRouter {
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
        if opened.contains(&assignment.key.shard) {
            lifecycle.opened(&assignment.key, assignment.generation);
        }
    }
    let ingress = IngressRouter::new(router, Arc::clone(lifecycle.fence()));
    ingress.publish_servable(lifecycle.servable());
    ingress
}

/// The property that must not regress: a broker with no cluster identity
/// behaves exactly as it did before clustering existed.
#[tokio::test]
async fn a_single_node_broker_always_serves_locally() {
    assert_eq!(dispatch(None, &key(0)), Dispatch::Local { generation: 0 });
    assert_eq!(dispatch(None, &key(41)), Dispatch::Local { generation: 0 });
}

#[tokio::test]
async fn an_owned_and_open_shard_is_served_locally() {
    let ingress = ingress(&[assignment(0, "broker-a", 3)], &[0]);
    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Local { generation: 3 }
    );
}

/// The reason ownership and readiness are two separate sources: the cluster
/// says this shard is ours, but the log is not recovered, and acknowledging a
/// write now would promise durability that is not set up.
#[tokio::test]
async fn an_owned_shard_still_opening_is_not_served() {
    let ingress = ingress(&[assignment(0, "broker-a", 3)], &[]);
    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Unavailable(Reason::NotReady),
    );
}

/// Remote must be its own outcome, not an error, or forwarding has nothing to
/// act on.
#[tokio::test]
async fn a_shard_owned_elsewhere_is_forwardable() {
    let ingress = ingress(&[assignment(0, "broker-b", 2)], &[]);
    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Forward {
            node_id: "broker-b".to_string(),
            advertise_addr: SocketAddr::from(([10, 0, 0, 4], 7002)),
            // Carried through so the owner can compare it with its own.
            generation: 2,
            incarnation: 0,
        },
    );
}

/// The acceptance criterion: an unassigned shard must never be accepted
/// locally, or a broker writes data nobody asked it to hold.
#[tokio::test]
async fn an_unassigned_shard_is_refused() {
    let ingress = ingress(&[], &[]);
    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Unavailable(Reason::NotAssigned),
    );
}

#[tokio::test]
async fn an_unavailable_owner_is_reported_with_its_reason() {
    let nodes: HashMap<String, NodeRef> = [
        node("broker-a", 7001, true),
        node("broker-dead", 7009, false),
    ]
    .into_iter()
    .map(|n| (n.node_id.clone(), n))
    .collect();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let owned: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-dead", 1))]
        .into_iter()
        .collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);

    let ingress = IngressRouter::new(router, Arc::default());
    match dispatch(Some(&ingress), &key(0)) {
        Dispatch::Unavailable(Reason::OwnerUnavailable(detail)) => {
            assert!(detail.contains("broker-dead"), "{detail}");
        }
        other => panic!("expected an unavailable owner, got {other:?}"),
    }
}

/// A shard moving away must stop being served here immediately, not on the next
/// restart.
#[tokio::test]
async fn losing_a_shard_stops_local_service() {
    let nodes = catalog();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let mut lifecycle = ShardLifecycle::new("broker-a");
    let mine = assignment(0, "broker-a", 1);
    lifecycle.observe(&mine.key, Some(&mine));
    lifecycle.opened(&mine.key, 1);

    let owned: HashMap<ShardKey, ShardAssignment> = [(key(0), mine)].into_iter().collect();
    router.publish(routing_table_from(&owned, &nodes), &nodes);
    let ingress = IngressRouter::new(Arc::clone(&router), Arc::clone(lifecycle.fence()));
    ingress.publish_servable(lifecycle.servable());
    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Local { generation: 1 }
    );

    // Reassigned to broker-b. Local state has not caught up yet, and the new
    // route alone decides.
    let moved: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-b", 2))]
        .into_iter()
        .collect();
    ingress.publish(
        routing_table_from(&moved, &nodes),
        &nodes,
        lifecycle.servable(),
    );

    assert!(
        matches!(dispatch(Some(&ingress), &key(0)), Dispatch::Forward { .. }),
        "a reassigned shard must stop being served here",
    );
}

/// The router moved on before local state did. Serving at the old generation
/// would be two brokers believing they lead the same shard.
#[tokio::test]
async fn a_local_route_at_a_newer_generation_is_not_served_until_reopened() {
    let nodes = catalog();
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let mut lifecycle = ShardLifecycle::new("broker-a");
    let first = assignment(0, "broker-a", 1);
    lifecycle.observe(&first.key, Some(&first));
    lifecycle.opened(&first.key, 1);

    // The table now says generation 4; local state is still at 1.
    let newer: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-a", 4))]
        .into_iter()
        .collect();
    router.publish(routing_table_from(&newer, &nodes), &nodes);

    let ingress = IngressRouter::new(router, Arc::clone(lifecycle.fence()));
    ingress.publish_servable(lifecycle.servable());
    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Unavailable(Reason::NotReady),
        "local state must catch up before serving the new generation",
    );
}

#[tokio::test]
async fn a_multi_shard_stream_dispatches_per_shard() {
    let ingress = ingress(
        &[
            assignment(0, "broker-a", 1),
            assignment(1, "broker-b", 1),
            assignment(2, "broker-a", 1),
        ],
        &[0, 2],
    );

    assert_eq!(
        dispatch(Some(&ingress), &key(0)),
        Dispatch::Local { generation: 1 }
    );
    assert!(matches!(
        dispatch(Some(&ingress), &key(1)),
        Dispatch::Forward { .. }
    ));
    assert_eq!(
        dispatch(Some(&ingress), &key(2)),
        Dispatch::Local { generation: 1 }
    );
}

/// Without a routing key on the wire there is only one shard to choose, and
/// choosing it must be stable.
#[test]
fn shard_selection_is_deterministic() {
    assert_eq!(shard_for(1, None), 0);
    assert_eq!(shard_for(4, None), 0, "no key on the wire yet");

    // The mapping a negotiated key would use, pinned so it cannot drift.
    for shards in [2u32, 4, 8, 16] {
        let first = shard_for(shards, Some(b"customer-42"));
        assert!(first < shards);
        for _ in 0..10 {
            assert_eq!(shard_for(shards, Some(b"customer-42")), first);
        }
    }
}

#[test]
fn keys_spread_across_shards() {
    let mut seen = felix_common::random::HashSet::default();
    for i in 0..200u32 {
        seen.insert(shard_for(8, Some(format!("key-{i}").as_bytes())));
    }
    assert_eq!(seen.len(), 8, "every shard should be reachable: {seen:?}");
}

#[test]
fn a_single_shard_stream_always_maps_to_zero() {
    assert_eq!(shard_for(0, Some(b"anything")), 0);
    assert_eq!(shard_for(1, Some(b"anything")), 0);
}

/// **A failover's storage identities reach the route its fence reads**, and
/// an assignment that carries none, as from an older control plane, has none.
#[test]
fn a_failovers_storage_ids_reach_the_route() {
    let promoted: ShardAssignment = serde_json::from_value(serde_json::json!({
        "tenant_id": "t1", "namespace": "ns", "stream": "orders", "shard": 0, "kind": "stream",
        "leader": "broker-a", "replicas": ["broker-b"], "generation": 5, "state": "active",
        "fence_storage_ids": {"broker-b": "00112233445566778899aabbccddeeff"},
    }))
    .expect("assignment");
    let older = assignment(1, "broker-a", 5);
    let assignments: HashMap<ShardKey, ShardAssignment> = [promoted, older]
        .into_iter()
        .map(|a| (a.key.clone(), a))
        .collect();

    let table = routing_table_from(&assignments, &catalog());

    let ids = table
        .get(&to_router_key(&key(0)))
        .and_then(|route| route.fence_storage_ids.clone())
        .expect("ids");
    assert_eq!(
        ids.get("broker-b").map(String::as_str),
        Some("00112233445566778899aabbccddeeff")
    );
    assert_eq!(
        table
            .get(&to_router_key(&key(1)))
            .map(|route| route.fence_storage_ids.clone()),
        Some(None)
    );
}

/// The feed task: it keeps local shard state, the servable set, and the routing
/// table in step, and it is the only thing that refreshes the address book.
mod feed {
    use super::*;
    use crate::shards::lifecycle::EphemeralShardStore;
    use crate::shards::watch::ShardOwnership;
    use axum::Router;
    use axum::routing::get;
    use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// A `/v1/nodes` endpoint that can be told to start failing, so the
    /// refresh-failure path is exercised without stopping a server.
    async fn nodes_endpoint(failing: Arc<AtomicBool>) -> (String, tokio::task::JoinHandle<()>) {
        nodes_endpoint_at(
            failing,
            Arc::new(AtomicU16::new(7002)),
            Arc::new(AtomicBool::new(false)),
        )
        .await
    }

    /// As [`nodes_endpoint`], with broker-b's port settable, as a restart that
    /// re-registers on new ports would change it, and an endpoint that can be
    /// told to take requests and never answer, as a paused control plane does.
    async fn nodes_endpoint_at(
        failing: Arc<AtomicBool>,
        port: Arc<AtomicU16>,
        hanging: Arc<AtomicBool>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/v1/nodes",
            get(move || {
                let failing = Arc::clone(&failing);
                let hanging = hanging.load(Ordering::Acquire);
                let port = port.load(Ordering::Acquire);
                async move {
                    if hanging {
                        std::future::pending::<()>().await;
                    }
                    if failing.load(Ordering::Acquire) {
                        return Err(axum::http::StatusCode::SERVICE_UNAVAILABLE);
                    }
                    Ok(axum::Json(serde_json::json!({
                        "items": [{
                            "node": {
                                "node_id": "broker-b",
                                "spec": {
                                    "advertise_addr": format!("10.0.0.4:{port}"),
                                    "region": "us-west-2",
                                },
                            },
                            "placement": { "eligible": true },
                        }],
                    })))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        (format!("http://{addr}"), task)
    }

    fn state(assignments: &[ShardAssignment]) -> (FeedState, Arc<ShardRouter>) {
        let router = Arc::new(ShardRouter::new(
            "broker-a",
            "us-west-2",
            RegionRouter::new("us-west-2".to_string()),
        ));
        let lifecycle = ShardLifecycle::new("broker-a");
        let ingress = Arc::new(IngressRouter::new(
            Arc::clone(&router),
            Arc::clone(lifecycle.fence()),
        ));
        (
            FeedState {
                client_endpoints: None,
                ownership: Arc::new(tokio::sync::RwLock::new({
                    let mut ownership = ShardOwnership::default();
                    ownership.reset(assignments.to_vec());
                    ownership
                })),
                lifecycle: Arc::new(tokio::sync::Mutex::new(lifecycle)),
                store: Arc::new(EphemeralShardStore::default()),
                ingress,
                assignments_changed: Arc::default(),
                routes_changed: Arc::default(),
                joint_fence: None,
                prefetch: None,
                elections: None,
                nodes: HashMap::default(),
                engine_shards: None,
            },
            router,
        )
    }

    /// Poll rather than sleep a fixed amount: the feed ticks on its own clock.
    async fn until<F: Fn() -> bool>(what: &str, condition: F) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {what}");
    }

    /// A shard this broker leads becomes servable without anything else acting.
    #[tokio::test]
    async fn the_feed_opens_and_publishes_a_led_shard() {
        let (state, _router) = state(&[assignment(0, "broker-a", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let shutdown = CancellationToken::new();

        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_millis(20),
            shutdown.clone(),
        );
        until("the shard to be served locally", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Local { .. })
        })
        .await;

        shutdown.cancel();
        let _ = feed.await;
    }

    /// Takes shard 1 and records that it did.
    #[derive(Default)]
    struct TakesShardOne {
        taken: std::sync::Mutex<Vec<ShardAssignment>>,
    }

    impl EngineShards for TakesShardOne {
        fn take<'a>(
            &'a self,
            assignments: &'a mut HashMap<ShardKey, ShardAssignment>,
        ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                if let Some(assignment) = assignments.remove(&key(1)) {
                    *self.taken.lock().expect("lock") = vec![assignment];
                }
            })
        }

        fn dispatch(
            &self,
            _key: &ShardKey,
            _nodes: &dyn Fn(&str) -> Option<std::net::SocketAddr>,
        ) -> Option<Dispatch> {
            None
        }
    }

    /// **A shard another engine took is never opened or routed by the
    /// hybrid**, or one shard would have two engines writing it.
    #[tokio::test]
    async fn a_shard_another_engine_took_is_left_alone() {
        let (mut state, _router) =
            state(&[assignment(0, "broker-a", 1), assignment(1, "broker-a", 1)]);
        let engine = Arc::new(TakesShardOne::default());
        state.engine_shards = Some(Arc::clone(&engine) as Arc<dyn EngineShards>);
        let ingress = Arc::clone(&state.ingress);
        let lifecycle = Arc::clone(&state.lifecycle);
        let shutdown = CancellationToken::new();

        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_millis(20),
            shutdown.clone(),
        );
        // Shard 0 is opened in the same pass, after the take.
        until("the hybrid's shard to be served locally", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Local { .. })
        })
        .await;
        assert_eq!(engine.taken.lock().expect("lock").len(), 1);
        assert!(!matches!(ingress.dispatch(&key(1)), Dispatch::Local { .. }));
        let lifecycle = lifecycle.lock().await;
        assert!(lifecycle.generation(&key(0)).is_some());
        assert!(lifecycle.generation(&key(1)).is_none());
        drop(lifecycle);

        shutdown.cancel();
        let _ = feed.await;
    }

    /// A store whose every open takes `delay`, as one that fsyncs does.
    struct SlowStore {
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl crate::shards::lifecycle::ShardStore for SlowStore {
        async fn open(
            &self,
            _key: &ShardKey,
            _generation: u64,
            _incarnation: u64,
            _begins_here: bool,
        ) -> anyhow::Result<()> {
            tokio::time::sleep(self.delay).await;
            Ok(())
        }

        async fn release(&self, _key: &ShardKey) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// **A new shard serves once it is open, not once its whole batch is.**
    /// A bulk create hands the feed hundreds of new shards in one pass, and
    /// they open one after another.
    #[tokio::test]
    async fn a_new_shard_is_routed_before_the_rest_of_its_batch_opens() {
        let shards = 8;
        let assignments: Vec<_> = (0..shards)
            .map(|shard| assignment(shard, "broker-a", 1))
            .collect();
        let (mut state, _router) = state(&assignments);
        state.store = Arc::new(SlowStore {
            delay: Duration::from_millis(150),
        });
        let ingress = Arc::clone(&state.ingress);
        let shutdown = CancellationToken::new();
        let local = || {
            (0..shards)
                .filter(|shard| matches!(ingress.dispatch(&key(*shard)), Dispatch::Local { .. }))
                .count()
        };

        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        until("a shard to be served locally", || local() > 0).await;
        let first = local();
        until("every shard to be served locally", || {
            local() == shards as usize
        })
        .await;

        assert!(
            first < shards as usize,
            "no shard was routed until all {shards} had opened"
        );
        shutdown.cancel();
        let _ = feed.await;
    }

    /// **A led shard is not routed before it serves.** Replication starts
    /// work for every led route it sees, so naming the unopened rest of a
    /// batch would fill its pass slots with shards that cannot take a write.
    #[tokio::test]
    async fn a_led_shard_still_opening_is_not_routed_early() {
        let shards = 8;
        let assignments: Vec<_> = (0..shards)
            .map(|shard| assignment(shard, "broker-a", 1))
            .collect();
        let (mut state, _router) = state(&assignments);
        state.store = Arc::new(SlowStore {
            delay: Duration::from_millis(150),
        });
        let ingress = Arc::clone(&state.ingress);
        let shutdown = CancellationToken::new();
        let count = |f: &dyn Fn(u32) -> bool| (0..shards).filter(|shard| f(*shard)).count();

        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        let mut sampled = 0;
        for _ in 0..400 {
            // Routed first: serving only grows, so a routed shard that was
            // not yet serving would show as more routed than serving.
            let routed = count(&|shard| ingress.route(&key(shard)).is_some());
            let serving =
                count(&|shard| matches!(ingress.dispatch(&key(shard)), Dispatch::Local { .. }));
            assert!(routed <= serving, "{routed} routed, {serving} serving");
            if serving > 0 && serving < shards as usize {
                sampled += 1;
            }
            if serving == shards as usize {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(sampled > 0, "never saw the batch part open");
        shutdown.cancel();
        let _ = feed.await;
    }

    /// **Only the new shards change mid-pass.** A shard reassigned away in
    /// the same pass keeps the route and the serving state last published
    /// until the pass ends, so a write to it is held as it was, not refused.
    #[tokio::test]
    async fn a_shard_moving_away_mid_pass_keeps_its_published_view() {
        let (mut state, _router) = state(&[assignment(0, "broker-a", 1)]);
        state.store = Arc::new(SlowStore {
            delay: Duration::from_millis(150),
        });
        let ingress = Arc::clone(&state.ingress);
        let ownership = Arc::clone(&state.ownership);
        let changed = Arc::clone(&state.assignments_changed);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        until("shard 0 to be served locally", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Local { generation: 1 })
        })
        .await;

        let mut next = vec![assignment(0, "broker-b", 2)];
        next.extend((1..5).map(|shard| assignment(shard, "broker-a", 1)));
        ownership.write().await.reset(next);
        changed.notify_one();
        until("a new shard to be served locally", || {
            (1..5).any(|shard| matches!(ingress.dispatch(&key(shard)), Dispatch::Local { .. }))
        })
        .await;
        let mid_pass = ingress.dispatch(&key(0));
        let all_open =
            (1..5).all(|shard| matches!(ingress.dispatch(&key(shard)), Dispatch::Local { .. }));

        assert!(!all_open, "the pass published nothing before it ended");
        assert!(
            matches!(mid_pass, Dispatch::Local { generation: 1 }),
            "shard 0 changed mid-pass: {mid_pass:?}"
        );
        shutdown.cancel();
        let _ = feed.await;
    }

    /// **A shard whose fence finishes mid-pass stays served.** The fence
    /// publishes it on its own; the pass's interim publishes must not put it
    /// back to not ready until the pass ends.
    #[tokio::test]
    async fn a_shard_fenced_mid_pass_stays_served() {
        let (mut state, _router) = state(&[assignment(0, "broker-a", 1)]);
        state.lifecycle.lock().await.fence_promotions();
        state.store = Arc::new(SlowStore {
            delay: Duration::from_millis(150),
        });
        let ingress = Arc::clone(&state.ingress);
        let lifecycle = Arc::clone(&state.lifecycle);
        let ownership = Arc::clone(&state.ownership);
        let changed = Arc::clone(&state.assignments_changed);
        let fencing = || async {
            lifecycle
                .lock()
                .await
                .counts()
                .get(&crate::shards::lifecycle::Phase::Fencing)
                .copied()
                .unwrap_or(0)
        };
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        for _ in 0..200 {
            if fencing().await == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // Let the first pass end and publish shard 0 as not yet servable.
        tokio::time::sleep(Duration::from_millis(100)).await;

        ownership.write().await.reset(
            (0..5)
                .map(|shard| assignment(shard, "broker-a", 1))
                .collect(),
        );
        changed.notify_one();
        for _ in 0..200 {
            if fencing().await >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        {
            let mut owned = lifecycle.lock().await;
            assert!(owned.fenced(&key(0), 1), "shard 0 was not fencing");
            ingress.publish_servable(owned.servable());
        }
        // Long enough for at least one interim publish.
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mid_pass = ingress.dispatch(&key(0));
        let opened = fencing().await;

        assert!(opened < 4, "the pass ended before the check");
        assert!(
            matches!(mid_pass, Dispatch::Local { generation: 1 }),
            "shard 0 went back mid-pass: {mid_pass:?}"
        );
        shutdown.cancel();
        let _ = feed.await;
    }

    /// **A removal a fence publishes mid-pass stays published.** The pass
    /// holds back its own removals, but must not put back one the fence's
    /// publish already made.
    #[tokio::test]
    async fn a_shard_a_fence_unpublished_mid_pass_stays_unpublished() {
        let (mut state, _router) = state(&[assignment(0, "broker-a", 1)]);
        state.store = Arc::new(SlowStore {
            delay: Duration::from_millis(150),
        });
        let ingress = Arc::clone(&state.ingress);
        let lifecycle = Arc::clone(&state.lifecycle);
        let ownership = Arc::clone(&state.ownership);
        let changed = Arc::clone(&state.assignments_changed);
        let local = || {
            (1..5)
                .filter(|shard| matches!(ingress.dispatch(&key(*shard)), Dispatch::Local { .. }))
                .count()
        };
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        until("shard 0 to be served locally", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Local { generation: 1 })
        })
        .await;

        let mut next = vec![assignment(0, "broker-b", 2)];
        next.extend((1..5).map(|shard| assignment(shard, "broker-a", 1)));
        ownership.write().await.reset(next);
        changed.notify_one();
        until("a new shard to be served locally", || local() > 0).await;
        // What a fence finishing on another shard publishes.
        ingress.publish_servable(lifecycle.lock().await.servable());
        let before = local();
        until("an interim publish after the fence's", || local() > before).await;
        let mid_pass = ingress.dispatch(&key(0));
        let opened = local();

        assert!(opened < 4, "the pass ended before the check");
        assert!(
            !matches!(mid_pass, Dispatch::Local { .. }),
            "shard 0 was served again mid-pass: {mid_pass:?}"
        );
        shutdown.cancel();
        let _ = feed.await;
    }

    /// **A fence finishing as the pass publishes stays published.** The pass
    /// reads the servable set and then publishes it; a fence that lands in
    /// between must not be overwritten by the older set.
    #[tokio::test]
    async fn a_fence_finishing_during_the_end_of_pass_publish_is_kept() {
        let (state, _router) = state(&[assignment(0, "broker-a", 1)]);
        state.lifecycle.lock().await.fence_promotions();
        let ingress = Arc::clone(&state.ingress);
        let lifecycle = Arc::clone(&state.lifecycle);
        let publishes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        // A fence finishing just before the view is stored, if it can get
        // the lock it finishes under.
        *ingress.before_publish.lock().unwrap() = Some(Box::new({
            let lifecycle = Arc::clone(&lifecycle);
            let publishes = Arc::clone(&publishes);
            move |ingress: &IngressRouter| {
                if let Ok(mut owned) = lifecycle.try_lock()
                    && owned.fenced(&key(0), 1)
                {
                    ingress.publish_servable(owned.servable());
                }
                publishes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }));
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        // One interim publish for the new shard, then the end-of-pass one.
        until("the first pass to publish", || {
            publishes.load(std::sync::atomic::Ordering::SeqCst) >= 2
        })
        .await;
        // The fence finishes after the pass, if the pass kept it out.
        {
            let mut owned = lifecycle.lock().await;
            if owned.fenced(&key(0), 1) {
                ingress.publish_servable(owned.servable());
            }
        }

        let after = ingress.dispatch(&key(0));
        assert!(
            matches!(after, Dispatch::Local { generation: 1 }),
            "the pass undid the fence's publish: {after:?}"
        );
        shutdown.cancel();
        let _ = feed.await;
    }

    /// A shard led elsewhere is only forwardable once the catalog supplies an
    /// address. Before that it is known and unreachable, which is the state the
    /// address book exists to leave.
    #[tokio::test]
    async fn the_feed_makes_a_remote_shard_forwardable_once_the_catalog_arrives() {
        let failing = Arc::new(AtomicBool::new(false));
        let (base_url, server) = nodes_endpoint(Arc::clone(&failing)).await;
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let shutdown = CancellationToken::new();

        let feed = spawn_feed(
            state,
            Some(CatalogSource {
                link: HttpControlPlaneLink::new(
                    reqwest::Client::builder()
                        .no_proxy()
                        .build()
                        .expect("client"),
                    &base_url,
                ),
                token: Some(crate::cluster::credential::NodeCredential::new("a-token")),
            }),
            Duration::from_millis(20),
            shutdown.clone(),
        );

        until("the remote shard to become forwardable", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Forward { .. })
        })
        .await;

        shutdown.cancel();
        let _ = feed.await;
        server.abort();
    }

    /// The feed reads the catalog through the link alone.
    #[tokio::test]
    async fn a_fake_link_supplies_the_catalog() {
        let link = crate::test_support::fake_link::FakeLink::default().reply(
            "node_catalog_page",
            serde_json::json!({
                "items": [{
                    "node": {
                        "node_id": "broker-b",
                        "spec": { "advertise_addr": "10.0.0.4:7002", "region": "us-west-2" },
                    },
                    "placement": { "eligible": true },
                }],
            }),
        );
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let shutdown = CancellationToken::new();

        let feed = spawn_feed(
            state,
            Some(CatalogSource { link, token: None }),
            Duration::from_millis(20),
            shutdown.clone(),
        );
        until("the remote shard to become forwardable", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Forward { .. })
        })
        .await;

        shutdown.cancel();
        let _ = feed.await;
    }

    /// **A control-plane blip must not erase the address book.** Dropping it
    /// would turn every remote publish into a refusal, which looks exactly like
    /// a cluster with no brokers in it.
    #[tokio::test]
    async fn a_failed_catalog_refresh_keeps_the_previous_one() {
        let failing = Arc::new(AtomicBool::new(false));
        let (base_url, server) = nodes_endpoint(Arc::clone(&failing)).await;
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let shutdown = CancellationToken::new();

        let feed = spawn_feed(
            state,
            Some(CatalogSource {
                link: HttpControlPlaneLink::new(
                    reqwest::Client::builder()
                        .no_proxy()
                        .build()
                        .expect("client"),
                    &base_url,
                ),
                token: None,
            }),
            Duration::from_millis(20),
            shutdown.clone(),
        );

        until("the catalog to arrive", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Forward { .. })
        })
        .await;

        // The control plane starts refusing. Several ticks pass.
        failing.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(
            matches!(ingress.dispatch(&key(0)), Dispatch::Forward { .. }),
            "the route was dropped when the catalog refresh failed",
        );

        shutdown.cancel();
        let _ = feed.await;
        server.abort();
    }

    /// **A change the watch reports is acted on at once.** The tick is a
    /// minute here, so only the wake can make the shard servable in time, and
    /// replication is told the routes changed.
    #[tokio::test]
    async fn a_woken_feed_acts_before_its_next_tick() {
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let ownership = Arc::clone(&state.ownership);
        let assignments_changed = Arc::clone(&state.assignments_changed);
        let routes_changed = Arc::clone(&state.routes_changed);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_secs(60),
            shutdown.clone(),
        );

        // The first tick fires at once; let it pass.
        until("the first tick to publish", || {
            !matches!(
                ingress.dispatch(&key(0)),
                Dispatch::Unavailable(Reason::NotAssigned)
            )
        })
        .await;

        // The shard moves here, as a cut-over would.
        ownership
            .write()
            .await
            .apply(&key(0), Some(assignment(0, "broker-a", 2)));
        let replication_woken = routes_changed.notified();
        assignments_changed.notify_one();

        until("the woken feed to serve the shard", || {
            ingress.dispatch(&key(0)) == Dispatch::Local { generation: 2 }
        })
        .await;
        tokio::time::timeout(Duration::from_secs(1), replication_woken)
            .await
            .expect("the feed should wake replication after acting on a change");

        shutdown.cancel();
        let _ = feed.await;
    }

    /// **A wake picks up a known node's new address.** broker-b restarts and
    /// re-registers under the same id on a new port. It is not an unknown node,
    /// so only refreshing on the wake itself gets the new address in before the
    /// next tick, a minute away here.
    #[tokio::test]
    async fn a_wake_refreshes_the_address_of_a_node_that_moved() {
        let port = Arc::new(AtomicU16::new(7002));
        let (base_url, server) = nodes_endpoint_at(
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&port),
            Arc::new(AtomicBool::new(false)),
        )
        .await;
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let ownership = Arc::clone(&state.ownership);
        let assignments_changed = Arc::clone(&state.assignments_changed);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            Some(CatalogSource {
                link: HttpControlPlaneLink::new(
                    reqwest::Client::builder()
                        .no_proxy()
                        .build()
                        .expect("client"),
                    &base_url,
                ),
                token: None,
            }),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        let forwards_to = |port: u16| {
            matches!(
                ingress.dispatch(&key(0)),
                Dispatch::Forward { advertise_addr, .. } if advertise_addr.port() == port
            )
        };
        until("the first tick to learn broker-b's address", || {
            forwards_to(7002)
        })
        .await;

        port.store(7102, Ordering::Release);
        ownership
            .write()
            .await
            .apply(&key(0), Some(assignment(0, "broker-b", 2)));
        assignments_changed.notify_one();

        until("the wake to forward to broker-b's new address", || {
            forwards_to(7102)
        })
        .await;

        shutdown.cancel();
        let _ = feed.await;
        server.abort();
    }

    /// **A control plane that never answers does not hold up a route.** An
    /// election's leader has to be published within seconds, and the catalog
    /// read waits up to `CATALOG_FETCH_TIMEOUT` on a paused control plane. The
    /// woken pass publishes from what it already has.
    #[tokio::test]
    async fn a_hung_catalog_read_does_not_delay_the_routes() {
        let hanging = Arc::new(AtomicBool::new(false));
        let (base_url, server) = nodes_endpoint_at(
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU16::new(7002)),
            Arc::clone(&hanging),
        )
        .await;
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let ownership = Arc::clone(&state.ownership);
        let assignments_changed = Arc::clone(&state.assignments_changed);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            Some(CatalogSource {
                link: HttpControlPlaneLink::new(
                    reqwest::Client::builder()
                        .no_proxy()
                        .build()
                        .expect("client"),
                    &base_url,
                ),
                token: None,
            }),
            Duration::from_secs(60),
            shutdown.clone(),
        );
        until("the first tick to learn broker-b's address", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Forward { .. })
        })
        .await;

        hanging.store(true, Ordering::Release);
        ownership
            .write()
            .await
            .apply(&key(0), Some(assignment(0, "broker-a", 2)));
        assignments_changed.notify_one();

        let served = tokio::time::timeout(Duration::from_secs(1), async {
            while ingress.dispatch(&key(0)) != (Dispatch::Local { generation: 2 }) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            served.is_ok(),
            "the route waited on the catalog read: {:?}",
            ingress.dispatch(&key(0))
        );
        // The address book survives the hung read.
        ownership
            .write()
            .await
            .apply(&key(0), Some(assignment(0, "broker-b", 3)));
        assignments_changed.notify_one();
        until("the shard to forward to broker-b again", || {
            matches!(
                ingress.dispatch(&key(0)),
                Dispatch::Forward { generation: 3, .. }
            )
        })
        .await;

        shutdown.cancel();
        let _ = feed.await;
        server.abort();
    }

    /// After a surrender the lease serves again only once the feed has
    /// published routes from a watch answer that reaches the head the renewal
    /// was accepted at (#1955). The feed's publish is what confirms it.
    #[tokio::test]
    async fn a_published_watch_answer_at_the_renewals_head_confirms_the_lease() {
        let (state, _router) = state(&[assignment(0, "broker-a", 1)]);
        let lease = Arc::new(crate::cluster::lease::LeaseState::new(Duration::from_secs(
            60,
        )));
        state.ingress.fence().bind_lease(Arc::clone(&lease));
        lease.follow_routes();
        let ownership = Arc::clone(&state.ownership);
        let changed = Arc::clone(&state.assignments_changed);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_millis(20),
            shutdown.clone(),
        );

        lease.renew();
        lease.surrender();
        lease.renew_at(lease.now(), Some(5));
        ownership
            .write()
            .await
            .answered(tokio::time::Instant::now(), 4);
        changed.notify_one();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !lease.is_valid_now(),
            "routes short of the renewal's head confirmed it"
        );

        ownership
            .write()
            .await
            .answered(tokio::time::Instant::now(), 5);
        changed.notify_one();
        until("the lease to be confirmed", || lease.is_valid_now()).await;

        shutdown.cancel();
        let _ = feed.await;
    }

    /// Cancellation ends the task rather than leaving it ticking.
    #[tokio::test]
    async fn the_feed_stops_when_cancelled() {
        let (state, _router) = state(&[assignment(0, "broker-a", 1)]);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            no_catalog(),
            Duration::from_millis(20),
            shutdown.clone(),
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), feed)
            .await
            .expect("the feed should stop when cancelled")
            .expect("join");
    }
}

/// A leader in a region this broker has no bridge to is refused with its own
/// reason, one a client can tell apart from a leader that is down; a bridge
/// to that region lets the request through.
#[tokio::test]
async fn a_leader_in_an_unbridged_region_is_refused_by_name() {
    let mut nodes = catalog();
    nodes.get_mut("broker-b").expect("broker-b").region = "eu-west-1".to_string();
    let owned: HashMap<ShardKey, ShardAssignment> = [(key(0), assignment(0, "broker-b", 3))]
        .into_iter()
        .collect();
    let dispatch_with = |regions: RegionRouter<String>| {
        let router = Arc::new(ShardRouter::new("broker-a", "us-west-2", regions));
        router.publish(routing_table_from(&owned, &nodes), &nodes);
        let lifecycle = ShardLifecycle::new("broker-a");
        let ingress = IngressRouter::new(router, Arc::clone(lifecycle.fence()));
        dispatch(Some(&ingress), &key(0))
    };

    let refused = dispatch_with(RegionRouter::new("us-west-2".to_string()));
    assert_eq!(
        refused,
        Dispatch::Unavailable(Reason::RegionNotRoutable {
            region: "eu-west-1".to_string()
        })
    );
    let Dispatch::Unavailable(reason) = refused else {
        unreachable!()
    };
    assert_eq!(reason.wire_name(), "region_not_routable");

    let bridged = dispatch_with(RegionRouter::with_bridges(
        "us-west-2".to_string(),
        [("us-west-2".to_string(), "eu-west-1".to_string())],
    ));
    assert!(matches!(bridged, Dispatch::Forward { .. }), "{bridged:?}");
}

/// **A pending set change is routed as the new set, with the control plane's
/// set kept beside it.** The leader ships to and fences the new followers, and
/// counts the set being left (the assignment's own, the leader included) for
/// its acknowledgements.
#[test]
fn a_pending_set_change_routes_the_new_set_and_keeps_the_one_being_left() {
    use crate::shards::watch::PendingSet;

    let mut pending = assignment(0, "broker-a", 6);
    pending.replicas = vec!["broker-b".to_string(), "broker-c".to_string()];
    pending.pending_set = Some(PendingSet {
        replicas: vec!["broker-b".to_string(), "broker-d".to_string()],
        generation: 6,
    });
    let owned: HashMap<ShardKey, ShardAssignment> =
        HashMap::from_iter([(pending.key.clone(), pending)]);
    let nodes: HashMap<String, NodeRef> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|id| node(&format!("broker-{id}"), 7001, true))
        .map(|n| (n.node_id.clone(), n))
        .collect();

    let table = routing_table_from(&owned, &nodes);

    let route = table
        .get(&to_router_key(&key(0)))
        .expect("the shard is routed");
    let followers: Vec<&str> = route.replicas.iter().map(|n| n.node_id.as_str()).collect();
    assert_eq!(followers, ["broker-b", "broker-d"]);
    let change = route.pending.as_deref().expect("pending");
    assert_eq!(change.generation, 6);
    assert_eq!(change.leaving, ["broker-a", "broker-b", "broker-c"]);
}

/// **A shard is replicated whatever the node catalog says (#1845).** The
/// feed publishes routes before its first catalog read has succeeded. A
/// shard read as unreplicated acknowledges a `Quorum` write on its leader
/// alone, so a follower with no address must still count.
#[tokio::test]
async fn a_shard_whose_followers_have_no_address_is_still_replicated() {
    let mut led = assignment(0, "broker-a", 3);
    led.replicas = vec!["broker-b".to_string(), "broker-c".to_string()];
    let owned: HashMap<ShardKey, ShardAssignment> =
        HashMap::from_iter([(led.key.clone(), led.clone())]);
    let router = Arc::new(ShardRouter::new(
        "broker-a",
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let empty = HashMap::default();
    router.publish(routing_table_from(&owned, &empty), &empty);
    let mut lifecycle = ShardLifecycle::new("broker-a");
    lifecycle.observe(&led.key, Some(&led));
    lifecycle.opened(&led.key, led.generation);
    let ingress = IngressRouter::new(router, Arc::clone(lifecycle.fence()));
    ingress.publish_servable(lifecycle.servable());

    assert!(ingress.replicated(&key(0)));
    let route = ingress.route(&key(0)).expect("routed");
    let followers: Vec<&str> = route.replicas.iter().map(|n| n.node_id.as_str()).collect();
    assert_eq!(followers, ["broker-b", "broker-c"]);
}

/// **The feed names a copy for an early confirmed read only when it is new
/// (#1791):** a move's destination or a follower just added, not the routes
/// a broker starts with, not a copy it already followed, and not a shard's
/// creation, which skips the read.
#[test]
fn only_newly_named_followers_are_read_early() {
    let map = |assignments: Vec<ShardAssignment>| {
        assignments
            .into_iter()
            .map(|assignment| (assignment.key.clone(), assignment))
            .collect::<HashMap<_, _>>()
    };
    let named = |before: &HashMap<_, _>, after: &HashMap<_, _>| {
        let mut shards: Vec<u32> = newly_named(before, after, "broker-b")
            .map(|(key, _)| key.shard)
            .collect();
        shards.sort_unstable();
        shards
    };
    let staged = ShardAssignment {
        successor: Some("broker-b".to_string()),
        replicas: vec!["broker-b".to_string()],
        ..assignment(0, "broker-a", 3)
    };
    let followed = ShardAssignment {
        replicas: vec!["broker-b".to_string()],
        ..assignment(1, "broker-a", 2)
    };
    let created = ShardAssignment {
        replicas: vec!["broker-b".to_string()],
        incarnation: 4,
        member_storage_ids: [("broker-b".to_string(), "00".to_string())].into(),
        ..assignment(2, "broker-a", 1)
    };
    let led = ShardAssignment {
        replicas: vec!["broker-c".to_string()],
        ..assignment(3, "broker-b", 1)
    };
    let joining = ShardAssignment {
        replicas: vec!["broker-c".to_string()],
        pending_set: Some(crate::shards::watch::PendingSet {
            replicas: vec!["broker-b".to_string(), "broker-c".to_string()],
            generation: 6,
        }),
        ..assignment(4, "broker-a", 5)
    };
    let before = map(vec![assignment(0, "broker-a", 2), followed.clone()]);
    let after = map(vec![
        staged.clone(),
        followed.clone(),
        created,
        led,
        joining,
    ]);
    assert_eq!(named(&before, &after), [0, 4]);
    assert!(
        named(&HashMap::default(), &after).is_empty(),
        "the first view"
    );
    // A re-created stream is another log, so its copy is new again.
    let recreated = ShardAssignment {
        incarnation: 9,
        ..followed
    };
    assert_eq!(named(&before, &map(vec![staged, recreated])), [0, 1]);
}

/// **An elected leadership is laid over the control plane's assignment**
/// until the control plane names its generation: the elected leader at its
/// generation, on the set it stood on, with nothing pending or moving. The
/// control plane's own set is kept aside for a later candidate to compare
/// with. One for a shard with no assignment is dropped.
#[test]
fn an_elected_leadership_is_laid_over_the_assignment_until_named() {
    use felix_replication::election::{Elected, Elections};
    use felix_storage::StorageId;
    use felix_storage::disk_log::{Member, Membership};

    let copy = StorageId::from_bytes([7; 16]);
    let elections = Elections::new("broker-b", Arc::new(tokio::sync::Notify::new()));
    let elected = || Elected {
        leader: "broker-b".to_string(),
        generation: 6,
        set: Membership {
            generation: 5,
            members: ["broker-a", "broker-b", "broker-c"]
                .into_iter()
                .map(|node| Member {
                    node: node.to_string(),
                    storage: (node == "broker-a").then_some(copy),
                    learner: false,
                    wiped: false,
                })
                .collect(),
        },
        leaving: None,
        incarnation: 0,
    };
    for shard in [0, 1] {
        elections.record(to_router_key(&key(shard)), elected());
    }
    let mut named = assignment(0, "broker-a", 5);
    named.replicas = vec!["broker-b".to_string(), "broker-c".to_string()];
    named.successor = Some("broker-c".to_string());
    named.fence_storage_ids = [("broker-c".to_string(), "00".to_string())].into();
    let assignments: HashMap<ShardKey, ShardAssignment> = HashMap::from_iter([(key(0), named)]);

    let laid = with_elected(assignments.clone(), &elections);
    let route = &laid[&key(0)];
    assert_eq!(route.leader, "broker-b");
    assert_eq!(route.generation, 6);
    assert_eq!(route.replicas, ["broker-a", "broker-c"]);
    assert_eq!(
        route.member_storage_ids,
        [("broker-a".to_string(), copy.to_hex())].into()
    );
    assert!(route.fence_storage_ids.is_empty());
    assert_eq!(route.successor, None);
    assert_eq!(route.state, "active");
    let recorded = elections
        .recorded(&to_router_key(&key(0)))
        .expect("the control plane's set");
    assert_eq!(recorded.generation, 5);
    assert_eq!(
        recorded
            .members
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["broker-a", "broker-b", "broker-c"]
    );
    assert!(
        elections.get(&to_router_key(&key(1))).is_none(),
        "no assignment"
    );

    // Named broker-a at the elected generation: the standing stays, so its
    // reports can tell the control plane.
    let mut collided = assignments;
    collided.get_mut(&key(0)).expect("assigned").generation = 6;
    let laid = with_elected(collided.clone(), &elections);
    assert_eq!(laid[&key(0)].leader, "broker-b");

    let mut caught_up = collided;
    caught_up.get_mut(&key(0)).expect("assigned").generation = 7;
    let laid = with_elected(caught_up, &elections);
    assert_eq!(laid[&key(0)].leader, "broker-a");
    assert!(elections.is_empty());
}
