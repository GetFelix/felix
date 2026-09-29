//! Ingress dispatch: what a broker does with a request for each ownership state.
use std::collections::HashMap;

use felix_router::{NodeRef, RegionRouter};

use super::*;
use crate::shards::lifecycle::ShardLifecycle;
use crate::shards::watch::ShardAssignment;

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
    let mut seen = std::collections::HashSet::new();
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
        nodes_endpoint_at(failing, Arc::new(AtomicU16::new(7002))).await
    }

    /// As [`nodes_endpoint`], with broker-b's port settable, as a restart that
    /// re-registers on new ports would change it.
    async fn nodes_endpoint_at(
        failing: Arc<AtomicBool>,
        port: Arc<AtomicU16>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().route(
            "/v1/nodes",
            get(move || {
                let failing = Arc::clone(&failing);
                let port = port.load(Ordering::Acquire);
                async move {
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

        let feed = spawn_feed(state, None, Duration::from_millis(20), shutdown.clone());
        until("the shard to be served locally", || {
            matches!(ingress.dispatch(&key(0)), Dispatch::Local { .. })
        })
        .await;

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
                client: reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .expect("client"),
                base_url,
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
                client: reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .expect("client"),
                base_url,
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
        let feed = spawn_feed(state, None, Duration::from_secs(60), shutdown.clone());

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
        let (base_url, server) =
            nodes_endpoint_at(Arc::new(AtomicBool::new(false)), Arc::clone(&port)).await;
        let (state, _router) = state(&[assignment(0, "broker-b", 1)]);
        let ingress = Arc::clone(&state.ingress);
        let ownership = Arc::clone(&state.ownership);
        let assignments_changed = Arc::clone(&state.assignments_changed);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(
            state,
            Some(CatalogSource {
                client: reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .expect("client"),
                base_url,
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

    /// Cancellation ends the task rather than leaving it ticking.
    #[tokio::test]
    async fn the_feed_stops_when_cancelled() {
        let (state, _router) = state(&[assignment(0, "broker-a", 1)]);
        let shutdown = CancellationToken::new();
        let feed = spawn_feed(state, None, Duration::from_millis(20), shutdown.clone());

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
