//! Parsing the control plane's node list.
//!
//! The skipping rule is the part worth testing: one broker registered with an
//! address nobody can parse must not cost this broker every other route it
//! knows, because losing the catalog means refusing every remote publish.
use super::*;

fn response(json: serde_json::Value) -> NodeListResponse {
    serde_json::from_value(json).expect("decode")
}

fn node(node_id: &str, addr: &str, eligible: bool) -> serde_json::Value {
    serde_json::json!({
        "node": {
            "node_id": node_id,
            "spec": { "advertise_addr": addr, "region": "us-west-2" },
        },
        "placement": { "eligible": eligible },
    })
}

#[test]
fn a_registered_node_becomes_a_routable_entry() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "10.0.0.4:7000", true)],
    })));

    let entry = catalog.nodes.get("broker-a").expect("broker-a");
    assert_eq!(entry.node_id, "broker-a");
    assert_eq!(entry.advertise_addr.to_string(), "10.0.0.4:7000");
    assert_eq!(entry.region, "us-west-2");
    assert!(entry.live);
}

/// With no `routable`, from a control plane that predates it, `eligible` is
/// taken as-is. Re-deriving liveness from the lifecycle alone would keep
/// forwarding to a node whose heartbeat has lapsed but whose expiry sweep has
/// not run yet.
#[test]
fn placement_eligibility_decides_liveness() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "10.0.0.4:7000", false)],
    })));
    assert!(!catalog.nodes.get("broker-a").expect("broker-a").live);
}

/// A draining broker is not eligible for placement but still serves the shards
/// it has not handed off; the control plane says so with `routable`, and that
/// decides liveness when present.
#[test]
fn a_routable_draining_node_is_live() {
    let mut draining = node("broker-a", "10.0.0.4:7000", false);
    draining["placement"]["routable"] = serde_json::json!(true);
    let mut lapsed = node("broker-b", "10.0.0.5:7000", false);
    lapsed["placement"]["routable"] = serde_json::json!(false);
    let catalog = into_catalog(response(serde_json::json!({
        "items": [draining, lapsed],
    })));
    assert!(catalog.nodes.get("broker-a").expect("broker-a").live);
    assert!(!catalog.nodes.get("broker-b").expect("broker-b").live);
}

/// One unparseable address costs that node and nothing else.
#[test]
fn a_node_with_an_unusable_address_is_skipped_and_the_rest_survive() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node("broker-a", "10.0.0.4:7000", true),
            node("broker-b", "not-an-address", true),
            node("broker-c", "10.0.0.6:7000", true),
        ],
    })));

    assert!(catalog.nodes.contains_key("broker-a"));
    assert!(catalog.nodes.contains_key("broker-c"));
    assert!(
        !catalog.nodes.contains_key("broker-b"),
        "an address that does not parse cannot be forwarded to",
    );
    assert_eq!(
        catalog.nodes.len(),
        2,
        "the malformed entry must not take others with it"
    );
}

/// A hostname is not an address. The catalog holds `SocketAddr`, and resolving
/// names is not something the publish path can afford to do.
#[test]
fn a_hostname_is_not_accepted_as_an_address() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "broker-a.internal:7000", true)],
    })));
    assert!(catalog.nodes.is_empty());
}

#[test]
fn an_empty_catalog_decodes_to_an_empty_map() {
    let catalog = into_catalog(response(serde_json::json!({ "items": [] })));
    assert!(catalog.nodes.is_empty());
}

/// Two registrations for one id cannot both be routable; the map holds one.
#[test]
fn a_repeated_node_id_yields_one_entry() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node("broker-a", "10.0.0.4:7000", true),
            node("broker-a", "10.0.0.9:7000", true),
        ],
    })));
    assert_eq!(catalog.nodes.len(), 1);
}

/// IPv6 is a valid advertised address and must not be dropped as malformed.
#[test]
fn an_ipv6_address_is_routable() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "[::1]:7000", true)],
    })));
    assert!(catalog.nodes.contains_key("broker-a"));
}

/// The request half, against a real HTTP server.
///
/// A stub client would not exercise the status check or the bearer header, and
/// those are the two ways this call fails in a deployment: a control plane that
/// is up but refusing, and one that answers something other than a node list.
mod over_http {
    use super::*;
    use axum::Router;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;

    async fn serve(app: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app.into_make_service()).await;
        });
        (format!("http://{addr}"), task)
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .no_proxy()
            .build()
            .expect("client")
    }

    #[tokio::test]
    async fn a_node_list_is_fetched_and_parsed() {
        let body = serde_json::json!({ "items": [node("broker-a", "10.0.0.4:7000", true)] });
        let app = Router::new().route(
            "/v1/nodes",
            get(move || {
                let body = body.clone();
                async move { axum::Json(body) }
            }),
        );
        let (base, task) = serve(app).await;

        let catalog = fetch(&client(), &base, Some("a-token"))
            .await
            .expect("fetch");
        assert!(catalog.nodes.contains_key("broker-a"));

        task.abort();
    }

    /// Every page is read. Stopping at the first would drop brokers past it
    /// from the catalog, and routes to them with it.
    #[tokio::test]
    async fn every_page_of_the_node_list_is_fetched() {
        let app = Router::new().route(
            "/v1/nodes",
            get(
                |axum::extract::Query(query): axum::extract::Query<
                    std::collections::HashMap<String, String>,
                >| async move {
                    axum::Json(match query.get("cursor").map(String::as_str) {
                        None => serde_json::json!({
                            "items": [node("broker-a", "10.0.0.4:7000", true)],
                            "next_cursor": "after-a",
                        }),
                        Some("after-a") => serde_json::json!({
                            "items": [node("broker-b", "10.0.0.5:7000", true)],
                        }),
                        Some(other) => panic!("unexpected cursor {other}"),
                    })
                },
            ),
        );
        let (base, task) = serve(app).await;

        let catalog = fetch(&client(), &base, None).await.expect("fetch");
        assert!(catalog.nodes.contains_key("broker-a"));
        assert!(
            catalog.nodes.contains_key("broker-b"),
            "the second page was not read"
        );

        task.abort();
    }

    #[tokio::test]
    async fn a_cursor_that_does_not_advance_is_an_error() {
        let app = Router::new().route(
            "/v1/nodes",
            get(|| async { axum::Json(serde_json::json!({ "items": [], "next_cursor": "same" })) }),
        );
        let (base, task) = serve(app).await;

        assert!(fetch(&client(), &base, None).await.is_err());

        task.abort();
    }

    /// The credential is sent. Without it the control plane answers 403 and the
    /// broker silently loses every route it could forward to.
    #[tokio::test]
    async fn the_bearer_token_is_sent() {
        let app = Router::new().route(
            "/v1/nodes",
            get(|headers: HeaderMap| async move {
                match headers.get(axum::http::header::AUTHORIZATION) {
                    Some(value) if value == "Bearer a-token" => {
                        Ok(axum::Json(serde_json::json!({ "items": [] })))
                    }
                    _ => Err(StatusCode::FORBIDDEN),
                }
            }),
        );
        let (base, task) = serve(app).await;

        fetch(&client(), &base, Some("a-token"))
            .await
            .expect("the token should have been accepted");

        task.abort();
    }

    /// A refusal is an error, not an empty catalog. Treating 403 as "no nodes"
    /// would drop every route and look identical to a cluster with no brokers.
    #[tokio::test]
    async fn a_refusal_is_an_error_rather_than_an_empty_catalog() {
        let app = Router::new().route(
            "/v1/nodes",
            get(|| async { (StatusCode::FORBIDDEN, "missing node.view:cluster:*") }),
        );
        let (base, task) = serve(app).await;

        let err = fetch(&client(), &base, None)
            .await
            .expect_err("403 must not read as an empty catalog");
        let message = err.to_string();
        assert!(message.contains("403"), "{message}");
        assert!(
            message.contains("missing node.view"),
            "the body says why, and it should survive: {message}",
        );

        task.abort();
    }

    #[tokio::test]
    async fn a_body_that_is_not_a_node_list_is_an_error() {
        let app = Router::new().route("/v1/nodes", get(|| async { "not json" }));
        let (base, task) = serve(app).await;

        assert!(fetch(&client(), &base, None).await.is_err());

        task.abort();
    }

    /// Nothing listening at all, which is a control plane that is down.
    #[tokio::test]
    async fn an_unreachable_control_plane_is_an_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        drop(listener);

        assert!(
            fetch(&client(), &format!("http://{addr}"), None)
                .await
                .is_err()
        );
    }
}

/// A node as registered by a broker that also said where clients reach it.
fn node_with_client_addr(
    node_id: &str,
    addr: &str,
    client_addr: &str,
    eligible: bool,
) -> serde_json::Value {
    serde_json::json!({
        "node": {
            "node_id": node_id,
            "spec": {
                "advertise_addr": addr,
                "client_addr": client_addr,
                "region": "us-west-2",
            },
        },
        "placement": { "eligible": eligible },
    })
}

/// **A client is told the client address, never the internal one.** They are
/// different listeners, and the internal one would refuse an application.
#[test]
fn a_client_endpoint_is_the_client_address() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node_with_client_addr(
            "broker-a",
            "10.0.0.4:7000",
            "10.0.0.4:5000",
            true,
        )],
    })));

    assert_eq!(catalog.client_endpoints.len(), 1);
    assert_eq!(catalog.client_endpoints[0].node_id, "broker-a");
    assert_eq!(catalog.client_endpoints[0].addr, "10.0.0.4:5000");
}

/// **A broker that advertises no client address is not offered to clients.**
/// It is still perfectly routable for forwarding: the two are separate answers
/// from one listing, and only one of them is missing.
#[test]
fn a_broker_without_a_client_address_is_still_routable() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node("broker-a", "10.0.0.4:7000", true)],
    })));

    assert!(catalog.nodes.contains_key("broker-a"));
    assert!(
        catalog.client_endpoints.is_empty(),
        "a broker that said nothing about clients must not be handed to one",
    );
}

/// **A broker the cluster will not place work on is not offered to clients.**
/// Sending an application to a broker that is down is worse than sending it
/// nowhere: it has other addresses to try, and this one wastes an attempt.
#[test]
fn an_ineligible_broker_is_not_offered_to_clients() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [node_with_client_addr(
            "broker-a",
            "10.0.0.4:7000",
            "10.0.0.4:5000",
            false,
        )],
    })));

    assert!(catalog.client_endpoints.is_empty());
}

/// An unparseable client address is skipped, on the same reasoning as an
/// unparseable advertised one: one bad registration must not cost the rest.
#[test]
fn an_unparseable_client_address_is_skipped() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node_with_client_addr("broker-a", "10.0.0.4:7000", "not-an-address", true),
            node_with_client_addr("broker-b", "10.0.0.5:7000", "10.0.0.5:5000", true),
        ],
    })));

    assert_eq!(catalog.client_endpoints.len(), 1);
    assert_eq!(catalog.client_endpoints[0].node_id, "broker-b");
}

/// The answer is ordered, so a client that compares two refreshes sees a change
/// only when something actually changed.
#[test]
fn client_endpoints_come_back_in_a_stable_order() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node_with_client_addr("broker-c", "10.0.0.6:7000", "10.0.0.6:5000", true),
            node_with_client_addr("broker-a", "10.0.0.4:7000", "10.0.0.4:5000", true),
            node_with_client_addr("broker-b", "10.0.0.5:7000", "10.0.0.5:5000", true),
        ],
    })));

    let ids: Vec<&str> = catalog
        .client_endpoints
        .iter()
        .map(|endpoint| endpoint.node_id.as_str())
        .collect();
    assert_eq!(ids, vec!["broker-a", "broker-b", "broker-c"]);
}

/// **A redirect to a draining broker carries its address.** A draining broker
/// is no longer offered to new clients, but it still leads the shards it has
/// not handed off, and a request for one of them that lands elsewhere is
/// redirected there. Without the address a client has nowhere to follow to.
#[test]
fn a_redirect_to_a_draining_broker_carries_its_client_address() {
    let mut draining = node_with_client_addr("broker-a", "10.0.0.4:7000", "10.0.0.4:5000", false);
    draining["placement"]["routable"] = serde_json::json!(true);
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            draining,
            node_with_client_addr("broker-b", "10.0.0.5:7000", "10.0.0.5:5000", true),
        ],
    })));
    let endpoints = crate::cluster::client_endpoints::ClientEndpoints::new();
    endpoints.refresh(&catalog);

    let answer = crate::serving::quic::handlers::redirect::redirect_from(
        crate::shards::routing::Dispatch::Forward {
            node_id: "broker-a".to_string(),
            advertise_addr: "10.0.0.4:7000".parse().expect("addr"),
            generation: 3,
        },
        Some(&endpoints),
        "orders",
        felix_wire::FEATURE_REDIRECT,
    );
    match answer {
        Some(felix_wire::Message::NotLeader { node_id, addr, .. }) => {
            assert_eq!(node_id, "broker-a");
            assert_eq!(addr.as_deref(), Some("10.0.0.4:5000"));
        }
        other => panic!("expected a redirect, got {other:?}"),
    }
    let offered = endpoints.snapshot();
    let offered: Vec<&str> = offered
        .iter()
        .map(|endpoint| endpoint.node_id.as_str())
        .collect();
    assert_eq!(
        offered,
        vec!["broker-b"],
        "a draining broker must still not be offered to new clients",
    );
}

/// A node as registered by a broker running the Kafka listener.
fn node_with_kafka_addr(node_id: &str, kafka_addr: &str, eligible: bool) -> serde_json::Value {
    serde_json::json!({
        "node": {
            "node_id": node_id,
            "spec": {
                "advertise_addr": "10.0.0.4:7000",
                "kafka_addr": kafka_addr,
                "region": "us-west-2",
            },
        },
        "placement": { "eligible": eligible },
    })
}

/// Kafka addresses are usually hostnames, so unlike client addresses they are
/// kept without being parsed as socket addresses, and come back sorted.
#[test]
fn kafka_endpoints_keep_hostnames_in_node_order() {
    let catalog = into_catalog(response(serde_json::json!({
        "items": [
            node_with_kafka_addr("broker-b", "10.0.0.5:9092", true),
            node_with_kafka_addr("broker-a", "host.docker.internal:9092", true),
            node("broker-c", "10.0.0.6:7000", true),
        ],
    })));

    let kafka: Vec<(&str, &str)> = catalog
        .kafka_endpoints
        .iter()
        .map(|endpoint| (endpoint.node_id.as_str(), endpoint.addr.as_str()))
        .collect();
    assert_eq!(
        kafka,
        vec![
            ("broker-a", "host.docker.internal:9092"),
            ("broker-b", "10.0.0.5:9092"),
        ],
    );
    assert!(
        catalog.client_endpoints.is_empty(),
        "a Kafka address must not be offered to Felix clients",
    );
}

/// A draining broker still leads what it has not handed off, so a Kafka client
/// must be able to reach it; a broker that is not routable at all must not be
/// offered.
#[test]
fn kafka_endpoints_follow_routability_not_eligibility() {
    let mut draining = node_with_kafka_addr("broker-a", "broker-a.internal:9092", false);
    draining["placement"]["routable"] = serde_json::json!(true);
    let down = node_with_kafka_addr("broker-b", "broker-b.internal:9092", false);
    let catalog = into_catalog(response(serde_json::json!({
        "items": [draining, down],
    })));

    assert_eq!(catalog.kafka_endpoints.len(), 1);
    assert_eq!(catalog.kafka_endpoints[0].node_id, "broker-a");
}
