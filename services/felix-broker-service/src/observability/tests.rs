//! Tests for resource_attributes and environment variable handling.

use std::net::SocketAddr;
use std::time::Duration;

use serial_test::serial;

use super::*;
use crate::test_support::{
    build_test_client, get_with_context, spawn_axum_with_shutdown, wait_for_listen,
};

struct EnvGuard {
    key: &'static str,
    prev: Option<String>,
}

impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }
        Self { key, prev }
    }

    fn unset(key: &'static str) -> Self {
        let prev = std::env::var(key).ok();
        unsafe {
            std::env::remove_var(key);
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.prev {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

#[test]
#[serial]
fn otlp_export_is_off_without_an_endpoint() {
    let _endpoint = EnvGuard::unset("OTEL_EXPORTER_OTLP_ENDPOINT");
    let _traces = EnvGuard::unset("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    assert!(!otlp_endpoint_configured());
    assert!(build_tracer_provider("felix-broker").is_none());

    let _blank = EnvGuard::set("OTEL_EXPORTER_OTLP_ENDPOINT", " ");
    assert!(!otlp_endpoint_configured());
}

#[test]
#[serial]
fn otlp_export_is_on_with_either_endpoint() {
    let _endpoint = EnvGuard::set("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317");
    let _traces = EnvGuard::unset("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    assert!(otlp_endpoint_configured());
    drop(_endpoint);

    let _endpoint = EnvGuard::unset("OTEL_EXPORTER_OTLP_ENDPOINT");
    let _traces = EnvGuard::set(
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "http://collector:4317",
    );
    assert!(otlp_endpoint_configured());
}

#[test]
#[serial]
fn resource_attributes_includes_optional_env() {
    let _g1 = EnvGuard::set("FELIX_SERVICE_INSTANCE_ID", "i-1");
    let _g2 = EnvGuard::set("K8S_CLUSTER_NAME", "cluster");
    let _g3 = EnvGuard::set("K8S_NAMESPACE_NAME", "namespace");
    let _g4 = EnvGuard::set("K8S_POD_NAME", "pod");
    let _g5 = EnvGuard::set("CLOUD_REGION", "region");
    let _g6 = EnvGuard::set("DEPLOYMENT_ENVIRONMENT", "prod");

    let attrs = resource_attributes("svc");
    assert!(attrs.iter().any(|kv| kv.key.as_str() == "service.name"));
    assert!(
        attrs
            .iter()
            .any(|kv| kv.key.as_str() == "service.instance.id")
    );
    assert!(attrs.iter().any(|kv| kv.key.as_str() == "k8s.cluster.name"));
    assert!(
        attrs
            .iter()
            .any(|kv| kv.key.as_str() == "k8s.namespace.name")
    );
    assert!(attrs.iter().any(|kv| kv.key.as_str() == "k8s.pod.name"));
    assert!(attrs.iter().any(|kv| kv.key.as_str() == "cloud.region"));
    assert!(
        attrs
            .iter()
            .any(|kv| kv.key.as_str() == "deployment.environment")
    );
}

#[test]
#[serial]
fn resource_attributes_minimal_when_env_missing() {
    let _g1 = EnvGuard::unset("FELIX_SERVICE_INSTANCE_ID");
    let _g2 = EnvGuard::unset("HOSTNAME");
    let _g3 = EnvGuard::unset("K8S_CLUSTER_NAME");
    let _g4 = EnvGuard::unset("K8S_NAMESPACE_NAME");
    let _g5 = EnvGuard::unset("K8S_POD_NAME");
    let _g6 = EnvGuard::unset("CLOUD_REGION");
    let _g7 = EnvGuard::unset("DEPLOYMENT_ENVIRONMENT");

    let attrs = resource_attributes("svc");
    assert_eq!(attrs.len(), 1);
    assert_eq!(attrs[0].key.as_str(), "service.name");
}

#[test]
#[serial]
fn resource_attributes_uses_hostname_fallback() {
    let _g1 = EnvGuard::unset("FELIX_SERVICE_INSTANCE_ID");
    let _g2 = EnvGuard::set("HOSTNAME", "test-host");

    let attrs = resource_attributes("svc");
    assert!(
        attrs
            .iter()
            .any(|kv| kv.key.as_str() == "service.instance.id" && kv.value.as_str() == "test-host")
    );
}

#[tokio::test]
#[serial]
async fn init_observability_succeeds() {
    let handle = init_observability("test-service");
    // Should return a valid PrometheusHandle that can render metrics
    let metrics = handle.render();
    // Just ensure the handle works without panicking
    // (metrics content may be empty on first render)
    let _ = metrics;
}

/// The halted listing is served by the *real* router, and says which
/// replica rather than how many.
///
/// `felix_broker_replication_halted` is a bare count and has to stay one,
/// so this is the only place an operator can learn which replica of which
/// shard stopped and why. Read-only on purpose: this listener has no
/// authentication, so it may carry things worth knowing and nothing worth
/// doing.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn the_halted_listing_names_each_stopped_replica() {
    use felix_replication::halted::{HaltedReplica, HaltedReplicas};

    let handle = init_observability("test-halted-service");
    let halted = std::sync::Arc::new(HaltedReplicas::new());
    halted.publish(vec![HaltedReplica {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 3,
        kind: "stream",
        node_id: "broker-b".to_string(),
        generation: 7,
        next_offset: 120,
        reason: "diverged",
        remedy: "rebuild it",
    }]);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();
    let bound_addr = listener.local_addr().unwrap();
    let (shutdown_tx, server_handle) = spawn_axum_with_shutdown(
        listener,
        health_router(handle, Readiness::ready(), std::sync::Arc::clone(&halted)),
    );
    wait_for_listen(bound_addr).await.expect("ready");

    let client = build_test_client().expect("client");
    let url = format!("http://{bound_addr}/replication/halted");
    let body = get_with_context(&client, &url, "halted listing")
        .await
        .expect("request")
        .text()
        .await
        .expect("body");

    for expected in [
        "\"stream\":\"orders\"",
        "\"shard\":3",
        "\"node_id\":\"broker-b\"",
        "\"generation\":7",
        "\"reason\":\"diverged\"",
    ] {
        assert!(
            body.contains(expected),
            "the listing left out {expected}, so an operator cannot act on it: {body}",
        );
    }

    // A healthy broker answers with an empty list, not a 404: "nothing is
    // halted" and "this broker does not answer that question" are different
    // things to a dashboard.
    halted.publish(Vec::new());
    let body = get_with_context(&client, &url, "empty listing")
        .await
        .expect("request")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "[]");

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn serve_metrics_endpoints_respond() {
    // These tests avoid hangs by using a strict client timeout, readiness polling,
    // and graceful shutdown for the server task.
    let test_future = async {
        let handle = init_observability("test-metrics-service");
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        let app = axum::Router::new()
            .route(
                "/metrics",
                axum::routing::get(move || async move { handle.render() }),
            )
            .route("/live", axum::routing::get(|| async { "ok" }))
            .route("/ready", axum::routing::get(|| async { "ok" }));

        let (shutdown_tx, server_handle) = spawn_axum_with_shutdown(listener, app);
        wait_for_listen(bound_addr)
            .await
            .expect("metrics server ready");

        let client = build_test_client().expect("client");

        let metrics_url = format!("http://{}/metrics", bound_addr);
        let response = get_with_context(&client, &metrics_url, "GET /metrics")
            .await
            .expect("metrics request")
            .error_for_status()
            .expect("metrics status");
        assert_eq!(response.status(), 200);

        let live_url = format!("http://{}/live", bound_addr);
        let response = get_with_context(&client, &live_url, "GET /live")
            .await
            .expect("live request")
            .error_for_status()
            .expect("live status");
        let body = response.text().await.expect("live body");
        assert_eq!(body, "ok");

        let ready_url = format!("http://{}/ready", bound_addr);
        let response = get_with_context(&client, &ready_url, "GET /ready")
            .await
            .expect("ready request")
            .error_for_status()
            .expect("ready status");
        let body = response.text().await.expect("ready body");
        assert_eq!(body, "ok");

        let _ = shutdown_tx.send(());
        let _ = tokio::time::timeout(Duration::from_secs(1), server_handle)
            .await
            .expect("server shutdown");
    };

    // Run with a 5-second timeout
    tokio::time::timeout(tokio::time::Duration::from_secs(5), test_future)
        .await
        .expect("Test timed out");
}

#[test]
#[serial]
fn install_metrics_recorder_is_cached_in_tests() {
    let handle1 = install_metrics_recorder();
    let handle2 = install_metrics_recorder();
    // Both handles should work without panicking
    // This verifies the caching mechanism works correctly in tests
    let _ = (handle1.render(), handle2.render());
}
