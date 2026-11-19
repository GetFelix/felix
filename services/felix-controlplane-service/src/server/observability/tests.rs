use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serial_test::serial;
use tokio::sync::oneshot;

use super::*;

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
fn resource_attributes_capture_env() {
    let _g1 = EnvGuard::set("FELIX_SERVICE_INSTANCE_ID", "instance-1");
    let _g2 = EnvGuard::set("K8S_CLUSTER_NAME", "cluster-a");
    let _g3 = EnvGuard::set("K8S_NAMESPACE_NAME", "ns-a");
    let _g4 = EnvGuard::set("K8S_POD_NAME", "pod-a");
    let _g5 = EnvGuard::set("CLOUD_REGION", "region-a");
    let _g6 = EnvGuard::set("DEPLOYMENT_ENVIRONMENT", "staging");

    let attrs = resource_attributes("controlplane");
    let mut found = std::collections::HashMap::new();
    for attr in attrs {
        found.insert(attr.key.as_str().to_string(), attr.value.to_string());
    }

    assert_eq!(found.get("service.name"), Some(&"controlplane".to_string()));
    assert_eq!(
        found.get("service.instance.id"),
        Some(&"instance-1".to_string())
    );
    assert_eq!(
        found.get("k8s.cluster.name"),
        Some(&"cluster-a".to_string())
    );
    assert_eq!(found.get("k8s.namespace.name"), Some(&"ns-a".to_string()));
    assert_eq!(found.get("k8s.pod.name"), Some(&"pod-a".to_string()));
    assert_eq!(found.get("cloud.region"), Some(&"region-a".to_string()));
    assert_eq!(
        found.get("deployment.environment"),
        Some(&"staging".to_string())
    );
}

#[test]
#[serial]
fn resource_attributes_hostname_fallback() {
    let _g1 = EnvGuard::unset("FELIX_SERVICE_INSTANCE_ID");
    let _g2 = EnvGuard::set("HOSTNAME", "host-1");

    let attrs = resource_attributes("controlplane");
    let instance = attrs
        .iter()
        .find(|attr| attr.key.as_str() == "service.instance.id")
        .map(|attr| attr.value.to_string());
    assert_eq!(instance, Some("host-1".to_string()));
}

fn build_test_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(1))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build test client")
}

async fn wait_for_listen(addr: SocketAddr) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("server never became ready at {}", addr));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn spawn_metrics_server(
    handle: PrometheusHandle,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let (addr, shutdown_tx, server_handle, _readiness) =
        spawn_metrics_server_with_readiness(handle, Arc::new(crate::api::readiness::AlwaysReady))
            .await;
    (addr, shutdown_tx, server_handle)
}

/// The metrics server over `store`; the lifecycle flag it returns drains it.
async fn spawn_metrics_server_with_readiness(
    handle: PrometheusHandle,
    store: Arc<dyn crate::api::readiness::HealthProbe>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<std::io::Result<()>>,
    felix_common::lifecycle::Readiness,
) {
    let lifecycle = felix_common::lifecycle::Readiness::ready();
    let readiness = Arc::new(Readiness::with_lifecycle(
        lifecycle.clone(),
        store,
        Duration::from_secs(1),
        Duration::ZERO,
    ));
    let addr: SocketAddr = "127.0.0.1:0".parse().expect("parse addr");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind listener");
    let bound_addr = listener.local_addr().expect("local addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_readiness = readiness.clone();
    let server_handle = tokio::spawn(async move {
        serve_metrics_with_listener(handle, listener, server_readiness, async move {
            let _ = shutdown_rx.await;
        })
        .await
    });
    (bound_addr, shutdown_tx, server_handle, lifecycle)
}

#[test]
#[serial]
fn install_metrics_recorder_is_cached() {
    let handle1 = install_metrics_recorder();
    let handle2 = install_metrics_recorder();
    let _ = (handle1.render(), handle2.render());
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn init_observability_is_idempotent() {
    let handle1 = init_observability("controlplane-test");
    let handle2 = init_observability("controlplane-test");
    let _ = (handle1.render(), handle2.render());
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn serve_metrics_responds() {
    let handle = init_observability("controlplane-metrics-test");
    let (addr, shutdown_tx, server_handle) = spawn_metrics_server(handle).await;
    wait_for_listen(addr).await.expect("server ready");

    let client = build_test_client();
    let url = format!("http://{}/metrics", addr);
    let response = client
        .get(&url)
        .send()
        .await
        .unwrap_or_else(|err| panic!("GET /metrics failed for {}: {}", url, err));
    response.error_for_status().expect("metrics status");

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown");
}

// The point of the readiness flip is that a load balancer can observe it, so the
// assertion that matters is over HTTP, not over the flag in isolation.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn ready_reports_draining_after_readiness_flips() {
    let handle = init_observability("controlplane-readiness-test");
    let (addr, shutdown_tx, server_handle, readiness) =
        spawn_metrics_server_with_readiness(handle, Arc::new(crate::api::readiness::AlwaysReady))
            .await;
    wait_for_listen(addr).await.expect("server ready");

    let client = build_test_client();
    let ready_url = format!("http://{}/ready", addr);
    let live_url = format!("http://{}/live", addr);

    let response = client.get(&ready_url).send().await.expect("GET /ready");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.expect("body"), "ok");

    readiness.begin_draining();

    let response = client.get(&ready_url).send().await.expect("GET /ready");
    assert_eq!(
        response.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "/ready must fail once draining so load balancers stop routing here"
    );
    assert_eq!(response.text().await.expect("body"), "draining");

    // Liveness must stay healthy: the process is draining correctly, and failing
    // /live would make Kubernetes restart it mid-drain.
    let response = client.get(&live_url).send().await.expect("GET /live");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown");
}

struct Unhealthy;

#[async_trait::async_trait]
impl crate::api::readiness::HealthProbe for Unhealthy {
    async fn health_check(&self) -> crate::store::StoreResult<()> {
        Err(crate::store::StoreError::Unexpected(anyhow::anyhow!(
            "down"
        )))
    }
}

// Probes point at this listener's `/ready` (it never asks for a client
// certificate), so it has to fail on a store that does not answer, exactly
// like the API's `/v1/system/ready`.
#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn ready_fails_when_the_store_does_not_answer() {
    let handle = init_observability("controlplane-readiness-store-test");
    let (addr, shutdown_tx, server_handle, _readiness) =
        spawn_metrics_server_with_readiness(handle, Arc::new(Unhealthy)).await;
    wait_for_listen(addr).await.expect("server ready");

    let client = build_test_client();
    let response = client
        .get(format!("http://{addr}/ready"))
        .send()
        .await
        .expect("GET /ready");
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let response = client
        .get(format!("http://{addr}/live"))
        .send()
        .await
        .expect("GET /live");
    assert_eq!(response.status(), reqwest::StatusCode::OK);

    let _ = shutdown_tx.send(());
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle)
        .await
        .expect("server shutdown");
}
