//! Observability wiring for the control-plane service.
//!
//! Initializes tracing, OpenTelemetry propagation, and Prometheus metrics
//! endpoints with sensible defaults for both local and production usage.
//!
//! Initialization is guarded by `OnceLock` to keep startup idempotent in tests.
use std::future::Future;
use std::net::SocketAddr;
use std::sync::OnceLock;

use felix_common::lifecycle::Readiness;
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_exporter_prometheus::PrometheusHandle;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static OBS_INIT: OnceLock<()> = OnceLock::new();

pub(crate) fn init_observability(service_name: &str) -> PrometheusHandle {
    OBS_INIT.get_or_init(|| {
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let provider = build_tracer_provider(service_name);
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        // Unit tests in this crate install this subscriber for the whole
        // binary; the test writer lets libtest capture every test's logs.
        #[cfg(test)]
        let fmt_layer = tracing_subscriber::fmt::layer().with_test_writer();
        #[cfg(not(test))]
        let fmt_layer = tracing_subscriber::fmt::layer();
        let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);
        if let Some(provider) = provider {
            let tracer = provider.tracer(service_name.to_string());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            let _ = registry.with(otel_layer).try_init();
        } else {
            let _ = registry.try_init();
        }
    });

    install_metrics_recorder()
}

/// Whether an OTLP endpoint was configured. Without one the exporter would
/// default to `localhost:4317` and fail every batch where no collector runs,
/// so tracing export stays off unless asked for.
fn otlp_endpoint_configured() -> bool {
    [
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    .iter()
    .any(|key| std::env::var(key).is_ok_and(|value| !value.trim().is_empty()))
}

fn build_tracer_provider(
    service_name: &str,
) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    if !otlp_endpoint_configured() {
        return None;
    }
    let resource = Resource::builder_empty()
        .with_attributes(resource_attributes(service_name))
        .build();
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .build()
        .ok()?;
    Some(
        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    )
}

fn resource_attributes(service_name: &str) -> Vec<KeyValue> {
    let mut attrs = vec![KeyValue::new("service.name", service_name.to_string())];
    if let Ok(value) =
        std::env::var("FELIX_SERVICE_INSTANCE_ID").or_else(|_| std::env::var("HOSTNAME"))
    {
        attrs.push(KeyValue::new("service.instance.id", value));
    }
    if let Ok(value) = std::env::var("K8S_CLUSTER_NAME") {
        attrs.push(KeyValue::new("k8s.cluster.name", value));
    }
    if let Ok(value) = std::env::var("K8S_NAMESPACE_NAME") {
        attrs.push(KeyValue::new("k8s.namespace.name", value));
    }
    if let Ok(value) = std::env::var("K8S_POD_NAME") {
        attrs.push(KeyValue::new("k8s.pod.name", value));
    }
    if let Ok(value) = std::env::var("CLOUD_REGION") {
        attrs.push(KeyValue::new("cloud.region", value));
    }
    if let Ok(value) = std::env::var("DEPLOYMENT_ENVIRONMENT") {
        attrs.push(KeyValue::new("deployment.environment", value));
    }
    attrs
}

pub(crate) async fn serve_metrics<F>(
    handle: PrometheusHandle,
    addr: SocketAddr,
    readiness: Readiness,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_metrics_with_shutdown(handle, addr, readiness, shutdown).await
}

async fn serve_metrics_with_shutdown<F>(
    handle: PrometheusHandle,
    addr: SocketAddr,
    readiness: Readiness,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr).await?;
    serve_metrics_with_listener(handle, listener, readiness, shutdown).await
}

async fn serve_metrics_with_listener<F>(
    handle: PrometheusHandle,
    listener: tokio::net::TcpListener,
    readiness: Readiness,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(
        listener,
        health_router(handle, readiness).into_make_service(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

/// Build the metrics/health router.
///
/// `/live` stays "ok" for the whole process lifetime: during a drain the process is
/// alive and still serving, and reporting otherwise would make Kubernetes restart a
/// pod that is shutting down correctly. `/ready` is the one that flips, which is what
/// removes the instance from load-balancer rotation.
fn health_router(handle: PrometheusHandle, readiness: Readiness) -> axum::Router {
    axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { handle.render() }),
        )
        .route("/live", axum::routing::get(|| async { "ok" }))
        .route(
            "/ready",
            axum::routing::get(move || async move {
                if readiness.is_ready() {
                    (axum::http::StatusCode::OK, "ok")
                } else {
                    (axum::http::StatusCode::SERVICE_UNAVAILABLE, "draining")
                }
            }),
        )
}

fn install_metrics_recorder() -> PrometheusHandle {
    if let Some(handle) = METRICS_HANDLE.get() {
        return handle.clone();
    }
    use crate::cluster::placement;
    use metrics_exporter_prometheus::Matcher;
    // Buckets only where a histogram is wanted; everything else keeps the
    // exporter's default summary.
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(placement::SHARD_MOVE_DURATION_SECONDS.to_string()),
            placement::MOVE_DURATION_BUCKETS,
        )
        .and_then(|builder| {
            builder.set_buckets_for_metric(
                Matcher::Full(placement::SHARD_MOVE_FENCE_SECONDS.to_string()),
                placement::MOVE_FENCE_BUCKETS,
            )
        })
        .expect("histogram buckets are non-empty")
        .install_recorder()
        .expect("install metrics recorder");
    let _ = METRICS_HANDLE.set(handle.clone());
    handle
}

#[cfg(test)]
mod tests;
