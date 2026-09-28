//! This module sets up observability for the broker service, including tracing and metrics.
//! It configures a tracing subscriber with optional OpenTelemetry (OTLP) integration for distributed tracing.
//! OTLP export is on only when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces-specific
//! variant) is set; otherwise tracing is local logging only.
//! It installs a Prometheus metrics recorder and provides an HTTP server exposing `/metrics`, `/live`, and `/ready` endpoints.
//! Metrics serving is asynchronous and uses `axum` to handle requests.
//! In tests, metrics recorder initialization is cached to avoid conflicts, and subscriber initialization is adapted accordingly.

pub(crate) mod tenants;
pub mod timings;

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

#[cfg(test)]
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
static OBS_INIT: OnceLock<()> = OnceLock::new();

/// Initializes observability for the service.
///
/// Sets up the global OpenTelemetry text map propagator (W3C Trace Context),
/// builds an OTLP tracer provider when an endpoint is configured,
/// configures the tracing subscriber with environment filtering and formatting,
/// and installs a Prometheus metrics recorder.
///
/// Returns a `PrometheusHandle` for serving metrics.
///
/// In tests, metrics recorder is cached to avoid multiple installations.
pub(crate) fn init_observability(service_name: &str) -> PrometheusHandle {
    OBS_INIT.get_or_init(|| {
        // Set global propagator for trace context propagation across service boundaries.
        global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );

        let provider = build_tracer_provider(service_name);

        // Use environment variable for log filtering; default to "info" if unset or invalid.
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let fmt_layer = tracing_subscriber::fmt::layer();
        let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

        if let Some(provider) = provider {
            // If OTLP tracer provider is available, create a tracer and add OTLP layer.
            let tracer = provider.tracer(service_name.to_string());
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            init_subscriber(registry.with(otel_layer));
        } else {
            // Fallback to local tracing without OTLP.
            init_subscriber(registry);
        }
    });

    // Install Prometheus metrics recorder.
    install_metrics_recorder()
}

/// Serves Prometheus metrics and health endpoints on the given socket address.
///
/// Starts an asynchronous HTTP server exposing:
/// - `/metrics`: Prometheus metrics endpoint.
/// - `/live`: liveness probe returning "ok".
/// - `/ready`: readiness probe, gated on `readiness`.
/// - `/replication/halted`: replicas replication has stopped for.
///
/// Runs until `shutdown` resolves, then stops accepting new requests and lets
/// in-flight ones finish. Returns an I/O error if binding or serving fails.
pub(crate) async fn serve_metrics<F>(
    handle: PrometheusHandle,
    addr: SocketAddr,
    readiness: Readiness,
    halted: std::sync::Arc<felix_replication::halted::HaltedReplicas>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        health_router(handle, readiness, halted).into_make_service(),
    )
    .with_graceful_shutdown(shutdown)
    .await
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

/// Builds an OpenTelemetry tracer provider with OTLP exporter for the given service.
///
/// Attaches resource attributes describing the service and environment.
/// Uses Tokio runtime for batch processing of spans.
/// Returns `None` when no OTLP endpoint is configured or the exporter cannot be built.
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
    // Install batch span processor; failure returns None.
    Some(
        opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    )
}

/// Collects resource attributes for the tracer based on environment variables.
///
/// Includes service name and optional attributes like instance ID, Kubernetes cluster info,
/// cloud region, and deployment environment.
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

/// Build the metrics/health router.
///
/// `/live` stays "ok" for the whole process lifetime: during a drain the process is
/// alive and working, and reporting otherwise would make Kubernetes restart a pod
/// that is shutting down correctly. `/ready` is the one that flips, which is what
/// removes the instance from load-balancer rotation.
fn health_router(
    handle: PrometheusHandle,
    readiness: Readiness,
    halted: std::sync::Arc<felix_replication::halted::HaltedReplicas>,
) -> axum::Router {
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
        // Read-only, and deliberately so: this listener has no authentication
        // (#125, #126), so it may carry things worth knowing and nothing worth
        // doing. Discarding a replica's log is the obvious next step from here
        // and does not belong on an unauthenticated port.
        .route(
            "/replication/halted",
            axum::routing::get(move || async move { axum::Json(halted.snapshot()) }),
        )
}

/// Installs the Prometheus metrics recorder globally.
///
/// In tests, reuses a cached recorder handle to avoid conflicts with multiple installs.
/// Outside tests, installs a new recorder each call.
///
/// Panics if installation fails (should not happen under normal conditions).
fn install_metrics_recorder() -> PrometheusHandle {
    #[cfg(test)]
    {
        // Return cached handle if already installed in tests.
        if let Some(handle) = METRICS_HANDLE.get() {
            return handle.clone();
        }
        let handle = builder()
            .install_recorder()
            .expect("install metrics recorder");
        let _ = METRICS_HANDLE.set(handle.clone());
        handle
    }
    #[cfg(not(test))]
    {
        builder()
            .install_recorder()
            .expect("install metrics recorder")
    }
}

/// Buckets only where a histogram is wanted; everything else keeps the
/// exporter's default summary.
fn builder() -> PrometheusBuilder {
    use crate::shards::lifecycle::metrics as shard;
    use crate::shards::routing::hold::metrics as hold;
    use metrics_exporter_prometheus::Matcher;
    PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(shard::MOVE_SECONDS.to_string()),
            shard::MOVE_BUCKETS,
        )
        .and_then(|builder| {
            builder.set_buckets_for_metric(
                Matcher::Full(shard::SWITCHOVER_SECONDS.to_string()),
                shard::SWITCHOVER_BUCKETS,
            )
        })
        .and_then(|builder| {
            builder.set_buckets_for_metric(
                Matcher::Full(hold::HOLD_SECONDS.to_string()),
                hold::HOLD_BUCKETS,
            )
        })
        .expect("histogram buckets are non-empty")
}

/// Initializes the tracing subscriber.
///
/// In tests, uses `try_init` to avoid panics if the subscriber is already set.
/// In non-test builds, uses `init` which panics on multiple initializations.
fn init_subscriber<S>(subscriber: S)
where
    S: tracing::Subscriber + Send + Sync + 'static,
{
    #[cfg(test)]
    {
        let _ = subscriber.try_init();
    }
    #[cfg(not(test))]
    {
        subscriber.init();
    }
}

#[cfg(test)]
mod tests;
