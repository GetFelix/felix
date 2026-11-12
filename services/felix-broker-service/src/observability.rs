//! This module sets up observability for the broker service, including tracing and metrics.
//! It configures a tracing subscriber with optional OpenTelemetry (OTLP) integration for distributed tracing.
//! OTLP export is on only when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the traces-specific
//! variant) is set; otherwise tracing is local logging only.
//! It installs a Prometheus metrics recorder and provides an HTTP server exposing `/metrics`, `/live`, and `/ready` endpoints.
//! Metrics serving is asynchronous and uses `axum` to handle requests.
//! In tests, metrics recorder initialization is cached to avoid conflicts, and subscriber initialization is adapted accordingly.

pub(crate) mod backup;
pub mod log_writer;
pub(crate) mod tenants;
pub mod timings;

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
        // Off the calling thread: see `log_writer`.
        let fmt_layer = tracing_subscriber::fmt::layer().with_writer(log_writer::install());
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

/// Serves Prometheus metrics and health endpoints on `listener`.
///
/// Starts an asynchronous HTTP server exposing:
/// - `/metrics`: Prometheus metrics endpoint.
/// - `/live`: liveness probe returning "ok".
/// - `/ready`: readiness probe, gated on `readiness`.
/// - `/replication/halted`: replicas replication has stopped for.
/// - `/backup/offsets`: the committed offsets a backup point records.
///
/// Runs until `shutdown` resolves, then stops accepting new requests and lets
/// in-flight ones finish. Returns an I/O error if serving fails.
pub(crate) async fn serve_metrics<F>(
    handle: PrometheusHandle,
    listener: tokio::net::TcpListener,
    readiness: Readiness,
    halted: std::sync::Arc<felix_replication::halted::HaltedReplicas>,
    backup: backup::BackupOffsets,
    shutdown: F,
) -> std::io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    // Read-only for the same reason `/replication/halted` is.
    let router = health_router(handle, readiness, halted).merge(backup::router(backup));
    axum::serve(listener, router.into_make_service())
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
                ready_status(
                    &felix_storage::disk_log::poisoned_logs(),
                    readiness.is_ready(),
                )
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

/// What `/ready` answers. A poisoned log refuses writes until it is
/// reopened, so the body names each one's tenant and what stopped it. One
/// poisoned log fails the whole broker's probe for now: see "A failed fsync
/// poisons the log" in `docs/durable-storage.md`.
///
/// This listener has no authentication and `/metrics` on it exposes tenant
/// labels only, so the namespace and stream stay out of the body; the
/// broker's log has the full shard name. The process does not exit and
/// `/live` stays ok, so the body tells the operator what to do.
fn ready_status(
    poisoned: &[felix_storage::disk_log::PoisonedLog],
    ready: bool,
) -> (axum::http::StatusCode, String) {
    if !poisoned.is_empty() {
        let mut lines: Vec<String> = poisoned
            .iter()
            .map(|log| {
                let tenant = log.shard.split('/').next().unwrap_or_default();
                let reason = log.reason.replace(&log.shard, "<shard>");
                format!("a log of tenant {tenant} is poisoned: {reason}")
            })
            .collect();
        lines.push(
            "reboot the host before restarting the broker; the broker log names each shard"
                .to_string(),
        );
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            lines.join("\n"),
        )
    } else if ready {
        (axum::http::StatusCode::OK, "ok".to_string())
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "draining".to_string(),
        )
    }
}

/// The probes a broker answers before it is built: `/live` says ok, `/ready`
/// says why not, and `/metrics` renders. Startup can wait a long time there,
/// on the control plane or on recovery, and a liveness probe that finds no
/// listener restarts a broker that is working as intended.
pub(crate) struct StartingProbes {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

/// Serve [`StartingProbes`] on a duplicate of `listener`'s socket. The caller
/// keeps `listener` for the full server, so the port stays bound throughout
/// and a connection arriving between the two waits in its backlog.
pub(crate) fn serve_starting(
    listener: &std::net::TcpListener,
    handle: PrometheusHandle,
) -> std::io::Result<StartingProbes> {
    let duplicate = listener.try_clone()?;
    duplicate.set_nonblocking(true)?;
    let duplicate = tokio::net::TcpListener::from_std(duplicate)?;
    let router = axum::Router::new()
        .route(
            "/metrics",
            axum::routing::get(move || async move { handle.render() }),
        )
        .route("/live", axum::routing::get(|| async { "ok" }))
        .route(
            "/ready",
            axum::routing::get(|| async {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    "starting: waiting for the control plane or opening storage",
                )
            }),
        );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _ = axum::serve(duplicate, router)
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
            })
            .await;
    });
    Ok(StartingProbes { stop, task })
}

impl StartingProbes {
    /// Stop answering, so the full server is the only one accepting.
    pub(crate) async fn stop(self) {
        let _ = self.stop.send(());
        let mut task = self.task;
        if tokio::time::timeout(std::time::Duration::from_secs(1), &mut task)
            .await
            .is_err()
        {
            task.abort();
        }
    }
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
        let handle = install_recorder_with_upkeep();
        let _ = METRICS_HANDLE.set(handle.clone());
        handle
    }
    #[cfg(not(test))]
    {
        install_recorder_with_upkeep()
    }
}

/// Both builds go through here, so a test of this covers the production path.
fn install_recorder_with_upkeep() -> PrometheusHandle {
    let handle = builder()
        .install_recorder()
        .expect("install metrics recorder");
    spawn_metrics_upkeep(handle.clone());
    handle
}

/// Upkeep threads started by this process; lets a test see the recorder was wired.
static UPKEEP_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How often the exporter's raw histogram samples are folded into summaries.
const METRICS_UPKEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Folds the recorder's raw histogram samples into bounded summaries on a
/// timer.
///
/// The exporter keeps every recorded sample until something renders or runs
/// upkeep. Without this a broker nobody scrapes holds one `f64` per histogram
/// record for as long as it runs, and a scraped one holds a scrape interval's
/// worth.
pub fn spawn_metrics_upkeep(handle: PrometheusHandle) {
    spawn_metrics_upkeep_every(handle, METRICS_UPKEEP_INTERVAL);
}

/// [`spawn_metrics_upkeep`] with an explicit period. A plain thread, so it
/// needs no runtime and keeps draining while the runtime is saturated.
pub fn spawn_metrics_upkeep_every(handle: PrometheusHandle, every: std::time::Duration) {
    UPKEEP_THREADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let spawned = std::thread::Builder::new()
        .name("felix-metrics-upkeep".into())
        .spawn(move || {
            loop {
                std::thread::sleep(every);
                handle.run_upkeep();
            }
        });
    if let Err(err) = spawned {
        tracing::warn!(%err, "metrics upkeep thread did not start; histogram samples will accumulate");
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

#[cfg(test)]
pub(crate) fn upkeep_threads_started() -> usize {
    UPKEEP_THREADS.load(std::sync::atomic::Ordering::Relaxed)
}
