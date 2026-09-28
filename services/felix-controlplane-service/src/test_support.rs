//! Shared `AppState` construction for tests across this crate, so a new
//! field is added in one place.
#![cfg(test)]

use std::sync::Arc;

use crate::api::AppState;
use crate::api::readiness::{AlwaysReady, HealthProbe, Readiness};
use crate::api::types::{FeatureFlags, Region};
use crate::store::ControlPlaneAuthStore;

/// An `AppState` over `store`, ready unless `probe` says otherwise.
pub(crate) fn app_state(
    store: Arc<dyn ControlPlaneAuthStore + Send + Sync>,
    probe: Arc<dyn HealthProbe>,
) -> AppState {
    AppState {
        region: Region {
            region_id: "local".to_string(),
            display_name: "Local".to_string(),
        },
        api_version: "v1".to_string(),
        features: FeatureFlags {
            durable_storage: store.is_durable(),
            tiered_storage: false,
            bridges: false,
        },
        store,
        oidc_validator: crate::auth::oidc::UpstreamOidcValidator::default(),
        bootstrap_enabled: false,
        bootstrap_tokens: Vec::new(),
        node_liveness: Default::default(),
        readiness: Arc::new(Readiness::new(probe)),
        in_flight: Default::default(),
        placement_wakes: Default::default(),
        move_policy: Default::default(),
    }
}

/// [`app_state`] with a store that always answers ready -- what every
/// existing caller wanted before one of them needed to say otherwise.
pub(crate) fn app_state_ready(store: Arc<dyn ControlPlaneAuthStore + Send + Sync>) -> AppState {
    app_state(store, Arc::new(AlwaysReady))
}

/// A tenant `t1` with signing keys, a one-shard durable stream `t1/ns/orders`
/// replicated twice, and live brokers `broker-x` and `broker-y`. Nothing is
/// placed.
pub(crate) async fn one_shard_cluster() -> (
    Arc<crate::store::memory::InMemoryStore>,
    crate::auth::felix_token::TenantSigningKeys,
) {
    use crate::model::{
        ConsistencyLevel, DeliveryGuarantee, Namespace, Node, NodeCapacity, NodeLifecycle,
        NodeSpec, NodeStatus, RetentionPolicy, Stream, StreamKind, Tenant,
    };
    use crate::store::{AuthStore, ControlPlaneStore, StoreConfig};

    let store = Arc::new(crate::store::memory::InMemoryStore::new(StoreConfig {
        changes_limit: 1000,
        change_retention_max_rows: Some(1000),
    }));
    store
        .create_tenant(Tenant {
            tenant_id: "t1".to_string(),
            display_name: "T".to_string(),
        })
        .await
        .expect("tenant");
    let keys = crate::auth::keys::generate_signing_keys().expect("keys");
    store
        .set_tenant_signing_keys("t1", keys.clone())
        .await
        .expect("keys");
    store
        .create_namespace(Namespace {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            display_name: "NS".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            stream: "orders".to_string(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 2,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
        })
        .await
        .expect("stream");
    for (i, id) in ["broker-x", "broker-y"].into_iter().enumerate() {
        store
            .register_node(Node {
                node_id: id.to_string(),
                spec: NodeSpec {
                    advertise_addr: format!("10.0.0.4:{}", 7100 + i),
                    client_addr: None,
                    kafka_addr: None,
                    region: "local".to_string(),
                    zone: None,
                    labels: Default::default(),
                    capacity: NodeCapacity::default(),
                },
                status: NodeStatus {
                    lifecycle: NodeLifecycle::Live,
                    last_heartbeat_at_millis: crate::clock::now_millis(),
                    registered_at_millis: 1,
                    incarnation: 0,
                    features: Default::default(),
                },
            })
            .await
            .expect("node");
    }
    (store, keys)
}

/// The key of `t1/ns/orders` shard 0.
pub(crate) fn shard_zero() -> crate::model::ShardKey {
    crate::model::ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: crate::model::ShardKind::Stream,
    }
}

/// A credential for `t1` carrying `perms`.
pub(crate) fn token(keys: &crate::auth::felix_token::TenantSigningKeys, perms: &[&str]) -> String {
    crate::auth::felix_token::mint_token_for(
        keys,
        "t1",
        "p:test",
        perms.iter().map(|perm| perm.to_string()).collect(),
        std::time::Duration::from_secs(900),
        crate::auth::felix_token::CONTROLPLANE_AUDIENCE,
    )
    .expect("token")
}

/// Records counter increments for the futures it runs, so a test can assert
/// that a refusal was counted. The recorder is thread-local, which is why
/// [`CountingRecorder::run`] drives the future on its own current-thread
/// runtime rather than inside `#[tokio::test]`.
#[derive(Clone, Default)]
pub(crate) struct CountingRecorder {
    counts: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
}

impl CountingRecorder {
    pub(crate) fn run<F: std::future::Future>(&self, future: F) -> F::Output {
        metrics::with_local_recorder(self, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(future)
        })
    }

    /// The total for a counter, written `name` or `name{label=value,...}` with
    /// labels in the order the code passes them.
    pub(crate) fn count(&self, key: &str) -> u64 {
        self.counts
            .lock()
            .expect("counts")
            .get(key)
            .copied()
            .unwrap_or(0)
    }
}

struct CountingHandle {
    key: String,
    counts: Arc<std::sync::Mutex<std::collections::HashMap<String, u64>>>,
}

impl metrics::CounterFn for CountingHandle {
    fn increment(&self, value: u64) {
        *self
            .counts
            .lock()
            .expect("counts")
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.counts
            .lock()
            .expect("counts")
            .insert(self.key.clone(), value);
    }
}

impl metrics::Recorder for CountingRecorder {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        let labels: Vec<String> = key
            .labels()
            .map(|label| format!("{}={}", label.key(), label.value()))
            .collect();
        let key = if labels.is_empty() {
            key.name().to_string()
        } else {
            format!("{}{{{}}}", key.name(), labels.join(","))
        };
        metrics::Counter::from_arc(Arc::new(CountingHandle {
            key,
            counts: self.counts.clone(),
        }))
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}
