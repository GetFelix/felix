use std::collections::HashMap;

use axum::extract::Query;
use axum::{Json, Router, routing::get};
use tokio::net::TcpListener;

use super::*;
use crate::test_support::{spawn_axum_with_shutdown, wait_for_listen};

/// Two pages, then nothing: the follower drains both in one round.
async fn feed(Query(query): Query<HashMap<String, String>>) -> Json<serde_json::Value> {
    let since: u64 = query.get("since").and_then(|s| s.parse().ok()).unwrap_or(0);
    Json(match since {
        0 => serde_json::json!({
            "items": [{"seq": 1, "tenant_id": "t1", "principal_id": "alice", "revoked_at": 100}],
            "next_seq": 2,
        }),
        2 => serde_json::json!({
            "items": [{"seq": 2, "tenant_id": "t1", "principal_id": "*", "revoked_at": 50}],
            "next_seq": 3,
        }),
        _ => serde_json::json!({"items": [], "next_seq": since}),
    })
}

#[tokio::test]
async fn revocations_from_the_feed_are_applied() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_stop, _server) = spawn_axum_with_shutdown(
        listener,
        Router::new().route("/v1/principal-revocations/changes", get(feed)),
    );
    wait_for_listen(addr).await.unwrap();

    let revocations = Arc::new(Revocations::default());
    let shutdown = CancellationToken::new();
    let follower = tokio::spawn(follow_revocations(
        Arc::clone(&revocations),
        format!("http://{addr}"),
        Duration::from_millis(10),
        None,
        shutdown.clone(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !revocations.revokes("t1", "bob", None, 50) {
        assert!(tokio::time::Instant::now() < deadline, "feed not applied");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(revocations.revokes("t1", "alice", None, 100));
    assert!(!revocations.revokes("t1", "alice", None, 101));
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), follower)
        .await
        .expect("stops on shutdown")
        .unwrap();
}

#[test]
fn a_failing_feed_is_warned_about_once_a_minute() {
    let mut outage = Outage::default();
    let start = Instant::now();
    assert!(outage.should_warn(start));
    assert!(!outage.should_warn(start + Duration::from_secs(1)));
    assert!(!outage.should_warn(start + WARN_EVERY - Duration::from_millis(1)));
    assert!(outage.should_warn(start + WARN_EVERY));
    outage.ended();
    assert!(outage.should_warn(start + WARN_EVERY + Duration::from_secs(1)));
}

/// Records the last value each gauge was set to.
#[derive(Clone, Default)]
struct Gauges(Arc<std::sync::Mutex<HashMap<String, f64>>>);

struct GaugeHandle {
    name: String,
    values: Arc<std::sync::Mutex<HashMap<String, f64>>>,
}

impl metrics::GaugeFn for GaugeHandle {
    fn increment(&self, _: f64) {}
    fn decrement(&self, _: f64) {}
    fn set(&self, value: f64) {
        self.values
            .lock()
            .expect("gauges")
            .insert(self.name.clone(), value);
    }
}

impl metrics::Recorder for Gauges {
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
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(Arc::new(GaugeHandle {
            name: key.name().to_string(),
            values: Arc::clone(&self.0),
        }))
    }
    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

/// A feed that is down from the start still exports the last-success
/// gauge, starting at the process's first poll, so its age can be alerted on.
#[test]
fn the_last_success_gauge_exists_while_the_feed_is_down_from_the_start() {
    let gauges = Gauges::default();
    metrics::with_local_recorder(&gauges, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(async {
                let shutdown = CancellationToken::new();
                let follower = tokio::spawn(follow_revocations(
                    Arc::new(Revocations::default()),
                    // Nothing listens on port 9 of the loopback.
                    "http://127.0.0.1:9".to_string(),
                    Duration::from_millis(10),
                    None,
                    shutdown.clone(),
                ));
                tokio::time::sleep(Duration::from_millis(100)).await;
                shutdown.cancel();
                follower.await.expect("follower");
            });
    });
    let set = gauges
        .0
        .lock()
        .expect("gauges")
        .get(LAST_SUCCESS_GAUGE)
        .copied();
    assert!(set.is_some_and(|at| at > 0.0), "{set:?}");
}
