//! A group held back for a short placement says so.

use std::sync::Mutex;

use super::*;

/// Records the last value each gauge was set to.
#[derive(Clone, Default)]
struct Gauges(Arc<Mutex<std::collections::HashMap<String, f64>>>);

struct GaugeHandle {
    name: String,
    values: Arc<Mutex<std::collections::HashMap<String, f64>>>,
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

fn held(shard: &ShardKey, generation: u64) -> ShortShard {
    ShortShard {
        key: shard.clone(),
        generation,
        replication_factor: 3,
        brokers: vec!["broker-a".to_string()],
    }
}

fn gauge(gauges: &Gauges) -> Option<f64> {
    gauges
        .0
        .lock()
        .expect("gauges")
        .get(SHORT_PLACEMENT)
        .copied()
}

/// **A shard held back for a short placement is warned about once per
/// placement generation and counted by a gauge.** The hold is deliberate, so
/// without both the only clue is "still opening the shard".
#[tokio::test(flavor = "current_thread")]
async fn a_short_placement_warns_once_and_is_counted() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker_of(root.path(), 3).await;
    let shards = unbound_shards(&broker);
    let gauges = Gauges::default();
    let _recorder = metrics::set_default_local_recorder(&gauges);
    let orders = key("orders", 0);

    assert_eq!(shards.report_short(&[held(&orders, 1)]), 1);
    assert_eq!(shards.report_short(&[held(&orders, 1)]), 0, "warned again");
    assert_eq!(shards.report_short(&[held(&orders, 1)]), 0, "warned again");
    assert_eq!(gauge(&gauges), Some(1.0));

    // A new placement generation of the same short set warns again.
    assert_eq!(shards.report_short(&[held(&orders, 2)]), 1);

    // Two held shards warn once each.
    assert_eq!(
        shards.report_short(&[held(&orders, 2), held(&key("audit", 0), 1)]),
        1
    );
    assert_eq!(gauge(&gauges), Some(2.0));

    // Nothing held clears the gauge and forgets the warning.
    assert_eq!(shards.report_short(&[]), 0);
    assert_eq!(gauge(&gauges), Some(0.0));
    assert_eq!(shards.report_short(&[held(&orders, 2)]), 1);
}

/// **A held shard that cannot be opened for a pass stays counted and is not
/// warned about again.** Here the restore gate is still closed.
#[tokio::test(flavor = "current_thread")]
async fn a_short_shard_that_cannot_open_stays_counted() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker_of(root.path(), 3).await;
    let shards = held_shards(&broker);
    let gauges = Gauges::default();
    let _recorder = metrics::set_default_local_recorder(&gauges);
    let orders = key("orders", 0);
    shards.report_short(&[held(&orders, 1)]);

    for _ in 0..2 {
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
    }
    assert_eq!(gauge(&gauges), Some(1.0), "the gauge dipped");
    assert_eq!(shards.short_warned.lock().get(&orders), Some(&1));

    // A full set is not held, open or not.
    shards.take(&mut assignments([replicated()])).await;
    assert_eq!(gauge(&gauges), Some(0.0));
}

/// A shard with a short set is classified once its member is open: blank
/// and short is counted, a full set is not.
#[tokio::test(flavor = "current_thread")]
async fn an_open_blank_short_shard_is_counted_and_a_full_one_is_not() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker_of(root.path(), 3).await;
    let shards = unbound_shards(&broker);
    let gauges = Gauges::default();
    let _recorder = metrics::set_default_local_recorder(&gauges);
    shards
        .take(&mut assignments([assigned(key("orders", 0), INCARNATION)]))
        .await;
    assert_eq!(gauge(&gauges), Some(1.0));
    shards.take(&mut assignments([replicated()])).await;
    assert_eq!(gauge(&gauges), Some(0.0));
}
