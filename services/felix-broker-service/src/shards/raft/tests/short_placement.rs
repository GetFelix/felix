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

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
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
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer({
            let captured = captured.clone();
            move || captured.clone()
        })
        .finish();
    let _log = tracing::subscriber::set_default(subscriber);
    let orders = key("orders", 0);
    let warnings = || {
        String::from_utf8(captured.0.lock().expect("lock").clone())
            .expect("utf8")
            .matches("fewer brokers than the stream's replication factor")
            .count()
    };
    let gauge = || {
        gauges
            .0
            .lock()
            .expect("gauges")
            .get(SHORT_PLACEMENT)
            .copied()
    };

    for _ in 0..3 {
        shards
            .take(&mut assignments([assigned(orders.clone(), INCARNATION)]))
            .await;
    }
    assert_eq!(warnings(), 1, "the hold warned on every pass");
    assert_eq!(gauge(), Some(1.0));

    // A new placement version of the same short set warns again.
    let mut next = assigned(orders.clone(), INCARNATION);
    next.generation += 1;
    shards.take(&mut assignments([next])).await;
    assert_eq!(warnings(), 2);

    // A full set is no longer held back.
    shards.take(&mut assignments([replicated()])).await;
    assert_eq!(gauge(), Some(0.0));
    assert_eq!(warnings(), 2);
}
