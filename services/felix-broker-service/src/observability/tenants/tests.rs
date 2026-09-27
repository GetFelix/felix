use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use metrics::{
    Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, Unit,
    with_local_recorder,
};

use super::*;

#[test]
fn tenants_past_the_cap_share_the_overflow_label() {
    let labels = TenantLabels::default();
    assert_eq!(&*labels.label("a", 2), "a");
    assert_eq!(&*labels.label("b", 2), "b");
    assert_eq!(&*labels.label("c", 2), OVERFLOW_LABEL);
    assert_eq!(&*labels.label("d", 2), OVERFLOW_LABEL);
    // A tenant that got a label keeps it.
    assert_eq!(&*labels.label("a", 2), "a");
    assert_eq!(
        labels.labelled.read().len(),
        2,
        "overflow tenants are not remembered"
    );
}

#[test]
fn a_cap_of_zero_labels_nobody() {
    let labels = TenantLabels::default();
    assert_eq!(&*labels.label("a", 0), OVERFLOW_LABEL);
}

#[test]
fn publishes_and_deliveries_are_counted_by_tenant() {
    let recorder = Counting::default();
    with_local_recorder(&recorder, || {
        record_published("acme", 3, 300);
        record_published("acme", 1, 50);
        let delivery = TenantDelivery::for_tenant("acme");
        delivery.record(2, 20);
        record_throttled("acme", THROTTLE_REFUSED);
    });
    assert_eq!(recorder.get(PUBLISHED_MESSAGES_TOTAL, "acme"), 4);
    assert_eq!(recorder.get(PUBLISHED_BYTES_TOTAL, "acme"), 350);
    assert_eq!(recorder.get(DELIVERED_MESSAGES_TOTAL, "acme"), 2);
    assert_eq!(recorder.get(DELIVERED_BYTES_TOTAL, "acme"), 20);
    assert_eq!(recorder.get(THROTTLED_TOTAL, "acme"), 1);
}

/// Sums counters by name and `tenant` label.
#[derive(Default)]
struct Counting {
    counts: Arc<Mutex<HashMap<(String, String), u64>>>,
}

impl Counting {
    fn get(&self, name: &str, tenant: &str) -> u64 {
        self.counts
            .lock()
            .expect("counts")
            .get(&(name.to_string(), tenant.to_string()))
            .copied()
            .unwrap_or(0)
    }
}

struct Handle {
    key: (String, String),
    counts: Arc<Mutex<HashMap<(String, String), u64>>>,
}

impl CounterFn for Handle {
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

impl Recorder for Counting {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let tenant = key
            .labels()
            .find(|label| label.key() == "tenant")
            .map(|label| label.value().to_string())
            .unwrap_or_default();
        Counter::from_arc(Arc::new(Handle {
            key: (key.name().to_string(), tenant),
            counts: Arc::clone(&self.counts),
        }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}
