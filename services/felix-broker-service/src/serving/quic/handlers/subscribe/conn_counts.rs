//! Per-connection active-subscriber bookkeeping behind the
//! `felix_sub_active_connections` and `felix_sub_connection_subscribers` gauges.
//!
//! Neither gauge is labelled by connection. A series per connection id is one
//! the exporter keeps for the life of the process, so connection churn grew
//! memory and every scrape without bound.

use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;

pub(super) static ACTIVE_SUB_CONN_COUNTS: OnceLock<DashMap<u64, usize>> = OnceLock::new();

/// Subscribers registered across every connection.
static SUBSCRIBERS: AtomicUsize = AtomicUsize::new(0);

pub(super) fn hash64(value: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub(super) fn connection_subscriber_register(connection_id: Option<u64>) {
    let Some(connection_id) = connection_id else {
        return;
    };
    let map = ACTIVE_SUB_CONN_COUNTS.get_or_init(DashMap::new);
    *map.entry(connection_id).or_insert(0) += 1;
    let subscribers = SUBSCRIBERS.fetch_add(1, Ordering::Relaxed) + 1;
    metrics::gauge!("felix_sub_active_connections").set(map.len() as f64);
    metrics::gauge!("felix_sub_connection_subscribers").set(subscribers as f64);
}

pub(super) fn connection_subscriber_unregister(connection_id: Option<u64>) {
    let Some(connection_id) = connection_id else {
        return;
    };
    let Some(map) = ACTIVE_SUB_CONN_COUNTS.get() else {
        return;
    };
    let decremented = match map.get_mut(&connection_id) {
        Some(mut entry) if *entry > 1 => {
            *entry -= 1;
            true
        }
        Some(entry) => {
            drop(entry);
            map.remove(&connection_id);
            true
        }
        None => false,
    };
    if decremented {
        let subscribers = SUBSCRIBERS
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        metrics::gauge!("felix_sub_connection_subscribers").set(subscribers as f64);
    }
    metrics::gauge!("felix_sub_active_connections").set(map.len() as f64);
}
