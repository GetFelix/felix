//! Per-tenant traffic metrics, with a cap on how many tenants get a label.
//!
//! Every distinct label value is a series the Prometheus scraper keeps for
//! good, so an unbounded `tenant` label is a memory leak in two processes.
//! The first `FELIX_TENANT_METRICS_MAX` tenants this broker sees get their
//! own series; the rest are counted together under [`OVERFLOW_LABEL`], and
//! `felix_tenant_metrics_overflow_total` says that is happening.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use metrics::{Counter, SharedString};
use parking_lot::RwLock;

/// Messages a tenant published through this broker, by `tenant`.
pub(crate) const PUBLISHED_MESSAGES_TOTAL: &str = "felix_tenant_published_messages_total";
/// Payload bytes of those messages, by `tenant`.
pub(crate) const PUBLISHED_BYTES_TOTAL: &str = "felix_tenant_published_bytes_total";
/// Messages this broker handed to a tenant's subscribers, by `tenant`.
pub(crate) const DELIVERED_MESSAGES_TOTAL: &str = "felix_tenant_delivered_messages_total";
/// Payload bytes of those messages, by `tenant`.
pub(crate) const DELIVERED_BYTES_TOTAL: &str = "felix_tenant_delivered_bytes_total";
/// Publishes held back by a tenant's quota, by `tenant` and `action`:
/// `refused` (the client was told to retry), `dropped` (a fire-and-forget
/// publish was shed) or `delayed` (the publish waited for its quota).
pub(crate) const THROTTLED_TOTAL: &str = "felix_tenant_publish_throttled_total";
/// Publishes the publish queue had no room for, by `tenant` and `action`:
/// `refused` (the client was told to retry) or `dropped` (a fire-and-forget
/// publish was shed).
pub(crate) const QUEUE_FULL_TOTAL: &str = "felix_tenant_publish_queue_full_total";
/// Recordings that went to the overflow label because the cap was reached.
pub(crate) const OVERFLOW_TOTAL: &str = "felix_tenant_metrics_overflow_total";

/// The label every tenant past the cap is counted under. Leading underscore
/// so it sorts apart from real tenant ids in a dashboard.
pub(crate) const OVERFLOW_LABEL: &str = "_overflow";

pub(crate) const THROTTLE_REFUSED: &str = "refused";
pub(crate) const THROTTLE_DROPPED: &str = "dropped";
pub(crate) const THROTTLE_DELAYED: &str = "delayed";

/// Until [`set_max_tenants`] runs, e.g. in tests.
const DEFAULT_MAX_TENANTS: usize = 100;

static MAX_TENANTS: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_TENANTS);
static LABELS: OnceLock<TenantLabels> = OnceLock::new();

/// Set how many tenants get their own label. Called once at startup; tenants
/// already labelled keep their label.
pub(crate) fn set_max_tenants(max: usize) {
    MAX_TENANTS.store(max, Ordering::Relaxed);
}

/// Count a publish `tenant` made.
pub(crate) fn record_published(tenant: &str, messages: u64, bytes: u64) {
    let label = labels().label(tenant, MAX_TENANTS.load(Ordering::Relaxed));
    metrics::counter!(PUBLISHED_MESSAGES_TOTAL, "tenant" => label.clone()).increment(messages);
    metrics::counter!(PUBLISHED_BYTES_TOTAL, "tenant" => label).increment(bytes);
}

/// Count a publish held back by `tenant`'s quota.
pub(crate) fn record_throttled(tenant: &str, action: &'static str) {
    let label = labels().label(tenant, MAX_TENANTS.load(Ordering::Relaxed));
    metrics::counter!(THROTTLED_TOTAL, "tenant" => label, "action" => action).increment(1);
}

/// Count a publish `tenant` made that found no room in the publish queue.
pub(crate) fn record_queue_full(tenant: &str, action: &'static str) {
    let label = labels().label(tenant, MAX_TENANTS.load(Ordering::Relaxed));
    metrics::counter!(QUEUE_FULL_TOTAL, "tenant" => label, "action" => action).increment(1);
}

/// Delivery counters for one subscription, resolved once so the per-event
/// path is two atomic adds.
#[derive(Clone)]
pub(crate) struct TenantDelivery {
    messages: Counter,
    bytes: Counter,
}

impl TenantDelivery {
    pub(crate) fn for_tenant(tenant: &str) -> Self {
        let label = labels().label(tenant, MAX_TENANTS.load(Ordering::Relaxed));
        Self {
            messages: metrics::counter!(DELIVERED_MESSAGES_TOTAL, "tenant" => label.clone()),
            bytes: metrics::counter!(DELIVERED_BYTES_TOTAL, "tenant" => label),
        }
    }

    pub(crate) fn record(&self, messages: usize, bytes: usize) {
        self.messages.increment(messages as u64);
        self.bytes.increment(bytes as u64);
    }
}

fn labels() -> &'static TenantLabels {
    LABELS.get_or_init(TenantLabels::default)
}

/// The tenants that have a label of their own.
#[derive(Default)]
pub(crate) struct TenantLabels {
    labelled: RwLock<HashSet<Arc<str>>>,
}

impl TenantLabels {
    /// `tenant`'s label: its own id while fewer than `max` tenants hold one,
    /// [`OVERFLOW_LABEL`] after.
    pub(crate) fn label(&self, tenant: &str, max: usize) -> SharedString {
        if let Some(known) = self.labelled.read().get(tenant) {
            return SharedString::from(Arc::clone(known));
        }
        let mut labelled = self.labelled.write();
        if let Some(known) = labelled.get(tenant) {
            return SharedString::from(Arc::clone(known));
        }
        if labelled.len() >= max {
            drop(labelled);
            metrics::counter!(OVERFLOW_TOTAL).increment(1);
            return SharedString::const_str(OVERFLOW_LABEL);
        }
        let label: Arc<str> = Arc::from(tenant);
        labelled.insert(Arc::clone(&label));
        SharedString::from(label)
    }
}

#[cfg(test)]
mod tests;
