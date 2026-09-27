//! Per-tenant publish rate limits: a token bucket per tenant for bytes and
//! one for messages.
//!
//! A bucket may go into debt. A publish is admitted whenever its tenant is not
//! already in debt, and its whole cost is taken, so a batch larger than the
//! burst still goes through and the tenant then waits out what it overdrew.
//! Refusing a batch for being larger than the burst would make it
//! unpublishable at any rate.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::config::{LimitsConfig, TenantQuota};

/// The most a tenant can overdraw, as time at its rate. It bounds how long
/// one huge batch locks its tenant out, and how long a Kafka produce is held:
/// kept well under the 30 s request timeout Kafka clients default to, so a
/// held request is not retried as a duplicate.
pub(crate) const MAX_DEBT: Duration = Duration::from_secs(5);
/// Tenants tracked before idle buckets are swept. Tenants are authenticated,
/// so this is not an attacker's lever; it only keeps a long-lived broker from
/// holding a bucket for every tenant it has ever seen.
const SWEEP_ABOVE: usize = 4_096;
/// A bucket untouched this long is full again and carries no state worth
/// keeping.
const IDLE: Duration = Duration::from_secs(60);

/// Publish rate limits for every tenant on this broker.
pub(crate) struct TenantRates {
    default: TenantQuota,
    overrides: BTreeMap<String, TenantQuota>,
    burst: Duration,
    enabled: bool,
    buckets: DashMap<String, Buckets>,
}

impl TenantRates {
    pub(crate) fn new(config: &LimitsConfig) -> Self {
        Self {
            default: config.tenant_publish_default,
            overrides: config.tenant_publish_overrides.clone(),
            burst: Duration::from_millis(config.tenant_publish_burst_ms.max(1)),
            enabled: config.any_quota(),
            buckets: DashMap::new(),
        }
    }

    /// No quota anywhere: every call admits without touching shared state.
    pub(crate) fn unlimited() -> Self {
        Self::new(&LimitsConfig::default())
    }

    /// Admit a publish of `msgs` messages and `bytes` payload bytes for
    /// `tenant`, or say how long until it would be.
    ///
    /// Nothing is taken from a refused publish, so a client that retries after
    /// the wait is admitted.
    pub(crate) fn try_admit(&self, tenant: &str, msgs: u64, bytes: u64) -> Result<(), Duration> {
        self.try_admit_at(tenant, msgs, bytes, Instant::now())
    }

    /// Take the cost of a publish that will happen regardless, and return how
    /// long the tenant must wait for its balance to recover. How the Kafka
    /// listener throttles: a Kafka client is slowed by holding its request,
    /// not refused.
    pub(crate) fn charge(&self, tenant: &str, msgs: u64, bytes: u64) -> Duration {
        self.charge_at(tenant, msgs, bytes, Instant::now())
    }

    pub(super) fn try_admit_at(
        &self,
        tenant: &str,
        msgs: u64,
        bytes: u64,
        now: Instant,
    ) -> Result<(), Duration> {
        self.with_buckets(tenant, now, |buckets| {
            let wait = buckets.wait();
            if !wait.is_zero() {
                return Err(wait);
            }
            buckets.take(msgs, bytes);
            Ok(())
        })
        .unwrap_or(Ok(()))
    }

    pub(super) fn charge_at(&self, tenant: &str, msgs: u64, bytes: u64, now: Instant) -> Duration {
        self.with_buckets(tenant, now, |buckets| {
            buckets.take(msgs, bytes);
            buckets.wait()
        })
        .unwrap_or(Duration::ZERO)
    }

    /// Run `f` on `tenant`'s refilled buckets, or return `None` when the
    /// tenant has no quota.
    fn with_buckets<T>(
        &self,
        tenant: &str,
        now: Instant,
        f: impl FnOnce(&mut Buckets) -> T,
    ) -> Option<T> {
        if !self.enabled {
            return None;
        }
        if let Some(mut buckets) = self.buckets.get_mut(tenant) {
            buckets.refill(now);
            return Some(f(&mut buckets));
        }
        let quota = self.overrides.get(tenant).copied().unwrap_or(self.default);
        if quota.is_unlimited() {
            return None;
        }
        if self.buckets.len() >= SWEEP_ABOVE {
            self.sweep(now);
        }
        let mut entry = self
            .buckets
            .entry(tenant.to_string())
            .or_insert_with(|| Buckets::new(quota, self.burst, now));
        entry.refill(now);
        Some(f(&mut entry))
    }

    fn sweep(&self, now: Instant) {
        self.buckets.retain(|_, buckets| !buckets.idle(now));
    }

    #[cfg(test)]
    pub(super) fn tracked(&self) -> usize {
        self.buckets.len()
    }
}

/// One tenant's buckets. A dimension with no rate has no bucket.
struct Buckets {
    bytes: Option<Bucket>,
    msgs: Option<Bucket>,
}

impl Buckets {
    fn new(quota: TenantQuota, burst: Duration, now: Instant) -> Self {
        Self {
            bytes: Bucket::new(quota.bytes_per_sec, burst, now),
            msgs: Bucket::new(quota.msgs_per_sec, burst, now),
        }
    }

    fn refill(&mut self, now: Instant) {
        self.each(|bucket| bucket.refill(now));
    }

    fn take(&mut self, msgs: u64, bytes: u64) {
        if let Some(bucket) = &mut self.msgs {
            bucket.take(msgs as f64);
        }
        if let Some(bucket) = &mut self.bytes {
            bucket.take(bytes as f64);
        }
    }

    /// How long until neither bucket is in debt.
    fn wait(&self) -> Duration {
        [&self.bytes, &self.msgs]
            .into_iter()
            .flatten()
            .map(Bucket::wait)
            .max()
            .unwrap_or(Duration::ZERO)
    }

    fn idle(&self, now: Instant) -> bool {
        [&self.bytes, &self.msgs]
            .into_iter()
            .flatten()
            .all(|bucket| bucket.tokens >= 0.0 && now.duration_since(bucket.updated) >= IDLE)
    }

    fn each(&mut self, mut f: impl FnMut(&mut Bucket)) {
        if let Some(bucket) = &mut self.bytes {
            f(bucket);
        }
        if let Some(bucket) = &mut self.msgs {
            f(bucket);
        }
    }
}

struct Bucket {
    /// Tokens per second.
    rate: f64,
    capacity: f64,
    /// Negative while the tenant is in debt.
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    fn new(rate: u64, burst: Duration, now: Instant) -> Option<Self> {
        if rate == 0 {
            return None;
        }
        let rate = rate as f64;
        // At least one token, so a rate below one per burst window can still
        // admit anything.
        let capacity = (rate * burst.as_secs_f64()).max(1.0);
        Some(Self {
            rate,
            capacity,
            tokens: capacity,
            updated: now,
        })
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.capacity);
        self.updated = now;
    }

    fn take(&mut self, cost: f64) {
        let floor = -self.rate * MAX_DEBT.as_secs_f64();
        self.tokens = (self.tokens - cost).max(floor);
    }

    fn wait(&self) -> Duration {
        if self.tokens >= 0.0 {
            return Duration::ZERO;
        }
        // Rounded up to the millisecond: a wait reported as 0 ms would send a
        // client straight back into the same refusal.
        let millis = (-self.tokens / self.rate * 1_000.0).ceil();
        Duration::from_millis(millis as u64)
    }
}

#[cfg(test)]
mod tests;
