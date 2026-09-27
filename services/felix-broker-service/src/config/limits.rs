//! Limits on what one client address or one tenant may take from a broker.
//!
//! Every value here is per broker: a tenant publishing through three brokers
//! gets three times its quota. What each limit protects against is in
//! `docs/broker-config.md`.

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use serde::Serialize;

/// Connections one source address may hold on the client QUIC listeners.
/// Generous, because a NAT gateway or a pooled client legitimately holds many;
/// the point is that one address cannot hold all of them.
const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 512;
/// One second of rate is the burst a tenant may send above its average.
const DEFAULT_TENANT_PUBLISH_BURST_MS: u64 = 1_000;
/// Tenants given their own metrics series before the rest share one.
const DEFAULT_TENANT_METRICS_MAX: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LimitsConfig {
    /// Connections one source IP may hold across the client QUIC listeners.
    /// `0` is unlimited.
    pub max_connections_per_ip: usize,
    /// The publish rate every tenant gets unless `tenant_publish_overrides`
    /// names it.
    pub tenant_publish_default: TenantQuota,
    /// Per-tenant publish rates that replace the default.
    pub tenant_publish_overrides: BTreeMap<String, TenantQuota>,
    /// How much a tenant may send in a burst, as time at its rate.
    pub tenant_publish_burst_ms: u64,
    /// Distinct tenants that get their own `tenant` label on the per-tenant
    /// metrics; later ones are counted under `_overflow`.
    pub tenant_metrics_max: usize,
}

/// A tenant's publish rate. `0` in either field leaves that dimension
/// unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct TenantQuota {
    pub bytes_per_sec: u64,
    pub msgs_per_sec: u64,
}

impl TenantQuota {
    pub fn is_unlimited(&self) -> bool {
        self.bytes_per_sec == 0 && self.msgs_per_sec == 0
    }
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
            tenant_publish_default: TenantQuota::default(),
            tenant_publish_overrides: BTreeMap::new(),
            tenant_publish_burst_ms: DEFAULT_TENANT_PUBLISH_BURST_MS,
            tenant_metrics_max: DEFAULT_TENANT_METRICS_MAX,
        }
    }
}

impl LimitsConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// [`Self::from_env`] over any source of variables, so the parsing is
    /// testable without touching the process environment.
    ///
    /// A value that does not parse is refused rather than defaulted: a quota
    /// that silently fell back to "unlimited" is the failure this exists to
    /// prevent.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let get = |name: &str| {
            lookup(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let number = |name: &str, default: u64| -> Result<u64> {
            match get(name) {
                None => Ok(default),
                Some(value) => value.parse::<u64>().with_context(|| {
                    format!("{name} must be a non-negative integer, not {value:?}")
                }),
            }
        };
        let defaults = Self::default();
        let burst_ms = number(
            "FELIX_TENANT_PUBLISH_BURST_MS",
            defaults.tenant_publish_burst_ms,
        )?;
        if burst_ms == 0 {
            bail!("FELIX_TENANT_PUBLISH_BURST_MS must be at least 1");
        }
        Ok(Self {
            max_connections_per_ip: number(
                "FELIX_MAX_CONNECTIONS_PER_IP",
                defaults.max_connections_per_ip as u64,
            )? as usize,
            tenant_publish_default: TenantQuota {
                bytes_per_sec: number("FELIX_TENANT_PUBLISH_BYTES_PER_SEC", 0)?,
                msgs_per_sec: number("FELIX_TENANT_PUBLISH_MSGS_PER_SEC", 0)?,
            },
            tenant_publish_overrides: match get("FELIX_TENANT_PUBLISH_QUOTAS") {
                None => BTreeMap::new(),
                Some(value) => parse_overrides(&value)?,
            },
            tenant_publish_burst_ms: burst_ms,
            tenant_metrics_max: number(
                "FELIX_TENANT_METRICS_MAX",
                defaults.tenant_metrics_max as u64,
            )? as usize,
        })
    }

    /// The quota `tenant` publishes under.
    pub fn quota_for(&self, tenant: &str) -> TenantQuota {
        self.tenant_publish_overrides
            .get(tenant)
            .copied()
            .unwrap_or(self.tenant_publish_default)
    }

    /// Whether any tenant has a quota at all.
    pub fn any_quota(&self) -> bool {
        !self.tenant_publish_default.is_unlimited()
            || self
                .tenant_publish_overrides
                .values()
                .any(|quota| !quota.is_unlimited())
    }
}

/// `tenant:bytes_per_sec:msgs_per_sec` entries, comma-separated. The numbers
/// are taken from the right, so a tenant id may itself contain `:`.
fn parse_overrides(value: &str) -> Result<BTreeMap<String, TenantQuota>> {
    let mut overrides = BTreeMap::new();
    for entry in value.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let mut parts = entry.rsplitn(3, ':');
        let (Some(msgs), Some(bytes), Some(tenant)) = (parts.next(), parts.next(), parts.next())
        else {
            bail!(
                "FELIX_TENANT_PUBLISH_QUOTAS entry {entry:?} is not tenant:bytes_per_sec:msgs_per_sec"
            );
        };
        let parse = |field: &str, what: &str| {
            field.trim().parse::<u64>().with_context(|| {
                format!(
                    "FELIX_TENANT_PUBLISH_QUOTAS entry {entry:?}: {what} {field:?} is not a number"
                )
            })
        };
        let tenant = tenant.trim();
        if tenant.is_empty() {
            bail!("FELIX_TENANT_PUBLISH_QUOTAS entry {entry:?} names no tenant");
        }
        let quota = TenantQuota {
            bytes_per_sec: parse(bytes, "bytes_per_sec")?,
            msgs_per_sec: parse(msgs, "msgs_per_sec")?,
        };
        if overrides.insert(tenant.to_string(), quota).is_some() {
            bail!("FELIX_TENANT_PUBLISH_QUOTAS names tenant {tenant:?} twice");
        }
    }
    Ok(overrides)
}
