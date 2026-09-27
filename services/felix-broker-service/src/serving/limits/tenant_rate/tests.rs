use std::time::{Duration, Instant};

use super::*;
use crate::config::{LimitsConfig, TenantQuota};

fn rates(default: TenantQuota, overrides: &[(&str, TenantQuota)]) -> TenantRates {
    TenantRates::new(&LimitsConfig {
        tenant_publish_default: default,
        tenant_publish_overrides: overrides
            .iter()
            .map(|(tenant, quota)| (tenant.to_string(), *quota))
            .collect(),
        tenant_publish_burst_ms: 1_000,
        ..LimitsConfig::default()
    })
}

fn msgs(per_sec: u64) -> TenantQuota {
    TenantQuota {
        bytes_per_sec: 0,
        msgs_per_sec: per_sec,
    }
}

fn bytes(per_sec: u64) -> TenantQuota {
    TenantQuota {
        bytes_per_sec: per_sec,
        msgs_per_sec: 0,
    }
}

#[test]
fn a_tenant_is_admitted_up_to_its_burst_then_refused_until_it_refills() {
    let rates = rates(msgs(100), &[]);
    let t0 = Instant::now();
    for _ in 0..100 {
        rates
            .try_admit_at("t1", 1, 10, t0)
            .expect("within the burst");
    }
    // Exactly at zero is not debt; one more takes it under.
    rates.try_admit_at("t1", 1, 10, t0).expect("at zero");
    let wait = rates
        .try_admit_at("t1", 1, 10, t0)
        .expect_err("in debt now");
    assert_eq!(wait, Duration::from_millis(10), "one message at 100/s");

    // A refused publish takes nothing, so the reported wait is enough.
    rates
        .try_admit_at("t1", 1, 10, t0 + wait)
        .expect("refilled after the wait");
}

#[test]
fn the_long_run_rate_is_the_quota() {
    let rates = rates(bytes(1_000), &[]);
    let t0 = Instant::now();
    let mut admitted = 0u64;
    // Ten seconds of 100-byte publishes offered every millisecond.
    for ms in 0..10_000u64 {
        let now = t0 + Duration::from_millis(ms);
        if rates.try_admit_at("t1", 1, 100, now).is_ok() {
            admitted += 100;
        }
    }
    // The burst (1 s) plus ten seconds at 1000 B/s, give or take one publish.
    assert!(
        (10_900..=11_200).contains(&admitted),
        "admitted {admitted} bytes"
    );
}

#[test]
fn a_batch_larger_than_the_burst_is_admitted_once_and_then_paid_for() {
    let rates = rates(bytes(1_000), &[]);
    let t0 = Instant::now();
    rates
        .try_admit_at("t1", 1, 3_000, t0)
        .expect("a full bucket admits any size");
    let wait = rates.try_admit_at("t1", 1, 1, t0).expect_err("in debt");
    assert_eq!(wait, Duration::from_secs(2));
}

#[test]
fn debt_is_bounded() {
    let rates = rates(bytes(1_000), &[]);
    let t0 = Instant::now();
    let wait = rates.charge_at("t1", 1, 1_000_000, t0);
    assert_eq!(
        wait, MAX_DEBT,
        "one huge batch locks the tenant out for at most MAX_DEBT"
    );
}

#[test]
fn tenants_are_limited_separately_and_overrides_win() {
    let rates = rates(
        msgs(1),
        &[("big", msgs(1_000)), ("free", TenantQuota::default())],
    );
    let t0 = Instant::now();
    rates.try_admit_at("a", 1, 0, t0).expect("a first");
    rates.try_admit_at("a", 1, 0, t0).expect("a to zero");
    assert!(
        rates.try_admit_at("a", 1, 0, t0).is_err(),
        "a is at its rate"
    );
    rates
        .try_admit_at("b", 1, 0, t0)
        .expect("b has its own bucket");
    for _ in 0..500 {
        rates
            .try_admit_at("big", 1, 0, t0)
            .expect("big has a larger quota");
    }
    for _ in 0..10_000 {
        rates
            .try_admit_at("free", 1, 1 << 20, t0)
            .expect("an override of zero is unlimited");
    }
    assert_eq!(rates.tracked(), 3, "an unlimited tenant keeps no bucket");
}

#[test]
fn either_dimension_can_refuse() {
    let both = TenantQuota {
        bytes_per_sec: 1_000_000,
        msgs_per_sec: 10,
    };
    let rates = rates(both, &[]);
    let t0 = Instant::now();
    for _ in 0..11 {
        let _ = rates.try_admit_at("t1", 1, 1, t0);
    }
    assert!(
        rates.try_admit_at("t1", 1, 1, t0).is_err(),
        "the message rate refuses even with bytes to spare"
    );
}

#[test]
fn with_no_quota_nothing_is_tracked() {
    let rates = TenantRates::unlimited();
    let t0 = Instant::now();
    for _ in 0..1_000 {
        rates
            .try_admit_at("t1", 1_000, 1 << 30, t0)
            .expect("unlimited");
    }
    assert_eq!(rates.charge_at("t1", 1, 1, t0), Duration::ZERO);
    assert_eq!(rates.tracked(), 0);
}

#[test]
fn idle_tenants_are_swept_once_many_are_tracked() {
    let rates = rates(msgs(10), &[]);
    let t0 = Instant::now();
    for n in 0..SWEEP_ABOVE {
        rates
            .try_admit_at(&format!("t{n}"), 1, 0, t0)
            .expect("admit");
    }
    assert_eq!(rates.tracked(), SWEEP_ABOVE);
    let later = t0 + IDLE + Duration::from_secs(1);
    rates.try_admit_at("new", 1, 0, later).expect("admit");
    assert_eq!(rates.tracked(), 1, "only the new tenant is left");
}
