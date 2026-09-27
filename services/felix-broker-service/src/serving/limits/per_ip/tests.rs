use std::net::{IpAddr, Ipv4Addr};

use super::*;

const A: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
const B: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

#[test]
fn an_address_at_its_cap_is_refused_and_others_are_not() {
    let limiter = PerIpLimiter::new(2);
    let first = limiter.try_acquire(A).expect("first");
    let _second = limiter.try_acquire(A).expect("second");
    assert!(
        limiter.try_acquire(A).is_none(),
        "third from A is over the cap"
    );
    assert!(limiter.try_acquire(B).is_some(), "B has its own count");

    drop(first);
    assert!(
        limiter.try_acquire(A).is_some(),
        "a closed connection frees its place"
    );
}

#[test]
fn a_released_address_leaves_no_entry_behind() {
    let limiter = PerIpLimiter::new(4);
    let permits: Vec<_> = (0..3)
        .map(|_| limiter.try_acquire(A).expect("permit"))
        .collect();
    assert_eq!(limiter.held(A), 3);
    drop(permits);
    assert_eq!(limiter.held(A), 0);
    assert_eq!(limiter.addresses(), 0);
}

#[test]
fn an_ipv4_mapped_address_shares_the_ipv4_count() {
    let limiter = PerIpLimiter::new(1);
    let _v4 = limiter.try_acquire(A).expect("v4");
    let mapped = IpAddr::V6(Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped());
    assert!(
        limiter.try_acquire(mapped).is_none(),
        "the mapped form is the same host"
    );
}

#[test]
fn zero_is_unlimited() {
    let limiter = PerIpLimiter::new(0);
    let permits: Vec<_> = (0..10_000)
        .map(|_| limiter.try_acquire(A).expect("permit"))
        .collect();
    assert_eq!(permits.len(), 10_000);
    assert_eq!(limiter.addresses(), 0, "nothing is tracked when unlimited");
}
