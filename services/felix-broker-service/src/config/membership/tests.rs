use super::{features_for, is_host_port};

#[test]
fn a_client_advertise_address_is_a_host_and_a_port() {
    for good in [
        "10.0.0.5:5000",
        "broker-1.felix:5000",
        "[::1]:5000",
        "localhost:1",
    ] {
        assert!(is_host_port(good), "{good}");
    }
    for bad in [
        "broker-1.felix",
        ":5000",
        "broker:port",
        "broker:70000",
        "::1:5000",
    ] {
        assert!(!is_host_port(bad), "{bad}");
    }
}

/// **A broker whose reads stay on the lease does not report
/// `lease_free_reads`.** Placement reads the enabled feature as "no leader
/// serves reads on its lease", and this broker would.
#[test]
fn a_broker_reading_on_the_lease_does_not_report_lease_free_reads() {
    let lease_free = felix_common::fleet::LEASE_FREE_READS.name();
    assert!(features_for(true, false).contains(lease_free));
    let on_lease = features_for(true, true);
    assert!(!on_lease.contains(lease_free));
    // Only that one: its writes can still be acknowledged by its followers.
    assert!(on_lease.contains(felix_common::fleet::MAJORITY_ACK.name()));
    assert!(on_lease.contains(felix_common::fleet::GENERATION_START.name()));
    assert!(!features_for(false, false).contains(lease_free));
}
