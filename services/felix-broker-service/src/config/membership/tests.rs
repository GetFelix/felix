use super::{election_features, features_for, is_host_port};

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

/// Every broker of this build names start holders in its reports, which the
/// control plane needs before it finalizes `joint_fence`.
#[test]
fn a_broker_reports_that_it_names_start_holders() {
    let start_holders = felix_common::fleet::START_HOLDERS.name();
    for (fences, by_lease) in [(true, false), (true, true), (false, false)] {
        assert!(features_for(fences, by_lease).contains(start_holders));
    }
}

/// A broker offering `ELECTION` says so, and that it stands for election;
/// one that does not reports neither.
#[test]
fn a_broker_offering_election_reports_it() {
    let added: Vec<String> = election_features().collect();
    assert!(added.contains(&felix_common::fleet::PEER_ELECTION.name().to_string()));
    assert!(added.contains(&felix_common::fleet::REPLICA_ELECTION.name().to_string()));
    for fences in [false, true] {
        let reported = features_for(fences, false);
        assert!(added.iter().all(|feature| !reported.contains(feature)));
    }
}
