//! The retired `FELIX_SHARD_ENGINE` and what a Raft broker needs before it starts.

use std::time::Duration;

use crate::config::env::{
    RaftPrerequisites, ShardEngine, check_shard_engine, parse_raft_group_threads,
    parse_shard_raft_cache_snapshot_every, parse_shard_raft_elections_at_once,
    parse_shard_raft_lease,
};

const ALL: RaftPrerequisites = RaftPrerequisites {
    compiled: true,
    membership: true,
    peer_transport: true,
    durable_storage: true,
};

/// `FELIX_SHARD_ENGINE` is retired. Unset or `raft` starts the broker,
/// which reports the variable as retired; `hybrid` is refused, so nobody
/// believes they still run it.
#[test]
fn the_hybrid_is_refused_and_raft_is_accepted() {
    for accepted in [None, Some(""), Some("  "), Some("raft"), Some(" RAFT ")] {
        check_shard_engine(accepted).expect("accepted");
    }
    for hybrid in ["hybrid", " Hybrid "] {
        let err = check_shard_engine(Some(hybrid)).expect_err("hybrid");
        let text = err.to_string();
        assert!(
            text.contains("FELIX_SHARD_ENGINE=hybrid is refused"),
            "{text}"
        );
        assert!(text.contains("empty FELIX_DURABLE_STORAGE_DIR"), "{text}");
    }
}

/// A typo must not read as the default either: it is refused, naming the
/// variable as retired.
#[test]
fn an_unknown_engine_is_refused() {
    let err = check_shard_engine(Some("rafts")).expect_err("unknown");
    assert!(format!("{err}").contains("retired"), "{err}");
}

/// A clustered, durable broker runs Raft.
#[test]
fn a_clustered_broker_runs_raft() {
    assert_eq!(ALL.resolve().expect("resolve"), ShardEngine::Raft);
}

/// A standalone broker with durable storage runs its durable shards as
/// Raft groups of one; it needs no node id and no internal listener.
#[test]
fn a_standalone_durable_broker_runs_raft() {
    let standalone = RaftPrerequisites {
        membership: false,
        peer_transport: false,
        ..ALL
    };
    assert_eq!(standalone.missing(), None);
    assert_eq!(standalone.resolve().expect("resolve"), ShardEngine::Raft);
}

/// A standalone broker in memory has nothing durable for an engine to run,
/// with the `raft` feature or without it.
#[test]
fn a_standalone_broker_in_memory_runs_no_engine() {
    for compiled in [true, false] {
        let standalone = RaftPrerequisites {
            compiled,
            membership: false,
            peer_transport: false,
            durable_storage: false,
        };
        assert_eq!(standalone.missing(), None);
        let engine = standalone.resolve().expect("resolve");
        assert_eq!(engine, ShardEngine::InMemory);
    }
}

/// A broker with a node id and no durable storage runs Raft with no group:
/// the control plane gives it only in-memory stream shards.
#[test]
fn a_registering_broker_without_durable_storage_runs_raft() {
    let diskless = RaftPrerequisites {
        durable_storage: false,
        ..ALL
    };
    assert_eq!(diskless.missing(), None);
    assert_eq!(diskless.resolve().expect("resolve"), ShardEngine::Raft);
}

/// A broker that registers, or keeps durable shards, runs Raft or does not
/// start: there is no hybrid to fall back on. The refusal names what Raft
/// needs.
#[test]
fn a_broker_that_cannot_run_raft_is_refused() {
    let no_listener = RaftPrerequisites {
        peer_transport: false,
        ..ALL
    };
    let no_feature = RaftPrerequisites {
        compiled: false,
        ..ALL
    };
    for (broker, needs) in [
        (no_listener, "FELIX_INTERNAL_BIND"),
        (no_feature, "cargo feature `raft`"),
    ] {
        let text = broker.resolve().expect_err("refused").to_string();
        assert!(
            text.contains("FELIX_NODE_ID") && text.contains(needs),
            "{text}"
        );
    }
    let standalone_without_feature = RaftPrerequisites {
        compiled: false,
        membership: false,
        peer_transport: false,
        durable_storage: true,
    };
    let text = standalone_without_feature
        .resolve()
        .expect_err("refused")
        .to_string();
    assert!(text.contains("cargo feature `raft`"), "{text}");
}

#[test]
fn the_raft_lease_is_the_default_unless_set() {
    for unset in [None, Some(""), Some("  ")] {
        assert_eq!(parse_shard_raft_lease(unset).expect("unset"), None);
    }
    let off = parse_shard_raft_lease(Some("0")).expect("zero");
    assert_eq!(off, Some(Duration::ZERO));
    let set = parse_shard_raft_lease(Some(" 750 ")).expect("set");
    assert_eq!(set, Some(Duration::from_millis(750)));
}

/// Read as the default, a mistyped `0` would leave the lease on where the
/// operator turned it off.
#[test]
fn a_raft_lease_that_is_not_a_number_is_refused() {
    for bad in ["off", "1.5s", "-1"] {
        let err = parse_shard_raft_lease(Some(bad)).expect_err("not a number");
        assert!(
            format!("{err}").contains("FELIX_SHARD_RAFT_LEASE_MS"),
            "{err}"
        );
    }
}

/// Unset keeps the runtime's bound; `0` takes it off; anything that is not
/// a whole number is refused rather than read as the default.
#[test]
fn the_raft_election_bound_is_the_default_unless_set() {
    for unset in [None, Some(""), Some("  ")] {
        let parsed = parse_shard_raft_elections_at_once(unset).expect("unset");
        assert_eq!(parsed, None);
    }
    let off = parse_shard_raft_elections_at_once(Some("0")).expect("zero");
    assert_eq!(off, Some(0));
    let set = parse_shard_raft_elections_at_once(Some(" 16 ")).expect("set");
    assert_eq!(set, Some(16));
    for bad in ["off", "1.5", "-1"] {
        let err = parse_shard_raft_elections_at_once(Some(bad)).expect_err("not a number");
        let err = format!("{err}");
        assert!(err.contains("FELIX_SHARD_RAFT_ELECTIONS_AT_ONCE"), "{err}");
    }
}

/// Unset keeps the runtime's interval. `0`, which the runtime reads as
/// never snapshotting, and anything not a whole number are refused.
#[test]
fn the_raft_cache_snapshot_interval_is_the_default_unless_set() {
    for unset in [None, Some(""), Some("  ")] {
        let parsed = parse_shard_raft_cache_snapshot_every(unset).expect("unset");
        assert_eq!(parsed, None);
    }
    let set = parse_shard_raft_cache_snapshot_every(Some(" 32 ")).expect("set");
    assert_eq!(set, Some(32));
    for bad in ["0", "off", "1.5", "-1"] {
        let err = parse_shard_raft_cache_snapshot_every(Some(bad)).expect_err("refused");
        let err = format!("{err}");
        assert!(
            err.contains("FELIX_SHARD_RAFT_CACHE_SNAPSHOT_EVERY"),
            "{err}"
        );
    }
}

#[test]
fn raft_group_threads_are_derived_unless_set() {
    assert_eq!(parse_raft_group_threads(None).expect("unset"), None);
    assert_eq!(parse_raft_group_threads(Some(" 4 ")).expect("set"), Some(4));
    let err = parse_raft_group_threads(Some("auto")).expect_err("not a number");
    assert!(
        format!("{err}").contains("FELIX_TEST_RAFT_GROUP_THREADS"),
        "{err}"
    );
}
