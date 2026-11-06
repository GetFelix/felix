use super::*;

fn small() -> SimConfig {
    SimConfig {
        brokers: 3,
        shards: 6,
        cache_shards: 2,
        clients: 3,
        cache_keys: 4,
        run_for: Duration::from_secs(30),
        fault_window: Duration::from_secs(15),
        settle_within: Duration::from_secs(20),
        faults: false,
        ..SimConfig::for_seed(1)
    }
}

#[test]
fn a_run_without_faults_passes() {
    let outcome = run_seed(1, small());
    println!("{outcome}");
    assert!(outcome.passed());
    assert!(outcome.history.acknowledged() > 20, "{}", outcome.summary);
}

/// A key the search could not decide is neither proven nor refuted, so the
/// seed does not pass.
#[test]
fn an_undecided_cache_key_fails_the_seed() {
    let mut outcome = failed_start(1, small(), String::new(), Duration::ZERO);
    outcome.findings.clear();
    assert!(outcome.passed());
    outcome
        .report
        .undecided
        .push("cache key k: not decided after 20000000 steps".into());
    assert!(!outcome.passed());
    assert!(outcome.to_string().contains("cache key k"), "{outcome}");
}
