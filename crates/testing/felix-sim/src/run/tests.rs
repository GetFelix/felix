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
