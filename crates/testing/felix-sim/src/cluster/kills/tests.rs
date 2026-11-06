use super::*;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// One shard led by `leader` at `generation`, with these members.
fn view(leader: &str, generation: u64, members: &[&str]) -> Views {
    BTreeMap::from([(
        "stream/0".to_string(),
        ShardView {
            leader: leader.to_string(),
            generation,
            members: members.iter().map(|m| m.to_string()).collect(),
        },
    )])
}

fn held(log: &KillLog) -> Vec<String> {
    log.held().into_iter().collect()
}

const THREE: [&str; 3] = ["b1", "b2", "b3"];

#[test]
fn a_follower_then_the_leader_killed_in_a_set_of_three_hold_the_shard() {
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.kill("b1", secs(15), view("b1", 1, &THREE));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn the_leader_alone_or_two_followers_do_not() {
    let mut log = KillLog::default();
    log.kill("b1", secs(10), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());

    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.kill("b3", secs(12), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn disjoint_windows_do_not() {
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.restart("b2", secs(14));
    log.kill("b1", secs(15), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn a_follower_killed_after_the_leader_does_not() {
    let mut log = KillLog::default();
    log.kill("b1", secs(10), view("b1", 1, &THREE));
    log.kill("b2", secs(15), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn leadership_moving_between_the_kills_does_not() {
    // b2 dies under b1; b3 takes over at generation 2; then b3 dies.
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.kill("b3", secs(20), view("b3", 2, &THREE));
    assert!(held(&log).is_empty());
    // Same leader again, but at a newer generation.
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.kill("b1", secs(20), view("b1", 2, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn a_set_of_five_needs_three_uncounted_copies() {
    let five = ["b1", "b2", "b3", "b4", "b5"];
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &five));
    log.kill("b1", secs(15), view("b1", 1, &five));
    assert!(held(&log).is_empty(), "two of five are not a majority");
    log.kill("b3", secs(12), view("b1", 1, &five));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn a_broker_outside_the_set_does_not_count() {
    let mut log = KillLog::default();
    log.kill("b4", secs(10), view("b1", 1, &THREE));
    log.kill("b1", secs(15), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn a_power_loss_the_restart_doubted_counts_like_a_kill() {
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.doubted("b1", secs(15), secs(20), view("b1", 1, &THREE));
    assert_eq!(held(&log), ["stream/0"]);

    // Doubted only after b2 was back: the windows do not overlap.
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.restart("b2", secs(12));
    log.doubted("b1", secs(15), secs(20), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}
