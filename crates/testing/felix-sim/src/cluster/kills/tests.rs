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

#[test]
fn the_leader_counts_in_the_set_when_the_view_leaves_it_out() {
    // The control plane's replicas never name the leader.
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &["b2", "b3"]));
    log.kill("b1", secs(15), view("b1", 1, &["b2", "b3"]));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn a_seat_replacement_in_flight_needs_a_majority_of_the_wider_set() {
    // b4 is joining in place of b3: four copies, so two doubted are not enough.
    let four = ["b2", "b3", "b4"];
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &four));
    log.kill("b1", secs(15), view("b1", 1, &four));
    assert!(held(&log).is_empty());
    log.kill("b3", secs(12), view("b1", 1, &four));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn abandoning_the_log_ends_the_hold_until_the_next_kills() {
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b1", 1, &THREE));
    log.kill("b1", secs(15), view("b1", 1, &THREE));
    log.abandon("stream/0", secs(20));
    assert!(held(&log).is_empty());

    log.restart("b1", secs(25));
    log.restart("b2", secs(25));
    log.kill("b3", secs(30), view("b2", 3, &THREE));
    log.kill("b2", secs(35), view("b2", 3, &THREE));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn a_leader_killed_before_it_opened_does_not_hold_the_shard() {
    // b2 dies under b1's move to b3; b3 is named at generation 2 and killed
    // before it opens, so nothing was acknowledged at generation 2.
    let mut log = KillLog::default();
    log.kill("b2", secs(10), view("b3", 2, &THREE));
    let mut died_under = view("b3", 2, &THREE);
    leave_out_unopened(
        &mut died_under,
        "b3",
        &BTreeSet::from(["stream/0".to_string()]),
    );
    log.kill("b3", secs(12), died_under);
    assert!(held(&log).is_empty());
}

#[test]
fn an_opened_leader_or_another_shard_stays_in_the_view() {
    let mut views = view("b1", 1, &THREE);
    leave_out_unopened(&mut views, "b1", &BTreeSet::new());
    assert_eq!(views.len(), 1);
    leave_out_unopened(&mut views, "b2", &BTreeSet::from(["stream/0".to_string()]));
    assert_eq!(views.len(), 1);
}

fn opened(log: &mut KillLog, leader: &str, generation: u64) {
    let v = view(leader, generation, &THREE);
    log.opened("stream/0", &v["stream/0"], secs(1));
}

#[test]
fn a_majority_of_doubted_copies_in_the_opened_set_is_held() {
    // b1 opened; b1 and b2 were killed and nothing opened since. b2 comes
    // back but no opened leader has shipped to it.
    let mut log = KillLog::default();
    opened(&mut log, "b1", 1);
    log.kill("b1", secs(5), view("b1", 1, &THREE));
    log.kill("b2", secs(20), view("b1", 2, &THREE));
    log.restart("b2", secs(30));
    assert_eq!(held(&log), ["stream/0"]);
}

#[test]
fn a_shipped_copy_is_trusted_so_a_stuck_fence_is_not_held() {
    // The J shape: b2 restarted and b1 shipped to it, so only b1 is doubted
    // once it is killed. The shard is stuck, not held.
    let mut log = KillLog::default();
    opened(&mut log, "b1", 1);
    log.kill("b2", secs(5), view("b1", 1, &THREE));
    log.restart("b2", secs(10));
    log.shipped("stream/0", "b2", secs(12));
    log.kill("b1", secs(20), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn a_single_doubted_copy_is_not_held() {
    let mut log = KillLog::default();
    opened(&mut log, "b1", 1);
    log.kill("b3", secs(5), view("b1", 1, &THREE));
    assert!(held(&log).is_empty());
}

#[test]
fn a_newer_open_starts_the_set_over() {
    let mut log = KillLog::default();
    opened(&mut log, "b1", 1);
    log.kill("b1", secs(5), view("b1", 1, &THREE));
    log.kill("b2", secs(6), view("b1", 1, &THREE));
    assert_eq!(held(&log), ["stream/0"]);
    let v = view("b3", 2, &THREE);
    log.opened("stream/0", &v["stream/0"], secs(40));
    assert!(held(&log).is_empty());
}
