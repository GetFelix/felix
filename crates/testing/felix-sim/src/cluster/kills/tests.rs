use super::*;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

/// One shard led by `leader`, with these members.
fn view(leader: &str, members: &[&str]) -> Views {
    BTreeMap::from([(
        "stream/0".to_string(),
        ShardView {
            leader: leader.to_string(),
            generation: 1,
            members: members.iter().map(|m| m.to_string()).collect(),
            joining: BTreeSet::new(),
        },
    )])
}

/// `stream/0`'s copies on these nodes, up and trusted or not.
fn trust(copies: &[(&str, bool)]) -> Trust {
    copies
        .iter()
        .map(|(node, trusted)| {
            let copy = if *trusted {
                Copy::Trusted
            } else {
                Copy::Untrusted
            };
            (("stream/0".to_string(), node.to_string()), copy)
        })
        .collect()
}

fn held(log: &KillLog, views: &Views, trust: &Trust) -> Vec<String> {
    log.held(views, trust).into_iter().collect()
}

/// A log that saw every broker start unclean, so an untrusted copy has the
/// injected fault that explains it.
fn restarted_unclean() -> KillLog {
    let mut log = KillLog::default();
    for node in ["b1", "b2", "b3", "b4"] {
        log.started_unclean(node, secs(1));
    }
    log
}

const THREE: [&str; 3] = ["b1", "b2", "b3"];

#[test]
fn a_leader_and_a_follower_killed_later_in_its_term_hold_the_shard() {
    // b1 led and was killed, then b3: both come back untrusted, whichever
    // died first.
    let log = restarted_unclean();
    let copies = trust(&[("b1", false), ("b2", true), ("b3", false)]);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn a_learner_awaiting_readmission_counts_as_untrusted() {
    // b3 restarted unclean and is still a learner when its leader b2 is
    // killed: two of three copies cannot cover.
    let log = restarted_unclean();
    let copies = trust(&[("b1", true), ("b2", false), ("b3", false)]);
    assert_eq!(held(&log, &view("b2", &THREE), &copies), ["stream/0"]);
}

#[test]
fn an_unserved_shard_with_trusted_copies_is_still_a_stall() {
    // An elected leader that cannot open (an empty storage id in its set, a
    // joint fence short of the old set) leaves the copies trusted.
    let log = restarted_unclean();
    let copies = trust(&[("b1", true), ("b2", true), ("b3", false)]);
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    assert!(held(&log, &view("b1", &THREE), &Trust::new()).is_empty());
}

#[test]
fn the_leader_counts_in_the_set_when_the_view_leaves_it_out() {
    // The control plane's replicas never name the leader.
    let log = restarted_unclean();
    let copies = trust(&[("b1", false), ("b2", false), ("b3", true)]);
    assert_eq!(
        held(&log, &view("b1", &["b2", "b3"]), &copies),
        ["stream/0"]
    );
}

#[test]
fn a_seat_replacement_in_flight_needs_a_majority_of_the_wider_set() {
    // b4 is joining: four copies, so two untrusted are not enough.
    let log = restarted_unclean();
    let four = ["b2", "b3", "b4"];
    let copies = trust(&[("b1", false), ("b2", false), ("b3", true), ("b4", true)]);
    assert!(held(&log, &view("b1", &four), &copies).is_empty());
    let copies = trust(&[("b1", false), ("b2", false), ("b3", false), ("b4", true)]);
    assert_eq!(held(&log, &view("b1", &four), &copies), ["stream/0"]);
}

#[test]
fn a_broker_outside_the_set_does_not_count() {
    let log = restarted_unclean();
    let copies = trust(&[("b1", false), ("b4", false), ("b2", true), ("b3", true)]);
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
}

#[test]
fn abandoning_the_log_ends_the_hold_until_the_next_unclean_stop() {
    let mut log = restarted_unclean();
    let copies = trust(&[("b1", false), ("b2", false), ("b3", true)]);
    log.kill("b2", secs(10));
    log.kill("b1", secs(15));
    log.abandon("stream/0", secs(20));
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    log.kill("b4", secs(30));
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    log.kill("b2", secs(35));
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn a_copy_up_while_a_leader_served_and_still_untrusted_is_a_stall() {
    // b2 and b3 restarted unclean, then b1 served beside them before it was
    // killed: a leader had the chance to vouch both, so their doubt is the
    // broker's to clear, not a reason to hold the shard.
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.started_unclean("b3", secs(12));
    for at in [20, 21, 22, 23, 24, 25, 26] {
        log.served("stream/0", "b2", secs(at), Some(Duration::ZERO), false);
        log.served("stream/0", "b3", secs(at), Some(Duration::ZERO), false);
    }
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::DownAfterKill);
    let views = view("b1", &THREE);
    assert!(held(&log, &views, &copies).is_empty());
    assert_eq!(
        log.vouchable_untrusted(&views, &copies)
            .into_iter()
            .collect::<Vec<_>>(),
        ["stream/0"]
    );
    // Served only before b3's latest unclean start: b3 had no leader since.
    log.started_unclean("b3", secs(30));
    assert_eq!(held(&log, &views, &copies), ["stream/0"]);
}

#[test]
fn a_copy_down_after_a_kill_counts_untrusted_though_a_leader_served_it_before() {
    // b2 restarted unclean, a leader served beside it, then b2 and the
    // leader b1 were killed. b2's next start distrusts the copy anyway.
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.served("stream/0", "b1", secs(20), Some(Duration::ZERO), false);
    log.served("stream/0", "b2", secs(20), Some(Duration::ZERO), false);
    log.kill("b2", secs(25));
    log.kill("b1", secs(30));
    let copies = Trust::from([
        (("stream/0".into(), "b1".into()), Copy::DownAfterKill),
        (("stream/0".into(), "b2".into()), Copy::DownAfterKill),
        (("stream/0".into(), "b3".into()), Copy::Trusted),
    ]);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn an_untrusted_copy_with_no_injected_fault_behind_it_is_a_stall() {
    // Nothing the sim injected explains b2 and b3 being untrusted, as a
    // broker that lost a vouch would leave them: the shard must fail.
    let log = KillLog::default();
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::Trusted);
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
}

#[test]
fn a_copy_a_leader_could_not_reach_is_held_and_explained() {
    // b2 and b3 restarted unclean while b1 served only with the control
    // plane paused or over a partitioned link, so it never had the chance to
    // vouch them. Nothing is recorded as served.
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.started_unclean("b3", secs(12));
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::Trusted);
    let views = view("b1", &THREE);
    assert_eq!(held(&log, &views, &copies), ["stream/0"]);
    let why = log.explain("stream/0", &views["stream/0"], &copies);
    assert_eq!(why.len(), 2, "{why:?}");
    assert!(why[0].starts_with("b2 restarted unclean at 10"), "{why:?}");
}

#[test]
fn a_blip_of_service_beside_a_copy_is_not_a_chance_to_vouch() {
    // b1 served beside b2 and b3 for a second: too short for a vouch.
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.started_unclean("b3", secs(12));
    log.served("stream/0", "b2", secs(20), Some(Duration::ZERO), false);
    log.served("stream/0", "b3", secs(20), Some(Duration::ZERO), false);
    log.served("stream/0", "b2", secs(21), Some(Duration::ZERO), false);
    log.served("stream/0", "b3", secs(21), Some(Duration::ZERO), false);
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::Trusted);
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

/// `view` with these members still joining.
fn learners(mut views: Views, joining: &[&str]) -> Views {
    for view in views.values_mut() {
        view.joining = joining.iter().map(|m| m.to_string()).collect();
    }
    views
}

/// b1 trusted, b2 and b3 restarted unclean at 10 s and 12 s.
fn two_restarted() -> (KillLog, Trust) {
    let mut log = KillLog::default();
    log.started_unclean("b2", secs(10));
    log.started_unclean("b3", secs(12));
    let mut copies = trust(&[("b2", false), ("b3", false)]);
    copies.insert(("stream/0".into(), "b1".into()), Copy::Trusted);
    (log, copies)
}

#[test]
fn a_copy_vouched_then_untrusted_with_no_new_unclean_start_is_a_stall() {
    // b2 was seen trusted after its start, so its later doubt has no
    // injected fault behind it and does not count toward a hold.
    let (mut log, copies) = two_restarted();
    log.trusted("stream/0", "b2");
    assert!(held(&log, &view("b1", &THREE), &copies).is_empty());
    // A new unclean start explains it again.
    log.started_unclean("b2", secs(40));
    assert_eq!(held(&log, &view("b1", &THREE), &copies), ["stream/0"]);
}

#[test]
fn a_voter_a_leader_reached_over_a_link_is_vouchable_whatever_the_control_plane_does() {
    // Shipping vouches a voter without the control plane, so a serving
    // leader on a good link that never vouched it is a stall.
    let (mut log, copies) = two_restarted();
    for at in 20..24 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), None, false);
        }
    }
    let views = view("b1", &THREE);
    assert!(held(&log, &views, &copies).is_empty());
    assert_eq!(log.vouchable_untrusted(&views, &copies).len(), 1);
}

#[test]
fn a_learner_is_not_readmittable_while_the_control_plane_answers_nothing() {
    let (mut log, copies) = two_restarted();
    for at in 20..60 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), None, true);
        }
    }
    let views = learners(view("b1", &THREE), &["b2", "b3"]);
    assert_eq!(held(&log, &views, &copies), ["stream/0"]);
}

#[test]
fn a_learner_needs_a_run_as_long_as_reconcile_and_three_slow_calls() {
    let (mut log, copies) = two_restarted();
    let views = learners(view("b1", &THREE), &["b2", "b3"]);
    let delay = Some(Duration::from_secs(4));
    // Reconcile is 5 s by default: 5 + 2.5 + 12 = 19.5 s.
    for at in 20..38 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), delay, true);
        }
    }
    assert_eq!(held(&log, &views, &copies), ["stream/0"]);
    for at in 38..=40 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), delay, true);
        }
    }
    assert!(held(&log, &views, &copies).is_empty());
    assert_eq!(log.vouchable_untrusted(&views, &copies).len(), 1);
}

#[test]
fn a_leader_reaches_a_copy_unless_a_partition_cuts_the_link() {
    let up = |from: usize, to: usize| !(from == 0 && to == 2);
    assert!(reaches(&[0], 1, &up));
    assert!(!reaches(&[0], 2, &up), "cut off");
    assert!(reaches(&[0], 0, &up), "a leader reaches its own copy");
    assert!(
        reaches(&[0, 1], 2, &up),
        "another serving broker reaches it"
    );
    assert!(!reaches(&[], 1, &up), "nobody serves");
}

#[test]
fn only_a_paused_or_down_control_plane_answers_nothing() {
    use crate::controlplane::CpFault;
    assert_eq!(cp_delay(CpFault::Healthy), Some(Duration::ZERO));
    assert_eq!(cp_delay(CpFault::Slow(secs(2))), Some(secs(2)));
    assert_eq!(cp_delay(CpFault::Paused), None);
    assert_eq!(cp_delay(CpFault::Down), None);
}

#[test]
fn a_leader_a_copy_accepted_a_newer_one_over_is_no_chance_to_vouch() {
    // b2 serves at generation 15; this copy accepted 21.
    assert!(unsuperseded(&[(1, 15)], Some(21)).is_empty());
    assert_eq!(unsuperseded(&[(1, 15), (0, 21)], Some(21)), [0]);
}

#[test]
fn a_newer_generation_the_copy_never_accepted_leaves_the_old_leader_a_chance() {
    // The control plane routes 21 to b1, which never fenced this copy: the
    // copy still accepts b2 at 15, so b2 could vouch it and an untrusted
    // copy is a stall, not a hold.
    let servers = [(1, 15)];
    for accepted in [Some(15), Some(3), None] {
        assert_eq!(unsuperseded(&servers, accepted), [1]);
    }
    let (mut log, copies) = two_restarted();
    for at in 20..24 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), Some(Duration::ZERO), false);
        }
    }
    let views = view("b1", &THREE);
    assert!(held(&log, &views, &copies).is_empty());
    assert_eq!(log.vouchable_untrusted(&views, &copies).len(), 1);
}

#[test]
fn a_run_is_judged_by_the_role_the_copy_had_then() {
    // b2 and b3 shipped for 6 s as voters, long enough to vouch a voter,
    // then became learners. The runs happened as voters and still count.
    let (mut log, copies) = two_restarted();
    for at in 20..27 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), Some(Duration::ZERO), false);
        }
    }
    let views = learners(view("b1", &THREE), &["b2", "b3"]);
    assert!(held(&log, &views, &copies).is_empty());
    // The other way: learner samples too short for readmission do not
    // become a voter's shipping window when the copy is promoted.
    let (mut log, copies) = two_restarted();
    for at in 20..27 {
        for node in ["b2", "b3"] {
            log.served("stream/0", node, secs(at), Some(Duration::ZERO), true);
        }
    }
    let views = view("b1", &THREE);
    assert_eq!(held(&log, &views, &copies), ["stream/0"]);
}
