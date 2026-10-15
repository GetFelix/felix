//! The rules a shared cursor has to keep, each one exercised on its own.
use super::*;

const VIS: Duration = Duration::from_secs(30);
/// High enough that the cases below never reach it. The bound has its own tests.
const MANY: u32 = 1_000;

fn at(base: Instant, secs: u64) -> Instant {
    base + Duration::from_secs(secs)
}

#[test]
fn a_new_group_hands_out_from_its_committed_position() {
    let now = Instant::now();
    let mut group = GroupTracker::new(5, MANY);

    assert_eq!(group.claim(9, 10, now, VIS).offsets, vec![5, 6, 7, 8]);
    assert_eq!(group.committed(), 5, "handing out settles nothing");
}

#[test]
fn nothing_at_or_above_the_tail_is_handed_out() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    assert_eq!(group.claim(0, 10, now, VIS).offsets, Vec::<u64>::new());
    assert_eq!(group.claim(2, 10, now, VIS).offsets, vec![0, 1]);
}

/// **The queue property.** A record handed to one consumer is not handed to
/// another while the first still holds it.
#[test]
fn an_offset_in_flight_is_not_handed_out_again() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    let first = group.claim(4, 2, now, VIS).offsets;
    let second = group.claim(4, 2, now, VIS).offsets;

    assert_eq!(first, vec![0, 1]);
    assert_eq!(second, vec![2, 3], "a second consumer got the same records");
    assert_eq!(group.claim(4, 10, now, VIS).offsets, Vec::<u64>::new());
}

/// **The other half of it.** A consumer that stops answering must not hold a
/// record for ever, or the group stops making progress at that offset.
#[test]
fn a_lapsed_claim_is_handed_out_again() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    assert_eq!(group.claim(3, 10, base, VIS).offsets, vec![0, 1, 2]);
    // Still held while the claim stands.
    assert_eq!(
        group.claim(3, 10, at(base, 29), VIS).offsets,
        Vec::<u64>::new()
    );
    // And owed again once it lapses.
    assert_eq!(group.claim(3, 10, at(base, 31), VIS).offsets, vec![0, 1, 2]);
}

#[test]
fn an_acknowledged_offset_is_never_handed_out_again() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(3, 10, base, VIS);
    group.ack(0);
    group.ack(1);
    group.ack(2);

    assert_eq!(
        group.claim(3, 10, at(base, 300), VIS).offsets,
        Vec::<u64>::new()
    );
    assert_eq!(group.committed(), 3);
}

/// The cursor may only move over a contiguous run. Advancing past a gap would
/// mark a record finished that nobody has finished, and it would never be
/// handed out again.
#[test]
fn the_cursor_does_not_advance_over_a_gap() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(4, 10, now, VIS);

    assert_eq!(group.ack(1), None, "1 is acked but 0 is not");
    assert_eq!(group.ack(3), None);
    assert_eq!(group.committed(), 0);

    // Closing the run releases everything behind it at once.
    assert_eq!(group.ack(0), Some(2));
    assert_eq!(group.ack(2), Some(4));
}

/// A record acknowledged out of order is still finished — it must not come back
/// when the offsets below it are settled.
#[test]
fn an_out_of_order_ack_is_not_redelivered_when_the_gap_closes() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(3, 10, base, VIS);

    group.ack(2);
    // 0 and 1 lapse and come back; 2 must not.
    assert_eq!(group.claim(3, 10, at(base, 31), VIS).offsets, vec![0, 1]);
}

#[test]
fn a_nack_makes_a_record_owed_at_once() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(3, 10, now, VIS);

    group.nack(1);

    // Immediately, without waiting out the visibility timeout.
    assert_eq!(group.claim(3, 10, now, VIS).offsets, vec![1]);
}

#[test]
fn a_nack_after_an_ack_does_not_resurrect_the_record() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(2, 10, now, VIS);

    group.ack(1);
    group.nack(1);

    assert_eq!(group.claim(2, 10, now, VIS).offsets, Vec::<u64>::new());
}

/// Owed records go out before new ones. A group that preferred new work would
/// starve the redeliveries behind a fast producer — and those are exactly the
/// records a consumer already failed to finish once.
#[test]
fn owed_records_go_out_before_new_ones() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(2, 10, base, VIS);
    group.nack(0);
    group.nack(1);

    // The log has grown, but the owed records go first.
    assert_eq!(group.claim(10, 3, base, VIS).offsets, vec![0, 1, 2]);
}

/// A late acknowledgement, from a consumer whose claim lapsed and whose record
/// has since gone to someone else, is harmless. Both consumers answer; the
/// record is finished once.
#[test]
fn a_late_ack_after_redelivery_is_harmless() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(1, 10, base, VIS);
    let redelivered = group.claim(1, 10, at(base, 31), VIS).offsets;
    assert_eq!(redelivered, vec![0]);

    // The original consumer finally answers.
    assert_eq!(group.ack(0), Some(1));
    // And so does the second. Neither moves the cursor twice.
    assert_eq!(group.ack(0), None);
    assert_eq!(group.committed(), 1);
}

/// A claim can lapse before its consumer answers, leaving the record *owed*
/// rather than in flight. An acknowledgement arriving in that window still
/// finishes it: handing it out again would deliver a record someone has already
/// completed, which is the duplicate the visibility timeout is meant to bound,
/// not create.
#[test]
fn an_ack_while_a_record_is_owed_still_finishes_it() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(2, 10, base, VIS);
    // The claim lapses, so 0 and 1 are owed but not yet re-claimed.
    group.expire(at(base, 31));

    assert_eq!(group.ack(0), Some(1));
    assert_eq!(
        group.claim(2, 10, at(base, 31), VIS).offsets,
        vec![1],
        "an acknowledged record was handed out again",
    );
}

/// The same, reached by a nack rather than a lapse.
#[test]
fn an_ack_after_a_nack_finishes_the_record() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(2, 10, now, VIS);
    group.nack(0);
    assert_eq!(group.ack(0), Some(1));

    assert_eq!(group.claim(2, 10, now, VIS).offsets, Vec::<u64>::new());
}

#[test]
fn acking_below_the_cursor_is_ignored() {
    let now = Instant::now();
    let mut group = GroupTracker::new(10, MANY);

    assert_eq!(group.ack(3), None);
    assert_eq!(group.committed(), 10);
    assert_eq!(group.claim(12, 10, now, VIS).offsets, vec![10, 11]);
}

/// An acknowledgement for an offset that was never handed out.
///
/// Nothing in the broker produces one, but the tracker takes its input from a
/// client and must not be wrecked by it. If the cursor can move past offsets
/// that were never claimed, the next claim starts *below* the cursor and hands
/// out records the group has already finished.
#[test]
fn an_ack_for_an_unclaimed_offset_does_not_rewind_the_next_claim() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    for offset in 0..5 {
        group.ack(offset);
    }
    assert_eq!(group.committed(), 5);

    let claimed = group.claim(8, 10, now, VIS).offsets;
    assert_eq!(
        claimed,
        vec![5, 6, 7],
        "the group handed out records below its own cursor",
    );
}

/// Nothing is lost or duplicated across a long run of claims, lapses, nacks and
/// acknowledgements: every offset ends up finished exactly once.
#[test]
fn every_offset_is_finished_exactly_once() {
    const TAIL: u64 = 200;
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    let mut finished: Vec<u64> = Vec::new();
    let mut clock = 0u64;

    while group.committed() < TAIL {
        clock += 7;
        let batch = group.claim(TAIL, 5, at(base, clock), VIS).offsets;
        assert!(!batch.is_empty(), "the group stopped making progress");
        for (i, offset) in batch.into_iter().enumerate() {
            match i % 3 {
                // Acknowledged.
                0 => {
                    group.ack(offset);
                    finished.push(offset);
                }
                // Handed back, to be redelivered.
                1 => group.nack(offset),
                // Abandoned: the claim will lapse.
                _ => {}
            }
        }
        // Let some claims lapse.
        clock += 31;
        group.expire(at(base, clock));
    }

    assert_eq!(group.committed(), TAIL);
    finished.sort_unstable();
    finished.dedup();
    assert_eq!(
        finished.len() as u64,
        TAIL,
        "some offset was never acknowledged, yet the cursor passed it",
    );
}

// --- Giving up on a record ---------------------------------------------------

/// **The poison-record bound.** Without it a record that always fails is handed
/// out for ever and the group never gets past it.
#[test]
fn a_record_is_given_up_on_after_the_attempt_bound() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 3);

    // Three deliveries, each abandoned.
    for round in 0..3 {
        let claim = group.claim(1, 10, at(base, round * 31), VIS);
        assert_eq!(claim.offsets, vec![0], "round {round}");
        assert!(claim.dead_lettered.is_empty(), "round {round}");
    }

    // The fourth attempt is not made.
    let claim = group.claim(1, 10, at(base, 4 * 31), VIS);
    assert!(claim.offsets.is_empty(), "a fourth delivery was attempted");
    assert_eq!(
        claim.dead_lettered,
        vec![DeadLettered {
            offset: 0,
            attempts: 3
        }],
    );
}

/// Giving up is reported, not settled here: the caller records the dead letter
/// first and settles it after, so a crash between the two cannot move the
/// cursor past a record with nothing saying it was ever tried.
#[test]
fn giving_up_does_not_move_the_cursor_by_itself() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    group.claim(1, 10, base, VIS);
    let claim = group.claim(1, 10, at(base, 31), VIS);

    assert_eq!(claim.dead_lettered.len(), 1);
    assert_eq!(group.committed(), 0, "the tracker settled it on its own");
}

/// The bound counts deliveries, not failures of a particular kind: a nack
/// counts the same as a claim that lapsed.
#[test]
fn nacks_count_towards_the_bound() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, 2);

    assert_eq!(group.claim(1, 10, now, VIS).offsets, vec![0]);
    group.nack(0);
    assert_eq!(group.claim(1, 10, now, VIS).offsets, vec![0]);
    group.nack(0);

    let claim = group.claim(1, 10, now, VIS);
    assert!(claim.offsets.is_empty());
    assert_eq!(claim.dead_lettered.len(), 1);
}

/// A record that succeeds does not carry its attempts forward: the count is
/// about the record in play, not the offset for ever.
#[test]
fn acknowledging_clears_the_attempt_count() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 2);

    group.claim(2, 10, base, VIS);
    assert_eq!(group.attempts(0), 1);
    group.ack(0);
    assert_eq!(group.attempts(0), 0, "a finished record kept its count");
}

/// Attempts are reported so a consumer can tell a retry from a first delivery
/// and act differently on it.
#[test]
fn the_attempt_count_rises_with_each_delivery() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 10);

    group.claim(1, 10, base, VIS);
    assert_eq!(group.attempts(0), 1);
    group.claim(1, 10, at(base, 31), VIS);
    assert_eq!(group.attempts(0), 2);
}

/// A bound of zero would give up before delivering anything. Clamped to one, so
/// every record is tried at least once.
#[test]
fn a_bound_of_zero_still_delivers_once() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, 0);

    assert_eq!(group.claim(1, 10, now, VIS).offsets, vec![0]);
}

/// A poison record does not stop the group: the records behind it still flow.
#[test]
fn the_group_makes_progress_past_a_poison_record() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    // Take 0 and 1; abandon both so they are owed.
    group.claim(2, 10, base, VIS);
    let claim = group.claim(2, 10, at(base, 31), VIS);
    assert!(claim.offsets.is_empty());
    assert_eq!(claim.dead_lettered.len(), 2);

    // Settle them the way the caller does, and the group moves on.
    for dead in claim.dead_lettered {
        group.ack(dead.offset);
    }
    assert_eq!(group.committed(), 2);
    assert_eq!(group.claim(4, 10, at(base, 31), VIS).offsets, vec![2, 3]);
}

// --- Putting a dead letter back ----------------------------------------------

/// A redriven record is handed out again, without the cursor moving backwards.
/// Everything the group finished since stays finished.
#[test]
fn a_redriven_record_is_handed_out_again() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    // 0 and 1 are given up on; the group moves to 2.
    group.claim(3, 10, base, VIS);
    let claim = group.claim(3, 10, at(base, 31), VIS);
    for dead in &claim.dead_lettered {
        group.ack(dead.offset);
    }
    group.ack(2);
    assert_eq!(group.committed(), 3);

    assert!(group.redrive(0));
    let again = group.claim(3, 10, at(base, 62), VIS);

    assert_eq!(
        again.offsets,
        vec![0],
        "the redriven record was not reissued"
    );
    assert_eq!(
        group.committed(),
        3,
        "redriving rewound the cursor and will repeat finished work",
    );
}

/// Its attempt count starts over, or a record redriven after a fix would be
/// given up on again immediately.
#[test]
fn a_redriven_record_gets_its_attempts_back() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    group.claim(1, 10, base, VIS);
    let claim = group.claim(1, 10, at(base, 31), VIS);
    group.ack(claim.dead_lettered[0].offset);

    assert!(group.redrive(0));
    let again = group.claim(1, 10, at(base, 62), VIS);
    assert_eq!(again.offsets, vec![0]);
    assert_eq!(group.attempts(0), 1, "the count did not start over");
}

/// Finishing a redriven record settles it without disturbing the cursor, which
/// was never waiting on it.
#[test]
fn finishing_a_redriven_record_leaves_the_cursor_alone() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    group.claim(1, 10, base, VIS);
    let claim = group.claim(1, 10, at(base, 31), VIS);
    group.ack(claim.dead_lettered[0].offset);
    assert_eq!(group.committed(), 1);

    group.redrive(0);
    group.claim(1, 10, at(base, 62), VIS);
    assert_eq!(group.ack(0), None);
    assert_eq!(group.committed(), 1);
    // And it is not owed any more.
    assert!(group.claim(1, 10, at(base, 93), VIS).offsets.is_empty());
}

/// A record still in play has not been given up on. Redriving it would reset
/// its attempts and let it evade the bound for ever.
#[test]
fn a_record_at_or_above_the_cursor_cannot_be_redriven() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, 3);

    group.claim(2, 10, now, VIS);
    assert!(!group.redrive(0), "a record in play was redriven");
    assert!(!group.redrive(5), "a record never delivered was redriven");
}

/// A record given up on while a gap below it holds the cursor back is settled
/// but not yet passed. Redriving it has to make the cursor wait on it again,
/// or the gap closing would carry the cursor over a record that is owed.
#[test]
fn a_dead_letter_above_the_cursor_can_be_redriven() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    group.claim(2, 10, base, VIS);
    // 1 is given up on and settled; 0 is still in flight.
    group.nack(1);
    let claim = group.claim(2, 10, base, VIS);
    assert_eq!(claim.dead_lettered[0].offset, 1);
    group.ack(1);
    assert_eq!(group.committed(), 0);

    assert!(group.redrive(1));
    assert_eq!(group.ack(0), Some(1), "the cursor passed an owed record");
    assert_eq!(group.claim(2, 10, base, VIS).offsets, vec![1]);
}

// --- Validating what a consumer settles --------------------------------------

#[test]
fn only_offsets_handed_out_can_be_settled() {
    let now = Instant::now();
    let mut group = GroupTracker::new(5, MANY);
    group.claim(10, 2, now, VIS);

    assert!(
        group.handed_out(0),
        "below the cursor is a harmless duplicate"
    );
    assert!(group.handed_out(6));
    assert!(!group.handed_out(7), "never handed out");
    assert!(!group.handed_out(u64::MAX));
}

/// A claim given back because it never reached the consumer does not count as
/// an attempt, or a string of failed reads would dead-letter a record nobody
/// ever tried.
#[test]
fn an_unclaimed_record_keeps_its_attempts() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, 1);

    group.claim(1, 10, now, VIS);
    group.unclaim(0);
    let again = group.claim(1, 10, now, VIS);
    assert_eq!(again.offsets, vec![0], "given up on without being tried");
    assert_eq!(group.attempts(0), 1);
}

#[test]
fn one_claim_is_capped() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    let tail = MAX_CLAIM as u64 * 2;

    assert_eq!(
        group.claim(tail, usize::MAX, now, VIS).offsets.len(),
        MAX_CLAIM
    );
}

/// Claims lapse by deadline, not by offset: a later offset claimed earlier is
/// owed first, and one claimed later is left standing.
#[test]
fn claims_lapse_in_deadline_order() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(2, 2, base, VIS);
    group.nack(0);
    // 0 again, claimed ten seconds after 1.
    assert_eq!(group.claim(2, 10, at(base, 10), VIS).offsets, vec![0]);
    assert_eq!(group.next_lapse(), Some(at(base, 30)));

    assert_eq!(group.claim(2, 10, at(base, 31), VIS).offsets, vec![1]);
    assert_eq!(group.next_lapse(), Some(at(base, 40)));
    assert_eq!(group.claim(2, 10, at(base, 41), VIS).offsets, vec![0]);
}

/// Every way a claim ends takes its deadline with it. One left behind would
/// later lapse a claim that no longer exists, or one handed out since.
#[test]
fn a_settled_claim_leaves_no_deadline_behind() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);

    group.claim(4, 4, base, VIS);
    group.ack(0);
    group.nack(1);
    group.unclaim(2);
    assert_eq!(group.in_flight(), 1);
    assert_eq!(group.next_lapse(), Some(at(base, 30)));

    // 1 and 2 again, with a new deadline each.
    assert_eq!(group.claim(4, 10, at(base, 20), VIS).offsets, vec![1, 2]);
    assert_eq!(group.in_flight(), 3);
    // Only 3's claim lapses at 30; 1 and 2 stand until 50.
    assert_eq!(group.claim(4, 10, at(base, 31), VIS).offsets, vec![3]);
    assert_eq!(
        group.claim(4, 10, at(base, 49), VIS).offsets,
        Vec::<u64>::new()
    );
    // 3 was claimed again at 31, so it stands until 61.
    assert_eq!(group.claim(4, 10, at(base, 51), VIS).offsets, vec![1, 2]);
    assert_eq!(group.in_flight(), 3);
}

/// Past the cap a claim hands out nothing, and says the cap is why, until an
/// acknowledgement or a lapse frees room.
#[test]
fn claims_stop_at_the_in_flight_cap() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.set_max_in_flight(3);

    let first = group.claim(10, 10, base, VIS);
    assert_eq!(first.offsets, vec![0, 1, 2]);
    assert!(first.capped);

    let full = group.claim(10, 10, base, VIS);
    assert!(full.offsets.is_empty());
    assert!(full.capped);

    group.ack(1);
    assert_eq!(group.claim(10, 10, base, VIS).offsets, vec![3]);

    // Room freed by a lapse goes to what lapsed, which is owed first.
    assert_eq!(
        group.claim(10, 10, at(base, 31), VIS).offsets,
        vec![0, 2, 3]
    );
    assert_eq!(group.in_flight(), 3);
}

/// A claim that took everything there was is not reported as capped, even
/// when it filled the cap exactly.
#[test]
fn filling_the_cap_with_everything_available_is_not_capped() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.set_max_in_flight(3);

    let claim = group.claim(3, 10, now, VIS);
    assert_eq!(claim.offsets, vec![0, 1, 2]);
    assert!(!claim.capped);
    assert!(!group.claim(3, 10, now, VIS).capped);
}

/// `name` polling as principal `p` on `connection`.
fn member(name: &str, connection: u64, reclaim: bool) -> GroupConsumer {
    GroupConsumer::new("p", name, connection, reclaim)
}

/// **A restarted member takes back what its predecessor held**, before newer
/// records and without waiting for the claims to lapse. Records held by other
/// members are left with them.
#[test]
fn a_member_that_reclaims_gets_its_standing_claims_back_first() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(
        group
            .claim_as(10, 2, now, VIS, Some(&member("snapshotter", 1, false)))
            .offsets,
        vec![0, 1]
    );
    assert_eq!(
        group
            .claim_as(10, 1, now, VIS, Some(&member("other", 1, false)))
            .offsets,
        vec![2]
    );

    // The process restarts under the same name, on a new connection.
    let back = group.claim_as(10, 10, now, VIS, Some(&member("snapshotter", 2, true)));
    assert_eq!(back.offsets[..2], [0, 1]);
    assert!(
        !back.offsets.contains(&2),
        "another member's claim is left alone"
    );
    assert_eq!(
        group.attempts(0),
        2,
        "taking a record back is another attempt"
    );

    for offset in 0..10 {
        group.ack(offset);
    }
    assert!(
        group.members.is_empty(),
        "a member holding nothing is forgotten"
    );
}

/// Without `reclaim` a named member's poll takes new records, as any poll does.
#[test]
fn a_named_poll_without_reclaim_leaves_its_claims_standing() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    let snapshotter = member("snapshotter", 1, false);
    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&snapshotter)).offsets,
        vec![0, 1]
    );
    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&snapshotter)).offsets,
        vec![2, 3]
    );
}

/// **A name reaches only its own principal's claims.** Another principal
/// that polls under the same name takes new records and leaves the first
/// one's claims, and their attempt counts, alone.
#[test]
fn another_principal_using_the_name_cannot_reclaim() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    let alice = GroupConsumer::new("alice", "worker", 1, false);
    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&alice)).offsets,
        [0, 1]
    );

    let mallory = GroupConsumer::new("mallory", "worker", 2, true);
    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&mallory)).offsets,
        [2, 3]
    );
    assert_eq!(group.attempts(0), 1);
    assert_eq!(group.attempts(1), 1);
}

/// **A reclaim happens once per connection, and only from older ones.** Two
/// live processes under one name, both leaving `reclaim` set: the newer takes
/// what the older held when it first polled, once. After that neither takes
/// the other's claims, so neither burns attempts on records still being
/// worked on.
#[test]
fn two_live_members_under_one_name_reclaim_once() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    let older = member("worker", 1, true);
    let newer = member("worker", 2, true);

    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&older)).offsets,
        [0, 1]
    );
    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&newer)).offsets,
        [0, 1]
    );

    // The older one carries on, and claims something new.
    assert_eq!(group.claim_as(10, 1, now, VIS, Some(&older)).offsets, [2]);
    // The newer one's next poll still says `reclaim`: it is not a reclaim.
    assert_eq!(group.claim_as(10, 1, now, VIS, Some(&newer)).offsets, [3]);
    assert_eq!(group.attempts(2), 1, "the older one's new claim is its own");
    assert_eq!(group.attempts(0), 2);
}

/// An older connection's reclaim takes nothing from a newer one.
#[test]
fn an_older_connection_cannot_reclaim_from_a_newer_one() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(
        group
            .claim_as(10, 2, now, VIS, Some(&member("worker", 5, false)))
            .offsets,
        [0, 1]
    );
    assert_eq!(
        group
            .claim_as(10, 2, now, VIS, Some(&member("worker", 3, true)))
            .offsets,
        [2, 3]
    );
}

/// **Reclaimed records go first, ahead of records owed to the group** even
/// when those have lower offsets.
#[test]
fn reclaimed_records_go_out_before_owed_ones() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(group.claim(10, 1, now, VIS).offsets, [0]);
    assert_eq!(
        group
            .claim_as(10, 2, now, VIS, Some(&member("worker", 1, false)))
            .offsets,
        [1, 2]
    );
    group.nack(0);

    let back = group.claim_as(10, 2, now, VIS, Some(&member("worker", 2, true)));
    assert_eq!(back.offsets, [1, 2]);
    assert_eq!(group.claim(10, 1, now, VIS).offsets, [0], "still owed");
}

/// **Asking for fewer than were held** leaves the rest reserved for the
/// member, across polls: other members do not get them in the meantime.
#[test]
fn reclaimed_records_beyond_max_records_wait_for_the_member() {
    let now = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(
        group
            .claim_as(10, 4, now, VIS, Some(&member("worker", 1, false)))
            .offsets,
        [0, 1, 2, 3]
    );

    let restarted = member("worker", 2, true);
    assert_eq!(
        group.claim_as(10, 2, now, VIS, Some(&restarted)).offsets,
        [0, 1]
    );
    assert_eq!(
        group
            .claim_as(10, 10, now, VIS, Some(&member("other", 3, false)))
            .offsets,
        [4, 5, 6, 7, 8, 9],
        "the rest are not anyone else's"
    );
    let next = member("worker", 2, false);
    assert_eq!(
        group.claim_as(12, 3, now, VIS, Some(&next)).offsets,
        [2, 3, 10]
    );
    assert_eq!(group.attempts(3), 2);
}

/// **A reserved claim that lapses during the takeover** is owed to the whole
/// group like any lapsed claim, and the member does not get it a second time
/// once someone else has it.
#[test]
fn a_reserved_claim_that_lapses_goes_to_the_group() {
    let start = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(
        group
            .claim_as(10, 3, start, VIS, Some(&member("worker", 1, false)))
            .offsets,
        [0, 1, 2]
    );
    let restarted = member("worker", 2, true);
    assert_eq!(
        group
            .claim_as(10, 1, at(start, 20), VIS, Some(&restarted))
            .offsets,
        [0]
    );

    // Offsets 1 and 2 were claimed at `start`, so they lapse at 30 s.
    assert_eq!(
        group
            .claim_as(10, 2, at(start, 31), VIS, Some(&member("other", 3, false)))
            .offsets,
        [1, 2]
    );
    assert_eq!(
        group
            .claim_as(10, 2, at(start, 32), VIS, Some(&member("worker", 2, false)))
            .offsets,
        [3, 4],
        "a reservation someone else now holds is skipped"
    );
}

#[test]
fn a_run_of_skipped_offsets_is_counted_and_dropped_once_passed() {
    let mut group = GroupTracker::new(0, MANY);
    group.skip(3);
    group.skip(4);
    assert_eq!(group.skipped_before(5), 2);
    assert_eq!(group.skipped_before(6), 0);

    for offset in 0..=5 {
        group.ack(offset);
    }
    assert_eq!(group.committed(), 6);
    assert_eq!(
        group.skipped_before(5),
        0,
        "the record after the run is settled"
    );
}

mod claim_control;
