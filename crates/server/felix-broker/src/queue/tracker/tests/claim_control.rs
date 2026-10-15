//! A consumer extending its claim, nacking with a delay, and giving up.

use super::*;

#[test]
fn an_extended_claim_is_not_handed_out_when_it_would_have_lapsed() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(group.claim(1, 10, base, VIS).offsets, vec![0]);

    assert!(group.extend(0, 1, at(base, 20), at(base, 80)));

    assert!(group.claim(1, 10, at(base, 31), VIS).offsets.is_empty());
    assert_eq!(group.next_lapse(), Some(at(base, 80)));
    assert_eq!(group.claim(1, 10, at(base, 81), VIS).offsets, vec![0]);
    assert_eq!(group.in_flight(), 1);
}

/// **A lost claim stays lost.** Once a claim lapses and the record goes to
/// someone else, the first consumer's extend must not reach the new claim:
/// it would keep the record from the group for as long as it liked, even
/// after the consumer now holding it died.
#[test]
fn an_extend_after_the_record_was_handed_out_again_is_refused() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(group.claim(1, 10, base, VIS).offsets, vec![0]);
    // Lapsed, and claimed again by someone else.
    assert_eq!(group.claim(1, 10, at(base, 31), VIS).offsets, vec![0]);

    assert!(!group.extend(0, 1, at(base, 32), at(base, 600)));
    assert_eq!(
        group.next_lapse(),
        Some(at(base, 61)),
        "the new claim moved"
    );
    // The holder of the new claim can.
    assert!(group.extend(0, 2, at(base, 32), at(base, 600)));
}

#[test]
fn a_lapsed_claim_cannot_be_extended() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(group.claim(1, 10, base, VIS).offsets, vec![0]);

    assert!(!group.extend(0, 1, at(base, 31), at(base, 90)));
    // Owed, so the next poll has it.
    assert_eq!(group.claim(1, 10, at(base, 31), VIS).offsets, vec![0]);
}

#[test]
fn a_settled_record_cannot_be_extended() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(1, 10, base, VIS);
    group.ack(0);

    assert!(!group.extend(0, 1, base, at(base, 90)));
    assert_eq!(group.in_flight(), 0);
}

#[test]
fn a_delayed_nack_is_owed_once_the_delay_passes() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    assert_eq!(group.claim(2, 10, base, VIS).offsets, vec![0, 1]);

    group.nack_after(0, at(base, 60));

    // Not at once, and not when the claim would have lapsed.
    assert!(group.claim(2, 10, at(base, 1), VIS).offsets.is_empty());
    group.ack(1);
    assert!(group.claim(2, 10, at(base, 31), VIS).offsets.is_empty());
    let again = group.claim(2, 10, at(base, 61), VIS);
    assert_eq!(again.offsets, vec![0]);
    assert_eq!(group.attempts(0), 2, "the delay is not a second attempt");
}

#[test]
fn a_delayed_nack_holds_a_place_under_the_in_flight_cap() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.set_max_in_flight(1);
    assert_eq!(group.claim(3, 10, base, VIS).offsets, vec![0]);

    group.nack_after(0, at(base, 60));

    let claim = group.claim(3, 10, at(base, 1), VIS);
    assert!(claim.offsets.is_empty());
    assert!(claim.capped);
}

/// A nack hands the record back. Its consumer no longer holds it, so it
/// cannot extend its way back into a claim.
#[test]
fn a_delayed_nack_cannot_be_extended() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(1, 10, base, VIS);
    group.nack_after(0, at(base, 60));

    assert!(!group.extend(0, 1, at(base, 1), at(base, 600)));
    assert_eq!(group.next_lapse(), Some(at(base, 60)));
}

#[test]
fn a_delayed_nack_after_an_ack_does_nothing() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(2, 10, base, VIS);
    group.ack(1);
    group.nack_after(1, at(base, 60));

    assert_eq!(group.in_flight(), 1);
    assert!(!group.in_play(1));
}

/// A record nacked with a delay still counts towards the attempt bound when
/// it comes round again.
#[test]
fn delayed_nacks_count_towards_the_bound() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, 2);
    group.claim(1, 10, base, VIS);
    group.nack_after(0, at(base, 5));
    assert_eq!(group.claim(1, 10, at(base, 6), VIS).offsets, vec![0]);
    group.nack_after(0, at(base, 11));

    let claim = group.claim(1, 10, at(base, 12), VIS);
    assert!(claim.offsets.is_empty());
    assert_eq!(
        claim.dead_lettered,
        vec![DeadLettered {
            offset: 0,
            attempts: 2
        }]
    );
}

#[test]
fn a_named_members_delayed_nack_is_not_reclaimed() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    let first = member("w", 1, true);
    assert_eq!(
        group.claim_as(1, 10, base, VIS, Some(&first)).offsets,
        vec![0]
    );
    group.nack_after(0, at(base, 60));

    // The member restarts and reclaims. The record it handed back is not its
    // claim any more, so it waits out the delay like anyone else's.
    let restarted = member("w", 2, true);
    assert!(
        group
            .claim_as(1, 10, at(base, 1), VIS, Some(&restarted))
            .offsets
            .is_empty()
    );
}

#[test]
fn what_is_in_play() {
    let base = Instant::now();
    let mut group = GroupTracker::new(0, MANY);
    group.claim(3, 10, base, VIS);
    group.ack(0);
    group.ack(2);
    group.nack(1);

    assert!(!group.in_play(0), "below the cursor");
    assert!(group.in_play(1), "owed");
    assert!(!group.in_play(2), "acknowledged above a gap");
}
