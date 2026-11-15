//! The re-form rule against hand traces from `FelixRaftReform.tla` and
//! `FelixRaftWritten.tla`.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::*;
use crate::raft_log::LogPoint;

const INC: u64 = 7;

/// A copy whose entries `0..terms.len()` have these terms, read with every
/// index probed.
fn copy(name: &str, terms: &[u64], committed: Option<u64>, vote: Option<u64>) -> CopyRead {
    CopyRead {
        path: PathBuf::from(name),
        copy: Some(1),
        epoch: None,
        last: terms.last().map(|term| LogPoint {
            term: *term,
            index: terms.len() as u64 - 1,
        }),
        purged: None,
        committed,
        vote,
        reserved: None,
        rotated_by: None,
        terms: terms
            .iter()
            .enumerate()
            .map(|(index, term)| (index as u64, Some(*term)))
            .collect(),
    }
}

/// [`copy`] holding entries at these indexes only, as a re-formed log holds
/// none in its gaps.
fn copy_from(name: &str, terms: &[(u64, u64)], vote: Option<u64>) -> CopyRead {
    let mut read = copy(name, &[], None, vote);
    for (index, term) in terms {
        read.terms.insert(*index, Some(*term));
    }
    read.last = terms.last().map(|(index, term)| LogPoint {
        term: *term,
        index: *index,
    });
    read
}

fn kept(mut read: CopyRead, by: RotatedBy) -> CopyRead {
    read.rotated_by = Some(by);
    read
}

fn at_epoch(mut read: CopyRead, bases: &[EpochBase]) -> CopyRead {
    read.epoch = Some(EpochFile {
        incarnation: INC,
        bases: bases.to_vec(),
    });
    read
}

fn reserved(mut read: CopyRead, index: u64) -> CopyRead {
    read.reserved = Some(index);
    read
}

fn report(current: CopyRead, kept: Vec<CopyRead>) -> BrokerReport<&'static str> {
    BrokerReport {
        leads: false,
        names: BTreeSet::new(),
        copies: BrokerCopies { current, kept },
    }
}

fn floor(index: u64, epoch: u64, term: u64) -> Option<EpochLogId> {
    Some(EpochLogId { index, epoch, term })
}

fn record(
    placed: &[&'static str],
    dead: &[&'static str],
    bases: &[EpochBase],
    floor: Option<EpochLogId>,
) -> GroupRecord<&'static str> {
    GroupRecord {
        incarnation: INC,
        bases: bases.to_vec(),
        floor,
        offset_floor: None,
        placed: Some(placed.iter().copied().collect()),
        dead: dead.iter().copied().collect(),
    }
}

fn plan(
    record: &GroupRecord<&'static str>,
    reports: Vec<(&'static str, BrokerReport<&'static str>)>,
) -> ReformPlan<&'static str> {
    let reports: BTreeMap<_, _> = reports.into_iter().collect();
    plan_reform(record, &reports).expect("a plan")
}

fn blank() -> CopyRead {
    copy("blank", &[], None, None)
}

/// The floor-holder shortcut's counterexample: `a` holds [X1 t1, Z t1], `b`
/// and `c` commit W at index 1 in t2 on a written majority, a power cut
/// takes W from both, and the floor still names index 0 of t1. Their kept
/// votes say t2, so `a` is cut to the floor and Z is never served where W
/// was delivered.
#[test]
fn a_kept_vote_above_every_entry_cuts_to_the_floor() {
    let rec = record(&["a", "b", "c"], &[], &[], floor(0, 0, 1));
    let cut = |name| {
        kept(
            reserved(copy(name, &[1], Some(0), Some(2)), 1),
            RotatedBy::PowerCut { to: 1 },
        )
    };
    let plan = plan(
        &rec,
        vec![
            ("a", report(copy("a", &[1, 1], Some(0), Some(1)), vec![])),
            ("b", report(blank(), vec![cut("b")])),
            ("c", report(blank(), vec![cut("c")])),
        ],
    );
    assert_eq!(plan.top, (0, 2));
    assert_eq!(plan.base_end(), 1);
    assert!(!plan.marked);
    assert!(plan.cut_by_vote, "W is lost, so the operator signs off");
    assert!(plan.needs_accept_loss());
    assert_eq!(plan.first_index, 2, "past the reservations that counted W");
    assert_eq!(plan.voters.len(), 3);
}

/// The same with `b` and `c` restored from before t2: their votes say t1,
/// so T alone would vouch for `a`'s Z. A restored copy of the epoch marks
/// the group, and a marked group vouches only to the commit or the floor.
#[test]
fn a_restored_copy_of_the_epoch_marks_the_group() {
    let rec = record(&["a", "b", "c"], &[], &[], floor(0, 0, 1));
    let restored = |name| kept(copy(name, &[1], Some(0), Some(1)), RotatedBy::Restore);
    let plan = plan(
        &rec,
        vec![
            ("a", report(copy("a", &[1, 1], Some(0), Some(1)), vec![])),
            ("b", report(blank(), vec![restored("b")])),
            ("c", report(blank(), vec![restored("c")])),
        ],
    );
    assert_eq!(plan.top, (0, 1));
    assert!(plan.marked);
    assert_eq!(plan.base_end(), 1);
}

/// MUST 1 of the design, scaled down. Epoch 0: B led t2 with C and
/// committed 0..=14, the floor names index 8, A holds 0..4 of t1. B and C
/// were declared dead and re-form 1 took A's 0..4, first index 9. Epoch 1
/// acknowledged R at 9..=11 on A, then A power-cut rotated. B came back
/// under a new storage id with its epoch-0 copy kept as a restore, was
/// placed in epoch 1 as a learner of A's base, and power-cut rotated too.
/// Re-form 2 keeps R from A's epoch-1 kept copy: B's longer epoch-0 log is
/// capped at epoch 1's base end, and its restore, of epoch 0, leaves epoch
/// 1 unmarked.
#[test]
fn an_older_epochs_copy_is_capped_and_never_marks() {
    let bases = [EpochBase {
        base_last: Some(EpochLogId {
            index: 4,
            epoch: 0,
            term: 1,
        }),
        first_index: 9,
    }];
    let rec = record(&["a", "b"], &[], &bases, floor(8, 0, 2));
    let mut a_log = copy("a-kept", &[1; 5], Some(4), Some(3));
    for index in 9..=11 {
        a_log.terms.insert(index, Some(3));
    }
    a_log.last = Some(LogPoint { term: 3, index: 11 });
    let a_kept = kept(at_epoch(a_log, &bases), RotatedBy::PowerCut { to: 1 });
    let b_old = kept(
        copy(
            "b-kept",
            &[1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2],
            Some(14),
            Some(2),
        ),
        RotatedBy::Restore,
    );
    let b_learner = kept(
        at_epoch(copy("b-kept1", &[1; 5], Some(4), Some(3)), &bases),
        RotatedBy::PowerCut { to: 1 },
    );
    let plan = plan(
        &rec,
        vec![
            ("a", report(blank(), vec![a_kept])),
            ("b", report(blank(), vec![b_old, b_learner])),
        ],
    );
    assert!(!plan.marked);
    assert_eq!(plan.top, (1, 3));
    assert_eq!(
        plan.base_last,
        Some(EpochLogId {
            index: 11,
            epoch: 1,
            term: 3
        }),
        "R is in the base"
    );
    assert_eq!(plan.source, ("a", Source::Kept(PathBuf::from("a-kept"))));
    assert_eq!(plan.first_index, 15, "above B's epoch-0 records too");
    assert_eq!(plan.voters.len(), 1);
    assert_eq!(plan.learners, vec!["b"]);
}

/// `FelixRaftWritten.tla`'s counterexample to counting a written entry with
/// no reservation: `a` led t2 and committed its blank at index 0 on `b`'s
/// write, then was declared dead; `b` never learned the commit, and `b`
/// and `c` power-cut rotated, `b` losing the unsynced entry. Nothing holds
/// it, so the base is empty, and only `b`'s kept reservation keeps index 0
/// from being given out again.
#[test]
fn the_reservation_pads_above_an_entry_nothing_vouches_for() {
    let rec = record(&["a", "b", "c"], &["a"], &[], None);
    let reports = |b_reserved: Option<u64>| {
        let mut b = copy("b", &[], None, Some(2));
        b.reserved = b_reserved;
        vec![
            (
                "b",
                report(blank(), vec![kept(b, RotatedBy::PowerCut { to: 1 })]),
            ),
            (
                "c",
                report(
                    blank(),
                    vec![kept(
                        copy("c", &[], None, Some(2)),
                        RotatedBy::PowerCut { to: 1 },
                    )],
                ),
            ),
        ]
    };
    let padded = plan(&rec, reports(Some(0)));
    assert!(padded.marked, "a placed broker is dead");
    assert_eq!(padded.base_last, None);
    assert_eq!(padded.first_index, 1);

    // The model's trace: with no reservation the offset is reused.
    assert_eq!(plan(&rec, reports(None)).first_index, 0);

    // The offset floor a leader of an earlier epoch reported keeps it, even
    // with no reservation left on any disk.
    let mut floored = rec.clone();
    floored.offset_floor = Some(6);
    let floored = plan(&floored, reports(None));
    assert_eq!(floored.first_index, 7);
}

/// An index in a re-form's gap of a later epoch than the floor's stands for
/// the floor's entry.
#[test]
fn entry_epochs_are_derived_from_the_bases() {
    let bases = [
        EpochBase {
            base_last: Some(EpochLogId {
                index: 1,
                epoch: 0,
                term: 1,
            }),
            first_index: 5,
        },
        EpochBase {
            base_last: Some(EpochLogId {
                index: 6,
                epoch: 1,
                term: 2,
            }),
            first_index: 9,
        },
    ];
    let at = |epoch, index| EpochFile::epoch_at(&bases, epoch, index);
    assert_eq!(at(2, 0), EpochAt::Entry(0));
    assert_eq!(at(2, 3), EpochAt::Gap(1));
    assert_eq!(at(2, 6), EpochAt::Entry(1));
    assert_eq!(at(2, 7), EpochAt::Gap(2));
    assert_eq!(at(2, 9), EpochAt::Entry(2));
    assert_eq!(at(1, 9), EpochAt::Entry(1));
    assert_eq!(at(0, 9), EpochAt::Entry(0));

    let rec = record(&["a"], &[], &bases, floor(3, 0, 1));
    let a = at_epoch(
        copy_from("a", &[(0, 1), (1, 1), (5, 2), (6, 2), (9, 4)], Some(4)),
        &bases,
    );
    let plan = plan(&rec, vec![("a", report(a, vec![]))]);
    assert_eq!(plan.top, (2, 4));
    assert_eq!(plan.base_end(), 10);
}

#[test]
fn a_plan_refuses_before_vouching() {
    let rec = record(&["a", "b"], &[], &[], None);
    let one = |leads| {
        let mut r = report(copy("a", &[1], None, Some(1)), vec![]);
        r.leads = leads;
        BTreeMap::from([("a", r)])
    };
    assert_eq!(
        plan_reform(&rec, &one(false)),
        Err(Refused::Unaccounted("b"))
    );

    let rec = record(&["a"], &[], &[], None);
    assert_eq!(plan_reform(&rec, &one(true)), Err(Refused::Leads("a")));

    let mut reports = one(false);
    reports.get_mut("a").expect("a").names.insert("z");
    assert_eq!(
        plan_reform(&rec, &reports),
        Err(Refused::OffRecord {
            broker: "a",
            names: "z"
        })
    );

    let mut none = rec.clone();
    none.placed = None;
    assert_eq!(
        plan_reform(&none, &one(false)),
        Err(Refused::NoPlacedRecord)
    );

    let other = [EpochBase {
        base_last: None,
        first_index: 3,
    }];
    let mut ahead = one(false);
    ahead.get_mut("a").expect("a").copies.current.epoch = Some(EpochFile {
        incarnation: INC,
        bases: other.to_vec(),
    });
    assert_eq!(
        plan_reform(&rec, &ahead),
        Err(Refused::EpochAhead {
            broker: "a",
            epoch: 1
        })
    );
    let mut rec1 = rec.clone();
    rec1.bases = vec![EpochBase {
        base_last: None,
        first_index: 4,
    }];
    assert_eq!(plan_reform(&rec1, &ahead), Err(Refused::BasesDiffer("a")));

    let mut unprobed = one(false);
    let a = &mut unprobed.get_mut("a").expect("a").copies.current;
    *a = copy("a", &[1, 1, 2], Some(0), Some(2));
    a.terms.clear();
    let mut rec = rec.clone();
    rec.floor = floor(1, 0, 1);
    assert_eq!(
        plan_reform(&rec, &unprobed),
        Err(Refused::NotProbed {
            broker: "a",
            index: 1
        })
    );
}

/// Another incarnation's copy vouches for nothing and sets no key, but its
/// reservation still pads.
#[test]
fn another_incarnations_copy_only_pads() {
    let rec = record(&["a"], &[], &[], None);
    let mut foreign = reserved(copy("old", &[9, 9, 9], Some(2), Some(9)), 40);
    foreign.epoch = Some(EpochFile {
        incarnation: INC + 1,
        bases: vec![],
    });
    let plan = plan(
        &rec,
        vec![(
            "a",
            report(
                copy("a", &[1, 1], Some(0), Some(1)),
                vec![kept(foreign, RotatedBy::PowerCut { to: 1 })],
            ),
        )],
    );
    assert_eq!(plan.top, (0, 1));
    assert_eq!(plan.base_end(), 2);
    assert_eq!(plan.first_index, 41);
    assert_eq!(plan.voters[0].source, Source::Current);
}

#[test]
fn probes_cover_every_place_a_base_can_end() {
    let rec = record(&["a"], &[], &[], floor(3, 0, 1));
    let reports = BTreeMap::from([("a", report(copy("a", &[1; 6], Some(1), None), vec![]))]);
    assert_eq!(probes(&rec, &reports), BTreeSet::from([1, 3, 5]));
}

/// A copy of the epoch that never voted still puts T at (epoch, 0), so a
/// restored older-epoch copy with a higher term is not vouched whole. Epoch
/// 0: `s`, declared dead but running, wrote t3 at index 1 while `a` held
/// [t1, t2] committed. Re-form 1 took `a`'s log; `a` never voted in epoch 1.
#[test]
fn a_copy_that_never_voted_keys_its_epoch_at_term_zero() {
    let bases = [EpochBase {
        base_last: Some(EpochLogId {
            index: 1,
            epoch: 0,
            term: 2,
        }),
        first_index: 2,
    }];
    let rec = record(&["a"], &[], &bases, None);
    let a = at_epoch(copy("a", &[1, 2], Some(1), None), &bases);
    let s = kept(
        copy("s-kept", &[1, 3], Some(0), Some(3)),
        RotatedBy::Restore,
    );
    let plan = plan(
        &rec,
        vec![("a", report(a, vec![])), ("s", report(blank(), vec![s]))],
    );
    assert_eq!(plan.top, (1, 0));
    assert_eq!(plan.source, ("a", Source::Current));
    assert_eq!(
        plan.base_last,
        Some(EpochLogId {
            index: 1,
            epoch: 0,
            term: 2
        })
    );
}

/// A broker with no copy file holds the base yet has no id a membership
/// could name, so it is a learner.
#[test]
fn a_broker_without_a_copy_id_is_never_a_voter() {
    let rec = record(&["a", "b"], &[], &[], None);
    let mut b = copy("b", &[1, 1], Some(1), Some(1));
    b.copy = None;
    let plan = plan(
        &rec,
        vec![
            ("a", report(copy("a", &[1, 1], Some(1), Some(1)), vec![])),
            ("b", report(b, vec![])),
        ],
    );
    assert_eq!(plan.base_end(), 2);
    assert_eq!(
        plan.voters,
        vec![Voter {
            broker: "a",
            copy: 1,
            source: Source::Current
        }]
    );
    assert_eq!(plan.learners, vec!["b"]);
}

/// The base ends below what its source purged: the term there is not on
/// that disk, and the refusal says so.
#[test]
fn a_base_ending_in_a_purged_prefix_refuses_as_purged() {
    let bases = [EpochBase {
        base_last: Some(EpochLogId {
            index: 2,
            epoch: 0,
            term: 1,
        }),
        first_index: 5,
    }];
    let rec = record(&["a"], &[], &bases, None);
    let mut a = copy("a", &[1; 9], Some(8), Some(1));
    a.purged = Some(6);
    a.terms.retain(|index, _| *index >= 6);
    let reports = BTreeMap::from([("a", report(a, vec![]))]);
    assert_eq!(
        plan_reform(&rec, &reports),
        Err(Refused::Purged {
            broker: "a",
            index: 2
        })
    );
}

/// A vouch that ends inside a re-form's gap backs off to that epoch's base
/// end: the floor names index 3 of epoch 0, which re-form 1 put in its gap.
#[test]
fn a_base_never_ends_inside_a_gap() {
    let bases = [EpochBase {
        base_last: Some(EpochLogId {
            index: 1,
            epoch: 0,
            term: 1,
        }),
        first_index: 5,
    }];
    let rec = record(&["a", "d"], &["d"], &bases, floor(3, 0, 1));
    let a = at_epoch(copy_from("a", &[(0, 1), (1, 1), (5, 2)], Some(2)), &bases);
    let mut a = a;
    a.committed = Some(1);
    let plan = plan(&rec, vec![("a", report(a, vec![]))]);
    assert!(plan.marked);
    assert_eq!(
        plan.base_last,
        Some(EpochLogId {
            index: 1,
            epoch: 0,
            term: 1
        })
    );
    assert_eq!(plan.first_index, 6, "above the epoch-1 record at 5");
}

/// A vote above every entry flags a loss only when it cuts a copy: here the
/// one copy's commit already covers its whole log.
#[test]
fn a_vote_above_every_entry_that_cuts_nothing_needs_no_sign_off() {
    let rec = record(&["a"], &[], &[], None);
    let plan = plan(
        &rec,
        vec![("a", report(copy("a", &[1, 1], Some(1), Some(2)), vec![]))],
    );
    assert_eq!(plan.top, (0, 2));
    assert_eq!(plan.base_end(), 2);
    assert!(!plan.cut_by_vote);
    assert!(!plan.needs_accept_loss());
}

/// Epoch 0, placed {a, b, c}: `a` wrote X at 5 alone; `b`, elected by `b`
/// and `c`, committed W at 5, delivered, floor at 4; then `b` and `c` lost
/// their disks, which keeps no copy. Their empty currents never voted, so
/// the group is marked and `a` is cut to the floor, never served whole.
#[test]
fn a_wiped_copy_at_epoch_zero_marks_the_group() {
    let rec = record(&["a", "b", "c"], &[], &[], floor(4, 0, 1));
    let plan = plan(
        &rec,
        vec![
            ("a", report(copy("a", &[1; 6], Some(3), Some(1)), vec![])),
            ("b", report(blank(), vec![])),
            ("c", report(blank(), vec![])),
        ],
    );
    assert!(plan.marked);
    assert!(plan.needs_accept_loss());
    assert_eq!(plan.base_end(), 5, "X is not in the base");
    assert_eq!(plan.first_index, 6);
}

/// A re-form never gives out an index below an earlier epoch's first one,
/// even when no copy holds a record past that epoch's gap.
#[test]
fn the_first_index_never_goes_below_an_earlier_epochs() {
    let bases = [EpochBase {
        base_last: Some(EpochLogId {
            index: 1,
            epoch: 0,
            term: 1,
        }),
        first_index: 5,
    }];
    let rec = record(&["a"], &[], &bases, None);
    let a = at_epoch(copy("a", &[1, 1], Some(1), Some(2)), &bases);
    let plan = plan(&rec, vec![("a", report(a, vec![]))]);
    assert_eq!(plan.base_end(), 2);
    assert_eq!(plan.first_index, 5);
}

#[test]
fn a_plan_with_no_voter_refuses() {
    let rec = record(&["a"], &[], &[], None);
    let mut a = copy("a", &[1, 1], Some(1), Some(1));
    a.copy = None;
    let reports = BTreeMap::from([("a", report(a, vec![]))]);
    assert_eq!(plan_reform(&rec, &reports), Err(Refused::NoVoters));
}

/// `b` and `c` power-cut rotated in t1, and their kept copies (vote t1)
/// stay. Then `b` led t2 and committed W at 5 over `a`'s lone X at 5, and
/// only `b`'s and `c`'s shard directories were lost: the empty rotation
/// keeps nothing. The old power cuts name the copies they rotated into, not
/// the empty ones now current, so the group is marked and X is never
/// served whole. At epoch 0 and at a later epoch.
#[test]
fn an_older_power_cut_never_vouches_for_a_later_copy() {
    let bases = [EpochBase {
        base_last: Some(EpochLogId {
            index: 0,
            epoch: 0,
            term: 1,
        }),
        first_index: 1,
    }];
    for epoch in [0, 1] {
        let bases = &bases[..epoch];
        let rec = record(&["a", "b", "c"], &[], bases, floor(4, epoch as u64, 1));
        let lost = |name| {
            let old = copy(name, &[1; 5], Some(3), Some(1));
            let old = kept(at_epoch(old, bases), RotatedBy::PowerCut { to: 9 });
            let mut wiped = blank();
            wiped.copy = Some(10);
            report(wiped, vec![old])
        };
        let a = at_epoch(copy("a", &[1; 6], Some(3), Some(1)), bases);
        let plan = plan(
            &rec,
            vec![("a", report(a, vec![])), ("b", lost("b")), ("c", lost("c"))],
        );
        assert!(plan.marked, "epoch {epoch}");
        assert!(plan.needs_accept_loss(), "epoch {epoch}");
        assert_eq!(plan.base_end(), 5, "epoch {epoch}: X is not in the base");
    }
}
