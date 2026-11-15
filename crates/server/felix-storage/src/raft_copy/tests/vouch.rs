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
        copy: None,
        epoch: None,
        last: terms.last().map(|term| LogPoint {
            term: *term,
            index: terms.len() as u64 - 1,
        }),
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
            RotatedBy::PowerCut,
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
    let a_kept = kept(at_epoch(a_log, &bases), RotatedBy::PowerCut);
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
        RotatedBy::PowerCut,
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
    assert_eq!(plan.first_index, 12);
    assert_eq!(plan.voters.len(), 1);
    assert_eq!(plan.learners, vec!["b"]);
}

/// `FelixRaftWritten.tla`'s counterexample to counting a fsynced write with
/// no reservation: `a` led t2 and committed its blank at index 0 on `b`'s
/// fsync, then was declared dead; `b` never learned the commit and `c`
/// power-cut rotated empty. Nothing vouches for the entry, so the base is
/// empty, and only `b`'s reservation keeps index 0 from being given out
/// again.
#[test]
fn the_reservation_pads_above_an_entry_nothing_vouches_for() {
    let rec = record(&["a", "b", "c"], &["a"], &[], None);
    let reports = |b_reserved: Option<u64>| {
        let mut b = copy("b", &[2], None, Some(2));
        b.reserved = b_reserved;
        vec![
            ("b", report(b, vec![])),
            (
                "c",
                report(
                    blank(),
                    vec![kept(copy("c", &[], None, Some(2)), RotatedBy::PowerCut)],
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
                vec![kept(foreign, RotatedBy::PowerCut)],
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
