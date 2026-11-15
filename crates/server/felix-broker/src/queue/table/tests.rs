//! Each test pins one rule of `docs/formal/FelixRaftGroups.tla`'s `Step`,
//! named after the knob that removes it there, and fails with the rule
//! taken out of [`GroupTable::apply`].

use super::*;

const TERM: u64 = 2;

fn hydrated(index: u64) -> CreationToken {
    CreationToken {
        term: TERM,
        index,
        by: CreatedBy::Hydrate,
    }
}

fn seeked(term: u64, index: u64) -> CreationToken {
    CreationToken {
        term,
        index,
        by: CreatedBy::Seek,
    }
}

fn entry(group: &str, op: GroupOp) -> GroupEntry {
    GroupEntry {
        group: group.to_owned(),
        op,
    }
}

/// Applies entries at consecutive indexes from 1, in `TERM`.
struct Log {
    table: ShardGroups,
    next: u64,
}

impl Log {
    fn new() -> Self {
        Self {
            table: ShardGroups::default(),
            next: 1,
        }
    }

    fn at(&self) -> u64 {
        self.next
    }

    fn apply(&mut self, group: &str, op: GroupOp) -> Folded {
        let index = self.next;
        self.next += 1;
        self.table.apply(TERM, index, &entry(group, op))
    }

    fn state(&self, group: &str) -> Option<&GroupState> {
        self.table.table().group(group)
    }
}

/// A group's first advance creates it at its token; later advances only
/// move the cursor forward.
#[test]
fn an_advance_creates_the_group_and_only_moves_forward() {
    let mut log = Log::new();
    let cre = hydrated(0);
    log.apply("a", GroupOp::Advance { cre, to: 5 });
    log.apply("a", GroupOp::Advance { cre, to: 3 });
    let state = log.state("a").expect("created");
    assert_eq!((state.cre, state.cursor), (cre, 5));
    log.apply("a", GroupOp::Advance { cre, to: 9 });
    assert_eq!(log.state("a").expect("live").cursor, 9);
}

/// CreationCheck: an advance decided before a seek lands after it and is
/// dropped, so it never undoes the seek.
#[test]
fn creation_check_a_late_advance_does_not_undo_a_seek() {
    let mut log = Log::new();
    let old = hydrated(0);
    log.apply("a", GroupOp::Advance { cre: old, to: 4 });
    let seek_at = log.at();
    log.apply(
        "a",
        GroupOp::Seek {
            to: 1,
            if_new: false,
        },
    );
    let folded = log.apply("a", GroupOp::Advance { cre: old, to: 8 });
    assert_eq!(
        folded,
        Folded::Dropped {
            tracker_dropped: false
        }
    );
    let state = log.state("a").expect("live");
    assert_eq!((state.cre, state.cursor), (seeked(TERM, seek_at), 1));
    // The seek's own incarnation moves it.
    log.apply(
        "a",
        GroupOp::Advance {
            cre: seeked(TERM, seek_at),
            to: 3,
        },
    );
    assert_eq!(log.state("a").expect("live").cursor, 3);
}

/// A seek's token is its log id: a seek dropped at a change of leader and a
/// later seek at the same index in another term are different incarnations.
#[test]
fn creation_check_the_seeks_term_is_part_of_its_token() {
    let mut table = GroupTable::default();
    let seek = entry(
        "a",
        GroupOp::Seek {
            to: 2,
            if_new: false,
        },
    );
    table.apply(3, 1, &seek);
    let other_term = entry(
        "a",
        GroupOp::Advance {
            cre: seeked(2, 1),
            to: 7,
        },
    );
    assert!(matches!(
        table.apply(3, 2, &other_term),
        Folded::Dropped { .. }
    ));
    assert_eq!(table.group("a").expect("live").cursor, 2);
}

/// DeleteWatermark: a write decided before a delete of its group does not
/// bring the group back.
#[test]
fn delete_watermark_a_late_write_does_not_revive_a_deleted_group() {
    let mut log = Log::new();
    let cre = hydrated(0);
    log.apply("a", GroupOp::Advance { cre, to: 4 });
    log.apply("a", GroupOp::Delete);
    for op in [
        GroupOp::Advance { cre, to: 8 },
        GroupOp::DeadLetter { cre, offset: 5 },
    ] {
        assert!(matches!(log.apply("a", op), Folded::Dropped { .. }));
    }
    assert_eq!(log.state("a"), None);
    // A tracker hydrated after the delete creates it again.
    let after = hydrated(log.at() - 1);
    log.apply("a", GroupOp::Advance { cre: after, to: 2 });
    assert_eq!(log.state("a").expect("again").cursor, 2);
}

/// StaleByHyd, the rejected rule: a delete of another group leaves a live
/// group's writes alone, however early its tracker hydrated.
#[test]
fn stale_by_hyd_a_delete_of_another_group_leaves_a_live_group_alone() {
    let mut log = Log::new();
    let cre = hydrated(0);
    log.apply("a", GroupOp::Advance { cre, to: 2 });
    log.apply("b", GroupOp::Advance { cre, to: 1 });
    log.apply("b", GroupOp::Delete);
    assert_eq!(
        log.apply("a", GroupOp::Advance { cre, to: 6 }),
        Folded::Applied
    );
    assert_eq!(
        log.apply("a", GroupOp::DeadLetter { cre, offset: 6 }),
        Folded::Applied
    );
    let state = log.state("a").expect("live");
    assert_eq!((state.cursor, state.dead.len()), (6, 1));
    assert!(log.state("b").is_none());
}

/// RefreshOnDrop: a dropped write carrying this member's tracker's token
/// forgets the tracker; one carrying another token leaves it.
#[test]
fn refresh_on_drop_a_dropped_write_of_the_tracker_forgets_it() {
    let mut log = Log::new();
    // Hydrated before another group's delete, with nothing committed yet.
    let early = hydrated(0);
    log.table.track("a", early);
    log.apply("b", GroupOp::Advance { cre: early, to: 1 });
    log.apply("b", GroupOp::Delete);
    let stranger = hydrated(0);
    log.table.track("c", stranger);
    assert_eq!(
        log.apply(
            "a",
            GroupOp::DeadLetter {
                cre: early,
                offset: 3
            }
        ),
        Folded::Dropped {
            tracker_dropped: true
        }
    );
    assert_eq!(log.table.tracker("a"), None);
    assert_eq!(log.table.tracker("c"), Some(stranger));
    // Applied writes leave the tracker in place.
    let fresh = hydrated(log.at() - 1);
    log.table.track("a", fresh);
    assert_eq!(
        log.apply("a", GroupOp::Advance { cre: fresh, to: 1 }),
        Folded::Applied
    );
    assert_eq!(log.table.tracker("a"), Some(fresh));
}

/// TermBoundTrackers: a change of term forgets every tracker, and so does a
/// rebuilt table.
#[test]
fn term_bound_trackers_end_with_the_term_and_with_a_rebuild() {
    let mut groups = ShardGroups::default();
    groups.track("a", hydrated(0));
    groups.end_term();
    assert_eq!(groups.tracker("a"), None);
    groups.track("a", hydrated(0));
    groups.reset(GroupTable::default());
    assert_eq!(groups.tracker("a"), None);
}

/// KeepRedriven: a redrive keeps the record owed in the table until it is
/// settled, a seek keeps it, and dead-lettering it again moves it back.
#[test]
fn keep_redriven_a_redriven_record_stays_owed_until_finished() {
    let mut log = Log::new();
    let cre = hydrated(0);
    log.apply("a", GroupOp::Advance { cre, to: 2 });
    log.apply("a", GroupOp::DeadLetter { cre, offset: 2 });
    log.apply("a", GroupOp::Advance { cre, to: 3 });
    log.apply("a", GroupOp::Redrive { cre, offset: 2 });
    let state = log.state("a").expect("live");
    assert_eq!(
        (&state.dead, &state.redriven),
        (&BTreeSet::new(), &BTreeSet::from([2]))
    );
    log.apply(
        "a",
        GroupOp::Seek {
            to: 0,
            if_new: false,
        },
    );
    let cre = log.state("a").expect("live").cre;
    assert_eq!(log.state("a").expect("live").redriven, BTreeSet::from([2]));
    log.apply("a", GroupOp::DeadLetter { cre, offset: 2 });
    let state = log.state("a").expect("live");
    assert_eq!(
        (&state.dead, &state.redriven),
        (&BTreeSet::from([2]), &BTreeSet::new())
    );
    log.apply("a", GroupOp::Redrive { cre, offset: 2 });
    log.apply("a", GroupOp::FinishRedrive { cre, offset: 2 });
    let state = log.state("a").expect("live");
    assert!(state.dead.is_empty() && state.redriven.is_empty());
}

/// A redrive or discard of a record that is not a dead letter changes
/// nothing, and neither, nor a finish, creates a group.
#[test]
fn user_writes_never_create_a_group_or_invent_a_dead_letter() {
    let mut log = Log::new();
    let cre = hydrated(0);
    for op in [
        GroupOp::Redrive { cre, offset: 1 },
        GroupOp::FinishRedrive { cre, offset: 1 },
        GroupOp::Discard { cre, offset: 1 },
    ] {
        log.apply("a", op);
    }
    assert!(log.state("a").is_none());
    log.apply("a", GroupOp::Advance { cre, to: 1 });
    log.apply("a", GroupOp::Redrive { cre, offset: 5 });
    log.apply("a", GroupOp::DeadLetter { cre, offset: 4 });
    log.apply("a", GroupOp::Discard { cre, offset: 4 });
    let state = log.state("a").expect("live");
    assert!(state.dead.is_empty() && state.redriven.is_empty());
}

/// A dead letter is never the write that creates a group (the creating
/// advance comes first), but if one is, the cursor starts at 0 rather than
/// pass records nobody settled.
#[test]
fn a_dead_letter_on_an_absent_group_skips_nothing() {
    let mut log = Log::new();
    log.apply(
        "a",
        GroupOp::DeadLetter {
            cre: hydrated(0),
            offset: 7,
        },
    );
    let state = log.state("a").expect("created");
    assert_eq!((state.cursor, state.dead.len()), (0, 1));
}

/// A rebuild may overlap entries applied after it: an index at or below
/// the last applied is a replay and changes nothing.
#[test]
fn a_replayed_index_is_ignored() {
    let mut table = GroupTable::default();
    let cre = hydrated(0);
    let advance = |to| entry("a", GroupOp::Advance { cre, to });
    table.apply(TERM, 4, &advance(3));
    assert_eq!(table.apply(TERM, 4, &advance(9)), Folded::Replayed);
    assert_eq!(
        table.apply(TERM, 2, &entry("a", GroupOp::Delete)),
        Folded::Replayed
    );
    assert_eq!(table.group("a").expect("live").cursor, 3);
    assert_eq!(table.applied(), 4);
}

/// Two members that fold the same entries hold equal tables, whatever
/// trackers each keeps.
#[test]
fn the_same_entries_fold_to_the_same_table() {
    let cre = hydrated(0);
    let entries = [
        entry("a", GroupOp::Advance { cre, to: 3 }),
        entry("b", GroupOp::Advance { cre, to: 1 }),
        entry("a", GroupOp::DeadLetter { cre, offset: 3 }),
        entry("b", GroupOp::Delete),
        entry("a", GroupOp::Redrive { cre, offset: 3 }),
        entry(
            "c",
            GroupOp::Seek {
                to: 4,
                if_new: true,
            },
        ),
    ];
    let mut leader = ShardGroups::default();
    leader.track("a", cre);
    let mut follower = ShardGroups::default();
    for (index, entry) in (1..).zip(&entries) {
        leader.apply(TERM, index, entry);
        follower.apply(TERM, index, entry);
    }
    assert_eq!(leader.table(), follower.table());
    assert_eq!(leader.table().last_delete(), 4);
}
