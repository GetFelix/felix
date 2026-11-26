//! The re-form rule: from what every reached copy of a group holds, the base
//! the next epoch starts from, its voters and its first index.
//!
//! Pure and deterministic, so the control plane and a broker reach the same
//! plan from the same reports. It implements `ReformKept` in
//! `docs/formal/FelixRaftReform.tla`; the reasoning is in
//! `docs/per-shard-raft.md`, "Re-forming a group".

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use super::epoch_file::{EpochAt, EpochBase, EpochFile, EpochLogId};
use super::reform::{BrokerCopies, CopyRead};
use super::rotated_by::RotatedBy;
use crate::disk_log::CarriedFloor;

/// An (epoch, term) pair. Every term of an epoch is above every term of an
/// earlier one.
pub(crate) type Key = (u64, u64);

/// A Raft voter: a broker under one copy id. A broker whose disk was
/// replaced runs a new copy, which never stands for the old one's votes.
pub type VoterId<B> = (B, u64);

/// What the control plane holds for the group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRecord<B> {
    /// The stream incarnation the group runs.
    pub incarnation: u64,
    /// How each re-form so far started its epoch; the group is at epoch
    /// `bases.len()`.
    pub bases: Vec<EpochBase>,
    /// The highest commit a leader reported, if any.
    pub floor: Option<EpochLogId>,
    /// The highest committed index a leader of any epoch of this
    /// incarnation reported (`offset_floor` on the shard's record). The
    /// next epoch gives out no offset at or below it.
    pub offset_floor: Option<u64>,
    /// Every broker placed in the current epoch, removed ones included.
    /// `None` when the shard has no such record.
    pub placed: Option<BTreeSet<B>>,
    /// Brokers declared dead. Their copies are not read.
    pub dead: BTreeSet<B>,
    /// The voters the epoch started with: the plan that formed it, or the
    /// group's first voters at epoch 0. A broker forms a group only on the
    /// target the control plane stored, so those are also on `ordered`.
    /// `None` when not recorded.
    pub seed: Option<BTreeSet<VoterId<B>>>,
    /// Every voter target the control plane ordered in the epoch, recorded
    /// before the leader was asked: the control plane's
    /// `ShardAssignment::ordered_voter_sets`. `None` when not recorded.
    pub ordered: Option<Vec<BTreeSet<VoterId<B>>>>,
    /// Set when the operator re-forms the group from a backup point.
    pub restore: Option<RestoreAt>,
}

/// How far past the time line (`RESTORE_OFFSETS_PER_MILLI` times the
/// plan's time) a restore's epoch starts when the operator names no gap.
pub const DEFAULT_OFFSET_GAP: u64 = 1 << 40;

/// The smallest gap the control plane takes without the operator accepting
/// a short one by name.
pub const MIN_OFFSET_GAP: u64 = 1 << 32;

/// The largest gap a restore takes. The bound a later restore or abandon
/// checks assumes no earlier epoch started further above the line.
/// `docs/per-shard-raft.md`, "A restore from a backup point is a re-form".
pub const MAX_OFFSET_GAP: u64 = DEFAULT_OFFSET_GAP;

/// The slope of the time line a restore starts on, in offsets per
/// millisecond of control-plane wall time (16 per microsecond). Safety
/// assumes a shard gives out offsets at no more than half of it, which
/// leaves room for clock skew between the control-plane nodes that plan
/// two restores. `docs/per-shard-raft.md`, "A restore from a backup point
/// is a re-form".
pub const RESTORE_OFFSETS_PER_MILLI: u64 = 1 << 14;

/// A re-form from a backup point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestoreAt {
    /// The point's offset for the shard's log: every record below it was
    /// acknowledged and is in the restored copy, and the base ends here.
    pub end: u64,
    /// How far past the time line the new epoch starts.
    pub gap: u64,
    /// The control plane's wall clock when the plan was made, in Unix
    /// milliseconds. Fixed once per plan, so every reading of the plan
    /// gives the same first index.
    pub planned_at_millis: u64,
}

impl RestoreAt {
    /// The lowest first index the restore takes:
    /// `RESTORE_OFFSETS_PER_MILLI * planned_at_millis + gap`, or `None` past
    /// the last index.
    ///
    /// Anchored to absolute time rather than the point, so every restored
    /// epoch starts on one line that grows faster than any shard writes:
    /// a later restore, from any backup, starts past every offset an
    /// earlier one gave out. The gap is not added to the point's end as
    /// well; an epoch started above the line would stay ahead of it.
    pub fn first_index(&self) -> Option<u64> {
        RESTORE_OFFSETS_PER_MILLI
            .checked_mul(self.planned_at_millis)?
            .checked_add(self.gap)
    }
}

/// One broker's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerReport<B> {
    /// Whether it leads the current epoch.
    pub(crate) leads: bool,
    /// The brokers of the newest membership of the record's epoch in its
    /// copies, both halves of a joint one.
    pub(crate) names: BTreeSet<B>,
    /// Both halves of every membership entry of the record's epoch in every
    /// copy read, uncommitted ones included. `None` when not read.
    pub(crate) memberships: Option<Vec<BTreeSet<VoterId<B>>>>,
    pub(crate) copies: BrokerCopies,
}

/// Which of a broker's copies.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Source {
    Current,
    /// A kept copy, by its directory.
    Kept(PathBuf),
}

/// A voter of the next epoch: the broker under its current copy id, and the
/// copy on it that holds the base.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Voter<B> {
    pub(crate) broker: B,
    pub(crate) copy: u64,
    pub(crate) source: Source,
}

/// Where a voter set of the epoch comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VoterSource<B> {
    /// [`GroupRecord::seed`].
    Seed,
    /// [`GroupRecord::ordered`].
    Ordered,
    /// A membership entry read from this broker's copies.
    Membership(B),
}

/// Why no copy is vouched for past its saved commit or the floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Marked<B> {
    /// A source of the epoch's voter sets is not known.
    Unknown(VoterSource<B>),
    /// The brokers whose votes are not known are a majority of this voter
    /// set, so a term could have been elected without any vote read.
    Majority {
        from: VoterSource<B>,
        set: BTreeSet<VoterId<B>>,
        unknown: BTreeSet<VoterId<B>>,
    },
}

/// The next epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReformPlan<B> {
    pub(crate) epoch: u64,
    /// The base's last log id, `None` for an empty base.
    pub(crate) base_last: Option<EpochLogId>,
    /// The copy the base was taken from.
    pub(crate) source: (B, Source),
    pub(crate) voters: Vec<Voter<B>>,
    /// Every other broker read.
    pub(crate) learners: Vec<B>,
    /// Above the base, the offset floor, every reservation read, every
    /// earlier epoch's first index, every record a copy of the incarnation
    /// holds, and every floor a copy carries. For a restore, also at least
    /// the point plus the gap.
    pub(crate) first_index: u64,
    /// The restore's gap, when the plan is one.
    pub(crate) offset_gap: Option<u64>,
    /// The highest key of any vote or entry read, and of the floor.
    pub(crate) top: Key,
    /// Set when the voters whose votes are unknown (dead, no vote of the
    /// epoch read, a copy of the epoch a restore rotated, or a copy id the
    /// broker no longer runs and no power cut leads from) are a majority of
    /// some voter set of the epoch, or a voter set is not known. No copy
    /// is then vouched for past its saved commit or the floor.
    pub(crate) marked: Option<Marked<B>>,
    /// Unmarked, a vote or the floor is above every entry read, and that
    /// cut a copy below what it holds.
    pub(crate) cut_by_vote: bool,
    /// The highest floor any copy read holds, with its trim's headers.
    /// Every copy starts with it ([`super::Start::floor`]).
    pub(crate) floor: Option<CarriedFloor>,
}

impl<B> ReformPlan<B> {
    /// Whether applying it can drop acknowledged records a reached disk
    /// still holds, so the operator has to accept the loss by name.
    pub(crate) fn needs_accept_loss(&self) -> bool {
        self.marked.is_some() || self.cut_by_vote
    }

    pub(crate) fn base_end(&self) -> u64 {
        self.base_last.map_or(0, |last| last.index + 1)
    }
}

/// Why no plan is made. Each is checked before any vouch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Refused<B> {
    /// The shard has no record of the brokers placed in its epoch.
    NoPlacedRecord,
    /// A broker placed in the epoch neither answered nor is declared dead.
    Unaccounted(B),
    /// A broker leads the current epoch.
    Leads(B),
    /// A broker's membership names a voter neither placed nor dead.
    OffRecord { broker: B, names: B },
    /// A copy of this incarnation is at a later epoch than the record.
    EpochAhead { broker: B, epoch: u64 },
    /// A copy's recorded bases differ from the control plane's.
    BasesDiffer(B),
    /// A copy holds a record inside a re-form's gap.
    RecordInGap { broker: B, index: u64 },
    /// A copy was not asked for its term at an index the plan needs.
    NotProbed { broker: B, index: u64 },
    /// The base ends at an index the source copy purged, so its term is
    /// not on that disk.
    Purged { broker: B, index: u64 },
    /// A restore's base does not end at its backup point.
    NotAtPoint { base_end: u64, point: u64 },
    /// Every index is reserved.
    NoIndexLeft,
    /// A restore's time line at its plan time is past the last index.
    RestoreTooLate { planned_at_millis: u64 },
    /// A restore's time line plus gap is not far enough above `given`, the
    /// floor or an earlier epoch's first index, to clear what that epoch
    /// can have given out (`restore_line_clears`).
    LineBehind { line: u64, given: u64 },
    /// No copy was read.
    NothingRead,
    /// No broker with a copy id holds the base, so the epoch would have no
    /// voter.
    NoVoters,
    /// A re-form of one was asked for a group another broker was or could
    /// have been a voter of.
    NotAlone,
}

/// One copy as the rule sees it.
struct Copy<'a, B> {
    broker: &'a B,
    source: Source,
    read: &'a CopyRead,
    /// `None` for a copy of another incarnation: it vouches for nothing.
    epoch: Option<u64>,
    end: u64,
    last: Option<Key>,
}

/// The indexes a plan may ask each copy's term at: one below every place a
/// base can end (a saved commit, a log's end, the floor, an epoch's base
/// end) and the floor's index. A survey reads them with
/// [`super::read_copies`] before [`plan_reform`].
pub(crate) fn probes<B>(
    record: &GroupRecord<B>,
    reports: &BTreeMap<B, BrokerReport<B>>,
) -> BTreeSet<u64> {
    let mut ends: BTreeSet<u64> = record.bases.iter().map(EpochBase::base_end).collect();
    let mut out = BTreeSet::new();
    if let Some(floor) = record.floor {
        out.insert(floor.index);
    }
    for report in reports.values() {
        for read in std::iter::once(&report.copies.current).chain(&report.copies.kept) {
            ends.insert(read.committed.map_or(0, |c| c + 1));
            ends.insert(read.last.map_or(0, |last| last.index + 1));
        }
    }
    out.extend(ends.into_iter().filter_map(|end| end.checked_sub(1)));
    out
}

/// The plan for the next epoch, by the rule in `ReformKept`.
#[cfg(test)]
pub(crate) fn plan_reform<B: Ord + Clone>(
    record: &GroupRecord<B>,
    reports: &BTreeMap<B, BrokerReport<B>>,
) -> Result<ReformPlan<B>, Refused<B>> {
    plan_reform_as(record, reports, false)
}

/// [`plan_reform`], or with `alone` the plan of a group whose only broker
/// re-forms it by itself (`AloneWhole` in `FelixRaftReform`). Then every
/// copy vouches for its whole log: a lone voter committed every entry by
/// itself, and no other disk can hold a newer one, so cutting at the saved
/// commit would drop synced records for nothing. Refused unless one broker
/// is placed, none is dead, and every voter set read names only it.
pub(crate) fn plan_reform_as<B: Ord + Clone>(
    record: &GroupRecord<B>,
    reports: &BTreeMap<B, BrokerReport<B>>,
    alone: bool,
) -> Result<ReformPlan<B>, Refused<B>> {
    let placed = record.placed.as_ref().ok_or(Refused::NoPlacedRecord)?;
    if alone && !is_alone(record, reports) {
        return Err(Refused::NotAlone);
    }
    let epoch = record.bases.len() as u64;
    let live: BTreeMap<&B, &BrokerReport<B>> = reports
        .iter()
        .filter(|(broker, _)| !record.dead.contains(*broker))
        .collect();
    for broker in placed.difference(&record.dead) {
        if !live.contains_key(broker) {
            return Err(Refused::Unaccounted(broker.clone()));
        }
    }
    for (broker, report) in &live {
        if report.leads {
            return Err(Refused::Leads((*broker).clone()));
        }
        if let Some(names) = report
            .names
            .iter()
            .find(|name| !placed.contains(*name) && !record.dead.contains(*name))
        {
            return Err(Refused::OffRecord {
                broker: (*broker).clone(),
                names: names.clone(),
            });
        }
    }

    let rule = Rule { record, epoch };
    let mut copies = Vec::new();
    for (broker, report) in &live {
        let current = std::iter::once((Source::Current, &report.copies.current));
        let kept = report
            .copies
            .kept
            .iter()
            .map(|read| (Source::Kept(read.path.clone()), read));
        for (source, read) in current.chain(kept) {
            copies.push(rule.copy(broker, source, read)?);
        }
    }

    // T over this incarnation's votes and entries, and the floor.
    let entries = copies.iter().filter_map(|c| c.last).max();
    let votes = copies
        .iter()
        .filter_map(|c| Some((c.epoch?, c.read.vote.unwrap_or(0))))
        .max();
    let floor = record.floor.map(|floor| (floor.epoch, floor.term));
    let top = entries.max(votes).max(floor).unwrap_or((0, 0));

    // A term above every vote read needs a majority of some voter set of
    // the epoch, and votes are fsynced before the reply and kept across a
    // power cut. So unless the unknown votes are a majority of one, the top
    // term's copy holds every committed entry. The placed set is never a
    // voter set: learners inflate it.
    let unknown_vote = |(broker, id): &VoterId<B>| {
        let Some(report) = live.get(broker) else {
            return true;
        };
        let current = &report.copies.current;
        let kept: Vec<&CopyRead> = report
            .copies
            .kept
            .iter()
            .filter(|read| rule.copy_epoch(read) == Some(epoch))
            .collect();
        if kept
            .iter()
            .any(|read| read.rotated_by == Some(RotatedBy::Restore))
        {
            return true;
        }
        let Some(now) = current.copy else {
            return true;
        };
        // An older id speaks only through power cuts, each kept with its
        // newest vote, that lead to the copy now running. A replaced disk
        // keeps no such chain, so its old id stays unknown even after the
        // new copy votes.
        let mut at = *id;
        for _ in 0..=kept.len() {
            if at == now {
                break;
            }
            let Some(to) = kept.iter().find_map(|read| match read.rotated_by {
                Some(RotatedBy::PowerCut { to }) if read.copy == Some(at) => Some(to),
                _ => None,
            }) else {
                return true;
            };
            at = to;
        }
        if at != now {
            return true;
        }
        // A copy that never voted could be one that lost its votes, unless
        // a power cut rotated into it: the kept copy holds its newest vote.
        // Only while it has none of its own; once it votes, that vote is
        // known only if the copy reads as of this epoch. A power cut speaks
        // for the copy it rotated into and no later one: a wiped directory
        // or an empty rotation leaves no kept copy, so an older power cut
        // would otherwise vouch for the copy after it.
        if current.vote.is_none() {
            return !kept
                .iter()
                .any(|read| read.rotated_by == Some(RotatedBy::PowerCut { to: now }));
        }
        rule.copy_epoch(current) != Some(epoch)
    };
    let marked = marked_by(record, &live, unknown_vote);

    let mut vouched = Vec::with_capacity(copies.len());
    let mut cut_by_vote = false;
    for copy in &copies {
        let Some(copy_epoch) = copy.epoch else {
            vouched.push(0);
            continue;
        };
        let cap = rule.cap(copy_epoch).min(copy.end);
        let mut safe = copy.read.committed.map_or(0, |c| c + 1);
        if let Some(floor) = record.floor
            && rule.holds(copy, floor.index, (floor.epoch, floor.term), true)?
        {
            safe = safe.max(floor.index + 1);
        }
        let whole = alone || (marked.is_none() && copy.last == Some(top));
        let vouch = if whole { copy.end } else { safe }.min(cap);
        if !alone && marked.is_none() && entries.is_some_and(|entries| entries < top) && cap > vouch
        {
            cut_by_vote = true;
        }
        vouched.push(vouch);
    }

    // The longest vouched prefix; ties go to the higher last key, then the
    // longer log, then the order the copies were read in.
    let (pick, _) = copies
        .iter()
        .zip(&vouched)
        .enumerate()
        .max_by(|(n, (a, va)), (m, (b, vb))| {
            va.cmp(vb)
                .then(a.last.cmp(&b.last))
                .then(a.end.cmp(&b.end))
                .then(m.cmp(n))
        })
        .ok_or(Refused::NothingRead)?;
    let src = &copies[pick];
    let mut base_end = vouched[pick];
    // A gap holds no record, so a base never ends inside one.
    while let Some(index) = base_end.checked_sub(1)
        && let EpochAt::Gap(e) = rule.epoch_at(src, index)
    {
        base_end = record.bases[e as usize - 1].base_end();
    }
    let base_last = match base_end.checked_sub(1) {
        None => None,
        Some(index) => {
            let EpochAt::Entry(at) = rule.epoch_at(src, index) else {
                unreachable!("the loop above leaves no gap at the base's end");
            };
            let term = rule.term(src, index)?.ok_or_else(|| Refused::NotProbed {
                broker: src.broker.clone(),
                index,
            })?;
            Some(EpochLogId {
                index,
                epoch: at,
                term,
            })
        }
    };

    // Below the point every record was acknowledged; a base that ends
    // anywhere else either drops some of them or keeps records past it.
    if let Some(restore) = record.restore
        && base_end != restore.end
    {
        return Err(Refused::NotAtPoint {
            base_end,
            point: restore.end,
        });
    }

    let mut voters = Vec::new();
    let mut learners = Vec::new();
    for (broker, report) in &live {
        let mut best: Option<&Copy<'_, B>> = None;
        for copy in copies.iter().filter(|c| c.broker == *broker) {
            let holds = match base_last {
                None => copy.epoch.is_some(),
                Some(last) => rule.holds(copy, last.index, (last.epoch, last.term), false)?,
            };
            let better = best.is_none_or(|best| {
                (copy.last, copy.end, copy.read.copy) > (best.last, best.end, best.read.copy)
            });
            if holds && better {
                best = Some(copy);
            }
        }
        // A broker with no copy file has no id a membership could name.
        match (best, report.copies.current.copy) {
            (Some(best), Some(copy)) => voters.push(Voter {
                broker: (*broker).clone(),
                copy,
                source: best.source.clone(),
            }),
            _ => learners.push((*broker).clone()),
        }
    }

    if voters.is_empty() {
        return Err(Refused::NoVoters);
    }

    let reserved = copies
        .iter()
        .filter_map(|c| c.read.reserved)
        .max()
        .map(|r| r.checked_add(1).ok_or(Refused::NoIndexLeft))
        .transpose()?;
    // The new epoch keeps the highest floor any copy read holds, so reads
    // below it stay refused, and gives out nothing at or below the trim that
    // set it: a copy that installed a purge snapshot holds neither record.
    let floor = copies
        .iter()
        .filter_map(|c| c.read.floor.clone())
        .max_by_key(|floor| floor.floor);
    let carried = floor
        .as_ref()
        .map(|floor| floor.floor.max(floor.offset.saturating_add(1)));
    let floor_end = record
        .floor
        .map(|floor| floor.index)
        .max(record.offset_floor)
        .map(|index| index.checked_add(1).ok_or(Refused::NoIndexLeft))
        .transpose()?;
    // Never below an index an earlier epoch gave out: its first index, or
    // a record any copy of the incarnation holds.
    let earlier = record.bases.iter().map(|base| base.first_index).max();
    let held = copies
        .iter()
        .filter(|c| c.epoch.is_some())
        .map(|c| c.end)
        .max();
    let first_index = base_end
        .max(floor_end.unwrap_or(0))
        .max(reserved.unwrap_or(0))
        .max(earlier.unwrap_or(0))
        .max(held.unwrap_or(0))
        .max(carried.unwrap_or(0));
    // Offsets acknowledged after the copy, by the lost cluster or by an
    // earlier restore from any backup, are in no restored file. All of
    // them are below the time line at the plan's time.
    let first_index = match record.restore {
        Some(restore) => {
            let line = restore.first_index().ok_or(Refused::RestoreTooLate {
                planned_at_millis: restore.planned_at_millis,
            })?;
            let given = record
                .floor
                .map(|floor| floor.index)
                .max(record.offset_floor)
                .max(earlier);
            if let Some(given) = given
                && !restore_line_clears(line, restore.gap, given)
            {
                return Err(Refused::LineBehind { line, given });
            }
            first_index.max(line)
        }
        None => first_index,
    };

    Ok(ReformPlan {
        epoch: epoch + 1,
        base_last,
        source: (src.broker.clone(), src.source.clone()),
        voters,
        learners,
        first_index,
        offset_gap: record.restore.map(|restore| restore.gap),
        top,
        marked,
        cut_by_vote,
        floor,
    })
}

/// Whether a restore whose time line plus `gap` is `line` starts past every
/// offset an epoch whose first index or floor is `given` can have given
/// out.
///
/// Being above `given` is not enough: the clock that planned that epoch may
/// have run ahead, or planned it with a larger gap, so its start says little
/// about how long it has run. Say it ran `D` ahead with gap `g0 <=
/// MAX_OFFSET_GAP`, and this plan comes `E` of true time later. Then `line -
/// given = R (E - D) + gap - g0`, and the epoch gave out at most `R/2 x E`
/// past `given`. Requiring `line - given > MIN_OFFSET_GAP + MAX_OFFSET_GAP -
/// gap` covers that for any `g0` and any `D` up to `MIN_OFFSET_GAP / R`
/// (about four minutes). With the default gap it is the abandon's bound.
fn restore_line_clears(line: u64, gap: u64, given: u64) -> bool {
    let margin = MIN_OFFSET_GAP.saturating_add(MAX_OFFSET_GAP.saturating_sub(gap));
    line > given.saturating_add(margin)
}

/// Whether one broker is placed, none is dead, and every voter set the
/// record holds or a report read names only that broker.
fn is_alone<B: Ord + Clone>(
    record: &GroupRecord<B>,
    reports: &BTreeMap<B, BrokerReport<B>>,
) -> bool {
    let Some(placed) = &record.placed else {
        return false;
    };
    let mut only = placed.iter();
    let (Some(me), None) = (only.next(), only.next()) else {
        return false;
    };
    let mine = |set: &BTreeSet<VoterId<B>>| set.iter().all(|(broker, _)| broker == me);
    record.dead.is_empty()
        && reports.keys().all(|broker| broker == me)
        && record.seed.as_ref().is_some_and(mine)
        && record.ordered.iter().flatten().all(mine)
        && reports.values().all(|report| {
            report
                .memberships
                .as_ref()
                .is_some_and(|sets| sets.iter().all(mine))
        })
}

/// The first voter set of the epoch whose majority has unknown votes, or
/// the first source not known.
fn marked_by<B: Ord + Clone>(
    record: &GroupRecord<B>,
    live: &BTreeMap<&B, &BrokerReport<B>>,
    unknown_vote: impl Fn(&VoterId<B>) -> bool,
) -> Option<Marked<B>> {
    let mut sets: Vec<(VoterSource<B>, &BTreeSet<VoterId<B>>)> = Vec::new();
    match &record.seed {
        Some(seed) if !seed.is_empty() => sets.push((VoterSource::Seed, seed)),
        _ => return Some(Marked::Unknown(VoterSource::Seed)),
    }
    let Some(ordered) = &record.ordered else {
        return Some(Marked::Unknown(VoterSource::Ordered));
    };
    sets.extend(ordered.iter().map(|set| (VoterSource::Ordered, set)));
    for (broker, report) in live {
        let Some(memberships) = &report.memberships else {
            return Some(Marked::Unknown(VoterSource::Membership((*broker).clone())));
        };
        sets.extend(
            memberships
                .iter()
                .map(|set| (VoterSource::Membership((*broker).clone()), set)),
        );
    }
    sets.into_iter().find_map(|(from, set)| {
        let unknown: BTreeSet<VoterId<B>> =
            set.iter().filter(|b| unknown_vote(b)).cloned().collect();
        (unknown.len() * 2 > set.len()).then(|| Marked::Majority {
            from,
            set: set.clone(),
            unknown,
        })
    })
}

struct Rule<'a, B> {
    record: &'a GroupRecord<B>,
    epoch: u64,
}

impl<B: Clone> Rule<'_, B> {
    /// The epoch of a copy of this incarnation, `None` for another's.
    fn copy_epoch(&self, read: &CopyRead) -> Option<u64> {
        match &read.epoch {
            None => Some(0),
            Some(file) if file.incarnation == self.record.incarnation => Some(file.epoch()),
            Some(_) => None,
        }
    }

    fn copy<'a>(
        &self,
        broker: &'a B,
        source: Source,
        read: &'a CopyRead,
    ) -> Result<Copy<'a, B>, Refused<B>> {
        let epoch = self.copy_epoch(read);
        if let (Some(e), Some(file)) = (epoch, &read.epoch) {
            if e > self.epoch {
                return Err(Refused::EpochAhead {
                    broker: broker.clone(),
                    epoch: e,
                });
            }
            if file.bases[..] != self.record.bases[..file.bases.len()] {
                return Err(Refused::BasesDiffer(broker.clone()));
            }
        }
        let mut copy = Copy {
            broker,
            source,
            read,
            epoch,
            end: read.last.map_or(0, |last| last.index + 1),
            last: None,
        };
        if epoch.is_some()
            && let Some(last) = read.last
        {
            match self.epoch_at(&copy, last.index) {
                EpochAt::Entry(e) => copy.last = Some((e, last.term)),
                EpochAt::Gap(_) => {
                    return Err(Refused::RecordInGap {
                        broker: broker.clone(),
                        index: last.index,
                    });
                }
            }
        }
        Ok(copy)
    }

    fn epoch_at(&self, copy: &Copy<'_, B>, index: u64) -> EpochAt {
        EpochFile::epoch_at(&self.record.bases, copy.epoch.unwrap_or(0), index)
    }

    /// An epoch-`e` copy agrees with today's log only below the lowest base
    /// end of the epochs after `e`: each later epoch put its gap and its
    /// own entries above it.
    fn cap(&self, e: u64) -> u64 {
        self.record
            .bases
            .iter()
            .skip(usize::try_from(e).unwrap_or(usize::MAX))
            .map(EpochBase::base_end)
            .min()
            .unwrap_or(u64::MAX)
    }

    fn term(&self, copy: &Copy<'_, B>, index: u64) -> Result<Option<u64>, Refused<B>> {
        if let Some(last) = copy.read.last
            && last.index == index
        {
            return Ok(Some(last.term));
        }
        if index >= copy.end {
            return Ok(None);
        }
        if copy.read.purged.is_some_and(|purged| index < purged) {
            return Err(Refused::Purged {
                broker: copy.broker.clone(),
                index,
            });
        }
        copy.read
            .terms
            .get(&index)
            .copied()
            .ok_or_else(|| Refused::NotProbed {
                broker: copy.broker.clone(),
                index,
            })
    }

    /// Whether the copy holds the entry with `key` at `index`. For the floor
    /// (`gap_counts`), a later epoch's gap there stands for it: everything
    /// below that epoch's first index was its committed base. Only for a
    /// copy with a record past the gap: the start must save each voter's
    /// commit at or above the base end (#2640).
    fn holds(
        &self,
        copy: &Copy<'_, B>,
        index: u64,
        key: Key,
        gap_counts: bool,
    ) -> Result<bool, Refused<B>> {
        // A purged entry's term is gone, so the copy cannot show it holds
        // it; it joins as a learner and catches up.
        if copy.epoch.is_none()
            || index >= copy.end
            || copy.read.purged.is_some_and(|purged| index < purged)
        {
            return Ok(false);
        }
        Ok(match self.epoch_at(copy, index) {
            EpochAt::Gap(e) => gap_counts && e > key.0,
            EpochAt::Entry(e) => e == key.0 && self.term(copy, index)? == Some(key.1),
        })
    }
}
