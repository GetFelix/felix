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

/// An (epoch, term) pair. Every term of an epoch is above every term of an
/// earlier one.
pub(crate) type Key = (u64, u64);

/// What the control plane holds for the group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GroupRecord<B> {
    pub(crate) incarnation: u64,
    /// How each re-form so far started its epoch; the group is at epoch
    /// `bases.len()`.
    pub(crate) bases: Vec<EpochBase>,
    /// The highest commit a leader reported, if any.
    pub(crate) floor: Option<EpochLogId>,
    /// Every broker placed in the current epoch, removed ones included.
    /// `None` when the shard has no such record.
    pub(crate) placed: Option<BTreeSet<B>>,
    /// Brokers declared dead. Their copies are not read.
    pub(crate) dead: BTreeSet<B>,
}

/// One broker's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrokerReport<B> {
    /// Whether it leads the current epoch.
    pub(crate) leads: bool,
    /// The voters its newest membership names.
    pub(crate) names: BTreeSet<B>,
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
    /// Above the base, the floor and every reservation read.
    pub(crate) first_index: u64,
    /// The highest key of any vote or entry read, and of the floor.
    pub(crate) top: Key,
    /// A broker placed in the epoch is dead, has no vote of the epoch read,
    /// or has a copy of the epoch a restore rotated. No copy is then
    /// vouched for past its saved commit or the floor.
    pub(crate) marked: bool,
    /// Unmarked, a vote or the floor is above every entry read, and that
    /// cut a copy below what it holds.
    pub(crate) cut_by_vote: bool,
}

impl<B> ReformPlan<B> {
    /// Whether applying it can drop acknowledged records a reached disk
    /// still holds, so the operator has to accept the loss by name.
    pub(crate) fn needs_accept_loss(&self) -> bool {
        self.marked || self.cut_by_vote
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
    /// Every index is reserved.
    NoIndexLeft,
    /// No copy was read.
    NothingRead,
    /// No broker with a copy id holds the base, so the epoch would have no
    /// voter.
    NoVoters,
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
pub(crate) fn plan_reform<B: Ord + Clone>(
    record: &GroupRecord<B>,
    reports: &BTreeMap<B, BrokerReport<B>>,
) -> Result<ReformPlan<B>, Refused<B>> {
    let placed = record.placed.as_ref().ok_or(Refused::NoPlacedRecord)?;
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

    let marked = placed.iter().any(|broker| {
        let Some(report) = live.get(broker) else {
            return true;
        };
        let kept_of_epoch = |by: RotatedBy| {
            report
                .copies
                .kept
                .iter()
                .any(|read| read.rotated_by == Some(by) && rule.copy_epoch(read) == Some(epoch))
        };
        // At epoch 0 no `raft-epoch` proves the copy went through a start,
        // and a wiped disk or an empty rotation leaves no kept copy, so a
        // copy that never voted could be one that lost its votes.
        let lost_votes = epoch == 0 && report.copies.current.vote.is_none();
        ((rule.copy_epoch(&report.copies.current) != Some(epoch) || lost_votes)
            && !kept_of_epoch(RotatedBy::PowerCut))
            || kept_of_epoch(RotatedBy::Restore)
    });

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
        let whole = !marked && copy.last == Some(top);
        let vouch = if whole { copy.end } else { safe }.min(cap);
        if !marked && entries.is_some_and(|entries| entries < top) && cap > vouch {
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
    let floor_end = record.floor.map(|floor| floor.index + 1);
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
        .max(held.unwrap_or(0));

    Ok(ReformPlan {
        epoch: epoch + 1,
        base_last,
        source: (src.broker.clone(), src.source.clone()),
        voters,
        learners,
        first_index,
        top,
        marked,
        cut_by_vote,
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
