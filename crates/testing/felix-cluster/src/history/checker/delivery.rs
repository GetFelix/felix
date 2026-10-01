//! Rules 9 to 11: what each live subscriber was delivered, held against the
//! final log.
//!
//! A subscriber may drop records (its queue is `DropNew`), so a gap is not a
//! violation as long as the offsets show it. What it must not do is go
//! backwards, deliver a record the final log does not hold, hide a record
//! behind `skipped_before`, or stop short of the end of the final log.

use std::collections::{BTreeMap, HashSet};

use super::super::model::{Consistency, Element, History, Subscription, millis};
use super::{Rule, Violation, malformed};

/// Check every subscription in `history`.
pub(super) fn check(history: &History, out: &mut Vec<Violation>) {
    for subscription in &history.subscriptions {
        let Some(spec) = history.lists.get(&subscription.list) else {
            out.push(malformed(
                &subscription.list,
                &format!(
                    "subscriber {} follows a list the history does not declare",
                    subscription.subscriber
                ),
            ));
            continue;
        };
        let mut check = SubscriptionCheck {
            subscription,
            base: spec.base,
            out,
        };
        check.order();
        // Under `Leader` a failover may rewrite a delivered tail. A missing
        // final read is already reported by the list rules.
        if spec.consistency != Consistency::Quorum {
            continue;
        }
        let Some(final_read) = history.final_reads.get(&subscription.list) else {
            continue;
        };
        let log: BTreeMap<u64, u64> = final_read.iter().map(|e| (e.offset, e.value)).collect();
        check.nothing_lost(&log);
        check.nothing_missing(&log);
    }
}

/// How many records of the final log `subscription` visibly dropped: offsets
/// from its start to the last one it was delivered that hold a value, were
/// not delivered, and were not claimed empty by `skipped_before`.
pub(crate) fn dropped(subscription: &Subscription, base: u64, final_read: &[Element]) -> usize {
    let coverage = Coverage::of(subscription, base);
    let Some(last) = coverage.last else {
        return 0;
    };
    final_read
        .iter()
        .filter(|e| e.offset >= subscription.start && e.offset <= last)
        .filter(|e| !coverage.delivered.contains(&e.offset))
        .filter(|e| !coverage.claimed.contains_key(&e.offset))
        .count()
}

/// The rules for one subscription.
struct SubscriptionCheck<'a> {
    subscription: &'a Subscription,
    base: u64,
    out: &'a mut Vec<Violation>,
}

impl SubscriptionCheck<'_> {
    /// Rule 9: offsets strictly increase, from the start and across resumes.
    /// A step back is reported once, then checking carries on from it, so a
    /// replayed range is one finding rather than one per record.
    fn order(&mut self) {
        let mut previous: Option<(usize, Element)> = None;
        for (index, element) in self.in_range() {
            let floor = previous.map_or(self.subscription.start, |(_, p)| p.offset + 1);
            if element.offset < floor {
                let after = match previous {
                    Some((at, p)) => format!(
                        "after offset {} (value {}, {})",
                        p.offset,
                        p.value,
                        self.session(at)
                    ),
                    None => format!("before its start at offset {}", self.subscription.start),
                };
                let explanation = format!(
                    "{} was delivered offset {} (value {}, {}) {after}",
                    self.who(),
                    element.offset,
                    element.value,
                    self.session(index)
                );
                self.push(Rule::DeliveryOrder, explanation);
            }
            previous = Some((index, element));
        }
    }

    /// Rule 10: every delivered record is in the final log, at the same
    /// offset with the same value.
    fn nothing_lost(&mut self, log: &BTreeMap<u64, u64>) {
        let end = log.keys().next_back().copied();
        let mut reported = HashSet::new();
        for (index, element) in self.in_range() {
            let problem = match log.get(&element.offset) {
                Some(&value) if value == element.value => continue,
                Some(&value) => format!("the final log has {value} there"),
                None => match end {
                    Some(end) if element.offset <= end => {
                        "the final log holds no record there".to_string()
                    }
                    Some(end) => format!("the final log ends at offset {end}"),
                    None => "the final log is empty".to_string(),
                },
            };
            if !reported.insert(element.offset) {
                continue;
            }
            let explanation = format!(
                "{} was delivered {} at offset {} ({}), but {problem}",
                self.who(),
                element.value,
                element.offset,
                self.session(index)
            );
            self.push(Rule::LostDelivery, explanation);
        }
    }

    /// Rule 11: every record of the final log from the start on was delivered
    /// or visibly dropped, up to the end of the final log.
    fn nothing_missing(&mut self, log: &BTreeMap<u64, u64>) {
        let coverage = Coverage::of(self.subscription, self.base);
        for (&offset, &index) in &coverage.claimed {
            let Some(&value) = log.get(&offset) else {
                continue;
            };
            let element = self.subscription.delivered[index];
            let explanation = format!(
                "{} was told offsets {}..{} hold no event with offset {} ({}), but the final \
                 log has {value} at offset {offset}",
                self.who(),
                element.offset - element.skipped_before,
                element.offset,
                element.offset,
                self.session(index)
            );
            self.push(Rule::MissingDelivery, explanation);
        }

        let from = coverage
            .last
            .map_or(self.subscription.start, |last| last + 1);
        let undelivered: Vec<(u64, u64)> = log
            .range(from.max(self.subscription.start)..)
            .map(|(&offset, &value)| (offset, value))
            .collect();
        if let (Some(&(first, value)), Some(&(end, _))) = (undelivered.first(), undelivered.last())
        {
            let reached = match coverage.last {
                Some(last) => format!("its last delivery was offset {last}"),
                None => format!(
                    "it was delivered nothing from offset {}",
                    self.subscription.start
                ),
            };
            let explanation = format!(
                "{} stopped short: {reached} and it was waiting for offset {}, but the final log \
                 runs to offset {end}; {} record(s) never delivered, the first {value} at offset \
                 {first}",
                self.who(),
                self.subscription.next,
                undelivered.len()
            );
            self.push(Rule::MissingDelivery, explanation);
        }
    }

    /// Deliveries at or above the base, with their index in the record.
    fn in_range(&self) -> Vec<(usize, Element)> {
        self.subscription
            .delivered
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, e)| e.offset >= self.base)
            .collect()
    }

    fn who(&self) -> String {
        format!("subscriber {}", self.subscription.subscriber)
    }

    /// Which session delivered the event at `index`, and where it began.
    fn session(&self, index: usize) -> String {
        let resumes = &self.subscription.resumes;
        match resumes.iter().rposition(|resume| resume.first <= index) {
            None => format!("first session, from offset {}", self.subscription.start),
            Some(at) => {
                let resume = &resumes[at];
                format!(
                    "session {}, resumed from offset {} at {}",
                    at + 2,
                    resume.from,
                    millis(resume.at)
                )
            }
        }
    }

    fn push(&mut self, rule: Rule, explanation: String) {
        self.out.push(Violation {
            rule,
            list: self.subscription.list.clone(),
            ops: vec![],
            explanation,
        });
    }
}

/// Which offsets a subscription accounts for.
struct Coverage {
    /// Offsets it was delivered.
    delivered: HashSet<u64>,
    /// Offsets it was told hold no event, each with the index of the event
    /// that said so.
    claimed: BTreeMap<u64, usize>,
    /// The highest offset it was delivered.
    last: Option<u64>,
}

impl Coverage {
    fn of(subscription: &Subscription, base: u64) -> Self {
        let mut coverage = Coverage {
            delivered: HashSet::new(),
            claimed: BTreeMap::new(),
            last: None,
        };
        for (index, element) in subscription.delivered.iter().enumerate() {
            if element.offset < base {
                continue;
            }
            coverage.delivered.insert(element.offset);
            coverage.last = coverage.last.max(Some(element.offset));
            let skipped = element.offset.saturating_sub(element.skipped_before)..element.offset;
            for offset in skipped.filter(|&o| o >= subscription.start) {
                coverage.claimed.entry(offset).or_insert(index);
            }
        }
        coverage
    }
}

#[cfg(test)]
mod tests;
