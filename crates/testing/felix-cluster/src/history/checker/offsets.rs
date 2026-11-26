//! Rule 22: no offset of a list is ever held as two records.
//!
//! Every observation of `(offset, value)` in the history is held against
//! every other: the final read, every read (from whichever broker served it),
//! every event delivered to every subscriber across all its sessions, and the
//! offset a `Quorum` append was acknowledged at. The first two observations
//! of an offset that disagree are a violation, whichever sources they came
//! from. Seeing one record many times is fine, and an offset nobody saw (a
//! term's blank entry, a membership change) says nothing.
//!
//! On a `Quorum` list with a final read, rules 1, 3 and 10 already hold
//! every observation against the final log, so any two that disagree are
//! reported there and this rule leaves the list alone. It judges the rest:
//! every `Leader` list, whose readers are served only committed records, and
//! a list whose final read could not be taken. A `Leader` acknowledgement may
//! be lost across a change of leader and its offset taken by the next record,
//! so it is not an observation.

use std::collections::{BTreeMap, BTreeSet};

use super::super::model::{Action, AppendOutcome, Consistency, Element, History};
use super::{Rule, Violation};

/// Where an observation came from.
#[derive(Debug, Clone, Copy)]
enum Seen {
    /// The offset op `.0` was acknowledged at.
    Acknowledged(usize),
    Final,
    Read(usize),
    /// Event `.1` delivered to the subscription at index `.0`.
    Delivered(usize, usize),
}

/// Check every list in `history`.
pub(super) fn check(history: &History, out: &mut Vec<Violation>) {
    for (list, spec) in &history.lists {
        let quorum = spec.consistency == Consistency::Quorum;
        if quorum && history.final_reads.contains_key(list) {
            continue;
        }
        let mut held: BTreeMap<u64, (u64, Seen)> = BTreeMap::new();
        let mut reported: BTreeSet<u64> = BTreeSet::new();
        let mut observe = |offset: u64, value: u64, seen: Seen, out: &mut Vec<Violation>| {
            if offset < spec.base {
                return;
            }
            let &mut (first, by) = held.entry(offset).or_insert((value, seen));
            if first != value && reported.insert(offset) {
                out.push(Violation {
                    rule: Rule::OffsetReuse,
                    list: list.clone(),
                    ops: [by, seen].iter().filter_map(Seen::op).collect(),
                    explanation: format!(
                        "offset {offset} holds {first} in {} but {value} in {}",
                        describe(history, by),
                        describe(history, seen)
                    ),
                });
            }
        };

        if quorum {
            for (index, op) in history.ops.iter().enumerate() {
                if let Action::Append {
                    list: appended,
                    value,
                    outcome:
                        AppendOutcome::Ok {
                            offset: Some(offset),
                        },
                } = &op.action
                    && appended == list
                {
                    observe(*offset, *value, Seen::Acknowledged(index), out);
                }
            }
        }
        let mut each = |elements: &[Element], seen: &dyn Fn(usize) -> Seen| {
            for (at, element) in elements.iter().enumerate() {
                observe(element.offset, element.value, seen(at), out);
            }
        };
        if let Some(final_read) = history.final_reads.get(list) {
            each(final_read, &|_| Seen::Final);
        }
        for (index, op) in history.ops.iter().enumerate() {
            if let Action::Read {
                list: read,
                observed,
            } = &op.action
                && read == list
            {
                each(observed, &|_| Seen::Read(index));
            }
        }
        for (index, subscription) in history.subscriptions.iter().enumerate() {
            if subscription.list == *list {
                each(&subscription.delivered, &|at| Seen::Delivered(index, at));
            }
        }
    }
}

impl Seen {
    fn op(&self) -> Option<usize> {
        match *self {
            Seen::Acknowledged(index) | Seen::Read(index) => Some(index),
            Seen::Final | Seen::Delivered(..) => None,
        }
    }
}

fn describe(history: &History, seen: Seen) -> String {
    match seen {
        Seen::Acknowledged(index) => {
            format!(
                "the acknowledgement of op #{index} ({})",
                history.ops[index]
            )
        }
        Seen::Final => "the final read".to_string(),
        Seen::Read(index) => format!("op #{index} ({})", history.ops[index]),
        Seen::Delivered(index, at) => format!(
            "event {at} delivered to subscriber {}",
            history.subscriptions[index].subscriber
        ),
    }
}

#[cfg(test)]
mod tests;
