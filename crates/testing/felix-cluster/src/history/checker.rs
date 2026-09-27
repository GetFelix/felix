//! Checks a recorded [`History`] against what a replicated append-only log
//! promises, in the style of Elle's list-append checker.
//!
//! Each list is a log with explicit offsets, so unlike Elle this never has to
//! infer an order: every read says where each value sits. That turns most
//! rules into lookups against the final read, and real-time order into one
//! sorted sweep per list.
//!
//! The final read is the reference, so it has to be whole: it must start at
//! the list's base offset and have no holes. A final read that is not whole is
//! reported on its own rule rather than as a pile of lost writes, because the
//! fault is in the read, not necessarily in the log.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use super::model::{
    Action, AppendOutcome, Consistency, Element, FOREIGN_PAYLOAD, FaultEvent, History, ListSpec,
    millis,
};

/// How many violations the report prints in full. The rest are counted, since
/// one real bug tends to produce a cascade of the same finding.
const PRINTED_VIOLATIONS: usize = 25;

/// What [`check`] found.
#[derive(Debug, Clone, Default)]
pub struct Report {
    pub violations: Vec<Violation>,
    /// Copied from the history so the report can be read on its own.
    pub faults: Vec<FaultEvent>,
}

/// One broken rule, with enough to find it in the history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub rule: Rule,
    pub list: String,
    /// Indices into [`History::ops`] of the operations involved.
    pub ops: Vec<usize>,
    pub explanation: String,
}

/// The rules, numbered as the history-checker doc numbers them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Rule {
    /// 1. An acknowledged append is missing from the final read.
    LostWrite,
    /// 2. A value sits at two offsets.
    Duplicate,
    /// 3. A read is not a prefix of the final log: an offset holds different
    ///    values in two reads, a read goes backwards, or a read saw past the
    ///    end of the final log.
    NotAPrefix,
    /// 4. An append acknowledged before another began sits after it.
    RealTimeOrder,
    /// 5. A read saw a value no append wrote, or saw it before it was written.
    Phantom,
    /// 6. An append answered as definitely failed is in a read.
    FailedWriteVisible,
    /// The final read has a hole, so the lost-write rule cannot be trusted.
    IncompleteFinalRead,
    /// The history itself is malformed: the recorder is wrong, not the broker.
    MalformedHistory,
}

impl Report {
    /// Whether no rule was broken.
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }

    /// Whether any violation was found under `rule`.
    pub fn has(&self, rule: Rule) -> bool {
        self.violations.iter().any(|v| v.rule == rule)
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.violations.is_empty() {
            return write!(f, "history is valid");
        }
        let mut counts: BTreeMap<Rule, usize> = BTreeMap::new();
        for violation in &self.violations {
            *counts.entry(violation.rule).or_default() += 1;
        }
        let counts: Vec<String> = counts
            .iter()
            .map(|(rule, n)| format!("{rule} x{n}"))
            .collect();
        writeln!(
            f,
            "{} violation(s): {}",
            self.violations.len(),
            counts.join(", ")
        )?;
        for violation in self.violations.iter().take(PRINTED_VIOLATIONS) {
            writeln!(f, "  {violation}")?;
        }
        if self.violations.len() > PRINTED_VIOLATIONS {
            writeln!(
                f,
                "  ... and {} more",
                self.violations.len() - PRINTED_VIOLATIONS
            )?;
        }
        if !self.faults.is_empty() {
            writeln!(f, "fault timeline:")?;
            for fault in &self.faults {
                writeln!(f, "  {} {}", millis(fault.at), fault.what)?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.rule, self.list, self.explanation)
    }
}

impl Rule {
    /// The short name a report prints.
    pub fn as_str(self) -> &'static str {
        match self {
            Rule::LostWrite => "1 lost-write",
            Rule::Duplicate => "2 duplicate",
            Rule::NotAPrefix => "3 not-a-prefix",
            Rule::RealTimeOrder => "4 real-time-order",
            Rule::Phantom => "5 phantom",
            Rule::FailedWriteVisible => "6 failed-write-visible",
            Rule::IncompleteFinalRead => "incomplete-final-read",
            Rule::MalformedHistory => "malformed-history",
        }
    }
}

impl fmt::Display for Rule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Check every rule on every list.
pub fn check(history: &History) -> Report {
    let mut violations = Vec::new();
    let appends = index_appends(history, &mut violations);
    for list in history.final_reads.keys() {
        if !history.lists.contains_key(list) {
            violations.push(malformed(
                list,
                "a final read for a list the history does not declare",
            ));
        }
    }
    for (list, spec) in &history.lists {
        let mut checker = ListCheck {
            history,
            list,
            spec: *spec,
            appends: &appends,
            out: &mut violations,
        };
        checker.run();
    }
    Report {
        violations,
        faults: history.faults.clone(),
    }
}

/// Every append by its value. Values are unique by construction, so a repeat
/// means the history cannot say which append a read saw.
fn index_appends(history: &History, out: &mut Vec<Violation>) -> HashMap<u64, usize> {
    let mut appends = HashMap::new();
    for (index, op) in history.ops.iter().enumerate() {
        if op.complete < op.invoke {
            out.push(malformed(
                op.list(),
                &format!("op #{index} completes before it starts: {op}"),
            ));
        }
        if !history.lists.contains_key(op.list()) {
            out.push(malformed(
                op.list(),
                &format!("op #{index} touches an undeclared list: {op}"),
            ));
        }
        if let Action::Append { value, .. } = op.action {
            if value == FOREIGN_PAYLOAD {
                out.push(malformed(
                    op.list(),
                    &format!("op #{index} appends the reserved value"),
                ));
            } else if let Some(first) = appends.insert(value, index) {
                out.push(malformed(
                    op.list(),
                    &format!("value {value} is appended by op #{first} and op #{index}"),
                ));
            }
        }
    }
    appends
}

/// The rules for one list.
struct ListCheck<'a> {
    history: &'a History,
    list: &'a str,
    spec: ListSpec,
    appends: &'a HashMap<u64, usize>,
    out: &'a mut Vec<Violation>,
}

/// Where a read came from: an operation, or the final read.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
    Final,
    Op(usize),
}

impl ListCheck<'_> {
    fn run(&mut self) {
        let Some(final_read) = self.history.final_reads.get(self.list) else {
            self.out.push(malformed(self.list, "no final read"));
            return;
        };
        self.final_read_is_whole(final_read);
        let positions = self.reads_agree(final_read);
        if self.spec.consistency == Consistency::Quorum {
            self.acknowledged_appends_survive(final_read, &positions);
        }
        self.real_time_order(&positions);
    }

    /// The final read starts at the base and has no holes, or nothing that
    /// relies on it being the whole log can be believed.
    fn final_read_is_whole(&mut self, final_read: &[Element]) {
        for (expected, element) in (self.spec.base..).zip(final_read) {
            if element.offset != expected {
                let explanation = if element.offset < expected {
                    format!(
                        "the final read has offset {} where offset {expected} belongs",
                        element.offset
                    )
                } else {
                    format!(
                        "the final read skips from offset {expected} to {}; lost-write results \
                         for this list are unreliable",
                        element.offset
                    )
                };
                self.push(Rule::IncompleteFinalRead, vec![], explanation);
                return;
            }
        }
    }

    /// Rules 2, 3, 5 and 6, which are all about what reads saw. Returns where
    /// each value sits in the final read.
    fn reads_agree(&mut self, final_read: &[Element]) -> HashMap<u64, u64> {
        let final_end = final_read.last().map(|e| e.offset);
        let mut positions: HashMap<u64, u64> = HashMap::new();
        for element in final_read {
            positions.entry(element.value).or_insert(element.offset);
        }

        // The final read goes first, so an offset's first observation is the
        // final log's and a disagreement names the read that strayed from it.
        let mut reads: Vec<(Source, &[Element])> = vec![(Source::Final, final_read)];
        for (index, op) in self.history.ops.iter().enumerate() {
            if let Action::Read { list, observed } = &op.action
                && list == self.list
            {
                reads.push((Source::Op(index), observed));
            }
        }

        let mut value_at: HashMap<u64, (u64, Source)> = HashMap::new();
        let mut offset_of: HashMap<u64, (u64, Source)> = HashMap::new();
        let mut reported: HashSet<(Rule, u64)> = HashSet::new();
        let quorum = self.spec.consistency == Consistency::Quorum;

        for (source, elements) in reads {
            let mut previous: Option<u64> = None;
            for element in elements {
                if element.offset < self.spec.base {
                    continue;
                }
                if let Some(prev) = previous
                    && element.offset <= prev
                {
                    self.push(
                        Rule::NotAPrefix,
                        source.ops(),
                        format!(
                            "{} returned offset {} after offset {prev}",
                            self.describe(source),
                            element.offset
                        ),
                    );
                    break;
                }
                previous = Some(element.offset);

                // Under `Leader` a failover may rewrite an acknowledged tail,
                // so a read that saw the old tail is not wrong.
                if quorum {
                    if let Some(&(other, first)) = value_at.get(&element.offset) {
                        if other != element.value
                            && reported.insert((Rule::NotAPrefix, element.offset))
                        {
                            let mut ops = first.ops();
                            ops.extend(source.ops());
                            self.push(
                                Rule::NotAPrefix,
                                ops,
                                format!(
                                    "offset {} holds {other} in {} but {} in {}",
                                    element.offset,
                                    self.describe(first),
                                    element.value,
                                    self.describe(source)
                                ),
                            );
                        }
                    } else if source != Source::Final
                        && final_end.is_none_or(|end| element.offset > end)
                        && reported.insert((Rule::NotAPrefix, element.offset))
                    {
                        let end = final_end.map_or("is empty".to_string(), |end| {
                            format!("ends at offset {end}")
                        });
                        self.push(
                            Rule::NotAPrefix,
                            source.ops(),
                            format!(
                                "{} saw {} at offset {}, but the final log {end}",
                                self.describe(source),
                                element.value,
                                element.offset
                            ),
                        );
                    }
                }
                value_at
                    .entry(element.offset)
                    .or_insert((element.value, source));

                match offset_of.get(&element.value) {
                    Some(&(other, first))
                        if other != element.offset
                            && reported.insert((Rule::Duplicate, element.value)) =>
                    {
                        let mut ops = first.ops();
                        ops.extend(source.ops());
                        self.push(
                            Rule::Duplicate,
                            ops,
                            format!(
                                "value {} is at offset {other} in {} and at offset {} in {}",
                                element.value,
                                self.describe(first),
                                element.offset,
                                self.describe(source)
                            ),
                        );
                    }
                    Some(_) => {}
                    None => {
                        offset_of.insert(element.value, (element.offset, source));
                    }
                }

                self.value_was_written(source, *element, &mut reported);
            }
        }
        positions
    }

    /// Rules 5 and 6 for one observed element.
    fn value_was_written(
        &mut self,
        source: Source,
        element: Element,
        reported: &mut HashSet<(Rule, u64)>,
    ) {
        let Some(&index) = self.appends.get(&element.value) else {
            if reported.insert((Rule::Phantom, element.value)) {
                let what = if element.value == FOREIGN_PAYLOAD {
                    "a payload no append wrote".to_string()
                } else {
                    format!("value {}, which no append wrote", element.value)
                };
                self.push(
                    Rule::Phantom,
                    source.ops(),
                    format!(
                        "{} saw {what} at offset {}",
                        self.describe(source),
                        element.offset
                    ),
                );
            }
            return;
        };
        let append = &self.history.ops[index];
        let Action::Append { list, outcome, .. } = &append.action else {
            unreachable!("the append index holds appends only");
        };
        if list != self.list {
            if reported.insert((Rule::Phantom, element.value)) {
                let mut ops = vec![index];
                ops.extend(source.ops());
                self.push(
                    Rule::Phantom,
                    ops,
                    format!(
                        "{} saw value {} at offset {}, but it was appended to {list}: op #{index} {append}",
                        self.describe(source),
                        element.value,
                        element.offset
                    ),
                );
            }
            return;
        }
        if *outcome == AppendOutcome::Fail
            && reported.insert((Rule::FailedWriteVisible, element.value))
        {
            let mut ops = vec![index];
            ops.extend(source.ops());
            self.push(
                Rule::FailedWriteVisible,
                ops,
                format!(
                    "op #{index} {append} was answered as not written, yet {} has it at offset {}",
                    self.describe(source),
                    element.offset
                ),
            );
        }
        if let Source::Op(read) = source {
            let read_op = &self.history.ops[read];
            if read_op.complete < append.invoke && reported.insert((Rule::Phantom, element.value)) {
                self.push(
                    Rule::Phantom,
                    vec![read, index],
                    format!(
                        "op #{read} {read_op} saw value {} at offset {} before op #{index} {append} began",
                        element.value, element.offset
                    ),
                );
            }
        }
    }

    /// Rule 1: every acknowledged append is in the final read, at the offset
    /// the broker gave it if it gave one.
    fn acknowledged_appends_survive(
        &mut self,
        final_read: &[Element],
        positions: &HashMap<u64, u64>,
    ) {
        let range = match (final_read.first(), final_read.last()) {
            (Some(first), Some(last)) => format!("offsets {}..={}", first.offset, last.offset),
            _ => "an empty log".to_string(),
        };
        for (index, op) in self.history.ops.iter().enumerate() {
            let Action::Append {
                list,
                value,
                outcome: AppendOutcome::Ok { offset },
            } = &op.action
            else {
                continue;
            };
            if list != self.list {
                continue;
            }
            match (positions.get(value), offset) {
                (None, _) => {
                    let seen = self.first_read_of(*value);
                    let seen = seen.map_or(String::new(), |(read, at)| {
                        format!("; op #{read} had read it at offset {at}")
                    });
                    self.push(
                        Rule::LostWrite,
                        vec![index],
                        format!(
                            "op #{index} {op} was acknowledged but the final read ({range}) does \
                             not have it{seen}"
                        ),
                    );
                }
                (Some(found), Some(acked)) if found != acked => self.push(
                    Rule::NotAPrefix,
                    vec![index],
                    format!(
                        "op #{index} {op} was acknowledged at offset {acked}, but the final read \
                         has it at {found}"
                    ),
                ),
                _ => {}
            }
        }
    }

    /// Rule 4: if A was acknowledged before B began, A sits before B.
    ///
    /// A must be acknowledged, since an unknown append may land at any time.
    /// B may have any outcome, as long as it landed: it cannot have been
    /// written before it was sent.
    fn real_time_order(&mut self, positions: &HashMap<u64, u64>) {
        struct Placed {
            index: usize,
            invoke: u64,
            complete: u64,
            offset: u64,
            acknowledged: bool,
        }
        let mut placed: Vec<Placed> = Vec::new();
        for (index, op) in self.history.ops.iter().enumerate() {
            if let Action::Append {
                list,
                value,
                outcome,
            } = &op.action
                && list == self.list
                && let Some(&offset) = positions.get(value)
            {
                placed.push(Placed {
                    index,
                    invoke: op.invoke,
                    complete: op.complete,
                    offset,
                    acknowledged: matches!(outcome, AppendOutcome::Ok { .. }),
                });
            }
        }

        // Acknowledged appends by completion time, with the furthest offset
        // any of them reached so far: B is out of order exactly when the
        // furthest append completed before it began sits after it.
        let mut acknowledged: Vec<&Placed> = placed.iter().filter(|p| p.acknowledged).collect();
        acknowledged.sort_by_key(|p| p.complete);
        let mut furthest: Vec<(u64, usize)> = Vec::with_capacity(acknowledged.len());
        for (i, append) in acknowledged.iter().enumerate() {
            let best = match furthest.last() {
                Some(&(offset, at)) if offset > append.offset => (offset, at),
                _ => (append.offset, i),
            };
            furthest.push(best);
        }

        let mut found = Vec::new();
        for b in &placed {
            let before = acknowledged.partition_point(|a| a.complete < b.invoke);
            if before == 0 {
                continue;
            }
            let (offset, at) = furthest[before - 1];
            if offset > b.offset {
                found.push((acknowledged[at].index, b.index, offset, b.offset));
            }
        }
        for (a, b, a_offset, b_offset) in found {
            let (a_op, b_op) = (&self.history.ops[a], &self.history.ops[b]);
            self.push(
                Rule::RealTimeOrder,
                vec![a, b],
                format!(
                    "op #{a} {a_op} was acknowledged before op #{b} {b_op} began, yet sits at \
                     offset {a_offset}, after it at {b_offset}"
                ),
            );
        }
    }

    /// The first read operation that saw `value`, and where.
    fn first_read_of(&self, value: u64) -> Option<(usize, u64)> {
        self.history
            .ops
            .iter()
            .enumerate()
            .find_map(|(index, op)| match &op.action {
                Action::Read { list, observed } if list == self.list => observed
                    .iter()
                    .find(|e| e.value == value)
                    .map(|e| (index, e.offset)),
                _ => None,
            })
    }

    fn describe(&self, source: Source) -> String {
        match source {
            Source::Final => "the final read".to_string(),
            Source::Op(index) => format!("op #{index} ({})", self.history.ops[index]),
        }
    }

    fn push(&mut self, rule: Rule, ops: Vec<usize>, explanation: String) {
        self.out.push(Violation {
            rule,
            list: self.list.to_string(),
            ops,
            explanation,
        });
    }
}

impl Source {
    fn ops(self) -> Vec<usize> {
        match self {
            Source::Final => vec![],
            Source::Op(index) => vec![index],
        }
    }
}

fn malformed(list: &str, explanation: &str) -> Violation {
    Violation {
        rule: Rule::MalformedHistory,
        list: list.to_string(),
        ops: vec![],
        explanation: explanation.to_string(),
    }
}

#[cfg(test)]
mod tests;
