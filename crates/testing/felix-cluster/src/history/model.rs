//! What a history is: every operation the clients ran, when each started and
//! ended, and what it returned, plus the final read of every list.
//!
//! Each list is one single-shard stream, so a list's positions are the shard's
//! log offsets. Times are nanoseconds on one monotonic clock shared by every
//! client, which is what makes "A finished before B started" meaningful.

use std::collections::BTreeMap;
use std::fmt;

/// The value a read reports for a payload no append could have written.
///
/// Appends draw values from a counter that starts at zero, so it never reaches
/// this, and the checker reports it as a phantom like any other unknown value.
pub const FOREIGN_PAYLOAD: u64 = u64::MAX;

/// A recorded run: operations, faults, and the final state of each list.
#[derive(Debug, Clone, Default)]
pub struct History {
    /// Every list the workload wrote, and what the checker may assume of it.
    pub lists: BTreeMap<String, ListSpec>,
    /// Operations in the order they were recorded, which is completion order.
    pub ops: Vec<Op>,
    /// What each list held once every fault was healed. Must start at the
    /// list's base offset and have no holes, or lost-write checks are unsound.
    pub final_reads: BTreeMap<String, Vec<Element>>,
    /// The nemesis's timeline, so a violation can be read against it.
    pub faults: Vec<FaultEvent>,
}

/// What the checker may assume of one list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListSpec {
    pub consistency: Consistency,
    /// The first offset the workload owns. Records below it were written
    /// before the run (the harness's readiness probe) and are ignored.
    pub base: u64,
}

/// How the stream acknowledges, which decides what may be lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consistency {
    /// Acknowledged once a majority holds it: an acknowledged append must
    /// survive any single failure.
    Quorum,
    /// Acknowledged by the leader alone. A failover may lose an acknowledged
    /// suffix, so lost writes and a rewritten tail are not violations.
    Leader,
}

/// One client operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Op {
    /// The client that ran it. A client runs one operation at a time.
    pub process: usize,
    /// When the client started it.
    pub invoke: u64,
    /// When the client learned the outcome, or gave up on learning it.
    pub complete: u64,
    pub action: Action,
}

/// What an operation did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Append `value` to `list`. Every append in a history has its own value.
    Append {
        list: String,
        value: u64,
        outcome: AppendOutcome,
    },
    /// Read `list`. A read that failed outright observed nothing.
    Read {
        list: String,
        observed: Vec<Element>,
    },
}

/// How an append ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    /// Acknowledged. `offset` is where the broker said it landed, when it said.
    Ok { offset: Option<u64> },
    /// Definitely not written: the broker answered that it applied nothing.
    Fail,
    /// Unknown: a timeout, a lost connection, or an `outcome_unknown` answer.
    Info,
}

/// One record a read saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Element {
    pub offset: u64,
    pub value: u64,
}

/// A fault starting or ending, at a time on the clients' clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultEvent {
    pub at: u64,
    pub what: String,
}

impl History {
    /// Appends that were acknowledged.
    pub fn acknowledged(&self) -> usize {
        self.ops
            .iter()
            .filter(|op| {
                matches!(
                    op.action,
                    Action::Append {
                        outcome: AppendOutcome::Ok { .. },
                        ..
                    }
                )
            })
            .count()
    }

    /// One line of counts and latencies, for the test log.
    pub fn summary(&self) -> String {
        let (mut ok, mut fail, mut info, mut reads) = (Vec::new(), 0, 0, Vec::new());
        for op in &self.ops {
            let took = op.complete - op.invoke;
            match &op.action {
                Action::Append { outcome, .. } => match outcome {
                    AppendOutcome::Ok { .. } => ok.push(took),
                    AppendOutcome::Fail => fail += 1,
                    AppendOutcome::Info => info += 1,
                },
                Action::Read { .. } => reads.push(took),
            }
        }
        let records: usize = self.final_reads.values().map(Vec::len).sum();
        format!(
            "{} ops: {} appends ok (median {}), {fail} failed, {info} unknown; {} reads \
             (median {}); {records} records in the final reads; {} fault events",
            self.ops.len(),
            ok.len(),
            median(&mut ok),
            reads.len(),
            median(&mut reads),
            self.faults.len(),
        )
    }
}

impl Op {
    /// The list this operation touched.
    pub fn list(&self) -> &str {
        match &self.action {
            Action::Append { list, .. } | Action::Read { list, .. } => list,
        }
    }
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let span = format!("{}..{}", millis(self.invoke), millis(self.complete));
        match &self.action {
            Action::Append {
                list,
                value,
                outcome,
            } => {
                let outcome = match outcome {
                    AppendOutcome::Ok { offset: Some(at) } => format!("ok at {at}"),
                    AppendOutcome::Ok { offset: None } => "ok".to_string(),
                    AppendOutcome::Fail => "fail".to_string(),
                    AppendOutcome::Info => "info".to_string(),
                };
                write!(
                    f,
                    "client {} append {value} to {list} -> {outcome} [{span}]",
                    self.process
                )
            }
            Action::Read { list, observed } => {
                let range = match (observed.first(), observed.last()) {
                    (Some(first), Some(last)) => {
                        format!("offsets {}..={}", first.offset, last.offset)
                    }
                    _ => "nothing".to_string(),
                };
                write!(
                    f,
                    "client {} read {list} -> {} records, {range} [{span}]",
                    self.process,
                    observed.len()
                )
            }
        }
    }
}

fn median(samples: &mut [u64]) -> String {
    samples.sort_unstable();
    samples
        .get(samples.len() / 2)
        .map_or("-".to_string(), |&nanos| millis(nanos))
}

/// A clock reading as milliseconds, which is the resolution a person reads a
/// fault timeline at.
pub(crate) fn millis(nanos: u64) -> String {
    format!("{:.1}ms", nanos as f64 / 1_000_000.0)
}
