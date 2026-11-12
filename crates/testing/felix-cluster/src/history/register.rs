//! `Quorum` cache keys as registers: puts of unique values and gets, checked
//! for a get that is stale or saw a value nobody put.
//!
//! Values are unique, so a get names the put it saw. Two checks run. A fast
//! sweep reports what no linearization could explain (a stale read, a
//! phantom) by name. A key it finds nothing wrong with then goes to the
//! [`linearizable`](super::linearizable) search, which is complete.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;

use super::checker::{Rule, Violation};
use super::linearizable::{self, Budget, Linearizability};
use super::model::{FOREIGN_PAYLOAD, millis};

/// One cache operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterOp {
    pub process: usize,
    pub invoke: u64,
    pub complete: u64,
    pub key: String,
    pub action: RegisterAction,
}

/// What a cache operation did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterAction {
    /// Store `value` under the key. Any error is recorded as unknown
    /// (`acknowledged: false`): a cache put has no answer that says it
    /// applied nothing.
    Put { value: u64, acknowledged: bool },
    /// Read the key. `None` is a miss. A get that failed is not recorded.
    Get { value: Option<u64> },
    /// Remove the key. `removed` is what the answer said was removed:
    /// `Some(None)` when the key was not there, `None` when the delete failed
    /// and may or may not have happened.
    Delete { removed: Option<Option<u64>> },
    /// Store `value` only if the key holds `expect` (`None`: no live entry).
    /// `applied` is the answer, `None` when the request failed and may or may
    /// not have applied.
    PutIf {
        expect: Option<u64>,
        value: u64,
        applied: Option<bool>,
    },
}

impl fmt::Display for RegisterOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let span = format!("{}..{}", millis(self.invoke), millis(self.complete));
        let (process, key) = (self.process, &self.key);
        match self.action {
            RegisterAction::Put {
                value,
                acknowledged,
            } => {
                let outcome = if acknowledged { "ok" } else { "info" };
                write!(
                    f,
                    "client {process} put {value} to {key} -> {outcome} [{span}]"
                )
            }
            RegisterAction::Get { value: Some(value) } => {
                write!(f, "client {process} get {key} -> {value} [{span}]")
            }
            RegisterAction::Get { value: None } => {
                write!(f, "client {process} get {key} -> miss [{span}]")
            }
            RegisterAction::Delete { removed } => {
                let outcome = match removed {
                    Some(Some(value)) => format!("removed {value}"),
                    Some(None) => "found nothing".to_string(),
                    None => "info".to_string(),
                };
                write!(f, "client {process} delete {key} -> {outcome} [{span}]")
            }
            RegisterAction::PutIf {
                expect,
                value,
                applied,
            } => {
                let expect = expect.map_or("absent".to_string(), |v| format!("holding {v}"));
                let outcome = match applied {
                    Some(true) => "applied",
                    Some(false) => "refused",
                    None => "info",
                };
                write!(
                    f,
                    "client {process} put {value} to {key} if {expect} -> {outcome} [{span}]"
                )
            }
        }
    }
}

/// Check every key, which starts absent. Returns the keys the linearizability
/// search gave up on (see [`Budget`]), with the reason.
///
/// The sweep: a get is stale when it returns `u`, `u`'s put was acknowledged, and before
/// the get began some value `w` was already in effect, known either from
/// `w`'s acknowledgement or from an earlier get that returned `w`, whose put
/// began only after `u`'s was acknowledged. Every linearization then puts `u`
/// before `w` before the get. A miss is stale once any value was in effect
/// before it began.
pub(crate) fn check(ops: &[RegisterOp], out: &mut Vec<Violation>) -> Vec<String> {
    check_with(ops, Budget::default(), out)
}

/// [`check`] with an explicit search budget.
pub(crate) fn check_with(
    ops: &[RegisterOp],
    budget: Budget,
    out: &mut Vec<Violation>,
) -> Vec<String> {
    let mut unknown = Vec::new();
    let mut keys: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, op) in ops.iter().enumerate() {
        keys.entry(&op.key).or_default().push(index);
    }
    for (key, indices) in keys {
        let before = out.len();
        check_key(ops, key, &indices, out);
        if out.len() > before {
            continue;
        }
        let indices = drop_unobserved_unknown_puts(ops, indices);
        match linearizable::check_key(ops, &indices, budget) {
            Linearizability::Ok => {}
            Linearizability::Violation(witness) => out.push(Violation {
                rule: Rule::NotLinearizable,
                list: format!("cache key {key}"),
                ops: witness
                    .prefix
                    .iter()
                    .chain(&witness.stuck)
                    .copied()
                    .collect(),
                explanation: witness.text,
            }),
            Linearizability::Unknown { steps } => unknown.push(format!(
                "cache key {key}: not decided after {steps} steps, \
                 so it is neither proven nor refuted"
            )),
        }
    }
    unknown
}

/// Drop unknown-outcome puts whose value no get or delete reported. Such a
/// put is optional, and any order that applied it can leave it out: nothing
/// read its value, so no read sat in its window. Left in, each one doubles the
/// search. A conditional put sees whether the key is present, so a key with
/// one keeps every operation.
fn drop_unobserved_unknown_puts(ops: &[RegisterOp], indices: Vec<usize>) -> Vec<usize> {
    let conditional = indices
        .iter()
        .any(|&i| matches!(ops[i].action, RegisterAction::PutIf { .. }));
    if conditional {
        return indices;
    }
    let observed: HashSet<u64> = indices
        .iter()
        .filter_map(|&i| match ops[i].action {
            RegisterAction::Get { value } => value,
            RegisterAction::Delete {
                removed: Some(Some(value)),
            } => Some(value),
            _ => None,
        })
        .collect();
    indices
        .into_iter()
        .filter(|&i| match ops[i].action {
            RegisterAction::Put {
                value,
                acknowledged: false,
            } => observed.contains(&value),
            _ => true,
        })
        .collect()
}

fn check_key(ops: &[RegisterOp], key: &str, indices: &[usize], out: &mut Vec<Violation>) {
    let mut push = |rule, involved: Vec<usize>, explanation: String| {
        out.push(Violation {
            rule,
            list: format!("cache key {key}"),
            ops: involved,
            explanation,
        });
    };

    let mut puts: HashMap<u64, usize> = HashMap::new();
    for &index in indices {
        if let RegisterAction::Put { value, .. } | RegisterAction::PutIf { value, .. } =
            ops[index].action
            && let Some(first) = puts.insert(value, index)
        {
            push(
                Rule::MalformedHistory,
                vec![first, index],
                format!("value {value} is put by op #{first} and op #{index}"),
            );
        }
    }

    // The sweep assumes plain puts and gets. A delete or conditional put can
    // make an older value correct again, so those keys are left to the search.
    let plain = indices.iter().all(|&index| {
        matches!(
            ops[index].action,
            RegisterAction::Put { .. } | RegisterAction::Get { .. }
        )
    });

    // When each value is known to be in effect, and the put that wrote it.
    let mut evidence: Vec<(u64, usize)> = Vec::new();
    for &index in indices {
        let op = &ops[index];
        match op.action {
            RegisterAction::Put {
                acknowledged: true, ..
            } => evidence.push((op.complete, index)),
            RegisterAction::Get { value: Some(value) } => {
                let Some(&put) = puts.get(&value) else {
                    let what = if value == FOREIGN_PAYLOAD {
                        "a payload the workload did not write".to_string()
                    } else {
                        format!("value {value}, which no put to this key wrote")
                    };
                    push(
                        Rule::Phantom,
                        vec![index],
                        format!("op #{index} {op} saw {what}"),
                    );
                    continue;
                };
                if ops[put].invoke > op.complete {
                    push(
                        Rule::Phantom,
                        vec![put, index],
                        format!(
                            "op #{index} {op} saw {value} before op #{put} {} began",
                            ops[put]
                        ),
                    );
                    continue;
                }
                evidence.push((op.complete, put));
            }
            _ => {}
        }
    }
    if !plain {
        return;
    }
    evidence.sort_unstable();
    // latest[i]: of the puts in evidence[..=i], the one that began last.
    let mut latest: Vec<usize> = Vec::with_capacity(evidence.len());
    for (i, &(_, put)) in evidence.iter().enumerate() {
        let best = match i {
            0 => put,
            _ if ops[put].invoke > ops[latest[i - 1]].invoke => put,
            _ => latest[i - 1],
        };
        latest.push(best);
    }

    for &index in indices {
        let op = &ops[index];
        let RegisterAction::Get { value } = op.action else {
            continue;
        };
        let known = evidence.partition_point(|&(at, _)| at < op.invoke);
        if known == 0 {
            continue;
        }
        let newest = latest[known - 1];
        match value {
            None => push(
                Rule::StaleRead,
                vec![newest, index],
                format!(
                    "op #{index} {op} missed, but op #{newest} {} was in effect before it began",
                    ops[newest]
                ),
            ),
            Some(value) => {
                let Some(&put) = puts.get(&value) else {
                    continue;
                };
                let RegisterAction::Put {
                    acknowledged: true, ..
                } = ops[put].action
                else {
                    continue;
                };
                if ops[newest].invoke > ops[put].complete {
                    push(
                        Rule::StaleRead,
                        vec![put, newest, index],
                        format!(
                            "op #{index} {op} returned the value of op #{put} {}, but op #{newest} \
                             {} began after that and was in effect before the get began",
                            ops[put], ops[newest]
                        ),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
