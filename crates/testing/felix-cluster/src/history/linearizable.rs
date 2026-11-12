//! A linearizability checker for cache keys, after Wing and Gong with
//! Lowe's memoization (the search Porcupine runs).
//!
//! A history of one key is linearizable when its operations can be put in a
//! single order that respects real time (A finished before B began puts A
//! first) and that a plain register would produce. Keys do not interact, so
//! each is searched alone. The search tries to place one operation at a time,
//! backs up when it gets stuck, and remembers every (placed set, register
//! value) pair it has already failed from.
//!
//! An operation with an unknown outcome may have happened at any time after
//! it began, or never, so it is optional and never forces another operation
//! to come after it. The search is bounded: past [`Budget`] it gives up and
//! says [`Linearizability::Unknown`] instead of hanging.
//!
//! Expiry is not modelled. A key that a TTL removes would look like a lost
//! write, so histories that use TTLs must not be fed to this check.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use super::register::{RegisterAction, RegisterOp};

/// How much work one key's search may do before it gives up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budget {
    /// Operations tried in total.
    pub steps: u64,
    /// Distinct (placed set, value) pairs remembered.
    pub states: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            steps: 20_000_000,
            states: 2_000_000,
        }
    }
}

/// What the search concluded about one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Linearizability {
    /// An order exists.
    Ok,
    /// No order exists.
    Violation(Witness),
    /// The budget ran out before the search finished either way.
    Unknown { steps: u64 },
}

/// Why a key is not linearizable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Witness {
    pub key: String,
    /// Indices into the checked slice of the longest run of operations the
    /// search managed to order, in that order.
    pub prefix: Vec<usize>,
    /// Indices of the operations that could not come next.
    pub stuck: Vec<usize>,
    /// The same, as lines a person can read.
    pub text: String,
}

impl fmt::Display for Witness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

/// Search every key in `ops` and report each separately.
pub fn check_all(ops: &[RegisterOp], budget: Budget) -> BTreeMap<String, Linearizability> {
    let mut keys: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, op) in ops.iter().enumerate() {
        keys.entry(&op.key).or_default().push(index);
    }
    keys.into_iter()
        .map(|(key, indices)| (key.to_string(), check_key(ops, &indices, budget)))
        .collect()
}

/// Search the operations at `indices`, which must all be on one key. Every
/// key starts absent.
pub fn check_key(ops: &[RegisterOp], indices: &[usize], budget: Budget) -> Linearizability {
    let mut nodes: Vec<Node> = indices
        .iter()
        .map(|&index| Node::new(index, &ops[index]))
        .collect();
    nodes.sort_by_key(|node| (node.invoke, node.index));
    Search::new(nodes, budget).run(ops)
}

/// The register's value: `None` is absent.
type State = Option<u64>;

#[derive(Debug, Clone, Copy)]
enum Kind {
    Write(u64),
    Read(State),
    /// Removes the key. `saw` is the value it reported removing, if known.
    Delete {
        saw: Option<State>,
    },
    /// Writes `value` only if the key holds `expect`. `applied` is the answer.
    Cas {
        expect: State,
        value: u64,
        applied: Option<bool>,
    },
}

struct Node {
    index: usize,
    invoke: u64,
    /// `u64::MAX` for an optional operation: nothing has to follow it.
    complete: u64,
    optional: bool,
    kind: Kind,
}

impl Node {
    fn new(index: usize, op: &RegisterOp) -> Self {
        let (kind, optional) = match op.action {
            RegisterAction::Put {
                value,
                acknowledged,
            } => (Kind::Write(value), !acknowledged),
            RegisterAction::Get { value } => (Kind::Read(value), false),
            RegisterAction::Delete { removed } => {
                (Kind::Delete { saw: removed }, removed.is_none())
            }
            RegisterAction::PutIf {
                expect,
                value,
                applied,
            } => (
                Kind::Cas {
                    expect,
                    value,
                    applied,
                },
                applied.is_none(),
            ),
        };
        Self {
            index,
            invoke: op.invoke,
            complete: if optional { u64::MAX } else { op.complete },
            optional,
            kind,
        }
    }

    /// The register after this operation runs on `state`, or `None` when it
    /// cannot run there.
    fn step(&self, state: State) -> Option<State> {
        match self.kind {
            Kind::Write(value) => Some(Some(value)),
            Kind::Read(seen) => (seen == state).then_some(state),
            Kind::Delete { saw: Some(saw) } => (saw == state).then_some(None),
            Kind::Delete { saw: None } => Some(None),
            Kind::Cas {
                expect,
                value,
                applied: Some(true) | None,
            } => (expect == state).then_some(Some(value)),
            Kind::Cas {
                expect,
                applied: Some(false),
                ..
            } => (expect != state).then_some(state),
        }
    }

    /// What this operation needed that `state` did not offer.
    fn needs(&self) -> String {
        match self.kind {
            Kind::Write(_) | Kind::Delete { saw: None } => "nothing".to_string(),
            Kind::Read(seen) => format!("the key to hold {}", show(seen)),
            Kind::Delete { saw: Some(saw) } => format!("the key to hold {}", show(saw)),
            Kind::Cas {
                expect,
                applied: Some(false),
                ..
            } => format!("the key to hold anything but {}", show(expect)),
            Kind::Cas { expect, .. } => format!("the key to hold {}", show(expect)),
        }
    }
}

fn show(state: State) -> String {
    state.map_or("nothing".to_string(), |value| value.to_string())
}

/// A set of placed operations, one bit each.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Bits(Vec<u64>);

impl Bits {
    fn new(n: usize) -> Self {
        Self(vec![0; n.div_ceil(64)])
    }

    fn get(&self, i: usize) -> bool {
        self.0[i / 64] >> (i % 64) & 1 == 1
    }

    fn flip(&mut self, i: usize) {
        self.0[i / 64] ^= 1 << (i % 64);
    }
}

struct Search {
    nodes: Vec<Node>,
    budget: Budget,
}

/// One operation placed, and where to resume trying candidates on backing up.
struct Placed {
    node: usize,
    before: State,
    resume: usize,
}

impl Search {
    fn new(nodes: Vec<Node>, budget: Budget) -> Self {
        Self { nodes, budget }
    }

    /// The earliest completion among operations still to place that must be
    /// placed. Nothing that began after it can go first.
    fn horizon(&self, placed: &Bits) -> u64 {
        (0..self.nodes.len())
            .filter(|&i| !placed.get(i) && !self.nodes[i].optional)
            .map(|i| self.nodes[i].complete)
            .min()
            .unwrap_or(u64::MAX)
    }

    fn run(&self, ops: &[RegisterOp]) -> Linearizability {
        let n = self.nodes.len();
        let mut required = self.nodes.iter().filter(|node| !node.optional).count();
        let mut placed = Bits::new(n);
        let mut state: State = None;
        let mut stack: Vec<Placed> = Vec::new();
        let mut seen: HashSet<(Bits, State)> = HashSet::new();
        let mut steps = 0u64;
        let mut cursor = 0usize;
        let mut best: (Vec<usize>, Bits, State) = (Vec::new(), placed.clone(), state);

        loop {
            if required == 0 {
                return Linearizability::Ok;
            }
            let horizon = self.horizon(&placed);
            let mut advanced = false;
            while cursor < n && self.nodes[cursor].invoke <= horizon {
                let i = cursor;
                cursor += 1;
                if placed.get(i) {
                    continue;
                }
                steps += 1;
                if steps > self.budget.steps || seen.len() > self.budget.states {
                    return Linearizability::Unknown { steps };
                }
                let Some(next) = self.nodes[i].step(state) else {
                    continue;
                };
                placed.flip(i);
                if !seen.insert((placed.clone(), next)) {
                    placed.flip(i);
                    continue;
                }
                stack.push(Placed {
                    node: i,
                    before: state,
                    resume: cursor,
                });
                state = next;
                if !self.nodes[i].optional {
                    required -= 1;
                }
                cursor = 0;
                if stack.len() > best.0.len() {
                    best = (
                        stack.iter().map(|p| p.node).collect(),
                        placed.clone(),
                        state,
                    );
                }
                advanced = true;
                break;
            }
            if advanced {
                continue;
            }
            let Some(back) = stack.pop() else {
                return Linearizability::Violation(self.witness(ops, &best));
            };
            placed.flip(back.node);
            state = back.before;
            if !self.nodes[back.node].optional {
                required += 1;
            }
            cursor = back.resume;
        }
    }

    /// Describe the longest prefix and the operations that could go next but
    /// do not fit.
    fn witness(&self, ops: &[RegisterOp], best: &(Vec<usize>, Bits, State)) -> Witness {
        let (order, placed, state) = best;
        let horizon = self.horizon(placed);
        let stuck: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| {
                !placed.get(i) && !self.nodes[i].optional && self.nodes[i].invoke <= horizon
            })
            .collect();
        let key = &ops[self.nodes[0].index].key;
        let mut text = format!("key {key} has no linearization.\n");
        if order.is_empty() {
            text.push_str("  no operation can go first.\n");
        } else {
            text.push_str("  longest order found (each op takes effect after the one above):\n");
            for &i in order {
                text.push_str(&format!("    {}\n", ops[self.nodes[i].index]));
            }
        }
        text.push_str(&format!(
            "  the key then holds {}. none of these can come next:\n",
            show(*state)
        ));
        for &i in &stuck {
            text.push_str(&format!(
                "    {}  (needs {})\n",
                ops[self.nodes[i].index],
                self.nodes[i].needs()
            ));
        }
        Witness {
            key: key.clone(),
            prefix: order.iter().map(|&i| self.nodes[i].index).collect(),
            stuck: stuck.iter().map(|&i| self.nodes[i].index).collect(),
            text,
        }
    }
}

#[cfg(test)]
mod tests;
