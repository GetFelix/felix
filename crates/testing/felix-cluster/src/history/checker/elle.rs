//! Rules 12 to 19: the dependency graph of a list's appends and reads, and
//! the cycles in it, named the way Adya and Elle name them.
//!
//! Every position in a `Quorum` list is a log offset, so the order of the
//! list is the order of the offsets that appends and reads, subscriber
//! deliveries and acknowledgements agree on. Over that order an operation
//! depends on another in four ways:
//!
//! - `ww`: an append sits right before another append.
//! - `wr`: a read saw an append, the last one it saw.
//! - `rw`: a read did not see the append just after its last record (the
//!   first append, for an empty read). Only
//!   with [`History::exact_tails`], since a subscriber may drop its newest
//!   records and a short read is then not proof of anything.
//! - `rt`: one operation finished before another began. Appends whose
//!   outcome is unknown never start an `rt` edge, since they may land late.
//!
//! A cycle means no single order explains what the clients saw. Its name
//! says how: `G0` is `ww` only, `G1c` adds `wr`, `G-single` has one `rw`,
//! `G2` more. A `-realtime` suffix means an `rt` edge was needed.
//!
//! With one total order and single-operation transactions, a cycle needs at
//! least one `rt` edge unless the order itself is contradictory: a value at
//! two offsets, or a read that disagrees with the final read about what an
//! offset holds (rule 3), which gives a backward `wr` edge. So `G1c`,
//! `G-single` and `G2` without `-realtime` only show up next to one of
//! those. Cycles already named by rules 2 to 6 are not reported a second
//! time.
//!
//! Cycles are found with Tarjan's strongly connected components, then one
//! short cycle per component by breadth-first search from a few of its
//! operations, so the work stays near-linear.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Bound;

use super::super::model::{Action, AppendOutcome, Consistency, Element, History};
use super::{Rule, Violation};

/// How many operations of a component the cycle search starts from.
const STARTS_PER_COMPONENT: usize = 8;

/// Every dependency cycle in `history`, without the filtering [`check`](super::check)
/// does against the other rules. For callers that feed their own history and
/// want the graph findings alone.
pub fn check_list_append(history: &History) -> Vec<Violation> {
    let mut out = Vec::new();
    for (list, spec) in &history.lists {
        if spec.consistency == Consistency::Quorum {
            out.extend(ListGraph::build(history, list, spec.base).cycles());
        }
    }
    out
}

/// Add the cycles of every `Quorum` list to `out`, minus the ones the
/// violations already there explain.
pub(super) fn check(history: &History, out: &mut Vec<Violation>) {
    for (list, spec) in &history.lists {
        if spec.consistency != Consistency::Quorum {
            continue;
        }
        let found = ListGraph::build(history, list, spec.base).cycles();
        let explained = |cycle: &Violation| {
            let list_order_broken = out.iter().any(|v| {
                v.list == cycle.list
                    && matches!(
                        v.rule,
                        Rule::Duplicate | Rule::NotAPrefix | Rule::RealTimeOrder
                    )
            });
            let only_order = matches!(cycle.rule, Rule::G0 | Rule::G0Realtime);
            let by_one = out.iter().any(|v| {
                v.list == cycle.list
                    && matches!(v.rule, Rule::Phantom | Rule::FailedWriteVisible)
                    && cycle.ops.iter().all(|op| v.ops.contains(op))
            });
            (only_order && list_order_broken) || by_one
        };
        let kept: Vec<Violation> = found.into_iter().filter(|c| !explained(c)).collect();
        out.extend(kept);
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Edge {
    Ww,
    Wr,
    Rw,
    Rt,
}

impl Edge {
    fn name(self) -> &'static str {
        match self {
            Edge::Ww => "ww",
            Edge::Wr => "wr",
            Edge::Rw => "rw",
            Edge::Rt => "rt",
        }
    }
}

/// One list's operations as graph nodes (`0..ops.len()`), then the
/// real-time chain's barrier nodes. A barrier stands for "everything that
/// finished by this time", which keeps real-time edges linear in number.
struct ListGraph<'a> {
    history: &'a History,
    list: &'a str,
    /// History op index of each operation node.
    ops: Vec<usize>,
    adj: Vec<Vec<(usize, Edge)>>,
}

impl<'a> ListGraph<'a> {
    fn build(history: &'a History, list: &'a str, base: u64) -> Self {
        let position = positions(history, list, base);
        let mut node_of: HashMap<usize, usize> = HashMap::new();
        let mut ops: Vec<usize> = Vec::new();
        let mut node = |index: usize, ops: &mut Vec<usize>| {
            *node_of.entry(index).or_insert_with(|| {
                ops.push(index);
                ops.len() - 1
            })
        };

        // The append node that owns each value in the list, unless its
        // append definitely failed (rule 6's business, not a graph node).
        let in_list: HashSet<u64> = position.values().copied().collect();
        let mut owner: HashMap<u64, usize> = HashMap::new();
        for (index, op) in history.ops.iter().enumerate() {
            if let Action::Append {
                list: l,
                value,
                outcome,
            } = &op.action
                && l == list
                && *outcome != AppendOutcome::Fail
                && in_list.contains(value)
                && !owner.contains_key(value)
            {
                owner.insert(*value, node(index, &mut ops));
            }
        }

        let mut edges: Vec<(usize, usize, Edge)> = Vec::new();
        let mut previous: Option<usize> = None;
        for value in position.values() {
            if let Some(&n) = owner.get(value) {
                if let Some(p) = previous
                    && p != n
                {
                    edges.push((p, n, Edge::Ww));
                }
                previous = Some(n);
            }
        }

        for (index, op) in history.ops.iter().enumerate() {
            let Action::Read { list: l, observed } = &op.action else {
                continue;
            };
            if l != list {
                continue;
            }
            let seen: Vec<&Element> = observed.iter().filter(|e| e.offset >= base).collect();
            let reader = node(index, &mut ops);
            if let Some(&w) = seen.iter().rev().find_map(|e| owner.get(&e.value)) {
                edges.push((w, reader, Edge::Wr));
            }
            if history.exact_tails {
                // An empty read missed the list's first record.
                let from = seen
                    .last()
                    .map_or(Bound::Unbounded, |last| Bound::Excluded(last.offset));
                let saw: HashSet<u64> = seen.iter().map(|e| e.value).collect();
                let missed = position
                    .range((from, Bound::Unbounded))
                    .find_map(|(_, v)| owner.get(v).filter(|_| !saw.contains(v)));
                if let Some(&x) = missed {
                    edges.push((reader, x, Edge::Rw));
                }
            }
        }

        // Real time. Sources are operations that surely finished when they
        // say they did; targets are all of them.
        let n = ops.len();
        let mut sources: Vec<(u64, usize)> = ops
            .iter()
            .enumerate()
            .filter(|&(_, &index)| finished_for_sure(&history.ops[index].action))
            .map(|(node, &index)| (history.ops[index].complete, node))
            .collect();
        sources.sort_unstable();
        for (k, &(_, source)) in sources.iter().enumerate() {
            edges.push((source, n + k, Edge::Rt));
            if k + 1 < sources.len() {
                edges.push((n + k, n + k + 1, Edge::Rt));
            }
        }
        for (target, &index) in ops.iter().enumerate() {
            let invoke = history.ops[index].invoke;
            let before = sources.partition_point(|&(complete, _)| complete < invoke);
            if before > 0 {
                edges.push((n + before - 1, target, Edge::Rt));
            }
        }

        let mut adj = vec![Vec::new(); n + sources.len()];
        for (from, to, kind) in edges {
            adj[from].push((to, kind));
        }
        ListGraph {
            history,
            list,
            ops,
            adj,
        }
    }

    /// One short cycle for each strongly connected component.
    fn cycles(&self) -> Vec<Violation> {
        let mut found = Vec::new();
        for component in self.components() {
            let members: HashSet<usize> = component.iter().copied().collect();
            let mut best: Option<Vec<(usize, Edge)>> = None;
            let starts = component
                .iter()
                .filter(|&&node| node < self.ops.len())
                .take(STARTS_PER_COMPONENT);
            for &start in starts {
                if let Some(cycle) = self.shortest_cycle(start, &members) {
                    let score = |c: &Vec<(usize, Edge)>| {
                        (c.len(), c.iter().filter(|(_, e)| *e == Edge::Rw).count())
                    };
                    if best.as_ref().is_none_or(|b| score(&cycle) < score(b)) {
                        best = Some(cycle);
                    }
                }
            }
            if let Some(cycle) = best {
                found.push(self.report(&cycle));
            }
        }
        found.sort_by_key(|v| v.ops.clone());
        found
    }

    /// The shortest cycle through `start` inside `members`, as the nodes on
    /// it and the edge leaving each.
    fn shortest_cycle(&self, start: usize, members: &HashSet<usize>) -> Option<Vec<(usize, Edge)>> {
        let mut parent: HashMap<usize, (usize, Edge)> = HashMap::new();
        let mut queue = VecDeque::from([start]);
        while let Some(at) = queue.pop_front() {
            for &(to, kind) in &self.adj[at] {
                if !members.contains(&to) {
                    continue;
                }
                if to == start {
                    let mut path = vec![(at, kind)];
                    let mut cursor = at;
                    while cursor != start {
                        let (from, edge) = parent[&cursor];
                        path.push((from, edge));
                        cursor = from;
                    }
                    path.reverse();
                    return Some(path);
                }
                if let std::collections::hash_map::Entry::Vacant(slot) = parent.entry(to) {
                    slot.insert((at, kind));
                    queue.push_back(to);
                }
            }
        }
        None
    }

    /// Tarjan's algorithm, without recursion. Components of one node have no
    /// self-edges here, so they are not cycles and are dropped.
    fn components(&self) -> Vec<Vec<usize>> {
        let n = self.adj.len();
        let mut index = vec![usize::MAX; n];
        let mut low = vec![0; n];
        let mut on_stack = vec![false; n];
        let mut stack = Vec::new();
        let mut next = 0;
        let mut out = Vec::new();
        for root in 0..n {
            if index[root] != usize::MAX {
                continue;
            }
            let mut work = vec![(root, 0usize)];
            while let Some(&mut (at, ref mut cursor)) = work.last_mut() {
                if *cursor == 0 && index[at] == usize::MAX {
                    index[at] = next;
                    low[at] = next;
                    next += 1;
                    stack.push(at);
                    on_stack[at] = true;
                }
                if let Some(&(to, _)) = self.adj[at].get(*cursor) {
                    *cursor += 1;
                    if index[to] == usize::MAX {
                        work.push((to, 0));
                    } else if on_stack[to] {
                        low[at] = low[at].min(index[to]);
                    }
                    continue;
                }
                work.pop();
                if let Some(&(parent, _)) = work.last() {
                    low[parent] = low[parent].min(low[at]);
                }
                if low[at] == index[at] {
                    let mut component = Vec::new();
                    while let Some(member) = stack.pop() {
                        on_stack[member] = false;
                        component.push(member);
                        if member == at {
                            break;
                        }
                    }
                    if component.len() > 1 {
                        out.push(component);
                    }
                }
            }
        }
        out
    }

    /// Name a cycle and describe it.
    fn report(&self, cycle: &[(usize, Edge)]) -> Violation {
        // An op's edge into a barrier is already `rt`, so dropping the
        // barriers leaves the right edge between the ops either side.
        let steps: Vec<(usize, Edge)> = cycle
            .iter()
            .copied()
            .filter(|&(node, _)| node < self.ops.len())
            .collect();
        let count = |kind: Edge| steps.iter().filter(|(_, e)| *e == kind).count();
        let (rw, wr, rt) = (count(Edge::Rw), count(Edge::Wr), count(Edge::Rt));
        let rule = match (rw, wr > 0, rt > 0) {
            (0, false, false) => Rule::G0,
            (0, false, true) => Rule::G0Realtime,
            (0, true, false) => Rule::G1c,
            (0, true, true) => Rule::G1cRealtime,
            (1, _, false) => Rule::GSingle,
            (1, _, true) => Rule::GSingleRealtime,
            (_, _, false) => Rule::G2,
            (_, _, true) => Rule::G2Realtime,
        };
        let mut text = String::new();
        let mut ops = Vec::new();
        for &(node, edge) in &steps {
            let index = self.ops[node];
            ops.push(index);
            text.push_str(&format!(
                "op #{index} {} -{}-> ",
                self.history.ops[index],
                edge.name()
            ));
        }
        let first = self.ops[steps[0].0];
        text.push_str(&format!("op #{first} again"));
        Violation {
            rule,
            list: self.list.to_string(),
            ops,
            explanation: format!("no order explains these operations: {text}"),
        }
    }
}

/// Whether the outcome of an operation was known by the time it completed.
fn finished_for_sure(action: &Action) -> bool {
    match action {
        Action::Read { .. } => true,
        Action::Append { outcome, .. } => matches!(outcome, AppendOutcome::Ok { .. }),
    }
}

/// The list's order: offset to value, from the final read first, then reads,
/// then subscriber deliveries, then acknowledged offsets. The first claim on
/// an offset wins; a later one that disagrees is rule 3's to report.
fn positions(history: &History, list: &str, base: u64) -> BTreeMap<u64, u64> {
    let mut position = BTreeMap::new();
    let mut claim = |element: &Element| {
        if element.offset >= base {
            position.entry(element.offset).or_insert(element.value);
        }
    };
    history
        .final_reads
        .get(list)
        .into_iter()
        .flatten()
        .for_each(&mut claim);
    for op in &history.ops {
        if let Action::Read { list: l, observed } = &op.action
            && l == list
        {
            observed.iter().for_each(&mut claim);
        }
    }
    for subscription in history.subscriptions.iter().filter(|s| s.list == list) {
        subscription.delivered.iter().for_each(&mut claim);
    }
    for op in &history.ops {
        if let Action::Append {
            list: l,
            value,
            outcome: AppendOutcome::Ok {
                offset: Some(offset),
            },
        } = &op.action
            && l == list
        {
            claim(&Element {
                offset: *offset,
                value: *value,
                skipped_before: 0,
            });
        }
    }
    position
}

#[cfg(test)]
mod tests;
