//! Leaders spread per stream as well as per node: a stream whose shards all
//! lead on one broker runs on that broker alone, however even the cluster's
//! totals look.
use std::collections::HashMap;

use super::*;
use crate::model::ConsistencyLevel;

const NODES: [&str; 3] = ["broker-0", "broker-1", "broker-2"];

/// Every copy level with its leader at the generation the shard is at, and
/// every fenced leader drained.
struct Level {
    generations: HashMap<ShardKey, u64>,
    draining: std::collections::HashSet<ShardKey>,
}

impl Level {
    fn of(existing: &[ShardAssignment]) -> Self {
        Self {
            generations: existing
                .iter()
                .map(|a| (a.key.clone(), a.generation))
                .collect(),
            draining: existing
                .iter()
                .filter(|a| a.state == ShardState::Draining)
                .map(|a| a.key.clone())
                .collect(),
        }
    }
}

impl CaughtUp for Level {
    fn is_caught_up(&self, _key: &ShardKey, _node_id: &str) -> bool {
        true
    }

    fn is_drained(&self, key: &ShardKey, generation: u64) -> bool {
        self.draining.contains(key) && self.generations.get(key) == Some(&generation)
    }

    fn reported_generation(&self, key: &ShardKey) -> Option<u64> {
        self.generations.get(key).copied()
    }
}

fn quorum(name: &str, shards: u32) -> Stream {
    Stream {
        consistency: ConsistencyLevel::Quorum,
        ..replicated_stream(name, shards, 3)
    }
}

fn quorum_cache(name: &str, shards: u32) -> Cache {
    Cache {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        cache: name.to_string(),
        display_name: name.to_string(),
        shards,
        replication_factor: 3,
        consistency: ConsistencyLevel::Quorum,
        incarnation: 0,
    }
}

/// `kind`/`name`'s shards led as `counts` says, in order: the first
/// `counts[0]` on `broker-0`, and so on. Every broker holds a copy.
fn led(kind: ShardKind, name: &str, counts: [u32; 3]) -> Vec<ShardAssignment> {
    let mut shard = 0;
    let mut out = Vec::new();
    for (node, count) in NODES.iter().zip(counts) {
        for _ in 0..count {
            let mut a = assigned(name, node, &[]);
            a.key.shard = shard;
            a.key.kind = kind;
            a.replicas = NODES
                .iter()
                .filter(|n| *n != node)
                .map(|n| n.to_string())
                .collect();
            out.push(a);
            shard += 1;
        }
    }
    out
}

/// One shard move: the shard, and the leader it left for the one it went to.
type Moved = (ShardKey, String, String);

/// Run passes, applying every write, until one writes nothing. Returns the
/// cut-overs made, in order.
fn converge(
    streams: &[Stream],
    caches: &[Cache],
    existing: &mut [ShardAssignment],
    passes: usize,
) -> Vec<Moved> {
    let nodes = live(&NODES);
    let mut moved = Vec::new();
    for _ in 0..passes {
        let plan = plan_with(
            streams,
            caches,
            &nodes,
            existing,
            &Level::of(existing),
            MovePolicy::default(),
        );
        let mut wrote = false;
        for (key, step, next) in plan.moves() {
            let slot = existing.iter_mut().find(|a| &a.key == key).expect("known");
            if let MoveStep::CutOver { from, to } = step {
                moved.push((key.clone(), from.clone(), to.clone()));
            }
            let mut next = next.clone();
            next.generation = slot.generation + 1;
            *slot = next;
            wrote = true;
        }
        if !wrote {
            return moved;
        }
    }
    panic!("still moving after {passes} passes: {moved:?}");
}

/// Leaders per broker of `name` (any kind when `kind` is `None`).
fn spread(existing: &[ShardAssignment], kind: Option<ShardKind>, name: Option<&str>) -> [u32; 3] {
    let mut counts = [0; 3];
    for a in existing.iter().filter(|a| {
        kind.is_none_or(|kind| a.key.kind == kind) && name.is_none_or(|name| a.key.stream == name)
    }) {
        let at = NODES.iter().position(|n| *n == a.leader).expect("a broker");
        counts[at] += 1;
    }
    counts
}

fn within_one(counts: [u32; 3]) -> bool {
    counts.iter().max().unwrap_or(&0) - counts.iter().min().unwrap_or(&0) <= 1
}

/// No shard goes back to a broker it was moved off: the moves only ever
/// narrow a spread, so none undoes another.
fn assert_no_move_undone(moved: &[Moved]) {
    for (i, (key, from, to)) in moved.iter().enumerate() {
        assert!(
            !moved[i + 1..]
                .iter()
                .any(|(k, f, t)| k == key && f == to && t == from),
            "{key:?} moved {from} -> {to} and back: {moved:?}"
        );
    }
}

/// The campaign's case: other streams fill two brokers to their share of
/// the cluster, and a 12-shard stream sits 1/1/10. It ends 4/4/4 and the
/// totals stay within the cluster's share.
#[test]
fn a_twelve_shard_stream_on_three_brokers_ends_four_four_four() {
    let streams = vec![
        quorum("perf", 12),
        quorum("perf-durable", 12),
        quorum("perf-durable-quorum", 12),
        quorum("perf-quorum", 12),
    ];
    let caches = vec![quorum_cache("perf-quorum", 12)];
    let mut existing: Vec<ShardAssignment> = [
        led(ShardKind::Stream, "perf", [8, 4, 0]),
        led(ShardKind::Stream, "perf-durable", [5, 6, 1]),
        led(ShardKind::Stream, "perf-durable-quorum", [2, 4, 6]),
        led(ShardKind::Stream, "perf-quorum", [1, 1, 10]),
        led(ShardKind::Cache, "perf-quorum", [5, 5, 2]),
    ]
    .concat();

    let moved = converge(&streams, &caches, &mut existing, 400);

    assert_eq!(
        spread(&existing, Some(ShardKind::Stream), Some("perf-quorum")),
        [4, 4, 4]
    );
    for (kind, name) in [
        (ShardKind::Stream, "perf"),
        (ShardKind::Stream, "perf-durable"),
        (ShardKind::Stream, "perf-durable-quorum"),
        (ShardKind::Cache, "perf-quorum"),
    ] {
        let counts = spread(&existing, Some(kind), Some(name));
        assert!(within_one(counts), "{kind:?} {name}: {counts:?}");
    }
    let total = spread(&existing, None, None);
    assert!(within_one(total), "totals {total:?}");
    assert_no_move_undone(&moved);
}

/// Several streams, each lopsided a different way, balance per stream and
/// in total, with no move undoing another and nothing moving once settled.
#[test]
fn several_streams_balance_per_stream_and_in_total() {
    let streams = vec![
        quorum("a", 12),
        quorum("b", 7),
        quorum("c", 4),
        quorum("d", 1),
        quorum("e", 2),
    ];
    let mut existing: Vec<ShardAssignment> = [
        led(ShardKind::Stream, "a", [0, 0, 12]),
        led(ShardKind::Stream, "b", [7, 0, 0]),
        led(ShardKind::Stream, "c", [0, 4, 0]),
        led(ShardKind::Stream, "d", [0, 0, 1]),
        led(ShardKind::Stream, "e", [0, 0, 2]),
    ]
    .concat();

    let moved = converge(&streams, &[], &mut existing, 400);

    for name in ["a", "b", "c", "d", "e"] {
        let counts = spread(&existing, None, Some(name));
        assert!(within_one(counts), "{name}: {counts:?}");
    }
    assert!(within_one(spread(&existing, None, None)));
    assert_no_move_undone(&moved);
    let shard_moves: HashMap<&ShardKey, usize> =
        moved.iter().fold(HashMap::new(), |mut by, (key, _, _)| {
            *by.entry(key).or_default() += 1;
            by
        });
    assert!(
        shard_moves.values().all(|n| *n == 1),
        "a shard moved twice: {moved:?}"
    );
}

/// A stream created beside brokers already at their share of the cluster's
/// leaders is placed evenly from the start, so it needs no move of its own.
#[test]
fn a_new_stream_beside_full_brokers_is_placed_evenly() {
    let streams = vec![quorum("old", 12), quorum("new", 12)];
    let existing = led(ShardKind::Stream, "old", [6, 6, 0]);
    let plan = plan_with(
        &streams,
        &[],
        &live(&NODES),
        &existing,
        &Level::of(&existing),
        MovePolicy::default(),
    );
    let placed: Vec<ShardAssignment> = plan
        .to_place()
        .map(|(key, leader, replicas)| assignment_for(key, leader, replicas.to_vec()))
        .collect();
    assert_eq!(spread(&placed, None, Some("new")), [4, 4, 4]);

    let mut existing = [existing, placed].concat();
    let moved = converge(&streams, &[], &mut existing, 400);
    assert!(
        moved.iter().all(|(key, _, _)| key.stream == "old"),
        "the new stream moved: {moved:?}"
    );
    assert_eq!(spread(&existing, None, Some("new")), [4, 4, 4]);
    assert!(within_one(spread(&existing, None, Some("old"))));
}
