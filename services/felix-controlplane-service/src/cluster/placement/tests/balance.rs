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
    led_on(&NODES, 3, kind, name, &counts)
}

/// The same over `ids`, each shard with `rf` copies: its leader and the
/// brokers after it in `ids`, wrapping round.
fn led_on(
    ids: &[&str],
    rf: usize,
    kind: ShardKind,
    name: &str,
    counts: &[u32],
) -> Vec<ShardAssignment> {
    let mut shard = 0;
    let mut out = Vec::new();
    for (at, count) in counts.iter().enumerate() {
        for _ in 0..*count {
            let mut a = assigned(name, ids[at], &[]);
            a.key.shard = shard;
            a.key.kind = kind;
            a.replicas = (1..rf)
                .map(|i| ids[(at + i) % ids.len()].to_string())
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
    converge_on(&live(&NODES), streams, caches, existing, passes)
}

fn converge_on(
    nodes: &[Node],
    streams: &[Stream],
    caches: &[Cache],
    existing: &mut [ShardAssignment],
    passes: usize,
) -> Vec<Moved> {
    let mut moved = Vec::new();
    for _ in 0..passes {
        let plan = plan_with(
            streams,
            caches,
            nodes,
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
fn spread(existing: &[ShardAssignment], kind: Option<ShardKind>, name: Option<&str>) -> Vec<u32> {
    spread_on(&NODES, existing, kind, name)
}

fn spread_on(
    ids: &[&str],
    existing: &[ShardAssignment],
    kind: Option<ShardKind>,
    name: Option<&str>,
) -> Vec<u32> {
    let mut counts = vec![0; ids.len()];
    for a in existing.iter().filter(|a| {
        kind.is_none_or(|kind| a.key.kind == kind) && name.is_none_or(|name| a.key.stream == name)
    }) {
        let at = ids.iter().position(|n| *n == a.leader).expect("a broker");
        counts[at] += 1;
    }
    counts
}

fn within_one(counts: &[u32]) -> bool {
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
        vec![4, 4, 4]
    );
    for (kind, name) in [
        (ShardKind::Stream, "perf"),
        (ShardKind::Stream, "perf-durable"),
        (ShardKind::Stream, "perf-durable-quorum"),
        (ShardKind::Cache, "perf-quorum"),
    ] {
        let counts = spread(&existing, Some(kind), Some(name));
        assert!(within_one(&counts), "{kind:?} {name}: {counts:?}");
    }
    let total = spread(&existing, None, None);
    assert!(within_one(&total), "totals {total:?}");
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
        assert!(within_one(&counts), "{name}: {counts:?}");
    }
    assert!(within_one(&spread(&existing, None, None)));
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
    assert_eq!(spread(&placed, None, Some("new")), vec![4, 4, 4]);

    let mut existing = [existing, placed].concat();
    let moved = converge(&streams, &[], &mut existing, 400);
    assert!(
        moved.iter().all(|(key, _, _)| key.stream == "old"),
        "the new stream moved: {moved:?}"
    );
    assert_eq!(spread(&existing, None, Some("new")), vec![4, 4, 4]);
    assert!(within_one(&spread(&existing, None, Some("old"))));
}

/// A failed-over shard counts toward the broker it is promoted to in the same
/// pass. Otherwise a later shard of the stream sees that broker as two short
/// and moves a leader onto it for nothing.
#[test]
fn a_promotion_counts_toward_its_broker_in_the_same_pass() {
    let streams = vec![replicated_stream("s", 5, 2)];
    let mut gone = assigned("s", "broker-gone", &["broker-0"]);
    gone.key.shard = 0;
    let mut existing = vec![gone];
    for (shard, leader) in [
        (1, "broker-1"),
        (2, "broker-1"),
        (3, "broker-2"),
        (4, "broker-2"),
    ] {
        let mut a = assigned("s", leader, &["broker-0"]);
        a.key.shard = shard;
        existing.push(a);
    }
    let plan = plan_with(
        &streams,
        &[],
        &live(&NODES),
        &existing,
        &Level::of(&existing),
        MovePolicy::default(),
    );
    assert_eq!(
        plan.to_place()
            .map(|(_, leader, _)| leader)
            .collect::<Vec<_>>(),
        vec!["broker-0"]
    );
    assert_eq!(
        plan.moves().count(),
        0,
        "{:?}",
        plan.moves().collect::<Vec<_>>()
    );
}

fn zoned(id: &str, zone: &str) -> Node {
    let mut n = node(id, NodeLifecycle::Live, None);
    n.spec.zone = Some(zone.to_string());
    n
}

/// Six brokers in three zones, three copies a shard: balancing the stream's
/// leaders keeps every shard in three zones.
#[test]
fn balancing_across_zones_keeps_each_shard_spread() {
    let ids = ["a-0", "b-0", "c-0", "a-1", "b-1", "c-1"];
    let nodes: Vec<Node> = ids.iter().map(|id| zoned(id, &id[..1])).collect();
    let streams = vec![quorum("s", 12)];
    let mut existing = led_on(&ids, 3, ShardKind::Stream, "s", &[12, 0, 0, 0, 0, 0]);

    let moved = converge_on(&nodes, &streams, &[], &mut existing, 2_000);

    assert!(within_one(&spread_on(&ids, &existing, None, Some("s"))));
    assert_no_move_undone(&moved);
    for a in &existing {
        let zones: BTreeSet<&str> = a.nodes().map(|n| &n[..1]).collect();
        assert_eq!(zones.len(), 3, "{a:?}");
    }
}

/// Fewer copies than brokers: each move copies the log to a broker that held
/// none, and the stream still ends even.
#[test]
fn three_copies_on_five_brokers_balance() {
    let ids = ["broker-0", "broker-1", "broker-2", "broker-3", "broker-4"];
    let streams = vec![quorum("s", 10), quorum("t", 7)];
    let mut existing = [
        led_on(&ids, 3, ShardKind::Stream, "s", &[10, 0, 0, 0, 0]),
        led_on(&ids, 3, ShardKind::Stream, "t", &[0, 3, 4, 0, 0]),
    ]
    .concat();

    let moved = converge_on(&live(&ids), &streams, &[], &mut existing, 2_000);

    assert_eq!(spread_on(&ids, &existing, None, Some("s")), vec![2; 5]);
    assert!(within_one(&spread_on(&ids, &existing, None, Some("t"))));
    assert!(within_one(&spread_on(&ids, &existing, None, None)));
    assert!(existing.iter().all(|a| a.replicas.len() == 2));
    assert_no_move_undone(&moved);
}

/// A broker joining a balanced cluster takes its share of every stream, and
/// no shard moves twice.
#[test]
fn a_joining_broker_takes_its_share_without_thrash() {
    let ids = ["broker-0", "broker-1", "broker-2", "broker-3"];
    let streams = vec![quorum("s", 12), quorum("t", 6), quorum("u", 3)];
    let mut existing = [
        led_on(&ids[..3], 3, ShardKind::Stream, "s", &[4, 4, 4]),
        led_on(&ids[..3], 3, ShardKind::Stream, "t", &[2, 2, 2]),
        led_on(&ids[..3], 3, ShardKind::Stream, "u", &[1, 1, 1]),
    ]
    .concat();

    let moved = converge_on(&live(&ids), &streams, &[], &mut existing, 2_000);

    assert_eq!(spread_on(&ids, &existing, None, Some("s")), vec![3; 4]);
    for name in ["t", "u"] {
        assert!(
            within_one(&spread_on(&ids, &existing, None, Some(name))),
            "{name}"
        );
    }
    assert!(within_one(&spread_on(&ids, &existing, None, None)));
    assert_no_move_undone(&moved);
    let mut seen = BTreeSet::new();
    for (key, _, _) in &moved {
        assert!(
            seen.insert((key.stream.clone(), key.shard)),
            "{key:?} moved twice: {moved:?}"
        );
    }
    assert!(moved.iter().all(|(_, _, to)| to == "broker-3"), "{moved:?}");
}

/// A draining broker is no broker's peer for balance: it is emptied, the
/// stream spreads over the rest, and nothing is moved onto it.
#[test]
fn a_draining_broker_is_left_out_of_the_balance() {
    let ids = ["broker-0", "broker-1", "broker-2", "broker-3"];
    let mut nodes = live(&ids);
    nodes[3].status.lifecycle = NodeLifecycle::Draining;
    let streams = vec![quorum("s", 12)];
    let mut existing = led_on(&ids, 3, ShardKind::Stream, "s", &[0, 0, 6, 6]);

    let moved = converge_on(&nodes, &streams, &[], &mut existing, 2_000);

    assert_eq!(
        spread_on(&ids, &existing, None, Some("s")),
        vec![4, 4, 4, 0]
    );
    assert!(moved.iter().all(|(_, _, to)| to != "broker-3"), "{moved:?}");
    assert_no_move_undone(&moved);
}

/// A broker capped by `max_shards` is never pushed past its cap to even a
/// stream out, and the rest settle without trading shards.
#[test]
fn a_capped_broker_is_not_filled_past_its_cap() {
    let ids = ["broker-0", "broker-1", "broker-2"];
    let mut nodes = live(&ids);
    nodes[2].spec.capacity.max_shards = Some(2);
    let streams = vec![stream("s", 12)];
    let mut existing = led_on(&ids, 1, ShardKind::Stream, "s", &[10, 0, 2]);

    let moved = converge_on(&nodes, &streams, &[], &mut existing, 2_000);

    assert_eq!(spread_on(&ids, &existing, None, Some("s")), vec![5, 5, 2]);
    assert_no_move_undone(&moved);
}
