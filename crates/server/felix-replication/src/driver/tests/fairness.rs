//! Which shard gets a pass slot when more shards have work than there are
//! slots.
//!
//! These drive `Shards` by hand: a scan after every append, as a busy broker
//! scans, and one event between scans, so who holds the slots is exact
//! rather than a matter of timing.

use std::time::Duration;

use felix_router::ShardRouter;

use super::*;
use crate::status::ShardStatusBoard;

/// How long a follower takes to store a batch. Long enough that a scan lands
/// between any two passes ending.
const STORE: Duration = Duration::from_millis(5);

/// Shards with an append on every scan: more than there are slots.
const HOT: u32 = 24;

/// The shard with a single append, added once the hot shards hold every slot.
const COLD: u32 = HOT;

/// Stores every batch after [`STORE`], and records which shard it was for.
#[derive(Default)]
struct Follower {
    stored: Mutex<Vec<(u32, u64)>>,
    /// Followers that are gone: every request to them fails.
    gone: &'static [&'static str],
    /// Followers cut off for now: like `gone`, until taken off the list.
    cut_off: Mutex<Vec<&'static str>>,
    /// Followers that are up but refuse every batch as `StaleRoute`.
    refusing: Mutex<Vec<&'static str>>,
    /// Followers that are up but fail to store every batch.
    failing: Mutex<Vec<&'static str>>,
}

impl Follower {
    fn stored_for(&self, shard: u32) -> bool {
        self.stored
            .lock()
            .expect("lock")
            .iter()
            .any(|(held, _)| *held == shard)
    }
}

impl PeerRequester for Follower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        if self.gone.contains(&node_id) || self.cut_off.lock().expect("lock").contains(&node_id) {
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "gone".to_string(),
            });
        }
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        tokio::time::sleep(STORE).await;
        let code = if self.refusing.lock().expect("lock").contains(&node_id) {
            Some(felix_wire::internal::ErrorCode::StaleRoute)
        } else if self.failing.lock().expect("lock").contains(&node_id) {
            Some(felix_wire::internal::ErrorCode::StorageFailed)
        } else {
            None
        };
        if let Some(code) = code {
            return Ok(InternalMessage::ReplicateError(
                felix_wire::internal::ReplicateError {
                    correlation_id: 0,
                    code,
                    expected_offset: 0,
                    detail: "behind".to_string(),
                },
            ));
        }
        let durable_offset = batch.first_offset + batch.payloads.len() as u64;
        self.stored
            .lock()
            .expect("lock")
            .push((batch.shard.shard, durable_offset));
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset,
            learner: false,
        }))
    }
}

fn shard_key(shard: u32) -> ShardKey {
    ShardKey { shard, ..key() }
}

/// Lead shards `0..shards`, each with followers b and c.
fn lead(router: &ShardRouter, shards: u32) {
    let nodes = nodes();
    let table = RoutingTable::build(
        (0..shards).map(|shard| {
            (
                shard_key(shard),
                LOCAL.to_string(),
                vec!["broker-b".to_string(), "broker-c".to_string()],
                1,
            )
        }),
        &nodes,
    );
    router.publish(table, &nodes);
}

/// Append one record to `shard`, and mark it appended as the publish path does.
async fn append(broker: &Arc<Broker>, shard: u32) {
    let log = broker
        .shard_log(
            felix_broker::LogKind::Stream,
            TENANT,
            NAMESPACE,
            STREAM,
            shard,
        )
        .await
        .expect("log");
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        runtime
            .block_on(log.append(&[Bytes::from_static(b"v")]))
            .expect("append")
    })
    .await
    .expect("append task");
    broker
        .appended_shards()
        .mark(felix_broker::AppendedShards::slot(
            TENANT, NAMESPACE, STREAM, shard,
        ));
}

/// Wait for one event and act on it, as the driver loop does. True when a
/// pass ended.
async fn step(shards: &mut Shards<'_, Follower>) -> bool {
    tokio::select! {
        Some((scan, ended)) = shards.passes.next(), if !shards.passes.is_empty() => {
            shards.fold(scan, ended);
            true
        }
        Some(answer) = shards.exchanges.next(), if !shards.exchanges.is_empty() => {
            shards.apply(answer);
            false
        }
    }
}

/// **A shard with one append gets a pass while busier shards keep
/// appending.** More shards than slots append before every scan, and one
/// more shard takes a single append once they hold every slot, which is the
/// worst place to ask from whatever order the table iterates in. Its pass
/// must start once the shards that asked before it have had theirs, not wait
/// until the others go quiet.
#[tokio::test(start_paused = true)]
async fn a_shard_with_one_append_is_not_starved_by_busier_ones() {
    let (broker, _dir) = leader_with_shards(HOT + 1, 0).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, HOT);
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let peers = Follower::default();
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });

    for shard in 0..HOT {
        append(&broker, shard).await;
    }
    shards.scan(WakeKind::Append);
    assert_eq!(shards.passes.len(), SHARD_CONCURRENCY, "the slots are full");

    lead(&router, HOT + 1);
    append(&broker, COLD).await;

    // The hot shards past the slots asked first, so they go first; then it
    // is the cold shard's turn, one pass ending per slot handed over.
    let cold = watch_key(&shard_key(COLD));
    let started = || {
        status
            .live_report(&cold)
            .is_some_and(|live| live.pass.is_some())
    };
    let bound = (HOT as usize - SHARD_CONCURRENCY) + 1;
    let mut folds = 0;
    while !started() {
        assert!(
            folds < bound,
            "the shard with one append got no pass in {folds} passes ending"
        );
        for shard in 0..HOT {
            append(&broker, shard).await;
        }
        shards.scan(WakeKind::Append);
        // A scan after every pass that ends, as an append stream gives.
        while !step(&mut shards).await {}
        folds += 1;
    }
    while !peers.stored_for(COLD) {
        step(&mut shards).await;
    }
}

/// **A scan woken by an append leaves settled shards alone.** Once every
/// shard's followers hold its whole log, an append to one shard passes that
/// shard and no other; the tick still passes them all.
#[tokio::test(start_paused = true)]
async fn an_append_passes_only_the_shard_it_was_for() {
    const SHARDS: u32 = 4;
    let (broker, _dir) = leader_with_shards(SHARDS, 1).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, SHARDS);
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let peers = Follower::default();
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });
    let settle =
        |shards: &mut Shards<'_, Follower>| shards.passes.is_empty() && shards.exchanges.is_empty();

    // A pass to ship each shard's record, and one to see the followers level.
    for _ in 0..2 {
        shards.scan(WakeKind::Tick);
        while !settle(&mut shards) {
            step(&mut shards).await;
        }
    }

    append(&broker, 2).await;
    shards.scan(WakeKind::Append);
    assert_eq!(shards.passes.len(), 1, "an idle shard got a pass");
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    let stored = peers.stored.lock().expect("lock").clone();
    assert!(
        stored.contains(&(2, 2)),
        "the appended record was not shipped"
    );

    shards.scan(WakeKind::Tick);
    assert_eq!(
        shards.passes.len(),
        SHARDS as usize,
        "the tick skipped a shard"
    );
}

/// **A follower that is gone does not get every shard a pass on each
/// append.** Its shards never settle, since it never holds their tail, but a
/// pass cannot change that. Asked on every append anywhere, each of them took
/// a slot from the shards with writes waiting, and under a steady load those
/// writes timed out while one follower was down (#1944).
#[tokio::test(start_paused = true)]
async fn an_append_passes_only_its_shard_while_a_follower_is_gone() {
    append_passes_only_its_shard(&["broker-c"]).await;
}

/// **Nor does a shard whose followers are all gone, though its mark is
/// owed.** No pass can raise the mark until one of them answers, so the
/// shards wait for the tick rather than taking a slot on every append.
#[tokio::test(start_paused = true)]
async fn an_append_passes_only_its_shard_while_every_follower_is_gone() {
    append_passes_only_its_shard(&["broker-b", "broker-c"]).await;
}

async fn append_passes_only_its_shard(gone: &'static [&'static str]) {
    const SHARDS: u32 = 4;
    let (broker, _dir) = leader_with_shards(SHARDS, 1).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, SHARDS);
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let peers = Follower {
        gone,
        ..Follower::default()
    };
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });
    let settle =
        |shards: &mut Shards<'_, Follower>| shards.passes.is_empty() && shards.exchanges.is_empty();

    for _ in 0..2 {
        shards.scan(WakeKind::Tick);
        while !settle(&mut shards) {
            step(&mut shards).await;
        }
    }

    append(&broker, 2).await;
    shards.scan(WakeKind::Append);
    assert_eq!(
        shards.passes.len(),
        1,
        "shards nothing was appended to got a pass"
    );
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    if gone.len() < 2 {
        assert!(
            peers.stored.lock().expect("lock").contains(&(2, 2)),
            "the appended record was not shipped to the follower still up"
        );
    }
}

/// **A shard whose only live follower refused a batch still gets a pass on
/// the next append, and its mark reaches the tail.** A refusal such as
/// `StaleRoute` comes from a follower that is up; in a fleet that keeps
/// sets, the next pass sends it the fence again and brings it level.
/// Counted as out of reach like a dead one, the shard waited for a tick in
/// the refresh queue, behind every written shard, while its write waited on
/// the mark.
#[tokio::test(start_paused = true)]
async fn a_shard_whose_follower_refused_gets_a_pass_on_the_next_append() {
    use felix_common::fleet::{FleetGate, REPLICA_ELECTION};

    let names = [REPLICA_ELECTION.name()];
    let fleet = FleetGate::new(names);
    fleet.restart(names);
    refused_follower_on_next_append(QuorumMarks::with_fleet(Arc::new(fleet)), true).await;
}

/// **A refusal no pass can fix leaves the shard to the tick.** Without a
/// candidacy or a fleet that keeps sets, a pass sends a follower whose
/// routing view is behind nothing that brings it level: it catches up from
/// the control plane. Passing the shard on every append would only take
/// slots from shards with writes waiting.
#[tokio::test(start_paused = true)]
async fn a_refusal_no_pass_can_fix_does_not_make_its_shard_owed() {
    refused_follower_on_next_append(QuorumMarks::new(), false).await;
}

/// Lead two shards with broker-b gone, have broker-c refuse a batch on shard
/// 0 as `StaleRoute`, then append to shard 1: shard 0 gets a pass too only
/// when `can_refence`.
async fn refused_follower_on_next_append(marks: QuorumMarks, can_refence: bool) {
    let (broker, _dir) = leader_with_shards(2, 1).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, 2);
    let status = ShardStatusBoard::new();
    let peers = Follower {
        gone: &["broker-b"],
        ..Follower::default()
    };
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });
    let settle =
        |shards: &mut Shards<'_, Follower>| shards.passes.is_empty() && shards.exchanges.is_empty();
    for _ in 0..2 {
        shards.scan(WakeKind::Tick);
        while !settle(&mut shards) {
            step(&mut shards).await;
        }
    }
    let watched = watch_key(&shard_key(0));
    let before = marks.offset(&watched, 1);

    peers.refusing.lock().expect("lock").push("broker-c");
    append(&broker, 0).await;
    shards.scan(WakeKind::Append);
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    assert_eq!(
        marks.offset(&watched, 1),
        before,
        "a refused batch moved the mark"
    );

    peers.refusing.lock().expect("lock").clear();
    append(&broker, 1).await;
    shards.scan(WakeKind::Append);
    if !can_refence {
        assert_eq!(
            shards.passes.len(),
            1,
            "a refusal no pass can fix got its shard a pass on another shard's append"
        );
        return;
    }
    assert_eq!(
        shards.passes.len(),
        2,
        "the shard with a write waiting on its mark got no pass"
    );
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    assert!(
        marks.offset(&watched, 1) > before,
        "the mark did not reach the tail once the follower answered"
    );
}

/// **A refusal that arrives after its pass ended leaves the shard owed too.**
/// Right after an election the exchange to a follower can outlive the pass,
/// which then found no follower in reach. The follower's `StaleRoute`, merged
/// on its own, must still get the shard a pass on the next append.
#[tokio::test(start_paused = true)]
async fn a_straggler_refusal_leaves_its_shard_owed() {
    let (broker, _dir) = leader_with_shards(2, 1).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, 2);
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let peers = Follower {
        gone: &["broker-b"],
        ..Follower::default()
    };
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });
    let settle =
        |shards: &mut Shards<'_, Follower>| shards.passes.is_empty() && shards.exchanges.is_empty();
    for _ in 0..2 {
        shards.scan(WakeKind::Tick);
        while !settle(&mut shards) {
            step(&mut shards).await;
        }
    }
    let watched = watch_key(&shard_key(0));
    let before = marks.offset(&watched, 1);

    peers.cut_off.lock().expect("lock").push("broker-c");
    append(&broker, 0).await;
    shards.scan(WakeKind::Append);
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    peers.cut_off.lock().expect("lock").clear();
    let mut answer = shards.answer_held(&shard_key(0), "broker-c", false);
    answer.cursor.stalled = true;
    answer.cursor.route_behind = true;
    answer.cursor.refused = true;
    shards.apply(answer);

    append(&broker, 1).await;
    shards.scan(WakeKind::Append);
    assert_eq!(
        shards.passes.len(),
        2,
        "the shard whose follower refused after its pass got no pass"
    );
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    assert!(
        marks.offset(&watched, 1) > before,
        "the mark did not reach the tail once the follower answered"
    );
}

/// **A follower refusing for a reason a pass cannot fix leaves its shard to
/// the tick.** A full disk or a rejected credential answers every batch the
/// same way, so a pass on each append would only take slots from shards
/// with writes waiting, as a dead follower's would (#1944).
#[tokio::test(start_paused = true)]
async fn a_follower_failing_every_batch_does_not_make_its_shard_owed() {
    let (broker, _dir) = leader_with_shards(2, 1).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, 2);
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let peers = Follower {
        gone: &["broker-b"],
        ..Follower::default()
    };
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });
    let settle =
        |shards: &mut Shards<'_, Follower>| shards.passes.is_empty() && shards.exchanges.is_empty();
    for _ in 0..2 {
        shards.scan(WakeKind::Tick);
        while !settle(&mut shards) {
            step(&mut shards).await;
        }
    }

    peers.failing.lock().expect("lock").push("broker-c");
    append(&broker, 0).await;
    shards.scan(WakeKind::Append);
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    append(&broker, 1).await;
    shards.scan(WakeKind::Append);
    assert_eq!(
        shards.passes.len(),
        1,
        "a shard no pass can help got one on another shard's append"
    );
}

/// **But not once the mark is level.** Another follower carried the mark to
/// the tail, so no write waits on the shard; a follower stuck behind on
/// routing would otherwise get each of its shards a pass on every append.
#[tokio::test(start_paused = true)]
async fn a_straggler_refusal_on_a_level_shard_leaves_it_alone() {
    let (broker, _dir) = leader_with_shards(2, 1).await;
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        felix_router::RegionRouter::new("us-west-2".to_string()),
    ));
    lead(&router, 2);
    let marks = QuorumMarks::new();
    let status = ShardStatusBoard::new();
    let peers = Follower::default();
    let rebuilds = Rebuilds::new(RebuildPolicy::default());
    let throttle = MoveThrottle::unlimited();
    let mut shards = Shards::new(Context {
        requester: &peers,
        broker: &broker,
        router: &router,
        fence: &Unfenced,
        gate: &crate::promotion::NoGate,
        marks: &marks,
        status: &status,
        reporter: None,
        rebuilds: &rebuilds,
        throttle: &throttle,
        refresh: Duration::from_secs(300),
    });
    let settle =
        |shards: &mut Shards<'_, Follower>| shards.passes.is_empty() && shards.exchanges.is_empty();
    for _ in 0..2 {
        shards.scan(WakeKind::Tick);
        while !settle(&mut shards) {
            step(&mut shards).await;
        }
    }

    peers.cut_off.lock().expect("lock").push("broker-c");
    append(&broker, 0).await;
    shards.scan(WakeKind::Append);
    while !settle(&mut shards) {
        step(&mut shards).await;
    }
    assert_eq!(marks.offset(&watch_key(&shard_key(0)), 1), Some(2));
    let mut answer = shards.answer_held(&shard_key(0), "broker-c", false);
    answer.cursor.stalled = true;
    answer.cursor.route_behind = true;
    answer.cursor.refused = true;
    shards.apply(answer);

    append(&broker, 1).await;
    shards.scan(WakeKind::Append);
    assert_eq!(
        shards.passes.len(),
        1,
        "a level shard got a pass for a follower stuck behind on routing"
    );
}
