//! A move's destination while it copies: shipped to, but not counted toward
//! the quorum, and not allowed to hold a pass for the length of the copy.

use std::time::Duration;

use super::*;

/// Every answer from `slow` takes `delay`; everyone else answers at once.
struct SlowNode {
    slow: &'static str,
    delay: Duration,
}

impl PeerRequester for SlowNode {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let InternalMessage::ReplicateRecords(batch) = message else {
            panic!("the driver sent something other than a replication batch");
        };
        if node_id == self.slow {
            tokio::time::sleep(self.delay).await;
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// Publish routes where the local broker leads with `replicas`, moving to
/// `successor` if one is named.
fn publish_move(router: &ShardRouter, replicas: &[&str], successor: Option<&str>, generation: u64) {
    let nodes = nodes();
    let table = RoutingTable::build_with(
        [felix_router::Placed {
            key: key(),
            leader: LOCAL.to_string(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            generation,
            draining: false,
            successor: successor.map(str::to_string),
        }],
        &nodes,
    );
    router.publish(table, &nodes);
}

struct Cursors {
    streams: HashMap<ShardKey, ShardCursors>,
    group: HashMap<ShardKey, ShardCursors>,
    dead: HashMap<ShardKey, ShardCursors>,
    counters: HashMap<ShardKey, ShardCursors>,
}

impl Cursors {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            group: HashMap::new(),
            dead: HashMap::new(),
            counters: HashMap::new(),
        }
    }

    async fn pass(
        &mut self,
        requester: &impl PeerRequester,
        broker: &Arc<Broker>,
        router: &ShardRouter,
        marks: &QuorumMarks,
    ) -> Pass {
        replicate_once(
            requester,
            broker,
            router,
            marks,
            None,
            &mut self.streams,
            &mut self.group,
            &mut self.dead,
            &mut self.counters,
        )
        .await
    }
}

/// How long until the mark at `generation` reaches `offset`, while `pass`
/// runs.
async fn mark_reached(
    marks: &QuorumMarks,
    generation: u64,
    offset: u64,
    pass: impl std::future::Future<Output = Pass>,
) -> Duration {
    let started = tokio::time::Instant::now();
    let watched = watch_key(&key());
    let observe = async {
        loop {
            if marks.offset(&watched, generation) >= Some(offset) {
                return started.elapsed();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::join!(pass, observe).1
}

/// The P1-G case: a stream with one replica, moving. The destination is the
/// only other copy, and while it copies a `Quorum` publish must not wait for
/// it -- the stream asked for one copy and has it.
#[tokio::test(start_paused = true)]
async fn a_destination_still_copying_does_not_hold_the_quorum_mark() {
    const COPY: Duration = Duration::from_secs(60);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &[], 1);
    let marks = QuorumMarks::new();
    let mut cursors = Cursors::new();
    let slow = SlowNode {
        slow: "broker-b",
        delay: COPY,
    };
    cursors.pass(&slow, &broker, &router, &marks).await;

    publish_move(&router, &["broker-b"], Some("broker-b"), 2);
    let reached = mark_reached(&marks, 2, 3, cursors.pass(&slow, &broker, &router, &marks)).await;
    assert!(
        reached < COPY,
        "the mark waited {reached:?} for the destination's copy",
    );
}

/// Only a node this leader saw being added is left out. A successor that was
/// already a replica is part of the set the stream asked for, and counts.
#[tokio::test(start_paused = true)]
async fn a_successor_that_was_already_a_replica_still_counts() {
    const SLOW: Duration = Duration::from_secs(60);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &["broker-b"], 1);
    let marks = QuorumMarks::new();
    let mut cursors = Cursors::new();
    let slow = SlowNode {
        slow: "broker-b",
        delay: SLOW,
    };
    cursors.pass(&slow, &broker, &router, &marks).await;

    publish_move(&router, &["broker-b"], Some("broker-b"), 2);
    let reached = mark_reached(&marks, 2, 3, cursors.pass(&slow, &broker, &router, &marks)).await;
    assert!(
        reached >= SLOW,
        "the mark reached the tail after {reached:?}, before the only other \
         replica had the records",
    );
}

/// A leader with no earlier pass cannot tell a staged destination from a
/// replica it never saw, so it counts it: slower, never weaker.
#[tokio::test(start_paused = true)]
async fn a_leader_that_never_saw_the_set_before_counts_the_successor() {
    const SLOW: Duration = Duration::from_secs(60);
    let (broker, _dir) = leader_with(3).await;
    let router = router(LOCAL, &[], 1);
    publish_move(&router, &["broker-b"], Some("broker-b"), 2);
    let marks = QuorumMarks::new();
    let slow = SlowNode {
        slow: "broker-b",
        delay: SLOW,
    };

    let reached = mark_reached(
        &marks,
        2,
        3,
        Cursors::new().pass(&slow, &broker, &router, &marks),
    )
    .await;
    assert!(reached >= SLOW, "counted out after {reached:?}");
}

/// A leader with records totalling `batches` shipping batches.
async fn leader_with_batches(batches: usize) -> (Arc<Broker>, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = DurableStorage::open(
        dir.path(),
        LogConfig {
            fsync_mode: FsyncMode::None,
            preallocate_segments: false,
            ..LogConfig::default()
        },
    )
    .expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    // Just over half a batch each, so no two share one.
    let record = Bytes::from(vec![7u8; MAX_BATCH_BYTES / 2 + 1]);
    for _ in 0..batches {
        log.append(std::slice::from_ref(&record))
            .await
            .expect("append");
    }
    let broker = Broker::new(EphemeralCache::new().into()).with_durable_storage(storage);
    (Arc::new(broker), dir)
}

/// A pass ends when its slowest follower does. A copy shipped to the tail in
/// one pass would hold the next mark -- and every `Quorum` publish -- for the
/// whole copy, so the destination gets a slice per pass and the driver comes
/// straight back for more.
#[tokio::test(start_paused = true)]
async fn a_copy_is_shipped_a_slice_at_a_time() {
    const PER_BATCH: Duration = Duration::from_millis(200);
    const BATCHES: usize = 6;
    let (broker, _dir) = leader_with_batches(BATCHES).await;
    let router = router(LOCAL, &["broker-c"], 1);
    let marks = QuorumMarks::new();
    let mut cursors = Cursors::new();
    let slow = SlowNode {
        slow: "broker-b",
        delay: PER_BATCH,
    };
    cursors.pass(&slow, &broker, &router, &marks).await;

    publish_move(&router, &["broker-c", "broker-b"], Some("broker-b"), 2);
    let started = tokio::time::Instant::now();
    let pass = cursors.pass(&slow, &broker, &router, &marks).await;
    let took = started.elapsed();
    assert!(
        took < PER_BATCH * 2,
        "one pass took {took:?}: it waited for the copy instead of a slice of it",
    );
    assert!(pass.copying, "the pass should say the copy is unfinished");

    let mut passes = 1;
    while cursors.pass(&slow, &broker, &router, &marks).await.copying {
        passes += 1;
        assert!(passes <= BATCHES, "the copy never finished");
    }
    let destination = cursors.streams[&key()]
        .followers
        .iter()
        .find(|follower| follower.node_id == "broker-b")
        .expect("destination cursor")
        .next_offset;
    assert_eq!(destination, BATCHES as u64, "the copy reached the tail");
}
