//! A stream's group and dead-letter logs: they hold no pass on a follower
//! that is gone, they reach a follower in the order they are written, and a
//! follower short of them is no candidate.
//!
//! They ride each follower's exchange, so a follower that stops answering is
//! handed off like any other straggler instead of waited on at the end of the
//! pass. The clock is paused in those tests, so "well under a second" against
//! a peer that takes a minute is exact.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;

use crate::FollowerCursor;

use super::*;

/// How long a mark may take once there is a majority for it.
const PROMPT: Duration = Duration::from_millis(500);

const GENERATION: u64 = 4;

/// Stores everything at once until [`Self::kill`]; after that, whatever is
/// sent to `gone` sits on a half-open connection for a minute and fails.
struct Dies {
    gone: &'static str,
    dead: AtomicBool,
    /// Auxiliary records stored by the followers still alive.
    aux_stored: AtomicUsize,
}

impl Dies {
    fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }
}

impl PeerRequester for Dies {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let (batch, aux) = match message {
            InternalMessage::ReplicateRecords(batch) => (batch, false),
            InternalMessage::ReplicateGroupRecords(batch)
            | InternalMessage::ReplicateDeadLetterRecords(batch) => (batch, true),
            other => panic!("unexpected {:?}", other.kind()),
        };
        if node_id == self.gone && self.dead.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_secs(60)).await;
            return Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "gone".to_string(),
            });
        }
        if aux {
            self.aux_stored
                .fetch_add(batch.payloads.len(), Ordering::SeqCst);
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// A broker leading three records on the shard, with consumer groups.
async fn leader_with_groups(
    dir: &std::path::Path,
) -> (
    Arc<Broker>,
    Arc<felix_broker::ConsumerGroups>,
    Arc<felix_broker::DeadLetters>,
) {
    let config = LogConfig {
        fsync_mode: FsyncMode::None,
        preallocate_segments: false,
        ..LogConfig::default()
    };
    let storage = DurableStorage::open(dir.join("streams"), config.clone()).expect("storage");
    let log = storage
        .open_stream(TENANT, NAMESPACE, STREAM, 0)
        .expect("open");
    for i in 0..3 {
        log.append(&[Bytes::from(format!("v{i}"))])
            .await
            .expect("append");
    }
    let groups = Arc::new(
        felix_broker::ConsumerGroups::open(dir.join("groups"), config.clone()).expect("groups"),
    );
    let dead_letters = Arc::new(
        felix_broker::DeadLetters::open(dir.join("dead-letters"), config).expect("dead letters"),
    );
    let broker = Arc::new(
        Broker::new(EphemeralCache::new().into())
            .with_durable_storage(storage)
            .with_consumer_groups(
                Arc::clone(&groups),
                Arc::clone(&dead_letters),
                Duration::from_secs(30),
                3,
            ),
    );
    (broker, groups, dead_letters)
}

fn driver(
    requester: &Arc<Dies>,
    broker: &Arc<Broker>,
    router: &Arc<ShardRouter>,
    marks: &Arc<QuorumMarks>,
) -> Replication {
    spawn(
        Arc::clone(requester),
        Arc::clone(broker),
        Arc::clone(router),
        Arc::new(Unfenced),
        Arc::new(crate::promotion::NoGate),
        Published {
            marks: Arc::clone(marks),
            halted: Arc::new(crate::halted::HaltedReplicas::new()),
            status: Arc::default(),
        },
        None,
        // Reaching the tick would mean nothing else woke the driver.
        Duration::from_secs(300),
        Arc::default(),
        RebuildPolicy::default(),
        MoveThrottle::unlimited(),
        CancellationToken::new(),
    )
}

/// Run `work` off the paused runtime and wake the driver, as a write path
/// does. The logs write on their own threads, and a paused runtime with
/// nothing to poll jumps its clock to the next timer meanwhile, which would
/// be the gone follower's. Blocking work holds the clock.
async fn write<F>(broker: &Broker, work: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || runtime.block_on(work))
        .await
        .expect("write task");
    broker.appended().notify_one();
}

async fn append(broker: &Arc<Broker>) {
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    write(broker, async move {
        log.append(&[Bytes::from_static(b"late")])
            .await
            .expect("append");
    })
    .await;
}

/// Whether the shard's mark reaches `offset` within [`PROMPT`].
async fn prompt(marks: &QuorumMarks, offset: u64) -> bool {
    let watched = watch_key(&key());
    tokio::time::timeout(PROMPT, async {
        while marks.offset(&watched, GENERATION) < Some(offset) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}

/// c dies once every log is level on it. Then `change` writes to a log that
/// rides the shard, and a pass ships it with the shard's own log level on
/// everyone: c's exchange has nothing of the shard's to send. A record
/// published next must still be acknowledged on b alone.
async fn dead_follower_after<F, Fut>(change: F)
where
    F: FnOnce(Arc<felix_broker::ConsumerGroups>, Arc<felix_broker::DeadLetters>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, groups, dead_letters) = leader_with_groups(dir.path()).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], GENERATION);
    let marks = Arc::new(QuorumMarks::new());
    let peer = Arc::new(Dies {
        gone: "broker-c",
        dead: AtomicBool::new(false),
        aux_stored: AtomicUsize::new(0),
    });
    let driver = driver(&peer, &broker, &router, &marks);
    assert!(prompt(&marks, 3).await, "the first mark waited");
    // Let c finish its exchange, so it is level and not busy when it dies.
    tokio::time::sleep(Duration::from_millis(50)).await;
    peer.kill();

    write(&broker, change(groups, dead_letters)).await;
    // That pass is under way, and shipping to c, before the record lands.
    tokio::time::sleep(Duration::from_millis(50)).await;
    append(&broker).await;
    assert!(
        prompt(&marks, 4).await,
        "the next pass waited on the dead follower's auxiliary logs",
    );
    assert!(
        peer.aux_stored.load(Ordering::SeqCst) > 0,
        "the change never reached the follower that is alive",
    );
    driver.stop().await;
}

/// **A group cursor change holds no pass on a dead follower.**
#[tokio::test(start_paused = true)]
async fn a_dead_follower_holds_no_pass_on_a_group_cursor() {
    dead_follower_after(|groups, _| async move {
        groups
            .commit(TENANT, NAMESPACE, STREAM, 0, "workers", 2)
            .await
            .expect("commit");
    })
    .await;
}

/// **Nor does a dead letter.**
#[tokio::test(start_paused = true)]
async fn a_dead_follower_holds_no_pass_on_a_dead_letter() {
    dead_follower_after(|_, dead_letters| async move {
        dead_letters
            .record(
                &felix_broker::GroupKey {
                    tenant_id: TENANT.to_string(),
                    namespace: NAMESPACE.to_string(),
                    stream: STREAM.to_string(),
                    shard: 0,
                    group: "workers".to_string(),
                },
                1,
            )
            .await
            .expect("dead letter");
        // Synced here, off the paused clock: the leader syncs dead letters
        // before it ships them, and a sync inside the pass would let the
        // paused clock jump to the dead follower's timer.
        felix_broker::StreamLog::from_log(
            dead_letters
                .shard_log(TENANT, NAMESPACE, STREAM, 0)
                .await
                .expect("log"),
        )
        .sync()
        .await
        .expect("sync");
    })
    .await;
}

fn workers() -> felix_broker::GroupKey {
    felix_broker::GroupKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        group: "workers".to_string(),
    }
}

/// Stores every batch, and records each group batch that lands as
/// `(follower, log, records)`. Once `crash_after` group batches have landed
/// the leader is gone and nothing more reaches anyone. `refuses` takes the
/// shard's records and none of its group batches.
#[derive(Default)]
struct GroupFollower {
    landed: Mutex<Vec<(String, &'static str, usize)>>,
    crash_after: Option<usize>,
    refuses: Option<&'static str>,
    /// Runs once on the leader, as the first dead-letter batch lands. The
    /// next dead-letter batch then fails, so what it wrote is not on the
    /// follower when the cursors go.
    on_dead_letters: Mutex<Option<BoxFuture<'static, ()>>>,
    lose_next_dead_letters: AtomicBool,
}

impl GroupFollower {
    fn landed_on(&self, node: &str) -> Vec<(&'static str, usize)> {
        self.landed
            .lock()
            .expect("lock")
            .iter()
            .filter(|(to, _, _)| to == node)
            .map(|(_, log, records)| (*log, *records))
            .collect()
    }
}

impl PeerRequester for GroupFollower {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> std::result::Result<InternalMessage, PeerError> {
        let crashed = self
            .crash_after
            .is_some_and(|after| self.landed.lock().expect("lock").len() >= after);
        let gone = || {
            Err(PeerError::Unavailable {
                node_id: node_id.to_string(),
                detail: "gone".to_string(),
            })
        };
        if crashed {
            return gone();
        }
        let (batch, log) = match message {
            InternalMessage::ReplicateRecords(batch) => (batch, None),
            InternalMessage::ReplicateGroupRecords(batch) => (batch, Some("cursors")),
            InternalMessage::ReplicateDeadLetterRecords(batch) => (batch, Some("dead letters")),
            other => panic!("unexpected {:?}", other.kind()),
        };
        if let Some(log) = log {
            if self.refuses == Some(node_id)
                || (log == "dead letters"
                    && self.lose_next_dead_letters.swap(false, Ordering::SeqCst))
            {
                return gone();
            }
            self.landed.lock().expect("lock").push((
                node_id.to_string(),
                log,
                batch.payloads.len(),
            ));
            if log == "dead letters" {
                let write = self.on_dead_letters.lock().expect("lock").take();
                if let Some(write) = write {
                    write.await;
                    self.lose_next_dead_letters.store(true, Ordering::SeqCst);
                }
            }
        }
        Ok(InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: 0,
            durable_offset: batch.first_offset + batch.payloads.len() as u64,
        }))
    }
}

/// One pass of the driver over the shard.
async fn group_pass(
    follower: &GroupFollower,
    broker: &Arc<Broker>,
    router: &ShardRouter,
    cursors: &mut [Map<ShardKey, ShardCursors>; 4],
) -> Pass {
    let [main, group, dead_letters, counters] = cursors;
    replicate_once_with(
        follower,
        broker,
        router,
        &crate::driver::Unfenced,
        &crate::promotion::NoGate,
        &QuorumMarks::new(),
        None,
        main,
        group,
        dead_letters,
        counters,
        &Rebuilds::disabled(),
        &MoveThrottle::unlimited(),
    )
    .await
}

/// A group gave up on offset 1 and moved past it.
async fn gave_up_on_one(
    groups: &felix_broker::ConsumerGroups,
    dead_letters: &felix_broker::DeadLetters,
) {
    dead_letters
        .record(&workers(), 1)
        .await
        .expect("dead letter");
    groups
        .commit(TENANT, NAMESPACE, STREAM, 0, "workers", 2)
        .await
        .expect("commit");
}

/// **A follower never holds a group's cursor past a record without the dead
/// letter for it.** The leader dies after its first group batch lands. Had
/// that been the cursor, the follower, promoted, would neither redeliver
/// offset 1 nor list it as dead.
#[tokio::test]
async fn the_dead_letter_reaches_a_follower_before_the_cursor_that_passed_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, groups, dead_letters) = leader_with_groups(dir.path()).await;
    gave_up_on_one(&groups, &dead_letters).await;
    let router = router(LOCAL, &["broker-b"], GENERATION);
    let follower = GroupFollower {
        crash_after: Some(1),
        ..GroupFollower::default()
    };

    group_pass(&follower, &broker, &router, &mut Default::default()).await;

    assert_eq!(follower.landed_on("broker-b"), vec![("dead letters", 1)]);
}

/// **Nor a cursor written after its dead letters were shipped.** A group gives
/// up on offset 2 and moves past it while the follower is storing the first
/// dead letter, and that dead letter does not reach the follower in this
/// pass. The cursor past it waits for the next pass, which ships the dead
/// letter first.
#[tokio::test]
async fn a_cursor_written_after_the_dead_letters_shipped_waits_for_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, groups, dead_letters) = leader_with_groups(dir.path()).await;
    gave_up_on_one(&groups, &dead_letters).await;
    let router = router(LOCAL, &["broker-b"], GENERATION);
    let follower = GroupFollower::default();
    let (later_groups, later_dead) = (Arc::clone(&groups), Arc::clone(&dead_letters));
    *follower.on_dead_letters.lock().expect("lock") = Some(Box::pin(async move {
        later_dead.record(&workers(), 2).await.expect("dead letter");
        later_groups
            .commit(TENANT, NAMESPACE, STREAM, 0, "workers", 3)
            .await
            .expect("commit");
    }));
    let mut cursors = Default::default();

    group_pass(&follower, &broker, &router, &mut cursors).await;
    assert_eq!(
        follower.landed_on("broker-b"),
        vec![("dead letters", 1), ("cursors", 1)],
        "the second cursor went ahead of its dead letter",
    );

    group_pass(&follower, &broker, &router, &mut cursors).await;
    assert_eq!(
        follower.landed_on("broker-b"),
        vec![
            ("dead letters", 1),
            ("cursors", 1),
            ("dead letters", 1),
            ("cursors", 1)
        ],
    );
}

/// **On a `Quorum` stream a follower short of the group logs is no
/// candidate**, as one short of a `Quorum` cache's counters is not: promoted,
/// it would put the group back before work a majority already holds as done.
#[tokio::test]
async fn a_follower_short_of_the_group_logs_is_not_caught_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, groups, dead_letters) = leader_with_groups(dir.path()).await;
    broker.register_tenant(TENANT).await.expect("tenant");
    broker
        .register_namespace(TENANT, NAMESPACE)
        .await
        .expect("namespace");
    broker
        .register_stream(
            TENANT,
            NAMESPACE,
            STREAM,
            felix_broker::StreamMetadata {
                durable: true,
                shards: 1,
                consistency: felix_broker::ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    gave_up_on_one(&groups, &dead_letters).await;
    let router = router(LOCAL, &["broker-b", "broker-c"], GENERATION);
    let follower = GroupFollower {
        refuses: Some("broker-c"),
        ..GroupFollower::default()
    };

    let pass = group_pass(&follower, &broker, &router, &mut Default::default()).await;

    let report = pass.reports.last().expect("a report");
    assert_eq!(report.caught_up, vec!["broker-b".to_string()]);
}

/// The leader's dead-letter and cursor logs, two records in each.
async fn two_of_each(broker: &Broker) -> (felix_broker::StreamLog, felix_broker::StreamLog) {
    let open = |kind| broker.shard_log(kind, TENANT, NAMESPACE, STREAM, 0);
    let dead = open(felix_broker::LogKind::GroupDeadLetters)
        .await
        .expect("dead letters");
    let cursors = open(felix_broker::LogKind::GroupCursors)
        .await
        .expect("cursors");
    for log in [&dead, &cursors] {
        log.append(&[Bytes::from_static(b"1"), Bytes::from_static(b"2")])
            .await
            .expect("append");
    }
    (dead, cursors)
}

fn at(node_id: &str, confirmed: u64) -> FollowerCursor {
    let mut cursor = FollowerCursor::new(
        node_id.to_string(),
        "10.0.0.1:7002".parse().expect("addr"),
        confirmed,
    );
    cursor.confirmed = confirmed;
    cursor
}

/// **Ranked together, a pair of group logs always leaves a candidate.** b
/// is ahead on dead letters and c on cursors. Each log's "as far as a
/// majority" set alone names a different one of them, and the two together
/// name nobody: a leader dying then has no follower to promote.
#[tokio::test]
async fn followers_ahead_on_different_group_logs_leave_a_candidate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _, _) = leader_with_groups(dir.path()).await;
    let (dead, cursors) = two_of_each(&broker).await;

    let level = group_level(
        [
            (Some(dead), vec![at("broker-b", 2), at("broker-c", 1)]),
            (Some(cursors), vec![at("broker-b", 1), at("broker-c", 2)]),
        ],
        None,
    )
    .await;

    assert_eq!(level, Some(vec!["broker-b".to_string()]));
}

/// **No cursor goes out without the dead-letter log beside it.** A broker
/// with consumer groups always has both, so a dead-letter log missing here
/// is one that failed to open, and what it holds is unknown.
#[tokio::test]
async fn without_the_dead_letter_log_no_cursor_ships() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (broker, _, _) = leader_with_groups(dir.path()).await;
    let (_, cursors) = two_of_each(&broker).await;
    let follower = GroupFollower::default();
    let mut cursor = at("broker-b", 0);

    ship_group_logs(
        &follower,
        &felix_wire::internal::ShardRef {
            tenant_id: TENANT.to_string(),
            namespace: NAMESPACE.to_string(),
            stream: STREAM.to_string(),
            shard: 0,
            generation: GENERATION,
        },
        Some((&cursors, &mut cursor)),
        None,
        &Rebuilds::disabled(),
    )
    .await;

    assert!(follower.landed_on("broker-b").is_empty());
}
