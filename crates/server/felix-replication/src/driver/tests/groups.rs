//! A stream's group and dead-letter logs hold no pass on a follower that is
//! gone.
//!
//! They ride each follower's exchange, so a follower that stops answering is
//! handed off like any other straggler instead of waited on at the end of the
//! pass. The clock is paused, so "well under a second" against a peer that
//! takes a minute is exact.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

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
    })
    .await;
}
