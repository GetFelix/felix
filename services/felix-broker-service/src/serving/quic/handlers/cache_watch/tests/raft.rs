//! Cache watches on a cache shard a Raft group runs on this broker.

use felix_replication::raft::{CacheOp, RaftEngine};

use super::*;
use crate::shards::ShardKey;
use crate::shards::raft::RaftShards;
use crate::shards::raft::tests as fixture;
use crate::shards::routing::EngineShards;

const TIMEOUT: Duration = Duration::from_secs(10);

// A watch has the group append a watched entry, which takes an index and
// changes nothing, so a confirmation's `resume_offset` can land past an edge
// read before the watch. The changes that follow, by the versions their
// writes were acknowledged at, are what pin replay and order.
const ORDERS: &str = "orders";

fn cache_key() -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: ORDERS.to_string(),
        shard: 0,
        kind: crate::shards::ShardKind::Cache,
    }
}

/// A broker whose one cache shard a Raft group of one runs and leads.
struct RaftCache {
    _root: tempfile::TempDir,
    broker: Arc<felix_broker::Broker>,
    shards: Arc<RaftShards>,
    engine: Arc<crate::shards::routing::StreamEngine>,
}

impl RaftCache {
    async fn start() -> Self {
        let cache = Self::open(true).await;
        let key = cache_key();
        fixture::until("the term is served", || {
            cache.shards.leading(&key).is_some()
        })
        .await;
        cache
    }

    /// The shard opened here but never formed, so no leader is known and
    /// the read barrier cannot complete.
    async fn leaderless() -> Self {
        Self::open(false).await
    }

    /// A broker that runs its caches on Raft with no member of the shard
    /// open yet: at startup, or before placement reaches it after a
    /// failover.
    async fn unopened() -> Self {
        Self::opened(None).await
    }

    async fn open(formed: bool) -> Self {
        Self::opened(Some(formed)).await
    }

    async fn opened(formed: Option<bool>) -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let broker = fixture::cache_broker(root.path(), ORDERS).await;
        let shards = fixture::shards(&broker);
        let assignment = fixture::assigned(cache_key(), fixture::INCARNATION);
        match formed {
            Some(true) => fixture::take_formed(&shards, assignment).await,
            Some(false) => {
                shards.take(&mut fixture::assignments([assignment])).await;
            }
            None => {}
        }
        let engine = Arc::new(fixture::engine(&shards));
        Self {
            _root: root,
            broker,
            shards,
            engine,
        }
    }

    fn raft(&self) -> &RaftEngine {
        self.engine.raft(&cache_key()).expect("a member here")
    }

    /// Write `op` and return the entry it applied at.
    async fn write(&self, op: CacheOp) -> u64 {
        use felix_replication::raft::CacheReply;
        match self
            .raft()
            .write_cache(&cache_key(), op, None, TIMEOUT)
            .await
        {
            Ok(CacheReply::Written { version, .. }) => version,
            Ok(CacheReply::Removed { .. }) => self.edge() - 1,
            other => panic!("write: {other:?}"),
        }
    }

    fn put(key: &str, value: &'static [u8]) -> CacheOp {
        CacheOp::Put {
            key: key.to_string(),
            value: bytes::Bytes::from_static(value),
            ttl: None,
            condition: felix_replication::raft::CacheCondition::None,
        }
    }

    /// The group's live edge here.
    fn edge(&self) -> u64 {
        self.raft()
            .cache_watch_point(&cache_key(), &|_| false)
            .expect("a cache group")
            .tail
    }

    /// Open a watch of key `k` and return its confirmation and event stream.
    async fn watch(
        &self,
        retained: bool,
        from_offset: Option<u64>,
    ) -> Result<(Message, Option<Events>)> {
        self.watch_offering(retained, from_offset, 0).await
    }

    /// [`Self::watch`] from a client that offered `peer_features`.
    async fn watch_offering(
        &self,
        retained: bool,
        from_offset: Option<u64>,
        peer_features: u32,
    ) -> Result<(Message, Option<Events>)> {
        open_watch(
            &self.broker,
            &self.engine,
            ORDERS,
            retained,
            from_offset,
            peer_features,
            None,
        )
        .await
    }
}

/// Open a watch of key `k` in `cache` on `broker`, as `owner` when given,
/// and return its confirmation and event stream.
async fn open_watch(
    broker: &Arc<felix_broker::Broker>,
    engine: &Arc<crate::shards::routing::StreamEngine>,
    cache: &str,
    retained: bool,
    from_offset: Option<u64>,
    peer_features: u32,
    owner: Option<&crate::serving::auth::AuthContext>,
) -> Result<(Message, Option<Events>)> {
    let (client, client_conn, connection) = connection().await?;
    let publish_ctx = build_publish_context(
        Arc::clone(broker),
        &BrokerConfig {
            publish_quorum_timeout_ms: 300,
            ..BrokerConfig::default()
        },
        ClusterContext {
            engine: Arc::clone(engine),
            ..ClusterContext::default()
        },
    );
    let (out_ack_tx, mut out_ack_rx) = mpsc::channel(4);
    let out_ack_depth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (ack_throttle_tx, _ack_throttle_rx) = tokio::sync::watch::channel(false);
    let ack_timeout_state = Arc::new(parking_lot::Mutex::new(AckTimeoutState::new(
        std::time::Instant::now(),
    )));
    let (cancel_tx, _cancel_rx) = tokio::sync::watch::channel(false);
    handle_cache_watch_message(
        Arc::clone(broker),
        connection.clone(),
        &publish_ctx,
        WatchResponder {
            out_ack_tx: &out_ack_tx,
            out_ack_depth: &out_ack_depth,
            ack_throttle_tx: &ack_throttle_tx,
            ack_timeout_state: &ack_timeout_state,
            cancel_tx: &cancel_tx,
        },
        WatchRequest {
            tenant_id: "t1".to_string(),
            namespace: "ns".to_string(),
            cache: cache.to_string(),
            key: Some("k".to_string()),
            prefix: None,
            shard: None,
            from_offset,
            retained,
            subscription_id: Some(7),
        },
        peer_features,
        owner,
    )
    .await?;
    let Outgoing::Message(answer) = out_ack_rx.recv().await.context("ack missing")? else {
        panic!("expected a control message");
    };
    if !matches!(answer, Message::CacheWatchStarted { .. }) {
        return Ok((answer, None));
    }
    let stream = tokio::time::timeout(Duration::from_secs(2), client_conn.accept_uni())
        .await
        .context("accept uni timeout")??;
    let mut events = Events {
        _held: (client, client_conn, connection),
        stream,
        scratch: crate::serving::quic::FrameScratch::new(),
    };
    let hello = events.next().await?;
    assert!(
        matches!(hello, Message::EventStreamHello { .. }),
        "{hello:?}"
    );
    Ok((answer, Some(events)))
}

/// A watch's event stream, with the connection it runs on held open.
struct Events {
    _held: (
        QuicClient,
        felix_transport::QuicConnection,
        felix_transport::QuicConnection,
    ),
    stream: quinn::RecvStream,
    scratch: crate::serving::quic::FrameScratch,
}

impl Events {
    async fn next(&mut self) -> Result<Message> {
        tokio::time::timeout(
            Duration::from_secs(5),
            crate::serving::quic::codec::read_message_limited(
                &mut self.stream,
                64 * 1024,
                &mut self.scratch,
            ),
        )
        .await
        .context("no event")??
        .context("the event stream ended")
    }

    /// Wait for the stream to finish with nothing more on it.
    async fn ended(&mut self) {
        let end = tokio::time::timeout(
            Duration::from_secs(5),
            crate::serving::quic::codec::read_message_limited(
                &mut self.stream,
                64 * 1024,
                &mut self.scratch,
            ),
        )
        .await
        .expect("the event stream did not end");
        assert!(matches!(end, Ok(None)), "{end:?}");
    }

    /// The next change, as key, value and offset.
    async fn change(&mut self) -> Result<(Option<Vec<u8>>, u64)> {
        match self.next().await? {
            Message::CacheEvent { value, offset, .. } => {
                Ok((value.map(|value| value.to_vec()), offset))
            }
            other => panic!("expected a change, got {other:?}"),
        }
    }
}

/// **A Raft cache shard serves `cache_watch`** as the hybrid's store does:
/// a retained watch is handed the key's value at the put that wrote it,
/// then each later put and delete at its own entry, in order.
#[tokio::test]
async fn a_raft_cache_shard_serves_a_watch() -> Result<()> {
    let cache = RaftCache::start().await;
    let first = cache.write(RaftCache::put("k", b"one")).await;
    cache.write(RaftCache::put("other", b"x")).await;

    let (started, events) = cache.watch(true, None).await?;
    let Message::CacheWatchStarted {
        resume_offset,
        retained_count,
        resnapshot,
        ..
    } = started
    else {
        panic!("the watch was refused: {started:?}");
    };
    assert_eq!(retained_count, Some(1));
    assert!(!resnapshot);
    assert_eq!(resume_offset, cache.edge());
    let mut events = events.expect("events");
    assert_eq!(events.change().await?, (Some(b"one".to_vec()), first));

    let second = cache.write(RaftCache::put("k", b"two")).await;
    cache.write(RaftCache::put("other", b"y")).await;
    let gone = cache
        .write(CacheOp::Delete {
            key: "k".to_string(),
            version: None,
        })
        .await;
    assert_eq!(events.change().await?, (Some(b"two".to_vec()), second));
    assert_eq!(events.change().await?, (None, gone));
    assert!(resume_offset <= second);
    Ok(())
}

/// **A watch ends when the credential it was opened with is revoked**, and
/// says so: an `error` with the `revoked` code is its last frame.
#[tokio::test]
async fn a_raft_cache_watch_ends_when_its_credential_is_revoked() -> Result<()> {
    let cache = RaftCache::start().await;
    let revocations = Arc::new(crate::serving::revocation::Revocations::default());
    let owner = crate::serving::auth::AuthContext {
        tenant_id: "t1".to_string(),
        matcher: felix_authz::PermissionMatcher::from_strings(&[]).expect("matcher"),
        token: "token".to_string(),
        subject: "alice".to_string(),
        actor: None,
        publisher: None,
        issued_at: 0,
        expires_at: i64::MAX,
        lease_token: false,
        revocable: crate::serving::revocation::Revocable::new(
            Arc::clone(&revocations),
            "t1".to_string(),
            "alice".to_string(),
            None,
            100,
        ),
        client_chain: None,
    };
    let (started, events) = open_watch(
        &cache.broker,
        &cache.engine,
        ORDERS,
        false,
        None,
        felix_wire::FEATURE_ERROR_CODES,
        Some(&owner),
    )
    .await?;
    assert!(
        matches!(started, Message::CacheWatchStarted { .. }),
        "{started:?}"
    );
    let mut events = events.expect("events");

    revocations.revoke("t1", "alice", 100);
    let last = loop {
        let message = events.next().await?;
        if let Message::Error { .. } = message {
            break message;
        }
    };
    let Message::Error { code, .. } = last else {
        unreachable!()
    };
    assert_eq!(code, Some(felix_wire::ErrorCode::Revoked));
    Ok(())
}

/// **A resume joins at the applied edge.** From the edge it is handed
/// nothing until the next change; from below where the member began keeping
/// changes, every change from there on, replayed from the Raft log as the
/// hybrid replays its shard log; from past an edge this member never
/// reaches, a retryable refusal.
#[tokio::test]
async fn a_raft_cache_watch_resumes_by_offset() -> Result<()> {
    let cache = RaftCache::start().await;
    let one = cache.write(RaftCache::put("k", b"one")).await;
    let now = cache.write(RaftCache::put("k", b"two")).await;
    let edge = cache.edge();

    let (started, events) = cache.watch(false, Some(edge)).await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted { resume_offset, resnapshot: false, .. } if resume_offset >= edge
        ),
        "{started:?}"
    );
    let mut events = events.expect("events");
    let next = cache.write(RaftCache::put("k", b"three")).await;
    assert_eq!(events.change().await?, (Some(b"three".to_vec()), next));

    let (started, events) = cache.watch(false, Some(one)).await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted {
                resnapshot: false,
                ..
            }
        ),
        "{started:?}"
    );
    let mut events = events.expect("events");
    assert_eq!(events.change().await?, (Some(b"one".to_vec()), one));
    assert_eq!(events.change().await?, (Some(b"two".to_vec()), now));
    assert_eq!(events.change().await?, (Some(b"three".to_vec()), next));

    let (refused, _) = cache.watch(false, Some(cache.edge() + 5)).await?;
    assert!(
        matches!(
            refused,
            Message::Error {
                code: Some(felix_wire::ErrorCode::ShardUnavailable),
                retry: Some(felix_wire::RetryClass::Retry),
                ..
            }
        ),
        "{refused:?}"
    );
    Ok(())
}

/// **A join that hands out state passes the read barrier**: a follower's
/// applied edge can trail a put its leader acknowledged. With the barrier
/// unable to complete, a retained join is a retryable refusal, not a stale
/// set. A live-only watch and a resume replayed from the log hand out no
/// state, only applied changes, and still start.
#[tokio::test]
async fn a_raft_cache_join_with_state_needs_the_read_barrier() -> Result<()> {
    let cache = RaftCache::leaderless().await;
    let (refused, _) = cache.watch(true, None).await?;
    assert!(
        matches!(
            refused,
            Message::Error {
                code: Some(felix_wire::ErrorCode::ShardUnavailable),
                retry: Some(felix_wire::RetryClass::Retry),
                ..
            }
        ),
        "{refused:?}"
    );
    let (started, _events) = cache.watch(false, None).await?;
    assert!(
        matches!(started, Message::CacheWatchStarted { .. }),
        "{started:?}"
    );
    let (started, _events) = cache.watch(false, Some(0)).await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted {
                resnapshot: false,
                ..
            }
        ),
        "{started:?}"
    );
    Ok(())
}

/// **A resume on a member behind the one that named the offset waits for
/// it to apply that far**, then joins there: a lagged client told to resume
/// at the next change by one member can re-watch on a slower one.
#[tokio::test]
async fn a_raft_cache_watch_resume_past_the_edge_waits_for_it() -> Result<()> {
    let cache = Arc::new(RaftCache::start().await);
    let from = cache.edge() + 1;
    let watching = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move { cache.watch(false, Some(from)).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let below = cache.write(RaftCache::put("k", b"one")).await;
    let at = cache.write(RaftCache::put("k", b"two")).await;
    let (started, events) = tokio::time::timeout(TIMEOUT, watching).await???;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted { resume_offset, resnapshot: false, .. } if resume_offset >= from
        ),
        "{started:?}"
    );
    // The first change at or past `from`, whichever write made it.
    let first = if below >= from {
        (Some(b"one".to_vec()), below)
    } else {
        (Some(b"two".to_vec()), at)
    };
    assert_eq!(events.expect("events").change().await?, first);
    Ok(())
}

/// **Closing a Raft cache member ends its watches**: left in the hub, a
/// watch would go quiet, which reads as a quiet key.
#[tokio::test]
async fn closing_a_raft_cache_member_ends_its_watches() -> Result<()> {
    let cache = RaftCache::start().await;
    let (_, events) = cache.watch(false, None).await?;
    let mut events = events.expect("events");
    cache.shards.take(&mut fixture::assignments([])).await;
    events.ended().await;
    let hub = cache.broker.cache_watches().expect("a hub");
    assert_eq!(hub.registered_watchers("t1", "ns", ORDERS, 0), 0);
    Ok(())
}

/// **A cache made again ends the old one's watches.** The hub keys watches
/// by shard alone, so a watch left from the old incarnation would see the
/// new group's low indexes and drop them as already delivered.
#[tokio::test]
async fn a_remade_raft_cache_ends_the_old_watches() -> Result<()> {
    let cache = RaftCache::start().await;
    for _ in 0..3 {
        cache.write(RaftCache::put("k", b"old")).await;
    }
    let (_, events) = cache.watch(false, Some(cache.edge())).await?;
    let mut events = events.expect("events");
    let remade = fixture::assigned(cache_key(), fixture::INCARNATION + 1);
    cache.shards.take(&mut fixture::assignments([remade])).await;
    events.ended().await;
    let hub = cache.broker.cache_watches().expect("a hub");
    assert_eq!(hub.registered_watchers("t1", "ns", ORDERS, 0), 0);
    Ok(())
}

/// **A feed pass that finds a cache member with no leader ends its watches
/// as lagged**, at the first change they were not told of.
#[tokio::test]
async fn a_leaderless_raft_cache_member_ends_its_watches() {
    let root = tempfile::tempdir().expect("tempdir");
    let broker = fixture::cache_broker(root.path(), ORDERS).await;
    let shards = fixture::shards(&broker);
    let key = cache_key();
    let assignment = fixture::assigned(key.clone(), fixture::INCARNATION);
    // Opened but never formed, so it follows no leader.
    shards
        .take(&mut fixture::assignments([assignment.clone()]))
        .await;
    let engine = fixture::engine(&shards);
    let raft = engine.raft(&key).expect("a member here");
    let edge = raft
        .cache_watch_point(&key, &|_| false)
        .expect("a cache group")
        .tail;
    let hub = Arc::clone(broker.cache_watches().expect("a hub"));
    let mut watch = hub.register(
        "t1",
        "ns",
        ORDERS,
        0,
        CacheWatchFilter::Key("k".to_string()),
        8,
    );
    shards.take(&mut fixture::assignments([assignment])).await;
    let end = tokio::time::timeout(Duration::from_secs(5), watch.recv())
        .await
        .expect("the watch was not ended");
    assert!(end.is_none());
    assert_eq!(watch.lagged(), Some(edge));
    EngineShards::shutdown(&*shards).await;
}

/// **A client that follows moves is told the shard moved, not that it
/// lagged**, when its member can no longer say its state is current. A lag
/// ends a cluster client's watch, and a member losing its leader is a
/// failover the watch has to carry on through, at the same edge.
#[tokio::test]
async fn an_ended_raft_cache_watch_is_followed_by_a_client_that_follows_moves() -> Result<()> {
    let cache = RaftCache::start().await;
    let (_, events) = cache
        .watch_offering(false, None, felix_wire::FEATURE_SHARD_MOVED)
        .await?;
    let mut events = events.expect("events");
    let put = cache.write(RaftCache::put("k", b"one")).await;
    assert_eq!(events.change().await?, (Some(b"one".to_vec()), put));

    let hub = Arc::clone(cache.broker.cache_watches().expect("a hub"));
    let edge = cache.edge();
    cache.raft().end_cache_watches(&cache_key(), hub.as_ref());
    let ended = events.next().await?;
    assert!(
        matches!(
            ended,
            Message::ShardMoved { subscription_id: 7, resume_from: Some(from), node_id: None, .. }
                if from == edge
        ),
        "{ended:?}"
    );
    Ok(())
}

/// **A watcher whose queue fills on a Raft shard ends as lagged at the
/// first change it could not hold**, as on the hybrid: the changes come
/// from the group's apply, and the hub bounds them the same way.
#[tokio::test]
async fn a_full_raft_cache_watch_ends_as_lagged() {
    let cache = RaftCache::start().await;
    let hub = Arc::clone(cache.broker.cache_watches().expect("a hub"));
    let mut watch = hub.register(
        "t1",
        "ns",
        ORDERS,
        0,
        CacheWatchFilter::Key("k".to_string()),
        1,
    );
    let first = cache.write(RaftCache::put("k", b"one")).await;
    let missed = cache.write(RaftCache::put("k", b"two")).await;
    cache.write(RaftCache::put("k", b"three")).await;
    let mut recv = async || {
        tokio::time::timeout(Duration::from_secs(5), watch.recv())
            .await
            .expect("no change reached the watcher")
    };
    assert_eq!(recv().await.map(|event| event.offset), Some(first));
    assert!(recv().await.is_none(), "the queue held more than one");
    assert_eq!(watch.lagged(), Some(missed));
}

/// **A resume from a change the member still keeps is replayed**: every
/// change to the key from the resume offset on, deletes included, then the
/// live ones, with no resnapshot. The member keeps changes once a watch of
/// the shard took one, so a client that drops and comes back is replayed
/// what it missed rather than handed the key again.
#[tokio::test]
async fn a_raft_cache_resume_is_replayed_from_the_kept_changes() -> Result<()> {
    let cache = RaftCache::start().await;
    // Before any watch, so below every change the member keeps.
    let zero = cache.write(RaftCache::put("k", b"zero")).await;
    let (_, events) = cache.watch(false, None).await?;
    let mut events = events.expect("events");
    let one = cache.write(RaftCache::put("k", b"one")).await;
    cache.write(RaftCache::put("other", b"x")).await;
    let two = cache.write(RaftCache::put("k", b"two")).await;
    assert_eq!(events.change().await?, (Some(b"one".to_vec()), one));
    assert_eq!(events.change().await?, (Some(b"two".to_vec()), two));
    drop(events);

    let gone = cache
        .write(CacheOp::Delete {
            key: "k".to_string(),
            version: None,
        })
        .await;
    let three = cache.write(RaftCache::put("k", b"three")).await;
    let edge = cache.edge();
    let (started, events) = cache.watch(false, Some(two)).await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted { resume_offset, resnapshot: false, .. } if resume_offset >= edge
        ),
        "{started:?}"
    );
    let mut events = events.expect("events");
    assert_eq!(events.change().await?, (Some(b"two".to_vec()), two));
    assert_eq!(events.change().await?, (None, gone));
    assert_eq!(events.change().await?, (Some(b"three".to_vec()), three));
    let four = cache.write(RaftCache::put("k", b"four")).await;
    assert_eq!(events.change().await?, (Some(b"four".to_vec()), four));

    // From before the first kept change, replayed from the Raft log.
    assert!(zero < one);
    let (started, events) = cache.watch(false, Some(zero)).await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted {
                resnapshot: false,
                ..
            }
        ),
        "{started:?}"
    );
    let mut events = events.expect("events");
    for (value, at) in [
        (Some(&b"zero"[..]), zero),
        (Some(b"one"), one),
        (Some(b"two"), two),
        (None, gone),
        (Some(b"three"), three),
        (Some(b"four"), four),
    ] {
        assert_eq!(events.change().await?, (value.map(<[u8]>::to_vec), at));
    }
    Ok(())
}

/// **A Raft broker with no member of the shard open refuses a watch
/// retryably**, a fresh one and a resume alike, rather than serving it from
/// the hybrid store, which nothing writes on Raft: there the watch would
/// join an empty log, a retained one would be told the key holds nothing,
/// and a resume handed out after a failover would be refused as in the
/// future. Nothing stays registered.
#[tokio::test]
async fn a_watch_on_a_raft_broker_with_no_member_open_is_refused_retryably() -> Result<()> {
    let cache = RaftCache::unopened().await;
    assert!(cache.engine.runs_raft());
    assert!(cache.engine.raft(&cache_key()).is_none());
    for (retained, from_offset) in [(false, None), (true, None), (false, Some(5))] {
        let (refused, events) = cache.watch(retained, from_offset).await?;
        assert!(
            events.is_none(),
            "retained {retained}, from {from_offset:?}"
        );
        match &refused {
            Message::Error {
                code: Some(felix_wire::ErrorCode::ShardUnavailable),
                retry: Some(felix_wire::RetryClass::Retry),
                message,
                ..
            } => assert!(message.contains("not open on this broker"), "{message}"),
            other => panic!("retained {retained}, from {from_offset:?}: {other:?}"),
        }
    }
    let hub = cache.broker.cache_watches().expect("a hub");
    assert_eq!(hub.registered_watchers("t1", "ns", ORDERS, 0), 0);
    Ok(())
}

/// **A watch registers before it reads the member's edge**, so a write
/// applied while it opens is either below the edge it is told or delivered
/// live. Read the other way round, a write landing in between is in
/// neither, and the first change delivered skips it. Writers run
/// throughout; every watch's first change must be the first write at or
/// past its `resume_offset`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_raft_cache_watch_misses_no_write_that_lands_while_it_opens() -> Result<()> {
    const WRITERS: usize = 4;
    let cache = Arc::new(RaftCache::start().await);
    // Each writer's acknowledged versions, in the order it wrote them.
    let written: Arc<Vec<parking_lot::Mutex<Vec<u64>>>> = Arc::new(
        (0..WRITERS)
            .map(|_| parking_lot::Mutex::default())
            .collect(),
    );
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = tokio::task::JoinSet::new();
    for writer in 0..WRITERS {
        let (cache, written, stop) = (Arc::clone(&cache), Arc::clone(&written), Arc::clone(&stop));
        writers.spawn(async move {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let version = cache.write(RaftCache::put("k", b"v")).await;
                written[writer].lock().push(version);
            }
        });
    }
    for round in 0..300 {
        let (started, events) = cache.watch(false, None).await?;
        let Message::CacheWatchStarted { resume_offset, .. } = started else {
            panic!("round {round}: {started:?}");
        };
        let (_, first) = events.expect("events").change().await?;
        // A writer that has recorded a version at or past `first` has
        // recorded every one of its writes below it.
        fixture::until("every writer to pass the first change", || {
            written
                .iter()
                .all(|versions| versions.lock().last().is_some_and(|last| *last >= first))
        })
        .await;
        let skipped: Vec<u64> = written
            .iter()
            .flat_map(|versions| versions.lock().clone())
            .filter(|version| (resume_offset..first).contains(version))
            .collect();
        assert!(
            skipped.is_empty(),
            "round {round}: joined at {resume_offset}, first change {first}, missed {skipped:?}"
        );
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    while let Some(writer) = writers.join_next().await {
        writer?;
    }
    Ok(())
}

/// **A replay from the log that outlasts the live queue is resnapshotted,
/// not handed out.** Ended as lagged, the watch would have the client
/// resume below the recorded changes and run the same replay again, a loop
/// a slow replay never leaves. Here the replay waits at the bound while
/// more changes than the queue holds arrive.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replay_that_outlasts_the_live_queue_is_resnapshotted() -> Result<()> {
    let cache = RaftCache::start().await;
    let one = cache.write(RaftCache::put("k", b"one")).await;
    let held = cache
        .raft()
        .hold_cache_log_replays(&cache_key())
        .await
        .expect("a cache group");
    let hub = Arc::clone(cache.broker.cache_watches().expect("a hub"));
    let writes = async {
        fixture::until("the watch registered", || {
            hub.registered_watchers("t1", "ns", ORDERS, 0) > 0
        })
        .await;
        let mut last = 0;
        for _ in 0..=WATCH_QUEUE_CAPACITY {
            last = cache.write(RaftCache::put("k", b"more")).await;
        }
        drop(held);
        last
    };
    let (watched, last) = tokio::join!(cache.watch(false, Some(one)), writes);
    let (started, events) = watched?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted {
                resnapshot: true,
                ..
            }
        ),
        "{started:?}"
    );
    let (value, at) = events.expect("events").change().await?;
    assert_eq!(value, Some(b"more".to_vec()));
    assert!(at <= last, "{at} past {last}");
    Ok(())
}

/// **A resume the snapshot covers is resnapshotted, and passes the read
/// barrier first**, as a retained join does: a follower's applied edge can
/// trail a put its leader acknowledged. A follower that took a snapshot
/// past the resume offset answers it with `resnapshot`, and once it cannot
/// reach its leader, with a retryable refusal rather than a stale snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resnapshotted_raft_resume_needs_the_read_barrier() -> Result<()> {
    use crate::shards::raft::tests::{BROKERS, Loopback, Member};
    let net = Arc::new(Loopback::default());
    let sessions = ShardKey {
        stream: "sessions".to_string(),
        ..cache_key()
    };
    let mut placed = fixture::assigned(sessions.clone(), fixture::INCARNATION);
    placed.replicas = BROKERS.iter().map(|name| name.to_string()).collect();
    let mut members = Vec::new();
    for name in BROKERS {
        members.push(Member::start_cache(&net, name).await);
    }
    let all: Vec<_> = members.iter().map(|member| &member.shards).collect();
    fixture::form_on(&all, placed).await;
    let mut leader = 0;
    fixture::until("a member serves its term", || {
        members
            .iter()
            .position(|member| member.shards.leading(&sessions).is_some())
            .map(|at| leader = at)
            .is_some()
    })
    .await;
    let raft = |member: &Member| {
        member
            .engine
            .raft(&sessions)
            .expect("a member here")
            .clone()
    };
    let write = |value: &'static [u8]| {
        let raft = raft(&members[leader]);
        let sessions = sessions.clone();
        async move {
            match raft
                .write_cache(&sessions, RaftCache::put("k", value), None, TIMEOUT)
                .await
            {
                Ok(felix_replication::raft::CacheReply::Written { version, .. }) => version,
                other => panic!("write: {other:?}"),
            }
        }
    };
    let one = write(b"one").await;
    let two = write(b"two").await;
    let follower = &members[(leader + 1) % members.len()];
    let edge = || {
        raft(follower)
            .cache_watch_point(&sessions, &|_| false)
            .expect("a cache group")
            .tail
    };
    fixture::until("the follower applied the writes", || edge() > two).await;
    let covered = raft(follower)
        .snapshot_applied(&sessions)
        .await
        .expect("a snapshot");
    assert!(covered >= two);

    let (started, events) = open_watch(
        &follower.broker,
        &follower.engine,
        "sessions",
        false,
        Some(one),
        0,
        None,
    )
    .await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted {
                resnapshot: true,
                ..
            }
        ),
        "{started:?}"
    );
    assert_eq!(
        events.expect("events").change().await?,
        (Some(b"two".to_vec()), two)
    );

    crate::shards::raft::tests::isolate(&net, follower.name);
    let (refused, _) = open_watch(
        &follower.broker,
        &follower.engine,
        "sessions",
        false,
        Some(one),
        0,
        None,
    )
    .await?;
    assert!(
        matches!(
            refused,
            Message::Error {
                code: Some(felix_wire::ErrorCode::ShardUnavailable),
                retry: Some(felix_wire::RetryClass::Retry),
                ..
            }
        ),
        "{refused:?}"
    );
    crate::shards::raft::tests::stop(&members).await;
    Ok(())
}
