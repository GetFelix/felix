//! Cache watches on a cache shard a Raft group runs on this broker.

use felix_replication::raft::{CacheOp, RaftEngine};

use super::*;
use crate::shards::ShardKey;
use crate::shards::raft::RaftShards;
use crate::shards::raft::tests as fixture;

const TIMEOUT: Duration = Duration::from_secs(10);
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
    _shards: Arc<RaftShards>,
    engine: Arc<crate::shards::routing::StreamEngine>,
}

impl RaftCache {
    async fn start() -> Self {
        let root = tempfile::tempdir().expect("tempdir");
        let broker = fixture::cache_broker(root.path(), ORDERS).await;
        let shards = fixture::shards(&broker);
        let key = cache_key();
        fixture::take_formed(
            &shards,
            fixture::assigned(key.clone(), fixture::INCARNATION),
        )
        .await;
        fixture::until("the term is served", || shards.leading(&key).is_some()).await;
        let engine = Arc::new(fixture::engine(&shards));
        Self {
            _root: root,
            broker,
            _shards: shards,
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
        let (client, client_conn, connection) = connection().await?;
        let publish_ctx = build_publish_context(
            Arc::clone(&self.broker),
            &BrokerConfig::default(),
            ClusterContext {
                engine: Arc::clone(&self.engine),
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
            Arc::clone(&self.broker),
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
                cache: ORDERS.to_string(),
                key: Some("k".to_string()),
                prefix: None,
                shard: None,
                from_offset,
                retained,
                subscription_id: Some(7),
            },
            0,
            None,
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

/// **A resume joins at the applied edge.** From the edge it is handed
/// nothing until the next change; from below it, the key's current value
/// with `resnapshot`, since a Raft shard keeps no change history to replay;
/// from past it, the cursor error.
#[tokio::test]
async fn a_raft_cache_watch_resumes_by_offset() -> Result<()> {
    let cache = RaftCache::start().await;
    cache.write(RaftCache::put("k", b"one")).await;
    let now = cache.write(RaftCache::put("k", b"two")).await;
    let edge = cache.edge();

    let (started, events) = cache.watch(false, Some(edge)).await?;
    assert!(
        matches!(
            started,
            Message::CacheWatchStarted { resume_offset, resnapshot: false, .. } if resume_offset == edge
        ),
        "{started:?}"
    );
    let mut events = events.expect("events");
    let next = cache.write(RaftCache::put("k", b"three")).await;
    assert_eq!(events.change().await?, (Some(b"three".to_vec()), next));

    let (started, events) = cache.watch(false, Some(1)).await?;
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
        (Some(b"three".to_vec()), next)
    );
    assert!(now < next);

    let (refused, _) = cache.watch(false, Some(cache.edge() + 5)).await?;
    assert!(
        matches!(refused, Message::SubscribeCursorError { .. }),
        "{refused:?}"
    );
    Ok(())
}

/// **A member that can no longer say its state is current ends its watches
/// loudly**, at the first change they were not told of, so the client
/// re-watches from there. What was queued is delivered first.
#[tokio::test]
async fn an_ended_raft_cache_watch_names_where_to_resume() -> Result<()> {
    let cache = RaftCache::start().await;
    let (_, events) = cache.watch(false, None).await?;
    let mut events = events.expect("events");
    let put = cache.write(RaftCache::put("k", b"one")).await;
    assert_eq!(events.change().await?, (Some(b"one".to_vec()), put));

    let hub = Arc::clone(cache.broker.cache_watches().expect("a hub"));
    let edge = cache.edge();
    cache.raft().end_cache_watches(&cache_key(), hub.as_ref());
    let ended = events.next().await?;
    assert!(
        matches!(ended, Message::CacheWatchLagged { resume_from } if resume_from == edge),
        "{ended:?}"
    );
    assert_eq!(hub.registered_watchers("t1", "ns", ORDERS, 0), 0);
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
