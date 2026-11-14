//! Raft over real peer listeners and pools on loopback.

use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use felix_storage::DiskLog;
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::{InternalMessage, PeerCapabilities};
use openraft::raft::VoteRequest;
use openraft::type_config::alias::VoteOf;
use openraft::vote::{RaftLeaderId, RaftVote};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::*;
use crate::peer::{PeerError, PeerPool, PeerServer, PeerTransportConfig, UnavailableHandler};
use crate::raft::network::Request;
use crate::raft::types::ShardTypes;
use crate::raft::{GroupId, MemberId, Role, RuntimeConfig};
use crate::{ShardKey, ShardKind};

const BROKERS: [&str; 3] = ["a", "b", "c"];
const PATIENCE: Duration = Duration::from_secs(120);

type Book = Arc<parking_lot::Mutex<HashMap<String, SocketAddr>>>;

fn config() -> PeerTransportConfig {
    PeerTransportConfig {
        bind: "127.0.0.1:0".parse().expect("addr"),
        ..Default::default()
    }
}

fn book(book: &Book) -> Arc<dyn PeerAddresses> {
    let book = Arc::clone(book);
    Arc::new(move |broker: &str| book.lock().get(broker).copied())
}

fn shard() -> ShardKey {
    ShardKey {
        tenant_id: "t".into(),
        namespace: "ns".into(),
        stream: "orders".into(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn vote_for(group: GroupId, to: MemberId) -> RaftRequest {
    let leader = openraft::impls::leader_id_std::LeaderId::new(1, MemberId::new("a", 1));
    RaftRequest {
        group,
        to,
        beat: 0,
        deadline: std::time::Duration::from_secs(1),
        body: Request::Vote(VoteRequest::new(
            VoteOf::<ShardTypes>::from_leader_id(leader, false),
            None,
        )),
    }
}

/// One broker: a runtime whose transport is a peer pool, answering Raft on
/// its own listener.
struct Broker {
    runtime: GroupRuntime,
    pool: Arc<PeerPool>,
    stop: CancellationToken,
}

impl Broker {
    async fn start(name: &str, book: &Book, raft: bool) -> Self {
        Self::start_with(name, book, raft, RuntimeConfig::default()).await
    }

    async fn start_with(name: &str, book: &Book, raft: bool, timers: RuntimeConfig) -> Self {
        let stop = CancellationToken::new();
        let pool = PeerPool::new(name.into(), config(), stop.clone()).expect("pool");
        let transport = Arc::new(PeerTransport::new(Arc::clone(&pool), self::book(book)));
        let runtime = GroupRuntime::start(name, timers, transport).expect("timers");
        let mut server =
            PeerServer::bind(name.into(), &config(), Arc::new(UnavailableHandler)).expect("bind");
        if raft {
            server = server.with_raft(Arc::new(runtime.clone()));
        }
        book.lock()
            .insert(name.into(), server.local_addr().expect("addr"));
        tokio::spawn(server.serve(stop.clone()));
        Self {
            runtime,
            pool,
            stop,
        }
    }

    async fn stop(self) {
        self.runtime.shutdown().await;
        self.pool.shutdown().await;
        self.stop.cancel();
    }
}

fn open_log(root: &TempDir, broker: &str) -> DiskLog {
    let dir = root.path().join(broker);
    std::fs::create_dir_all(&dir).expect("dir");
    let config = LogConfig {
        fsync_mode: FsyncMode::OnCommit,
        preallocate_segments: false,
        raft_log: true,
        ..LogConfig::default()
    };
    DiskLog::open(dir, "t/ns/orders/0", config).expect("open")
}

/// Votes and appends, entries included, cross real listeners: three brokers
/// elect a leader and every member learns the first entries committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_forms_over_the_peer_transport() {
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let mut brokers = Vec::new();
    for name in BROKERS {
        brokers.push(Broker::start(name, &book, true).await);
    }
    let members: BTreeSet<MemberId> = BROKERS.map(|b| MemberId::new(b, 1)).into();
    for (name, broker) in BROKERS.iter().zip(&brokers) {
        let group = GroupId::new(shard(), 0);
        broker
            .runtime
            .open(group, 1, open_log(&root, name))
            .await
            .expect("open");
    }
    for broker in &brokers {
        broker
            .runtime
            .initialize(&shard(), members.clone())
            .await
            .expect("initialize");
    }

    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let statuses: Vec<_> = brokers
            .iter()
            .map(|broker| broker.runtime.status(&shard()).expect("open"))
            .collect();
        let leader = statuses.iter().find(|s| s.role == Role::Leader);
        let settled = leader.is_some_and(|leader| {
            statuses.iter().all(|s| {
                s.leader.as_ref() == Some(&leader.member)
                    && s.committed.is_some()
                    && s.committed == leader.last_index
            })
        });
        if settled {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the group did not form: {statuses:#?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for broker in brokers {
        broker.stop().await;
    }
}

/// A refusal travels back as itself: a member the peer does not hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_comes_back_from_the_peer() {
    let book = Book::default();
    let a = Broker::start("a", &book, true).await;
    let b = Broker::start("b", &book, true).await;
    let transport = PeerTransport::new(Arc::clone(&a.pool), self::book(&book));
    let refused = transport
        .send(vote_for(GroupId::new(shard(), 0), MemberId::new("b", 1)))
        .await
        .err()
        .expect("refused");
    assert!(matches!(refused, Refused::NoSuchMember(_)), "{refused}");
    a.stop().await;
    b.stop().await;
}

/// A broker whose listener does not run Raft does not offer it, and the
/// transport says so without sending.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_without_raft_is_unreachable() {
    let book = Book::default();
    let a = Broker::start("a", &book, true).await;
    let b = Broker::start("b", &book, false).await;
    let transport = PeerTransport::new(Arc::clone(&a.pool), self::book(&book));
    let refused = transport
        .send(vote_for(GroupId::new(shard(), 0), MemberId::new("b", 1)))
        .await
        .err()
        .expect("refused");
    assert!(
        matches!(&refused, Refused::Unreachable(why) if why.contains("not offered Raft")),
        "{refused}"
    );
    a.stop().await;
    b.stop().await;
}

/// A requester that counts what it is asked to send and says its peer
/// offers `offered`.
struct Counting {
    offered: PeerCapabilities,
    sent: AtomicUsize,
}

impl crate::peer::PeerRequester for Counting {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        Err(PeerError::Disconnected {
            node_id: node_id.into(),
        })
    }

    async fn capabilities(
        &self,
        _node_id: &str,
        _addr: SocketAddr,
    ) -> Result<PeerCapabilities, PeerError> {
        Ok(self.offered)
    }
}

#[tokio::test]
async fn a_peer_that_has_not_offered_raft_is_never_sent_one() {
    let book = Book::default();
    book.lock()
        .insert("b".into(), "127.0.0.1:1".parse().expect("addr"));
    let requester = Arc::new(Counting {
        offered: PeerCapabilities::PING,
        sent: AtomicUsize::new(0),
    });
    let transport = PeerTransport::new(Arc::clone(&requester), self::book(&book));
    let refused = transport
        .send(vote_for(GroupId::new(shard(), 0), MemberId::new("b", 1)))
        .await
        .err()
        .expect("refused");
    assert!(matches!(refused, Refused::Unreachable(_)), "{refused}");
    assert_eq!(requester.sent.load(Ordering::SeqCst), 0);

    // The same peer once it offers Raft is sent the message.
    let requester = Arc::new(Counting {
        offered: PeerCapabilities::RAFT,
        sent: AtomicUsize::new(0),
    });
    let transport = PeerTransport::new(Arc::clone(&requester), self::book(&book));
    let _ = transport
        .send(vote_for(GroupId::new(shard(), 0), MemberId::new("b", 1)))
        .await;
    assert_eq!(requester.sent.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_broker_with_no_address_is_unreachable() {
    let requester = Arc::new(Counting {
        offered: PeerCapabilities::RAFT,
        sent: AtomicUsize::new(0),
    });
    let transport = PeerTransport::new(Arc::clone(&requester), book(&Book::default()));
    let refused = transport
        .send(vote_for(GroupId::new(shard(), 0), MemberId::new("b", 1)))
        .await
        .err()
        .expect("refused");
    assert!(matches!(refused, Refused::Unreachable(_)), "{refused}");
    assert_eq!(requester.sent.load(Ordering::SeqCst), 0);
}

/// A body this build cannot read is answered, not dropped: the sender learns
/// the member was not reached.
#[tokio::test]
async fn an_unreadable_message_is_answered_unreachable() {
    let runtime = GroupRuntime::start(
        "b",
        RuntimeConfig::default(),
        Arc::new(crate::raft::AgreeingPeers),
    )
    .expect("timers");
    let answer =
        crate::peer::RaftHandler::answer(&runtime, bytes::Bytes::from_static(b"\x09\x03")).await;
    let refused = crate::raft::wire::decode_reply(answer)
        .expect("an answer")
        .err()
        .expect("refused");
    assert!(matches!(refused, Refused::Unreachable(_)), "{refused}");
    runtime.shutdown().await;
}

/// A member that joins after its leader cut the log to a snapshot is sent
/// the snapshot over real listeners. It is past one piece's budget, so it
/// crosses as more than one `Request::Snapshot`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_crosses_the_peer_transport_in_pieces() {
    use openraft::async_runtime::watch::WatchReceiver;

    use crate::raft::cache::{CacheCommand, Condition, Request as Asked};
    use crate::raft::types::Command;

    const VALUE_BYTES: usize = 1 << 20;
    const WRITES: u64 = 48;
    let shard = ShardKey {
        kind: ShardKind::Cache,
        shard: 1,
        ..shard()
    };
    let timers = RuntimeConfig {
        cache_snapshot_every: 16,
        cache_log_keep: 1,
        ..RuntimeConfig::default()
    };
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let mut brokers = Vec::new();
    for name in BROKERS {
        brokers.push(Broker::start_with(name, &book, true, timers.clone()).await);
    }
    let members: BTreeSet<MemberId> = BROKERS.map(|b| MemberId::new(b, 1)).into();
    // "c" opens the group only once the others have purged its start.
    for (name, broker) in BROKERS.iter().zip(&brokers).take(2) {
        let group = GroupId::new(shard.clone(), 0);
        broker
            .runtime
            .open(group, 1, open_log(&root, name))
            .await
            .expect("open");
        broker
            .runtime
            .initialize(&shard, members.clone())
            .await
            .expect("initialize");
    }
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let leader = loop {
        let leading = brokers[..2].iter().find(|broker| {
            let status = broker.runtime.status(&shard).expect("open");
            status.role == Role::Leader && status.committed.is_some()
        });
        if let Some(leading) = leading {
            break leading.runtime.group(&shard).expect("group");
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let mut last = 0;
    for sequence in 1..=WRITES {
        let put = CacheCommand::Put {
            key: format!("k{sequence}"),
            value: vec![sequence as u8; VALUE_BYTES].into(),
            expires_at: 0,
            cond: Condition::None,
            request: Asked {
                producer: 7,
                sequence,
                floor: 0,
            },
        };
        let command = Command {
            timestamp_micros: sequence,
            payload: put.encode(),
            written: None,
        };
        let written = leader.raft().client_write(command).await.expect("write");
        last = written.log_id.index;
    }
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let snapshot = loop {
        let snapshot = leader.raft().get_snapshot().await.expect("read");
        let taken = snapshot.filter(|snapshot| {
            let upto = snapshot.meta.last_log_id.map(|id| id.index);
            upto.is_some_and(|upto| upto + 16 > last)
        });
        let purged = leader.raft().metrics().borrow_watched().purged;
        if let Some(snapshot) = taken.filter(|_| purged.is_some()) {
            break snapshot;
        }
        assert!(tokio::time::Instant::now() < deadline, "no snapshot");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let piece = Duration::from_millis(crate::raft::runtime::SNAPSHOT_PIECE_MILLIS);
    let piece = crate::raft::wire::append_budget(piece);
    assert!(
        snapshot.snapshot.bytes.len() > piece,
        "one piece would carry it"
    );

    let group = GroupId::new(shard.clone(), 0);
    brokers[2]
        .runtime
        .open(group, 1, open_log(&root, "c"))
        .await
        .expect("open");
    let joined = brokers[2].runtime.group(&shard).expect("group");
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let installed = joined.raft().metrics().borrow_watched().snapshot;
        if installed.is_some_and(|id| id.index >= snapshot.meta.last_log_id.expect("id").index) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no snapshot sent");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let data = |group: &crate::raft::group::Group| group.cache().read().snapshot();
    let deadline = tokio::time::Instant::now() + PATIENCE;
    while data(&joined) != data(&leader) {
        assert!(tokio::time::Instant::now() < deadline, "not caught up");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for broker in brokers {
        broker.stop().await;
    }
}
