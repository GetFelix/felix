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
        beat: crate::raft::network::Beat::default(),
        report: crate::raft::network::Ask::Nothing,
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

    /// Admit brokers to the shard's copies here, as the broker's feed of
    /// the control plane would ([`GroupRuntime::admit`]).
    fn admit(&self, shard: &ShardKey, blank: &[&str], current: &[&str]) {
        let names = |names: &[&str]| names.iter().map(|b| b.to_string()).collect();
        self.runtime.admit(
            &GroupId::new(shard.clone(), 0),
            names(blank),
            names(current),
        );
        self.runtime
            .admit_until(Some(tokio::time::Instant::now() + PATIENCE));
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
    for broker in &brokers {
        broker.admit(&shard(), &BROKERS, &BROKERS);
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
    let answer = crate::peer::RaftHandler::answer(
        &runtime,
        Some("b".into()),
        bytes::Bytes::from_static(b"\x09\x03"),
    )
    .await;
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
    for broker in &brokers {
        broker.admit(&shard, &BROKERS, &BROKERS);
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
            producer: None,
            expected: None,
            trim: None,
            commit: false,
            group: false,
            publisher: None,
            headers: None,
            written: None,
            cond: None,
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

/// A vote names its candidate, and openraft grants it to that member. A
/// connection bound to one broker may not ask in another's name, nor ask at
/// all before it is bound.
#[tokio::test]
async fn a_raft_message_in_another_brokers_name_is_refused() {
    let runtime = GroupRuntime::start(
        "b",
        RuntimeConfig::default(),
        Arc::new(crate::raft::AgreeingPeers),
    )
    .expect("timers");
    let vote = vote_for(GroupId::new(shard(), 0), MemberId::new("b", 1));
    let answer = |from: Option<&str>| {
        let bytes = wire::encode_request(&vote).expect("encode");
        let answer = runtime.answer(from.map(Arc::from), bytes);
        async { wire::decode_reply(answer.await).expect("reply") }
    };
    // Past the check, there is no member here to answer it.
    let passed = answer(Some("a")).await.err().expect("no member");
    assert!(matches!(passed, Refused::NoSuchMember(_)), "{passed}");
    for from in [Some("c"), None] {
        let refused = answer(from).await.err().expect("refused");
        assert!(matches!(refused, Refused::Unreachable(_)), "{refused}");
    }
    runtime.shutdown().await;
}

/// The answer path itself holds a connection to its bound broker: an append
/// or a written position sent in another member's name is refused before the
/// group sees it.
#[tokio::test]
async fn answering_refuses_an_append_or_written_in_another_brokers_name() {
    let book = Book::default();
    let broker = Broker::start("a", &book, true).await;
    let group = GroupId::new(shard(), 0);
    let to = MemberId::new("a", 1);
    let leader = openraft::impls::leader_id_std::LeaderId::new(1, MemberId::new("b", 1));
    let append = Request::Append(openraft::raft::AppendEntriesRequest {
        vote: VoteOf::<ShardTypes>::from_leader_id(leader, true),
        prev_log_id: None,
        entries: Vec::new(),
        leader_commit: None,
    });
    let written = Request::Written {
        from: MemberId::new("b", 1),
        at: felix_storage::raft_log::LogPoint { term: 1, index: 1 },
    };
    for body in [append, written] {
        let mut request = vote_for(group.clone(), to.clone());
        request.body = body;
        let bytes = wire::encode_request(&request).expect("encode");
        let reply = broker.runtime.answer(Some("c".into()), bytes).await;
        let Err(Refused::Unreachable(why)) = wire::decode_reply(reply).expect("reply") else {
            panic!("expected a refusal");
        };
        assert!(why.contains("sent a message as"), "{why}");
    }
    broker.stop().await;
}

/// A broker outside the group's membership may not raise a member's term,
/// take its vote or append to it, even in its own name and at a higher
/// term. A member of the same message is answered.
#[tokio::test]
async fn an_outsider_at_a_higher_term_is_refused_and_changes_nothing() {
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let broker = Broker::start("a", &book, true).await;
    let group = GroupId::new(shard(), 0);
    let to = MemberId::new("a", 1);
    broker
        .runtime
        .open(group.clone(), 1, open_log(&root, "a"))
        .await
        .expect("open");
    let members: BTreeSet<MemberId> = ["a", "b"].map(|b| MemberId::new(b, 1)).into();
    broker
        .runtime
        .initialize(&shard(), members)
        .await
        .expect("initialize");
    let member = broker.runtime.group(&shard()).expect("group");
    // The member campaigns on its own meanwhile, so only the outsider's
    // term and name are looked for. Asked of the core, which has handled
    // every earlier message by then: its metrics may still be on the way.
    let vote = || async {
        let state = member
            .raft()
            .with_raft_state(|state| state.vote_ref().clone());
        state.await.expect("core")
    };
    let ask = |name: &str, append: bool| {
        let sender = MemberId::new(name, 1);
        let leader = openraft::impls::leader_id_std::LeaderId::new(100, sender);
        let mut request = vote_for(group.clone(), to.clone());
        request.body = if append {
            Request::Append(openraft::raft::AppendEntriesRequest {
                vote: VoteOf::<ShardTypes>::from_leader_id(leader, true),
                prev_log_id: None,
                entries: Vec::new(),
                leader_commit: None,
            })
        } else {
            Request::Vote(VoteRequest::new(
                VoteOf::<ShardTypes>::from_leader_id(leader, false),
                None,
            ))
        };
        wire::encode_request(&request).expect("encode")
    };
    for append in [true, false] {
        let reply = broker
            .runtime
            .answer(Some("c".into()), ask("c", append))
            .await;
        let Err(Refused::Unreachable(why)) = wire::decode_reply(reply).expect("reply") else {
            panic!("expected a refusal");
        };
        assert!(why.contains("not a member"), "{why}");
        let now = vote().await;
        assert!(term_of(&now) < 100, "an outsider raised the term: {now:?}");
        assert_ne!(now.leader_id().node_id().broker.as_ref(), "c", "{now:?}");
    }
    let reply = broker
        .runtime
        .answer(Some("b".into()), ask("b", true))
        .await;
    wire::decode_reply(reply)
        .expect("reply")
        .expect("a member's append is answered");
    assert_eq!(
        term_of(&vote().await),
        100,
        "a member's higher term is taken"
    );
    // A member that missed the change adding `c` hears from it once the
    // control plane places `c` in the group, and not from the epoch's
    // record alone.
    broker.admit(&shard(), &["a", "b", "c"], &["a", "b"]);
    let reply = broker
        .runtime
        .answer(Some("c".into()), ask("c", true))
        .await;
    assert!(wire::decode_reply(reply).expect("reply").is_err());
    broker.admit(&shard(), &["a", "b", "c"], &["a", "b", "c"]);
    let reply = broker
        .runtime
        .answer(Some("c".into()), ask("c", true))
        .await;
    wire::decode_reply(reply)
        .expect("reply")
        .expect("a broker placed now is answered");
    broker.stop().await;
}

/// A message for another copy of a member is answered with that member's
/// copy id, which only a broker of the group is told. The answer path
/// refuses an outsider before that refusal is built.
#[tokio::test]
async fn an_outsider_is_not_told_a_members_copy() {
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let broker = Broker::start("a", &book, true).await;
    let group = GroupId::new(shard(), 0);
    broker
        .runtime
        .open(group.clone(), 1, open_log(&root, "a"))
        .await
        .expect("open");
    let members: BTreeSet<MemberId> = ["a", "b"].map(|b| MemberId::new(b, 1)).into();
    broker
        .runtime
        .initialize(&shard(), members)
        .await
        .expect("initialize");
    let ask = |name: &str| {
        let leader = openraft::impls::leader_id_std::LeaderId::new(1, MemberId::new(name, 1));
        let mut request = vote_for(group.clone(), MemberId::new("a", 2));
        request.body = Request::Vote(VoteRequest::new(
            VoteOf::<ShardTypes>::from_leader_id(leader, false),
            None,
        ));
        wire::encode_request(&request).expect("encode")
    };
    let reply = broker.runtime.answer(Some("c".into()), ask("c")).await;
    let Err(Refused::Unreachable(why)) = wire::decode_reply(reply).expect("reply") else {
        panic!("expected a refusal");
    };
    assert!(why.contains("not a member"), "{why}");
    let reply = broker.runtime.answer(Some("b".into()), ask("b")).await;
    assert!(
        matches!(
            wire::decode_reply(reply).expect("reply"),
            Err(Refused::OtherCopy(_, 1))
        ),
        "a member is told the copy"
    );
    broker.stop().await;
}

/// A copy with no membership cannot know its leader yet, so it takes a
/// first message only from a broker placed in the group's epoch. An
/// outsider racing the group's first form neither raises its term, takes
/// its vote nor writes the membership that would make it the leader.
#[tokio::test]
async fn a_blank_copy_refuses_a_broker_not_placed_in_its_group() {
    use openraft::async_runtime::watch::WatchReceiver;
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let broker = Broker::start("a", &book, true).await;
    let group = GroupId::new(shard(), 0);
    let to = MemberId::new("a", 1);
    broker.admit(&shard(), &["a", "b"], &["a", "b"]);
    broker
        .runtime
        .open(group.clone(), 1, open_log(&root, "a"))
        .await
        .expect("open");
    let member = broker.runtime.group(&shard()).expect("group");
    let ask = |name: &str, append: bool| {
        let sender = MemberId::new(name, 1);
        let leader = openraft::impls::leader_id_std::LeaderId::new(100, sender.clone());
        let mut request = vote_for(group.clone(), to.clone());
        request.body = if append {
            use openraft::entry::RaftEntry;
            use openraft::type_config::alias::{EntryOf, LogIdOf};
            let membership = crate::raft::types::Membership::new(
                vec![[sender.clone(), to.clone()].into()],
                BTreeSet::new(),
                crate::raft::TargetVersion::default(),
            );
            Request::Append(openraft::raft::AppendEntriesRequest {
                vote: VoteOf::<ShardTypes>::from_leader_id(leader, true),
                prev_log_id: None,
                entries: vec![EntryOf::<ShardTypes>::new_membership(
                    LogIdOf::<ShardTypes>::new(100.into(), 0),
                    membership,
                )],
                leader_commit: None,
            })
        } else {
            Request::Vote(VoteRequest::new(
                VoteOf::<ShardTypes>::from_leader_id(leader, false),
                None,
            ))
        };
        wire::encode_request(&request).expect("encode")
    };
    for append in [false, true] {
        let reply = broker
            .runtime
            .answer(Some("c".into()), ask("c", append))
            .await;
        let Err(Refused::Unreachable(why)) = wire::decode_reply(reply).expect("reply") else {
            panic!("expected a refusal");
        };
        assert!(why.contains("not a member"), "{why}");
        let metrics = member.raft().metrics().borrow_watched().clone();
        assert_eq!(term_of(&metrics.vote), 0, "{:?}", metrics.vote);
        assert!(metrics.last_log_index.is_none(), "{metrics:?}");
        assert_eq!(
            metrics.membership_config.membership().voter_ids().count(),
            0,
            "an outsider wrote the membership"
        );
    }
    let reply = broker
        .runtime
        .answer(Some("b".into()), ask("b", true))
        .await;
    wire::decode_reply(reply)
        .expect("reply")
        .expect("a placed broker's first append is answered");
    broker.stop().await;
}

/// An append at a high term from `sender`, naming it the leader, whose one
/// entry is a membership of `voters`.
fn forged_append(group: &GroupId, to: &MemberId, sender: &str, voters: &[&str]) -> bytes::Bytes {
    use openraft::entry::RaftEntry;
    use openraft::type_config::alias::{EntryOf, LogIdOf};
    let leader = openraft::impls::leader_id_std::LeaderId::new(100, MemberId::new(sender, 1));
    let membership = crate::raft::types::Membership::new(
        vec![voters.iter().map(|b| MemberId::new(*b, 1)).collect()],
        BTreeSet::new(),
        crate::raft::TargetVersion::default(),
    );
    let mut request = vote_for(group.clone(), to.clone());
    request.body = Request::Append(openraft::raft::AppendEntriesRequest {
        vote: VoteOf::<ShardTypes>::from_leader_id(leader, true),
        prev_log_id: None,
        entries: vec![EntryOf::<ShardTypes>::new_membership(
            LogIdOf::<ShardTypes>::new(100.into(), 0),
            membership,
        )],
        leader_commit: None,
    });
    wire::encode_request(&request).expect("encode")
}

/// A broker the control plane placed in the group earlier in its epoch and
/// then removed cannot take over a new copy. `x` was moved off the shard;
/// `d` is added, and its blank copy hears only from the brokers in the
/// group now (placement `a b d`, reported voters `a b`). `x` racing the
/// leader with a membership naming `a b d` neither writes it nor raises the
/// copy's term, so `d` cannot later win an election on a log `x` forged.
/// With no admission at all the blank copy hears from no one.
#[tokio::test]
async fn a_removed_broker_cannot_write_a_new_copys_first_membership() {
    use openraft::async_runtime::watch::WatchReceiver;
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let broker = Broker::start("d", &book, true).await;
    let group = GroupId::new(shard(), 0);
    let to = MemberId::new("d", 1);
    broker
        .runtime
        .open(group.clone(), 1, open_log(&root, "d"))
        .await
        .expect("open");
    let member = broker.runtime.group(&shard()).expect("group");
    let untouched = || {
        let metrics = member.raft().metrics().borrow_watched().clone();
        assert_eq!(term_of(&metrics.vote), 0, "{:?}", metrics.vote);
        assert!(metrics.last_log_index.is_none(), "{metrics:?}");
    };
    let refused = |reply: bytes::Bytes| {
        let Err(Refused::Unreachable(why)) = wire::decode_reply(reply).expect("reply") else {
            panic!("expected a refusal");
        };
        assert!(why.contains("not a member"), "{why}");
    };
    let ask = |sender: &str| forged_append(&group, &to, sender, &["a", "b", "d"]);
    refused(broker.runtime.answer(Some("a".into()), ask("a")).await);
    untouched();
    broker.admit(&shard(), &["a", "b", "d"], &["b", "d"]);
    refused(broker.runtime.answer(Some("x".into()), ask("x")).await);
    untouched();
    wire::decode_reply(broker.runtime.answer(Some("a".into()), ask("a")).await)
        .expect("reply")
        .expect("the reported leader's first append is answered");
    broker.stop().await;
}

/// Past [`GroupRuntime::admit_until`] a member hears only from its own
/// membership: a broker the feed placed may have been removed since the
/// feed last answered.
#[tokio::test]
async fn a_lapsed_admission_admits_only_the_membership() {
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let broker = Broker::start("a", &book, true).await;
    let group = GroupId::new(shard(), 0);
    let to = MemberId::new("a", 1);
    broker
        .runtime
        .open(group.clone(), 1, open_log(&root, "a"))
        .await
        .expect("open");
    let members: BTreeSet<MemberId> = ["a", "b"].map(|b| MemberId::new(b, 1)).into();
    broker
        .runtime
        .initialize(&shard(), members)
        .await
        .expect("initialize");
    broker.admit(&shard(), &["a", "b", "c"], &["a", "b", "c"]);
    broker
        .runtime
        .admit_until(Some(tokio::time::Instant::now()));
    let ask = |sender: &str| forged_append(&group, &to, sender, &["a", "b", "c"]);
    let reply = broker.runtime.answer(Some("c".into()), ask("c")).await;
    assert!(
        wire::decode_reply(reply).expect("reply").is_err(),
        "a lapsed placement admitted c"
    );
    let reply = broker.runtime.answer(Some("b".into()), ask("b")).await;
    wire::decode_reply(reply)
        .expect("reply")
        .expect("a member is answered whatever the feed");
    broker.stop().await;
}

fn term_of(vote: &VoteOf<ShardTypes>) -> u64 {
    vote.leader_id().term()
}

/// A broker being added is not refused anywhere on its way in: as a new
/// copy with no membership it takes the leader's appends, as a learner it
/// catches up, and once the joint change lands it is a voter every member
/// answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_added_through_joint_consensus_is_answered() {
    joint_add(false).await;
}

/// As [`a_member_added_through_joint_consensus_is_answered`], with the
/// placement moved off the leader: the new copy hears from it as a voter
/// the leader reported, and no broker as placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_off_placement_still_adds_a_member() {
    joint_add(true).await;
}

async fn joint_add(off_placement: bool) {
    use crate::raft::{Target, TargetVersion};
    let root = tempfile::tempdir().expect("dir");
    let book = Book::default();
    let mut brokers = Vec::new();
    for name in BROKERS {
        brokers.push(Broker::start(name, &book, true).await);
    }
    for broker in &brokers {
        broker.admit(&shard(), &BROKERS, &BROKERS);
    }
    let first: BTreeSet<MemberId> = ["a", "b"].map(|b| MemberId::new(b, 1)).into();
    for (name, broker) in BROKERS.iter().zip(&brokers) {
        broker
            .runtime
            .open(GroupId::new(shard(), 0), 1, open_log(&root, name))
            .await
            .expect("open");
    }
    for broker in &brokers[..2] {
        broker
            .runtime
            .initialize(&shard(), first.clone())
            .await
            .expect("initialize");
    }
    let deadline = tokio::time::Instant::now() + PATIENCE;
    let (leader, term) = loop {
        if let Some((leader, term)) = brokers[..2]
            .iter()
            .find_map(|b| b.runtime.led_term(&shard()).map(|term| (b, term)))
        {
            break (leader, term);
        }
        assert!(tokio::time::Instant::now() < deadline, "no leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    if off_placement {
        let led = leader.runtime.status(&shard()).expect("open").member.broker;
        let placed: Vec<&str> = BROKERS.into_iter().filter(|b| **b != *led).collect();
        for broker in &brokers {
            broker.admit(&shard(), &BROKERS, &placed);
        }
    }
    let all: BTreeSet<MemberId> = BROKERS.map(|b| MemberId::new(b, 1)).into();
    let target = Target {
        voters: all.clone(),
        version: TargetVersion {
            incarnation: 0,
            counter: 1,
        },
    };
    assert!(leader.runtime.retarget(&shard(), target, term));
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        let statuses: Vec<_> = brokers
            .iter()
            .map(|broker| broker.runtime.status(&shard()).expect("open"))
            .collect();
        let voters = |b: &Broker| b.runtime.group(&shard()).expect("group").voters();
        let joined = brokers.iter().all(|b| voters(b) == all)
            && statuses
                .iter()
                .all(|s| s.committed.is_some() && s.committed == statuses[0].committed);
        if joined {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the new member did not join: {statuses:#?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for broker in brokers {
        broker.stop().await;
    }
}
