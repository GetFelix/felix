use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Mutex;

use felix_broker::{ConsistencyLevel, DurableStorage, StreamMetadata};
use felix_common::fleet::{
    FleetGate, GENERATION_START, LEASE_FREE_READS, MAJORITY_ACK, REPLICA_ELECTION,
};
use felix_router::{NodeRef, RegionRouter, RoutingTable};
use felix_storage::EphemeralCache;
use felix_storage::disk_log::{Member, ReplicaSet};
use felix_storage::log::{FsyncMode, LogConfig};
use felix_wire::internal::PreVoteAnswer;
use tokio::sync::Notify;

use super::*;
use crate::peer::PeerError;

const LOCAL: &str = "broker-b";
const TENANT: &str = "t1";
const NAMESPACE: &str = "ns";
const STREAM: &str = "orders";

fn key() -> ShardKey {
    ShardKey {
        tenant_id: TENANT.into(),
        namespace: NAMESPACE.into(),
        stream: STREAM.into(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

/// Answers every pre-vote as `answers` says for the node asked, and records
/// who was asked.
struct Voters {
    answers: Vec<(&'static str, Option<PreVoteRefusal>)>,
    asked: Mutex<Vec<String>>,
}

impl PeerRequester for Voters {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        assert!(
            matches!(message, InternalMessage::PreVote(_)),
            "{message:?}"
        );
        self.asked.lock().expect("lock").push(node_id.to_string());
        let (_, refusal) = self
            .answers
            .iter()
            .find(|(node, _)| *node == node_id)
            .expect("asked a node outside the set");
        Ok(InternalMessage::PreVoteAnswer(PreVoteAnswer {
            correlation_id: 0,
            refusal: *refusal,
        }))
    }

    fn offers_election(&self) -> bool {
        true
    }
}

fn node(node_id: &str, port: u16) -> NodeRef {
    NodeRef {
        node_id: node_id.to_string(),
        advertise_addr: format!("10.0.0.1:{port}").parse().expect("addr"),
        region: "us-west-2".to_string(),
        live: true,
    }
}

/// `LOCAL` following broker-a at `generation`, with broker-c.
fn router(generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LOCAL,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [("broker-a", 7001), (LOCAL, 7002), ("broker-c", 7003)]
        .into_iter()
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let table = RoutingTable::build(
        [(
            key(),
            "broker-a".to_string(),
            vec![LOCAL.to_string(), "broker-c".to_string()],
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

fn set(generation: u64) -> Membership {
    Membership {
        generation,
        members: ["broker-a", LOCAL, "broker-c"]
            .into_iter()
            .map(|node| Member {
                node: node.to_string(),
                storage: None,
                learner: false,
                wiped: false,
            })
            .collect(),
    }
}

/// A broker holding a `Quorum` stream whose log keeps the set at 3.
async fn follower() -> (Arc<Broker>, tempfile::TempDir) {
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
    let broker = Arc::new(Broker::new(EphemeralCache::new().into()).with_durable_storage(storage));
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
            StreamMetadata {
                durable: true,
                shards: 1,
                consistency: ConsistencyLevel::Quorum,
                ..Default::default()
            },
        )
        .await
        .expect("stream");
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(3, Some("broker-a"))
        .await
        .expect("ballot");
    log.accept_set(ReplicaSet {
        current: set(3),
        leaving: None,
    })
    .await
    .expect("set");
    (broker, dir)
}

fn ctx(broker: &Arc<Broker>, voters: Voters) -> Ctx<Voters> {
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
        REPLICA_ELECTION.name(),
    ];
    let fleet = FleetGate::new(names);
    fleet.observe(names);
    let marks = Arc::new(QuorumMarks::with_fleet(Arc::new(fleet)));
    let elections = Arc::new(Elections::new(LOCAL, Arc::new(Notify::new())));
    marks.set_elections(Arc::clone(&elections));
    let suspects = Arc::new(Suspects::default());
    suspects.publish(BTreeSet::from(["broker-a".to_string()]));
    Ctx {
        requester: Arc::new(voters),
        broker: Arc::clone(broker),
        router: router(3),
        marks,
        elections,
        suspects,
        held_back: Default::default(),
    }
}

fn voters(c: Option<PreVoteRefusal>) -> Voters {
    Voters {
        answers: vec![
            ("broker-a", Some(PreVoteRefusal::LeaderReachable)),
            ("broker-c", c),
        ],
        asked: Mutex::new(Vec::new()),
    }
}

/// **A follower with a majority's pre-vote stands**: the ballot for the next
/// generation is on disk, and the standing is recorded on the set it keeps.
/// Nothing opens it here, so it is withdrawn and the ballot stays.
#[tokio::test(start_paused = true)]
async fn a_granted_pre_vote_puts_the_ballot_on_disk_then_records_the_standing() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let log = shard_log(&broker, &key()).await.expect("log");

    let standing = {
        let ctx = &ctx;
        async move {
            stand(
                ctx,
                &key(),
                &ctx.router.snapshot().get(&key()).expect("route").clone(),
            )
            .await
        }
    };
    let watched = async {
        loop {
            if let Some(elected) = ctx.elections.get(&key()) {
                return elected;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let (round, elected) = tokio::join!(standing, watched);

    assert_eq!(elected.leader, LOCAL);
    assert_eq!(elected.generation, 4);
    assert_eq!(elected.set.generation, 3, "standing changes no set");
    assert_eq!(log.accepted_generation(), 4);
    assert_eq!(log.accepted_leader().as_deref(), Some(LOCAL));
    assert_eq!(round, Round::Failed, "nothing opened it");
    assert!(ctx.elections.get(&key()).is_none(), "not withdrawn");
}

/// **Without a majority nothing is written**: a refused pre-vote leaves no
/// ballot and no standing (rule 2).
#[tokio::test(start_paused = true)]
async fn a_refused_pre_vote_writes_nothing() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(Some(PreVoteRefusal::LeaderReachable)));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();

    assert_eq!(stand(&ctx, &key(), &route).await, Round::Failed);
    let log = shard_log(&broker, &key()).await.expect("log");
    assert_eq!(log.accepted_generation(), 3);
    assert!(ctx.elections.get(&key()).is_none());
}

/// **A kept set the control plane does not record is not stood on**: the
/// candidate's fence would miss a majority of the recorded one.
#[tokio::test(start_paused = true)]
async fn a_set_the_control_plane_does_not_record_is_not_stood_on() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let mut recorded = Recorded::of_route(&route);
    recorded.members.remove("broker-c");
    recorded.members.insert("broker-d".to_string(), None);
    ctx.elections.note_recorded(key(), recorded);

    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);
    assert!(ctx.requester.asked.lock().expect("lock").is_empty());
}

/// **No standing while a set change is pending**, on the kept set or on the
/// control plane's: a candidate fences one set, so it would miss a majority
/// of the other, and with it the counter updates and records acknowledged
/// there (#1830).
#[tokio::test(start_paused = true)]
async fn a_pending_set_change_is_not_stood_on() {
    let (broker, _dir) = follower().await;
    let recorded_pending = ctx(&broker, voters(None));
    let route = recorded_pending
        .router
        .snapshot()
        .get(&key())
        .expect("route")
        .clone();
    let mut recorded = Recorded::of_route(&route);
    recorded.pending = true;
    recorded_pending.elections.note_recorded(key(), recorded);
    assert_eq!(
        stand(&recorded_pending, &key(), &route).await,
        Round::Ineligible
    );
    assert!(
        recorded_pending
            .requester
            .asked
            .lock()
            .expect("lock")
            .is_empty()
    );

    // The kept set's members match the route; only its leaving set differs.
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let log = shard_log(&broker, &key()).await.expect("log");
    let mut leaving = set(3);
    leaving.members[2].node = "broker-d".to_string();
    log.accept_set(ReplicaSet {
        current: set(4),
        leaving: Some(leaving),
    })
    .await
    .expect("set");
    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);

    assert!(ctx.requester.asked.lock().expect("lock").is_empty());
    assert_eq!(log.accepted_generation(), 3, "no ballot");
}

/// The place counts voters with a log ahead and granting voters with a
/// lower node id.
#[tokio::test]
async fn the_place_counts_the_voters_ahead() {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    let place = |c| {
        let ctx = ctx(&broker, voters(c));
        let log = log.clone();
        async move { pre_vote(&ctx, &key(), &log, &set(3), 4).await.ok() }
    };
    assert_eq!(place(None).await, Some(0), "broker-c sorts after broker-b");
    assert_eq!(place(Some(PreVoteRefusal::LogBehind)).await, None);
}

/// A voter that takes longer to dial than a pre-vote waits. Only a dial
/// that runs to the end leaves a connection; a request dials if there is
/// none.
struct SlowToDial {
    dial: Duration,
    connected: Mutex<BTreeSet<String>>,
}

impl SlowToDial {
    async fn dial(&self, node_id: &str) {
        if !self.connected.lock().expect("lock").contains(node_id) {
            tokio::time::sleep(self.dial).await;
            self.connected
                .lock()
                .expect("lock")
                .insert(node_id.to_string());
        }
    }
}

impl PeerRequester for SlowToDial {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        _message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        self.dial(node_id).await;
        let refusal = (node_id == "broker-a").then_some(PreVoteRefusal::LeaderReachable);
        Ok(InternalMessage::PreVoteAnswer(PreVoteAnswer {
            correlation_id: 0,
            refusal,
        }))
    }

    async fn capabilities(
        &self,
        node_id: &str,
        _addr: SocketAddr,
    ) -> Result<felix_wire::internal::PeerCapabilities, PeerError> {
        self.dial(node_id).await;
        Ok(felix_wire::internal::PeerCapabilities::NONE)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// **A dial slower than the pre-vote wait still serves the next round.** The
/// first round times out on it, but the dial runs on, so the second finds
/// the voter connected and wins. A round whose dial went with its request
/// would start it over each time and never get a vote.
#[tokio::test(start_paused = true)]
async fn a_slow_dial_carries_over_to_the_next_round() {
    let (broker, _dir) = follower().await;
    let Ctx {
        broker,
        router,
        marks,
        elections,
        suspects,
        ..
    } = ctx(&broker, voters(None));
    let ctx = Ctx {
        requester: Arc::new(SlowToDial {
            dial: PRE_VOTE_WITHIN + Duration::from_millis(500),
            connected: Mutex::new(BTreeSet::new()),
        }),
        broker,
        router,
        marks,
        elections,
        suspects,
        held_back: Default::default(),
    };
    let log = shard_log(&ctx.broker, &key()).await.expect("log");

    assert!(pre_vote(&ctx, &key(), &log, &set(3), 4).await.is_err());
    tokio::time::sleep(BACKOFF_FIRST_TOP).await;
    assert_eq!(pre_vote(&ctx, &key(), &log, &set(3), 4).await, Ok(0));
}

/// A failed round waits from the floor to a top that doubles to the cap.
#[test]
fn the_backoff_doubles_to_its_cap() {
    let mut backoff = Backoff::default();
    for _ in 0..10 {
        let before = Instant::now();
        backoff.failed();
        let wait = backoff.until - before;
        assert!(wait >= BACKOFF_FLOOR && wait <= BACKOFF_TOP + Duration::from_millis(5));
    }
    assert_eq!(backoff.failures, 10);
}

/// Stand for `key()` on the route `ctx` has, and return the round with the
/// standing it recorded.
async fn stand_and_watch(ctx: &Ctx<Voters>) -> (Round, Elected) {
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let watched = async {
        loop {
            if let Some(elected) = ctx.elections.get(&key()) {
                return elected;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let shard = key();
    tokio::join!(stand(ctx, &shard, &route), watched)
}

/// **A candidate stands above the route's generation as well as its own
/// ballot.** The control plane named generation 5, which never fenced this
/// replica; standing at 4 would lose every vote to that leader's ballot, and
/// one generation more would collide with it.
#[tokio::test(start_paused = true)]
async fn a_candidate_stands_above_the_routes_generation() {
    let (broker, _dir) = follower().await;
    let mut ctx = ctx(&broker, voters(None));
    ctx.router = router(5);

    let (_, elected) = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 6);
}

/// **A candidate does not stand past the control plane's adoption window**:
/// the control plane would refuse to adopt the leadership, so its route would
/// never drop. At the edge of the window it still stands.
#[tokio::test(start_paused = true)]
async fn a_candidate_does_not_stand_past_the_adoption_window() {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(2 + ELECTION_GENERATION_WINDOW, Some("broker-a"))
        .await
        .expect("ballot");
    let (_, elected) = stand_and_watch(&ctx(&broker, voters(None))).await;
    assert_eq!(elected.generation, 3 + ELECTION_GENERATION_WINDOW);

    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(3 + ELECTION_GENERATION_WINDOW, Some("broker-a"))
        .await
        .expect("ballot");
    let past = ctx(&broker, voters(None));
    let route = past.router.snapshot().get(&key()).expect("route").clone();
    assert_eq!(stand(&past, &key(), &route).await, Round::Ineligible);
    assert!(past.requester.asked.lock().expect("lock").is_empty());
    assert_eq!(log.accepted_generation(), 3 + ELECTION_GENERATION_WINDOW);
}

/// **A standing whose fence is still running is not withdrawn** when it
/// takes longer than [`OPEN_WITHIN`]: a fence or catch-up can, and giving up
/// would burn the generation. Once the fence ends without opening, it is.
#[tokio::test(start_paused = true)]
async fn a_standing_is_kept_while_its_fence_runs() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let fencing = async {
        let generation = loop {
            if let Some(elected) = ctx.elections.get(&key()) {
                break elected.generation;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let fence = ctx.elections.fencing(&key(), generation);
        tokio::time::sleep(OPEN_WITHIN * 2).await;
        let kept = ctx.elections.get(&key()).is_some();
        drop(fence);
        kept
    };
    let shard = key();
    let (round, kept) = tokio::join!(stand(&ctx, &shard, &route), fencing);
    assert!(kept, "withdrawn while its fence ran");
    assert_eq!(round, Round::Failed);
    assert!(ctx.elections.get(&key()).is_none());
}

/// **A standing whose generation this broker has since promised to another
/// leader is withdrawn** (rule 5); one it has not promised past stays.
#[tokio::test]
async fn a_standing_promised_away_is_withdrawn() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some(LOCAL)).await.expect("ballot");
    ctx.elections.record(
        key(),
        Elected {
            leader: LOCAL.to_string(),
            generation: 4,
            set: set(3),
            incarnation: 0,
        },
    );

    step_down_promised_away(&ctx).await;
    assert!(ctx.elections.get(&key()).is_some(), "not promised past");

    log.accept_generation(5, Some("broker-c"))
        .await
        .expect("a newer fence");
    step_down_promised_away(&ctx).await;
    assert!(ctx.elections.get(&key()).is_none());
}
