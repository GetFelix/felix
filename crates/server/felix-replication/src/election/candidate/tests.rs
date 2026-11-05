use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Mutex;

use felix_broker::{ConsistencyLevel, DurableStorage, StreamMetadata};
use felix_common::fleet::{
    FleetGate, GENERATION_START, JOINT_FENCE, LEASE_FREE_READS, MAJORITY_ACK, REPLICA_ELECTION,
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

/// Answers every pre-vote as `answers` says for the node asked, or from its
/// second ask on as `then` says if it names the node, and records who was
/// asked.
struct Voters {
    answers: Vec<(&'static str, Option<PreVoteRefusal>)>,
    then: Vec<(&'static str, Option<PreVoteRefusal>)>,
    /// Nodes that do not answer from their second ask on.
    silent_again: Vec<&'static str>,
    asked: Mutex<Vec<String>>,
    /// Run before answering the ask with this index, counted from zero.
    on_ask: Mutex<Option<(usize, OnAsk)>>,
}

type OnAsk = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

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
        let (again, index) = {
            let mut asked = self.asked.lock().expect("lock");
            let again = asked.iter().any(|asked| asked == node_id);
            asked.push(node_id.to_string());
            (again, asked.len() - 1)
        };
        let hook = {
            let mut on_ask = self.on_ask.lock().expect("lock");
            match on_ask.take() {
                Some((at, hook)) if at == index => Some(hook),
                other => {
                    *on_ask = other;
                    None
                }
            }
        };
        if let Some(hook) = hook {
            hook.await;
        }
        if again && self.silent_again.contains(&node_id) {
            return Err(PeerError::Disconnected {
                node_id: node_id.to_string(),
            });
        }
        let (_, refusal) = again
            .then(|| self.then.iter().find(|(node, _)| *node == node_id))
            .flatten()
            .or_else(|| self.answers.iter().find(|(node, _)| *node == node_id))
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
        earlier: Vec::new(),
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
        JOINT_FENCE.name(),
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
        deferring: Default::default(),
        stood: Default::default(),
        named_here: Default::default(),
        pre_vote_gate: None,
    }
}

fn voters(c: Option<PreVoteRefusal>) -> Voters {
    Voters {
        answers: vec![
            ("broker-a", Some(PreVoteRefusal::LeaderReachable)),
            ("broker-c", c),
        ],
        then: Vec::new(),
        silent_again: Vec::new(),
        asked: Mutex::new(Vec::new()),
        on_ask: Mutex::new(None),
    }
}

/// broker-a granting puts `LOCAL` second in the order, so it waits its place
/// and asks again; broker-a answers that second ask with `then`.
fn second_in_order(then: Option<PreVoteRefusal>) -> Voters {
    Voters {
        answers: vec![("broker-a", None), ("broker-c", None)],
        then: vec![("broker-a", then)],
        silent_again: Vec::new(),
        asked: Mutex::new(Vec::new()),
        on_ask: Mutex::new(None),
    }
}

/// **A candidate second in the order does not stand against one ahead that
/// stood while it waited.** broker-a's ballot is on disk, so it refuses the
/// second ask, though its fence has not reached `LOCAL`. Standing anyway
/// would split the vote at one generation (#1914).
#[tokio::test(start_paused = true)]
async fn a_voter_ahead_that_stood_meanwhile_is_not_stood_against() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(
        &broker,
        second_in_order(Some(PreVoteRefusal::StaleGeneration)),
    );
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();

    assert_eq!(stand(&ctx, &key(), &route).await, Round::Failed);
    let log = shard_log(&broker, &key()).await.expect("log");
    assert_eq!(log.accepted_generation(), 3, "no ballot");
    assert!(ctx.elections.get(&key()).is_none());
}

/// **A voter silent when asked again does not stop a candidate**: silence
/// is not evidence that anyone stood.
#[tokio::test(start_paused = true)]
async fn a_voter_silent_when_asked_again_does_not_stop_the_candidate() {
    let (broker, _dir) = follower().await;
    let mut voters = second_in_order(None);
    voters.silent_again = vec!["broker-a", "broker-c"];
    let ctx = ctx(&broker, voters);

    let (_, elected) = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 4);
}

/// **A candidate second in the order stands once its voters still agree**
/// after its wait, having asked each twice.
#[tokio::test(start_paused = true)]
async fn a_candidate_second_in_order_stands_when_its_voters_still_agree() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, second_in_order(None));

    let (_, elected) = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 4);
    let asked = ctx.requester.asked.lock().expect("lock").clone();
    assert_eq!(asked.iter().filter(|node| *node == "broker-a").count(), 2);
    assert_eq!(asked.iter().filter(|node| *node == "broker-c").count(), 2);
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
/// there (#1830). The model's `Elect` has the kept-set half as its `oldSet`
/// guard (docs/formal/FelixShard.tla).
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
        earlier: Vec::new(),
    })
    .await
    .expect("set");
    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);

    assert!(ctx.requester.asked.lock().expect("lock").is_empty());
    assert_eq!(log.accepted_generation(), 3, "no ballot");
}

/// **The set a move's cut-over leaves is stood on**: it names the same
/// copies, the staged successor (here broker-a, the leader now suspected)
/// flagged a learner. Refusing it would leave a moved shard unable to elect
/// until the control plane is back. The standing carries the set being left,
/// so its fence covers that set's voters too. A route with a change pending,
/// or naming another set, still keeps it from standing.
#[tokio::test(start_paused = true)]
async fn the_set_a_move_left_is_stood_on_jointly() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let log = shard_log(&broker, &key()).await.expect("log");
    let mut leaving = set(3);
    leaving.members[0].learner = true;
    log.accept_set(ReplicaSet {
        current: set(4),
        leaving: Some(leaving.clone()),
        earlier: Vec::new(),
    })
    .await
    .expect("set");

    let (_, elected) = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
        .await
        .expect("never stood");

    assert_eq!(elected.leader, LOCAL);
    assert_eq!(elected.set.generation, 4);
    assert_eq!(elected.leaving, Some(leaving.clone()));

    // Only while the control plane's route records the kept set with
    // nothing pending: the leader then counts the kept set alone.
    for pending in [true, false] {
        let (broker, _dir) = follower().await;
        let held = self::ctx(&broker, voters(None));
        let log = shard_log(&broker, &key()).await.expect("log");
        log.accept_set(ReplicaSet {
            current: set(4),
            leaving: Some(leaving.clone()),
            earlier: Vec::new(),
        })
        .await
        .expect("set");
        let route = held.router.snapshot().get(&key()).expect("route").clone();
        let mut recorded = Recorded::of_route(&route);
        if pending {
            recorded.pending = true;
        } else {
            recorded.members.remove("broker-c");
            recorded.members.insert("broker-d".to_string(), None);
        }
        held.elections.note_recorded(key(), recorded);

        assert_eq!(stand(&held, &key(), &route).await, Round::Ineligible);
        assert_eq!(log.accepted_generation(), 3, "no ballot");
    }
}

/// **A finished move's set being left is not fenced again**: a start record
/// at the kept set's generation means a leader opened on it, its joint fence
/// already past. Carrying the set being left would need a majority of its
/// voters, the dead leader among them, so no candidate could ever open. A
/// later start may be a named leader's that fenced the kept set alone. A
/// change pending on the kept set still keeps it from standing.
#[tokio::test(start_paused = true)]
async fn a_finished_moves_set_being_left_is_not_fenced_again() {
    let mut leaving = set(3);
    leaving.members[0].learner = true;
    let mut changed = set(3);
    changed.members[2].node = "broker-d".to_string();
    for (opened, left) in [(3, &leaving), (4, &leaving), (5, &leaving), (4, &changed)] {
        let (broker, _dir) = follower().await;
        let ctx = ctx(&broker, voters(None));
        let log = shard_log(&broker, &key()).await.expect("log");
        log.accept_generation(opened, Some("broker-a"))
            .await
            .expect("ballot");
        let start = log.append_generation_start(opened).await.expect("start");
        log.record_generation(opened, start).expect("epoch");
        log.accept_set(ReplicaSet {
            current: set(4),
            leaving: Some(left.clone()),
            earlier: Vec::new(),
        })
        .await
        .expect("set");

        if left == &changed {
            let route = ctx.router.snapshot().get(&key()).expect("route").clone();
            assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);
            assert_eq!(log.accepted_generation(), opened, "no ballot");
            continue;
        }
        let (_, elected) = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
            .await
            .expect("never stood");
        let expected = (opened != 4).then(|| leaving.clone());
        assert_eq!(elected.leaving, expected, "opened at {opened}");
    }
}

/// **A set kept beside with the same flags is stood on too**: a leader the
/// control plane named under `joint_fence` keeps the set before beside its
/// own even when nothing changed. Without `joint_fence` finalized nothing
/// stands on a set kept beside, since its fence could never open.
#[tokio::test(start_paused = true)]
async fn an_unchanged_set_kept_beside_is_stood_on_once_joint_fences_are_on() {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_set(ReplicaSet {
        current: set(4),
        leaving: Some(set(3)),
        earlier: Vec::new(),
    })
    .await
    .expect("set");

    let mut off = ctx(&broker, voters(None));
    let names = [
        GENERATION_START.name(),
        MAJORITY_ACK.name(),
        LEASE_FREE_READS.name(),
        REPLICA_ELECTION.name(),
    ];
    let fleet = FleetGate::new(names);
    fleet.observe(names);
    off.marks = Arc::new(QuorumMarks::with_fleet(Arc::new(fleet)));
    off.marks.set_elections(Arc::clone(&off.elections));
    let route = off.router.snapshot().get(&key()).expect("route").clone();
    assert_eq!(stand(&off, &key(), &route).await, Round::Ineligible);
    assert_eq!(log.accepted_generation(), 3, "no ballot");

    let on = ctx(&broker, voters(None));
    let (_, elected) = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&on))
        .await
        .expect("never stood");
    assert_eq!(elected.leaving, Some(set(3)));
}

/// A set kept from before a fence that never opened is not stood on: the
/// candidate would fence its own set alone and miss a majority of the
/// earlier one (#1881).
#[tokio::test(start_paused = true)]
async fn a_set_keeping_earlier_sets_is_not_stood_on() {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    let mut earlier = set(2);
    earlier.members[2].node = "broker-d".to_string();
    log.accept_set_keeping_earlier(ReplicaSet {
        current: set(4),
        leaving: None,
        earlier: vec![earlier],
    })
    .await
    .expect("set");
    assert!(!log.replica_set().expect("kept").earlier.is_empty());

    let ctx = ctx(&broker, voters(None));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
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
        async move {
            pre_vote(&ctx, &key(), &log, &set(3), 4)
                .await
                .ok()
                .filter(PreVoted::majority)
                .map(|voted| voted.place)
        }
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
        deferring: Default::default(),
        stood: Default::default(),
        named_here: Default::default(),
        pre_vote_gate: None,
    };
    let log = shard_log(&ctx.broker, &key()).await.expect("log");

    assert!(
        !pre_vote(&ctx, &key(), &log, &set(3), 4)
            .await
            .expect("answers")
            .majority()
    );
    tokio::time::sleep(BACKOFF_FIRST_TOP).await;
    assert_eq!(
        pre_vote(&ctx, &key(), &log, &set(3), 4)
            .await
            .map(|voted| voted.place),
        Ok(0)
    );
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
            leaving: None,
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

/// **A follower that took another leader's fence above the route does not
/// stand against it** while it does not suspect that leader and that leader
/// still answers as fencing or leading (#1914). Its view may still name the
/// dead leader; standing would take the fenced leader's last follower away
/// with a ballot nothing ever opens. Once it suspects that leader too, it
/// stands.
#[tokio::test(start_paused = true)]
async fn a_follower_of_a_newer_leader_it_does_not_suspect_does_not_stand() {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some("broker-c"))
        .await
        .expect("ballot");
    let ctx = ctx(
        &broker,
        Voters {
            answers: vec![
                ("broker-a", None),
                ("broker-c", Some(PreVoteRefusal::LeaderReachable)),
            ],
            ..voters(None)
        },
    );
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);
    assert_eq!(log.accepted_generation(), 4);

    ctx.suspects.publish(BTreeSet::from([
        "broker-a".to_string(),
        "broker-c".to_string(),
    ]));
    let (_, elected) = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
        .await
        .expect("never stood");
    assert_eq!(elected.generation, 5);
}

/// **A follower stands over a newer leader that answers as stood down**
/// (#1914): one that restarted and dropped its standing says yes to the
/// pre-vote, and it is not suspected, being alive. Without this the shard
/// waits out the defer bound with the control plane down.
#[tokio::test(start_paused = true)]
async fn a_follower_stands_once_the_newer_leader_has_stood_down() {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some("broker-c"))
        .await
        .expect("ballot");
    let ctx = ctx(
        &broker,
        Voters {
            answers: vec![
                ("broker-a", Some(PreVoteRefusal::LeaderReachable)),
                ("broker-c", Some(PreVoteRefusal::LeaderReachable)),
            ],
            then: vec![("broker-c", None)],
            ..voters(None)
        },
    );
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);

    let (_, elected) = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
        .await
        .expect("never stood");
    assert_eq!(elected.generation, 5);
}

/// **A follower defers to a newer leader only while that leader may still
/// open** (#1914). One that took the fence and never started its generation,
/// alive but stuck fencing or done with the shard, would otherwise hold the
/// shard until the control plane is back. Once that leader's start record is
/// here, it opened and reached this copy, and the follower keeps deferring.
#[tokio::test(start_paused = true)]
async fn a_follower_stands_over_a_newer_leader_that_never_starts() {
    for started in [false, true] {
        let (broker, _dir) = follower().await;
        let log = shard_log(&broker, &key()).await.expect("log");
        log.accept_generation(4, Some("broker-c"))
            .await
            .expect("ballot");
        if started {
            let start = log.append_generation_start(4).await.expect("start");
            log.record_generation(4, start).expect("epoch");
        }
        // Broker-c still answers as fencing; broker-a's yes is the majority.
        let ctx = ctx(
            &broker,
            Voters {
                answers: vec![
                    ("broker-a", None),
                    ("broker-c", Some(PreVoteRefusal::LeaderReachable)),
                ],
                ..voters(None)
            },
        );
        let route = ctx.router.snapshot().get(&key()).expect("route").clone();
        assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);

        tokio::time::advance(ctx.elections.defer_for()).await;
        if started {
            assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);
            assert_eq!(log.accepted_generation(), 4, "no ballot");
            continue;
        }
        let (_, elected) = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
            .await
            .expect("never stood");
        assert_eq!(elected.generation, 5);
    }
}

/// **A broker the control plane named leader does not stand against the
/// leader its routes still name** (#1933). The lifecycle takes the ballot
/// before the routes catch up; standing then would raise this copy's ballot
/// past the generation it serves, and it would never count a mark there.
#[tokio::test(start_paused = true)]
async fn a_broker_named_leader_does_not_stand_before_its_routes_catch_up() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some(LOCAL)).await.expect("ballot");
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();

    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);
    assert!(ctx.requester.asked.lock().expect("lock").is_empty());
    assert_eq!(log.accepted_generation(), 4);
    assert!(ctx.elections.get(&key()).is_none());

    // A ballot this process does not remember writing holds it off only so
    // long, as one a standing left before a restart.
    tokio::time::sleep(NAMED_HERE_FOR).await;
    let (_, elected) = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 5);
}

/// **A standing withdrawn here stands again a generation up**: its own
/// ballot does not hold it off as one the control plane named.
#[tokio::test(start_paused = true)]
async fn a_withdrawn_standing_stands_again() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let (_, elected) = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 4);
    ctx.elections.withdraw(&key(), 4);

    let (_, elected) = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 5);
}

/// **A round whose routes moved on while it asked writes no ballot** (#1933):
/// the leader it suspected is no longer the one named.
#[tokio::test(start_paused = true)]
async fn a_round_on_routes_that_moved_on_writes_no_ballot() {
    let (broker, _dir) = follower().await;
    let mut ctx = ctx(&broker, voters(None));
    let stale = ctx.router.snapshot().get(&key()).expect("route").clone();
    ctx.router = router(4);
    let log = shard_log(&broker, &key()).await.expect("log");

    assert_eq!(stand(&ctx, &key(), &stale).await, Round::Failed);
    assert_eq!(log.accepted_generation(), 3);
    assert!(ctx.elections.get(&key()).is_none());
}

/// When, in a round second in the order, the copy's kept set changes.
#[derive(Clone, Copy)]
enum Changes {
    /// While the first pre-vote is out.
    InFirstPreVote,
    /// While the candidate waits its place.
    InPlaceDelay,
    /// While the second pre-vote is out.
    InSecondPreVote,
}

/// The routes name broker-a at 5 while `LOCAL` keeps the set of 3 with its
/// ballot at 3, so it stands at 6. While the round runs, the copy takes a set
/// at 4 that is leaving the one it stood on with other copies: with
/// `with_ballot` as another leader's fence at 4 would (ballot first, then its
/// set), otherwise without a ballot, as a refused learner or replaced copy
/// keeps a fence's set. Either way the round's checks read a set the copy no
/// longer keeps, so it must write no ballot and record nothing, and so never
/// fence.
async fn set_changes_during_the_round(when: Changes, with_ballot: bool) {
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    let changed = {
        let log = log.clone();
        async move {
            if with_ballot {
                log.accept_generation(4, Some("broker-d"))
                    .await
                    .expect("ballot");
            }
            let mut current = set(4);
            current.members[2].node = "broker-d".to_string();
            log.accept_set(ReplicaSet {
                current,
                leaving: Some(set(3)),
                earlier: Vec::new(),
            })
            .await
            .expect("set");
        }
    };
    let mut voters = second_in_order(None);
    match when {
        Changes::InFirstPreVote => voters.on_ask = Mutex::new(Some((0, Box::pin(changed)))),
        // broker-a and broker-c answered the first; this is the third ask.
        Changes::InSecondPreVote => voters.on_ask = Mutex::new(Some((2, Box::pin(changed)))),
        Changes::InPlaceDelay => {
            tokio::spawn(async move {
                tokio::time::sleep(PLACE_DELAY / 2).await;
                changed.await;
            });
        }
    }
    let mut ctx = ctx(&broker, voters);
    ctx.router = router(5);
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();

    assert_eq!(stand(&ctx, &key(), &route).await, Round::Failed);
    let ballot = if with_ballot { 4 } else { 3 };
    assert_eq!(log.accepted_generation(), ballot, "no ballot at 6");
    assert!(ctx.elections.get(&key()).is_none(), "nothing recorded");
    assert_eq!(
        log.replica_set().expect("kept").current.generation,
        4,
        "the set changed mid-round",
    );
    // Only pre-votes went out: `Voters` refuses any other request.
    assert_eq!(ctx.requester.asked.lock().expect("lock").len(), 4);
}

/// **A fence taken mid-round, below the candidate's generation, stops it**
/// (#1970): during the first pre-vote.
#[tokio::test(start_paused = true)]
async fn a_fence_taken_in_the_first_pre_vote_stops_the_standing() {
    set_changes_during_the_round(Changes::InFirstPreVote, true).await;
}

/// **A fence taken mid-round stops it**: while it waits its place.
#[tokio::test(start_paused = true)]
async fn a_fence_taken_in_the_place_delay_stops_the_standing() {
    set_changes_during_the_round(Changes::InPlaceDelay, true).await;
}

/// **A fence taken mid-round stops it**: during the second pre-vote.
#[tokio::test(start_paused = true)]
async fn a_fence_taken_in_the_second_pre_vote_stops_the_standing() {
    set_changes_during_the_round(Changes::InSecondPreVote, true).await;
}

/// **A set kept with no ballot mid-round stops it**: during the first
/// pre-vote.
#[tokio::test(start_paused = true)]
async fn a_set_kept_in_the_first_pre_vote_stops_the_standing() {
    set_changes_during_the_round(Changes::InFirstPreVote, false).await;
}

/// **A set kept with no ballot mid-round stops it**: while it waits its place.
#[tokio::test(start_paused = true)]
async fn a_set_kept_in_the_place_delay_stops_the_standing() {
    set_changes_during_the_round(Changes::InPlaceDelay, false).await;
}

/// **A set kept with no ballot mid-round stops it**: during the second
/// pre-vote.
#[tokio::test(start_paused = true)]
async fn a_set_kept_in_the_second_pre_vote_stops_the_standing() {
    set_changes_during_the_round(Changes::InSecondPreVote, false).await;
}
