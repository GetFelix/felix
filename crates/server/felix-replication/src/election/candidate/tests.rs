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
    router_of(LOCAL, generation)
}

/// broker-a leading at `generation`, as `local` sees it.
fn router_of(local: &str, generation: u64) -> Arc<ShardRouter> {
    router_led(local, "broker-a", generation)
}

/// `leader` leading at `generation`, as `local` sees it.
fn router_led(local: &str, leader: &str, generation: u64) -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        local,
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
            leader.to_string(),
            ["broker-a", LOCAL, "broker-c"]
                .into_iter()
                .filter(|node| *node != leader)
                .map(str::to_string)
                .collect(),
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
    copy_promised_to("broker-a").await
}

/// As [`follower`], with generation 3 promised to `leader`.
async fn copy_promised_to(leader: &str) -> (Arc<Broker>, tempfile::TempDir) {
    let (broker, dir) = registered().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(3, Some(leader))
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

/// A broker with a `Quorum` stream registered and no log for it on disk.
async fn registered() -> (Arc<Broker>, tempfile::TempDir) {
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
    (broker, dir)
}

fn ctx(broker: &Arc<Broker>, voters: Voters) -> Ctx<Voters> {
    ctx_with(broker, voters)
}

fn ctx_with<R>(broker: &Arc<Broker>, requester: R) -> Ctx<R> {
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
        requester: Arc::new(requester),
        broker: Arc::clone(broker),
        router: router(3),
        marks,
        elections,
        suspects,
        held_back: Default::default(),
        deferring: Default::default(),
        shard_after: crate::suspicion::DEFAULT_SHARD_SUSPECT_AFTER,
        quiet: Default::default(),
        stuck: Default::default(),
        stood: Default::default(),
        named_here: Default::default(),
        pre_vote_gate: None,
        leads_after: None,
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

    let elected = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 4);
}

/// **A candidate second in the order stands once its voters still agree**
/// after its wait, having asked each twice.
#[tokio::test(start_paused = true)]
async fn a_candidate_second_in_order_stands_when_its_voters_still_agree() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, second_in_order(None));

    let elected = stand_and_watch(&ctx).await;
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

    let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
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
        let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
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
    let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&on))
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

/// **A shard the control plane moved is stood on once its last set opened**
/// (#2012). A follower's raises through a move (staged at 5, fenced at 6, cut
/// over to broker-a at 7) carry an older set it holds no record of, here one
/// naming no copies, as earlier sets. Batches at 7 raise nothing, so they
/// stay. Once its log holds a record of 7, that fence opened and the
/// candidate fences 7's set alone. Without one it still does not stand.
#[tokio::test(start_paused = true)]
async fn a_moved_shard_is_stood_on_once_its_set_opened() {
    use felix_storage::StorageId;

    let named = |generation: u64, learner: Option<&str>| {
        let mut set = set(generation);
        for (i, member) in set.members.iter_mut().enumerate() {
            member.storage = Some(StorageId::from_bytes([i as u8 + 1; 16]));
            member.learner = Some(member.node.as_str()) == learner;
        }
        set
    };
    for opened in [true, false] {
        let (broker, _dir) = registered().await;
        let log = shard_log(&broker, &key()).await.expect("log");
        log.accept_generation(4, Some("broker-c"))
            .await
            .expect("ballot");
        log.accept_set(ReplicaSet {
            current: set(4),
            leaving: None,
            earlier: Vec::new(),
        })
        .await
        .expect("set");
        let staged = Some("broker-a");
        for (current, leaving) in [
            (named(5, staged), named(4, None)),
            (named(6, staged), named(5, staged)),
            (named(7, None), named(6, staged)),
        ] {
            log.accept_generation(current.generation, Some("broker-a"))
                .await
                .expect("ballot");
            log.accept_set_keeping_earlier(ReplicaSet {
                current,
                leaving: Some(leaving),
                earlier: Vec::new(),
            })
            .await
            .expect("set");
        }
        let kept = log.replica_set().expect("kept");
        assert_eq!(kept.current.generation, 7);
        assert_eq!(kept.earlier, vec![set(4)]);

        let mut ctx = ctx(&broker, voters(None));
        ctx.router = router(7);
        if !opened {
            assert_eq!(
                standing_sets(&ctx, &log, &kept),
                Err("the kept replica set keeps earlier sets"),
                "refused for the earlier sets, not another reason"
            );
            assert_eq!(
                stand(&ctx, &key(), &route_of(&ctx)).await,
                Round::Ineligible
            );
            assert_eq!(log.accepted_generation(), 7, "no ballot");
            continue;
        }
        open_at(&log, 7).await;
        let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
            .await
            .expect("never stood");
        assert_eq!(elected.generation, 8);
        assert_eq!(elected.set, named(7, None));
        assert_eq!(elected.leaving, None, "the move's sets are past");
    }
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
        shard_after: crate::suspicion::DEFAULT_SHARD_SUSPECT_AFTER,
        quiet: Default::default(),
        stuck: Default::default(),
        stood: Default::default(),
        named_here: Default::default(),
        pre_vote_gate: None,
        leads_after: None,
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
async fn stand_and_watch(ctx: &Ctx<Voters>) -> Elected {
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
    // The standing stays until its route is published, which no test router
    // does here, so only the record is waited for.
    tokio::select! {
        round = stand(ctx, &shard, &route) => panic!("ended {round:?} before it was recorded"),
        elected = watched => elected,
    }
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

    let elected = stand_and_watch(&ctx).await;
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
    let elected = stand_and_watch(&ctx(&broker, voters(None))).await;
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

/// `follower()` after this broker stood at generation 4 and opened there:
/// its ballot names it, and its log holds the start record.
async fn elected_before_restart() -> (Arc<Broker>, tempfile::TempDir) {
    let (broker, dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some(LOCAL)).await.expect("ballot");
    open_at(&log, 4).await;
    (broker, dir)
}

async fn open_at(log: &StreamLog, generation: u64) {
    let end = log.tail_offset().await.expect("end");
    log.record_generation(generation, end).expect("stint");
    log.append_generation_start(generation)
        .await
        .expect("start record");
}

fn route_of(ctx: &Ctx<Voters>) -> Route {
    ctx.router.snapshot().get(&key()).expect("route").clone()
}

/// **An elected leader that restarts rebuilds its standing from its log**:
/// the ballot names it at 4, the log holds 4's start record, and the route
/// still names broker-a at 3. It is put back on the set it keeps.
#[tokio::test(start_paused = true)]
async fn a_standing_that_opened_is_rebuilt_from_the_log() {
    let (broker, _dir) = elected_before_restart().await;
    let ctx = ctx(&broker, voters(None));

    let (_, elected) = restorable(&ctx, &key(), &route_of(&ctx))
        .await
        .expect("known")
        .expect("restorable");
    assert_eq!(elected.leader, LOCAL);
    assert_eq!(elected.generation, 4);
    assert_eq!(elected.set, set(3));
    assert_eq!(elected.leaving, None);
    assert_eq!(elected.incarnation, 0);

    // A collision: the control plane named broker-a at 4 too.
    let mut collided = route_of(&ctx);
    collided.generation = 4;
    assert!(
        restorable(&ctx, &key(), &collided)
            .await
            .expect("known")
            .is_some()
    );
}

/// **Each condition alone keeps a standing from being rebuilt.**
#[tokio::test(start_paused = true)]
async fn each_condition_alone_keeps_a_standing_from_being_rebuilt() {
    // The ballot names another candidate at 4.
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some("broker-c"))
        .await
        .expect("ballot");
    open_at(&log, 4).await;
    let c = ctx(&broker, voters(None));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );

    // The route names this broker at 4: the control plane adopted it.
    let (broker, _dir) = elected_before_restart().await;
    let c = ctx(&broker, voters(None));
    let mut adopted = route_of(&c);
    adopted.generation = 4;
    adopted.leader = node(LOCAL, 7002);
    adopted.replicas = vec![node("broker-a", 7001), node("broker-c", 7003)];
    assert!(
        restorable(&c, &key(), &adopted)
            .await
            .expect("known")
            .is_none()
    );

    // The route is past 4.
    let mut newer = route_of(&c);
    newer.generation = 5;
    assert!(
        restorable(&c, &key(), &newer)
            .await
            .expect("known")
            .is_none()
    );

    // No start record at 4: the standing never opened.
    let (broker, _dir) = follower().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.accept_generation(4, Some(LOCAL)).await.expect("ballot");
    let c = ctx(&broker, voters(None));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );

    // A change pending on the kept set.
    let (broker, _dir) = elected_before_restart().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    let mut leaving = set(3);
    leaving.members[2].node = "broker-d".into();
    log.accept_set(ReplicaSet {
        current: set(4),
        leaving: Some(leaving),
        earlier: Vec::new(),
    })
    .await
    .expect("pending set");
    let c = ctx(&broker, voters(None));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );

    // This copy is marked wiped in the set it keeps.
    let (broker, _dir) = elected_before_restart().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    let mut wiped = set(4);
    wiped.members[1].wiped = true;
    log.accept_set(ReplicaSet {
        current: wiped,
        leaving: None,
        earlier: Vec::new(),
    })
    .await
    .expect("wiped");
    let c = ctx(&broker, voters(None));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );

    // The route names another incarnation than the catalog's: looked at
    // again once the catalog holds it.
    let (broker, _dir) = elected_before_restart().await;
    let c = ctx(&broker, voters(None));
    let mut recreated = route_of(&c);
    recreated.incarnation = 7;
    assert!(restorable(&c, &key(), &recreated).await.is_err());

    // A standing at 4 is already in memory.
    let (broker, _dir) = elected_before_restart().await;
    let c = ctx(&broker, voters(None));
    let mut known = set(3);
    known.generation = 4;
    assert!(c.elections.record(
        key(),
        Elected {
            leader: LOCAL.into(),
            generation: 4,
            set: known,
            leaving: None,
            incarnation: 0,
        }
    ));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );

    // This copy is a learner in the set it keeps.
    let (broker, _dir) = elected_before_restart().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    let mut learner = set(4);
    learner.members[1].learner = true;
    log.accept_set(ReplicaSet {
        current: learner,
        leaving: None,
        earlier: Vec::new(),
    })
    .await
    .expect("learner");
    let c = ctx(&broker, voters(None));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );

    // The generation was given up.
    let (broker, _dir) = elected_before_restart().await;
    let log = shard_log(&broker, &key()).await.expect("log");
    log.give_up_generation().await.expect("give up");
    let c = ctx(&broker, voters(None));
    assert!(
        restorable(&c, &key(), &route_of(&c))
            .await
            .expect("known")
            .is_none()
    );
}

/// **A rebuilt standing that does not open is withdrawn** when no fence at
/// its generation is taken since the start, as when a majority moved on and
/// refuses it, even with nothing shipping to this broker. The start record
/// already in the log does not count as opening. Not before the defer bound
/// though, even once its route is published: it is not put back, so a fence
/// queued behind others after the restart gets that long. The ballot stays,
/// and is this broker's own, so nothing holds off a later standing.
#[tokio::test(start_paused = true)]
async fn a_rebuilt_standing_that_does_not_open_is_withdrawn() {
    let (broker, _dir) = elected_before_restart().await;
    let mut ctx = ctx(&broker, voters(None));
    let (log, elected) = restorable(&ctx, &key(), &route_of(&ctx))
        .await
        .expect("known")
        .expect("restorable");
    // The routing feed names this broker at the rebuilt generation.
    ctx.router = router_led(LOCAL, LOCAL, 4);

    let started = Instant::now();
    assert_eq!(restore(&ctx, &key(), &log, elected).await, Round::Failed);
    assert!(started.elapsed() >= ctx.elections.defer_for());
    assert!(started.elapsed() <= ctx.elections.defer_for() + OPEN_POLL * 2);
    assert!(ctx.elections.get(&key()).is_none());
    assert_eq!(log.accepted_generation(), 4);
    assert_eq!(log.accepted_leader().as_deref(), Some(LOCAL));
    assert!(!ctx.named_here(&key(), 4));
}

/// **A rebuilt standing opens once a fence at its generation is taken
/// again** since the start, as the driver records it.
#[tokio::test(start_paused = true)]
async fn a_rebuilt_standing_opens_on_a_fence_taken_since_the_start() {
    let (broker, _dir) = elected_before_restart().await;
    let ctx = ctx(&broker, voters(None));
    let (log, elected) = restorable(&ctx, &key(), &route_of(&ctx))
        .await
        .expect("known")
        .expect("restorable");
    let driver = async {
        while ctx.elections.get(&key()).is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _fencing = ctx.elections.fencing(&key(), 4);
        tokio::time::sleep(Duration::from_millis(100)).await;
        ctx.elections.opened(&key(), 4);
    };
    let shard = key();
    let (round, ()) = tokio::join!(restore(&ctx, &shard, &log, elected), driver);
    assert_eq!(round, Round::Opened);
    assert_eq!(
        ctx.elections.get(&key()).map(|elected| elected.generation),
        Some(4)
    );
}

/// **The candidate task rebuilds a standing when it starts**, before any
/// leader is suspected, so the routes name this broker leader at 4 again.
#[tokio::test(start_paused = true)]
async fn the_candidate_task_rebuilds_standings_when_it_starts() {
    let (broker, _dir) = elected_before_restart().await;
    let ctx = ctx(&broker, voters(None));
    ctx.suspects.publish(BTreeSet::new());
    let ctx = Arc::new(ctx);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(Arc::clone(&ctx), shutdown.clone()));

    let rebuilt = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(elected) = ctx.elections.get(&key()) {
                return elected;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rebuilt within a tick");
    shutdown.cancel();
    task.await.expect("candidate task");
    assert_eq!(rebuilt.leader, LOCAL);
    assert_eq!(rebuilt.generation, 4);
}

/// **A broker that may not lead yet neither restores nor stands**: after a
/// start from the saved view its peers suspect it until it registers, so a
/// standing it won would be taken back. Once allowed, it rebuilds the
/// standing at 4.
#[tokio::test(start_paused = true)]
async fn a_broker_that_may_not_lead_yet_neither_restores_nor_stands() {
    let (broker, _dir) = elected_before_restart().await;
    let leads_after = CancellationToken::new();
    let ctx = Arc::new(Ctx {
        leads_after: Some(leads_after.clone()),
        ..ctx(&broker, voters(None))
    });
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(Arc::clone(&ctx), shutdown.clone()));
    let log = shard_log(&broker, &key()).await.expect("log");
    let accepted = log.accepted_generation();

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert!(
        ctx.elections.get(&key()).is_none(),
        "restored before it may lead"
    );
    assert_eq!(
        log.accepted_generation(),
        accepted,
        "stood before it may lead"
    );

    leads_after.cancel();
    let rebuilt = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Some(elected) = ctx.elections.get(&key()) {
                return elected;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rebuilt once it may lead");
    shutdown.cancel();
    task.await.expect("candidate task");
    assert_eq!(rebuilt.generation, 4);
}

/// **A shard whose stream the catalog does not hold yet is looked at
/// later**, not skipped for good.
#[tokio::test(start_paused = true)]
async fn a_stream_the_catalog_does_not_hold_yet_is_looked_at_later() {
    let (broker, _dir) = elected_before_restart().await;
    let c = ctx(&broker, voters(None));
    let mut other = key();
    other.stream = "not-yet".into();
    assert!(restorable(&c, &other, &route_of(&c)).await.is_err());
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
    let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
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

    let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
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
        let elected = tokio::time::timeout(Duration::from_secs(30), stand_and_watch(&ctx))
            .await
            .expect("never stood");
        assert_eq!(elected.generation, 5);
    }
}

/// Answers pre-votes from real replica handlers, one per broker.
struct Cluster {
    handlers: HashMap<String, crate::replica::ReplicaHandler>,
}

impl PeerRequester for Cluster {
    async fn request(
        &self,
        node_id: &str,
        _addr: SocketAddr,
        message: InternalMessage,
    ) -> Result<InternalMessage, PeerError> {
        let InternalMessage::PreVote(request) = message else {
            panic!("only pre-votes are asked here: {message:?}");
        };
        let handler = self.handlers.get(node_id).expect("a broker of the set");
        Ok(handler.pre_vote(request).await)
    }

    fn offers_election(&self) -> bool {
        true
    }
}

/// `broker`'s copy taking `leader`'s ballot at `generation`.
async fn ballot(broker: &Broker, generation: u64, leader: &str) {
    let log = shard_log(broker, &key()).await.expect("log");
    log.accept_generation(generation, Some(leader))
        .await
        .expect("ballot");
}

async fn start(broker: &Broker, generation: u64) {
    let log = shard_log(broker, &key()).await.expect("log");
    let start = log
        .append_generation_start(generation)
        .await
        .expect("start");
    log.record_generation(generation, start).expect("epoch");
}

/// **A shard stuck on a live leader heals by election with the control plane
/// down** (#1914, the model's `FelixShardCpDown` trace). broker-a restarted
/// fencing as the control plane's leader at 3, but `LOCAL` and broker-c
/// promised 4 to broker-c's standing, which it withdrew. Every broker is
/// alive, so none is suspected, and broker-a's fence is refused for good.
/// Once the shard bound passes without a word from broker-a, `LOCAL` stands:
/// broker-a refuses as the leader it is, broker-c grants, and that is a
/// majority. Not before the bound.
#[tokio::test(start_paused = true)]
async fn a_shard_stuck_on_a_live_leader_heals_by_election() {
    let (a, _a_dir) = follower().await;
    start(&a, 3).await;
    let (b, _b_dir) = follower().await;
    ballot(&b, 4, "broker-c").await;
    let (c, _c_dir) = follower().await;
    ballot(&c, 4, "broker-c").await;
    let handlers = [("broker-a", &a), ("broker-c", &c)]
        .into_iter()
        .map(|(node, broker)| {
            let handler =
                crate::replica::ReplicaHandler::new(Arc::clone(broker), router_of(node, 3));
            (node.to_string(), handler)
        })
        .collect();
    let ctx = ctx_with(&b, Cluster { handlers });
    ctx.suspects.publish(BTreeSet::new());
    let ctx = Arc::new(ctx);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(Arc::clone(&ctx), shutdown.clone()));
    let log = shard_log(&b, &key()).await.expect("log");

    tokio::time::sleep(ctx.shard_after - SHARD_SCAN * 2).await;
    assert_eq!(log.accepted_generation(), 4, "stood before the bound");

    let stood = async {
        while log.accepted_generation() < 5 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(SHARD_SCAN * 4, stood)
        .await
        .expect("never stood");
    assert_eq!(log.accepted_generation(), 5);
    assert_eq!(log.accepted_leader().as_deref(), Some(LOCAL));
    shutdown.cancel();
    task.await.expect("run");
}

/// **A leader whose own ballot is above its route stands for itself** with
/// the control plane down (#1914, the model's `SelfSuspicion`). The control
/// plane named `LOCAL` at 2; it was elected at 3 and opened, then restarted.
/// The standing at 3 is rebuilt first, but nothing here fences, so it never
/// opens and is withdrawn. The driver's open at 2 is refused for its own
/// ballot, and its followers took its fence and start record at 3, so
/// neither suspects anything. Once the shard bound passes, `LOCAL` stands at
/// 4 and both followers grant. Not before the bound.
#[tokio::test(start_paused = true)]
async fn a_leader_refused_for_its_own_ballot_stands_for_itself() {
    let mut copies = Vec::new();
    for _ in 0..3 {
        let (broker, dir) = copy_promised_to(LOCAL).await;
        start(&broker, 3).await;
        copies.push((broker, dir));
    }
    let handlers = [("broker-a", &copies[0].0), ("broker-c", &copies[2].0)]
        .into_iter()
        .map(|(node, broker)| {
            let handler =
                crate::replica::ReplicaHandler::new(Arc::clone(broker), router_led(node, LOCAL, 2));
            (node.to_string(), handler)
        })
        .collect();
    let b = &copies[1].0;
    let mut ctx = ctx_with(b, Cluster { handlers });
    ctx.router = router_led(LOCAL, LOCAL, 2);
    ctx.suspects.publish(BTreeSet::new());
    let ctx = Arc::new(ctx);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(Arc::clone(&ctx), shutdown.clone()));
    let log = shard_log(b, &key()).await.expect("log");

    tokio::time::sleep(TICK * 4).await;
    let rebuilt = ctx.elections.get(&key()).expect("rebuilt");
    assert_eq!((rebuilt.leader.as_str(), rebuilt.generation), (LOCAL, 3));
    tokio::time::sleep(ctx.elections.defer_for() + TICK).await;
    assert!(
        ctx.elections.get(&key()).is_none(),
        "the rebuilt standing stayed"
    );

    tokio::time::sleep(ctx.shard_after - SHARD_SCAN * 2).await;
    assert_eq!(log.accepted_generation(), 3, "stood before the bound");

    let stood = async {
        while log.accepted_generation() < 4 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(SHARD_SCAN * 4, stood)
        .await
        .expect("never stood");
    assert_eq!(log.accepted_leader().as_deref(), Some(LOCAL));
    let elected = ctx.elections.get(&key()).expect("recorded");
    assert_eq!((elected.leader.as_str(), elected.generation), (LOCAL, 4));
    shutdown.cancel();
    task.await.expect("run");
}

/// **A leader is not stuck on a ballot its election holds**, while the route
/// naming that election is yet to be published, nor on a ballot it promised
/// another broker, which that broker's fence decides.
#[tokio::test(start_paused = true)]
async fn a_leader_is_stuck_only_on_its_own_unelected_ballot() {
    let (broker, _dir) = copy_promised_to(LOCAL).await;
    let mut ctx = ctx(&broker, voters(None));
    ctx.router = router_led(LOCAL, LOCAL, 2);
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let log = shard_log(&broker, &key()).await.expect("log");
    ctx.elections.record(
        key(),
        Elected {
            leader: LOCAL.to_string(),
            generation: 3,
            set: set(3),
            leaving: None,
            incarnation: 0,
        },
    );
    let stuck = || ctx.shard_stuck(&key(), &route, &log, Instant::now());
    assert!(!stuck());
    tokio::time::advance(ctx.shard_after * 2).await;
    assert!(!stuck(), "its election holds the ballot");

    ctx.elections.withdraw(&key(), 3);
    assert!(!stuck());
    tokio::time::advance(ctx.shard_after).await;
    assert!(stuck());

    let (other, _other_dir) = follower().await;
    let promised = shard_log(&other, &key()).await.expect("log");
    tokio::time::advance(ctx.shard_after * 2).await;
    assert!(
        !ctx.shard_stuck(&key(), &route, &promised, Instant::now()),
        "promised to another broker"
    );
}

/// **A ballot the control plane named this broker at holds off self-suspicion
/// for [`NAMED_HERE_FOR`]**, however short the shard bound: the routes may
/// not have caught up yet. A ballot a standing here wrote does not.
#[tokio::test(start_paused = true)]
async fn a_leader_named_at_its_ballot_waits_for_the_routes_before_it_is_stuck() {
    let (broker, _dir) = copy_promised_to(LOCAL).await;
    let mut ctx = ctx(&broker, voters(None));
    ctx.router = router_led(LOCAL, LOCAL, 2);
    ctx.shard_after = Duration::from_secs(1);
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let log = shard_log(&broker, &key()).await.expect("log");
    let stuck = || ctx.shard_stuck(&key(), &route, &log, Instant::now());
    assert!(!stuck());
    tokio::time::advance(ctx.shard_after * 2).await;
    assert!(!stuck(), "named here, the routes may yet catch up");
    tokio::time::advance(NAMED_HERE_FOR).await;
    assert!(stuck());

    ctx.quiet.lock().expect("quiet lock").clear();
    ctx.named_here.lock().expect("named_here lock").clear();
    ctx.stood.lock().expect("stood lock").insert(key(), 3);
    assert!(!stuck());
    tokio::time::advance(ctx.shard_after).await;
    assert!(stuck(), "a ballot a standing here wrote");
}

/// **An idle shard is not suspected** however long its leader is silent:
/// its generation started here. Nothing is shipped on an idle shard, so
/// silence alone would depose every idle leader.
#[tokio::test(start_paused = true)]
async fn an_idle_shard_is_not_suspected() {
    let (broker, _dir) = follower().await;
    start(&broker, 3).await;
    let ctx = ctx(&broker, voters(None));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let log = shard_log(&broker, &key()).await.expect("log");
    assert!(!ctx.shard_stuck(&key(), &route, &log, Instant::now()));
    tokio::time::advance(ctx.shard_after * 10).await;
    assert!(!ctx.shard_stuck(&key(), &route, &log, Instant::now()));

    ctx.suspects.publish(BTreeSet::new());
    assert_eq!(stand(&ctx, &key(), &route).await, Round::Ineligible);
    assert!(ctx.requester.asked.lock().expect("lock").is_empty());
}

/// **A leader heard within the bound is not suspected**, however long its
/// generation takes to start here: a catch-up under load, or a leader whose
/// batches come with gaps shorter than the bound. Once it goes quiet for the
/// bound, it is.
#[tokio::test(start_paused = true)]
async fn a_leader_heard_within_the_bound_is_not_suspected() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let log = shard_log(&broker, &key()).await.expect("log");
    for _ in 0..10 {
        ctx.elections.heard_leader(&key());
        tokio::time::advance(ctx.shard_after * 9 / 10).await;
        assert!(!ctx.shard_stuck(&key(), &route, &log, Instant::now()));
    }
    tokio::time::advance(ctx.shard_after / 10).await;
    assert!(ctx.shard_stuck(&key(), &route, &log, Instant::now()));
}

/// **The control plane renaming the shard restarts the clock**, so with the
/// control plane up its backstop acts first and the two do not both take a
/// generation. A copy behind the route, which its leader has yet to fence,
/// is never stuck.
#[tokio::test(start_paused = true)]
async fn a_rename_restarts_the_clock() {
    let (broker, _dir) = follower().await;
    ballot(&broker, 4, "broker-c").await;
    let ctx = ctx(&broker, voters(None));
    let at_3 = ctx.router.snapshot().get(&key()).expect("route").clone();
    let at_4 = router(4).snapshot().get(&key()).expect("route").clone();
    let log = shard_log(&broker, &key()).await.expect("log");
    let stuck = |route: &Route| ctx.shard_stuck(&key(), route, &log, Instant::now());

    assert!(!stuck(&at_3));
    tokio::time::advance(ctx.shard_after * 7 / 10).await;
    assert!(!stuck(&at_4), "renamed");
    tokio::time::advance(ctx.shard_after * 4 / 10).await;
    assert!(!stuck(&at_4), "the clock went on across the rename");
    tokio::time::advance(ctx.shard_after * 6 / 10).await;
    assert!(stuck(&at_4));

    let at_5 = router(5).snapshot().get(&key()).expect("route").clone();
    tokio::time::advance(ctx.shard_after * 2).await;
    assert!(!stuck(&at_5), "behind the route");
}

/// Publish a route naming `LOCAL` leader at `generation` on `router`.
fn publish_led_here(router: &ShardRouter, generation: u64) {
    let nodes: HashMap<String, NodeRef> = [("broker-a", 7001), (LOCAL, 7002), ("broker-c", 7003)]
        .into_iter()
        .map(|(id, port)| (id.to_string(), node(id, port)))
        .collect();
    let table = RoutingTable::build(
        [(
            key(),
            LOCAL.to_string(),
            vec!["broker-a".to_string(), "broker-c".to_string()],
            generation,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
}

/// **The open bound runs from the route's publication, not the ballot**
/// (#1914). The driver fences only published routes, and a routing feed
/// waiting on a paused control plane held them back for seconds, so every
/// standing was withdrawn before its fence began and the next went a
/// generation higher. Once published, the standing still gives up after
/// [`OPEN_WITHIN`].
#[tokio::test(start_paused = true)]
async fn a_standing_gets_its_open_bound_from_the_routes_publication() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();
    let publishing = async {
        let generation = loop {
            if let Some(elected) = ctx.elections.get(&key()) {
                break elected.generation;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        tokio::time::sleep(OPEN_WITHIN * 3).await;
        let kept = ctx.elections.get(&key()).is_some();
        publish_led_here(&ctx.router, generation);
        let published = Instant::now();
        (kept, published)
    };
    let shard = key();
    let (round, (kept, published)) = tokio::join!(stand(&ctx, &shard, &route), publishing);
    assert!(kept, "withdrawn before its route was published");
    assert_eq!(round, Round::Failed);
    assert!(ctx.elections.get(&key()).is_none());
    let waited = Instant::now().saturating_duration_since(published);
    assert!(
        waited >= OPEN_WITHIN,
        "withdrawn {waited:?} after publication"
    );
    assert!(
        waited < OPEN_WITHIN * 2,
        "kept {waited:?} after publication"
    );
}

fn shard_at(generation: u64) -> felix_wire::internal::ShardRef {
    felix_wire::internal::ShardRef {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        generation,
        incarnation: None,
    }
}

fn fence_at(generation: u64, candidate: bool) -> felix_wire::internal::Fence {
    felix_wire::internal::Fence {
        correlation_id: 1,
        shard: shard_at(generation),
        log: ReplicaLog::Stream,
        election: Some(felix_wire::internal::FenceElection {
            candidate,
            set: crate::promotion::joint::view(&set(4)),
            leaving: None,
            earlier: Vec::new(),
        }),
    }
}

/// **An elected leader the control plane collided with fences again one
/// generation up** (#1914, the model's `FelixShardCpDownDegraded` trace).
/// `LOCAL` was elected at 4 and opened, with broker-c's vote. The control
/// plane then named broker-a at 4 and stopped. broker-a's copy promised 4 to
/// itself and its route names it leader, so it refuses `LOCAL`'s batches for
/// good, and per-shard suspicion never fires: broker-a leads its route and
/// broker-c holds `LOCAL`'s start. Once the collision has stood for the
/// shard bound with that copy still refusing, `LOCAL` takes a ballot at 5
/// with no pre-vote and records the standing. broker-c, which hears it,
/// and broker-a both take its fence there. Not before the bound.
#[tokio::test(start_paused = true)]
async fn a_leader_the_control_plane_collided_with_fences_again_one_generation_up() {
    let (a, _a_dir) = follower().await;
    ballot(&a, 4, "broker-a").await;
    let (b, _b_dir) = follower().await;
    let (c, _c_dir) = follower().await;
    for broker in [&b, &c] {
        ballot(broker, 4, LOCAL).await;
        let log = shard_log(broker, &key()).await.expect("log");
        log.accept_set(ReplicaSet {
            current: set(4),
            leaving: None,
            earlier: Vec::new(),
        })
        .await
        .expect("set");
    }
    start(&b, 4).await;
    // Each broker's own view: the control plane's route names broker-a at 4.
    let a_replica = crate::replica::ReplicaHandler::new(Arc::clone(&a), router_of("broker-a", 4));
    let c_replica = crate::replica::ReplicaHandler::new(Arc::clone(&c), router_of("broker-c", 4));
    let mut ctx = ctx_with(
        &b,
        Cluster {
            handlers: HashMap::new(),
        },
    );
    ctx.router = router_led(LOCAL, LOCAL, 4);
    ctx.suspects.publish(BTreeSet::new());
    let ctx = Arc::new(ctx);
    let incarnation = crate::incarnation::of(&b, &key());
    ctx.elections.record(
        key(),
        Elected {
            leader: LOCAL.to_string(),
            generation: 4,
            set: set(4),
            leaving: None,
            incarnation,
        },
    );
    ctx.elections.opened(&key(), 4);
    let named = crate::election::Named {
        generation: 4,
        incarnation,
        leader: "broker-a".to_string(),
    };
    assert_eq!(ctx.elections.overlay(|_| Some(named.clone())).len(), 1);

    // What `LOCAL`'s driver hears from broker-a for each batch.
    let answer = a_replica
        .apply(
            Some(LOCAL),
            felix_wire::internal::ReplicateRecords {
                times: None,
                correlation_id: 1,
                shard: shard_at(4),
                first_offset: 0,
                checksum: felix_wire::internal::batch_checksum(
                    &[bytes::Bytes::from_static(b"x")],
                    &[],
                    &[],
                    &[],
                ),
                payloads: vec![bytes::Bytes::from_static(b"x")],
                marks: Vec::new(),
                commit_offset: None,
                generations: None,
                publishers: Vec::new(),
                headers: None,
                set_generation: Some(4),
            },
            felix_broker::LogKind::Stream,
        )
        .await;
    let InternalMessage::ReplicateError(refused) = answer else {
        panic!("broker-a stored a batch from the leader it collided with: {answer:?}");
    };
    assert!(crate::replica::refuses_leader(
        refused.code,
        &refused.detail,
        4
    ));

    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(Arc::clone(&ctx), shutdown.clone()));
    let log = shard_log(&b, &key()).await.expect("log");
    let mut waited = Duration::ZERO;
    while waited + SHARD_SCAN < ctx.shard_after {
        ctx.elections.note_refused_copy(&key(), 4);
        tokio::time::sleep(SHARD_SCAN).await;
        waited += SHARD_SCAN;
        assert_eq!(
            log.accepted_generation(),
            4,
            "fenced again before the bound"
        );
    }
    let stood = async {
        while log.accepted_generation() < 5 {
            ctx.elections.note_refused_copy(&key(), 4);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::time::timeout(SHARD_SCAN * 4, stood)
        .await
        .expect("never fenced again");
    assert_eq!(log.accepted_leader().as_deref(), Some(LOCAL));
    let elected = ctx.elections.get(&key()).expect("standing");
    assert_eq!((elected.leader.as_str(), elected.generation), (LOCAL, 5));

    // The driver's fence at 5. broker-c hears `LOCAL` and still takes it.
    let ok = |answer: InternalMessage| {
        assert!(matches!(answer, InternalMessage::FenceOk(_)), "{answer:?}");
    };
    ok(c_replica.fence(Some(LOCAL), fence_at(4, false)).await);
    ok(c_replica.fence(Some(LOCAL), fence_at(5, true)).await);
    ok(a_replica.fence(Some(LOCAL), fence_at(5, true)).await);
    let a_log = shard_log(&a, &key()).await.expect("log");
    assert_eq!(a_log.accepted_generation(), 5);
    assert_eq!(a_log.accepted_leader().as_deref(), Some(LOCAL));

    // Not open yet, and no fence running: a standing would be given up
    // after `OPEN_WITHIN`, but this one already left the generation it
    // served, so it waits out the defer bound.
    publish_led_here(&ctx.router, 5);
    tokio::time::sleep(OPEN_WITHIN * 3).await;
    let elected = ctx
        .elections
        .get(&key())
        .expect("withdrawn at the standing's bound");
    assert_eq!(elected.generation, 5);
    tokio::time::sleep(ctx.elections.defer_for()).await;
    assert!(ctx.elections.get(&key()).is_none(), "never withdrawn");

    shutdown.cancel();
    task.await.expect("run");
}

/// **No collision, no second fence**: an elected leader a copy refuses,
/// with the control plane naming nobody else at its generation, is left to
/// the rules that already cover it.
#[tokio::test(start_paused = true)]
async fn a_refusing_copy_alone_does_not_fence_again() {
    let (b, _b_dir) = follower().await;
    ballot(&b, 4, LOCAL).await;
    start(&b, 4).await;
    let mut ctx = ctx_with(
        &b,
        Cluster {
            handlers: HashMap::new(),
        },
    );
    ctx.router = router_led(LOCAL, LOCAL, 4);
    ctx.suspects.publish(BTreeSet::new());
    let ctx = Arc::new(ctx);
    let incarnation = crate::incarnation::of(&b, &key());
    ctx.elections.record(
        key(),
        Elected {
            leader: LOCAL.to_string(),
            generation: 4,
            set: set(4),
            leaving: None,
            incarnation,
        },
    );
    ctx.elections.opened(&key(), 4);
    // The control plane's route is older: an ordinary elected leadership.
    let named = crate::election::Named {
        generation: 3,
        incarnation,
        leader: "broker-a".to_string(),
    };
    ctx.elections.overlay(|_| Some(named.clone()));
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(run(Arc::clone(&ctx), shutdown.clone()));
    for _ in 0..(ctx.shard_after.as_secs() / SHARD_SCAN.as_secs()) * 2 {
        ctx.elections.note_refused_copy(&key(), 4);
        tokio::time::sleep(SHARD_SCAN).await;
    }
    let log = shard_log(&b, &key()).await.expect("log");
    assert_eq!(log.accepted_generation(), 4);
    shutdown.cancel();
    task.await.expect("run");
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
    let elected = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 5);
}

/// **A standing withdrawn here stands again a generation up**: its own
/// ballot does not hold it off as one the control plane named.
#[tokio::test(start_paused = true)]
async fn a_withdrawn_standing_stands_again() {
    let (broker, _dir) = follower().await;
    let ctx = ctx(&broker, voters(None));
    let elected = stand_and_watch(&ctx).await;
    assert_eq!(elected.generation, 4);
    ctx.elections.withdraw(&key(), 4);

    let elected = stand_and_watch(&ctx).await;
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
enum ChangesIn {
    /// While the first pre-vote is out.
    FirstPreVote,
    /// While the candidate waits its place.
    PlaceDelay,
    /// While the second pre-vote is out.
    SecondPreVote,
}

/// The routes name broker-a at 5 while `LOCAL` keeps the set of 3 with its
/// ballot at 3, so it stands at 6. While the round runs, the copy takes a set
/// at 4 that is leaving the one it stood on with other copies: with
/// `with_ballot` as another leader's fence at 4 would (ballot first, then its
/// set), otherwise without a ballot, as a refused learner or replaced copy
/// keeps a fence's set. Either way the round's checks read a set the copy no
/// longer keeps, so it must write no ballot and record nothing, and so never
/// fence.
async fn set_changes_during_the_round(when: ChangesIn, with_ballot: bool) {
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
        ChangesIn::FirstPreVote => voters.on_ask = Mutex::new(Some((0, Box::pin(changed)))),
        // broker-a and broker-c answered the first; this is the third ask.
        ChangesIn::SecondPreVote => voters.on_ask = Mutex::new(Some((2, Box::pin(changed)))),
        ChangesIn::PlaceDelay => {
            tokio::spawn(async move {
                tokio::time::sleep(PLACE_DELAY / 2).await;
                changed.await;
            });
        }
    }
    let mut ctx = ctx(&broker, voters);
    ctx.router = router(5);
    let route = ctx.router.snapshot().get(&key()).expect("route").clone();

    let round = stand(&ctx, &key(), &route).await;
    assert!(
        matches!(round, Round::Failed | Round::SetMoved),
        "{round:?}"
    );
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
    set_changes_during_the_round(ChangesIn::FirstPreVote, true).await;
}

/// **A fence taken mid-round stops it**: while it waits its place.
#[tokio::test(start_paused = true)]
async fn a_fence_taken_in_the_place_delay_stops_the_standing() {
    set_changes_during_the_round(ChangesIn::PlaceDelay, true).await;
}

/// **A fence taken mid-round stops it**: during the second pre-vote.
#[tokio::test(start_paused = true)]
async fn a_fence_taken_in_the_second_pre_vote_stops_the_standing() {
    set_changes_during_the_round(ChangesIn::SecondPreVote, true).await;
}

/// **A set kept with no ballot mid-round stops it**: during the first
/// pre-vote.
#[tokio::test(start_paused = true)]
async fn a_set_kept_in_the_first_pre_vote_stops_the_standing() {
    set_changes_during_the_round(ChangesIn::FirstPreVote, false).await;
}

/// **A set kept with no ballot mid-round stops it**: while it waits its place.
#[tokio::test(start_paused = true)]
async fn a_set_kept_in_the_place_delay_stops_the_standing() {
    set_changes_during_the_round(ChangesIn::PlaceDelay, false).await;
}

/// **A set kept with no ballot mid-round stops it**: during the second
/// pre-vote.
#[tokio::test(start_paused = true)]
async fn a_set_kept_in_the_second_pre_vote_stops_the_standing() {
    set_changes_during_the_round(ChangesIn::SecondPreVote, false).await;
}
