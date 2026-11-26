//! A cluster's RF 1 shard after a relaxed crash: it plans and stops its
//! copy, asks the control plane to record the next epoch, and starts from
//! the seed that record holds. The control plane here is a fake that keeps
//! the same record the real one does and replays a write it already holds.

use std::sync::atomic::{AtomicUsize, Ordering};

use felix_broker::Broker;
use felix_common::fleet::{self, FleetGate};
use felix_storage::raft_copy::{group_epoch, highest_reserved};
use felix_storage::run_state;
use reqwest::StatusCode;

use super::super::alone_cluster::AloneWriter;
use super::super::standalone::tests::members;
use super::super::standalone::tests::publish;
use super::super::standalone::tests::reform::{
    PERIODIC, Written, begin, broker_in, copy_tree, payloads, put_back, write, written,
};
use super::super::standalone::{LOCAL_MEMBER, fleet_of_one};
use super::*;
use crate::shards::watch::{PlacedEpoch, RaftBases, RaftEpochBase, RaftFloor, RaftVoters};
use felix_replication::engine::Engine;

/// What the control plane says to a re-form request.
#[derive(Clone, Copy, PartialEq)]
enum Say {
    /// Record it, or replay the record, and answer.
    Take,
    /// Record it, then lose the answer: the broker never hears.
    TakeThenSilence,
    /// Refuse it with a conflict, though the record would take it.
    Conflict,
}

#[derive(Default, Clone)]
struct Record {
    epoch: u64,
    bases: Vec<RaftEpochBase>,
    seed: Option<RaftVoters>,
    template: Option<ShardAssignment>,
}

struct FakeControlPlane {
    say: parking_lot::Mutex<Say>,
    record: parking_lot::Mutex<Record>,
    requests: parking_lot::Mutex<Vec<serde_json::Value>>,
    bumps: AtomicUsize,
}

impl FakeControlPlane {
    fn new(say: Say) -> Arc<Self> {
        Arc::new(Self {
            say: parking_lot::Mutex::new(say),
            record: Default::default(),
            requests: Default::default(),
            bumps: AtomicUsize::new(0),
        })
    }

    fn say(&self, say: Say) {
        *self.say.lock() = say;
    }

    fn requests(&self) -> Vec<serde_json::Value> {
        self.requests.lock().clone()
    }

    fn epoch(&self) -> u64 {
        self.record.lock().epoch
    }

    /// The assignment the watch feeds for what `assignment` says, with
    /// the record's epoch, bases and seed on it.
    fn apply(&self, assignment: &mut ShardAssignment) {
        let mut record = self.record.lock();
        record.template = Some(assignment.clone());
        Self::stamp(&record, assignment);
    }

    fn stamp(record: &Record, assignment: &mut ShardAssignment) {
        assignment.leader = LOCAL_MEMBER.to_string();
        assignment.replicas = vec![LOCAL_MEMBER.to_string()];
        assignment.placed = Some(PlacedEpoch {
            epoch: record.epoch,
            brokers: [LOCAL_MEMBER.to_string()].into(),
        });
        assignment.raft_floor = None::<RaftFloor>;
        if record.epoch > 0 {
            assignment.raft_bases = Some(RaftBases {
                incarnation: assignment.incarnation,
                bases: record.bases.clone(),
            });
            assignment.raft_seed = record.seed.clone();
        }
    }
}

#[async_trait::async_trait]
impl AloneWriter for FakeControlPlane {
    async fn send(
        &self,
        body: &serde_json::Value,
    ) -> anyhow::Result<felix_replication::controlplane_link::LinkResponse> {
        use felix_replication::controlplane_link::LinkResponse;
        self.requests.lock().push(body.clone());
        let say = *self.say.lock();
        let start = &body["start"];
        let expected = start["expected_epoch"].as_u64().expect("an epoch");
        let mut record = self.record.lock();
        let bumped = |record: &Record| {
            let mut next = record.clone();
            let base: RaftEpochBase =
                serde_json::from_value(start["base"].clone()).expect("a base");
            next.epoch += 1;
            next.bases.push(base);
            next.seed = Some(RaftVoters {
                incarnation: start["incarnation"].as_u64().expect("an incarnation"),
                epoch: next.epoch,
                members: serde_json::from_value(start["voters"].clone()).expect("voters"),
                starts: serde_json::from_value(start["starts"].clone()).expect("starts"),
            });
            next
        };
        // A conflict's body carries the record the write would have made,
        // so a broker that read the answer instead of the status would
        // start from it.
        let shown = if say == Say::Conflict {
            bumped(&record)
        } else if record.epoch == expected {
            let next = bumped(&record);
            *record = next;
            self.bumps.fetch_add(1, Ordering::SeqCst);
            record.clone()
        } else {
            record.clone()
        };
        if say == Say::TakeThenSilence {
            anyhow::bail!("the answer was lost");
        }
        let mut answer = record.template.clone().expect("a fed assignment");
        Self::stamp(&shown, &mut answer);
        let status = if say == Say::Conflict || shown.epoch != expected + 1 {
            StatusCode::CONFLICT
        } else {
            StatusCode::OK
        };
        Ok(LinkResponse {
            status,
            body: serde_json::to_vec(&answer).expect("json").into(),
        })
    }
}

struct Cluster {
    broker: Arc<Broker>,
    shards: Arc<RaftShards>,
    engine: Arc<crate::shards::routing::StreamEngine>,
}

/// A cluster broker's shards in `root`: no feed of their own, the control
/// plane fake's writer set, on `fleet`.
async fn cluster(
    root: &std::path::Path,
    fsync: felix_storage::log::FsyncMode,
    fleet: FleetGate,
    cp: &Arc<FakeControlPlane>,
) -> Cluster {
    let broker = broker_in(root, fsync).await;
    let (shards, engine) = members(&broker, Arc::new(fleet));
    shards.set_alone_writer(cp.clone());
    Cluster {
        broker,
        shards,
        engine,
    }
}

impl Cluster {
    /// One pass of the watch's feed, `change` applied to the assignment.
    async fn pass(&self, cp: &FakeControlPlane, change: &dyn Fn(&mut ShardAssignment)) {
        let mut all = self.shards.standalone_assignments().await;
        for assignment in all.values_mut() {
            cp.apply(assignment);
            change(assignment);
        }
        self.shards.take(&mut all).await;
        self.shards
            .answered(Some(tokio::time::Instant::now()), None);
    }

    /// Feed until the shard is led and every synced record reads back.
    async fn serve(&self, cp: &FakeControlPlane, end: u64) {
        let orders = key("orders", 0);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            self.pass(cp, &|_| {}).await;
            if self.shards.leading(&orders).is_some()
                && matches!(
                    self.engine.committed_bound(&orders),
                    felix_broker::ReadBound::Committed(bound) if bound >= end
                )
            {
                return;
            }
            assert!(tokio::time::Instant::now() < deadline, "the shard serves");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Feed a few passes with `change` applied; for a shard that must stay down.
    async fn feed(&self, cp: &FakeControlPlane, change: &dyn Fn(&mut ShardAssignment)) {
        for _ in 0..6 {
            self.pass(cp, change).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn stop(self) {
        assert!(EngineShards::shutdown(&*self.shards).await);
    }
}

/// A root a relaxed crash rotated after the first run wrote five records.
async fn crashed(root: &std::path::Path) -> Written {
    begin(root, true, 1);
    let written = write(root, PERIODIC).await;
    assert!(begin(root, true, 2), "a relaxed crash rotates");
    written
}

fn stopped(dir: &std::path::Path) -> bool {
    felix_storage::raft_copy::stopped(dir)
        .expect("marker")
        .is_some()
}

/// **A cluster's RF 1 shard re-forms itself after a Periodic crash**: one
/// request to the control plane, then the copy starts from the recorded
/// seed and serves. Every synced record is kept and the next offset is
/// above everything the crashed run gave out.
#[tokio::test]
async fn a_relaxed_crash_re_forms_a_cluster_shard_through_the_control_plane() {
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::Take);
    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.serve(&cp, w.end).await;

    let requests = cp.requests();
    assert_eq!(requests.len(), 1, "one request: {requests:?}");
    let start = &requests[0]["start"];
    assert_eq!(start["expected_epoch"], 0);
    assert_eq!(cp.epoch(), 1);
    assert_eq!(group_epoch(&w.dir).expect("epoch"), Some(1));
    let first = start["base"]["first_index"].as_u64().expect("first index");
    assert!(first > w.reserved.expect("a reservation"));
    assert_eq!(payloads(&c.broker).await, written());
    let next = publish(&c.broker, &c.engine, &[b"six"]).await;
    assert!(next >= w.end, "offset {next} reused below {}", w.end);
    c.stop().await;
}

/// **A crash before the write resumes it**: the answer never came, so the
/// next run sends the same request and the record is bumped once.
#[tokio::test]
async fn a_crash_before_the_write_sends_the_same_request_again() {
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::Take);
    // The control plane is down on the first run: no answer at all.
    struct Down;
    #[async_trait::async_trait]
    impl AloneWriter for Down {
        async fn send(
            &self,
            _: &serde_json::Value,
        ) -> anyhow::Result<felix_replication::controlplane_link::LinkResponse> {
            anyhow::bail!("down")
        }
    }
    let broker = broker_in(root.path(), PERIODIC).await;
    let (shards, _engine) = members(&broker, Arc::new(fleet_of_one()));
    shards.set_alone_writer(Arc::new(Down));
    let down = Cluster {
        broker,
        shards,
        engine: _engine,
    };
    // Seed the fake's template so `apply` has one to stamp.
    down.pass(&cp, &|_| {}).await;
    assert!(stopped(&w.dir), "the copy was stopped for the epoch");
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    down.stop().await;

    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.serve(&cp, w.end).await;
    assert_eq!(cp.requests().len(), 1);
    assert_eq!(cp.bumps.load(Ordering::SeqCst), 1);
    assert_eq!(group_epoch(&w.dir).expect("epoch"), Some(1));
    assert_eq!(payloads(&c.broker).await, written());
    c.stop().await;
}

/// **A crash after the write, before the start, starts from the recorded
/// seed** and asks for nothing: the control plane's record is not written
/// a second time.
#[tokio::test]
async fn a_crash_after_the_write_starts_from_the_recorded_seed() {
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::TakeThenSilence);
    let first = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    // One pass, then the broker is gone: the answer never came.
    first.pass(&cp, &|_| {}).await;
    assert_eq!(cp.epoch(), 1, "the record was written");
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None, "not started");
    first.stop().await;
    let sent = cp.requests().len();

    cp.say(Say::Take);
    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.serve(&cp, w.end).await;
    assert_eq!(cp.requests().len(), sent, "nothing is asked again");
    assert_eq!(cp.bumps.load(Ordering::SeqCst), 1);
    assert_eq!(group_epoch(&w.dir).expect("epoch"), Some(1));
    assert_eq!(payloads(&c.broker).await, written());
    c.stop().await;
}

/// **A crash after the start owes nothing**: the next run asks for nothing.
#[tokio::test]
async fn a_restart_after_the_start_asks_for_nothing() {
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::Take);
    let first = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    first.serve(&cp, w.end).await;
    first.stop().await;
    let sent = cp.requests().len();
    assert_eq!(sent, 1);
    // A crash again, after the re-form: the id rotates once more, but the
    // copy of the new epoch is its own voter and nothing is owed here.
    let id = felix_storage::storage_identity::load(root.path())
        .expect("id")
        .expect("an id");
    run_state::mark_clean(root.path(), id).expect("clean");
    assert!(!begin(root.path(), true, 3));

    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.serve(&cp, w.end).await;
    assert_eq!(cp.requests().len(), sent);
    assert_eq!(payloads(&c.broker).await, written());
    c.stop().await;
}

/// **A fleet that has not finalized the feature leaves the shard for an
/// operator**: no request, and the copy is not touched.
#[tokio::test]
async fn without_the_fleet_feature_nothing_is_asked() {
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::Take);
    let raft = || {
        fleet::IMPLEMENTED
            .iter()
            .map(|feature| feature.name())
            .filter(|name| name.starts_with("raft_") && *name != "raft_alone_reform")
    };
    let gate = FleetGate::new(raft());
    gate.restart(raft());
    let c = cluster(root.path(), PERIODIC, gate, &cp).await;
    c.feed(&cp, &|_| {}).await;
    assert!(cp.requests().is_empty());
    assert!(!stopped(&w.dir));
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    assert!(c.shards.leading(&key("orders", 0)).is_none());
    c.stop().await;
}

/// **A shard with no relaxed crash behind it is left alone**: OnCommit
/// rotates nothing, so there is no evidence and no request.
#[tokio::test]
async fn an_on_commit_shard_is_not_re_formed() {
    use felix_storage::log::FsyncMode;
    let root = tempfile::tempdir().expect("tempdir");
    begin(root.path(), false, 1);
    let w = write(root.path(), FsyncMode::OnCommit).await;
    assert!(!begin(root.path(), false, 2));
    let cp = FakeControlPlane::new(Say::Take);
    let c = cluster(root.path(), FsyncMode::OnCommit, fleet_of_one(), &cp).await;
    c.serve(&cp, w.end).await;
    assert!(cp.requests().is_empty());
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None);
    assert_eq!(payloads(&c.broker).await, written());
    c.stop().await;
}

/// **A shard that is not alone is never re-formed here**: a placement or a
/// membership that names another broker, a move and a set change are the
/// control plane's to settle, and the copy is not stopped.
#[tokio::test]
async fn a_shard_that_is_not_alone_is_not_re_formed() {
    type Change = Box<dyn Fn(&mut ShardAssignment)>;
    let other = || "broker-b".to_string();
    let changes: Vec<(&str, Change)> = vec![
        (
            "placed on another broker too",
            Box::new(move |a| {
                a.placed
                    .as_mut()
                    .expect("placed")
                    .brokers
                    .insert("broker-b".to_string());
            }),
        ),
        (
            "a replica elsewhere",
            Box::new(move |a| a.replicas.push(other())),
        ),
        (
            "a move's successor",
            Box::new(move |a| a.successor = Some(other())),
        ),
        (
            "a set change",
            Box::new(move |a| {
                a.pending_set = Some(crate::shards::watch::PendingSet {
                    replicas: vec![LOCAL_MEMBER.to_string(), other()],
                    generation: 2,
                })
            }),
        ),
        ("an unrecorded placement", Box::new(|a| a.placed = None)),
    ];
    for (what, change) in changes {
        let root = tempfile::tempdir().expect("tempdir");
        let w = crashed(root.path()).await;
        let cp = FakeControlPlane::new(Say::Take);
        let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
        c.feed(&cp, &*change).await;
        assert!(cp.requests().is_empty(), "{what}: asked");
        assert!(!stopped(&w.dir), "{what}: stopped the copy");
        assert_eq!(group_epoch(&w.dir).expect("epoch"), None, "{what}");
        c.stop().await;
    }
}

/// **A refusal leaves the shard not ready and an operator's re-form still
/// works**: the copy stays stopped at its epoch, which is where an
/// operator's own stop leaves it, so the operator's stop round answers, the
/// plan from its report is recorded, and the next pass starts the copy from
/// that record and serves with every synced record kept.
#[tokio::test]
async fn a_conflict_leaves_the_shard_down_for_an_operator() {
    use crate::shards::raft::reform_survey::SurveyKind;
    use felix_storage::raft_copy::{
        BrokerReport, EpochRecord, GroupRecord, LocalRecord, SurveyStep, first_probes, plan_starts,
        survey_step,
    };
    let orders = key("orders", 0);
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::Conflict);
    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.feed(&cp, &|_| {}).await;
    assert!(!cp.requests().is_empty(), "the request was sent");
    assert_eq!(cp.epoch(), 0);
    assert!(c.shards.leading(&orders).is_none());
    assert!(stopped(&w.dir));
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None, "not started");

    // The operator's re-form, as the control plane runs it: the stop
    // round, the plan from its reports with the loss accepted, the epoch
    // write.
    let copy = LocalRecord
        .load(&w.dir)
        .expect("load")
        .expect("a plan")
        .copy;
    let me = BTreeSet::from([(LOCAL_MEMBER.to_string(), copy)]);
    let record = GroupRecord {
        incarnation: INCARNATION,
        bases: Vec::new(),
        floor: None,
        offset_floor: None,
        placed: Some(BTreeSet::from([LOCAL_MEMBER.to_string()])),
        dead: BTreeSet::new(),
        seed: Some(me.clone()),
        ordered: Some(vec![me]),
        restore: None,
    };
    let mut asked = first_probes(&record);
    let read = c.shards.reform_reader();
    let mut kind = SurveyKind::Stop;
    let (view, starts) = loop {
        let ask = super::reform::survey(&orders, kind, 0, asked.clone());
        let wire = if kind == SurveyKind::Stop {
            c.shards.stop_for_reform(&ask).await
        } else {
            read(&ask)
        }
        .expect("the copy answers");
        let reports = std::collections::BTreeMap::from([(
            LOCAL_MEMBER.to_string(),
            BrokerReport::<String>::try_from(wire).expect("version"),
        )]);
        match survey_step(&record, &reports, &asked) {
            SurveyStep::Planned(view) => {
                break (view, plan_starts(&record, &reports, true).expect("starts"));
            }
            SurveyStep::Ask(more) => {
                asked = more;
                kind = SurveyKind::Survey;
            }
            other => panic!("no plan: {other:?}"),
        }
    };
    {
        let mut held = cp.record.lock();
        held.epoch = 1;
        held.bases = vec![RaftEpochBase {
            base_last: view
                .base_last
                .map(|(index, epoch, term)| crate::shards::watch::RaftLogId { index, epoch, term }),
            first_index: view.first_index,
        }];
        held.seed = Some(RaftVoters {
            incarnation: INCARNATION,
            epoch: 1,
            members: view.voters.iter().cloned().collect(),
            starts,
        });
    }
    c.serve(&cp, w.end).await;
    assert_eq!(group_epoch(&w.dir).expect("epoch"), Some(1));
    // The operator's plan keeps the log to the saved commit, the first
    // batch here, and accepts the loss of the rest.
    let kept = payloads(&c.broker).await;
    assert!(
        kept.len() >= 3 && written().starts_with(&kept),
        "kept {kept:?}"
    );
    let next = publish(&c.broker, &c.engine, &[b"six"]).await;
    assert!(
        next > w.reserved.expect("a reservation"),
        "offset {next} at or below the crashed run's reservation"
    );
    let _ = highest_reserved;
    c.stop().await;
}

/// Stop the cluster's shards and record a clean stop, so the next begin
/// rotates nothing.
async fn stop_cleanly(root: &std::path::Path, c: Cluster) {
    c.stop().await;
    let id = felix_storage::storage_identity::load(root)
        .expect("id")
        .expect("an id");
    run_state::mark_clean(root, id).expect("clean");
}

/// **A directory restored from before a self re-form is not started in the
/// epoch the control plane recorded for it.** That epoch's seed was planned
/// for the copy the backup predates; starting the restored copy from it
/// would give out again the offsets the epoch gave out after the backup.
/// The shard stays down, its copy not stopped, and nothing is asked.
#[tokio::test]
async fn a_restore_from_before_the_re_form_is_not_started_from_its_seed() {
    let root = tempfile::tempdir().expect("tempdir");
    let backup = tempfile::tempdir().expect("backup");
    begin(root.path(), true, 1);
    let w = write(root.path(), PERIODIC).await;
    copy_tree(root.path(), backup.path());

    assert!(begin(root.path(), true, 2), "a relaxed crash rotates");
    let cp = FakeControlPlane::new(Say::Take);
    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.serve(&cp, w.end).await;
    assert_eq!(cp.epoch(), 1);
    let after = publish(&c.broker, &c.engine, &[b"six"]).await;
    stop_cleanly(root.path(), c).await;
    let sent = cp.requests().len();

    put_back(backup.path(), root.path());
    crate::node::storage::rotate_storage_id(root.path()).expect("rotate-storage-id");
    begin(root.path(), true, 3);
    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.feed(&cp, &|_| {}).await;
    if c.shards.leading(&key("orders", 0)).is_some() {
        let next = publish(&c.broker, &c.engine, &[b"seven"]).await;
        assert!(
            next > after,
            "offset {next} reused: the re-formed epoch gave out {after} after the backup"
        );
    }
    assert!(
        c.shards.leading(&key("orders", 0)).is_none(),
        "the shard stays down"
    );
    assert!(!stopped(&w.dir), "the restored copy was stopped");
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None, "started");
    assert_eq!(cp.requests().len(), sent, "asked the control plane");
    assert_eq!(cp.epoch(), 1);
    c.stop().await;
}

/// **A lost answer is retried while the broker runs**: the record was
/// written but the answer never came, and a later pass starts the copy
/// without a restart and without a second bump.
#[tokio::test]
async fn a_lost_answer_starts_the_copy_on_a_later_pass() {
    let root = tempfile::tempdir().expect("tempdir");
    let w = crashed(root.path()).await;
    let cp = FakeControlPlane::new(Say::TakeThenSilence);
    let c = cluster(root.path(), PERIODIC, fleet_of_one(), &cp).await;
    c.pass(&cp, &|_| {}).await;
    assert_eq!(cp.epoch(), 1, "the record was written");
    assert_eq!(group_epoch(&w.dir).expect("epoch"), None, "not started");
    cp.say(Say::Take);
    c.serve(&cp, w.end).await;
    assert_eq!(cp.bumps.load(Ordering::SeqCst), 1);
    assert_eq!(group_epoch(&w.dir).expect("epoch"), Some(1));
    assert_eq!(payloads(&c.broker).await, written());
    c.stop().await;
}
