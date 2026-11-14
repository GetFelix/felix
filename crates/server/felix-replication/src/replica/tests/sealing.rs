//! A seal order on the leader: the round runs against real follower
//! handlers, claims only once the shard is quiet, reports once, and gives
//! up to the control plane instead of running twice. And the side logs of
//! a sealed copy take no local write.

use std::sync::atomic::{AtomicBool, Ordering};

use felix_storage::StorageId;
use parking_lot::Mutex;
use reqwest::StatusCode;

use super::seal::{Followers, follower, holding, logs_of, seal_fence, sealing_broker};
use super::*;
use crate::controlplane_link::LinkResponse;
use crate::sealing::{SealOrder, SealOrders, SealTo, Sealing};

const SEAL: u64 = 5;
const LEADER: &str = "broker-a";

fn copy() -> StorageId {
    StorageId::from_bytes([1; 16])
}

/// One seal order on the shard, and a fence the test opens and closes.
struct Orders {
    continues: bool,
    quiet: AtomicBool,
}

impl Orders {
    fn new(continues: bool, quiet: bool) -> Arc<Self> {
        Arc::new(Self {
            continues,
            quiet: AtomicBool::new(quiet),
        })
    }
}

impl SealOrders for Orders {
    fn orders(&self) -> Vec<SealOrder> {
        vec![SealOrder {
            key: crate::driver::watch_key(&key()),
            generation: SEAL,
            continues: self.continues,
        }]
    }

    fn quiesced(&self, _key: &crate::ShardKey) -> bool {
        self.quiet.load(Ordering::SeqCst)
    }
}

/// A control plane that records each seal call and answers from a script;
/// once the script runs out it does not answer.
#[derive(Clone, Default)]
struct ControlPlane {
    calls: Arc<Mutex<Vec<(&'static str, serde_json::Value)>>>,
    answers: Arc<Mutex<std::collections::VecDeque<(StatusCode, serde_json::Value)>>>,
}

impl ControlPlane {
    fn answering(answers: impl IntoIterator<Item = (StatusCode, serde_json::Value)>) -> Self {
        Self {
            calls: Default::default(),
            answers: Arc::new(Mutex::new(answers.into_iter().collect())),
        }
    }

    fn calls(&self) -> Vec<(&'static str, serde_json::Value)> {
        self.calls.lock().clone()
    }

    fn take(
        &self,
        call: &'static str,
        node_id: &str,
        body: &(impl serde::Serialize + Sync),
    ) -> anyhow::Result<LinkResponse> {
        assert_eq!(node_id, LEADER);
        self.calls
            .lock()
            .push((call, serde_json::to_value(body).expect("json")));
        let (status, body) = self
            .answers
            .lock()
            .pop_front()
            .ok_or_else(|| anyhow::anyhow!("no answer"))?;
        Ok(LinkResponse {
            status,
            body: serde_json::to_vec(&body).expect("json").into(),
        })
    }
}

impl crate::controlplane_link::ControlPlaneLink for ControlPlane {
    fn endpoint(&self) -> String {
        "fake".to_string()
    }
    async fn seal_report(
        &self,
        _: Option<&str>,
        node_id: &str,
        body: &(impl serde::Serialize + Sync),
    ) -> anyhow::Result<LinkResponse> {
        self.take("seal-report", node_id, body)
    }
    async fn seal_give_up(
        &self,
        _: Option<&str>,
        node_id: &str,
        body: &(impl serde::Serialize + Sync),
    ) -> anyhow::Result<LinkResponse> {
        self.take("seal-give-up", node_id, body)
    }
    async fn confirmed_assignment(
        &self,
        _: Option<&str>,
        _: &felix_router::ShardKey,
        _: std::time::Duration,
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn confirmed_assignments(
        &self,
        _: Option<&str>,
        _: &[felix_router::ShardKey],
        _: std::time::Duration,
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn register(
        &self,
        _: &str,
        _: &(impl serde::Serialize + Sync),
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn node(&self, _: &str, _: &str) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn heartbeat(
        &self,
        _: &str,
        _: &str,
        _: &(impl serde::Serialize + Sync),
        _: std::time::Duration,
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn lifecycle(&self, _: &str, _: &str, _: &str) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn node_catalog_page(
        &self,
        _: Option<&str>,
        _: Option<&str>,
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn assignment_snapshot(
        &self,
        _: Option<&str>,
        _: std::time::Duration,
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn assignment_changes(
        &self,
        _: Option<&str>,
        _: u64,
        _: std::time::Duration,
        _: std::time::Duration,
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
    async fn replica_status(
        &self,
        _: Option<&str>,
        _: &str,
        _: &(impl serde::Serialize + Sync),
    ) -> anyhow::Result<LinkResponse> {
        unreachable!()
    }
}

/// The leader's routes: it leads the shard at the seal's generation with
/// `broker-b` and `broker-c` behind it.
fn leader_router() -> Arc<ShardRouter> {
    let router = Arc::new(ShardRouter::new(
        LEADER,
        "us-west-2",
        RegionRouter::new("us-west-2".to_string()),
    ));
    let nodes: HashMap<String, NodeRef> = [
        (LEADER.to_string(), node(LEADER, 7001)),
        ("broker-b".to_string(), node("broker-b", 7002)),
        ("broker-c".to_string(), node("broker-c", 7003)),
    ]
    .into_iter()
    .collect();
    let table = RoutingTable::build(
        [(
            key(),
            LEADER.to_string(),
            vec!["broker-b".to_string(), "broker-c".to_string()],
            SEAL,
        )],
        &nodes,
    );
    router.publish(table, &nodes);
    router
}

fn leader_sealing<'a>(
    leader: &Arc<Broker>,
    followers: Followers<'a>,
    orders: &Arc<Orders>,
    control_plane: &ControlPlane,
) -> Sealing<Followers<'a>, ControlPlane> {
    Sealing::new(
        Arc::new(followers),
        Arc::clone(leader),
        leader_router(),
        Arc::clone(orders) as Arc<dyn SealOrders>,
        SealTo {
            link: control_plane.clone(),
            node_id: LEADER.to_string(),
            token: None,
            held: None,
            storage_id: Some(copy()),
        },
    )
}

fn both<'a>(b: &'a ReplicaHandler, c: &'a ReplicaHandler) -> Followers<'a> {
    Followers {
        nodes: vec![
            ("broker-b", b, PeerCapabilities::SWITCH_SEAL),
            ("broker-c", c, PeerCapabilities::SWITCH_SEAL),
        ],
    }
}

async fn tail(broker: &Broker) -> u64 {
    let [log, _, _] = logs_of(broker).await;
    log.tail_offset().await.expect("tail")
}

fn accepted() -> (StatusCode, serde_json::Value) {
    (StatusCode::OK, serde_json::json!({"accepted": true}))
}

/// **The whole round under a seal order.** Nothing is claimed while a write
/// may still be inside the fence. Once quiet the record is claimed, both
/// followers store it, and the report goes to the control plane. A report
/// the control plane does not answer is sent again as it was: the round is
/// not run a second time, so there is one record at `N`. Once answered,
/// nothing more is sent.
#[tokio::test]
async fn a_seal_order_claims_once_quiet_and_reports_once() {
    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (c_broker, c) = follower(&root.path().join("c"), 3).await;
    let orders = Orders::new(true, false);
    // No answer to the first report, then an acceptance.
    let control_plane = ControlPlane::answering([]);
    let mut sealing = leader_sealing(&leader, both(&b, &c), &orders, &control_plane);

    assert!(sealing.pass().await.is_some());
    assert!(sealing.pass().await.is_some());
    assert_eq!(tail(&leader).await, 3, "claimed with a write in flight");
    assert!(control_plane.calls().is_empty());

    orders.quiet.store(true, Ordering::SeqCst);
    assert!(sealing.pass().await.is_some(), "the report went unanswered");
    assert_eq!(tail(&leader).await, 4);
    assert_eq!(tail(&b_broker).await, 4);
    assert_eq!(tail(&c_broker).await, 4);
    let sent = control_plane.calls();
    assert_eq!(sent.len(), 1, "{sent:?}");
    let (call, body) = &sent[0];
    assert_eq!(*call, "seal-report");
    assert_eq!(
        body["key"],
        serde_json::json!({
            "tenant_id": TENANT, "namespace": NAMESPACE, "stream": STREAM,
            "shard": 0, "kind": "stream",
        })
    );
    let report = &body["report"];
    assert_eq!(report["generation"], SEAL);
    assert_eq!(
        (&report["offset"], &report["start"]),
        (&3.into(), &0.into())
    );
    assert_eq!(report["digest"].as_str().map(str::len), Some(64));
    for side in ["dead_letters", "cursors"] {
        assert_eq!(report[side]["start"], 0, "{side}");
        assert_eq!(report[side]["end"], 1, "{side}");
        assert_eq!(report[side]["digest"].as_str().map(str::len), Some(64));
    }
    assert_eq!(
        report["voters"],
        serde_json::json!({
            "broker-a": copy().to_hex(),
            "broker-b": StorageId::from_bytes([2; 16]).to_hex(),
            "broker-c": StorageId::from_bytes([3; 16]).to_hex(),
        })
    );

    control_plane.answers.lock().push_back(accepted());
    assert!(sealing.pass().await.is_none(), "answered: nothing is left");
    let sent = control_plane.calls();
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1], sent[0], "the same report, not a second round");
    assert_eq!(tail(&leader).await, 4, "a second record at N");

    assert!(sealing.pass().await.is_none());
    assert_eq!(control_plane.calls().len(), 2, "reported after the answer");
}

/// **A leader that finds its claim already made gives the seal up.** Its
/// count went with the process that claimed, and a second record at the
/// same generation is never written. The give-up is sent until the control
/// plane answers it, and nothing reopens the shard meanwhile.
#[tokio::test]
async fn a_restart_that_finds_the_claim_gives_the_seal_up() {
    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (_b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (_c_broker, c) = follower(&root.path().join("c"), 3).await;
    let orders = Orders::new(true, true);
    // The run that claimed never got its report answered.
    let lost = ControlPlane::answering([]);
    let mut first = leader_sealing(&leader, both(&b, &c), &orders, &lost);
    assert!(first.pass().await.is_some());
    assert_eq!(tail(&leader).await, 4);
    drop(first);

    let control_plane = ControlPlane::answering([]);
    let mut restarted = leader_sealing(&leader, both(&b, &c), &orders, &control_plane);
    assert!(restarted.pass().await.is_some());
    assert!(restarted.pass().await.is_some());
    let sent = control_plane.calls();
    assert_eq!(sent.len(), 2, "sent again until answered: {sent:?}");
    for (call, body) in &sent {
        assert_eq!(*call, "seal-give-up");
        assert_eq!(body["generation"], SEAL);
        assert_eq!(body["key"]["stream"], STREAM);
        assert!(
            body["reason"]
                .as_str()
                .is_some_and(|why| why.contains("Claimed")),
            "{body}"
        );
    }
    assert_eq!(tail(&leader).await, 4, "a second record at the generation");

    control_plane
        .answers
        .lock()
        .push_back((StatusCode::OK, serde_json::json!({"given_up": true})));
    assert!(restarted.pass().await.is_none());
    assert!(restarted.pass().await.is_none());
    assert_eq!(control_plane.calls().len(), 3);
    let [log, _, _] = logs_of(&leader).await;
    assert!(
        log.disk_log().holds_seal(),
        "only a later generation ends the promise"
    );
}

/// **A leader that was not serving the generation before runs no round.**
/// It promises nothing, fences nobody and gives the order up.
#[tokio::test]
async fn an_order_the_leader_does_not_continue_is_given_up_unrun() {
    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (_b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (_c_broker, c) = follower(&root.path().join("c"), 3).await;
    let orders = Orders::new(false, true);
    let control_plane =
        ControlPlane::answering([(StatusCode::OK, serde_json::json!({"given_up": true}))]);
    let mut sealing = leader_sealing(&leader, both(&b, &c), &orders, &control_plane);

    assert!(sealing.pass().await.is_none());
    let sent = control_plane.calls();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0, "seal-give-up");
    let [log, _, _] = logs_of(&leader).await;
    assert!(log.disk_log().seal_promise().is_none());
    assert_eq!(tail(&leader).await, 3);
    let shard = batch(SEAL, 0, &[]).shard;
    assert!(b.seal_promise(&shard).await.is_none());
}

/// **A report the control plane keeps refusing is given up, and one for
/// another sealer is dropped.** A 409 `conflict` may pass, so it is sent
/// again for a while first.
#[tokio::test]
async fn a_refused_report_is_given_up_and_another_sealers_is_dropped() {
    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (_b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (_c_broker, c) = follower(&root.path().join("c"), 3).await;
    let orders = Orders::new(true, true);
    let conflict = || {
        (
            StatusCode::CONFLICT,
            serde_json::json!({"code": "conflict"}),
        )
    };
    let control_plane = ControlPlane::answering(std::iter::repeat_with(conflict).take(64));
    let mut sealing = leader_sealing(&leader, both(&b, &c), &orders, &control_plane);
    for _ in 0..40 {
        sealing.pass().await;
    }
    let sent = control_plane.calls();
    let reports = sent
        .iter()
        .filter(|(call, _)| *call == "seal-report")
        .count();
    assert!((2..40).contains(&reports), "{reports} reports");
    assert_eq!(sent.last().expect("a call").0, "seal-give-up");
    assert_eq!(tail(&leader).await, 4);

    let root = tempfile::tempdir().expect("tempdir");
    let leader = sealing_broker(&root.path().join("a"));
    holding(&leader).await;
    let (_b_broker, b) = follower(&root.path().join("b"), 2).await;
    let (_c_broker, c) = follower(&root.path().join("c"), 3).await;
    let control_plane = ControlPlane::answering([(
        StatusCode::CONFLICT,
        serde_json::json!({"code": "not_sealer"}),
    )]);
    let mut sealing = leader_sealing(&leader, both(&b, &c), &orders, &control_plane);
    assert!(sealing.pass().await.is_none());
    assert_eq!(control_plane.calls().len(), 1);
}

/// **A sealed copy's cursors and dead letters take no local write.** The
/// seal names their ranges; a commit or a dead letter after the promise
/// would leave this copy unable to acknowledge or to start from the base.
/// A later generation ends the promise and the hold with it.
#[tokio::test]
async fn a_sealed_copy_refuses_cursor_commits_and_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let broker = sealing_broker(dir.path());
    holding(&broker).await;
    let groups = Arc::clone(broker.consumer_groups().expect("groups"));
    let dead_letters = Arc::clone(broker.group_reader().expect("reader").dead_letters());
    let group = felix_broker::GroupKey {
        tenant_id: TENANT.to_string(),
        namespace: NAMESPACE.to_string(),
        stream: STREAM.to_string(),
        shard: 0,
        group: "workers".to_string(),
    };
    let [log, dead, cursors] = logs_of(&broker).await;

    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], SEAL))
        .with_local_storage(StorageId::from_bytes([2; 16]))
        .with_stickiness(std::time::Duration::ZERO);
    let answer = handler.seal_fence(SENDER, seal_fence(SEAL, copy())).await;
    assert!(
        matches!(answer, InternalMessage::SealFenceOk(_)),
        "{answer:?}"
    );
    let tails = (
        dead.tail_offset().await.expect("tail"),
        cursors.tail_offset().await.expect("tail"),
    );

    let commit = groups.commit(TENANT, NAMESPACE, STREAM, 0, "late", 2).await;
    assert!(commit.is_err(), "{commit:?}");
    let recorded = dead_letters.record(&group, 2).await;
    assert!(recorded.is_err(), "{recorded:?}");
    assert_eq!(
        (
            dead.tail_offset().await.expect("tail"),
            cursors.tail_offset().await.expect("tail"),
        ),
        tails,
        "a sealed copy's side log grew",
    );
    assert!(broker.group_logs_quiet(TENANT, NAMESPACE, STREAM, 0));

    log.accept_generation(SEAL + 1, Some("broker-a"))
        .await
        .expect("accept");
    groups
        .commit(TENANT, NAMESPACE, STREAM, 0, "late", 2)
        .await
        .expect("commit once the seal is over");
    dead_letters
        .record(&group, 2)
        .await
        .expect("dead letter once the seal is over");
}
