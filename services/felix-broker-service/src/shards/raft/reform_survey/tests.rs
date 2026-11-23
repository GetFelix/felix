use reqwest::StatusCode;

use super::*;
use crate::test_support::fake_link::FakeLink;
use felix_storage::raft_copy::{REPORT_VERSION, WireCopy};

fn survey_json(stream: &str, round: u32) -> serde_json::Value {
    serde_json::json!({
        "plan_id": "p1",
        "round": round,
        "key": {"tenant_id": "t1", "namespace": "ns", "stream": stream, "shard": 0, "kind": "stream"},
        "incarnation": 3,
        "epoch": 0,
        "probes": [1, 4],
    })
}

fn no_stop() -> Stop {
    Arc::new(|_| Box::pin(async { panic!("nothing is stopped") }))
}

fn report() -> WireReport {
    WireReport {
        version: REPORT_VERSION,
        leads: false,
        names: Default::default(),
        memberships: Some(Vec::new()),
        current: WireCopy {
            path: "shard".to_string(),
            copy: Some(1),
            epoch: None,
            last: None,
            purged: None,
            committed: None,
            vote: None,
            reserved: None,
            floor: None,
            rotated_by: None,
            terms: Default::default(),
        },
        kept: Vec::new(),
    }
}

/// Every survey read here is answered in one request, under the plan id
/// and round it was asked in; one this broker cannot read is left out.
#[tokio::test]
async fn answers_carry_the_plan_and_round_they_were_read_for() {
    let link = FakeLink::default().reply(
        "reform_surveys",
        serde_json::json!({ "surveys": [survey_json("held", 2), survey_json("elsewhere", 1)] }),
    );
    let read: Read = Arc::new(|survey: &Survey| {
        assert_eq!(survey.probes, BTreeSet::from([1, 4]));
        (survey.key.stream == "held").then(report)
    });
    tick(&link, "b1", None, read, no_stop()).await;
    let sent = link.reform_reports();
    assert_eq!(sent.len(), 1, "one request per tick");
    let reports = sent[0]["reports"].as_array().expect("reports");
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert_eq!(reports[0]["plan_id"], "p1");
    assert_eq!(reports[0]["round"], 2);
    assert_eq!(reports[0]["key"]["stream"], "held");
    assert_eq!(reports[0]["report"]["version"], REPORT_VERSION);
}

/// A control plane without surveys, or with none for this broker, is sent
/// nothing.
#[tokio::test]
async fn nothing_is_sent_without_a_survey() {
    let read: Read = Arc::new(|_: &Survey| Some(report()));
    let old = FakeLink::default().status("reform_surveys", StatusCode::NOT_FOUND);
    tick(&old, "b1", None, Arc::clone(&read), no_stop()).await;
    assert!(old.reform_reports().is_empty());
    let idle = FakeLink::default().reply("reform_surveys", serde_json::json!({ "surveys": [] }));
    tick(&idle, "b1", None, read, no_stop()).await;
    assert!(idle.reform_reports().is_empty());
}

/// A Raft cache shard is surveyed too (#2978): its copy is read from the
/// cache store's root, where `open_member` claimed it, not the stream
/// root, which holds nothing for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cache_shards_copy_is_read_from_the_cache_root() {
    use crate::shards::raft::tests::{
        INCARNATION, assigned, cache_broker, key, shards, take_formed,
    };

    let root = tempfile::tempdir().expect("tempdir");
    let broker = cache_broker(root.path(), "sessions").await;
    let shards = shards(&broker);
    let sessions = ShardKey {
        kind: ShardKind::Cache,
        ..key("sessions", 0)
    };
    take_formed(&shards, assigned(sessions.clone(), INCARNATION)).await;
    let copy = shards
        .held_copies()
        .into_iter()
        .find(|held| held.key == sessions)
        .expect("a cache member")
        .copy;

    let survey = Survey {
        plan_id: "p".to_string(),
        kind: SurveyKind::Survey,
        round: 1,
        key: sessions,
        incarnation: INCARNATION,
        epoch: 0,
        probes: BTreeSet::new(),
        start: None,
    };
    let read = shards.reform_reader();
    let report = tokio::task::spawn_blocking(move || read(&survey))
        .await
        .expect("read")
        .expect("the cache shard's copy is read");
    assert_eq!(report.current.copy, Some(copy));
    assert!(
        report.current.path.contains("caches"),
        "{}",
        report.current.path
    );
}

/// A stop goes to the stopper, not the read-only reader, and its answer says
/// it is one: the control plane takes a stop answer only with that kind. A
/// kind this build does not know is not read as a survey.
#[tokio::test]
async fn a_stop_is_answered_by_the_stopper_under_its_kind() {
    let mut stop = survey_json("held", 3);
    stop["kind"] = "stop".into();
    let mut unknown = survey_json("other", 3);
    unknown["kind"] = "rewind".into();
    let link = FakeLink::default().reply(
        "reform_surveys",
        serde_json::json!({ "surveys": [stop, unknown] }),
    );
    let read: Read = Arc::new(|survey: &Survey| panic!("read {survey:?}"));
    let stopped: Stop = Arc::new(|survey: Survey| {
        Box::pin(async move { (survey.key.stream == "held").then(report) })
    });
    tick(&link, "b1", None, read, stopped).await;
    let sent = link.reform_reports();
    let reports = sent[0]["reports"].as_array().expect("reports");
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert_eq!(reports[0]["kind"], "stop");
}

fn stop_survey(key: ShardKey) -> Survey {
    Survey {
        plan_id: "p".to_string(),
        kind: SurveyKind::Stop,
        round: 2,
        key,
        incarnation: crate::shards::raft::tests::INCARNATION,
        epoch: 0,
        probes: BTreeSet::new(),
        start: None,
    }
}

/// A member that leads the epoch is not stopped: the answer says it leads,
/// which leaves the group alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_leaves_a_leading_member_running() {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;

    let report = shards
        .stop_for_reform(&stop_survey(held.clone()))
        .await
        .expect("answered");
    assert!(report.leads, "{report:?}");
    let dir = reform_dir(&broker, &held, INCARNATION).expect("dir");
    assert_eq!(felix_storage::raft_copy::stopped(&dir).expect("read"), None);
    assert!(shards.held_copies().iter().any(|copy| copy.key == held));
}

/// A copy with no member running is stopped on disk before the answer, so
/// the read the answer carries is one no member can append past.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_syncs_its_marker_before_it_answers() {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};
    use felix_broker::EngineLogs;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;
    shards
        .close_through(&held.tenant_id, &held.namespace, &held.stream, INCARNATION)
        .await;

    let report = shards
        .stop_for_reform(&stop_survey(held.clone()))
        .await
        .expect("answered");
    assert!(!report.leads);
    let dir = reform_dir(&broker, &held, INCARNATION).expect("dir");
    let stopped = felix_storage::raft_copy::stopped(&dir)
        .expect("read")
        .expect("stopped");
    assert_eq!(stopped.epoch, 0);
    assert_eq!(stopped.last, report.current.last.map(|(_, index)| index));

    // A stop asked about another epoch stops nothing.
    let later = Survey {
        epoch: 1,
        ..stop_survey(held)
    };
    assert!(shards.stop_for_reform(&later).await.is_none());
}

/// A start is answered only once the copy holds the epoch it was asked to
/// start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_is_not_answered_before_the_copy_holds_the_epoch() {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;
    let start = Survey {
        kind: SurveyKind::Start,
        epoch: 1,
        ..stop_survey(held)
    };
    let read = shards.reform_reader();
    let answered = tokio::task::spawn_blocking(move || read(&start))
        .await
        .expect("read");
    assert!(answered.is_none(), "{answered:?}");
}
