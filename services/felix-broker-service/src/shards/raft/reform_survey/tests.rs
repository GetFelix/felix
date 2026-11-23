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
    tick(&link, "b1", None, read).await;
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
    tick(&old, "b1", None, Arc::clone(&read)).await;
    assert!(old.reform_reports().is_empty());
    let idle = FakeLink::default().reply("reform_surveys", serde_json::json!({ "surveys": [] }));
    tick(&idle, "b1", None, read).await;
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
        round: 1,
        key: sessions,
        incarnation: INCARNATION,
        epoch: 0,
        probes: BTreeSet::new(),
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
