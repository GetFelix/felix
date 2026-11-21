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
