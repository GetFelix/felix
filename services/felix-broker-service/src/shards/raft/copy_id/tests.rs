use reqwest::StatusCode;

use super::*;
use crate::shards::ShardKind;
use crate::test_support::fake_link::FakeLink;

fn key(stream: &str) -> ShardKey {
    ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: stream.to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    }
}

fn held(stream: &str, copy: u64, in_target: Option<u64>) -> Held {
    Held {
        key: key(stream),
        incarnation: 3,
        copy,
        set: ["n1", "n2"].map(String::from).into(),
        in_target,
    }
}

fn accepting() -> FakeLink {
    FakeLink::default().reply(
        "report_raft_copy_id",
        serde_json::json!({ "changed": true }),
    )
}

/// What each sent report carried, as (stream, copy id).
fn sent(link: &FakeLink) -> Vec<(String, u64)> {
    link.copy_ids()
        .iter()
        .map(|report| (report.key.stream.clone(), report.copy_id))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_copy_is_reported_with_its_registration_and_stream_incarnation() {
    let link = accepting();
    let mut ids = CopyIds::default();
    tick(
        &mut ids,
        &[held("a", 7, None)],
        (4, 1_000),
        &link,
        "n1",
        None,
    )
    .await;
    assert_eq!(
        link.copy_ids(),
        vec![RaftCopyId {
            key: key("a"),
            copy_id: 7,
            incarnation: 4,
            registered_at_millis: 1_000,
            shard_incarnation: 3,
        }]
    );
}

/// The control plane drops a shard's ids when its set changes and a
/// restore brings back older ones, so a taken report is not the end: only
/// a target naming the copy is.
#[tokio::test(start_paused = true)]
async fn a_taken_report_is_sent_again_until_the_target_names_the_copy() {
    let link = accepting();
    let mut ids = CopyIds::default();
    let waiting = [held("a", 7, None)];
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 1, "taken, so not sent at once again");

    tokio::time::advance(RESEND).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 2, "no target names it yet");

    // A target naming an older copy of this broker's is no better.
    tokio::time::advance(RESEND).await;
    tick(
        &mut ids,
        &[held("a", 7, Some(6))],
        (1, 1),
        &link,
        "n1",
        None,
    )
    .await;
    assert_eq!(sent(&link).len(), 3);

    tokio::time::advance(RESEND).await;
    tick(
        &mut ids,
        &[held("a", 7, Some(7))],
        (1, 1),
        &link,
        "n1",
        None,
    )
    .await;
    assert_eq!(sent(&link).len(), 3, "the target names the copy");
}

#[tokio::test(start_paused = true)]
async fn a_refused_report_is_retried() {
    let link = accepting().status("report_raft_copy_id", StatusCode::CONFLICT);
    let mut ids = CopyIds::default();
    let waiting = [held("a", 7, None)];
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 1, "waits before trying again");
    tokio::time::advance(RETRY).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 2);
}

/// Each refused report is an entry in the control plane's log, so a
/// report it keeps refusing waits longer each time, up to [`MAX_RETRY`].
#[tokio::test(start_paused = true)]
async fn a_report_refused_again_backs_off() {
    let link = accepting().status("report_raft_copy_id", StatusCode::CONFLICT);
    let mut ids = CopyIds::default();
    let waiting = [held("a", 7, None)];
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    tokio::time::advance(RETRY).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 2);
    tokio::time::advance(RETRY).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(
        sent(&link).len(),
        2,
        "the second refusal waits twice as long"
    );
    tokio::time::advance(RETRY).await;
    tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 3);

    for _ in 0..20 {
        tokio::time::advance(MAX_RETRY).await;
        tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
    }
    assert_eq!(sent(&link).len(), 23, "the wait stops growing at the cap");

    // A new copy is owed at once, and starts over from the first wait.
    tick(&mut ids, &[held("a", 8, None)], (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 24);
    tokio::time::advance(RETRY).await;
    tick(&mut ids, &[held("a", 8, None)], (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), 25);
}

/// A rotated copy, or a set the control plane has no ids for, is owed at
/// once, however recently the last report went.
#[tokio::test(start_paused = true)]
async fn a_new_copy_or_set_is_reported_at_once() {
    let link = accepting();
    let mut ids = CopyIds::default();
    tick(&mut ids, &[held("a", 7, None)], (1, 1), &link, "n1", None).await;
    tick(&mut ids, &[held("a", 8, None)], (1, 1), &link, "n1", None).await;
    let mut moved = held("a", 8, None);
    moved.set.insert("n3".to_string());
    tick(&mut ids, &[moved], (1, 1), &link, "n1", None).await;
    assert_eq!(
        sent(&link),
        vec![
            ("a".to_string(), 7),
            ("a".to_string(), 8),
            ("a".to_string(), 8)
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn one_tick_sends_at_most_its_share_and_the_rest_follow() {
    let link = accepting();
    let mut ids = CopyIds::default();
    let many: Vec<Held> = (0..MAX_PER_TICK + 3)
        .map(|i| held(&format!("s{i}"), 1, None))
        .collect();
    tick(&mut ids, &many, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), MAX_PER_TICK);
    tick(&mut ids, &many, (1, 1), &link, "n1", None).await;
    assert_eq!(sent(&link).len(), MAX_PER_TICK + 3);
}

/// A taken report starts the backoff over, so a later refusal waits only
/// the first interval again.
#[tokio::test(start_paused = true)]
async fn a_taken_report_resets_the_backoff() {
    let refusing = accepting().status("report_raft_copy_id", StatusCode::CONFLICT);
    let mut ids = CopyIds::default();
    let waiting = [held("a", 7, None)];
    for _ in 0..4 {
        tick(&mut ids, &waiting, (1, 1), &refusing, "n1", None).await;
        tokio::time::advance(MAX_RETRY).await;
    }
    tick(&mut ids, &waiting, (1, 1), &accepting(), "n1", None).await;
    tokio::time::advance(RESEND).await;
    tick(&mut ids, &waiting, (1, 1), &refusing, "n1", None).await;
    let before = sent(&refusing).len();
    tokio::time::advance(RETRY).await;
    tick(&mut ids, &waiting, (1, 1), &refusing, "n1", None).await;
    assert_eq!(sent(&refusing).len(), before + 1);
}

/// A report that got no answer wrote nothing to the control plane's log,
/// so it does not back off.
#[tokio::test(start_paused = true)]
async fn an_unanswered_report_does_not_back_off() {
    let link = FakeLink::default().fail("report_raft_copy_id");
    let mut ids = CopyIds::default();
    let waiting = [held("a", 7, None)];
    for _ in 0..5 {
        tick(&mut ids, &waiting, (1, 1), &link, "n1", None).await;
        tokio::time::advance(RETRY).await;
    }
    assert_eq!(sent(&link).len(), 5);
}
