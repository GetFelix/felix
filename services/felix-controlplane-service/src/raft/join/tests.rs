use std::sync::Arc;

use super::*;

fn answers<const N: usize>(answers: [(NodeId, Answer); N]) -> BTreeMap<NodeId, Answer> {
    BTreeMap::from(answers)
}

fn down() -> Answer {
    Answer::Unreachable("connection refused".to_string())
}

fn refused() -> Answer {
    Answer::Unreadable("answered 403 Forbidden".to_string())
}

#[test]
fn every_member_empty_is_a_first_boot() {
    let all = answers([(2, Answer::Empty), (3, Answer::Empty)]);
    assert_eq!(decide(&all, &BTreeSet::new(), 3), Decision::Form);
    assert_eq!(decide(&answers([]), &BTreeSet::new(), 1), Decision::Form);
}

/// Two of three empty is a majority, and still not a first boot: the
/// silent member may hold the group.
#[test]
fn an_empty_majority_does_not_form_past_a_silent_member() {
    for third in [down(), refused()] {
        let some = answers([(2, Answer::Empty), (3, third)]);
        assert!(matches!(
            decide(&some, &BTreeSet::new(), 3),
            Decision::Wait(waiting) if waiting.len() == 1 && waiting[0].contains("member 3")
        ));
    }
}

#[test]
fn a_group_that_exists_is_never_formed_over() {
    let waived = BTreeSet::from([3]);
    for third in [Answer::Empty, down(), refused()] {
        let some = answers([(2, Answer::Exists), (3, third)]);
        assert_eq!(decide(&some, &BTreeSet::new(), 3), Decision::Exists);
        assert_eq!(decide(&some, &waived, 3), Decision::Exists);
    }
}

#[test]
fn a_waiver_excuses_only_silence() {
    let waived = BTreeSet::from([3]);
    let silent = answers([(2, Answer::Empty), (3, down())]);
    assert_eq!(decide(&silent, &waived, 3), Decision::Form);
    let answering = answers([(2, Answer::Empty), (3, Answer::Empty)]);
    assert_eq!(decide(&answering, &waived, 3), Decision::Form);
    let refusing = answers([(2, Answer::Empty), (3, refused())]);
    assert!(matches!(decide(&refusing, &waived, 3), Decision::Wait(_)));
    // The waiver names slot 3, not slot 2.
    let other = answers([(2, down()), (3, Answer::Empty)]);
    assert!(matches!(decide(&other, &waived, 3), Decision::Wait(_)));
}

/// The members that answer empty must be a majority on their own, so a
/// waiver can name at most a minority.
#[test]
fn a_waiver_cannot_name_a_majority() {
    let waived = BTreeSet::from([2, 3]);
    let alone = answers([(2, down()), (3, down())]);
    assert!(matches!(
        decide(&alone, &waived, 3),
        Decision::Wait(waiting) if waiting[0].contains("not a majority")
    ));
    let waived = BTreeSet::from([4, 5]);
    let three_of_five = answers([
        (2, Answer::Empty),
        (3, Answer::Empty),
        (4, down()),
        (5, down()),
    ]);
    assert_eq!(decide(&three_of_five, &waived, 5), Decision::Form);
    let waived = BTreeSet::from([3, 4, 5]);
    let two_of_five = answers([(2, Answer::Empty), (3, down()), (4, down()), (5, down())]);
    assert!(matches!(
        decide(&two_of_five, &waived, 5),
        Decision::Wait(_)
    ));
}

struct NullApp;

#[async_trait::async_trait]
impl crate::raft::AppStateMachine for NullApp {
    async fn apply(&self, _command: &[u8]) -> Vec<u8> {
        Vec::new()
    }
    async fn snapshot(&self) -> Vec<u8> {
        Vec::new()
    }
    async fn restore(&self, _snapshot: &[u8]) {}
}

fn settings(dir: &std::path::Path) -> crate::raft::RaftSettings {
    let mut settings = crate::raft::RaftSettings::new(
        1,
        dir.into(),
        crate::raft::PeerSecurity {
            cluster_id: "test-cluster".to_string(),
            token: Some("0123456789abcdef0123456789abcdef".to_string()),
            tls: None,
        },
    );
    settings.heartbeat_interval = Duration::from_millis(50);
    settings.election_timeout = (Duration::from_millis(150), Duration::from_millis(300));
    settings
}

async fn wait_for(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A crash after the waiver is removed and before `initialize` leaves an
/// empty member with no waiver. It holds no state, so the tool accepts it
/// again and the next start forms.
#[tokio::test]
async fn a_crash_between_the_waiver_and_initialize_leaves_a_member_the_tool_accepts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let peers = BTreeMap::from([(1, "127.0.0.1:1".to_string())]);
    let waived = BTreeSet::from([2]);
    let waiver_file = dir.path().join("form-without");
    waiver::form_without(dir.path(), "test-cluster", &waived)
        .await
        .expect("write the waiver");

    let mut crashing = settings(dir.path());
    crashing.faults.stop_before_initialize = true;
    let handle = RaftHandle::start(crashing, Arc::new(NullApp))
        .await
        .expect("start");
    handle
        .enter_group(
            peers.clone(),
            InitialClusterState::New,
            CancellationToken::new(),
        )
        .await
        .expect("enter group");
    wait_for("the waiver to be removed", || !waiver_file.exists()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(handle.status().voters.is_empty(), "initialized");
    handle.shutdown().await.expect("shutdown");

    waiver::form_without(dir.path(), "test-cluster", &waived)
        .await
        .expect("a member that never initialized takes the waiver again");
    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("restart");
    handle
        .enter_group(peers, InitialClusterState::New, CancellationToken::new())
        .await
        .expect("enter group");
    wait_for("the group to form", || handle.status().leader == Some(1)).await;
    assert!(!waiver_file.exists(), "the waiver outlived the start");
    handle.shutdown().await.expect("shutdown");
}
