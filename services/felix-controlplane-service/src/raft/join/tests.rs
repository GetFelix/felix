use std::sync::Arc;

use super::*;

fn answers<const N: usize>(answers: [(NodeId, Answer); N]) -> BTreeMap<NodeId, Answer> {
    BTreeMap::from(answers)
}

fn down() -> Answer {
    Answer::Unreachable("connection refused".to_string())
}

fn form<const N: usize>(answered: [NodeId; N]) -> Decision {
    Decision::Form(BTreeSet::from(answered))
}

const FORMING: Mark = Mark {
    forming: true,
    reached: false,
};

fn refused() -> Answer {
    Answer::Unreadable("answered 403 Forbidden".to_string())
}

#[test]
fn every_member_empty_is_a_first_boot() {
    let all = answers([(2, Answer::Empty), (3, Answer::Empty)]);
    assert_eq!(
        decide(&all, &BTreeSet::new(), 3, Mark::default()),
        form([2, 3])
    );
    assert_eq!(
        decide(&answers([]), &BTreeSet::new(), 1, Mark::default()),
        form([])
    );
}

/// Two of three empty is a majority, and still not a first boot: the
/// silent member may hold the group.
#[test]
fn an_empty_majority_does_not_form_past_a_silent_member() {
    for third in [down(), refused()] {
        let some = answers([(2, Answer::Empty), (3, third)]);
        assert!(matches!(
            decide(&some, &BTreeSet::new(), 3, Mark::default()),
            Decision::Wait(waiting) if waiting.len() == 1 && waiting[0].contains("member 3")
        ));
    }
}

#[test]
fn a_group_that_exists_is_never_formed_over() {
    let waived = BTreeSet::from([3]);
    for third in [Answer::Empty, down(), refused()] {
        let some = answers([(2, Answer::Exists), (3, third)]);
        assert_eq!(
            decide(&some, &BTreeSet::new(), 3, Mark::default()),
            Decision::Exists
        );
        assert_eq!(decide(&some, &waived, 3, Mark::default()), Decision::Exists);
    }
}

#[test]
fn a_waiver_excuses_only_silence() {
    let waived = BTreeSet::from([3]);
    let silent = answers([(2, Answer::Empty), (3, down())]);
    // The silent slot is not among the members that must record this one.
    assert_eq!(decide(&silent, &waived, 3, Mark::default()), form([2]));
    let answering = answers([(2, Answer::Empty), (3, Answer::Empty)]);
    assert_eq!(
        decide(&answering, &waived, 3, Mark::default()),
        form([2, 3])
    );
    let refusing = answers([(2, Answer::Empty), (3, refused())]);
    assert!(matches!(
        decide(&refusing, &waived, 3, Mark::default()),
        Decision::Wait(_)
    ));
    // The waiver names slot 3, not slot 2.
    let other = answers([(2, down()), (3, Answer::Empty)]);
    assert!(matches!(
        decide(&other, &waived, 3, Mark::default()),
        Decision::Wait(_)
    ));
}

/// The members that answer empty must be a majority on their own, so a
/// waiver can name at most a minority.
#[test]
fn a_waiver_cannot_name_a_majority() {
    let waived = BTreeSet::from([2, 3]);
    let alone = answers([(2, down()), (3, down())]);
    assert!(matches!(
        decide(&alone, &waived, 3, Mark::default()),
        Decision::Wait(waiting) if waiting[0].contains("not a majority")
    ));
    let waived = BTreeSet::from([4, 5]);
    let three_of_five = answers([
        (2, Answer::Empty),
        (3, Answer::Empty),
        (4, down()),
        (5, down()),
    ]);
    assert_eq!(
        decide(&three_of_five, &waived, 5, Mark::default()),
        form([2, 3])
    );
    let waived = BTreeSet::from([3, 4, 5]);
    let two_of_five = answers([(2, Answer::Empty), (3, down()), (4, down()), (5, down())]);
    assert!(matches!(
        decide(&two_of_five, &waived, 5, Mark::default()),
        Decision::Wait(_)
    ));
}

/// #2374: a peer that recorded this member, which holds no mark, means the
/// volume that held the mark, and maybe a vote, is gone. One such answer is
/// enough, whatever the others say, and no waiver excuses it.
#[test]
fn a_member_named_without_its_mark_lost_its_volume() {
    let waived = BTreeSet::from([3]);
    for third in [Answer::Empty, Answer::NamesMe, down(), refused()] {
        let named = answers([(2, Answer::NamesMe), (3, third)]);
        assert_eq!(
            decide(&named, &BTreeSet::new(), 3, Mark::default()),
            Decision::Lost
        );
        assert_eq!(decide(&named, &waived, 3, Mark::default()), Decision::Lost);
    }
    // A group that exists is caught up with either way.
    let exists = answers([(2, Answer::NamesMe), (3, Answer::Exists)]);
    assert_eq!(
        decide(&exists, &BTreeSet::new(), 3, Mark::default()),
        Decision::Exists
    );
}

/// With its mark the member was interrupted before it voted. Being named
/// is then its own earlier request, and it goes on: the naming members
/// count as empty and are asked to record it again.
#[test]
fn a_member_named_with_its_mark_goes_on_forming() {
    let named = answers([(2, Answer::NamesMe), (3, Answer::NamesMe)]);
    assert_eq!(decide(&named, &BTreeSet::new(), 3, FORMING), form([2, 3]));
    let mixed = answers([(2, Answer::NamesMe), (3, Answer::Empty)]);
    assert_eq!(decide(&mixed, &BTreeSet::new(), 3, FORMING), form([2, 3]));
    // The mark excuses nothing else.
    let silent = answers([(2, Answer::NamesMe), (3, down())]);
    assert!(matches!(
        decide(&silent, &BTreeSet::new(), 3, FORMING),
        Decision::Wait(_)
    ));
    let exists = answers([(2, Answer::NamesMe), (3, Answer::Exists)]);
    assert_eq!(
        decide(&exists, &BTreeSet::new(), 3, FORMING),
        Decision::Exists
    );
}

/// #2407: a member that reached the vote from this store and finds its log
/// blank lost the log, which held its vote. Its mark is still there, so
/// without the flag it would go on forming. It never forms, whatever the
/// answers, the mark or a waiver say.
#[test]
fn a_member_that_reached_the_vote_with_a_blank_log_lost_it() {
    let waived = BTreeSet::from([3]);
    for forming in [true, false] {
        let mark = Mark {
            forming,
            reached: true,
        };
        for second in [Answer::Empty, Answer::NamesMe] {
            for third in [Answer::Empty, Answer::NamesMe, down(), refused()] {
                let given = answers([(2, second.clone()), (3, third)]);
                assert_eq!(decide(&given, &BTreeSet::new(), 3, mark), Decision::Lost);
                assert_eq!(decide(&given, &waived, 3, mark), Decision::Lost);
            }
        }
        assert_eq!(
            decide(&answers([]), &BTreeSet::new(), 1, mark),
            Decision::Lost
        );
        let exists = answers([(2, Answer::NamesMe), (3, Answer::Exists)]);
        assert_eq!(decide(&exists, &BTreeSet::new(), 3, mark), Decision::Exists);
    }
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
    member_settings(1, dir)
}

fn member_settings(id: NodeId, dir: &std::path::Path) -> crate::raft::RaftSettings {
    let mut settings = crate::raft::RaftSettings::new(
        id,
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

    static STOPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let mut crashing = settings(dir.path());
    crashing.faults.stop_before_initialize = Some(&STOPPED);
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
    wait_for("the member to stop short of initialize", || {
        STOPPED.load(Ordering::SeqCst) == 1
    })
    .await;
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

/// A member serving its peer routes on `addr`, or on a free port.
struct Served {
    handle: RaftHandle,
    addr: std::net::SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl Served {
    async fn start(
        settings: crate::raft::RaftSettings,
        addr: Option<std::net::SocketAddr>,
    ) -> Self {
        let handle = RaftHandle::start(settings, Arc::new(NullApp))
            .await
            .expect("start");
        let listener = bind(addr).await;
        let addr = listener.local_addr().expect("addr");
        let router = handle.rpc_router();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Self {
            handle,
            addr,
            server,
        }
    }

    async fn stop(self) {
        self.handle.shutdown().await.expect("shutdown");
        self.server.abort();
        let _ = self.server.await;
    }
}

/// A restart can find the stopped server still holding the port.
async fn bind(addr: Option<std::net::SocketAddr>) -> tokio::net::TcpListener {
    let addr = addr.unwrap_or("127.0.0.1:0".parse().expect("addr"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => return listener,
            Err(err) if tokio::time::Instant::now() >= deadline => panic!("bind {addr}: {err}"),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// #2374: every member is stopped after its peers recorded it and before
/// `initialize`, as three crashes there would. On the restart every peer
/// names every member. Each holds its own mark and has not reached the
/// vote, so each goes on and the group forms. Without the mark all three
/// would read as members that lost a volume, and nobody could ever form; the
/// same if the reached flag were written before `initialize` (#2407).
#[tokio::test]
async fn a_member_interrupted_after_its_mark_resumes_forming() {
    let dirs: Vec<tempfile::TempDir> = (0..3)
        .map(|_| tempfile::tempdir().expect("tempdir"))
        .collect();
    static STOPPED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let mut members = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let mut crashing = member_settings(i as NodeId + 1, dir.path());
        crashing.faults.stop_before_initialize = Some(&STOPPED);
        members.push(Served::start(crashing, None).await);
    }
    let peers: BTreeMap<NodeId, String> = members
        .iter()
        .map(|member| (member.handle.id, member.addr.to_string()))
        .collect();
    for member in &members {
        member
            .handle
            .enter_group(
                peers.clone(),
                InitialClusterState::New,
                CancellationToken::new(),
            )
            .await
            .expect("enter group");
    }
    // Each reached the vote step: its mark is written, the others recorded
    // it, and it stopped short of initialize.
    wait_for("every member to reach its vote", || {
        STOPPED.load(Ordering::SeqCst) == 3
    })
    .await;
    for member in &members {
        let handle = &member.handle;
        assert!(handle.member.forming_mark().await.expect("mark"));
        assert!(!handle.member.reached().await.expect("reached"));
        assert!(handle.status().voters.is_empty(), "initialized");
    }
    for member in &members {
        let others: BTreeSet<NodeId> = peers
            .keys()
            .copied()
            .filter(|id| *id != member.handle.id)
            .collect();
        assert_eq!(member.handle.member.formed().await.expect("formed"), others);
    }
    let addrs: Vec<_> = members.iter().map(|member| member.addr).collect();
    for member in members {
        member.stop().await;
    }

    let mut members = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        let settings = member_settings(i as NodeId + 1, dir.path());
        members.push(Served::start(settings, Some(addrs[i])).await);
    }
    for member in &members {
        member
            .handle
            .enter_group(
                peers.clone(),
                InitialClusterState::New,
                CancellationToken::new(),
            )
            .await
            .expect("enter group");
    }
    wait_for("the group to form and elect a leader", || {
        members
            .iter()
            .all(|member| member.handle.status().leader.is_some())
    })
    .await;
    for member in members {
        member.stop().await;
    }
}

/// A peer on a build without the forming route answers `standing` as an
/// empty member and 404 to `forming`. That is no record, so the member
/// keeps its mark and does not vote.
#[tokio::test]
async fn a_404_on_forming_blocks_the_vote() {
    let listener = bind(None).await;
    let old = listener.local_addr().expect("addr");
    let router = axum::Router::new().route(
        &crate::raft::peer_route("standing"),
        axum::routing::get(|| async { r#"{"last_log_index":null}"# }),
    );
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let member = Served::start(member_settings(1, dir.path()), None).await;
    let peers = BTreeMap::from([(1, member.addr.to_string()), (2, old.to_string())]);
    member
        .handle
        .enter_group(peers, InitialClusterState::New, CancellationToken::new())
        .await
        .expect("enter group");
    let handle = member.handle.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !handle.member.forming_mark().await.expect("mark") {
        assert!(tokio::time::Instant::now() < deadline, "no mark written");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!handle.may_vote.load(Ordering::SeqCst), "votes unrecorded");
    assert!(handle.status().voters.is_empty(), "initialized");
    assert!(!handle.member.holds_state().expect("state"));
    member.stop().await;
    server.abort();
}

/// A peer records a forming member only for the group it is configured
/// with itself, and never itself or a member outside the group.
#[tokio::test]
async fn a_peer_records_only_its_own_group() {
    let dir = tempfile::tempdir().expect("tempdir");
    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("start");
    let forming = |member, members: &[NodeId]| Forming {
        member,
        members: members.iter().copied().collect(),
    };
    for refused in [forming(1, &[1, 2, 3]), forming(4, &[1, 2, 3])] {
        let refusal = handle.record_forming(&refused).await;
        assert!(
            matches!(refusal, Err(NotRecorded::Refused(_))),
            "{refused:?}"
        );
    }

    let peers: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, "127.0.0.1:1".into())).collect();
    handle
        .enter_group(
            peers,
            InitialClusterState::Existing,
            CancellationToken::new(),
        )
        .await
        .expect("enter group");
    for other in [forming(2, &[1, 2]), forming(2, &[1, 2, 3, 4])] {
        let refusal = handle.record_forming(&other).await;
        assert!(matches!(refusal, Err(NotRecorded::Refused(_))), "{other:?}");
    }
    assert!(handle.standing().await.expect("standing").formed.is_empty());

    handle
        .record_forming(&forming(2, &[1, 2, 3]))
        .await
        .expect("record");
    handle
        .record_forming(&forming(2, &[1, 2, 3]))
        .await
        .expect("record again");
    assert_eq!(handle.standing().await.expect("standing").formed, [2]);
    // Neither the record nor a mark is consensus state: the member still
    // decides as an empty one, and the waiver tool still accepts it.
    handle.member.mark_forming().await.expect("mark");
    assert!(!handle.member.holds_state().expect("state"));
    handle.shutdown().await.expect("shutdown");

    // Both are on disk.
    let handle = RaftHandle::start(settings(dir.path()), Arc::new(NullApp))
        .await
        .expect("restart");
    assert_eq!(handle.standing().await.expect("standing").formed, [2]);
    assert!(handle.member.forming_mark().await.expect("mark"));
    assert!(!handle.member.holds_state().expect("state"));
    handle.shutdown().await.expect("shutdown");
}
