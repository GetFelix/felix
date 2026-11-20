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
            previous_token: None,
            next_token: None,
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
        axum::routing::get(|| async { r#"{"last_log_index":null,"member":2}"# }),
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

/// The address configured for member 3 can reach member 2. The announce
/// asks member 3 to record, so member 2 refuses it instead of recording a
/// member that member 3 never heard from.
#[tokio::test]
async fn an_announce_is_recorded_only_by_the_member_it_is_for() {
    let dir = tempfile::tempdir().expect("tempdir");
    let second = Served::start(member_settings(2, dir.path()), None).await;
    let peers: BTreeMap<NodeId, String> = (1..=3).map(|id| (id, "127.0.0.1:1".into())).collect();
    second
        .handle
        .enter_group(
            peers,
            InitialClusterState::Existing,
            CancellationToken::new(),
        )
        .await
        .expect("enter group");
    let client = second
        .handle
        .security
        .client(Duration::from_secs(5))
        .expect("client");
    let base = format!("http://{}", second.addr);
    let forming = Forming {
        member: 1,
        members: BTreeSet::from([1, 2, 3]),
    };

    let refused = announce(&client, 3, &base, &forming)
        .await
        .expect_err("recorded by the wrong member");
    assert!(refused.contains("503"), "{refused}");
    assert!(
        second
            .handle
            .standing()
            .await
            .expect("standing")
            .formed
            .is_empty(),
        "member 2 recorded an announce meant for member 3"
    );

    announce(&client, 2, &base, &forming)
        .await
        .expect("recorded");
    assert_eq!(
        second.handle.standing().await.expect("standing").formed,
        [1]
    );
    second.stop().await;
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

    // Until it has entered the group it does not know its own, and a
    // record now could be of another group's member.
    let early = handle.record_forming(&forming(2, &[1, 2, 3])).await;
    assert!(matches!(early, Err(NotRecorded::Failed(_))), "{early:?}");
    assert!(handle.standing().await.expect("standing").formed.is_empty());

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

/// Something at an address that answers `standing` with `body`.
async fn answering(body: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = bind(None).await;
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let route = crate::raft::peer_route("standing");
    let router = axum::Router::new().route(&route, axum::routing::get(move || async move { body }));
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (base, server)
}

/// An empty answer counts for the member that gave it and no other. The
/// address configured for member 3 may reach member 2, and then member 3
/// has not been asked. An answer that names nobody counts for nobody.
#[tokio::test]
async fn an_empty_answer_counts_only_for_the_member_that_gave_it() {
    let client = reqwest::Client::new();
    let (second, server) = answering(r#"{"last_log_index":null,"member":2}"#).await;
    assert_eq!(probe(&client, 1, 2, Some(&second)).await, Answer::Empty);
    let misdirected = probe(&client, 1, 3, Some(&second)).await;
    assert!(
        matches!(&misdirected, Answer::Unreadable(why) if why.contains("answered by member 2")),
        "{misdirected:?}"
    );
    server.abort();

    // This member's own answer, at another member's address.
    let (mine, server) = answering(r#"{"last_log_index":null,"member":1}"#).await;
    let own = probe(&client, 1, 2, Some(&mine)).await;
    assert!(matches!(own, Answer::Unreadable(_)), "{own:?}");
    server.abort();

    let (unnamed, server) = answering(r#"{"last_log_index":null}"#).await;
    let nobody = probe(&client, 1, 2, Some(&unnamed)).await;
    assert!(
        matches!(&nobody, Answer::Unreadable(why) if why.contains("which member")),
        "{nobody:?}"
    );
    server.abort();

    // A record of this member is not one the asked member holds.
    let (named, server) = answering(r#"{"last_log_index":null,"formed":[1],"member":2}"#).await;
    assert_eq!(probe(&client, 1, 2, Some(&named)).await, Answer::NamesMe);
    let other = probe(&client, 1, 3, Some(&named)).await;
    assert!(matches!(other, Answer::Unreadable(_)), "{other:?}");
    server.abort();

    // A log shows the group exists, whoever holds it.
    let (holder, server) = answering(r#"{"last_log_index":4}"#).await;
    assert_eq!(probe(&client, 1, 3, Some(&holder)).await, Answer::Exists);
    server.abort();
}

/// The floor is the highest term every other member reports. A member that
/// gives no term, being withheld itself or on an older build, leaves it
/// unknown, and until it is known no leader is served (#2365).
#[tokio::test]
async fn the_floor_waits_for_every_member_and_takes_the_highest_term() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"last_log_index":4,"member":2,"term":3}"#).await;
    let (withheld, s3) = answering(r#"{"last_log_index":null,"member":3}"#).await;
    assert_eq!(
        term_of(&client, 2, Some(&second)).await,
        Ok(Reported::Voting(3))
    );
    assert!(
        term_of(&client, 3, Some(&second)).await.is_err(),
        "misdirected"
    );
    assert!(
        term_of(&client, 3, Some(&withheld)).await.is_err(),
        "no term and no word that it is withheld"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let member = Served::start(member_settings(1, dir.path()), None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let unknown = [(2, Some(second.clone())), (3, Some(withheld))];
    let learned = tokio::time::timeout(
        Duration::from_secs(1),
        handle.learn_floor(&client, &unknown, false),
    )
    .await;
    assert!(learned.is_err(), "a floor from a member that gave no term");
    assert!(
        !handle.serves_leader(1_000),
        "served before the floor is known"
    );

    let (third, s4) = answering(r#"{"member":3,"term":5}"#).await;
    handle
        .learn_floor(&client, &[(2, Some(second)), (3, Some(third))], false)
        .await;
    assert!(!handle.serves_leader(4), "served a leader below the floor");
    assert!(handle.serves_leader(5));
    handle.may_vote.store(true, Ordering::SeqCst);
    assert!(handle.serves_leader(0), "a voting member refused a leader");
    member.stop().await;
    for server in [s2, s3, s4] {
        server.abort();
    }
}

/// A member that says it withholds its vote counts as answered with no
/// term, so two empty members of five do not wait on each other forever
/// (#2365).
/// One that gives a term while saying it is withheld contradicts itself and
/// does not count.
#[tokio::test]
async fn a_member_that_says_it_is_withheld_answers_with_no_term() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"last_log_index":4,"member":2,"term":3}"#).await;
    let (withheld, s3) = answering(r#"{"last_log_index":null,"member":3,"withheld":true}"#).await;
    let (both, s4) = answering(r#"{"member":3,"term":7,"withheld":true}"#).await;
    let (fourth, s5) = answering(r#"{"member":4,"term":2}"#).await;
    let (fifth, s6) = answering(r#"{"member":5,"term":1}"#).await;
    assert_eq!(
        term_of(&client, 3, Some(&withheld)).await,
        Ok(Reported::Withheld(None))
    );
    assert!(
        term_of(&client, 3, Some(&both)).await.is_err(),
        "a term from a member that says it is withheld"
    );
    assert!(
        term_of(&client, 2, Some(&withheld)).await.is_err(),
        "misdirected"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let member = Served::start(member_settings(1, dir.path()), None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let standing = handle.standing().await.expect("standing");
    assert!(
        standing.withheld && standing.term.is_none() && standing.withheld_term == Some(0),
        "{standing:?}"
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        handle.learn_floor(
            &client,
            &[
                (2, Some(second)),
                (3, Some(withheld)),
                (4, Some(fourth)),
                (5, Some(fifth)),
            ],
            false,
        ),
    )
    .await
    .expect("the floor waited on a member that said it is withheld");
    assert!(!handle.serves_leader(2), "served a leader below the floor");
    assert!(handle.serves_leader(3));

    handle.may_vote.store(true, Ordering::SeqCst);
    let standing = handle.standing().await.expect("standing");
    assert!(
        !standing.withheld && standing.term.is_some() && standing.withheld_term.is_none(),
        "{standing:?}"
    );
    member.stop().await;
    for server in [s2, s3, s4, s5, s6] {
        server.abort();
    }
}

/// Within the tolerance a withheld answer counts as answered; past it the
/// members that reported a term cannot prove the floor.
#[test]
fn the_floor_is_proven_only_within_the_failure_tolerance() {
    // (members, members that reported a term, proven)
    for (members, with_terms, want) in [
        (3, 2, true),
        (3, 1, false),
        (5, 3, true),
        (5, 2, false),
        (4, 2, true),
        (4, 1, false),
        (1, 0, false),
    ] {
        assert_eq!(
            crate::raft::floor::proven(members, with_terms),
            want,
            "{with_terms} of {members}"
        );
    }
}

/// An answer that can change while the floor is being learned.
async fn answering_changing(
    body: Arc<std::sync::Mutex<&'static str>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = bind(None).await;
    let base = format!("http://{}", listener.local_addr().expect("addr"));
    let route = crate::raft::peer_route("standing");
    let router = axum::Router::new().route(
        &route,
        axum::routing::get(move || {
            let body = Arc::clone(&body);
            async move { *body.lock().expect("body") }
        }),
    );
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (base, server)
}

/// A withheld member of three, with this one wiped too, is past the
/// tolerance: the member blocks and serves no leader. Only an operator
/// waiver that names the withheld member, sent with the cluster-admin
/// credential after the block is reported, lets it go on (#2365).
#[tokio::test]
async fn past_the_failure_tolerance_the_floor_blocks_until_waived() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"member":2,"term":3}"#).await;
    let (withheld, s3) =
        answering(r#"{"last_log_index":null,"member":3,"withheld":true,"withheld_term":0}"#).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = member_settings(1, dir.path());
    settings.floor_report_after = Duration::from_millis(300);
    let member = Served::start(settings, None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let _ = handle.configured.set(BTreeSet::from([1, 2, 3]));
    let waive_url = format!(
        "http://{}{}",
        member.addr,
        crate::raft::peer_route("waive-lost")
    );
    let admin = handle
        .security
        .client(Duration::from_secs(5))
        .expect("client");
    let waive = |members: &'static str| {
        let body = format!(r#"{{"members":{members},"reason":"volume destroyed"}}"#);
        admin
            .post(&waive_url)
            .header("content-type", "application/json")
            .body(body)
            .send()
    };

    // Too early: nothing is reported yet, so no waiver is taken.
    let early = waive("[3]").await.expect("send");
    assert_eq!(early.status(), reqwest::StatusCode::CONFLICT);

    let learning = tokio::spawn({
        let handle = handle.clone();
        let client = client.clone();
        async move {
            handle
                .learn_floor(&client, &[(2, Some(second)), (3, Some(withheld))], false)
                .await;
        }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !learning.is_finished(),
        "went on past the failure tolerance"
    );
    assert!(!handle.serves_leader(3), "served a leader while blocked");
    let ready = handle.readiness().expect_err("ready while blocked");
    assert!(ready.contains("failure tolerance"), "{ready}");

    let anonymous = client
        .post(&waive_url)
        .header("content-type", "application/json")
        .body(r#"{"members":[3],"reason":"volume destroyed"}"#)
        .send()
        .await
        .expect("send");
    assert!(
        matches!(anonymous.status().as_u16(), 401 | 403),
        "{}",
        anonymous.status()
    );
    for unnamed in ["[]", "[2]", "[1,3]", "[3,9]"] {
        let refused = waive(unnamed).await.expect("send");
        assert_eq!(refused.status(), reqwest::StatusCode::CONFLICT, "{unnamed}");
    }
    let no_reason = admin
        .post(&waive_url)
        .header("content-type", "application/json")
        .body(r#"{"members":[3],"reason":" "}"#)
        .send()
        .await
        .expect("send");
    assert_eq!(no_reason.status(), reqwest::StatusCode::CONFLICT);
    assert!(!learning.is_finished(), "a refused waiver unblocked it");

    let waived = waive("[3]").await.expect("send");
    assert_eq!(waived.status(), reqwest::StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(5), learning)
        .await
        .expect("still blocked after the waiver")
        .expect("join");
    assert!(!handle.serves_leader(2), "served a leader below the floor");
    assert!(handle.serves_leader(3));
    assert!(handle.floor_gate.reported().is_none(), "block left behind");
    member.stop().await;
    for server in [s2, s3] {
        server.abort();
    }
}

/// The catch-up case: the withheld member beside this one is a member
/// still catching up, not a lost one. It serves the current leader, so
/// its catch-up finishes and it reports a term, which lifts the block with
/// no waiver (#2365).
#[tokio::test]
async fn the_block_lifts_once_a_withheld_member_reports_a_term() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"member":2,"term":3}"#).await;
    let third_says = Arc::new(std::sync::Mutex::new(
        r#"{"last_log_index":7,"member":3,"withheld":true,"withheld_term":3}"#,
    ));
    let (third, s3) = answering_changing(Arc::clone(&third_says)).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = member_settings(1, dir.path());
    settings.floor_report_after = Duration::from_millis(100);
    let member = Served::start(settings, None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let learning = tokio::spawn({
        let handle = handle.clone();
        let client = client.clone();
        async move {
            handle
                .learn_floor(&client, &[(2, Some(second)), (3, Some(third))], false)
                .await;
        }
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !learning.is_finished(),
        "went on past the failure tolerance"
    );
    assert!(handle.floor_gate.reported().is_some(), "block not reported");

    *third_says.lock().expect("body") = r#"{"last_log_index":9,"member":3,"term":4}"#;
    tokio::time::timeout(Duration::from_secs(5), learning)
        .await
        .expect("still blocked once every member reports a term")
        .expect("join");
    assert!(!handle.serves_leader(3), "served a leader below the floor");
    assert!(handle.serves_leader(4));
    assert!(handle.floor_gate.reported().is_none(), "block left behind");
    member.stop().await;
    for server in [s2, s3] {
        server.abort();
    }
}

/// A member resumed from an interrupted catch-up already serves a leader at
/// or above the floor it learned, and its stored vote refuses older ones.
/// Blocking it past the tolerance would strand the wiped member that waits
/// for it to report a term, so it does not block (#2365).
#[tokio::test]
async fn a_resumed_catch_up_does_not_block_past_the_tolerance() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"member":2,"term":3}"#).await;
    let (withheld, s3) = answering(r#"{"last_log_index":null,"member":3,"withheld":true}"#).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let member = Served::start(member_settings(1, dir.path()), None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(5),
        handle.learn_floor(&client, &[(2, Some(second)), (3, Some(withheld))], true),
    )
    .await
    .expect("a resumed catch-up blocked past the tolerance");
    assert!(!handle.serves_leader(2), "served a leader below the floor");
    assert!(handle.serves_leader(3));
    member.stop().await;
    for server in [s2, s3] {
        server.abort();
    }
}

/// A withheld member reports the term of the leader it has served, so a
/// waived floor can rise to it (#2824).
#[tokio::test]
async fn a_withheld_member_reports_the_term_of_the_leader_it_served() {
    let dir = tempfile::tempdir().expect("tempdir");
    let member = Served::start(member_settings(1, dir.path()), None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let before = handle.standing().await.expect("standing");
    assert_eq!(before.withheld_term, Some(0), "{before:?}");
    let rpc = openraft::raft::AppendEntriesRequest {
        vote: crate::raft::types::Vote {
            leader_id: crate::raft::types::LeaderId {
                term: 7,
                voted_for: 2,
            },
            committed: true,
        },
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    handle.raft.append_entries(rpc).await.expect("append").ok();
    let after = handle.standing().await.expect("standing");
    assert!(after.withheld && after.term.is_none(), "{after:?}");
    assert_eq!(after.withheld_term, Some(7), "{after:?}");
    member.stop().await;
}

/// Post a floor waiver for `members` to `member` with the cluster-admin
/// credential.
async fn waive_on(member: &Served, members: &str) -> reqwest::StatusCode {
    let url = format!(
        "http://{}{}",
        member.addr,
        crate::raft::peer_route("waive-lost")
    );
    member
        .handle
        .security
        .client(Duration::from_secs(5))
        .expect("client")
        .post(&url)
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"members":{members},"reason":"volume destroyed"}}"#
        ))
        .send()
        .await
        .expect("send")
        .status()
}

/// Two wipes in three: member 3 lost its volume, learned its floor from
/// member 1's term-2 leadership and follows that leader, still withheld.
/// Then member 1 (this one) lost its volume too. Member 2 is a deposed
/// term-1 leader that never heard of term 2. Under the waiver the floor
/// must be 2, from member 3: at 1 this member would serve member 2, which
/// could then commit over the term-2 leader's entries and ack a record a
/// later leader truncates (#2824).
#[tokio::test]
async fn a_waived_floor_takes_the_term_a_withheld_member_follows() {
    let client = reqwest::Client::new();
    let (deposed, s2) = answering(r#"{"member":2,"term":1}"#).await;
    let (following, s3) =
        answering(r#"{"last_log_index":1,"member":3,"withheld":true,"withheld_term":2}"#).await;
    assert_eq!(
        term_of(&client, 3, Some(&following)).await,
        Ok(Reported::Withheld(Some(2)))
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = member_settings(1, dir.path());
    settings.floor_report_after = Duration::from_millis(100);
    let member = Served::start(settings, None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let _ = handle.configured.set(BTreeSet::from([1, 2, 3]));
    let learning = tokio::spawn({
        let handle = handle.clone();
        let client = client.clone();
        async move {
            handle
                .learn_floor(&client, &[(2, Some(deposed)), (3, Some(following))], false)
                .await;
        }
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.floor_gate.reported().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("block not reported");
    assert_eq!(waive_on(&member, "[3]").await, reqwest::StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(5), learning)
        .await
        .expect("still blocked after the waiver")
        .expect("join");
    assert!(
        !handle.serves_leader(1),
        "served the deposed leader below the term a withheld member follows"
    );
    assert!(handle.serves_leader(2));
    member.stop().await;
    for server in [s2, s3] {
        server.abort();
    }
}

/// Within the tolerance a withheld member proves nothing, but its term
/// still raises the floor. In a real group a voter reports a term at least
/// as high, so this only checks that the term is taken.
#[tokio::test]
async fn a_withheld_members_term_raises_a_proven_floor() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"member":2,"term":1}"#).await;
    let (third, s3) = answering(r#"{"member":3,"withheld":true,"withheld_term":4}"#).await;
    let (fourth, s4) = answering(r#"{"member":4,"term":1}"#).await;
    let (fifth, s5) = answering(r#"{"member":5,"term":2}"#).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let member = Served::start(member_settings(1, dir.path()), None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let others = [
        (2, Some(second)),
        (3, Some(third)),
        (4, Some(fourth)),
        (5, Some(fifth)),
    ];
    tokio::time::timeout(
        Duration::from_secs(5),
        handle.learn_floor(&client, &others, false),
    )
    .await
    .expect("a proven floor blocked");
    assert!(!handle.serves_leader(3), "served a leader below the floor");
    assert!(handle.serves_leader(4));
    member.stop().await;
    for server in [s2, s3, s4, s5] {
        server.abort();
    }
}

/// Past the tolerance, a withheld member on a build that does not give its
/// term is waited on rather than taken as term 0: a waiver would otherwise
/// lean on a floor that may be too low. The block, and so the waiver,
/// comes once it gives the term.
#[tokio::test]
async fn past_the_tolerance_a_withheld_member_must_give_its_term() {
    let client = reqwest::Client::new();
    let (second, s2) = answering(r#"{"member":2,"term":1}"#).await;
    let third_says = Arc::new(std::sync::Mutex::new(r#"{"member":3,"withheld":true}"#));
    let (third, s3) = answering_changing(Arc::clone(&third_says)).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut settings = member_settings(1, dir.path());
    settings.floor_report_after = Duration::from_millis(100);
    let member = Served::start(settings, None).await;
    let handle = member.handle.clone();
    handle.may_vote.store(false, Ordering::SeqCst);
    let _ = handle.configured.set(BTreeSet::from([1, 2, 3]));
    let learning = tokio::spawn({
        let handle = handle.clone();
        let client = client.clone();
        async move {
            handle
                .learn_floor(&client, &[(2, Some(second)), (3, Some(third))], false)
                .await;
        }
    });
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !learning.is_finished(),
        "took a floor without member 3's term"
    );
    assert!(
        handle.floor_gate.reported().is_none(),
        "blocked, not waiting"
    );
    assert_eq!(
        waive_on(&member, "[3]").await,
        reqwest::StatusCode::CONFLICT,
        "a waiver while waiting on an answer"
    );

    *third_says.lock().expect("body") = r#"{"member":3,"withheld":true,"withheld_term":2}"#;
    tokio::time::timeout(Duration::from_secs(5), async {
        while handle.floor_gate.reported().is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("block not reported once member 3 gives its term");
    assert_eq!(waive_on(&member, "[3]").await, reqwest::StatusCode::OK);
    tokio::time::timeout(Duration::from_secs(5), learning)
        .await
        .expect("still blocked after the waiver")
        .expect("join");
    assert!(!handle.serves_leader(1), "served a leader below the floor");
    assert!(handle.serves_leader(2));
    member.stop().await;
    for server in [s2, s3] {
        server.abort();
    }
}

/// A waiver that misses a member withheld since it was given is dropped,
/// not kept to fire once the set shrinks back.
#[test]
fn a_waiver_that_misses_a_newly_withheld_member_is_dropped() {
    let gate = crate::raft::floor::FloorGate::default();
    let now = Duration::ZERO;
    let blocked = gate.judge(3, &BTreeSet::from([3]), 1, false, now);
    assert_eq!(
        blocked,
        crate::raft::floor::Verdict::Blocked { report: true }
    );
    let waiver = crate::raft::floor::Waiver {
        members: BTreeSet::from([3]),
        reason: "volume destroyed".to_string(),
    };
    gate.waive(waiver).expect("waive");
    let wider = gate.judge(5, &BTreeSet::from([3, 4]), 2, false, now);
    assert!(matches!(wider, crate::raft::floor::Verdict::Blocked { .. }));
    let back = gate.judge(5, &BTreeSet::from([3]), 2, false, now);
    assert!(
        matches!(back, crate::raft::floor::Verdict::Blocked { .. }),
        "an old waiver fired: {back:?}"
    );
}
