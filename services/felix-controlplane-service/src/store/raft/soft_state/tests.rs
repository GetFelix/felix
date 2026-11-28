//! The leader's soft state on a single-member group, which leads at once.
use std::time::Duration;

use crate::model::{Node, NodeLifecycle};
use crate::store::ControlPlaneStore;
use crate::store::contract::nodes::node;
use crate::store::raft::tests::single_node_store;

fn applied(store: &crate::store::raft::RaftStore) -> Option<u64> {
    store.handle.status().last_applied_index
}

fn registered(node_id: &str, port: u16) -> Node {
    let mut node = node(node_id, port);
    // Long before this leader began: a replicated time that alone would
    // make the node stale by any cutoff below.
    node.status.last_heartbeat_at_millis = 1;
    node
}

/// A heartbeat is the leader's to remember, not the log's: no entry per
/// heartbeat, and the leader still reports the latest one.
#[tokio::test]
async fn heartbeats_write_nothing_to_the_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    let before = applied(&store);

    let mut last = 0;
    for _ in 0..20 {
        last = store
            .record_node_heartbeat("broker-a", 0, 0)
            .await
            .expect("heartbeat")
            .status
            .last_heartbeat_at_millis;
    }

    assert_eq!(applied(&store), before, "a heartbeat became a log entry");
    assert!(last > 1, "the leader's clock stamps the heartbeat");
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .last_heartbeat_at_millis,
        last,
        "the leader serves the heartbeat it holds"
    );
}

/// A leader that has just started judging has heard from nobody, and must
/// not read that as everybody having gone quiet: the full window runs from
/// when it began, whatever the log says about older heartbeats.
#[tokio::test]
async fn a_new_leader_expires_nobody_until_a_full_window_has_passed() {
    const WINDOW: Duration = Duration::from_millis(600);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");

    let cutoff = |store_now: u64| store_now.saturating_sub(WINDOW.as_millis() as u64);
    let now = store.now_millis().await.expect("now");
    let early = store.expire_stale_nodes(cutoff(now)).await.expect("sweep");
    assert!(
        early.is_empty(),
        "expired before the new leader had waited a window: {early:?}"
    );

    tokio::time::sleep(WINDOW + Duration::from_millis(200)).await;
    let now = store.now_millis().await.expect("now");
    let late = store.expire_stale_nodes(cutoff(now)).await.expect("sweep");
    assert_eq!(
        late.iter().map(|n| n.node_id.as_str()).collect::<Vec<_>>(),
        vec!["broker-a"],
        "a node silent for a whole window under this leader is expired"
    );
    assert_eq!(late[0].status.lifecycle, NodeLifecycle::Down);
}

/// A heartbeat within the window keeps the node, even though the log never
/// saw it.
#[tokio::test]
async fn a_soft_heartbeat_keeps_a_node_alive() {
    const WINDOW: Duration = Duration::from_millis(400);
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    store
        .register_node(registered("broker-b", 7002))
        .await
        .expect("register");

    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        store
            .record_node_heartbeat("broker-a", 0, 0)
            .await
            .expect("heartbeat");
    }
    let now = store.now_millis().await.expect("now");
    let expired = store
        .expire_stale_nodes(now - WINDOW.as_millis() as u64)
        .await
        .expect("sweep");
    assert_eq!(
        expired
            .iter()
            .map(|n| n.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["broker-b"]
    );
}

/// Renewing the placement lease is soft state too; only a change of holder
/// is written.
#[tokio::test]
async fn renewing_the_placement_lease_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    let first = store
        .acquire_placement_lease("instance-a", 60_000)
        .await
        .expect("acquire")
        .expect("granted");
    assert!(first.taken);
    let before = applied(&store);
    for _ in 0..10 {
        let renewed = store
            .acquire_placement_lease("instance-a", 60_000)
            .await
            .expect("renew")
            .expect("granted");
        assert!(!renewed.taken);
        assert_eq!(renewed.token, first.token);
    }
    assert_eq!(applied(&store), before, "a renewal became a log entry");
}

/// A node deleted and registered again under the same id starts over: what
/// the leader heard from the old record does not keep the new one alive.
#[tokio::test]
async fn a_recreated_node_does_not_inherit_old_heartbeats() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    store
        .record_node_heartbeat("broker-a", 0, 0)
        .await
        .expect("heartbeat");
    store.delete_node("broker-a").await.expect("delete");
    let again = store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register again");

    assert_eq!(
        store.get_node("broker-a").await.expect("get"),
        again,
        "the old record's heartbeat leaked into the new one"
    );
}

/// The log's heartbeat for a node that left can trail a beat the previous
/// leader granted, and placement waits out the lease from it. So a new
/// leader reads such a node as heard from when it began judging, as expiry
/// does, and a heartbeat after leaving moves nothing.
#[tokio::test]
async fn a_departed_node_is_heard_from_no_earlier_than_this_leader_began() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-a", 7001))
        .await
        .expect("register");
    store
        .set_node_lifecycle("broker-a", NodeLifecycle::Left)
        .await
        .expect("leave");
    // Starts this leader's judging, so the reading below holds still.
    store.expire_stale_nodes(0).await.expect("sweep");

    let left = store
        .get_node("broker-a")
        .await
        .expect("get")
        .status
        .last_heartbeat_at_millis;
    assert!(left > 1, "read as last heard from before this leader began");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let answer = store
        .record_node_heartbeat("broker-a", 0, 0)
        .await
        .expect("heartbeat");
    assert_eq!(answer.status.lifecycle, NodeLifecycle::Left);
    assert_eq!(
        store
            .get_node("broker-a")
            .await
            .expect("get")
            .status
            .last_heartbeat_at_millis,
        left,
        "a heartbeat after leaving moved the stamp",
    );
}

/// Passes entries through to the metadata machine, holding every apply while
/// paused.
struct HeldApplies {
    machine: std::sync::Arc<crate::store::raft::state_machine::MetadataStateMachine>,
    paused: tokio::sync::watch::Sender<bool>,
}

#[async_trait::async_trait]
impl crate::raft::AppStateMachine for HeldApplies {
    async fn apply(&self, command: &[u8]) -> Vec<u8> {
        let mut paused = self.paused.subscribe();
        paused
            .wait_for(|paused| !*paused)
            .await
            .expect("pause sender");
        self.machine.apply(command).await
    }
    async fn snapshot(&self) -> Vec<u8> {
        self.machine.snapshot().await
    }
    async fn restore(&self, snapshot: &[u8]) {
        self.machine.restore(snapshot).await
    }
    fn restamp(&self, command: &[u8], now_millis: u64) -> Option<Vec<u8>> {
        self.machine.restamp(command, now_millis)
    }
    fn settle_for_level(&self, command: &[u8], level: u16) -> Result<Option<Vec<u8>>, Vec<u8>> {
        self.machine.settle_for_level(command, level)
    }
    fn depends_on_level(&self, command: &[u8]) -> bool {
        self.machine.depends_on_level(command)
    }
    fn version(&self) -> u16 {
        self.machine.version()
    }
    fn join_floor(&self) -> u16 {
        self.machine.join_floor()
    }
}

/// A member restarted with an entry past the committed index it saved wins
/// the next term. Until its blank entry applies, it has not applied the
/// take of the lease that entry holds, and must not hand the lease to
/// someone else on that state.
#[tokio::test]
async fn a_restarted_leader_does_not_judge_before_its_blank_entry_applies() {
    use crate::raft::{AppStateMachine, RaftHandle, RaftSettings};
    use crate::store::StoreConfig;
    use crate::store::raft::command::{MetaCommand, encode_command};
    use crate::store::raft::state_machine::MetadataStateMachine;
    use crate::store::raft::{InMemoryStore, RaftStore};

    let dir = tempfile::tempdir().expect("tempdir");
    crate::raft::seed_unsaved_commands(
        dir.path(),
        vec![encode_command(&MetaCommand::TakePlacementLease {
            holder: "instance-a".to_string(),
        })],
    )
    .await;

    let machine = std::sync::Arc::new(MetadataStateMachine::new(std::sync::Arc::new(
        InMemoryStore::new(StoreConfig {
            changes_limit: 100,
            change_retention_max_rows: Some(1_000),
        }),
    )));
    let (paused, _) = tokio::sync::watch::channel(true);
    let app = std::sync::Arc::new(HeldApplies {
        machine: std::sync::Arc::clone(&machine),
        paused: paused.clone(),
    });
    let mut settings = RaftSettings::new(
        1,
        dir.path().into(),
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
    let handle = RaftHandle::start(settings, app as std::sync::Arc<dyn AppStateMachine>)
        .await
        .expect("start raft");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !handle.is_leader() || handle.current_term() <= 5 {
        assert!(std::time::Instant::now() < deadline, "never led a new term");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let store = std::sync::Arc::new(RaftStore::new(handle, machine));

    let asker = std::sync::Arc::clone(&store);
    let asked =
        tokio::spawn(async move { asker.acquire_placement_lease("instance-b", 60_000).await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    // Still waiting, or the take applied before it asked and this proves
    // nothing.
    assert!(!asked.is_finished(), "answered before the take applied");
    paused.send_replace(false);

    let answer = asked.await.expect("ask task").expect("acquire");
    assert!(
        answer.is_none(),
        "a second holder took the lease on state the leader had not applied: {answer:?}"
    );
    assert_eq!(
        store
            .local()
            .expect("caught up")
            .placement_holder()
            .await
            .as_deref(),
        Some("instance-a")
    );
    store.handle.shutdown().await.expect("shutdown");
}

/// A take decided in one term is not written in the next, even by the same
/// member: what it read was judged in a term that has ended.
#[tokio::test]
async fn a_lease_decided_in_one_term_is_not_taken_in_the_next() {
    use super::LeaderRequest;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    let soft = std::sync::Arc::clone(&store.soft);
    let term = store.handle.current_term();
    // Through the blank barrier first, so the ask below stops only at the
    // judging slot.
    soft.dispatch(LeaderRequest::ReleasePlacementLease {
        holder: "nobody".to_string(),
    })
    .await
    .expect("leading")
    .expect("answered");

    let slot = soft.serial.lock().await;
    let asker = std::sync::Arc::clone(&soft);
    let asked = tokio::spawn(async move {
        asker
            .dispatch(LeaderRequest::AcquirePlacementLease {
                holder: "instance-b".to_string(),
                ttl_millis: 60_000,
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !asked.is_finished(),
        "answered without waiting for the slot"
    );

    // No member 1000 exists.
    assert!(
        store.handle.give_up_term(1_000).await,
        "the transfer vote was refused"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !store.handle.is_leader() || store.handle.current_term() <= term {
        assert!(std::time::Instant::now() < deadline, "never led a new term");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(slot);

    let answer = asked.await.expect("ask task");
    assert!(
        answer.is_err(),
        "a take decided in term {term} was answered after the term moved: {answer:?}"
    );
    assert_eq!(
        store.local().expect("caught up").placement_holder().await,
        None,
        "the take was written in a term it was not decided in"
    );
    // The append alone, as a handler past its checks would make it.
    let late = soft
        .propose(
            term,
            crate::store::raft::command::MetaCommand::TakePlacementLease {
                holder: "instance-b".to_string(),
            },
        )
        .await;
    assert!(late.is_err(), "appended in a later term: {late:?}");
    assert_eq!(
        store.local().expect("caught up").placement_holder().await,
        None,
        "the take was written in a term it was not decided in"
    );
    store.handle.shutdown().await.expect("shutdown");
}

/// A heartbeat that waited out a term change records nothing: the view it
/// would write belongs to a term this member no longer leads.
#[tokio::test]
async fn a_heartbeat_that_waited_out_a_term_records_nothing() {
    use super::LeaderRequest;

    let dir = tempfile::tempdir().expect("tempdir");
    let store = single_node_store(dir.path()).await;
    store
        .register_node(registered("broker-b", 7002))
        .await
        .expect("register");
    let soft = std::sync::Arc::clone(&store.soft);
    let term = store.handle.current_term();
    soft.dispatch(LeaderRequest::ReleasePlacementLease {
        holder: "nobody".to_string(),
    })
    .await
    .expect("leading")
    .expect("answered");

    let slot = soft.serial.lock().await;
    let asker = std::sync::Arc::clone(&soft);
    let asked = tokio::spawn(async move {
        asker
            .dispatch(LeaderRequest::Heartbeat {
                node_id: "broker-b".to_string(),
                incarnation: 0,
            })
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !asked.is_finished(),
        "answered without waiting for the slot"
    );

    // No member 1000 exists.
    assert!(
        store.handle.give_up_term(1_000).await,
        "the transfer vote was refused"
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !store.handle.is_leader() || store.handle.current_term() <= term {
        assert!(std::time::Instant::now() < deadline, "never led a new term");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(slot);

    let answer = asked.await.expect("ask task");
    assert!(answer.is_err(), "answered after the term moved: {answer:?}");
    let beat = {
        let view = soft.view.lock().expect("soft state lock");
        view.as_ref()
            .and_then(|view| view.beats.get("broker-b").map(|_| view.term))
    };
    assert_eq!(beat, None, "a heartbeat judged in term {term} was recorded");
    store.handle.shutdown().await.expect("shutdown");
}

struct Member {
    store: std::sync::Arc<crate::store::raft::RaftStore>,
    server: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

/// A formed two-member group over real peer listeners.
async fn two_member_group() -> Vec<Member> {
    use crate::raft::{AppStateMachine, RaftHandle, RaftSettings};
    use crate::store::StoreConfig;
    use crate::store::raft::InMemoryStore;
    use crate::store::raft::state_machine::MetadataStateMachine;
    use std::collections::BTreeMap;
    let mut started = Vec::new();
    for id in 1..=2 {
        let dir = tempfile::tempdir().expect("tempdir");
        let machine = std::sync::Arc::new(MetadataStateMachine::new(std::sync::Arc::new(
            InMemoryStore::new(StoreConfig {
                changes_limit: 100,
                change_retention_max_rows: Some(1_000),
            }),
        )));
        let mut settings = RaftSettings::new(
            id,
            dir.path().into(),
            crate::raft::PeerSecurity {
                cluster_id: "test-cluster".to_string(),
                token: Some("0123456789abcdef0123456789abcdef".to_string()),
                previous_token: None,
                next_token: None,
                tls: None,
            },
        );
        settings.heartbeat_interval = Duration::from_millis(100);
        // Leadership moves here only when a test moves it. A timer near the
        // heartbeat interval fires on a loaded machine whenever a heartbeat
        // is late, and the group changes leader under the test.
        settings.election_timeout = (Duration::from_secs(20), Duration::from_secs(30));
        let handle = RaftHandle::start(
            settings,
            std::sync::Arc::clone(&machine) as std::sync::Arc<dyn AppStateMachine>,
        )
        .await
        .expect("start raft");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let router = handle.rpc_router();
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        started.push((id, addr, handle, machine, server, dir));
    }
    let peers: BTreeMap<u64, String> = started
        .iter()
        .map(|(id, addr, ..)| (*id, addr.to_string()))
        .collect();
    started[0].2.initialize(peers).await.expect("initialize");
    let members: Vec<Member> = started
        .into_iter()
        .map(|(_, _, handle, machine, server, dir)| Member {
            store: std::sync::Arc::new(crate::store::raft::RaftStore::new(handle, machine)),
            server,
            _dir: dir,
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let leader = loop {
        let first = members[0].store.handle.status().leader;
        let agreed = first.is_some()
            && members.iter().all(|member| {
                let status = member.store.handle.status();
                status.leader == first && status.last_applied_index.is_some()
            });
        if agreed {
            break first.expect("agreed leader");
        }
        assert!(std::time::Instant::now() < deadline, "no committed leader");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let leader = members
        .iter()
        .find(|member| member.store.handle.status().id == leader)
        .expect("leader is a member");
    assert!(
        leader
            .store
            .handle
            .confirm_leadership_within(Duration::from_secs(2))
            .await,
        "leader confirms"
    );
    members
}

/// A deposed leader's decision is not forwarded to the leader that replaced
/// it: the new leader has not seen what the old one judged from.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deposed_leaders_take_is_not_forwarded_to_its_successor() {
    let members = two_member_group().await;
    let leader = members
        .iter()
        .position(|member| member.store.handle.is_leader())
        .expect("a leader");
    let (old, new) = (&members[leader], &members[1 - leader]);
    // Members are numbered from 1 in order.
    let new_id = (2 - leader) as u64;
    let term = old.store.handle.current_term();

    let moved = || {
        new.store.handle.is_leader()
            && !old.store.handle.is_leader()
            && old.store.handle.status().leader == Some(new_id)
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !moved() {
        assert!(
            std::time::Instant::now() < deadline,
            "leadership never moved: old {:?} new {:?}",
            old.store.handle.status(),
            new.store.handle.status()
        );
        // Again if the old leader's own election timer won the term back.
        if old.store.handle.is_leader() && old.store.handle.give_up_term(new_id).await {
            new.store.handle.campaign().await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let taken = old
        .store
        .soft
        .propose(
            term,
            crate::store::raft::command::MetaCommand::TakePlacementLease {
                holder: "instance-b".to_string(),
            },
        )
        .await;
    assert!(
        taken.is_err(),
        "the old leader's take was answered: {taken:?}"
    );
    // Applied on the new leader by now if it was forwarded and committed.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        new.store
            .local()
            .expect("caught up")
            .placement_holder()
            .await,
        None,
        "the old leader's take committed through its successor"
    );
    for member in members {
        member.store.handle.shutdown().await.expect("shutdown");
        member.server.abort();
    }
}

/// Re-form plans live on the leader: a plan made, polled and answered
/// through a follower is the leader's plan, read from the leader's state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reform_plan_asked_through_a_follower_is_the_leaders() {
    use crate::store::reform::tests::{LOG, answer, copy, shard};
    use crate::store::reform::{ReformReply, ReformRequest, ReformScope};
    let members = two_member_group().await;
    let leader = members
        .iter()
        .position(|member| member.store.handle.is_leader())
        .expect("a leader");
    let (leader, follower) = (&members[leader].store, &members[1 - leader].store);
    // Only the leader's applied state holds the shard, so a plan with it
    // in was made there.
    let local = leader.machine.store();
    let mut shard = shard();
    catalog(local.as_ref(), &shard.key).await;
    for (id, port) in [("a", 7001), ("b", 7002), ("c", 7003)] {
        local.register_node(node(id, port)).await.expect("register");
    }
    let incarnation = local
        .list_streams(&shard.key.tenant_id, &shard.key.namespace)
        .await
        .expect("streams")[0]
        .incarnation;
    shard.incarnation = incarnation;
    shard.raft_seed.as_mut().unwrap().incarnation = incarnation;
    shard.raft_ordered.as_mut().unwrap().incarnation = incarnation;
    local.put_shard_assignment(shard).await.expect("put");

    let ReformReply::Created { plan_id } = follower
        .reform(ReformRequest::Create {
            scope: ReformScope::Cluster,
            restore: None,
        })
        .await
        .expect("create")
    else {
        panic!("not a create reply");
    };
    let ReformReply::Surveys { mut surveys } = follower
        .reform(ReformRequest::Surveys {
            node_id: "a".to_string(),
        })
        .await
        .expect("poll")
    else {
        panic!("not a surveys reply");
    };
    assert_eq!(surveys.len(), 1, "the follower's poll missed the plan");
    let survey = surveys.remove(0);
    assert_eq!(survey.plan_id, plan_id);
    let reply = follower
        .reform(ReformRequest::Reports {
            node_id: "a".to_string(),
            answers: vec![answer(
                &survey,
                copy(&LOG, Some(4), Some(2), &survey.probes),
            )],
        })
        .await
        .expect("answer");
    assert!(
        matches!(
            reply,
            ReformReply::Recorded {
                accepted: 1,
                ignored: 0
            }
        ),
        "{reply:?}"
    );
    let ReformReply::Plan { plan: Some(plan) } = leader
        .reform(ReformRequest::View { plan_id })
        .await
        .expect("view")
    else {
        panic!("the leader holds no such plan");
    };
    assert_eq!(plan.shards.len(), 1);
    assert!(
        !plan.shards[0].waiting_on.contains(&"a".to_string()),
        "the answer given through the follower did not reach the plan: {:?}",
        plan.shards[0]
    );
    for member in &members {
        member.store.handle.shutdown().await.expect("shutdown");
        member.server.abort();
    }
}

/// A follower whose own join floor would decide its level (a member it has
/// never heard from counts at that floor, and the floor can lag the log)
/// takes the leader's confirmed level instead. What it strips, the leader
/// appends as sent, so fields the group can store survive (#2878).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_strips_nothing_on_its_own_lagging_floor() {
    use crate::model::{ShardAssignment, ShardKey, ShardKind, ShardState};
    use crate::raft::AppStateMachine;
    use crate::store::raft::command::METADATA_VERSION;
    use crate::store::raft::tests::{SILENT, stub_member};
    use std::sync::atomic::{AtomicU16, Ordering};
    let members = two_member_group().await;
    let leader = members
        .iter()
        .position(|member| member.store.handle.is_leader())
        .expect("a leader");
    let (leader, follower) = (&members[leader].store, &members[1 - leader].store);
    let key = ShardKey {
        tenant_id: "t1".to_string(),
        namespace: "ns".to_string(),
        stream: "orders".to_string(),
        shard: 0,
        kind: ShardKind::Stream,
    };
    // On each member's applied state rather than through the log, as the
    // re-form test does: a create entry would raise the join floor past
    // every level put_shard_assignment strips at.
    for member in &members {
        let local = member.store.machine.store();
        catalog(local.as_ref(), &key).await;
        for (id, port) in [("broker-x", 7101), ("broker-y", 7102)] {
            local.register_node(node(id, port)).await.expect("register");
        }
    }
    // A third member the leader hears from once, and the follower never.
    let version = std::sync::Arc::new(AtomicU16::new(METADATA_VERSION));
    let addr = stub_member(std::sync::Arc::clone(&version)).await;
    leader
        .handle
        .add_learner_without_waiting(3, addr)
        .await
        .expect("add member");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while leader.handle.cluster_version_if_known().await != Some(METADATA_VERSION) {
        assert!(
            std::time::Instant::now() < deadline,
            "the leader never heard"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    version.store(SILENT, Ordering::SeqCst);
    while follower.handle.cluster_version_if_known().await.is_some() {
        assert!(
            std::time::Instant::now() < deadline,
            "the follower never counted the member"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let floor = follower.machine.join_floor();
    assert!(
        follower.handle.cluster_version_with_unknown_at(floor).await < super::super::LEARNER_LEVEL,
        "the follower's own floor {floor} would not strip learners"
    );

    let assignment = ShardAssignment {
        key: key.clone(),
        leader: "broker-x".to_string(),
        replicas: vec!["broker-y".to_string()],
        generation: 0,
        state: ShardState::Active,
        successor: None,
        joining: None,
        move_started_at_millis: None,
        move_reason: None,
        incarnation: 0,
        fence_storage_ids: [("broker-y".to_string(), "b".repeat(32))].into(),
        member_storage_ids: [("broker-y".to_string(), "c".repeat(32))].into(),
        learners: ["broker-y".to_string()].into(),
        pending_set: None,
        previous_generation: None,
        readmitted: Default::default(),
        placed: None,
        switch: None,
        raft_copy_ids: Default::default(),
        raft_target_counter: 0,
        raft_target: None,
        raft_floor: None,
        raft_formed: None,
        raft_voters: None,
        raft_seed: None,
        raft_bases: None,
        raft_leader: None,
        raft_ordered: None,
    };
    let written = follower
        .put_shard_assignment(assignment.clone())
        .await
        .expect("assign");
    assert_eq!(written.fence_storage_ids, assignment.fence_storage_ids);
    assert_eq!(written.member_storage_ids, assignment.member_storage_ids);
    assert_eq!(written.learners, assignment.learners);
    for member in &members {
        member.store.handle.shutdown().await.expect("shutdown");
        member.server.abort();
    }
}

/// The tenant, namespace and three-replica stream `key` names.
async fn catalog(store: &dyn ControlPlaneStore, key: &crate::model::ShardKey) {
    use crate::model::*;
    store
        .create_tenant(Tenant {
            tenant_id: key.tenant_id.clone(),
            display_name: "t".to_string(),
        })
        .await
        .expect("tenant");
    store
        .create_namespace(Namespace {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            display_name: "ns".to_string(),
        })
        .await
        .expect("namespace");
    store
        .create_stream(Stream {
            tenant_id: key.tenant_id.clone(),
            namespace: key.namespace.clone(),
            stream: key.stream.clone(),
            kind: StreamKind::Stream,
            shards: 1,
            replication_factor: 3,
            retention: RetentionPolicy {
                max_age_seconds: None,
                max_size_bytes: None,
            },
            consistency: ConsistencyLevel::Leader,
            delivery: DeliveryGuarantee::AtMostOnce,
            durable: true,
            region: None,
            routing: Default::default(),
            incarnation: 0,
            mount: None,
        })
        .await
        .expect("stream");
}
