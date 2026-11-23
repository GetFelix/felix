//! A whole re-form on three brokers: the stop, the plan from the stopped
//! copies' reports, the start survey, and the feed pass that starts and
//! opens each copy in the new epoch.

use std::collections::BTreeMap;

use felix_storage::raft_copy::{
    BrokerReport, GroupRecord, PlanView, SurveyStep, VoterId, WireStart, first_probes, plan_starts,
    survey_step,
};

use super::*;
use crate::shards::raft::reform_survey::{Survey, SurveyKind};
use crate::shards::watch::{
    PlacedEpoch, RaftBases, RaftEpochBase, RaftLogId, RaftTarget, RaftTargetVersion, RaftVoters,
};

pub(super) fn survey(
    key: &ShardKey,
    kind: SurveyKind,
    epoch: u64,
    probes: BTreeSet<u64>,
) -> Survey {
    Survey {
        plan_id: "p1".to_string(),
        kind,
        round: 1,
        key: key.clone(),
        incarnation: INCARNATION,
        epoch,
        probes,
        start: None,
    }
}

pub(super) async fn read(
    member: &Member,
    survey: Survey,
) -> Option<felix_storage::raft_copy::WireReport> {
    let read = member.shards.reform_reader();
    tokio::task::spawn_blocking(move || read(&survey))
        .await
        .expect("read")
}

/// The record the control plane plans from: every broker placed, none
/// dead, and the voters the group formed on.
fn record(members: &[Member], key: &ShardKey) -> GroupRecord<String> {
    let seed: BTreeSet<VoterId<String>> = members
        .iter()
        .map(|member| {
            let status = member.shards.runtime.status(key).expect("an open member");
            (member.name.to_string(), status.member.copy)
        })
        .collect();
    GroupRecord {
        incarnation: INCARNATION,
        bases: Vec::new(),
        floor: None,
        offset_floor: None,
        placed: Some(BROKERS.iter().map(|name| name.to_string()).collect()),
        dead: BTreeSet::new(),
        seed: Some(seed.clone()),
        ordered: Some(vec![seed]),
        restore: None,
    }
}

/// Stop every copy as the control plane's `stop` round does, then plan
/// from the stopped copies' reads, asking again at the probes each round
/// adds. The leader lost its member first (as a crash leaves it), so no
/// copy leads and the others cannot elect.
async fn stop_and_plan(
    members: &[Member],
    key: &ShardKey,
    leader: usize,
) -> (PlanView, BTreeMap<String, WireStart>) {
    use felix_broker::EngineLogs;
    let record = record(members, key);
    let gone = &members[leader];
    gone.shards
        .close_through(&key.tenant_id, &key.namespace, &key.stream, INCARNATION)
        .await;
    if key.kind == ShardKind::Stream {
        gone.broker
            .close_stream_shard(&key.tenant_id, &key.namespace, &key.stream, key.shard, None)
            .await
            .expect("close the stream");
    }
    let mut asked = first_probes(&record);
    let mut reports = BTreeMap::new();
    for member in members {
        let stop = survey(key, SurveyKind::Stop, 0, asked.clone());
        let report = member.shards.stop_for_reform(&stop).await.expect("stopped");
        assert!(!report.leads, "{} still leads", member.name);
        reports.insert(member.name.to_string(), report);
    }
    loop {
        let parsed: BTreeMap<String, BrokerReport<String>> = reports
            .iter()
            .map(|(name, wire)| (name.clone(), wire.clone().try_into().expect("version")))
            .collect();
        match survey_step(&record, &parsed, &asked) {
            SurveyStep::Planned(view) => {
                let starts = plan_starts(&record, &parsed, false).expect("starts");
                return (view, starts);
            }
            SurveyStep::Ask(next) => {
                asked = next;
                for member in members {
                    let again = survey(key, SurveyKind::Survey, 0, asked.clone());
                    let report = read(member, again).await.expect("read");
                    reports.insert(member.name.to_string(), report);
                }
            }
            other => panic!("no plan: {other:?}"),
        }
    }
}

/// `placed` with the record the epoch write leaves: epoch 1, its base and
/// first index, and the plan's voters as its seed.
pub(super) fn next_epoch(mut placed: ShardAssignment, view: &PlanView) -> ShardAssignment {
    placed.placed = Some(PlacedEpoch { epoch: 1 });
    placed.raft_bases = Some(RaftBases {
        incarnation: INCARNATION,
        bases: vec![RaftEpochBase {
            base_last: view
                .base_last
                .map(|(index, epoch, term)| RaftLogId { index, epoch, term }),
            first_index: view.first_index,
        }],
    });
    placed.raft_seed = Some(RaftVoters {
        incarnation: INCARNATION,
        epoch: 1,
        members: view.voters.iter().cloned().collect(),
        starts: Default::default(),
    });
    placed
}

/// Run the start round: each broker takes its `start` survey, then a feed
/// pass with the new record starts its copy and opens its member. Returns
/// the member that leads the new epoch.
pub(super) async fn start(
    members: &[Member],
    key: &ShardKey,
    next: &ShardAssignment,
    starts: &BTreeMap<String, WireStart>,
) -> usize {
    for member in members {
        let mut asked = survey(key, SurveyKind::Start, 1, BTreeSet::new());
        asked.start = Some(starts[member.name].clone());
        assert!(
            read(member, asked).await.is_none(),
            "{} answered a start before it started",
            member.name
        );
    }
    for member in members {
        member.shards.take(&mut assignments([next.clone()])).await;
    }
    for member in members {
        let dir =
            super::super::reform_survey::reform_dir(&member.broker, key, INCARNATION).expect("dir");
        assert_eq!(
            felix_storage::raft_copy::group_epoch(&dir).expect("epoch"),
            Some(1),
            "{} did not start epoch 1",
            member.name
        );
        let asked = survey(key, SurveyKind::Start, 1, BTreeSet::new());
        assert!(read(member, asked).await.is_some(), "{}", member.name);
    }
    let mut leader = None;
    until("a voter leads the new epoch", || {
        leader = members
            .iter()
            .position(|member| member.shards.leading(key).is_some());
        leader.is_some()
    })
    .await;
    leader.expect("a leader")
}

/// Each copy's id, as the control plane's target names it before the stop.
pub(super) fn copies(members: &[Member], key: &ShardKey) -> BTreeMap<String, u64> {
    members
        .iter()
        .map(|member| {
            let status = member.shards.runtime.status(key).expect("an open member");
            (member.name.to_string(), status.member.copy)
        })
        .collect()
}

/// The target the control plane stored before the stop, which the epoch
/// write carries: every placed copy, the learner too, which kept its id.
/// Fed to every member and told to the leader, as a heartbeat answer does,
/// so the leader adds the learner and, with `joins`, waits for it to be
/// promoted once it has caught up.
pub(super) async fn rejoin(
    members: &[Member],
    key: &ShardKey,
    next: &ShardAssignment,
    leader: usize,
    target: BTreeMap<String, u64>,
    joins: bool,
) {
    assert_eq!(copies(members, key), target, "a copy changed its id");
    let mut fed = next.clone();
    fed.target = Some(RaftTarget {
        version: RaftTargetVersion {
            incarnation: 1,
            counter: 1,
        },
        members: target,
    });
    for member in members {
        let lease = Arc::new(LeaseState::on_tokio_clock(Duration::from_secs(10)));
        member.shards.follow_lease(Arc::clone(&lease));
        lease.record_head(tokio::time::Instant::now(), 10);
        member.shards.take(&mut assignments([fed.clone()])).await;
    }
    let shards = &members[leader].shards;
    shards.answered(Some(tokio::time::Instant::now()), Some(10));
    if !joins {
        return;
    }
    until("the learner joins the voters", || {
        shards
            .runtime
            .voters(key)
            .is_some_and(|voters| voters.len() == members.len())
            && shards
                .runtime
                .status(key)
                .is_some_and(|status| status.membership == MembershipProgress::Settled)
    })
    .await;
}

/// **A three-copy group whose leader is gone re-forms through the stop,
/// the plan and the broker's start, and serves again with nothing acked
/// lost and no offset given out twice.** `broker-c` missed the last
/// acknowledged record, so the plan makes it a learner, and the new epoch's
/// voters are the two copies that hold it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_three_copy_stream_group_re_forms_and_serves() {
    use felix_replication::engine::Engine;

    let (net, members, leader) = trio().await;
    let orders = key("orders", 0);
    assert_eq!(members[leader].name, "broker-a");
    let term = members[leader].shards.leading(&orders).expect("term");
    let mut acked = vec![
        members[leader].publish(term, b"a1").await,
        members[leader].publish(term, b"a2").await,
    ];
    let target = copies(&members, &orders);
    isolate(&net, "broker-c");
    acked.push(members[leader].publish(term, b"a3").await);
    let last = *acked.last().expect("acked");

    let (view, starts) = stop_and_plan(&members, &orders, leader).await;
    assert_eq!(view.learners, ["broker-c"]);
    let voters: Vec<&str> = view.voters.iter().map(|(b, _)| b.as_str()).collect();
    assert_eq!(voters, ["broker-a", "broker-b"]);
    assert!(view.first_index > last, "{view:?}");
    net.cut.lock().clear();
    net.muted.lock().clear();

    let next = next_epoch(replicated(), &view);
    let now = start(&members, &orders, &next, &starts).await;
    assert_ne!(members[now].name, "broker-c", "a learner leads");
    let term = members[now].shards.leading(&orders).expect("term");
    let after = members[now].publish(term, b"b1").await;
    assert!(after > last, "offset {after} reused an acknowledged one");
    let status = members[now].shards.runtime.status(&orders).expect("open");
    assert!(
        status.committed >= Some(view.first_index),
        "{status:?} committed nothing in the new epoch"
    );
    let read = members[now]
        .broker
        .read_durable("t1", "ns", "orders", 0, acked[0], 1 << 20)
        .await
        .expect("read");
    let payloads: Vec<&[u8]> = read.iter().map(|record| &record.payload[..]).collect();
    assert_eq!(payloads, [&b"a1"[..], b"a2", b"a3", b"b1"]);
    let other = members
        .iter()
        .find(|member| member.name != members[now].name && member.name != "broker-c")
        .expect("the other voter");
    until("the other voter applies the new record", || {
        other.engine.committed_bound(&orders) == felix_broker::ReadBound::Committed(after + 1)
    })
    .await;

    // The learner is added but never promoted: the leader's snapshot at the
    // gap's end carries no records, so the learner refuses it rather than
    // serve a log missing the base (#3005). It leads nothing and votes in
    // nothing, so it cannot lose an acknowledged record.
    let c = members
        .iter()
        .position(|member| member.name == "broker-c")
        .expect("c");
    rejoin(&members, &orders, &next, now, target, false).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let voters = members[now].shards.runtime.voters(&orders).expect("voters");
    assert!(
        voters
            .iter()
            .all(|voter| voter.broker.as_ref() != "broker-c"),
        "a learner without the base's records became a voter: {voters:?}"
    );
    assert!(members[c].shards.leading(&orders).is_none());
    assert_ne!(
        members[c].engine.committed_bound(&orders),
        felix_broker::ReadBound::Committed(after + 1),
        "the learner claims records it was never sent"
    );
    stop(&members).await;
}

/// **A three-copy cache group re-forms the same way, and its cache is
/// folded into the new epoch**: every acknowledged value reads back from
/// the new leader, and a write after the re-form commits above the base.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_three_copy_cache_group_re_forms_and_serves() {
    use crate::serving::cache_routing::{apply_cache_request, put_request};
    use crate::serving::forward::{CacheRequest, ForwardCredential};

    let net = Arc::new(Loopback::default());
    let sessions = ShardKey {
        kind: ShardKind::Cache,
        ..key("sessions", 0)
    };
    let mut placed = assigned(sessions.clone(), INCARNATION);
    placed.replicas = BROKERS.iter().map(|name| name.to_string()).collect();
    let mut members = Vec::new();
    for name in BROKERS {
        members.push(Member::start_cache(&net, name).await);
    }
    let all: Vec<_> = members.iter().map(|member| &member.shards).collect();
    form_on(&all, placed.clone()).await;
    let leader = {
        let mut leader = None;
        until("a member serves its term", || {
            leader = members
                .iter()
                .position(|member| member.shards.leading(&sessions).is_some());
            leader.is_some()
        })
        .await;
        leader.expect("a leader")
    };
    assert_eq!(members[leader].name, "broker-a");
    let cache = |member: &Member, name: &'static str, request: CacheRequest| {
        let broker = Arc::clone(&member.broker);
        let engine = Arc::clone(&member.engine);
        let ingress = ingress(&member.shards);
        async move {
            apply_cache_request(
                &broker,
                (&engine, Duration::from_secs(10)),
                Some(&ingress),
                None,
                &ForwardCredential::from(""),
                "t1",
                "ns",
                "sessions",
                name,
                request,
                None,
            )
            .await
            .expect("served")
        }
    };
    let put = |value: &'static [u8]| put_request(bytes::Bytes::from_static(value), None);
    cache(&members[leader], "k1", put(b"one")).await;
    let target = copies(&members, &sessions);
    isolate(&net, "broker-c");
    cache(&members[leader], "k2", put(b"two")).await;

    let (view, starts) = stop_and_plan(&members, &sessions, leader).await;
    assert_eq!(view.learners, ["broker-c"]);
    net.cut.lock().clear();
    net.muted.lock().clear();

    let next = next_epoch(placed, &view);
    let now = start(&members, &sessions, &next, &starts).await;
    assert_ne!(members[now].name, "broker-c", "a learner leads");
    for (name, value) in [("k1", &b"one"[..]), ("k2", b"two")] {
        let found = cache(&members[now], name, CacheRequest::Get).await;
        assert_eq!(found.value.as_deref(), Some(value), "{name}");
    }
    cache(&members[now], "k3", put(b"three")).await;
    let status = members[now].shards.runtime.status(&sessions).expect("open");
    assert!(
        status.committed >= Some(view.first_index),
        "{status:?} committed nothing in the new epoch"
    );

    // The learner rejoins from the leader's snapshot and becomes a voter.
    let c = members
        .iter()
        .position(|member| member.name == "broker-c")
        .expect("c");
    rejoin(&members, &sessions, &next, now, target, true).await;
    let committed = members[now]
        .shards
        .runtime
        .status(&sessions)
        .expect("open")
        .committed;
    until("the learner applies what the leader committed", || {
        members[c]
            .shards
            .runtime
            .status(&sessions)
            .is_some_and(|status| status.committed >= committed)
    })
    .await;
    // Then the leadership moves to it, and it serves every acknowledged value.
    members[now].shards.runtime.prefer(&sessions, "broker-c");
    until("the learner leads", || {
        members[c].shards.leading(&sessions).is_some()
    })
    .await;
    for (name, value) in [("k1", &b"one"[..]), ("k2", b"two"), ("k3", b"three")] {
        let found = cache(&members[c], name, CacheRequest::Get).await;
        assert_eq!(found.value.as_deref(), Some(value), "{name} on broker-c");
    }
    cache(&members[c], "k4", put(b"four")).await;
    stop(&members).await;
}
