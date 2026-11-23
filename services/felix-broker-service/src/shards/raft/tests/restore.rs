//! A whole-cluster restore from a backup point on three brokers: the point
//! read from the leader under load, `restore-point` over the leader's
//! copy, a wiped cluster, and the re-form at the point.

use std::collections::BTreeMap;

use felix_storage::raft_copy::{
    BrokerReport, DEFAULT_OFFSET_GAP, GroupRecord, PlanView, RestoreAt, SurveyStep, VoterId,
    WireStart, first_probes, plan_starts, survey_step,
};

use super::reform::{next_epoch, read, start, survey};
use super::*;
use crate::shards::raft::reform_survey::SurveyKind;
use crate::shards::routing::EngineShards;

/// Publish `count` records on `member` while the point is read from it,
/// halfway through. Returns every acknowledged offset with its payload, and
/// the point.
async fn publish_under_a_point(
    member: &Member,
    key: &ShardKey,
    term: u64,
    count: usize,
) -> (Vec<(u64, &'static [u8])>, u64) {
    let (half_tx, half_rx) = tokio::sync::oneshot::channel();
    let load = async {
        let mut half_tx = Some(half_tx);
        let mut acked = Vec::new();
        for n in 0..count {
            let payload: &'static [u8] = Box::leak(format!("r{n}").into_bytes().into_boxed_slice());
            acked.push((member.publish(term, payload).await, payload));
            if n == count / 2
                && let Some(tx) = half_tx.take()
            {
                let _ = tx.send(());
            }
        }
        acked
    };
    let point = async {
        half_rx.await.expect("half way");
        member
            .shards
            .backup_members()
            .into_iter()
            .find(|read| read.key == *key)
            .and_then(|read| read.committed_end)
            .expect("the leader gives an end")
    };
    tokio::join!(load, point)
}

/// What `felixctl backup-point` writes for `orders` led by `leader`.
fn manifest(kind: &str, name: &str, leader: &str, point: u64) -> crate::restore::Manifest {
    serde_json::from_value(serde_json::json!({
        "format_version": 1,
        "name": "nightly",
        "shards": [{
            "tenant_id": "t1", "namespace": "ns", "name": name, "shard": 0,
            "kind": kind, "leader": leader, "incarnation": INCARNATION,
            "logs": { "records": point }
        }]
    }))
    .expect("manifest")
}

/// The voters of `key` as the members hold them now.
fn seed_of(members: &[Member], key: &ShardKey) -> BTreeSet<VoterId<String>> {
    members
        .iter()
        .map(|member| {
            let status = member.shards.runtime.status(key).expect("open");
            (member.name.to_string(), status.member.copy)
        })
        .collect()
}

/// Stop every member. The leader's directory, taken after the point, is the
/// copy; the others are dropped.
async fn stop_keeping(
    net: &Arc<Loopback>,
    members: &mut Vec<Member>,
    leader_name: &str,
) -> tempfile::TempDir {
    let mut root = None;
    for member in members.drain(..) {
        EngineShards::shutdown(&*member.shards).await;
        net.forget(member.name);
        if member.name == leader_name {
            root = Some(member.root);
        }
    }
    root.expect("the leader's root")
}

/// Bring the brokers back: the leader on its cut copy, the others empty, all
/// holding `placed`.
async fn restart(
    net: &Arc<Loopback>,
    root: tempfile::TempDir,
    leader_name: &str,
    cache: bool,
    placed: &ShardAssignment,
) -> Vec<Member> {
    let mut root = Some(root);
    let mut members = Vec::new();
    for name in BROKERS {
        let kept = root.take_if(|_| name == leader_name);
        let member = match (kept, cache) {
            (Some(root), false) => {
                Member::start_on(
                    net,
                    name,
                    root,
                    INCARNATION,
                    ConsistencyLevel::Quorum,
                    FsyncMode::None,
                    false,
                )
                .await
            }
            (None, false) => Member::start(net, name).await,
            (Some(root), true) => Member::start_cache_on(net, name, root).await,
            (None, true) => Member::start_cache(net, name).await,
        };
        members.push(member);
    }
    for member in &members {
        member.shards.take(&mut assignments([placed.clone()])).await;
    }
    members
}

/// Stop and survey the restored group as `reform plan --restore-point`
/// does, with a plan at `point` and the default gap.
async fn replan(
    members: &[Member],
    key: &ShardKey,
    seed: BTreeSet<VoterId<String>>,
    point: u64,
) -> (PlanView, BTreeMap<String, WireStart>) {
    let record = GroupRecord {
        incarnation: INCARNATION,
        bases: Vec::new(),
        floor: None,
        offset_floor: None,
        placed: Some(BROKERS.iter().map(|name| name.to_string()).collect()),
        dead: BTreeSet::new(),
        seed: Some(seed.clone()),
        ordered: Some(vec![seed]),
        restore: Some(RestoreAt {
            end: point,
            gap: DEFAULT_OFFSET_GAP,
            taken_at_millis: 0,
            restored_at_millis: 0,
        }),
    };
    let mut asked = first_probes(&record);
    let mut reports = BTreeMap::new();
    for member in members {
        let stop = survey(key, SurveyKind::Stop, 0, asked.clone());
        let report = member.shards.stop_for_reform(&stop).await.expect("stopped");
        reports.insert(member.name.to_string(), report);
    }
    loop {
        let parsed: BTreeMap<String, BrokerReport<String>> = reports
            .iter()
            .map(|(name, wire)| (name.clone(), wire.clone().try_into().expect("version")))
            .collect();
        match survey_step(&record, &parsed, &asked) {
            SurveyStep::Planned(view) => {
                // The wiped copies' votes are unknown: the operator accepts.
                let starts = plan_starts(&record, &parsed, true).expect("starts");
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

/// **A Raft group restored at a backup point re-forms from the leader's
/// cut copy**: every record acknowledged below the point reads back,
/// nothing at or past it does, and the next publish lands past the gap.
/// The point is read while publishes run; the followers' copies are wiped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_restored_at_a_backup_point_re_forms_at_it() {
    let (net, mut members, leader) = trio().await;
    let orders = key("orders", 0);
    let term = members[leader].shards.leading(&orders).expect("term");
    let (acked, point) = publish_under_a_point(&members[leader], &orders, term, 24).await;
    assert!(
        acked.iter().any(|(offset, _)| *offset >= point),
        "nothing landed past the point {point}"
    );
    let seed = seed_of(&members, &orders);
    let leader_name = members[leader].name;
    let root = stop_keeping(&net, &mut members, leader_name).await;
    {
        let broker = broker_at(
            root.path(),
            3,
            INCARNATION,
            ConsistencyLevel::Quorum,
            FsyncMode::None,
        )
        .await;
        let storage = broker.durable_storage().cloned();
        let cut = crate::restore::restore(
            &manifest("stream", "orders", leader_name, point),
            Some(leader_name),
            root.path(),
            &broker,
            storage,
        )
        .await
        .expect("restore-point");
        assert_eq!(cut, 1);
    }
    let placed = replicated();
    let members = restart(&net, root, leader_name, false, &placed).await;
    let (view, starts) = replan(&members, &orders, seed, point).await;
    assert_eq!(
        view.base_last.map(|(index, _, _)| index + 1),
        Some(point),
        "{view:?}"
    );
    assert_eq!(view.first_index, point + DEFAULT_OFFSET_GAP, "{view:?}");

    let next = next_epoch(placed, &view);
    let now = start(&members, &orders, &next, &starts).await;
    let term = members[now].shards.leading(&orders).expect("term");
    let after = members[now].publish(term, b"after").await;
    assert!(
        after >= view.first_index,
        "offset {after} is inside the gap"
    );

    let mut want: Vec<(u64, &[u8])> = acked
        .iter()
        .filter(|(offset, _)| *offset < point)
        .map(|(offset, payload)| (*offset, *payload))
        .collect();
    want.push((after, b"after"));
    let mut got = Vec::new();
    let mut from = 0;
    loop {
        let read = members[now]
            .broker
            .read_durable("t1", "ns", "orders", 0, from, 1 << 20)
            .await
            .expect("read");
        let Some(last) = read.last() else { break };
        from = last.offset + 1;
        got.extend(
            read.iter()
                .map(|record| (record.offset, record.payload.to_vec())),
        );
        if from > after {
            break;
        }
    }
    let got: Vec<(u64, &[u8])> = got
        .iter()
        .map(|(offset, payload)| (*offset, &payload[..]))
        .collect();
    assert_eq!(got, want);
    stop(&members).await;
}

/// **A cache group restored at a backup point keeps what was written below
/// it**: values, their versions and TTLs read back, nothing written after the
/// point does, a compare-and-set against a restored version applies, and the
/// next write commits past the gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cache_group_restored_at_a_backup_point_re_forms_at_it() {
    use crate::serving::cache_routing::{apply_cache_request, put_request};
    use crate::serving::forward::{CacheRequest, ForwardCredential};
    use felix_storage::CacheCondition;

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
    let mut leader = None;
    until("a member serves its term", || {
        leader = members
            .iter()
            .position(|member| member.shards.leading(&sessions).is_some());
        leader.is_some()
    })
    .await;
    let leader = leader.expect("a leader");
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
    let hour = Some(Duration::from_secs(3600));
    let one = |value: &'static [u8], ttl| put_request(bytes::Bytes::from_static(value), ttl);

    // Below the point: a plain value, one with a TTL, and one overwritten so
    // its version is not the first.
    cache(&members[leader], "plain", one(b"p1", None)).await;
    cache(&members[leader], "timed", one(b"t1", hour)).await;
    cache(&members[leader], "bumped", one(b"b1", None)).await;
    cache(&members[leader], "bumped", one(b"b2", None)).await;
    let mut versions = BTreeMap::new();
    for name in ["plain", "timed", "bumped"] {
        let found = cache(&members[leader], name, CacheRequest::GetVersioned).await;
        versions.insert(name, found.outcome.expect("versioned").version);
    }
    let point = members[leader]
        .shards
        .backup_members()
        .into_iter()
        .find(|read| read.key == sessions)
        .and_then(|read| read.committed_end)
        .expect("the leader gives an end");
    // After the point: a new key and an overwrite of a restored one.
    cache(&members[leader], "late", one(b"l1", None)).await;
    cache(&members[leader], "plain", one(b"p2", None)).await;

    let seed = seed_of(&members, &sessions);
    let leader_name = members[leader].name;
    let root = stop_keeping(&net, &mut members, leader_name).await;
    {
        let broker = cache_broker_of(root.path(), "sessions", 3).await;
        let storage = broker.durable_storage().cloned();
        let cut = crate::restore::restore(
            &manifest("cache", "sessions", leader_name, point),
            Some(leader_name),
            root.path(),
            &broker,
            storage,
        )
        .await
        .expect("restore-point");
        assert_eq!(cut, 1);
    }
    let members = restart(&net, root, leader_name, true, &placed).await;
    let (view, starts) = replan(&members, &sessions, seed, point).await;
    assert_eq!(view.first_index, point + DEFAULT_OFFSET_GAP, "{view:?}");

    let next = next_epoch(placed, &view);
    let now = start(&members, &sessions, &next, &starts).await;
    for (name, value) in [("plain", &b"p1"[..]), ("timed", b"t1"), ("bumped", b"b2")] {
        let found = cache(&members[now], name, CacheRequest::GetVersioned).await;
        assert_eq!(found.value.as_deref(), Some(value), "{name}");
        assert_eq!(
            found.outcome.expect("versioned").version,
            versions[name],
            "{name} keeps its version"
        );
    }
    let late = cache(&members[now], "late", CacheRequest::Get).await;
    assert_eq!(late.value, None, "a write after the point came back");

    // A compare-and-set against the restored version applies, and the write
    // commits past the gap.
    let swapped = cache(
        &members[now],
        "timed",
        CacheRequest::PutIf {
            value: bytes::Bytes::from_static(b"t2"),
            ttl_ms: 3_600_000,
            condition: CacheCondition::Version(versions["timed"].expect("version")),
        },
    )
    .await;
    assert!(swapped.outcome.expect("outcome").applied);
    let status = members[now].shards.runtime.status(&sessions).expect("open");
    assert!(
        status.committed >= Some(view.first_index),
        "{status:?} committed nothing past the gap"
    );
    stop(&members).await;
}
