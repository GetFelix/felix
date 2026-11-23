//! A whole-cluster restore from a backup point on three brokers: the point
//! read from the leader under load, `restore-point` over the leader's
//! copy, a wiped cluster, and the re-form at the point.

use std::collections::BTreeMap;

use felix_storage::raft_copy::{
    BrokerReport, DEFAULT_OFFSET_GAP, GroupRecord, RestoreAt, SurveyStep, VoterId, first_probes,
    plan_starts, survey_step,
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
fn manifest(leader: &str, point: u64) -> crate::restore::Manifest {
    serde_json::from_value(serde_json::json!({
        "format_version": 1,
        "name": "nightly",
        "shards": [{
            "tenant_id": "t1", "namespace": "ns", "name": "orders", "shard": 0,
            "kind": "stream", "leader": leader, "incarnation": INCARNATION,
            "logs": { "records": point }
        }]
    }))
    .expect("manifest")
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
    let seed: BTreeSet<VoterId<String>> = members
        .iter()
        .map(|member| {
            let status = member.shards.runtime.status(&orders).expect("open");
            (member.name.to_string(), status.member.copy)
        })
        .collect();

    // Stop the cluster. The leader's directory, taken after the point, is
    // the copy; the other brokers come back empty.
    let leader_name = members[leader].name;
    let mut root = None;
    for member in members.drain(..) {
        EngineShards::shutdown(&*member.shards).await;
        net.forget(member.name);
        if member.name == leader_name {
            root = Some(member.root);
        }
    }
    let root = root.expect("the leader's root");
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
            &manifest(leader_name, point),
            Some(leader_name),
            root.path(),
            &broker,
            storage,
        )
        .await
        .expect("restore-point");
        assert_eq!(cut, 1);
    }
    let mut root = Some(root);
    let mut members = Vec::new();
    for name in BROKERS {
        let member = match root.take_if(|_| name == leader_name) {
            Some(root) => {
                Member::start_on(
                    &net,
                    name,
                    root,
                    INCARNATION,
                    ConsistencyLevel::Quorum,
                    FsyncMode::None,
                    false,
                )
                .await
            }
            None => Member::start(&net, name).await,
        };
        members.push(member);
    }
    let placed = replicated();
    for member in &members {
        member.shards.take(&mut assignments([placed.clone()])).await;
    }

    // The control plane's record is back at the point, and the plan is
    // made with `--restore-point`.
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
        }),
    };
    let mut asked = first_probes(&record);
    let mut reports = BTreeMap::new();
    for member in &members {
        let stop = survey(&orders, SurveyKind::Stop, 0, asked.clone());
        let report = member.shards.stop_for_reform(&stop).await.expect("stopped");
        reports.insert(member.name.to_string(), report);
    }
    let (view, starts) = loop {
        let parsed: BTreeMap<String, BrokerReport<String>> = reports
            .iter()
            .map(|(name, wire)| (name.clone(), wire.clone().try_into().expect("version")))
            .collect();
        match survey_step(&record, &parsed, &asked) {
            SurveyStep::Planned(view) => {
                // The wiped copies' votes are unknown: the operator accepts.
                let starts = plan_starts(&record, &parsed, true).expect("starts");
                break (view, starts);
            }
            SurveyStep::Ask(next) => {
                asked = next;
                for member in &members {
                    let again = survey(&orders, SurveyKind::Survey, 0, asked.clone());
                    let report = read(member, again).await.expect("read");
                    reports.insert(member.name.to_string(), report);
                }
            }
            other => panic!("no plan: {other:?}"),
        }
    };
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
