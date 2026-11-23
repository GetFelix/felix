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

fn no_stop() -> Stop {
    Arc::new(|_| Box::pin(async { panic!("nothing is stopped") }))
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
    tick(&link, "b1", None, read, no_stop()).await;
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
    tick(&old, "b1", None, Arc::clone(&read), no_stop()).await;
    assert!(old.reform_reports().is_empty());
    let idle = FakeLink::default().reply("reform_surveys", serde_json::json!({ "surveys": [] }));
    tick(&idle, "b1", None, read, no_stop()).await;
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
        kind: SurveyKind::Survey,
        round: 1,
        key: sessions,
        incarnation: INCARNATION,
        epoch: 0,
        probes: BTreeSet::new(),
        start: None,
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

/// A stop goes to the stopper, not the read-only reader, and its answer says
/// it is one: the control plane takes a stop answer only with that kind. A
/// kind this build does not know is not read as a survey.
#[tokio::test]
async fn a_stop_is_answered_by_the_stopper_under_its_kind() {
    let mut stop = survey_json("held", 3);
    stop["kind"] = "stop".into();
    let mut unknown = survey_json("other", 3);
    unknown["kind"] = "rewind".into();
    let link = FakeLink::default().reply(
        "reform_surveys",
        serde_json::json!({ "surveys": [stop, unknown] }),
    );
    let read: Read = Arc::new(|survey: &Survey| panic!("read {survey:?}"));
    let stopped: Stop = Arc::new(|survey: Survey| {
        Box::pin(async move { (survey.key.stream == "held").then(report) })
    });
    tick(&link, "b1", None, read, stopped).await;
    let sent = link.reform_reports();
    let reports = sent[0]["reports"].as_array().expect("reports");
    assert_eq!(reports.len(), 1, "{reports:?}");
    assert_eq!(reports[0]["kind"], "stop");
}

fn stop_survey(key: ShardKey) -> Survey {
    Survey {
        plan_id: "p".to_string(),
        kind: SurveyKind::Stop,
        round: 2,
        key,
        incarnation: crate::shards::raft::tests::INCARNATION,
        epoch: 0,
        probes: BTreeSet::new(),
        start: None,
    }
}

/// A member that leads the epoch is not stopped: the answer says it leads,
/// which leaves the group alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_leaves_a_leading_member_running() {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;

    let report = shards
        .stop_for_reform(&stop_survey(held.clone()))
        .await
        .expect("answered");
    assert!(report.leads, "{report:?}");
    let dir = reform_dir(&broker, &held, INCARNATION).expect("dir");
    assert_eq!(felix_storage::raft_copy::stopped(&dir).expect("read"), None);
    assert!(shards.held_copies().iter().any(|copy| copy.key == held));
}

/// A copy with no member running is stopped on disk before the answer, so
/// the read the answer carries is one no member can append past.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_syncs_its_marker_before_it_answers() {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};
    use felix_broker::EngineLogs;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;
    shards
        .close_through(&held.tenant_id, &held.namespace, &held.stream, INCARNATION)
        .await;

    let report = shards
        .stop_for_reform(&stop_survey(held.clone()))
        .await
        .expect("answered");
    assert!(!report.leads);
    let dir = reform_dir(&broker, &held, INCARNATION).expect("dir");
    let stopped = felix_storage::raft_copy::stopped(&dir)
        .expect("read")
        .expect("stopped");
    assert_eq!(stopped.epoch, 0);
    assert_eq!(stopped.last, report.current.last.map(|(_, index)| index));

    // A stop asked about another epoch stops nothing.
    let later = Survey {
        epoch: 1,
        ..stop_survey(held)
    };
    assert!(shards.stop_for_reform(&later).await.is_none());
}

/// A start is answered only once the copy holds the epoch it was asked to
/// start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_is_not_answered_before_the_copy_holds_the_epoch() {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;
    let start = Survey {
        kind: SurveyKind::Start,
        epoch: 1,
        ..stop_survey(held)
    };
    let read = shards.reform_reader();
    let answered = tokio::task::spawn_blocking(move || read(&start))
        .await
        .expect("read");
    assert!(answered.is_none(), "{answered:?}");
}

/// A group of one on this broker, stopped by a re-form, with the record of
/// the epoch after it and this broker's start survey for that epoch taken.
struct Stopped {
    _root: tempfile::TempDir,
    broker: Arc<felix_broker::Broker>,
    shards: Arc<RaftShards>,
    key: ShardKey,
    copy: u64,
    next: crate::shards::watch::ShardAssignment,
    wire: WireStart,
}

/// The gap a test re-form leaves between the base and the first index.
const GAP: u64 = 100;

async fn stopped_group() -> Stopped {
    use crate::shards::raft::tests::{INCARNATION, assigned, broker, key, shards, take_formed};
    use crate::shards::watch::{PlacedEpoch, RaftBases, RaftEpochBase, RaftLogId, RaftVoters};
    use felix_broker::EngineLogs;

    let root = tempfile::tempdir().expect("tempdir");
    let broker = broker(root.path()).await;
    let shards = shards(&broker);
    let held = key("orders", 0);
    take_formed(&shards, assigned(held.clone(), INCARNATION)).await;
    shards
        .close_through(&held.tenant_id, &held.namespace, &held.stream, INCARNATION)
        .await;
    let report = shards
        .stop_for_reform(&stop_survey(held.clone()))
        .await
        .expect("answered");
    // A stop of a running member closes its stream too; this one's member
    // was closed first, so its stream is closed here.
    broker
        .close_stream_shard(
            &held.tenant_id,
            &held.namespace,
            &held.stream,
            held.shard,
            None,
        )
        .await
        .expect("close the stream");
    let copy = report.current.copy.expect("a copy id");
    let (term, index) = report.current.last.expect("a last entry");
    let mut next = assigned(held.clone(), INCARNATION);
    next.placed = Some(PlacedEpoch { epoch: 1 });
    next.raft_bases = Some(RaftBases {
        incarnation: INCARNATION,
        bases: vec![RaftEpochBase {
            base_last: Some(RaftLogId {
                index,
                epoch: 0,
                term,
            }),
            first_index: index + 1 + GAP,
        }],
    });
    next.raft_seed = Some(RaftVoters {
        incarnation: INCARNATION,
        epoch: 1,
        members: [("broker-a".to_string(), copy)].into(),
    });
    let wire = WireStart {
        voter: Some(copy),
        kept: None,
        floor: None,
        needs_accept_loss: false,
        accepted: false,
    };
    Stopped {
        _root: root,
        broker,
        shards,
        key: held,
        copy,
        next,
        wire,
    }
}

impl Stopped {
    /// The start survey of epoch 1, as the control plane asks it.
    fn start_survey(&self) -> Survey {
        Survey {
            kind: SurveyKind::Start,
            epoch: 1,
            start: Some(self.wire.clone()),
            ..stop_survey(self.key.clone())
        }
    }

    /// Read the start survey the way a tick does; `Some` once answered.
    async fn read_start(&self) -> Option<WireReport> {
        let read = self.shards.reform_reader();
        let survey = self.start_survey();
        tokio::task::spawn_blocking(move || read(&survey))
            .await
            .expect("read")
    }

    async fn feed(&self) {
        use crate::shards::raft::tests::assignments;
        use crate::shards::routing::EngineShards;
        self.shards
            .take(&mut assignments([self.next.clone()]))
            .await;
    }

    /// The member opened at epoch 1 under the copy id the seed names, and
    /// it elects itself on the voters the folded snapshot names.
    async fn opened_at_the_new_epoch(&self) {
        let dir = reform_dir(
            &self.broker,
            &self.key,
            crate::shards::raft::tests::INCARNATION,
        )
        .expect("dir");
        assert_eq!(
            felix_storage::raft_copy::group_epoch(&dir).expect("epoch"),
            Some(1)
        );
        let status = self
            .shards
            .runtime
            .status(&self.key)
            .expect("an open member");
        assert_eq!(status.member.copy, self.copy);
        crate::shards::raft::tests::until("the re-formed member leads", || {
            self.shards
                .runtime
                .status(&self.key)
                .is_some_and(|status| status.leader.as_ref() == Some(&status.member))
        })
        .await;
        let first = self.next.raft_bases.as_ref().expect("bases").bases[0].first_index;
        let last = self
            .shards
            .runtime
            .status(&self.key)
            .and_then(|status| status.last_index);
        assert!(
            last >= Some(first),
            "{last:?} below the first index {first}"
        );
        assert!(
            self.read_start().await.is_some(),
            "the start is not answered"
        );
    }
}

/// **A stopped copy starts in the epoch its shard's record names, from
/// the start survey, and opens as the voter the seed names.** Without the
/// start its open is refused as stopped and nothing serves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_copy_starts_in_its_re_formed_epoch_and_opens() {
    let group = stopped_group().await;
    // Before the start survey, the record alone starts nothing.
    group.feed().await;
    assert!(group.shards.runtime.status(&group.key).is_none());
    assert!(group.read_start().await.is_none());

    group.feed().await;
    group.opened_at_the_new_epoch().await;
}

/// **A crash after the start and before the member opens resumes on the
/// next pass**: the copy names the new epoch, so the stop marker is spent,
/// the start is not run again, and the open is an ordinary one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_between_the_start_and_the_open_resumes() {
    let group = stopped_group().await;
    assert!(group.read_start().await.is_none());
    let dir = reform_dir(
        &group.broker,
        &group.key,
        crate::shards::raft::tests::INCARNATION,
    )
    .expect("dir");
    let storage = group.broker.durable_storage().expect("durable storage");
    let config = felix_storage::log::LogConfig {
        raft_log: true,
        fsync_mode: felix_storage::log::FsyncMode::OnCommit,
        retention_bytes: None,
        retention_age: None,
        offload: None,
        ..storage.config().clone()
    };
    let voters = [felix_replication::raft::MemberId::new(
        "broker-a", group.copy,
    )]
    .into();
    // The start a pass runs, then the crash: no member opens.
    group
        .shards
        .root
        .get()
        .expect("the root")
        .start_stopped(
            &felix_replication::raft::mark_name(&group.key),
            &dir,
            crate::shards::raft::tests::INCARNATION,
            group.next.epoch_bases().expect("bases"),
            &group.wire,
            &felix_replication::raft::GapSnapshot::new(ShardKind::Stream, voters),
            &config,
        )
        .await
        .expect("start");

    group.feed().await;
    group.opened_at_the_new_epoch().await;
}
