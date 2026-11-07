//! A leader that offered `UNIFIED_FENCE` counts each attested answer by what
//! it attests: a promise when the copy kept its ballots (exclusion),
//! coverage when it holds what it acknowledged. `Promises`, `Covers` and
//! `OpenOK` in `docs/formal/FelixShardUnified.tla`.

use super::*;

/// A copy that kept its ballots and holds what it acknowledged.
pub(super) const TRUSTED: FenceAttestation = FenceAttestation {
    kept_from: 0,
    trusted: true,
    by_record: false,
};

/// A copy that kept its ballots but may have lost acknowledged records, as
/// one led before an unclean crash.
pub(super) const DOUBTED: FenceAttestation = FenceAttestation {
    kept_from: 0,
    trusted: false,
    by_record: false,
};

/// The marks of a leader counting by attestation, whose own directory kept
/// its ballots when `kept`.
pub(super) fn unified(kept: bool) -> crate::quorum::QuorumMarks {
    let marks = crate::quorum::QuorumMarks::new();
    marks.set_unified_fence(true);
    marks.set_kept_ballots(kept);
    marks
}

/// [`unified`] with kept ballots, doubting this broker's own copy of the
/// shard as a leader that crashed uncleanly does.
pub(super) fn doubting_own(dir: &std::path::Path) -> crate::quorum::QuorumMarks {
    let marks = unified(true);
    let trust = felix_storage::run_state::copy_trust::CopyTrust::open(dir).expect("open");
    trust
        .doubt(&crate::unclean::copy_key(&crate::driver::watch_key(&key())))
        .expect("doubt");
    marks.set_copy_trust(Arc::new(trust));
    marks
}

/// The promoted leader's fence, counted by attestation under `marks`.
pub(super) async fn fence_unified(
    replicas: &Replicas,
    leader: &Arc<Broker>,
    route: &Route,
    joint: Option<&Joint<'_>>,
    marks: &crate::quorum::QuorumMarks,
) -> Outcome {
    fence_shard_as(
        replicas,
        leader,
        LEADER,
        &key(),
        route,
        false,
        joint,
        None,
        Some(marks),
    )
    .await
}

/// **An attested answer counts whatever copy the route pins.** The leader
/// elected at 4 was killed, and the failover's report could not vouch for
/// broker-b, so the route pins it with an empty id. broker-c is down. An
/// unattested answer from broker-b stays out as before; one attesting kept
/// ballots and every acknowledged record opens with this broker: two
/// promises and two covering copies of three.
#[tokio::test]
async fn an_attested_answer_counts_past_an_empty_pin() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let mut route = route();
    route.fence_storage_ids = Some(Arc::new(
        [
            ("broker-b".to_string(), String::new()),
            ("broker-c".to_string(), storage_of("broker-c").to_hex()),
        ]
        .into(),
    ));
    let marks = unified(true);

    let outcome = fence_unified(&replicas, &leader, &route, None, &marks).await;
    assert!(
        matches!(&outcome, Outcome::Pending { why, .. } if why.contains("another copy")),
        "an unattested answer counted past its pin: {outcome:?}"
    );

    replicas.get_mut("broker-b").attests = Some(TRUSTED);
    let outcome = fence_unified(&replicas, &leader, &route, None, &marks).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A 2-voter set needs one covering copy.** This broker and broker-b,
/// which is doubted: two promises, and this broker's trusted copy meets
/// every majority of two. With this broker doubted as well, nothing covers
/// and the fence waits.
#[tokio::test]
async fn a_two_voter_set_opens_on_one_trusted_copy() {
    for own_doubted in [false, true] {
        let mut replicas = Replicas::new();
        replicas.replicas.remove("broker-c");
        replicas.get_mut("broker-b").attests = Some(DOUBTED);
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        let mut route = route();
        route
            .replicas
            .retain(|replica| replica.node_id != "broker-c");
        let trust_dir = tempfile::tempdir().expect("tempdir");
        let marks = if own_doubted {
            doubting_own(trust_dir.path())
        } else {
            unified(true)
        };

        let outcome = fence_unified(&replicas, &leader, &route, None, &marks).await;

        match (&outcome, own_doubted) {
            (Outcome::Fenced { .. }, false) => {}
            (Outcome::Pending { why, .. }, true) => {
                assert!(why.contains("0 of 2 copies attest"), "{why}");
            }
            _ => panic!("own doubted {own_doubted}: {outcome:?}"),
        }
    }
}

/// **An old peer's answer counts by today's rules beside attested ones.**
/// broker-b attests a doubted copy and broker-c, an older build, attests
/// nothing. broker-c's answer from the copy the route pins counts as a
/// promise and as coverage, so with this broker two of three cover and the
/// fence opens. From another copy than the pin, it counts for nothing and
/// the fence waits.
#[tokio::test]
async fn an_old_peer_counts_by_its_pin_beside_attested_answers() {
    for rotated in [false, true] {
        let mut replicas = Replicas::new();
        replicas.get_mut("broker-b").attests = Some(DOUBTED);
        if rotated {
            replicas.get_mut("broker-c").storage = Some(StorageId::from_bytes([0xee; 16]));
        }
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        let mut route = route();
        route.fence_storage_ids = Some(Arc::new(
            ["broker-b", "broker-c"]
                .into_iter()
                .map(|id| (id.to_string(), storage_of(id).to_hex()))
                .collect(),
        ));
        let marks = unified(true);

        let outcome = fence_unified(&replicas, &leader, &route, None, &marks).await;

        match (&outcome, rotated) {
            (Outcome::Fenced { .. }, false) => {}
            (Outcome::Pending { why, .. }, true) => {
                assert!(why.contains("another copy"), "{why}");
            }
            _ => panic!("rotated {rotated}: {outcome:?}"),
        }
    }
}

/// **A copy whose promises count from no generation is never a promise.**
/// Both followers attest `kept_from = u64::MAX`, as a directory that lost
/// its ballots does, though they hold what they acknowledged: only this
/// broker promised, and the fence waits rather than refuse, since a later
/// generation might count them.
#[tokio::test]
async fn an_unkept_copy_never_counts_as_a_promise() {
    let replicas = {
        let mut replicas = Replicas::new();
        for id in ["broker-b", "broker-c"] {
            replicas.get_mut(id).attests = Some(FenceAttestation {
                kept_from: u64::MAX,
                trusted: true,
                by_record: false,
            });
        }
        replicas
    };
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_unified(&replicas, &leader, &route(), None, &unified(true)).await;

    assert!(
        matches!(outcome, Outcome::Pending { .. }),
        "opened on promises that count for nothing: {outcome:?}"
    );
}

/// **This broker's own copy counts by its own attestation.** Its directory
/// lost its ballots, so its promise counts for nothing: broker-b alone is one
/// of three, and the fence waits until broker-c answers too.
#[tokio::test]
async fn an_unkept_own_copy_needs_a_majority_without_it() {
    let mut replicas = Replicas::new();
    for id in ["broker-b", "broker-c"] {
        replicas.get_mut(id).attests = Some(TRUSTED);
    }
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let marks = unified(false);

    let outcome = fence_unified(&replicas, &leader, &route(), None, &marks).await;
    assert!(
        matches!(outcome, Outcome::Pending { .. }),
        "counted its own unkept copy: {outcome:?}"
    );

    replicas.get_mut("broker-c").reachable = true;
    let outcome = fence_unified(&replicas, &leader, &route(), None, &marks).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// **A leader that does not count by attestation counts an attested answer
/// for nothing.** Such an answer may stand in place of a refusal the leader's
/// counting relies on, so broker-b's answer, with broker-c down, leaves the
/// fence pending rather than open on it.
#[tokio::test]
async fn a_leader_counting_without_attestations_counts_an_attested_answer_for_nothing() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-b").attests = Some(TRUSTED);
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;

    let outcome = fence_shard(&replicas, &leader, LEADER, &key(), &route(), false, None).await;

    assert!(
        matches!(outcome, Outcome::Pending { .. }),
        "counted an attested answer under the older rules: {outcome:?}"
    );
}

/// The handler for `node_id`, whose directory kept its ballots.
fn kept_handler(broker: &Arc<Broker>, node_id: &str) -> ReplicaHandler {
    ReplicaHandler::new(Arc::clone(broker), router_for(node_id, 4))
        .with_fleet(Arc::new(unified(true)))
}

/// **A copy its broker led before an unclean crash answers a unified fence,
/// untrusted, and the fence opens only once coverage is met.** broker-b led
/// generation 4 and crashed; it refuses an older caller but answers this
/// leader, which counts its promise and not its log. With broker-c down, this
/// broker alone covers one of three and the fence waits; once broker-c
/// answers, trusted, it opens.
#[tokio::test]
async fn a_crashed_leaders_copy_answers_untrusted_and_the_fence_waits_for_coverage() {
    let mut replicas = Replicas::new();
    replicas
        .get("broker-b")
        .holds("broker-b", 3, 0, &["a"])
        .await;
    let crashed = Arc::clone(&replicas.get("broker-b").broker);
    crashed
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_generation(4, Some("broker-b"))
        .await
        .expect("lead");
    for id in ["broker-b", "broker-c"] {
        let replica = replicas.get_mut(id);
        replica.unified = true;
        replica.handler = kept_handler(&replica.broker, id);
    }
    let replica = replicas.get_mut("broker-b");
    replica.handler = kept_handler(&crashed, "broker-b")
        .with_unclean_start(Arc::new(crate::unclean::UncleanStart::new("broker-b")));
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(3, &["a"]).await;
    let marks = unified(true);

    let outcome = fence_unified(&replicas, &leader, &route(), None, &marks).await;
    let Outcome::Pending { why, took } = &outcome else {
        panic!("opened with one covering copy of three: {outcome:?}");
    };
    assert_eq!(took, &vec!["broker-b".to_string()], "{why}");
    assert!(why.contains("1 of 3 copies attest"), "{why}");

    replicas.get_mut("broker-c").reachable = true;
    let outcome = fence_unified(&replicas, &leader, &route(), None, &marks).await;
    assert!(matches!(outcome, Outcome::Fenced { .. }), "{outcome:?}");
}

/// Fences with broker-b attesting and broker-c down, over a kept set that
/// lists this broker as a learner or as a voter.
async fn fence_with_own_membership(learner: bool) -> Outcome {
    use felix_storage::disk_log::{Member, Membership, ReplicaSet};
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-b").attests = Some(TRUSTED);
    replicas.get_mut("broker-c").reachable = false;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let member = |node: &str| Member {
        node: node.to_string(),
        storage: None,
        learner: learner && node == LEADER,
        wiped: false,
    };
    leader
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log")
        .accept_set(ReplicaSet {
            current: Membership {
                generation: 4,
                members: [LEADER, "broker-b", "broker-c"].map(member).to_vec(),
            },
            leaving: None,
            earlier: Vec::new(),
        })
        .await
        .expect("keep the set");

    fence_unified(&replicas, &leader, &route(), None, &unified(true)).await
}

/// **This broker's copy counts no promise while its kept set lists it as a
/// learner.** broker-b's promise and this broker's would be a majority of
/// three; without this broker's, broker-b alone is not, and the fence waits.
/// The same set with this broker a voter opens, so the wait is the learner.
#[tokio::test]
async fn a_learner_own_copy_counts_no_promise() {
    let outcome = fence_with_own_membership(true).await;
    assert!(
        matches!(outcome, Outcome::Pending { .. }),
        "counted a learner's promise: {outcome:?}"
    );

    let outcome = fence_with_own_membership(false).await;
    assert!(
        matches!(outcome, Outcome::Fenced { .. }),
        "a voter's own copy did not count: {outcome:?}"
    );
}

/// **A leader never trusts an answer on the control plane's record.** The
/// record stands in for a lost vouch only on the replica, which alone knows
/// whether its copy is also doubted, poisoned, given up or unclean. Here
/// broker-b attests a doubted copy, broker-c is down, and the route holds a
/// record matching broker-b's copy: nothing covers but this broker.
#[tokio::test]
async fn a_record_does_not_cover_an_untrusted_answer() {
    let mut replicas = Replicas::new();
    replicas.get_mut("broker-c").reachable = false;
    replicas.get_mut("broker-b").attests = Some(DOUBTED);
    replicas
        .get("broker-b")
        .holds("broker-b", 4, 0, &["a"])
        .await;
    let (leader, _dir) = leader_holding(4, &["a"]).await;
    let mut route = route();
    route.readmitted = Some(Arc::new(vec![felix_router::Readmitted {
        node: "broker-b".to_string(),
        storage_id: storage_of("broker-b").to_hex(),
        generation: 4,
        offset: 1,
    }]));
    let marks = unified(true);

    let outcome = fence_unified(&replicas, &leader, &route, None, &marks).await;

    assert!(matches!(outcome, Outcome::Pending { .. }), "{outcome:?}");
}

/// [`unified`] for a broker that knows the fleet finalized
/// `replica_election`.
fn electing(kept: bool) -> crate::quorum::QuorumMarks {
    use felix_common::fleet::{FleetGate, REPLICA_ELECTION};
    let fleet = FleetGate::new([REPLICA_ELECTION.name()]);
    fleet.restart([REPLICA_ELECTION.name()]);
    let marks = crate::quorum::QuorumMarks::with_fleet(Arc::new(fleet));
    marks.set_unified_fence(true);
    marks.set_kept_ballots(kept);
    marks
}

/// **A promise kept only from a readmitting set counts while the fleet has
/// not finalized `replica_election`, and not once it has**, whatever the
/// replica attests: an adopted elected leader may have opened that set's
/// generation before the copy's loss. broker-b attests promises from the set
/// of 1, trusted; broker-c is down. A copy whose directory kept its ballots
/// counts either way, but its trust counts only while elections are off when
/// it rests on the control plane's record: a replica that has not yet heard
/// the fleet finalized `replica_election` still takes the record.
#[tokio::test]
async fn a_readmitted_answer_counts_only_while_elections_are_off() {
    let readmitted = FenceAttestation {
        kept_from: 1,
        trusted: true,
        by_record: false,
    };
    let kept = FenceAttestation {
        kept_from: 0,
        trusted: true,
        by_record: false,
    };
    let recorded = FenceAttestation {
        by_record: true,
        ..kept
    };
    for (attests, electing_fleet, opens) in [
        (readmitted, false, true),
        (readmitted, true, false),
        (kept, true, true),
        (recorded, false, true),
        (recorded, true, false),
    ] {
        let mut replicas = Replicas::new();
        replicas.get_mut("broker-c").reachable = false;
        replicas.get_mut("broker-b").attests = Some(attests);
        replicas
            .get("broker-b")
            .holds("broker-b", 4, 0, &["a"])
            .await;
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        let marks = if electing_fleet {
            electing(true)
        } else {
            unified(true)
        };

        let outcome = fence_unified(&replicas, &leader, &route(), None, &marks).await;

        assert_eq!(
            matches!(outcome, Outcome::Fenced { .. }),
            opens,
            "{attests:?} electing {electing_fleet}: {outcome:?}"
        );
    }
}

/// **The leader's own copy takes the control plane's record as its vouch,
/// and only while elections are off.** This broker's directory lost its
/// ballots and a set of 4 readmitted its copy, which left it untrusted.
/// broker-b promises and covers, broker-c is down, so the fence opens only if
/// this broker's own copy covers: with a matching record and no elections.
#[tokio::test]
async fn the_leaders_own_readmitted_copy_takes_the_records_vouch() {
    let own_id = storage_of(LEADER);
    let entry = felix_router::Readmitted {
        node: LEADER.to_string(),
        storage_id: own_id.to_hex(),
        generation: 4,
        offset: 1,
    };
    for (recorded, electing_fleet, opens) in [
        (true, false, true),
        (false, false, false),
        (true, true, false),
    ] {
        let mut replicas = Replicas::new();
        replicas.get_mut("broker-c").reachable = false;
        replicas.get_mut("broker-b").attests = Some(FenceAttestation {
            kept_from: 0,
            trusted: true,
            by_record: false,
        });
        replicas
            .get("broker-b")
            .holds("broker-b", 4, 0, &["a"])
            .await;
        let (leader, _dir) = leader_holding(4, &["a"]).await;
        let trust_dir = tempfile::tempdir().expect("tempdir");
        felix_storage::storage_identity::replace(trust_dir.path(), own_id.to_bytes())
            .expect("storage id");
        let trust =
            felix_storage::run_state::copy_trust::CopyTrust::open(trust_dir.path()).expect("open");
        trust
            .readmit(
                &crate::unclean::copy_key(&crate::driver::watch_key(&key())),
                4,
                own_id,
            )
            .expect("readmit");
        let marks = if electing_fleet {
            electing(false)
        } else {
            unified(false)
        };
        marks.set_readmits(true);
        marks.set_local_storage(own_id, false);
        marks.set_copy_trust(Arc::new(trust));
        let mut route = route();
        route.readmitted = recorded.then(|| Arc::new(vec![entry.clone()]));

        let outcome = fence_unified(&replicas, &leader, &route, None, &marks).await;

        assert_eq!(
            matches!(outcome, Outcome::Fenced { .. }),
            opens,
            "recorded {recorded} electing {electing_fleet}: {outcome:?}"
        );
    }
}
