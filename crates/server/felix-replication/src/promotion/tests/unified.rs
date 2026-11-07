//! A leader that offered `UNIFIED_FENCE` counts each attested answer by what
//! it attests: a promise when the copy kept its ballots (exclusion),
//! coverage when it holds what it acknowledged. `Promises`, `Covers` and
//! `OpenOK` in `docs/formal/FelixShardUnified.tla`.

use super::*;

/// A copy that kept its ballots and holds what it acknowledged.
pub(super) const TRUSTED: FenceAttestation = FenceAttestation {
    kept_from: 0,
    trusted: true,
};

/// A copy that kept its ballots but may have lost acknowledged records, as
/// one led before an unclean crash.
pub(super) const DOUBTED: FenceAttestation = FenceAttestation {
    kept_from: 0,
    trusted: false,
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
