//! The directory's copy-trust record: after an unclean start a copy stays
//! untrusted, and a crashed leader's copy doubted, across clean restarts
//! until a leader vouches for it.

use felix_storage::run_state::copy_trust::CopyTrust;
use felix_storage::run_state::{BootId, begin};

use super::fence::{attested, fence, kept_marks, labelled, led_before_a_crash, taken};
use super::*;

/// A directory whose last start rotated its storage id, as a machine crash
/// under a relaxed fsync mode does. Kept apart from the broker's own data so
/// the broker's storage id is left alone.
fn rotated_directory() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut next = 0u8;
    let mut fresh = move || {
        next += 1;
        [next; 16]
    };
    for boot in [1, 2] {
        begin(
            dir.path(),
            true,
            None,
            Some(BootId::from_bytes([boot; 16])),
            || false,
            &mut fresh,
        )
        .expect("begin");
    }
    dir
}

/// One process on the directory: the record as a start reads it.
fn marks_on(trust_dir: &std::path::Path) -> Arc<crate::quorum::QuorumMarks> {
    let marks = kept_marks();
    marks.set_copy_trust(Arc::new(CopyTrust::open(trust_dir).expect("open")));
    marks
}

/// A batch at generation 9 from `first_offset`, labelled as starting at 0,
/// carrying `mark`.
fn at_nine(first_offset: u64, values: &[&str], mark: u64) -> ReplicateRecords {
    ReplicateRecords {
        commit_offset: Some(mark),
        generations: Some(vec![felix_wire::internal::GenerationStart {
            generation: 9,
            start_offset: 0,
        }]),
        ..batch(9, first_offset, values)
    }
}

async fn ship(handler: &ReplicaHandler, records: ReplicateRecords) {
    let answer = handler
        .apply(SENDER, records, felix_broker::LogKind::Stream)
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
}

/// **A copy that answered a fence after an unclean start attests untrusted
/// after any number of clean restarts, until a leader ships it level through
/// both its generation's start and its mark.** The fence's ballot leaves no
/// trace of the crash on the copy, and a clean start has no unclean record,
/// so only the directory's record keeps the copy from covering what its log
/// may have lost (`StandInNoCatchUp` in `docs/formal/FelixShardUnified.tla`).
/// A batch short of its mark leaves the record as it was (`VouchBelowStart`).
#[tokio::test]
async fn an_unclean_copy_stays_untrusted_across_clean_restarts_until_vouched_through_the_mark() {
    let dir = tempfile::tempdir().expect("tempdir");
    let trust_dir = rotated_directory();
    {
        let broker = broker_on(dir.path());
        ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 4))
            .apply(SENDER, batch(4, 0, &["a"]), felix_broker::LogKind::Stream)
            .await;
    }

    // The unclean start answers a fence, untrusted.
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 5))
            .with_fleet(marks_on(trust_dir.path()))
            .with_unclean_start(Arc::new(crate::unclean::UncleanStart::new(LOCAL)));
        assert_eq!(
            taken(&handler.fence(SENDER, fence(5)).await).attestation,
            attested(0, false)
        );
    }

    // Clean restarts: no unclean record, and still untrusted.
    for generation in 6..9 {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], generation))
            .with_fleet(marks_on(trust_dir.path()));
        assert_eq!(
            taken(&handler.fence(SENDER, fence(generation)).await).attestation,
            attested(0, false),
            "a clean restart at {generation}"
        );
    }

    // A leader ships the copy level through its start but short of its mark.
    // Its generation starts at 0, so the batch is level through the start.
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 9))
            .with_fleet(marks_on(trust_dir.path()));
        let records = at_nine(0, &["a", "b"], 5);
        ship(&handler, records).await;
    }
    {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 9))
            .with_fleet(marks_on(trust_dir.path()));
        assert_eq!(
            handler.attestation(&key(), true, false),
            attested(0, false).expect("some"),
            "a batch short of the mark vouches for nothing"
        );

        // Then through both.
        let records = at_nine(2, &["c"], 3);
        ship(&handler, records).await;
    }
    for _ in 0..2 {
        let broker = broker_on(dir.path());
        let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL], 10))
            .with_fleet(marks_on(trust_dir.path()));
        assert_eq!(
            taken(&handler.fence(SENDER, fence(10)).await).attestation,
            attested(0, true),
            "the vouch is on disk"
        );
    }
}

/// **A copy this broker led before a crash stays refused after it takes a
/// newer leader's ballot and the broker restarts cleanly**, until that leader
/// ships it level with its generation's start. The ballot naming the newer
/// leader replaces the one that said the copy was led here, and a clean
/// start knows nothing of the crash, so only the directory's record keeps an
/// older caller from counting the short copy (#1295, #2029).
#[tokio::test]
async fn a_crashed_leaders_copy_stays_doubted_after_a_ballot_and_a_clean_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let trust_dir = rotated_directory();
    led_before_a_crash(dir.path()).await;

    // The unclean start: a record it led at 5 comes back from the leader at
    // 6, which takes the ballot.
    {
        let broker = broker_on(dir.path());
        let leader = ReplicaHandler::new(
            Arc::clone(&broker),
            router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
        )
        .with_fleet(marks_on(trust_dir.path()))
        .with_unclean_start(Arc::new(
            crate::unclean::UncleanStart::new(LOCAL)
                .with_copy_trust(Arc::new(CopyTrust::open(trust_dir.path()).expect("open"))),
        ));
        let answer = leader
            .apply(
                Some("broker-c"),
                labelled(6, 5, 0, &["a"]),
                felix_broker::LogKind::Stream,
            )
            .await;
        assert!(
            matches!(answer, InternalMessage::ReplicateOk(_)),
            "{answer:?}"
        );
        let log = broker
            .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
            .await
            .expect("log");
        assert_eq!(log.accepted_leader().as_deref(), Some("broker-c"));
        assert!(!log.gave_up());
    }

    // Restarted cleanly: an older caller's fence is still refused.
    let broker = broker_on(dir.path());
    let marks = marks_on(trust_dir.path());
    let next = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-a", &[LOCAL, "broker-c"], 7),
    )
    .with_fleet(Arc::clone(&marks));
    assert_eq!(
        refusal(&next.fence(SENDER, fence(7)).await).code,
        ErrorCode::Unavailable
    );
    assert_eq!(
        next.attestation(&key(), true, false),
        attested(0, false).expect("some")
    );

    // The leader at 6 ships it level with where its generation starts.
    let leader = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-c", &[LOCAL, "broker-a"], 6),
    )
    .with_fleet(marks);
    let answer = leader
        .apply(
            Some("broker-c"),
            labelled(6, 6, 1, &["start"]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );
    drop((next, leader, broker));

    let broker = broker_on(dir.path());
    let next = ReplicaHandler::new(
        Arc::clone(&broker),
        router_led_by("broker-a", &[LOCAL, "broker-c"], 7),
    )
    .with_fleet(marks_on(trust_dir.path()));
    taken(&next.fence(SENDER, fence(7)).await);
}

/// **A crashed leader's copy that answered a unified fence stays untrusted
/// after a clean restart, and an older caller's fence stays refused.** The
/// ballot the unified fence hands it replaces the one that said it was led
/// here, so only the directory's record keeps the doubt (#1295).
#[tokio::test]
async fn a_crashed_leaders_unified_answer_stays_untrusted_after_a_clean_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let trust_dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    {
        let broker = broker_on(dir.path());
        // One record shared, as a broker's start wires it.
        let trust = Arc::new(CopyTrust::open(trust_dir.path()).expect("open"));
        let marks = kept_marks();
        marks.set_copy_trust(Arc::clone(&trust));
        let handler =
            ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
                .with_fleet(marks)
                .with_unclean_start(Arc::new(
                    crate::unclean::UncleanStart::new(LOCAL).with_copy_trust(trust),
                ));
        let ok = taken(
            &handler
                .fence_from(SENDER, PeerCapabilities::UNIFIED_FENCE, fence(6))
                .await,
        );
        assert_eq!(ok.attestation, attested(0, false));
    }

    let broker = broker_on(dir.path());
    let next = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_fleet(marks_on(trust_dir.path()));
    assert_eq!(
        next.attestation(&key(), true, false),
        attested(0, false).expect("some")
    );
    assert_eq!(
        refusal(&next.fence(SENDER, fence(7)).await).code,
        ErrorCode::Unavailable
    );
}

/// **A crashed leader's copy refuses a unified fence it cannot record as
/// doubted.** Its ballot would erase the given-up mark, so a clean restart
/// would trust the copy.
#[cfg(unix)]
#[tokio::test]
async fn a_crashed_leaders_copy_refuses_a_unified_fence_when_the_doubt_is_not_recorded() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let trust_dir = tempfile::tempdir().expect("tempdir");
    led_before_a_crash(dir.path()).await;
    let broker = broker_on(dir.path());
    let trust = Arc::new(CopyTrust::open(trust_dir.path()).expect("open"));
    let marks = kept_marks();
    marks.set_copy_trust(Arc::clone(&trust));
    let handler = ReplicaHandler::new(Arc::clone(&broker), router_with(&[LOCAL, "broker-c"], 6))
        .with_fleet(marks)
        .with_unclean_start(Arc::new(
            crate::unclean::UncleanStart::new(LOCAL).with_copy_trust(trust),
        ));
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    let before = log.accepted_generation();
    std::fs::set_permissions(trust_dir.path(), std::fs::Permissions::from_mode(0o500))
        .expect("read-only");

    let answer = handler
        .fence_from(SENDER, PeerCapabilities::UNIFIED_FENCE, fence(6))
        .await;
    std::fs::set_permissions(trust_dir.path(), std::fs::Permissions::from_mode(0o700))
        .expect("writable");
    assert_eq!(
        refusal(&answer).code,
        ErrorCode::StorageFailed,
        "{answer:?}"
    );
    assert_eq!(log.accepted_generation(), before);
}
