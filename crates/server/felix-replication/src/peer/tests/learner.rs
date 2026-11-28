//! A learner's answer, and a fence answer's kept ballot, say so only to a
//! caller that reads the flag.

use felix_wire::internal::{PeerCapabilities, ReplicateOk};

use super::lanes::{connect, greet, record};
use super::*;
use crate::peer::codec::{Incoming, read_frame, write_frame};

/// Answers every request as a learner would.
struct Learner;

#[async_trait::async_trait]
impl PeerRequestHandler for Learner {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        InternalMessage::ReplicateOk(ReplicateOk {
            correlation_id: request.correlation_id(),
            durable_offset: 1,
            learner: true,
        })
    }
}

/// What the listener answers a record with, to a caller offering
/// `capabilities`.
async fn answer_to(capabilities: Option<PeerCapabilities>) -> ReplicateOk {
    let listener = Listener::start_on(PEER, Arc::new(Learner), config()).await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, capabilities).await;
    write_frame(&mut send, &record(2, 1)).await.expect("record");
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("an answer");
    connection.close(0u32.into(), b"done");
    listener.stop().await;
    match answer {
        Ok(Incoming::Message(InternalMessage::ReplicateOk(ok))) => ok,
        Ok(Incoming::Message(other)) => panic!("expected ReplicateOk, got {other:?}"),
        _ => panic!("expected ReplicateOk"),
    }
}

/// **A caller that did not offer `LEARNER_ACKS` gets the original kind.** It
/// would refuse `ReplicateLearnerOk` as unknown and never move the cursor, so
/// a learner catching up from an older leader would stall.
#[tokio::test]
async fn an_older_caller_reads_a_learners_answer_as_the_original_kind() {
    assert!(!answer_to(None).await.learner);
    assert!(
        !answer_to(Some(PeerCapabilities::LANE_RENEWAL))
            .await
            .learner
    );
}

/// **A caller that offered it is told.** Otherwise it would count the
/// learner toward its marks (#1762).
#[tokio::test]
async fn a_caller_that_reads_the_flag_is_told_the_copy_is_a_learner() {
    assert!(
        answer_to(Some(PeerCapabilities::LEARNER_ACKS))
            .await
            .learner
    );
}

/// **A kept-ballot fence answer travels only to a caller that offered
/// `KEPT_BALLOTS`.** An older leader would refuse `FenceKeptOk` as unknown,
/// and the plain answer is all it can read (#2047).
#[test]
fn a_kept_ballot_reaches_only_a_caller_that_reads_it() {
    let answer = InternalMessage::FenceOk(felix_wire::internal::FenceOk {
        correlation_id: 7,
        log_end: 3,
        commit_offset: 2,
        last_generation: 4,
        kept_ballot: true,
        attestation: None,
    });
    let kept = |offered| match crate::peer::server::for_caller(answer.clone(), offered) {
        InternalMessage::FenceOk(ok) => ok.kept_ballot,
        other => panic!("expected FenceOk, got {other:?}"),
    };

    assert!(!kept(PeerCapabilities::NONE));
    assert!(!kept(PeerCapabilities::LEARNER_ACKS));
    assert!(kept(PeerCapabilities::KEPT_BALLOTS));
}

/// **An attested fence answer reaches only a caller that offered
/// `UNIFIED_FENCE`.** An older leader refuses `FenceAttestedOk` as an unknown
/// kind, so to it the answer goes as the kind it has always read, with the
/// kept ballot kept only for a caller that offered `KEPT_BALLOTS`.
#[test]
fn an_attestation_reaches_only_a_caller_that_reads_it() {
    use felix_wire::internal::{FenceAttestation, InternalHeader, Kind};

    let answer = InternalMessage::FenceOk(felix_wire::internal::FenceOk {
        correlation_id: 7,
        log_end: 3,
        commit_offset: 2,
        last_generation: 4,
        kept_ballot: true,
        attestation: Some(FenceAttestation {
            kept_from: 2,
            trusted: true,
            by_record: false,
        }),
    });
    let kind = |offered| {
        let shaped = crate::peer::server::for_caller(answer.clone(), offered);
        let bytes = shaped.encode().expect("encode");
        InternalHeader::decode(&bytes).expect("header").kind
    };

    assert_eq!(kind(PeerCapabilities::NONE), Kind::FenceOk);
    assert_eq!(kind(PeerCapabilities::KEPT_BALLOTS), Kind::FenceKeptOk);
    assert_eq!(
        kind(PeerCapabilities::KEPT_BALLOTS.union(PeerCapabilities::UNIFIED_FENCE)),
        Kind::FenceAttestedOk
    );
    match crate::peer::server::for_caller(answer.clone(), PeerCapabilities::UNIFIED_FENCE) {
        InternalMessage::FenceOk(ok) => {
            assert!(!ok.kept_ballot, "no KEPT_BALLOTS, no kept ballot");
            assert_eq!(
                ok.attestation,
                Some(FenceAttestation {
                    kept_from: 2,
                    trusted: true,
                    by_record: false,
                })
            );
        }
        other => panic!("expected FenceOk, got {other:?}"),
    }
}

/// **To a caller without `UNIFIED_FENCE` a fence answer encodes byte for
/// byte as it did before attestations**, whatever the replica attests.
#[test]
fn an_older_callers_fence_answer_is_unchanged_by_the_attestation() {
    use felix_wire::internal::FenceAttestation;

    let answer = |attestation| {
        InternalMessage::FenceOk(felix_wire::internal::FenceOk {
            correlation_id: 7,
            log_end: 3,
            commit_offset: 2,
            last_generation: 4,
            kept_ballot: true,
            attestation,
        })
    };
    let bytes = |attestation, offered| {
        crate::peer::server::for_caller(answer(attestation), offered)
            .encode()
            .expect("encode")
    };
    for offered in [
        PeerCapabilities::NONE,
        PeerCapabilities::KEPT_BALLOTS,
        PeerCapabilities::KEPT_BALLOTS.union(PeerCapabilities::LEARNER_ACKS),
    ] {
        let plain = bytes(None, offered);
        for attestation in [
            FenceAttestation {
                kept_from: 0,
                trusted: true,
                by_record: false,
            },
            FenceAttestation {
                kept_from: u64::MAX,
                trusted: false,
                by_record: false,
            },
        ] {
            assert_eq!(bytes(Some(attestation), offered), plain, "{offered:?}");
        }
    }
}
