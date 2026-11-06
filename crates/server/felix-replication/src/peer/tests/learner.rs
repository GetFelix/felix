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
    });
    let kept = |offered| match crate::peer::server::for_caller(answer.clone(), offered) {
        InternalMessage::FenceOk(ok) => ok.kept_ballot,
        other => panic!("expected FenceOk, got {other:?}"),
    };

    assert!(!kept(PeerCapabilities::NONE));
    assert!(!kept(PeerCapabilities::LEARNER_ACKS));
    assert!(kept(PeerCapabilities::KEPT_BALLOTS));
}

/// **A refusal's reason goes only to a caller that offered
/// `REFUSAL_REASONS`.** An older caller would reject the trailing fields as
/// a malformed body; it reads the detail it always has.
#[test]
fn a_refusal_reason_goes_only_to_a_caller_that_reads_it() {
    let refusal = InternalMessage::ReplicateError(felix_wire::internal::ReplicateError {
        correlation_id: 1,
        code: felix_wire::internal::ErrorCode::Unauthorized,
        expected_offset: 0,
        detail: crate::replica::NOT_A_REPLICA.to_string(),
        reason: Some(felix_wire::internal::RefusalReason::NotAReplica),
    });
    let reason = |caller| match crate::peer::server::for_caller(refusal.clone(), caller) {
        InternalMessage::ReplicateError(err) => err.reason,
        other => panic!("expected a refusal, got {other:?}"),
    };
    assert_eq!(reason(PeerCapabilities::LEARNER_ACKS), None);
    assert_eq!(
        reason(PeerCapabilities::REFUSAL_REASONS),
        Some(felix_wire::internal::RefusalReason::NotAReplica)
    );
}

/// Refuses every request as a replica whose ballot holds the generation for
/// another node, with the reason as fields.
struct PromisedAway;

#[async_trait::async_trait]
impl PeerRequestHandler for PromisedAway {
    async fn handle(&self, request: InternalMessage) -> InternalMessage {
        InternalMessage::ReplicateError(felix_wire::internal::ReplicateError {
            correlation_id: request.correlation_id(),
            code: felix_wire::internal::ErrorCode::FencedEpoch,
            expected_offset: 0,
            detail: crate::replica::promised_detail(7, "broker-d"),
            reason: Some(felix_wire::internal::RefusalReason::PromisedTo {
                generation: 7,
                leader: "broker-d".to_string(),
            }),
        })
    }
}

/// **A refused fence reaches an older leader in the body it decodes.**
/// Fences are answered out of turn, and that path must shape the answer for
/// its caller too: trailing reason fields would fail an older decoder's
/// check that the body ends, turning a `FencedEpoch` into a decode error.
#[tokio::test]
async fn a_fence_refused_out_of_turn_reaches_an_older_caller_without_its_reason() {
    let listener = Listener::start_on(PEER, Arc::new(PromisedAway), config()).await;
    let connection = connect(&listener).await;
    let (mut send, mut recv) = greet(&connection, Some(PeerCapabilities::LANE_RENEWAL)).await;
    let fence = InternalMessage::Fence(felix_wire::internal::Fence {
        correlation_id: 9,
        shard: shard(),
        log: felix_wire::internal::ReplicaLog::Stream,
        election: None,
    });
    write_frame(&mut send, &fence).await.expect("fence");
    let answer = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut recv))
        .await
        .expect("an answer");
    connection.close(0u32.into(), b"done");
    listener.stop().await;
    let Ok(Incoming::Message(InternalMessage::ReplicateError(refused))) = answer else {
        panic!("expected a refusal");
    };
    // The body an older build decodes: everything up to the detail, and
    // nothing after it.
    assert_eq!(
        refused.reason, None,
        "the reason went to a caller that cannot read it"
    );
    assert_eq!(refused.code, felix_wire::internal::ErrorCode::FencedEpoch);
    assert_eq!(
        crate::replica::promised_to(&refused, 7),
        Some("broker-d"),
        "read from the detail"
    );
}
