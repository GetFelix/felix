//! A learner's answer says so only to a caller that reads the flag.

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
