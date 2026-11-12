//! A follower whose log stamps each record with its writer's generation.
use felix_wire::internal::GenerationStart;

use super::*;

/// A follower whose logs are on segment format v9 from offset 1: the first
/// record fills the segment the log opened with, which is older, so the next
/// one starts a v9 segment.
async fn stamping_follower(
    generation: u64,

    handler_for: impl FnOnce(Arc<Broker>) -> ReplicaHandler,
) -> (Arc<Broker>, ReplicaHandler, TempDir) {
    let (broker, dir) = broker_with_storage().await;
    broker.record_generations_when(|| true);
    let handler = handler_for(Arc::clone(&broker));
    let big = "x".repeat(8 * 1024);
    let filler = handler
        .apply(
            SENDER,
            batch(generation, 0, &[&big]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(filler, InternalMessage::ReplicateOk(_)),
        "{filler:?}"
    );
    (broker, handler, dir)
}

fn written_by(mut batch: ReplicateRecords, runs: &[(u64, u64)]) -> ReplicateRecords {
    batch.writers = Some(
        runs.iter()
            .map(|&(generation, start_offset)| GenerationStart {
                generation,
                start_offset,
            })
            .collect(),
    );
    batch
}

/// **A batch that does not say who wrote it is refused**, and nothing is
/// stored: the follower would otherwise stamp its own generation on records
/// another one wrote.
#[tokio::test]
async fn a_batch_without_writers_is_refused_as_unsupported() {
    let (broker, handler, _dir) = stamping_follower(4, |broker| {
        ReplicaHandler::new(broker, router_with(&[LOCAL], 4))
    })
    .await;

    let answer = handler
        .apply(
            SENDER,
            batch(4, 1, &["a", "b"]),
            felix_broker::LogKind::Stream,
        )
        .await;

    let refused = refusal(&answer);
    assert_eq!(refused.code, ErrorCode::UnsupportedKind);
    assert!(refused.detail.contains("writer_generations"), "{refused:?}");
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.tail_offset().await.expect("tail"), 1);
}

/// **A writer below the generation of the record before it halts the
/// follower as diverged**, and nothing is stored or truncated.
#[tokio::test]
async fn a_writer_below_the_floor_is_a_conflict() {
    let (broker, handler, _dir) = stamping_follower(6, |broker| {
        ReplicaHandler::new(broker, router_with(&[LOCAL], 6))
    })
    .await;

    let answer = handler
        .apply(
            SENDER,
            written_by(batch(6, 1, &["a", "b"]), &[(5, 1)]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert!(
        matches!(answer, InternalMessage::ReplicateOk(_)),
        "{answer:?}"
    );

    let answer = handler
        .apply(
            SENDER,
            written_by(batch(6, 3, &["c"]), &[(4, 3)]),
            felix_broker::LogKind::Stream,
        )
        .await;
    assert_eq!(refusal(&answer).code, ErrorCode::LogConflict);
    let log = broker
        .shard_log(felix_broker::LogKind::Stream, TENANT, NAMESPACE, STREAM, 0)
        .await
        .expect("log");
    assert_eq!(log.tail_offset().await.expect("tail"), 3);
}
