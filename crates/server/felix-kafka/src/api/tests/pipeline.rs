//! A connection's requests overlap their commit waits and are answered in
//! request order.

use std::time::Duration;

use bytes::{Buf, Bytes};
use kafka_protocol::messages::{MetadataRequest, ProduceResponse};
use kafka_protocol::protocol::Decodable;

use super::produce::{idempotent, request};
use super::{Client, Fixture};
use crate::cluster::WriteError;
use crate::service::connection::MAX_PIPELINED;

const PRODUCER: i64 = 7;

/// The next frame's correlation id and body.
async fn answer(client: &mut Client) -> (i32, Bytes) {
    let mut frame = tokio::time::timeout(Duration::from_secs(5), client.read_frame())
        .await
        .expect("answered in time")
        .expect("a frame");
    (frame.get_i32(), frame)
}

/// A produce answer's `(error, base offset)`.
fn produced(mut body: Bytes) -> (i16, i64) {
    // The flexible header's empty tagged fields.
    body.advance(1);
    let response = ProduceResponse::decode(&mut body, 9).expect("decode");
    let partition = &response.responses[0].partition_responses[0];
    (partition.error_code, partition.base_offset)
}

async fn claimed(fixture: &Fixture, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.cluster.claimed() < count {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{count} batches claimed"));
}

async fn nothing_answered(client: &mut Client) {
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client.read_frame())
            .await
            .is_err(),
        "nothing is answered yet"
    );
}

/// **A produce's commit wait does not hold up the next request.** The second
/// batch is claimed while the first waits on its commit; it commits first,
/// yet the answers, and a metadata request behind both, come in request
/// order.
#[tokio::test]
async fn pipelined_produces_overlap_their_commits_and_answer_in_order() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.gate_commits();
    let mut client = fixture.connect();

    let first = client
        .send(&request(1, 0, idempotent(PRODUCER, 0, &["a", "b"])), 9)
        .await;
    let second = client
        .send(&request(1, 0, idempotent(PRODUCER, 2, &["c"])), 9)
        .await;
    let metadata = client.send(&MetadataRequest::default(), 12).await;
    claimed(&fixture, 2).await;

    fixture.cluster.release(1, None);
    nothing_answered(&mut client).await;
    fixture.cluster.release(0, None);

    let (id, body) = answer(&mut client).await;
    assert_eq!((id, produced(body)), (first, (0, 0)));
    let (id, body) = answer(&mut client).await;
    assert_eq!((id, produced(body)), (second, (0, 2)));
    assert_eq!(answer(&mut client).await.0, metadata);
}

/// **A full pipeline stops reading the connection.** A client that sends
/// more than the pipeline holds has only so many batches claimed until
/// earlier answers are written, and then every one is answered in order.
#[tokio::test]
async fn a_full_pipeline_stops_reading_until_answers_are_written() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.gate_commits();
    let mut client = fixture.connect();

    let sent = MAX_PIPELINED + 4;
    let mut ids = Vec::new();
    for at in 0..sent {
        let records = idempotent(PRODUCER, at as i32, &["x"]);
        ids.push(client.send(&request(1, 0, records), 9).await);
    }
    // Waiting on MAX_PIPELINED, one queued, one blocked queueing it.
    claimed(&fixture, MAX_PIPELINED + 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fixture.cluster.claimed(), MAX_PIPELINED + 2);

    for at in 0..sent {
        claimed(&fixture, at + 1).await;
        fixture.cluster.release(at, None);
    }
    for (at, expected) in ids.into_iter().enumerate() {
        let (id, body) = answer(&mut client).await;
        assert_eq!((id, produced(body)), (expected, (0, at as i64)));
    }
}

/// **A failed commit mid-pipeline is answered in its turn, and a re-send
/// lands each record once.** The batch behind it was claimed and fails with
/// it, as a Raft group's later entries do; neither answer passes the other,
/// and re-sending both writes nothing twice and leaves no gap.
#[tokio::test]
async fn a_failed_commit_mid_pipeline_is_answered_in_order_and_re_sent_once() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.gate_commits();
    let mut client = fixture.connect();

    let first = client
        .send(&request(1, 0, idempotent(PRODUCER, 0, &["a", "b"])), 9)
        .await;
    let second = client
        .send(&request(1, 0, idempotent(PRODUCER, 2, &["c"])), 9)
        .await;
    claimed(&fixture, 2).await;
    fixture.cluster.release(1, Some(WriteError::LeadershipLost));
    nothing_answered(&mut client).await;
    fixture.cluster.release(0, Some(WriteError::LeadershipLost));

    let not_leader = kafka_protocol::ResponseError::NotLeaderOrFollower.code();
    let (id, body) = answer(&mut client).await;
    assert_eq!((id, produced(body).0), (first, not_leader));
    let (id, body) = answer(&mut client).await;
    assert_eq!((id, produced(body).0), (second, not_leader));

    // A new leader that took both entries answers the re-sends from its log.
    *fixture.cluster.gated.lock().expect("lock") = None;
    let again = super::produce::produce(&mut client, 1, idempotent(PRODUCER, 0, &["a", "b"])).await;
    assert_eq!((again.error_code, again.base_offset), (0, 0));
    let again = super::produce::produce(&mut client, 1, idempotent(PRODUCER, 2, &["c"])).await;
    assert_eq!((again.error_code, again.base_offset), (0, 2));
    let next = super::produce::produce(&mut client, 1, idempotent(PRODUCER, 3, &["d"])).await;
    assert_eq!((next.error_code, next.base_offset), (0, 3));
}

/// **A long-polling fetch is answered before the produce sent behind it,
/// and does not hold the connection past its wait.** The produce runs only
/// once the fetch is answered, as on a connection that answers one request
/// at a time.
#[tokio::test]
async fn a_long_poll_is_not_overtaken_by_a_produce_behind_it() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();

    let fetch = client
        .send(
            &super::fetch::request(&[("orders.created", 0, 0)], 1, 300),
            12,
        )
        .await;
    let produce = client
        .send(&request(1, 0, idempotent(PRODUCER, 0, &["a"])), 9)
        .await;
    let started = tokio::time::Instant::now();
    assert_eq!(answer(&mut client).await.0, fetch);
    assert!(started.elapsed() >= Duration::from_millis(250));
    let (id, body) = answer(&mut client).await;
    assert_eq!((id, produced(body)), (produce, (0, 0)));
}
