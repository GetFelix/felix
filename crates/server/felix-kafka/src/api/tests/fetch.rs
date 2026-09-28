use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::fetch_response::PartitionData;
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;
use tokio_util::sync::CancellationToken;

use super::{Fixture, REMOTE};
use crate::cluster::Placement;

/// A fetch for `(topic, partition, offset)` triples.
pub(in crate::api::tests) fn request(
    partitions: &[(&str, i32, i64)],
    min_bytes: i32,
    max_wait_ms: i32,
) -> FetchRequest {
    let mut topics: Vec<FetchTopic> = Vec::new();
    for (name, partition, offset) in partitions {
        let entry = FetchPartition::default()
            .with_partition(*partition)
            .with_fetch_offset(*offset)
            .with_partition_max_bytes(1 << 20);
        match topics.iter_mut().find(|t| t.topic.as_str() == *name) {
            Some(topic) => topic.partitions.push(entry),
            None => topics.push(
                FetchTopic::default()
                    .with_topic(TopicName(StrBytes::from_string(name.to_string())))
                    .with_partitions(vec![entry]),
            ),
        }
    }
    FetchRequest::default()
        .with_max_wait_ms(max_wait_ms)
        .with_min_bytes(min_bytes)
        .with_max_bytes(1 << 24)
        .with_session_epoch(-1)
        .with_topics(topics)
}

/// `(offset, value)` for every record in a partition's answer.
pub(in crate::api::tests) fn records(partition: &PartitionData) -> Vec<(i64, String)> {
    let Some(bytes) = partition.records.clone().filter(|b| !b.is_empty()) else {
        return Vec::new();
    };
    RecordBatchDecoder::decode_all(&mut bytes.clone())
        .expect("decode")
        .into_iter()
        .flat_map(|set| set.records)
        .map(|record| {
            let value = record.value.unwrap_or_else(Bytes::new);
            (
                record.offset,
                String::from_utf8(value.to_vec()).expect("utf8"),
            )
        })
        .collect()
}

fn only(response: &FetchResponse) -> &PartitionData {
    &response.responses[0].partitions[0]
}

#[tokio::test]
async fn fetch_reads_from_the_offset_asked_for() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture
        .publish("orders", "created", 0, &["r0", "r1", "r2", "r3"])
        .await;
    let mut client = fixture.connect();

    let response = client
        .call(&request(&[("orders.created", 0, 0)], 1, 500), 11)
        .await;
    let partition = only(&response);
    assert_eq!(partition.error_code, 0);
    assert_eq!(partition.high_watermark, 4);
    assert_eq!(partition.log_start_offset, 0);
    assert_eq!(
        records(partition),
        vec![
            (0, "r0".to_string()),
            (1, "r1".to_string()),
            (2, "r2".to_string()),
            (3, "r3".to_string())
        ]
    );

    let response = client
        .call(&request(&[("orders.created", 0, 2)], 1, 500), 4)
        .await;
    assert_eq!(
        records(only(&response)),
        vec![(2, "r2".to_string()), (3, "r3".to_string())]
    );
}

#[tokio::test]
async fn one_fetch_reads_every_partition_of_a_sharded_topic() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 3, true).await;
    for shard in 0..3 {
        let value = format!("shard-{shard}");
        fixture
            .publish("orders", "created", shard, &[value.as_str()])
            .await;
    }
    let mut client = fixture.connect();
    let response = client
        .call(
            &request(
                &[
                    ("orders.created", 0, 0),
                    ("orders.created", 1, 0),
                    ("orders.created", 2, 0),
                ],
                1,
                500,
            ),
            12,
        )
        .await;
    let seen: Vec<Vec<(i64, String)>> = response.responses[0]
        .partitions
        .iter()
        .map(records)
        .collect();
    assert_eq!(
        seen,
        vec![
            vec![(0, "shard-0".to_string())],
            vec![(0, "shard-1".to_string())],
            vec![(0, "shard-2".to_string())],
        ]
    );
}

#[tokio::test]
async fn an_offset_outside_the_log_is_out_of_range() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["r0"]).await;
    let mut client = fixture.connect();
    for offset in [2, -5] {
        let response = client
            .call(&request(&[("orders.created", 0, offset)], 1, 5_000), 11)
            .await;
        assert_eq!(
            only(&response).error_code,
            1,
            "OFFSET_OUT_OF_RANGE for {offset}"
        );
        assert_eq!(only(&response).high_watermark, 1);
    }
}

#[tokio::test]
async fn a_long_poll_returns_a_record_published_while_it_waits() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["before"]).await;
    let mut client = fixture.connect();

    let started = Instant::now();
    let pending = tokio::spawn(async move {
        let response = client
            .call(&request(&[("orders.created", 0, 1)], 1, 20_000), 11)
            .await;
        (response, started.elapsed())
    });
    // Long enough for the fetch to find nothing and start waiting.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!pending.is_finished(), "a fetch at the tail waits");
    fixture.publish("orders", "created", 0, &["after"]).await;

    let (response, elapsed) = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("woken by the publish, not the 20 s wait")
        .expect("task");
    assert_eq!(records(only(&response)), vec![(1, "after".to_string())]);
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[tokio::test]
async fn a_long_poll_with_nothing_new_ends_at_max_wait() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let started = Instant::now();
    let response = client
        .call(&request(&[("orders.created", 0, 0)], 1, 300), 11)
        .await;
    let elapsed = started.elapsed();
    assert_eq!(only(&response).error_code, 0);
    assert!(records(only(&response)).is_empty());
    assert!(elapsed >= Duration::from_millis(250), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");

    // min_bytes 0 does not wait at all.
    let started = Instant::now();
    client
        .call(&request(&[("orders.created", 0, 0)], 0, 10_000), 11)
        .await;
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn shutdown_ends_a_long_poll_early() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let shutdown = CancellationToken::new();
    let mut client = fixture.connect_until(shutdown.clone());
    let pending = tokio::spawn(async move {
        client
            .call(&request(&[("orders.created", 0, 0)], 1, 20_000), 11)
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    shutdown.cancel();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("answered promptly")
        .expect("task");
    assert_eq!(only(&response).error_code, 0);
}

#[tokio::test]
async fn a_partition_led_elsewhere_sends_the_client_back_to_metadata() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 2, true).await;
    fixture.publish("orders", "created", 0, &["here"]).await;
    fixture.cluster.place(
        "created",
        1,
        Placement::Remote {
            leader: REMOTE.to_string(),
            replicas: Vec::new(),
        },
    );
    let mut client = fixture.connect();
    let started = Instant::now();
    let response = client
        .call(
            &request(
                &[("orders.created", 0, 0), ("orders.created", 1, 0)],
                1_000_000,
                10_000,
            ),
            11,
        )
        .await;
    let partitions = &response.responses[0].partitions;
    assert_eq!(records(&partitions[0]), vec![(0, "here".to_string())]);
    assert_eq!(partitions[1].error_code, 6, "NOT_LEADER_OR_FOLLOWER");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "an error answers at once rather than waiting out max_wait"
    );
}

#[tokio::test]
async fn unknown_topics_and_partitions_are_reported() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &request(
                &[("orders.created", 1, 0), ("orders.missing", 0, 0)],
                1,
                100,
            ),
            11,
        )
        .await;
    assert_eq!(response.responses[0].partitions[0].error_code, 3);
    assert_eq!(response.responses[1].partitions[0].error_code, 3);
}

#[tokio::test]
async fn max_bytes_bounds_a_fetch_but_never_below_one_record() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let big = "x".repeat(4_000);
    fixture
        .publish(
            "orders",
            "created",
            0,
            &[big.as_str(), big.as_str(), big.as_str()],
        )
        .await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &request(&[("orders.created", 0, 0)], 1, 100).with_max_bytes(10),
            11,
        )
        .await;
    assert_eq!(
        records(only(&response)).len(),
        1,
        "one record despite the limit"
    );
}

#[tokio::test]
async fn an_unknown_fetch_session_is_refused() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &request(&[], 1, 100)
                .with_session_id(42)
                .with_session_epoch(1),
            11,
        )
        .await;
    assert_eq!(response.error_code, 70, "FETCH_SESSION_ID_NOT_FOUND");
}

/// **A fetch stops at the commit point, not the log's end.** On a `Quorum`
/// shard a record past the quorum mark can be lost at failover and its
/// offset reused, so a consumer that read it would hold a position the next
/// leader's log contradicts.
#[tokio::test]
async fn a_fetch_reads_only_to_the_commit_point() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture
        .publish("orders", "created", 0, &["r0", "r1", "r2", "r3"])
        .await;
    fixture.cluster.commit_until("created", 0, 2);
    let mut client = fixture.connect();

    let response = client
        .call(&request(&[("orders.created", 0, 0)], 1, 0), 11)
        .await;
    let partition = only(&response);
    assert_eq!(partition.error_code, 0);
    assert_eq!(partition.high_watermark, 2);
    assert_eq!(partition.last_stable_offset, 2);
    assert_eq!(
        records(partition),
        vec![(0, "r0".to_string()), (1, "r1".to_string())]
    );

    // Past the commit point but within the log: not readable yet, and not an
    // error either.
    let response = client
        .call(&request(&[("orders.created", 0, 3)], 1, 0), 11)
        .await;
    assert_eq!(only(&response).error_code, 0);
    assert!(records(only(&response)).is_empty());
}

/// A fetch waiting at the commit point returns once the mark moves; the
/// broker wakes it the way it does for an append.
#[tokio::test]
async fn a_long_poll_at_the_commit_point_returns_once_it_moves() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["r0", "r1"]).await;
    fixture.cluster.commit_until("created", 0, 1);
    let mut client = fixture.connect();

    let pending = tokio::spawn(async move {
        client
            .call(&request(&[("orders.created", 0, 1)], 1, 20_000), 11)
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!pending.is_finished(), "r1 is not committed yet");

    fixture.cluster.commit_until("created", 0, 2);
    fixture
        .broker
        .resolve_stream_handle(super::TENANT, "orders", "created", 0)
        .await
        .expect("handle")
        .appended()
        .notify_waiters();
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("woken when the mark moved")
        .expect("task");
    assert_eq!(records(only(&response)), vec![(1, "r1".to_string())]);
}

/// A generation-start record holds an offset and is never returned. Each
/// record around it keeps its own offset, and a fetch from the
/// generation-start's offset gets the record after it.
#[tokio::test]
async fn fetch_skips_a_generation_start_record() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["r0", "r1"]).await;
    let marker = fixture.generation_start("orders", "created", 0).await;
    assert_eq!(marker, 2);
    fixture.publish("orders", "created", 0, &["r3", "r4"]).await;
    let mut client = fixture.connect();

    let response = client
        .call(&request(&[("orders.created", 0, 0)], 1, 500), 11)
        .await;
    let partition = only(&response);
    assert_eq!(partition.high_watermark, 5);
    assert_eq!(
        records(partition),
        vec![
            (0, "r0".to_string()),
            (1, "r1".to_string()),
            (3, "r3".to_string()),
            (4, "r4".to_string())
        ]
    );

    let response = client
        .call(&request(&[("orders.created", 0, 2)], 1, 500), 11)
        .await;
    assert_eq!(
        records(only(&response)),
        vec![(3, "r3".to_string()), (4, "r4".to_string())]
    );
}

/// With a generation-start record last, a consumer caught up to it is moved
/// past it by an empty batch rather than left one behind the high watermark.
#[tokio::test]
async fn fetch_moves_a_consumer_past_a_trailing_generation_start() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["r0"]).await;
    let marker = fixture.generation_start("orders", "created", 0).await;
    let mut client = fixture.connect();

    let response = client
        .call(
            &request(&[("orders.created", 0, marker as i64)], 1, 500),
            11,
        )
        .await;
    let partition = only(&response);
    assert_eq!(partition.error_code, 0);
    assert_eq!(partition.high_watermark, 2);
    assert!(records(partition).is_empty());
    let bytes = partition.records.clone().expect("a batch");
    let batches = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).expect("info");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].record_count, 0);
    assert_eq!(batches[0].min_offset, marker as i64);
    // Where the consumer fetches next: past the generation start.
    let next = batches[0].min_offset + i64::from(last_offset_delta(&bytes)) + 1;
    assert_eq!(next, 2);

    // From the start, the record comes back and the consumer still ends past
    // the trailing marker.
    let response = client
        .call(&request(&[("orders.created", 0, 0)], 1, 500), 11)
        .await;
    assert_eq!(records(only(&response)), vec![(0, "r0".to_string())]);
    let bytes = only(&response).records.clone().expect("batches");
    let batches = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).expect("info");
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[1].min_offset, marker as i64);
    assert_eq!(batches[1].record_count, 0);
}

/// The last offset delta of the first batch in `bytes`, which the decoder's
/// summary does not report.
fn last_offset_delta(bytes: &Bytes) -> i32 {
    // base offset, length, leader epoch, magic, crc, attributes.
    let at = 8 + 4 + 4 + 1 + 4 + 2;
    i32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}
