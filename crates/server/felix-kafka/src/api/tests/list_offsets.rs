use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{ListOffsetsRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::{Fixture, REMOTE};
use crate::cluster::Placement;

fn request(partition: i32, timestamp: i64) -> ListOffsetsRequest {
    ListOffsetsRequest::default().with_topics(vec![
        ListOffsetsTopic::default()
            .with_name(TopicName(StrBytes::from_static_str("orders.created")))
            .with_partitions(vec![
                ListOffsetsPartition::default()
                    .with_partition_index(partition)
                    .with_timestamp(timestamp),
            ]),
    ])
}

async fn answer(
    client: &mut super::Client,
    partition: i32,
    timestamp: i64,
    version: i16,
) -> (i16, i64, i64) {
    let response = client.call(&request(partition, timestamp), version).await;
    let partition = &response.topics[0].partitions[0];
    (partition.error_code, partition.timestamp, partition.offset)
}

#[tokio::test]
async fn earliest_and_latest_come_from_the_shard_log() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    let mut client = fixture.connect();
    assert_eq!(answer(&mut client, 0, -2, 1).await, (0, -1, 0));
    assert_eq!(answer(&mut client, 0, -1, 1).await, (0, -1, 0));

    fixture
        .publish("orders", "created", 0, &["a", "b", "c"])
        .await;
    for version in 1..=7 {
        assert_eq!(
            answer(&mut client, 0, -2, version).await,
            (0, -1, 0),
            "v{version}"
        );
        assert_eq!(
            answer(&mut client, 0, -1, version).await,
            (0, -1, 3),
            "v{version}"
        );
    }
}

/// `latest` is the commit point, not the log's end: a consumer that starts
/// from it must not be handed a position past what is committed.
#[tokio::test]
async fn latest_is_the_commit_point() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture
        .publish("orders", "created", 0, &["a", "b", "c"])
        .await;
    fixture.cluster.commit_until("created", 0, 1);
    let mut client = fixture.connect();
    assert_eq!(answer(&mut client, 0, -1, 1).await, (0, -1, 1));
}

#[tokio::test]
async fn a_time_finds_the_first_record_at_or_after_it() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.publish("orders", "created", 0, &["a"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let middle = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    fixture.publish("orders", "created", 0, &["b", "c"]).await;

    let mut client = fixture.connect();
    let (error, timestamp, offset) = answer(&mut client, 0, middle, 4).await;
    assert_eq!((error, offset), (0, 1));
    assert!(timestamp >= middle);
    assert_eq!(answer(&mut client, 0, 0, 4).await.2, 0, "before everything");
    assert_eq!(
        answer(&mut client, 0, i64::MAX / 2, 4).await,
        (0, -1, -1),
        "after everything"
    );

    // v7's "max timestamp" names the newest record.
    let (error, _, offset) = answer(&mut client, 0, -3, 7).await;
    assert_eq!((error, offset), (0, 2));
}

#[tokio::test]
async fn offsets_are_only_answered_by_the_leader() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.cluster.place(
        "created",
        0,
        Placement::Remote {
            leader: REMOTE.to_string(),
            replicas: Vec::new(),
        },
    );
    let mut client = fixture.connect();
    assert_eq!(answer(&mut client, 0, -1, 5).await.0, 6);
}
