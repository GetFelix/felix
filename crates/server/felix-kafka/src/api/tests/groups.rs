use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    FindCoordinatorRequest, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest,
    OffsetCommitRequest, OffsetFetchRequest, SyncGroupRequest, TopicName,
};
use kafka_protocol::protocol::StrBytes;

use super::Fixture;
use crate::api::groups::MESSAGE;

#[tokio::test]
async fn find_coordinator_is_refused_with_a_reason_at_every_version() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    for version in 0..=3 {
        let response = client
            .call(
                &FindCoordinatorRequest::default().with_key(StrBytes::from_static_str("g")),
                version,
            )
            .await;
        assert_eq!(
            response.error_code, 30,
            "GROUP_AUTHORIZATION_FAILED at v{version}"
        );
        if version >= 1 {
            assert_eq!(
                response.error_message.as_ref().map(|m| m.as_str()),
                Some(MESSAGE)
            );
        }
    }
    let response = client
        .call(
            &FindCoordinatorRequest::default()
                .with_coordinator_keys(vec![StrBytes::from_static_str("g")]),
            4,
        )
        .await;
    assert_eq!(response.coordinators.len(), 1);
    assert_eq!(response.coordinators[0].key.as_str(), "g");
    assert_eq!(response.coordinators[0].error_code, 30);
}

#[tokio::test]
async fn group_apis_reached_anyway_get_the_same_refusal() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    assert_eq!(
        client
            .call(&JoinGroupRequest::default(), 5)
            .await
            .error_code,
        30
    );
    assert_eq!(
        client
            .call(&HeartbeatRequest::default(), 3)
            .await
            .error_code,
        30
    );
    assert_eq!(
        client
            .call(&OffsetFetchRequest::default(), 5)
            .await
            .error_code,
        30
    );
}

/// **Syncing or leaving a group is refused the same way as joining one.**
#[tokio::test]
async fn sync_and_leave_get_the_group_refusal() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    for version in [0, 3] {
        let synced = client.call(&SyncGroupRequest::default(), version).await;
        assert_eq!(synced.error_code, 30, "SyncGroup v{version}");
        let left = client.call(&LeaveGroupRequest::default(), version).await;
        assert_eq!(left.error_code, 30, "LeaveGroup v{version}");
    }
}

/// **An offset commit is refused for every partition it names, echoing each
/// one**, so a consumer committing many partitions sees why for all of them.
#[tokio::test]
async fn an_offset_commit_is_refused_for_every_partition() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    let partitions = |indexes: &[i32]| {
        indexes
            .iter()
            .map(|index| {
                OffsetCommitRequestPartition::default()
                    .with_partition_index(*index)
                    .with_committed_offset(100)
            })
            .collect()
    };
    let committed = client
        .call(
            &OffsetCommitRequest::default().with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(partitions(&[0, 3])),
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("audit")))
                    .with_partitions(partitions(&[1])),
            ]),
            7,
        )
        .await;
    let answered: Vec<(String, Vec<(i32, i16)>)> = committed
        .topics
        .iter()
        .map(|topic| {
            (
                topic.name.0.to_string(),
                topic
                    .partitions
                    .iter()
                    .map(|p| (p.partition_index, p.error_code))
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        answered,
        vec![
            ("orders".to_string(), vec![(0, 30), (3, 30)]),
            ("audit".to_string(), vec![(1, 30)]),
        ]
    );
}

/// **A batched v4 lookup for a transaction coordinator gets the transaction
/// refusal and its message, not the group one.**
#[tokio::test]
async fn a_batched_transaction_coordinator_lookup_gets_the_transaction_refusal() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &FindCoordinatorRequest::default()
                .with_key_type(1)
                .with_coordinator_keys(vec![
                    StrBytes::from_static_str("tx-a"),
                    StrBytes::from_static_str("tx-b"),
                ]),
            4,
        )
        .await;
    let keys: Vec<(&str, i16, i32)> = response
        .coordinators
        .iter()
        .map(|c| (c.key.as_str(), c.error_code, c.node_id.0))
        .collect();
    assert_eq!(keys, vec![("tx-a", 53, -1), ("tx-b", 53, -1)]);
    assert_eq!(
        response.coordinators[0]
            .error_message
            .as_ref()
            .map(|m| m.as_str()),
        Some(crate::api::transactions::MESSAGE)
    );
}
