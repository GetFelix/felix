use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddPartitionsToTxnRequest, EndTxnRequest, FindCoordinatorRequest,
    GroupId, InitProducerIdRequest, TopicName, TransactionalId, TxnOffsetCommitRequest,
};
use kafka_protocol::protocol::StrBytes;

use super::Fixture;
use crate::api::transactions::MESSAGE;

#[tokio::test]
async fn each_idempotent_producer_gets_its_own_id_at_epoch_zero() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    let mut ids = Vec::new();
    for version in [0, 4] {
        let response = client
            .call(&InitProducerIdRequest::default(), version)
            .await;
        assert_eq!(response.error_code, 0);
        assert!(response.producer_id.0 > 0);
        // Fits the bits a Felix producer id keeps beside the epoch.
        assert!(response.producer_id.0 as u64 <= crate::api::producer_id::KAFKA_ID_MASK);
        assert_eq!(response.producer_epoch, 0);
        ids.push(response.producer_id.0);
    }
    assert_ne!(ids[0], ids[1]);
}

#[tokio::test]
async fn an_unauthenticated_connection_gets_no_producer_id() {
    let fixture = Fixture::secured(&[]).await;
    let mut client = fixture.connect();
    let response = client.call(&InitProducerIdRequest::default(), 4).await;
    assert_eq!(response.error_code, 31, "CLUSTER_AUTHORIZATION_FAILED");
}

/// A transactional producer is refused at each step it could reach: its
/// coordinator lookup (with a message), its producer id, and the
/// transaction APIs.
#[tokio::test]
async fn transactions_are_refused_everywhere_a_producer_could_start_one() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();

    let lookup = client
        .call(
            &FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("tx"))
                .with_key_type(1),
            2,
        )
        .await;
    assert_eq!(
        lookup.error_code, 53,
        "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
    );
    assert_eq!(
        lookup.error_message.as_ref().map(|m| m.as_str()),
        Some(MESSAGE)
    );

    let init = client
        .call(
            &InitProducerIdRequest::default()
                .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("tx")))),
            4,
        )
        .await;
    assert_eq!(init.error_code, 53);
    assert_eq!(init.producer_id.0, -1);

    let end = client
        .call(
            &EndTxnRequest::default()
                .with_transactional_id(TransactionalId(StrBytes::from_static_str("tx"))),
            3,
        )
        .await;
    assert_eq!(end.error_code, 53);
}

fn topic(name: &'static str) -> TopicName {
    TopicName(StrBytes::from_static_str(name))
}

/// **The transaction APIs a client sends mid-transaction are refused per
/// partition, echoing every topic and partition it named**, so a client that
/// got past the coordinator lookup still learns why for each one.
#[tokio::test]
async fn mid_transaction_requests_are_refused_for_every_partition() {
    let fixture = Fixture::anonymous().await;
    let mut client = fixture.connect();
    let tx = || TransactionalId(StrBytes::from_static_str("tx"));

    let added = client
        .call(
            &AddPartitionsToTxnRequest::default()
                .with_v3_and_below_transactional_id(tx())
                .with_v3_and_below_topics(vec![
                    AddPartitionsToTxnTopic::default()
                        .with_name(topic("orders"))
                        .with_partitions(vec![0, 2]),
                    AddPartitionsToTxnTopic::default()
                        .with_name(topic("audit"))
                        .with_partitions(vec![1]),
                ]),
            3,
        )
        .await;
    let answered: Vec<(String, Vec<(i32, i16)>)> = added
        .results_by_topic_v3_and_below
        .iter()
        .map(|t| {
            (
                t.name.0.to_string(),
                t.results_by_partition
                    .iter()
                    .map(|p| (p.partition_index, p.partition_error_code))
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        answered,
        vec![
            ("orders".to_string(), vec![(0, 53), (2, 53)]),
            ("audit".to_string(), vec![(1, 53)]),
        ]
    );

    let offsets = client
        .call(
            &AddOffsetsToTxnRequest::default()
                .with_transactional_id(tx())
                .with_group_id(GroupId(StrBytes::from_static_str("g"))),
            3,
        )
        .await;
    assert_eq!(offsets.error_code, 53);

    let committed = client
        .call(
            &TxnOffsetCommitRequest::default()
                .with_transactional_id(tx())
                .with_group_id(GroupId(StrBytes::from_static_str("g")))
                .with_topics(vec![
                    TxnOffsetCommitRequestTopic::default()
                        .with_name(topic("orders"))
                        .with_partitions(vec![
                            TxnOffsetCommitRequestPartition::default()
                                .with_partition_index(4)
                                .with_committed_offset(10),
                            TxnOffsetCommitRequestPartition::default()
                                .with_partition_index(5)
                                .with_committed_offset(11),
                        ]),
                ]),
            3,
        )
        .await;
    let answered: Vec<(String, Vec<(i32, i16)>)> = committed
        .topics
        .iter()
        .map(|t| {
            (
                t.name.0.to_string(),
                t.partitions
                    .iter()
                    .map(|p| (p.partition_index, p.error_code))
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        answered,
        vec![("orders".to_string(), vec![(4, 53), (5, 53)])]
    );
}
