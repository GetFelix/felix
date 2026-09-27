use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{BrokerId, MetadataRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use super::{Fixture, LOCAL, REMOTE, topic_errors};
use crate::cluster::{Placement, kafka_node_id};
use crate::service::Settings;

pub(in crate::api::tests) fn topic(name: &str) -> MetadataRequestTopic {
    MetadataRequestTopic::default()
        .with_name(Some(TopicName(StrBytes::from_string(name.to_string()))))
}

#[tokio::test]
async fn every_readable_durable_stream_is_listed_with_a_partition_per_shard() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 3, true).await;
    fixture.stream("orders", "scratch", 1, false).await;
    fixture.stream("eu.orders", "created", 1, true).await;
    fixture.stream("orders", "bad name", 1, true).await;
    let mut client = fixture.connect();

    let response = client
        .call(&MetadataRequest::default().with_topics(None), 12)
        .await;
    assert_eq!(
        topic_errors(&response),
        vec![("orders.created".to_string(), 0)],
        "in-memory, dotted-namespace and unnameable streams are left out"
    );
    let partitions = &response.topics[0].partitions;
    assert_eq!(partitions.len(), 3);
    let local = BrokerId(kafka_node_id(LOCAL));
    for (index, partition) in partitions.iter().enumerate() {
        assert_eq!(partition.partition_index, index as i32);
        assert_eq!(partition.leader_id, local);
        assert_eq!(partition.isr_nodes, vec![local]);
    }
    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.brokers[0].node_id, local);
    assert_eq!(response.brokers[0].host.as_str(), "kafka-a.test");
    assert_eq!(response.brokers[0].port, 9092);
    assert_eq!(response.controller_id, local);
    assert_eq!(
        response.cluster_id.as_ref().map(|id| id.as_str()),
        Some("test-cluster")
    );
}

#[tokio::test]
async fn partitions_name_the_shard_leader_wherever_it_is() {
    let fixture = Fixture::anonymous().await;
    fixture.cluster.add_broker(REMOTE, "kafka-b.test", 9093);
    fixture.stream("orders", "created", 3, true).await;
    fixture.cluster.place(
        "created",
        1,
        Placement::Remote {
            leader: REMOTE.to_string(),
            replicas: vec![LOCAL.to_string()],
        },
    );
    fixture.cluster.place("created", 2, Placement::Unavailable);
    let mut client = fixture.connect();

    let response = client
        .call(
            &MetadataRequest::default().with_topics(Some(vec![topic("orders.created")])),
            4,
        )
        .await;
    let partitions = &response.topics[0].partitions;
    let (local, remote) = (kafka_node_id(LOCAL), kafka_node_id(REMOTE));
    assert_eq!(partitions[0].leader_id, BrokerId(local));
    assert_eq!(partitions[1].leader_id, BrokerId(remote));
    assert_eq!(
        partitions[1].replica_nodes,
        vec![BrokerId(remote), BrokerId(local)]
    );
    assert_eq!(partitions[2].leader_id, BrokerId(-1));
    assert_eq!(partitions[2].error_code, 5, "LEADER_NOT_AVAILABLE");
    assert_eq!(response.brokers.len(), 2);
}

#[tokio::test]
async fn a_leader_without_a_kafka_listener_is_not_offered() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.cluster.place(
        "created",
        0,
        Placement::Remote {
            leader: "node-without-kafka".to_string(),
            replicas: Vec::new(),
        },
    );
    let mut client = fixture.connect();
    let response = client
        .call(
            &MetadataRequest::default().with_topics(Some(vec![topic("orders.created")])),
            9,
        )
        .await;
    assert_eq!(response.topics[0].partitions[0].error_code, 5);
    assert_eq!(response.topics[0].partitions[0].leader_id, BrokerId(-1));
}

#[tokio::test]
async fn named_topics_report_what_is_wrong_with_them() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 1, true).await;
    fixture.stream("orders", "scratch", 1, false).await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &MetadataRequest::default().with_topics(Some(vec![
                topic("orders.created"),
                topic("orders.missing"),
                topic("orders.scratch"),
                topic("no-namespace"),
            ])),
            1,
        )
        .await;
    assert_eq!(
        topic_errors(&response),
        vec![
            ("orders.created".to_string(), 0),
            ("orders.missing".to_string(), 3),
            ("orders.scratch".to_string(), 3),
            ("no-namespace".to_string(), 3),
        ]
    );
}

#[tokio::test]
async fn a_default_namespace_lets_a_bare_topic_name_a_stream() {
    let fixture = Fixture::with(Settings {
        anonymous_tenant: Some(super::TENANT.to_string()),
        default_namespace: Some("orders".to_string()),
        cluster_id: "c".to_string(),
        ..Settings::default()
    })
    .await;
    fixture.stream("orders", "created", 2, true).await;
    let mut client = fixture.connect();
    let response = client
        .call(
            &MetadataRequest::default().with_topics(Some(vec![topic("created")])),
            9,
        )
        .await;
    assert_eq!(topic_errors(&response), vec![("created".to_string(), 0)]);
    assert_eq!(response.topics[0].partitions.len(), 2);
}

#[tokio::test]
async fn every_offered_version_encodes() {
    let fixture = Fixture::anonymous().await;
    fixture.stream("orders", "created", 2, true).await;
    let mut client = fixture.connect();
    for version in 0..=12 {
        // v0 has no null list: empty means all.
        let request = if version == 0 {
            MetadataRequest::default().with_topics(Some(Vec::new()))
        } else {
            MetadataRequest::default().with_topics(None)
        };
        let response = client.call(&request, version).await;
        assert_eq!(
            topic_errors(&response),
            vec![("orders.created".to_string(), 0)],
            "v{version}"
        );
    }
}
