//! A PUBLISH from the socket to the write and back to a PUBACK.

use super::*;
use crate::write::Refusal;
use felix_wire::RecordHeaders;

const WAIT: Duration = Duration::from_secs(1);

impl Client {
    /// The next packet if one arrives within `WAIT`.
    async fn recv_within(&mut self) -> Option<Option<Packet>> {
        tokio::time::timeout(WAIT, self.recv()).await.ok()
    }

    async fn nothing_arrives(&mut self) {
        assert_eq!(self.recv_within().await, None, "no packet was due");
    }
}

async fn connected_as(version: Version, token: &'static str) -> Harness {
    let mut harness = start(Settings::default(), None);
    harness.client.version = Some(version);
    let mut c = connect(version);
    c.password = Some(Bytes::from_static(token.as_bytes()));
    harness.client.send(Packet::Connect(c), version).await;
    assert_eq!(
        connack(harness.client.recv().await).reason,
        ReasonCode::Success
    );
    harness
}

fn puback(packet: Option<Packet>) -> felix_mqtt_wire::Ack {
    match packet {
        Some(Packet::PubAck(ack)) => ack,
        other => panic!("expected PUBACK, got {other:?}"),
    }
}

fn disconnect_reason(packet: Option<Packet>) -> ReasonCode {
    match packet {
        Some(Packet::Disconnect(d)) => d.reason,
        other => panic!("expected DISCONNECT, got {other:?}"),
    }
}

async fn settle_down() {
    tokio::time::sleep(Duration::from_millis(10)).await;
}

#[tokio::test(start_paused = true)]
async fn connack_advertises_the_publish_limits() {
    let mut h = start(Settings::default(), None);
    h.client.version = Some(Version::V5);
    h.client
        .send(Packet::Connect(connect(Version::V5)), Version::V5)
        .await;
    let ack = connack(h.client.recv().await);
    assert_eq!(ack.properties.receive_maximum, Some(64));
    assert_eq!(ack.properties.maximum_qos, Some(QoS::AtLeastOnce));
    assert_eq!(ack.properties.retain_available, Some(false));
    assert_eq!(ack.properties.topic_alias_maximum, None);
}

#[tokio::test(start_paused = true)]
async fn a_puback_waits_for_the_write_to_be_acknowledged() {
    let writes = FakeWrites::new();
    writes.hold();
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(7))),
            Version::V5,
        )
        .await;
    h.client.nothing_arrives().await;
    assert_eq!(writes.batches().len(), 1, "the write is out");

    writes.release(1);
    let ack = puback(h.client.recv().await);
    assert_eq!((ack.packet_id, ack.reason), (7, ReasonCode::Success));
}

#[tokio::test(start_paused = true)]
async fn pubacks_leave_in_the_order_the_publishes_arrived() {
    let writes = FakeWrites::new();
    writes.hold();
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
            Version::V5,
        )
        .await;
    // Refused on arrival, but behind a write that has not returned.
    h.client
        .send(
            Packet::Publish(publish_to("yard/gate", QoS::AtLeastOnce, Some(2))),
            Version::V5,
        )
        .await;
    h.client.nothing_arrives().await;

    writes.release(1);
    let first = puback(h.client.recv().await);
    let second = puback(h.client.recv().await);
    assert_eq!((first.packet_id, first.reason), (1, ReasonCode::Success));
    assert_eq!(
        (second.packet_id, second.reason),
        (2, ReasonCode::TopicNameInvalid)
    );
}

#[tokio::test(start_paused = true)]
async fn publishes_that_arrive_while_a_batch_is_out_go_as_one_batch() {
    let writes = FakeWrites::new();
    writes.hold();
    let mut h = connected_with(Version::V311, Settings::default(), Arc::clone(&writes)).await;
    for id in 1..=4 {
        h.client
            .send(
                Packet::Publish(publish(QoS::AtLeastOnce, Some(id))),
                Version::V311,
            )
            .await;
        settle_down().await;
    }
    writes.release(2);
    for id in 1..=4 {
        assert_eq!(puback(h.client.recv().await).packet_id, id);
    }
    let batches = writes.batches();
    let sizes: Vec<_> = batches.iter().map(|b| b.records.len()).collect();
    assert_eq!(sizes, [1, 3]);
    let sequences: Vec<_> = batches.iter().map(|b| b.sequence).collect();
    assert_eq!(sequences, [0, 1]);
    assert_eq!(batches[0].producer_id, batches[1].producer_id);
}

#[tokio::test(start_paused = true)]
async fn a_qos_0_publish_is_written_with_nothing_to_wait_for() {
    let writes = FakeWrites::new();
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(Packet::Publish(publish(QoS::AtMostOnce, None)), Version::V5)
        .await;
    h.client.send(Packet::PingReq, Version::V5).await;
    assert!(matches!(h.client.recv().await, Some(Packet::PingResp)));
    settle_down().await;
    let batches = writes.batches();
    assert_eq!(batches.len(), 1);
    assert!(!batches[0].acknowledged);
}

#[tokio::test(start_paused = true)]
async fn each_connection_has_its_own_producer() {
    let writes = FakeWrites::new();
    let mut a = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    let mut b = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    for h in [&mut a, &mut b] {
        h.client
            .send(
                Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
                Version::V5,
            )
            .await;
        puback(h.client.recv().await);
    }
    let batches = writes.batches();
    assert_ne!(batches[0].producer_id, batches[1].producer_id);
    assert_eq!(batches[0].sequence, 0);
    assert_eq!(batches[1].sequence, 0);
}

#[tokio::test(start_paused = true)]
async fn the_record_is_the_topic_the_message_fields_and_the_payload() {
    let writes = FakeWrites::new();
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    let mut message = publish_to("plant/line1/temp", QoS::AtLeastOnce, Some(1));
    message.properties.payload_format_indicator = Some(true);
    message.properties.content_type = Some(Utf8::new("text/plain").unwrap());
    message.properties.response_topic = Some(Utf8::new("plant/replies").unwrap());
    message.properties.correlation_data = Some(Bytes::from_static(&[1, 2, 3]));
    message.properties.message_expiry_interval = Some(60);
    message.properties.user_properties = vec![
        (Utf8::new("site").unwrap(), Utf8::new("north").unwrap()),
        (Utf8::new("site").unwrap(), Utf8::new("east").unwrap()),
    ];
    message.payload = Bytes::from_static(b"21.5");
    h.client.send(Packet::Publish(message), Version::V5).await;
    puback(h.client.recv().await);

    let batch = writes.batches().remove(0);
    assert_eq!(&*batch.tenant_id, "acme");
    assert_eq!(&*batch.target.stream, "plant");
    let record = &batch.records[0];
    assert_eq!(record.payload, Bytes::from_static(b"21.5"));
    let block = RecordHeaders::decode(record.headers.clone()).unwrap();
    assert_eq!(
        block.key,
        Some(Bytes::from_static(b"plant/line1/temp")),
        "the key is the full topic"
    );
    let get = |name: &str| -> Vec<&Bytes> {
        block
            .headers
            .iter()
            .filter(|(n, _)| n == name)
            .map(|(_, v)| v)
            .collect()
    };
    assert_eq!(get("felix.mqtt.qos"), [&Bytes::from_static(b"1")]);
    assert_eq!(
        get("felix.mqtt.payload_format"),
        [&Bytes::from_static(b"1")]
    );
    assert_eq!(
        get("felix.mqtt.content_type"),
        [&Bytes::from_static(b"text/plain")]
    );
    assert_eq!(
        get("felix.mqtt.response_topic"),
        [&Bytes::from_static(b"plant/replies")]
    );
    assert_eq!(
        get("felix.mqtt.correlation_data"),
        [&Bytes::from_static(&[1, 2, 3])]
    );
    assert_eq!(
        get("site"),
        [&Bytes::from_static(b"north"), &Bytes::from_static(b"east")]
    );
    let expiry: u64 = std::str::from_utf8(get("felix.mqtt.expiry")[0])
        .unwrap()
        .parse()
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    assert!((now + 59_000..=now + 61_000).contains(&expiry), "{expiry}");
}

#[tokio::test(start_paused = true)]
async fn a_user_property_in_the_listeners_namespace_is_refused() {
    let writes = FakeWrites::new();
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    let mut message = publish(QoS::AtLeastOnce, Some(3));
    message.properties.user_properties = vec![(
        Utf8::new("felix.mqtt.qos").unwrap(),
        Utf8::new("0").unwrap(),
    )];
    h.client.send(Packet::Publish(message), Version::V5).await;
    let ack = puback(h.client.recv().await);
    assert_eq!(ack.reason, ReasonCode::ImplementationSpecificError);
    assert!(writes.batches().is_empty());
}

#[tokio::test(start_paused = true)]
async fn an_unknown_outcome_closes_the_connection_without_an_answer() {
    for version in [Version::V5, Version::V311] {
        let writes = FakeWrites::new();
        writes.fail_with(Err(WriteError::OutcomeUnknown("leadership_lost")));
        let mut h = connected_with(version, Settings::default(), Arc::clone(&writes)).await;
        h.client
            .send(Packet::Publish(publish(QoS::AtLeastOnce, Some(1))), version)
            .await;
        // Neither a PUBACK nor a DISCONNECT with a reason.
        assert!(h.client.closed().await, "{version:?}");
        assert_eq!(writes.batches().len(), 1);
    }
}

#[test]
fn an_unknown_outcome_is_counted_under_its_reason() {
    let refusals = counting(async {
        let writes = FakeWrites::new();
        writes.fail_with(Err(WriteError::OutcomeUnknown("quorum_timeout")));
        let mut h = connected_with(Version::V5, Settings::default(), writes).await;
        h.client
            .send(
                Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
                Version::V5,
            )
            .await;
        assert!(h.client.closed().await);
    });
    assert_eq!(refusals.get("quorum_timeout"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_shard_owned_elsewhere_is_told_to_5_clients_and_closes_3_1_1() {
    let writes = FakeWrites::new();
    writes.fail_with(Err(WriteError::Refused(Refusal::NotOwner)));
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(4))),
            Version::V5,
        )
        .await;
    let ack = puback(h.client.recv().await);
    assert_eq!(ack.packet_id, 4);
    assert_eq!(ack.reason, ReasonCode::ImplementationSpecificError);
    assert_eq!(
        ack.properties.reason_string.as_ref().map(|s| s.as_str()),
        Some("not_owner")
    );
    // A QoS 0 message is dropped, and the connection carries on.
    h.client
        .send(Packet::Publish(publish(QoS::AtMostOnce, None)), Version::V5)
        .await;
    h.client.send(Packet::PingReq, Version::V5).await;
    assert!(matches!(h.client.recv().await, Some(Packet::PingResp)));

    let mut h = connected_with(Version::V311, Settings::default(), writes).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
            Version::V311,
        )
        .await;
    assert!(h.client.closed().await);
}

#[test]
fn a_dropped_qos_0_message_is_counted() {
    let refusals = counting(async {
        let writes = FakeWrites::new();
        writes.fail_with(Err(WriteError::Refused(Refusal::NotOwner)));
        let mut h = connected_with(Version::V311, Settings::default(), writes).await;
        h.client
            .send(
                Packet::Publish(publish(QoS::AtMostOnce, None)),
                Version::V311,
            )
            .await;
        h.client.send(Packet::PingReq, Version::V311).await;
        assert!(matches!(h.client.recv().await, Some(Packet::PingResp)));
    });
    assert_eq!(refusals.get("not_owner"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_broker_without_record_headers_refuses_with_a_reason() {
    let writes = FakeWrites::new();
    writes.fail_with(Err(WriteError::Refused(Refusal::HeadersDisabled)));
    let mut h = connected_with(Version::V5, Settings::default(), writes).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(2))),
            Version::V5,
        )
        .await;
    let ack = puback(h.client.recv().await);
    assert_eq!(ack.reason, ReasonCode::ImplementationSpecificError);
    assert_eq!(
        ack.properties.reason_string.as_ref().map(|s| s.as_str()),
        Some("record_headers_disabled")
    );
}

#[tokio::test(start_paused = true)]
async fn a_refusal_does_not_use_up_a_sequence() {
    let writes = FakeWrites::new();
    writes.fail_with(Err(WriteError::Refused(Refusal::NotOwner)));
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
            Version::V5,
        )
        .await;
    puback(h.client.recv().await);
    writes.fail_with(Ok(()));
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(2))),
            Version::V5,
        )
        .await;
    assert_eq!(puback(h.client.recv().await).reason, ReasonCode::Success);
    let sequences: Vec<_> = writes.batches().iter().map(|b| b.sequence).collect();
    assert_eq!(sequences, [0, 0]);
}

#[tokio::test(start_paused = true)]
async fn a_principal_without_publish_permission_writes_nothing() {
    let writes = FakeWrites::new();
    let mut h = connected_as(Version::V5, "reader").await;
    h.writes = Arc::clone(&writes);
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(5))),
            Version::V5,
        )
        .await;
    let ack = puback(h.client.recv().await);
    assert_eq!((ack.packet_id, ack.reason), (5, ReasonCode::NotAuthorized));
    h.client
        .send(Packet::Publish(publish(QoS::AtMostOnce, None)), Version::V5)
        .await;
    assert_eq!(
        disconnect_reason(h.client.recv().await),
        ReasonCode::NotAuthorized
    );
    assert!(h.writes.batches().is_empty());

    let mut h = connected_as(Version::V311, "reader").await;
    h.client
        .send(
            Packet::Publish(publish(QoS::AtLeastOnce, Some(1))),
            Version::V311,
        )
        .await;
    assert!(h.client.closed().await);
    assert!(h.writes.batches().is_empty());
}

#[tokio::test(start_paused = true)]
async fn qos_2_is_not_supported() {
    let writes = FakeWrites::new();
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::ExactlyOnce, Some(1))),
            Version::V5,
        )
        .await;
    assert_eq!(
        disconnect_reason(h.client.recv().await),
        ReasonCode::QosNotSupported
    );
    assert!(h.client.closed().await);

    let mut h = connected_with(Version::V311, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(
            Packet::Publish(publish(QoS::ExactlyOnce, Some(1))),
            Version::V311,
        )
        .await;
    assert!(h.client.closed().await);
    assert!(writes.batches().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_retained_publish_is_refused() {
    let writes = FakeWrites::new();
    let mut retained = publish(QoS::AtLeastOnce, Some(1));
    retained.retain = true;

    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(Packet::Publish(retained.clone()), Version::V5)
        .await;
    assert_eq!(
        disconnect_reason(h.client.recv().await),
        ReasonCode::RetainNotSupported
    );

    let mut h = connected_with(Version::V311, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(Packet::Publish(retained), Version::V311)
        .await;
    assert!(h.client.closed().await);
    assert!(writes.batches().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_topic_alias_is_refused_because_none_is_offered() {
    let mut h = connected(Version::V5).await;
    let mut aliased = publish(QoS::AtLeastOnce, Some(1));
    aliased.properties.topic_alias = Some(1);
    h.client.send(Packet::Publish(aliased), Version::V5).await;
    assert_eq!(
        disconnect_reason(h.client.recv().await),
        ReasonCode::TopicAliasInvalid
    );
    assert!(h.writes.batches().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_5_client_past_receive_maximum_is_disconnected() {
    let writes = FakeWrites::new();
    writes.hold();
    let settings = Settings {
        max_inflight: 2,
        ..Settings::default()
    };
    let mut h = connected_with(Version::V5, settings, Arc::clone(&writes)).await;
    for id in 1..=3 {
        h.client
            .send(
                Packet::Publish(publish(QoS::AtLeastOnce, Some(id))),
                Version::V5,
            )
            .await;
        settle_down().await;
    }
    assert_eq!(
        disconnect_reason(h.client.recv().await),
        ReasonCode::ReceiveMaximumExceeded
    );
    assert!(h.client.closed().await);
}

#[tokio::test(start_paused = true)]
async fn a_3_1_1_client_past_the_limit_waits_instead_of_being_disconnected() {
    let writes = FakeWrites::new();
    writes.hold();
    let settings = Settings {
        max_inflight: 2,
        ..Settings::default()
    };
    let mut h = connected_with(Version::V311, settings, Arc::clone(&writes)).await;
    for id in 1..=4 {
        h.client
            .send(
                Packet::Publish(publish(QoS::AtLeastOnce, Some(id))),
                Version::V311,
            )
            .await;
    }
    h.client.nothing_arrives().await;
    writes.release(10);
    for id in 1..=4 {
        assert_eq!(puback(h.client.recv().await).packet_id, id);
    }
}

#[tokio::test(start_paused = true)]
async fn over_quota_the_connection_stops_reading_for_the_hold() {
    let writes = FakeWrites::new();
    *writes.quota_hold.lock().unwrap() = Duration::from_secs(3);
    let mut h = connected_with(Version::V5, Settings::default(), Arc::clone(&writes)).await;
    h.client
        .send(Packet::Publish(publish(QoS::AtMostOnce, None)), Version::V5)
        .await;
    settle_down().await;
    let start = tokio::time::Instant::now();
    h.client.send(Packet::PingReq, Version::V5).await;
    assert!(matches!(h.client.recv().await, Some(Packet::PingResp)));
    assert!(
        start.elapsed() >= Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );

    // A longer hold is cut to five seconds.
    *writes.quota_hold.lock().unwrap() = Duration::from_secs(600);
    h.client
        .send(Packet::Publish(publish(QoS::AtMostOnce, None)), Version::V5)
        .await;
    settle_down().await;
    let start = tokio::time::Instant::now();
    h.client.send(Packet::PingReq, Version::V5).await;
    assert!(matches!(h.client.recv().await, Some(Packet::PingResp)));
    assert!(
        start.elapsed() <= Duration::from_secs(6),
        "{:?}",
        start.elapsed()
    );
}
