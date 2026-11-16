//! SUBSCRIBE, UNSUBSCRIBE and delivery at QoS 0 and 1.

use std::collections::HashMap;
use std::sync::Mutex;

use felix_mqtt_wire::{Ack, SubAck, UnsubAck};
use felix_wire::RecordHeaders;

use super::*;
use crate::read::{ReadError, ReadPage, Reads, StoredMessage};

const WAIT: Duration = Duration::from_secs(2);

/// A stream of two shards held in memory. Mounts send every exact topic to
/// shard 0.
struct FakeReads {
    shards: Mutex<Vec<Vec<StoredMessage>>>,
    appended: Notify,
    /// Shards this broker does not serve, and how each is refused.
    refused: Mutex<HashMap<u32, ReadError>>,
}

impl FakeReads {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            shards: Mutex::new(vec![Vec::new(), Vec::new()]),
            appended: Notify::new(),
            refused: Mutex::default(),
        })
    }

    /// Stop serving `shard`, as a move or a lost leader does, and wake the
    /// readers as ending its subscribers does.
    fn refuse(&self, shard: u32, err: ReadError) {
        self.refused.lock().unwrap().insert(shard, err);
        self.appended.notify_waiters();
    }

    fn served(&self, shard: u32) -> Result<(), ReadError> {
        self.refused
            .lock()
            .unwrap()
            .get(&shard)
            .map_or(Ok(()), |err| Err(*err))
    }

    fn append(&self, shard: usize, topic: &str, qos: &str, payload: &'static [u8]) {
        self.append_with(
            shard,
            topic,
            vec![(
                "felix.mqtt.qos".into(),
                Bytes::copy_from_slice(qos.as_bytes()),
            )],
            payload,
        );
    }

    fn append_with(
        &self,
        shard: usize,
        topic: &str,
        headers: Vec<(String, Bytes)>,
        payload: &'static [u8],
    ) {
        let block = RecordHeaders {
            key: Some(Bytes::copy_from_slice(topic.as_bytes())),
            headers,
        }
        .encode()
        .unwrap();
        let mut shards = self.shards.lock().unwrap();
        let offset = shards[shard].len() as u64;
        shards[shard].push(StoredMessage {
            offset,
            payload: Bytes::from_static(payload),
            headers: Some(block),
        });
        drop(shards);
        self.appended.notify_waiters();
    }
}

#[async_trait]
impl Reads for FakeReads {
    fn shard_count(&self, _: &str, _: &str, _: &str) -> u32 {
        2
    }

    async fn tail(&self, _: &str, _: &str, _: &str, shard: u32) -> Result<u64, ReadError> {
        self.served(shard)?;
        Ok(self.shards.lock().unwrap()[shard as usize].len() as u64)
    }

    async fn read(
        &self,
        _: &str,
        _: &str,
        _: &str,
        shard: u32,
        from: u64,
        _: usize,
        wait: Duration,
    ) -> Result<ReadPage, ReadError> {
        loop {
            let notified = self.appended.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.served(shard)?;
            let messages: Vec<StoredMessage> = self.shards.lock().unwrap()[shard as usize]
                .iter()
                .filter(|m| m.offset >= from)
                .take(16)
                .cloned()
                .collect();
            if let Some(last) = messages.last() {
                let next = last.offset + 1;
                return Ok(ReadPage { messages, next });
            }
            if tokio::time::timeout(wait, notified).await.is_err() {
                return Ok(ReadPage {
                    messages: Vec::new(),
                    next: from,
                });
            }
        }
    }
}

async fn subscriber(
    version: Version,
    token: &'static str,
    reads: &Arc<FakeReads>,
    settings: Settings,
    receive_maximum: Option<u16>,
) -> Harness {
    let mut h = start_full(
        settings,
        None,
        FakeWrites::new(),
        Some(Arc::clone(reads) as Arc<dyn Reads>),
    );
    h.client.version = Some(version);
    let mut c = connect(version);
    c.password = Some(Bytes::from_static(token.as_bytes()));
    c.properties.receive_maximum = receive_maximum;
    h.client.send(Packet::Connect(c), version).await;
    assert_eq!(connack(h.client.recv().await).reason, ReasonCode::Success);
    h
}

fn sub(packet_id: u16, filters: &[(&str, QoS)]) -> Packet {
    Packet::Subscribe(Subscribe {
        packet_id,
        filters: filters
            .iter()
            .map(|(f, q)| SubscribeFilter::new(Utf8::new(f.to_string()).unwrap(), *q))
            .collect(),
        properties: Properties::default(),
    })
}

async fn subscribe_ok(h: &mut Harness, version: Version, filter: &str, qos: QoS) {
    h.client.send(sub(1, &[(filter, qos)]), version).await;
    let ack = suback(h.client.recv().await);
    assert!(
        matches!(
            ack.reasons[0],
            ReasonCode::Success | ReasonCode::GrantedQos1
        ),
        "{ack:?}"
    );
}

fn suback(packet: Option<Packet>) -> SubAck {
    match packet {
        Some(Packet::SubAck(ack)) => ack,
        other => panic!("expected SUBACK, got {other:?}"),
    }
}

async fn next_publish(h: &mut Harness) -> Publish {
    match tokio::time::timeout(WAIT, h.client.recv()).await {
        Ok(Some(Packet::Publish(publish))) => publish,
        other => panic!("expected PUBLISH, got {other:?}"),
    }
}

async fn nothing(h: &mut Harness) {
    assert!(tokio::time::timeout(WAIT, h.client.recv()).await.is_err());
}

async fn ack(h: &mut Harness, version: Version, id: u16) {
    h.client.send(Packet::PubAck(Ack::new(id)), version).await;
}

#[tokio::test(start_paused = true)]
async fn a_subscriber_gets_new_messages_at_the_lower_qos() {
    for version in [Version::V311, Version::V5] {
        let reads = FakeReads::new();
        let mut h = subscriber(version, "good", &reads, Settings::default(), None).await;
        // Written before the subscription: a clean session starts at the tail.
        reads.append(0, "plant/old", "1", b"old");
        h.client
            .send(
                sub(
                    7,
                    &[
                        ("plant/+", QoS::AtLeastOnce),
                        ("plant/q0/+", QoS::AtMostOnce),
                    ],
                ),
                version,
            )
            .await;
        let granted = suback(h.client.recv().await);
        assert_eq!(granted.packet_id, 7);
        assert_eq!(
            granted.reasons,
            vec![ReasonCode::GrantedQos1, ReasonCode::Success]
        );

        reads.append(0, "plant/a", "1", b"one");
        let first = next_publish(&mut h).await;
        assert_eq!(first.topic.as_str(), "plant/a");
        assert_eq!(first.payload, Bytes::from_static(b"one"));
        assert_eq!(first.qos, QoS::AtLeastOnce);
        let id = first.packet_id.expect("a QoS 1 delivery has an id");
        ack(&mut h, version, id).await;

        // Published at QoS 0: delivered at QoS 0, with no id.
        reads.append(0, "plant/b", "0", b"two");
        let second = next_publish(&mut h).await;
        assert_eq!((second.qos, second.packet_id), (QoS::AtMostOnce, None));
        // The old message and a topic no filter matches never arrive.
        reads.append(0, "plant/x/y/z", "1", b"unmatched");
        nothing(&mut h).await;
    }
}

#[tokio::test(start_paused = true)]
async fn the_granted_qos_caps_the_delivery() {
    let reads = FakeReads::new();
    let mut h = subscriber(Version::V5, "good", &reads, Settings::default(), None).await;
    subscribe_ok(&mut h, Version::V5, "plant/+", QoS::AtMostOnce).await;
    reads.append(0, "plant/a", "1", b"x");
    assert_eq!(next_publish(&mut h).await.qos, QoS::AtMostOnce);
}

#[tokio::test(start_paused = true)]
async fn a_wildcard_reads_every_shard_and_an_exact_filter_one() {
    let reads = FakeReads::new();
    let mut h = subscriber(Version::V311, "good", &reads, Settings::default(), None).await;
    subscribe_ok(&mut h, Version::V311, "plant/#", QoS::AtMostOnce).await;
    reads.append(1, "plant/a/b", "0", b"shard1");
    assert_eq!(
        next_publish(&mut h).await.payload,
        Bytes::from_static(b"shard1")
    );

    let mut e = subscriber(Version::V311, "good", &reads, Settings::default(), None).await;
    subscribe_ok(&mut e, Version::V311, "plant/a/b", QoS::AtMostOnce).await;
    reads.append(1, "plant/a/b", "0", b"elsewhere");
    nothing(&mut e).await;
    reads.append(0, "plant/a/b", "0", b"here");
    assert_eq!(
        next_publish(&mut e).await.payload,
        Bytes::from_static(b"here")
    );
}

#[tokio::test(start_paused = true)]
async fn filters_that_cannot_be_served_are_refused_one_by_one() {
    let reads = FakeReads::new();
    let mut h = subscriber(Version::V5, "good", &reads, Settings::default(), None).await;
    h.client
        .send(
            sub(
                1,
                &[
                    ("plant/ok", QoS::AtLeastOnce),
                    ("other/x", QoS::AtLeastOnce),
                    ("+/x", QoS::AtLeastOnce),
                    ("$share/g/plant/ok", QoS::AtLeastOnce),
                ],
            ),
            Version::V5,
        )
        .await;
    assert_eq!(
        suback(h.client.recv().await).reasons,
        vec![
            ReasonCode::GrantedQos1,
            ReasonCode::TopicFilterInvalid,
            ReasonCode::TopicFilterInvalid,
            ReasonCode::SharedSubscriptionsNotSupported,
        ]
    );
    // 3.1.1 has one failure code.
    let mut old = subscriber(Version::V311, "good", &reads, Settings::default(), None).await;
    old.client
        .send(sub(1, &[("other/x", QoS::AtMostOnce)]), Version::V311)
        .await;
    assert_eq!(
        suback(old.client.recv().await).reasons,
        vec![ReasonCode::UnspecifiedError]
    );
}

#[tokio::test(start_paused = true)]
async fn a_principal_without_the_grant_cannot_subscribe() {
    let reads = FakeReads::new();
    let mut h = subscriber(Version::V5, "nogrant", &reads, Settings::default(), None).await;
    h.client
        .send(sub(1, &[("plant/a", QoS::AtLeastOnce)]), Version::V5)
        .await;
    assert_eq!(
        suback(h.client.recv().await).reasons,
        vec![ReasonCode::NotAuthorized]
    );
    reads.append(0, "plant/a", "1", b"x");
    nothing(&mut h).await;
}

#[tokio::test(start_paused = true)]
async fn the_subscription_limit_is_per_session() {
    let reads = FakeReads::new();
    let settings = Settings {
        max_subscriptions: 2,
        ..Settings::default()
    };
    let mut h = subscriber(Version::V5, "good", &reads, settings, None).await;
    for (i, (filter, want)) in [
        ("plant/a", ReasonCode::Success),
        ("plant/b", ReasonCode::Success),
        ("plant/c", ReasonCode::QuotaExceeded),
        // Already held: a new QoS replaces the old.
        ("plant/a", ReasonCode::GrantedQos1),
    ]
    .into_iter()
    .enumerate()
    {
        let qos = if want == ReasonCode::GrantedQos1 {
            QoS::AtLeastOnce
        } else {
            QoS::AtMostOnce
        };
        h.client
            .send(sub(i as u16 + 1, &[(filter, qos)]), Version::V5)
            .await;
        assert_eq!(
            suback(h.client.recv().await).reasons,
            vec![want],
            "{filter}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn unsubscribe_stops_delivery() {
    for version in [Version::V311, Version::V5] {
        let reads = FakeReads::new();
        let mut h = subscriber(version, "good", &reads, Settings::default(), None).await;
        subscribe_ok(&mut h, version, "plant/+", QoS::AtMostOnce).await;
        h.client
            .send(
                Packet::Unsubscribe(Unsubscribe {
                    packet_id: 9,
                    filters: vec![
                        Utf8::new("plant/+").unwrap(),
                        Utf8::new("plant/never").unwrap(),
                    ],
                    properties: Properties::default(),
                }),
                version,
            )
            .await;
        let Some(Packet::UnsubAck(UnsubAck {
            packet_id, reasons, ..
        })) = h.client.recv().await
        else {
            panic!("expected UNSUBACK");
        };
        assert_eq!(packet_id, 9);
        let want = if version == Version::V5 {
            vec![ReasonCode::Success, ReasonCode::NoSubscriptionExisted]
        } else {
            vec![]
        };
        assert_eq!(reasons, want);
        reads.append(0, "plant/a", "0", b"x");
        nothing(&mut h).await;
    }
}

#[tokio::test(start_paused = true)]
async fn at_most_receive_maximum_qos1_messages_are_unacknowledged() {
    let reads = FakeReads::new();
    let mut h = subscriber(Version::V5, "good", &reads, Settings::default(), Some(2)).await;
    subscribe_ok(&mut h, Version::V5, "plant/+", QoS::AtLeastOnce).await;
    for n in 0..4u8 {
        reads.append(0, "plant/a", "1", [b"0", b"1", b"2", b"3"][n as usize]);
    }
    let first = next_publish(&mut h).await;
    let second = next_publish(&mut h).await;
    nothing(&mut h).await;
    // An acknowledgement frees one slot, and order is kept.
    ack(&mut h, Version::V5, first.packet_id.unwrap()).await;
    let third = next_publish(&mut h).await;
    nothing(&mut h).await;
    ack(&mut h, Version::V5, second.packet_id.unwrap()).await;
    let fourth = next_publish(&mut h).await;
    let got: Vec<&[u8]> = [&first, &second, &third, &fourth]
        .iter()
        .map(|p| p.payload.as_ref())
        .collect();
    assert_eq!(got, [b"0", b"1", b"2", b"3"]);
    assert_ne!(third.packet_id, second.packet_id);
}

#[tokio::test(start_paused = true)]
async fn a_slow_subscriber_neither_loses_qos1_nor_holds_up_another() {
    let reads = FakeReads::new();
    let settings = Settings {
        session_queue_messages: 2,
        ..Settings::default()
    };
    let mut slow = subscriber(Version::V5, "good", &reads, settings.clone(), Some(1)).await;
    let mut fast = subscriber(Version::V5, "good", &reads, settings, None).await;
    for h in [&mut slow, &mut fast] {
        subscribe_ok(h, Version::V5, "plant/+", QoS::AtLeastOnce).await;
    }
    const N: usize = 50;
    for _ in 0..N {
        reads.append(0, "plant/a", "1", b"m");
    }
    // The fast one gets all of them while the slow one holds its first.
    for _ in 0..N {
        let publish = next_publish(&mut fast).await;
        ack(&mut fast, Version::V5, publish.packet_id.unwrap()).await;
    }
    // The slow one has had one and, once it acknowledges, gets the rest, one
    // at a time, none lost.
    let mut seen = 0;
    loop {
        let publish = next_publish(&mut slow).await;
        seen += 1;
        ack(&mut slow, Version::V5, publish.packet_id.unwrap()).await;
        if seen == N {
            break;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn qos0_is_dropped_when_the_session_queue_is_full() {
    let reads = FakeReads::new();
    let settings = Settings {
        session_queue_messages: 4,
        ..Settings::default()
    };
    let mut h = subscriber(Version::V311, "good", &reads, settings, None).await;
    subscribe_ok(&mut h, Version::V311, "plant/+", QoS::AtMostOnce).await;
    // Far more than the pipe to the client holds, which it does not read.
    const N: usize = 3000;
    for _ in 0..N {
        reads.append_with(
            0,
            "plant/a",
            vec![("felix.mqtt.qos".into(), Bytes::from_static(b"0"))],
            &[7u8; 512],
        );
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut received = 0;
    while let Ok(Some(Packet::Publish(_))) =
        tokio::time::timeout(Duration::from_secs(1), h.client.recv()).await
    {
        received += 1;
    }
    assert!(received > 0 && received < N, "received {received} of {N}");
}

#[tokio::test(start_paused = true)]
async fn an_expired_message_is_not_delivered_and_a_live_one_says_how_long_is_left() {
    let reads = FakeReads::new();
    let mut h = subscriber(Version::V5, "good", &reads, Settings::default(), None).await;
    subscribe_ok(&mut h, Version::V5, "plant/+", QoS::AtMostOnce).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let text = |v: u64| Bytes::from(v.to_string());
    reads.append_with(
        0,
        "plant/a",
        vec![("felix.mqtt.expiry".into(), text(now - 1000))],
        b"dead",
    );
    reads.append_with(
        0,
        "plant/a",
        vec![
            ("felix.mqtt.expiry".into(), text(now + 60_000)),
            (
                "felix.mqtt.content_type".into(),
                Bytes::from_static(b"text/plain"),
            ),
            ("sensor".into(), Bytes::from_static(b"7")),
        ],
        b"live",
    );
    let publish = next_publish(&mut h).await;
    assert_eq!(publish.payload, Bytes::from_static(b"live"));
    let left = publish.properties.message_expiry_interval.unwrap();
    assert!((59..=61).contains(&left), "{left}");
    assert_eq!(
        publish.properties.content_type.unwrap().as_str(),
        "text/plain"
    );
    assert_eq!(publish.properties.user_properties.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn without_reads_subscribe_is_still_refused() {
    let mut h = connected(Version::V5).await;
    h.client
        .send(sub(1, &[("plant/a", QoS::AtMostOnce)]), Version::V5)
        .await;
    assert_eq!(
        suback(h.client.recv().await).reasons,
        vec![ReasonCode::NotAuthorized]
    );
}

#[tokio::test(start_paused = true)]
async fn a_filter_over_a_shard_not_served_here_is_refused_whole() {
    let reads = FakeReads::new();
    reads.refuse(1, ReadError::NotHere);
    let mut h = subscriber(Version::V5, "good", &reads, Settings::default(), None).await;
    h.client
        .send(
            sub(
                1,
                &[("plant/#", QoS::AtLeastOnce), ("plant/a", QoS::AtLeastOnce)],
            ),
            Version::V5,
        )
        .await;
    assert_eq!(
        suback(h.client.recv().await).reasons,
        vec![
            ReasonCode::ImplementationSpecificError,
            ReasonCode::GrantedQos1
        ]
    );
    // The refused wildcard is not served from the shard that is held.
    reads.append(0, "plant/b", "1", b"wildcard only");
    nothing(&mut h).await;
    reads.append(0, "plant/a", "1", b"exact");
    assert_eq!(
        next_publish(&mut h).await.payload,
        Bytes::from_static(b"exact")
    );

    let mut old = subscriber(Version::V311, "good", &reads, Settings::default(), None).await;
    old.client
        .send(sub(1, &[("plant/#", QoS::AtMostOnce)]), Version::V311)
        .await;
    assert_eq!(
        suback(old.client.recv().await).reasons,
        vec![ReasonCode::UnspecifiedError]
    );
}

#[tokio::test(start_paused = true)]
async fn a_reader_whose_shard_stops_being_served_ends_the_connection() {
    for (err, reason) in [
        (ReadError::NotHere, ReasonCode::UseAnotherServer),
        (ReadError::Stopped, ReasonCode::ServerBusy),
    ] {
        let reads = FakeReads::new();
        let mut h = subscriber(Version::V5, "good", &reads, Settings::default(), None).await;
        subscribe_ok(&mut h, Version::V5, "plant/a", QoS::AtLeastOnce).await;
        reads.refuse(0, err);
        let packet = tokio::time::timeout(WAIT, h.client.recv()).await.unwrap();
        let Some(Packet::Disconnect(disconnect)) = packet else {
            panic!("expected a DISCONNECT, got {packet:?}");
        };
        assert_eq!(disconnect.reason, reason);
    }
}

#[tokio::test(start_paused = true)]
async fn a_new_filter_on_a_lagging_reader_starts_at_its_suback() {
    let reads = FakeReads::new();
    let settings = Settings {
        session_queue_messages: 1,
        ..Settings::default()
    };
    let mut h = subscriber(Version::V5, "good", &reads, settings, Some(1)).await;
    subscribe_ok(&mut h, Version::V5, "plant/a", QoS::AtLeastOnce).await;
    // The reader of shard 0 falls behind: one in flight, one queued, and the
    // rest still in the log, with a `plant/b` record among them.
    for _ in 0..4 {
        reads.append(0, "plant/a", "1", b"a");
    }
    reads.append(0, "plant/b", "1", b"before");
    let first = next_publish(&mut h).await;
    subscribe_ok(&mut h, Version::V5, "plant/b", QoS::AtLeastOnce).await;
    reads.append(0, "plant/b", "1", b"after");
    ack(&mut h, Version::V5, first.packet_id.unwrap()).await;
    let mut b = Vec::new();
    for _ in 0..4 {
        let publish = next_publish(&mut h).await;
        if publish.topic.as_str() == "plant/b" {
            b.push(publish.payload.clone());
        }
        ack(&mut h, Version::V5, publish.packet_id.unwrap()).await;
    }
    assert_eq!(b, vec![Bytes::from_static(b"after")]);
    nothing(&mut h).await;
}
