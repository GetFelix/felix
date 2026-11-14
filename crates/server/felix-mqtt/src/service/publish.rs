//! PUBLISH: from a packet to a record, a batch, and an acknowledgement.
//!
//! A `PUBACK` is queued only once the write it answers has returned from
//! [`Writes::write`], which is when the stream's consistency level holds, and
//! `PUBACK`s leave in the order the PUBLISHes arrived (MQTT 4.6). Both come
//! from keeping one ordered list of owed answers: a refusal that needs no
//! write waits in it behind earlier publishes instead of overtaking them.
//!
//! The connection has one batch at a time at the broker. Publishes that
//! arrive meanwhile queue, and when the batch returns the leading run of
//! them for one shard goes out as the next one: group commit with no timer.
//! One batch at a time also keeps the producer's sequences in order without
//! any coordination.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use felix_mqtt_wire::{Ack, Packet, Publish, QoS, ReasonCode, Utf8, Version};
use felix_wire::{RESERVED_HEADER_PREFIX, RecordHeaders};
use tokio::io::{AsyncRead, AsyncWrite};

use super::Shared;
use super::connect::Session;
use super::connection::{Connection, End};
use crate::auth::Credential;
use crate::metrics;
use crate::mount::MountedTopic;
use crate::topic::check_topic_name;
use crate::write::{Record, Refusal, WriteBatch, WriteError, Writes};

/// Longest the connection stops reading to keep a tenant within its publish
/// quota.
pub(super) const MAX_QUOTA_HOLD: Duration = Duration::from_secs(5);

type ShardKey = (Arc<str>, Arc<str>, u32);

/// A connection's publishes between the socket and the broker.
pub(super) struct Publishing<'a> {
    tenant: Arc<str>,
    credential: Option<Credential>,
    producer_id: u64,
    max_inflight: usize,
    reason_strings: bool,
    /// Batches written so far to each shard, which is the next batch's
    /// sequence.
    sequences: HashMap<ShardKey, u64>,
    /// `PUBACK`s owed, in arrival order.
    owed: VecDeque<Owed>,
    /// The position of `owed[0]` among every answer ever owed.
    owed_base: u64,
    queue: VecDeque<Queued>,
    in_flight: Option<InFlight<'a>>,
}

struct Owed {
    packet_id: u16,
    answer: Option<Answer>,
}

struct Answer {
    reason: ReasonCode,
    text: Option<&'static str>,
}

struct Queued {
    target: MountedTopic,
    record: Record,
    /// Position in the owed list; `None` for QoS 0.
    owed: Option<u64>,
}

struct InFlight<'a> {
    future: Pin<Box<dyn Future<Output = Result<(), WriteError>> + Send + 'a>>,
    key: ShardKey,
    /// The owed position of each record, `None` for QoS 0.
    owed: Vec<Option<u64>>,
}

impl<'a> Publishing<'a> {
    pub(super) fn new(shared: &Shared, session: &Session) -> Self {
        Self {
            tenant: session.principal.tenant_id().into(),
            credential: session.principal.credential().cloned(),
            producer_id: session.producer_id,
            max_inflight: usize::from(shared.settings.max_inflight),
            reason_strings: session.reason_strings,
            sequences: HashMap::new(),
            owed: VecDeque::new(),
            owed_base: 0,
            queue: VecDeque::new(),
            in_flight: None,
        }
    }

    /// Publishes held for the broker, of any QoS.
    pub(super) fn pending(&self) -> usize {
        self.queue.len() + self.in_flight.as_ref().map_or(0, |batch| batch.owed.len())
    }

    /// Whether the connection has a write at the broker.
    pub(super) fn is_writing(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Resolves when the batch at the broker returns; never, if there is none.
    pub(super) async fn completed(&mut self) -> Result<(), WriteError> {
        match &mut self.in_flight {
            Some(batch) => batch.future.as_mut().await,
            None => std::future::pending().await,
        }
    }

    /// Send the next batch to the broker, if none is out and publishes wait.
    /// Returns how long the connection must stop reading to stay within the
    /// tenant's quota.
    pub(super) fn start(&mut self, writes: &'a Arc<dyn Writes>) -> Option<Duration> {
        if self.in_flight.is_some() {
            return None;
        }
        let first = self.queue.front()?;
        let key = shard_key(&first.target);
        let target = first.target.clone();
        let mut records = Vec::new();
        let mut owed = Vec::new();
        let mut bytes = 0u64;
        while self
            .queue
            .front()
            .is_some_and(|next| shard_key(&next.target) == key)
        {
            let next = self.queue.pop_front().expect("front was just seen");
            bytes += next.record.payload.len() as u64;
            records.push(next.record);
            owed.push(next.owed);
        }
        let hold = writes.admit_produce(&self.tenant, records.len() as u64, bytes);
        let batch = WriteBatch {
            tenant_id: Arc::clone(&self.tenant),
            credential: self.credential.clone(),
            target,
            producer_id: self.producer_id,
            sequence: self.sequences.get(&key).copied().unwrap_or(0),
            acknowledged: owed.iter().any(Option::is_some),
            records,
        };
        self.in_flight = Some(InFlight {
            future: writes.write(batch),
            key,
            owed,
        });
        Some(hold)
    }

    /// The `PUBACK`s that can go now: every answered one at the front of the
    /// owed list.
    pub(super) fn ready(&mut self) -> Vec<Ack> {
        let mut acks = Vec::new();
        while self.owed.front().is_some_and(|owed| owed.answer.is_some()) {
            let owed = self.owed.pop_front().expect("front was just seen");
            self.owed_base += 1;
            let answer = owed.answer.expect("front was just seen answered");
            let mut ack = Ack::new(owed.packet_id);
            ack.reason = answer.reason;
            if let (true, Some(text)) = (self.reason_strings, answer.text) {
                ack.properties.reason_string = Utf8::try_from(text).ok();
            }
            acks.push(ack);
        }
        acks
    }

    /// Owe an answer to `packet_id`, to be filled in when the write returns.
    fn owe(&mut self, packet_id: u16) -> u64 {
        self.owed.push_back(Owed {
            packet_id,
            answer: None,
        });
        self.owed_base + self.owed.len() as u64 - 1
    }

    fn answer(&mut self, position: u64, reason: ReasonCode, text: Option<&'static str>) {
        let index = (position - self.owed_base) as usize;
        self.owed[index].answer = Some(Answer { reason, text });
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Queue the `PUBACK`s that are due.
    pub(super) fn release(&mut self, publishing: &mut Publishing<'_>) -> Result<(), End> {
        for ack in publishing.ready() {
            self.queue(&Packet::PubAck(ack))?;
        }
        Ok(())
    }

    /// Take in one PUBLISH: refuse it, or queue it for the broker. Nothing is
    /// answered here except a refusal, and that waits its turn.
    pub(super) fn handle_publish(
        &mut self,
        shared: &Shared,
        session: &Session,
        publishing: &mut Publishing<'_>,
        publish: Publish,
    ) -> Result<(), End> {
        let version = session.version;
        let topic = publish.topic.as_str();
        if check_topic_name(topic).is_err() {
            return Err(End::Disconnect(
                ReasonCode::TopicNameInvalid,
                Some("invalid_topic_name"),
            ));
        }
        // `Maximum QoS` is 1, and retained messages are a later slice. Both
        // are the client's protocol error; 3.1.1 can only be closed.
        if publish.qos == QoS::ExactlyOnce {
            return Err(End::Disconnect(
                ReasonCode::QosNotSupported,
                Some("qos2_unsupported"),
            ));
        }
        if publish.retain {
            return Err(End::Disconnect(
                ReasonCode::RetainNotSupported,
                Some("retain_unsupported"),
            ));
        }
        // No topic alias is offered, so any is out of range.
        if publish.properties.topic_alias.is_some() {
            return Err(End::Disconnect(
                ReasonCode::TopicAliasInvalid,
                Some("topic_alias_invalid"),
            ));
        }
        let packet_id = match (publish.qos, publish.packet_id) {
            (QoS::AtMostOnce, _) => None,
            (_, Some(id)) => Some(id),
            (_, None) => {
                return Err(End::Disconnect(
                    ReasonCode::ProtocolError,
                    Some("protocol_error"),
                ));
            }
        };
        // 3.1.1 was never told the limit, so it is held to by not reading
        // rather than by disconnecting.
        if version == Version::V5
            && packet_id.is_some()
            && publishing.owed.len() >= publishing.max_inflight
        {
            return Err(End::Disconnect(
                ReasonCode::ReceiveMaximumExceeded,
                Some("receive_maximum_exceeded"),
            ));
        }

        let Some(target) = shared.mounts.resolve(session.principal.tenant_id(), topic) else {
            return self.refuse(publishing, version, packet_id, TOPIC_UNMOUNTED);
        };
        if !session
            .principal
            .may_publish(&target.namespace, &target.stream)
        {
            return self.refuse(publishing, version, packet_id, NOT_AUTHORIZED);
        }
        let record = match record(&publish) {
            Ok(record) => record,
            Err(RecordError::ReservedHeader) => {
                return self.refuse(publishing, version, packet_id, RESERVED_HEADER);
            }
            Err(RecordError::Encode) => return Err(End::Close("internal_error")),
        };
        let owed = packet_id.map(|id| publishing.owe(id));
        publishing.queue.push_back(Queued {
            target,
            record,
            owed,
        });
        Ok(())
    }

    /// A refusal where nothing was written. A 5.0 client with an
    /// acknowledgement to carry it is told per message, in its turn; one
    /// without (QoS 0) is disconnected with the reason, and 3.1.1 has no way
    /// to say no, so that connection ends.
    fn refuse(
        &mut self,
        publishing: &mut Publishing<'_>,
        version: Version,
        packet_id: Option<u16>,
        refusal: Refused,
    ) -> Result<(), End> {
        let (Version::V5, Some(packet_id)) = (version, packet_id) else {
            return Err(End::Disconnect(refusal.reason, Some(refusal.label)));
        };
        metrics::refused(refusal.label);
        let position = publishing.owe(packet_id);
        publishing.answer(position, refusal.reason, Some(refusal.label));
        Ok(())
    }

    /// Take the result of the batch at the broker: answer its `PUBACK`s, or
    /// end the connection when the client must not be answered.
    pub(super) fn settle(
        &mut self,
        publishing: &mut Publishing<'_>,
        version: Version,
        result: Result<(), WriteError>,
    ) -> Result<(), End> {
        let batch = publishing
            .in_flight
            .take()
            .expect("a result comes from a batch in flight");
        match result {
            Ok(()) => {
                *publishing.sequences.entry(batch.key).or_insert(0) += 1;
                let acknowledged = batch.owed.iter().flatten().count();
                metrics::published("1", acknowledged as u64);
                metrics::published("0", (batch.owed.len() - acknowledged) as u64);
                for position in batch.owed.into_iter().flatten() {
                    publishing.answer(position, ReasonCode::Success, None);
                }
            }
            // Not told either way: an error reason would say "not delivered",
            // which may be false. Closing leaves the resend to the client.
            Err(WriteError::OutcomeUnknown(reason)) => return Err(End::Close(reason)),
            Err(WriteError::Refused(refusal)) => {
                let (reason, label) = describe(refusal);
                for position in batch.owed {
                    match (version, position) {
                        (Version::V5, Some(position)) => {
                            metrics::refused(label);
                            publishing.answer(position, reason, Some(label));
                        }
                        (_, Some(_)) => return Err(End::Close(label)),
                        // Nobody is owed anything; the message is dropped.
                        (_, None) => metrics::refused(label),
                    }
                }
            }
        }
        self.release(publishing)
    }
}

/// What a client is told when a publish is refused.
struct Refused {
    reason: ReasonCode,
    label: &'static str,
}

const TOPIC_UNMOUNTED: Refused = Refused {
    reason: ReasonCode::TopicNameInvalid,
    label: "unmounted",
};
const NOT_AUTHORIZED: Refused = Refused {
    reason: ReasonCode::NotAuthorized,
    label: "not_authorized",
};
const RESERVED_HEADER: Refused = Refused {
    reason: ReasonCode::ImplementationSpecificError,
    label: "reserved_header",
};

fn describe(refusal: Refusal) -> (ReasonCode, &'static str) {
    match refusal {
        Refusal::NotOwner => (ReasonCode::ImplementationSpecificError, "not_owner"),
        Refusal::HeadersDisabled => (
            ReasonCode::ImplementationSpecificError,
            "record_headers_disabled",
        ),
        // 0x88 is not allowed on a PUBACK.
        Refusal::StreamUnavailable => (ReasonCode::UnspecifiedError, "stream_unavailable"),
        Refusal::NotAuthorized => (NOT_AUTHORIZED.reason, NOT_AUTHORIZED.label),
        Refusal::Unsupported => (ReasonCode::ImplementationSpecificError, "raft_stream"),
    }
}

fn shard_key(target: &MountedTopic) -> ShardKey {
    (
        Arc::clone(&target.namespace),
        Arc::clone(&target.stream),
        target.shard,
    )
}

enum RecordError {
    /// A user property named in the `felix.` namespace, which is the
    /// listener's own.
    ReservedHeader,
    Encode,
}

/// The record for a PUBLISH: the topic as the key, the message's fields as
/// `felix.mqtt.*` headers, user properties name for name, and the payload
/// as it came. Names and values are in `docs/mqtt-compatibility.md`.
fn record(publish: &Publish) -> Result<Record, RecordError> {
    let properties = &publish.properties;
    let text = |value: &str| Bytes::copy_from_slice(value.as_bytes());
    let mut headers: Vec<(String, Bytes)> =
        Vec::with_capacity(6 + properties.user_properties.len());
    let qos = match publish.qos {
        QoS::AtMostOnce => "0",
        _ => "1",
    };
    headers.push(("felix.mqtt.qos".into(), text(qos)));
    if let Some(utf8) = properties.payload_format_indicator {
        headers.push((
            "felix.mqtt.payload_format".into(),
            text(if utf8 { "1" } else { "0" }),
        ));
    }
    if let Some(content_type) = &properties.content_type {
        headers.push((
            "felix.mqtt.content_type".into(),
            text(content_type.as_str()),
        ));
    }
    if let Some(topic) = &properties.response_topic {
        headers.push(("felix.mqtt.response_topic".into(), text(topic.as_str())));
    }
    if let Some(data) = &properties.correlation_data {
        headers.push(("felix.mqtt.correlation_data".into(), data.clone()));
    }
    if let Some(secs) = properties.message_expiry_interval {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as u64);
        let expires = now.saturating_add(u64::from(secs) * 1000);
        headers.push(("felix.mqtt.expiry".into(), text(&expires.to_string())));
    }
    for (name, value) in &properties.user_properties {
        if name.as_str().starts_with(RESERVED_HEADER_PREFIX) {
            return Err(RecordError::ReservedHeader);
        }
        headers.push((name.as_str().to_owned(), text(value.as_str())));
    }
    let block = RecordHeaders {
        key: Some(text(publish.topic.as_str())),
        headers,
    }
    .encode()
    .map_err(|_| RecordError::Encode)?;
    Ok(Record {
        payload: publish.payload.clone(),
        headers: block,
    })
}
