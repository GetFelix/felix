//! SUBSCRIBE, UNSUBSCRIBE and delivery to the client.
//!
//! A clean session lives with its connection. Each stream shard a session's
//! filters can match gets one reader task, which starts at the shard's
//! committed tail when the filter is accepted and reads by offset from there.
//! The reader matches each record against the session's filters and offers
//! the matches to the connection through a bounded queue. The queue is the
//! only place a session can fall behind: a QoS 0 message that finds it full is
//! dropped, and a QoS 1 message makes the reader wait, which leaves the rest
//! of the backlog in the log. A slow device therefore stalls only its own
//! readers.
//!
//! A filter starts at each shard's committed tail when it is accepted, and a
//! reader that was already running for another filter, and is behind that
//! tail, delivers nothing below it to the new filter. A shard this broker does
//! not serve is never read: the SUBSCRIBE is refused, and a reader whose shard
//! moves or loses its leader ends the connection. `docs/mqtt-design.md`,
//! "Subscribing".

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use felix_mqtt_wire::{
    Ack, Packet, Properties, Publish, QoS, ReasonCode, SubAck, Subscribe, UnsubAck, Unsubscribe,
    Utf8, Version,
};
use felix_wire::{RESERVED_HEADER_PREFIX, RecordHeaders};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::connect::Session;
use super::connection::{Connection, End};
use super::{ReaderSlot, Shared};
use crate::filter::{FilterTrie, literal_prefix};
use crate::metrics;
use crate::read::{ReadError, Reads};

/// Bytes asked for in one read of a shard.
const READ_BYTES: usize = 256 * 1024;
/// How long a read waits for a record before the reader looks again. An
/// append or the shard's readers being ended wakes it sooner; a commit mark
/// that moves does not, and is seen at the next look.
const READ_WAIT: Duration = Duration::from_millis(500);
/// Pause before reading again after the broker could not answer.
const RETRY_PAUSE: Duration = Duration::from_millis(200);
/// Most messages taken off the queue per wakeup of the connection.
const DELIVER_BATCH: usize = 64;

type StreamShard = (Arc<str>, Arc<str>, u32);

/// A message that matched a filter, ready to send.
pub(super) struct Outgoing {
    publish: Publish,
}

pub(super) enum Fetched {
    Message(Box<Outgoing>),
    /// The stream is gone; the subscription cannot continue.
    Gone,
    /// This broker stopped serving a shard the session reads.
    Ended(ReadError),
}

/// A reader task, stopped when it is dropped.
struct Reader {
    task: JoinHandle<()>,
    /// Held for its drop, which gives the slot back.
    _slot: ReaderSlot,
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// What the readers share with the connection: its filters, and where each
/// filter starts on each shard.
type Filters = Arc<RwLock<FilterSet>>;

#[derive(Default)]
struct FilterSet {
    trie: FilterTrie,
    /// By filter id: the first offset of each shard the filter is given.
    starts: HashMap<u64, HashMap<StreamShard, u64>>,
}

impl FilterSet {
    /// The QoS `topic` is delivered at from `key` at `offset`, if any filter
    /// wants it there.
    fn matches(&self, topic: &str, key: &StreamShard, offset: u64) -> Option<u8> {
        self.trie.matches_where(topic, |id| {
            self.starts
                .get(&id)
                .and_then(|starts| starts.get(key))
                .is_some_and(|start| offset >= *start)
        })
    }
}

/// A connection's subscriptions and the deliveries in flight to its client.
pub(super) struct Subscriptions {
    filters: Filters,
    /// For each filter, the shards it reads.
    shards_of: HashMap<String, Vec<StreamShard>>,
    readers: HashMap<StreamShard, Reader>,
    tx: mpsc::Sender<Fetched>,
    rx: mpsc::Receiver<Fetched>,
    /// Packet ids of QoS 1 messages the client has not acknowledged.
    in_flight: HashSet<u16>,
    next_id: u16,
    max_in_flight: usize,
    client_max_packet: Option<u32>,
    /// A QoS 1 message taken off the queue while the client was at its
    /// receive maximum. Nothing is taken until it has gone.
    stalled: Option<Outgoing>,
}

impl Subscriptions {
    pub(super) fn new(shared: &Shared, session: &Session) -> Self {
        let (tx, rx) = mpsc::channel(shared.settings.session_queue_messages.max(1));
        Self {
            filters: Arc::default(),
            shards_of: HashMap::new(),
            readers: HashMap::new(),
            tx,
            rx,
            in_flight: HashSet::new(),
            next_id: 0,
            max_in_flight: usize::from(session.receive_maximum),
            client_max_packet: session.client_max_packet,
            stalled: None,
        }
    }

    /// Whether the connection should wait for a delivery.
    pub(super) fn can_take(&self) -> bool {
        self.stalled.is_none() && !self.readers.is_empty()
    }

    /// The next message from the readers; never resolves without readers.
    pub(super) async fn recv(&mut self) -> Fetched {
        match self.rx.recv().await {
            Some(fetched) => fetched,
            // `self.tx` is alive, so the queue never closes.
            None => std::future::pending().await,
        }
    }

    fn free_id(&mut self) -> u16 {
        loop {
            self.next_id = self.next_id.checked_add(1).unwrap_or(1);
            if !self.in_flight.contains(&self.next_id) {
                return self.next_id;
            }
        }
    }

    /// Whether sending `publish` would pass the client's Maximum Packet Size.
    fn too_large_for_client(&self, publish: &Publish, version: Version) -> bool {
        let Some(max) = self.client_max_packet else {
            return false;
        };
        // A QoS 1 publish carries a packet id the message does not have yet.
        let mut sized = publish.clone();
        if sized.qos != QoS::AtMostOnce {
            sized.packet_id = Some(1);
        }
        match Packet::Publish(sized).encoded_len(version) {
            Ok(len) => len as u64 > u64::from(max),
            Err(_) => true,
        }
    }

    fn at_limit(&self) -> bool {
        self.in_flight.len() >= self.max_in_flight
    }

    /// Stop the readers no filter needs any more.
    fn prune_readers(&mut self) {
        let needed: HashSet<&StreamShard> = self.shards_of.values().flatten().collect();
        self.readers.retain(|key, _| needed.contains(key));
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> Connection<S> {
    /// Send what the readers have for the client, up to a batch.
    pub(super) fn deliver(&mut self, subs: &mut Subscriptions, first: Fetched) -> Result<(), End> {
        let mut taken = 0;
        let mut fetched = first;
        loop {
            match fetched {
                Fetched::Gone => return Err(End::Close("subscribe_stream_gone")),
                // A 3.1.1 client is only closed; it reconnects, and the
                // SUBSCRIBE is refused until the shard is served here again.
                Fetched::Ended(ReadError::NotHere) => {
                    return Err(End::Disconnect(
                        ReasonCode::UseAnotherServer,
                        Some("subscribe_moved"),
                    ));
                }
                Fetched::Ended(_) => {
                    return Err(End::Disconnect(
                        ReasonCode::ServerBusy,
                        Some("subscribe_stopped"),
                    ));
                }
                Fetched::Message(boxed) => {
                    let outgoing = *boxed;
                    // MQTT-3.1.2-24: never send what the client said it
                    // cannot take. -25: discard it as if it were delivered,
                    // so a QoS 1 message needs no acknowledgement.
                    if subs.too_large_for_client(
                        &outgoing.publish,
                        self.version.unwrap_or(Version::V311),
                    ) {
                        metrics::dropped("client_max_packet_size");
                    } else if outgoing.publish.qos != QoS::AtMostOnce && subs.at_limit() {
                        subs.stalled = Some(outgoing);
                        return Ok(());
                    } else {
                        self.send_outgoing(subs, outgoing)?;
                    }
                }
            }
            taken += 1;
            if taken == DELIVER_BATCH {
                return Ok(());
            }
            match subs.rx.try_recv() {
                Ok(next) => fetched = next,
                Err(_) => return Ok(()),
            }
        }
    }

    fn send_outgoing(&mut self, subs: &mut Subscriptions, outgoing: Outgoing) -> Result<(), End> {
        let mut publish = outgoing.publish;
        if publish.qos == QoS::AtLeastOnce {
            let id = subs.free_id();
            subs.in_flight.insert(id);
            publish.packet_id = Some(id);
            metrics::delivered("1", 1);
        } else {
            metrics::delivered("0", 1);
        }
        self.queue(&Packet::Publish(publish))
    }

    /// A `PUBACK` for a message sent to the client. One for an id that is not
    /// in flight is counted and ignored.
    pub(super) fn handle_puback(&mut self, subs: &mut Subscriptions, ack: Ack) -> Result<(), End> {
        if !subs.in_flight.remove(&ack.packet_id) {
            metrics::refused("puback_unknown");
            return Ok(());
        }
        if let Some(stalled) = subs.stalled.take() {
            self.send_outgoing(subs, stalled)?;
        }
        Ok(())
    }

    pub(super) async fn handle_subscribe(
        &mut self,
        shared: &Shared,
        session: &Session,
        subs: &mut Subscriptions,
        subscribe: Subscribe,
    ) -> Result<(), End> {
        let version = session.version;
        let failure = |reason: ReasonCode| match version {
            Version::V5 => reason,
            Version::V311 => ReasonCode::UnspecifiedError,
        };
        let mut reasons = Vec::with_capacity(subscribe.filters.len());
        let identified = !subscribe.properties.subscription_identifiers.is_empty();
        for requested in &subscribe.filters {
            let filter = requested.filter.as_str();
            let reason = if identified {
                refuse(
                    "subscription_identifier",
                    ReasonCode::SubscriptionIdentifiersNotSupported,
                )
            } else if (version == Version::V5) && filter.starts_with("$share/") {
                refuse(
                    "shared_subscription",
                    ReasonCode::SharedSubscriptionsNotSupported,
                )
            } else if requested.no_local {
                refuse("no_local", ReasonCode::ImplementationSpecificError)
            } else {
                let granted = match requested.qos {
                    QoS::AtMostOnce => 0,
                    _ => 1,
                };
                match self
                    .add_filter(shared, session, subs, filter, granted)
                    .await
                {
                    Ok(()) if granted == 0 => ReasonCode::Success,
                    Ok(()) => ReasonCode::GrantedQos1,
                    Err((label, reason)) => refuse(label, reason),
                }
            };
            reasons.push(if (reason as u8) >= 0x80 {
                failure(reason)
            } else {
                reason
            });
        }
        self.queue(&Packet::SubAck(SubAck {
            packet_id: subscribe.packet_id,
            reasons,
            properties: Properties::default(),
        }))
    }

    /// Accept one filter: resolve its mount, check the grant, and make sure a
    /// reader runs on every shard it can match.
    async fn add_filter(
        &mut self,
        shared: &Shared,
        session: &Session,
        subs: &mut Subscriptions,
        filter: &str,
        granted: u8,
    ) -> Result<(), (&'static str, ReasonCode)> {
        let tenant = session.principal.tenant_id();
        let wildcard = literal_prefix(filter);
        if !subs.filters.read().unwrap().trie.contains(filter)
            && subs.shards_of.len() >= shared.settings.max_subscriptions
        {
            return Err(("subscription_limit", ReasonCode::QuotaExceeded));
        }
        // A filter that spans mounts resolves to none, since its literal
        // prefix is shorter than every mount that could match it.
        let Some(target) = shared.mounts.resolve(tenant, wildcard.unwrap_or(filter)) else {
            return Err(("filter_unmounted", ReasonCode::TopicFilterInvalid));
        };
        if !session
            .principal
            .may_subscribe(&target.namespace, &target.stream)
        {
            return Err(("not_authorized", ReasonCode::NotAuthorized));
        }
        let shards: Vec<u32> = if wildcard.is_some() {
            (0..shared
                .reads
                .shard_count(tenant, &target.namespace, &target.stream))
                .collect()
        } else {
            vec![target.shard]
        };
        let keys: Vec<StreamShard> = shards
            .iter()
            .map(|&shard| (target.namespace.clone(), target.stream.clone(), shard))
            .collect();

        // Every shard's tail is taken before the filter goes in, so a refused
        // filter changes nothing, and one a broker serves only in part is
        // refused whole. A record committed after the SUBACK is past these
        // tails and is matched by whichever reader reads it.
        let mut starts = HashMap::with_capacity(keys.len());
        for key in &keys {
            let tail = shared
                .reads
                .tail(tenant, &key.0, &key.1, key.2)
                .await
                .map_err(|err| ("subscribe_unavailable", read_failure(err)))?;
            starts.insert(key.clone(), tail);
        }
        let new_keys: Vec<&StreamShard> = keys
            .iter()
            .filter(|key| !subs.readers.contains_key(*key))
            .collect();
        let mut slots = shared
            .readers
            .acquire(new_keys.len(), shared.settings.max_readers)
            .ok_or(("reader_limit", ReasonCode::QuotaExceeded))?;
        {
            let mut filters = subs.filters.write().unwrap();
            let id = filters.trie.insert(filter, granted);
            // A replaced filter keeps reading from where it was.
            filters.starts.entry(id).or_insert_with(|| starts.clone());
        }
        for key in new_keys {
            let reader = tokio::spawn(read_loop(
                shared.reads.clone(),
                tenant.into(),
                key.clone(),
                starts[key],
                subs.filters.clone(),
                subs.tx.clone(),
                session.version,
            ));
            subs.readers.insert(
                key.clone(),
                Reader {
                    task: reader,
                    _slot: slots.pop().expect("one slot per new reader"),
                },
            );
        }
        subs.shards_of.insert(filter.to_owned(), keys);
        subs.prune_readers();
        Ok(())
    }

    pub(super) fn handle_unsubscribe(
        &mut self,
        subs: &mut Subscriptions,
        version: Version,
        unsubscribe: &Unsubscribe,
    ) -> Result<(), End> {
        let mut reasons = Vec::with_capacity(unsubscribe.filters.len());
        for filter in &unsubscribe.filters {
            let removed = {
                let mut filters = subs.filters.write().unwrap();
                let id = filters.trie.id_of(filter.as_str());
                let removed = filters.trie.remove(filter.as_str());
                if let Some(id) = id {
                    filters.starts.remove(&id);
                }
                removed
            };
            subs.shards_of.remove(filter.as_str());
            reasons.push(if removed {
                ReasonCode::Success
            } else {
                ReasonCode::NoSubscriptionExisted
            });
        }
        subs.prune_readers();
        if version == Version::V311 {
            reasons.clear();
        }
        self.queue(&Packet::UnsubAck(UnsubAck {
            packet_id: unsubscribe.packet_id,
            reasons,
            properties: Properties::default(),
        }))
    }
}

fn refuse(label: &'static str, reason: ReasonCode) -> ReasonCode {
    metrics::refused(label);
    reason
}

/// A SUBACK failure code. Each is one a 5.0 SUBACK may carry; a 3.1.1
/// client gets 0x80 for all of them.
fn read_failure(err: ReadError) -> ReasonCode {
    match err {
        // This broker does not hold a shard the filter covers. Not a fault
        // of the filter, and not a quota: a broker-specific limit until
        // cross-broker reads (#1693).
        ReadError::NotHere => ReasonCode::ImplementationSpecificError,
        ReadError::Unavailable | ReadError::Stopped => ReasonCode::UnspecifiedError,
        ReadError::TooOld { .. } | ReadError::Gone => ReasonCode::TopicFilterInvalid,
    }
}

/// Read one shard from `from` for the life of the session.
async fn read_loop(
    reads: Arc<dyn Reads>,
    tenant: Arc<str>,
    key: StreamShard,
    mut from: u64,
    filters: Filters,
    tx: mpsc::Sender<Fetched>,
    version: Version,
) {
    loop {
        let page = reads
            .read(&tenant, &key.0, &key.1, key.2, from, READ_BYTES, READ_WAIT)
            .await;
        match page {
            Ok(page) => {
                from = page.next;
                for message in page.messages {
                    let Some(outgoing) = match_message(&filters, &key, &message, version) else {
                        continue;
                    };
                    let qos0 = outgoing.publish.qos == QoS::AtMostOnce;
                    let fetched = Fetched::Message(Box::new(outgoing));
                    if qos0 {
                        // A full queue drops a QoS 0 message instead of
                        // waiting on a client that is not keeping up.
                        match tx.try_send(fetched) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                metrics::dropped("queue_full")
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                    } else if tx.send(fetched).await.is_err() {
                        return;
                    }
                }
            }
            Err(ReadError::Unavailable) => tokio::time::sleep(RETRY_PAUSE).await,
            // Retention passed the cursor: resume at what is left.
            Err(ReadError::TooOld { oldest }) => {
                metrics::dropped("retention");
                from = oldest;
            }
            Err(ReadError::Gone) => {
                let _ = tx.send(Fetched::Gone).await;
                return;
            }
            Err(err @ (ReadError::NotHere | ReadError::Stopped)) => {
                let _ = tx.send(Fetched::Ended(err)).await;
                return;
            }
        }
    }
}

/// The message as the client should get it, or `None` when no filter wants
/// it, it is not an MQTT message, or it has expired.
fn match_message(
    filters: &Filters,
    key: &StreamShard,
    message: &crate::read::StoredMessage,
    version: Version,
) -> Option<Outgoing> {
    let block = RecordHeaders::decode(message.headers.clone()?).ok()?;
    let topic = std::str::from_utf8(block.key.as_ref()?).ok()?;
    let granted = filters
        .read()
        .unwrap()
        .matches(topic, key, message.offset)?;

    let mut properties = Properties::default();
    let mut published = QoS::AtMostOnce;
    for (name, value) in &block.headers {
        let text = || std::str::from_utf8(value).ok();
        match name.as_str() {
            "felix.mqtt.qos" => {
                if value.as_ref() != b"0" {
                    published = QoS::AtLeastOnce;
                }
            }
            "felix.mqtt.payload_format" => {
                properties.payload_format_indicator = Some(value.as_ref() == b"1")
            }
            "felix.mqtt.content_type" => properties.content_type = text().and_then(utf8),
            "felix.mqtt.response_topic" => properties.response_topic = text().and_then(utf8),
            "felix.mqtt.correlation_data" => properties.correlation_data = Some(value.clone()),
            "felix.mqtt.expiry" => {
                let expires: u64 = text()?.parse().ok()?;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |since| since.as_millis() as u64);
                if expires <= now {
                    metrics::dropped("expired");
                    return None;
                }
                // Rounded up so a message never reads as expired in flight.
                properties.message_expiry_interval = Some((expires - now).div_ceil(1000) as u32);
            }
            other if other.starts_with(RESERVED_HEADER_PREFIX) => {}
            other => {
                if let (Some(name), Some(value)) = (utf8(other), text().and_then(utf8)) {
                    properties.user_properties.push((name, value));
                }
            }
        }
    }
    if !(version == Version::V5) {
        properties = Properties::default();
    }
    let qos = if granted == 0 {
        QoS::AtMostOnce
    } else {
        published
    };
    Some(Outgoing {
        publish: Publish {
            dup: false,
            qos,
            retain: false,
            topic: utf8(topic)?,
            packet_id: None,
            properties,
            payload: message.payload.clone(),
        },
    })
}

fn utf8(text: &str) -> Option<Utf8> {
    Utf8::new(Bytes::copy_from_slice(text.as_bytes())).ok()
}
