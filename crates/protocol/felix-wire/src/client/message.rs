//! V1 protocol message enum and its JSON codec.
//!
//! A field added to a variant must be optional and default to the behaviour
//! that existed before it, so an old peer and a new one still exchange
//! byte-identical frames.

mod base64_serde;
mod fields;

pub use fields::{
    AckMode, BrokerEndpoint, CursorErrorReason, GroupRecord, PublishRefusalReason, ShardKind,
    ShardOwner, StartPosition, StateChange,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::client::error_code::{ErrorCode, ErrorDetail, RetryClass};
use crate::client::frame::Frame;
use crate::error::{Error, Result};

/// V1 wire messages encoded in framed payloads.
///
/// Internally tagged by `type`, so the order of the variants is not part of the
/// wire format; they are grouped by what they are for.
///
/// ```
/// use felix_wire::Message;
///
/// let message = Message::Publish {
///     tenant_id: "t1".to_string(),
///     namespace: "default".to_string(),
///     stream: "updates".to_string(),
///     payload: b"hello".to_vec(),
///     // No routing key: shard 0, exactly as before routing keys existed.
///     key: None,
///     request_id: None,
///     ack: None,
/// };
/// let frame = message.encode().expect("encode");
/// let decoded = Message::decode(frame).expect("decode");
/// assert_eq!(message, decoded);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    // Session: authenticate a stream, and the generic answers.
    /// Authenticate a control stream for a specific tenant.
    Auth {
        tenant_id: String,
        token: String,
        /// Frame-flag bits this client understands, offered so the broker can
        /// reply with its own set.
        ///
        /// Absent means the client predates capability negotiation. Because
        /// serde ignores unknown fields, a broker that predates it also simply
        /// ignores this field and answers with a plain `Ok` — which is exactly
        /// the signal the client needs to fall back. Both directions therefore
        /// degrade without a version check.
        #[serde(skip_serializing_if = "Option::is_none")]
        client_flags: Option<u16>,
        /// Optional protocol features this client understands, as a bitset of
        /// `FEATURE_*` constants.
        ///
        /// The mirror of `AuthOk.server_features`, and needed for the same
        /// reason in the other direction: a broker must not send a client a
        /// message type it cannot decode, because an undecodable frame costs
        /// the connection. Absent means a client that predates features, which
        /// implements none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_features: Option<u32>,
    },
    /// Successful auth, carrying the broker's supported frame-flag bits.
    ///
    /// Only ever sent in response to an `Auth` that offered `client_flags`, so a
    /// client old enough not to understand this variant can never receive it.
    AuthOk {
        server_flags: u16,
        /// Optional protocol features this broker implements, as a bitset of
        /// `FEATURE_*` constants.
        ///
        /// Separate from `server_flags`, which is strictly about how a frame's
        /// *payload* is laid out. A feature bit says a request exists, not that
        /// a frame is shaped differently, and conflating the two would have a
        /// client set a frame flag it never intends to send.
        ///
        /// Absent means a broker that predates features, and the only safe
        /// reading of that silence is that it implements none: an unknown
        /// message type is a fatal protocol error to the broker's control loop,
        /// so a client must never send one speculatively.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        server_features: Option<u32>,
        /// Every port this broker's client-facing listeners are bound to,
        /// when it has more than one.
        ///
        /// A broker binds N UDP sockets to lift the single endpoint-driver
        /// ceiling: one socket is one `quinn` endpoint, and its driver is a
        /// single task reading every datagram for that socket. Spreading a
        /// client's connection pool across the ports spreads it across
        /// drivers, which is the whole point of binding more than one.
        ///
        /// Absent for a broker with a single listener -- which is the default,
        /// so the common case stays byte-identical on the wire. A client that
        /// does not understand this field ignores it and keeps using the one
        /// address it dialled, which is exactly what it does today.
        ///
        /// Ports only: the host is the one the client already connected to.
        /// Sending addresses would let a broker redirect a client elsewhere
        /// during auth, which is a much larger claim than "I also listen here".
        #[serde(default, skip_serializing_if = "Option::is_none")]
        listener_ports: Option<Vec<u16>>,
        /// How many acknowledged publishes may be unanswered at once, when
        /// the broker pipelines them: per stream if the broker advertises
        /// `FEATURE_STREAM_PUBLISH_WINDOW`, else per connection.
        ///
        /// Present only for a client that offered `FEATURE_PUBLISH_PIPELINE`
        /// and only when the broker grants it, so every other client gets the
        /// frame it always got. See that bit for what the broker promises.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        publish_window: Option<u32>,
    },
    /// Generic success response.
    Ok,
    /// A request failed.
    ///
    /// `code`, `retry` and `detail` are sent only to a client that offered
    /// `FEATURE_ERROR_CODES`, and are absent otherwise, so the frame is
    /// byte-identical to the one an older broker sends.
    Error {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<ErrorCode>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<RetryClass>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ErrorDetail>,
    },
    /// The broker does not implement the request it was sent, and is still
    /// serving the stream.
    ///
    /// Only ever sent to a client that offered `FEATURE_UNSUPPORTED`. Any
    /// other client that sends an unknown request has its stream closed, as
    /// before the feature existed.
    Unsupported {
        /// The request's `type`, as it arrived.
        request_type: String,
        /// For an `extension` request, the extension it named.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        extension: Option<String>,
        /// The request's `request_id`, when it carried one the broker could
        /// read, so a client with several requests outstanding knows which
        /// one this answers.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    /// The extension area: a request, or its answer, named by `name` rather
    /// than by a variant of this enum.
    ///
    /// Every peer that knows this variant can decode any extension, whatever
    /// its body, so a request defined here after a broker was built reaches
    /// that broker as a frame it can parse and answer with `unsupported`
    /// instead of one it has to reject. A client only sends one to a broker
    /// that advertised `FEATURE_UNSUPPORTED`; to an older broker it is an
    /// unknown `type` like any other.
    Extension {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        /// Whatever the extension defines; absent is `null`.
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        body: serde_json::Value,
    },

    // Topology and routing: who owns what, and how a stream or cache is sharded.
    /// This broker does not own the shard; the owner is named here.
    ///
    /// Only ever sent to a client that offered `FEATURE_REDIRECT`.
    NotLeader {
        /// Who owns it, so a client can recognise a redirect back to a broker
        /// it has already tried.
        node_id: String,
        /// `host:port` the owner serves *clients* on, or absent when the
        /// cluster has not been told where clients reach it. Absent means the
        /// client must find another way in rather than dial the wrong listener.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        addr: Option<String>,
        /// The assignment epoch this answer describes. A client holding a newer
        /// one has already moved on and should ignore this.
        generation: u64,
    },
    /// Ask the broker which brokers a client may connect to.
    ///
    /// Only ever sent to a broker that advertised `FEATURE_TOPOLOGY`.
    Topology,
    /// The brokers this one knows of that a client may connect to.
    TopologyView { brokers: Vec<BrokerEndpoint> },
    /// Ask how many shards a stream was placed with.
    ///
    /// A subscription reads one shard, so consuming a whole stream means one
    /// subscription per shard, and a client cannot know how many to open
    /// without asking. Only ever sent to a broker that advertised
    /// `FEATURE_STREAM_SHARDS`.
    StreamShards {
        tenant_id: String,
        namespace: String,
        stream: String,
        request_id: u64,
    },
    /// How many shards that stream has, as this broker's routing snapshot sees
    /// it.
    ///
    /// The answer can be stale in exactly the way any routing answer can: a
    /// stream whose shard count changed is described by whichever snapshot this
    /// broker last received. `0` means the broker knows nothing of the stream.
    StreamShardsView {
        shards: u32,
        request_id: u64,
        /// How the stream maps routing keys to shards. Absent is `modulo`,
        /// the only mapping that existed before this field, so a broker only
        /// sends it for a stream created with another mapping.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        routing: Option<crate::routing::ShardRouting>,
    },
    /// Ask how many shards a cache was placed with.
    ///
    /// A prefix watch reads one shard, so covering a prefix of a multi-shard
    /// cache means one watch per shard. Only ever sent to a broker that
    /// advertised `FEATURE_CACHE_SHARDS`.
    CacheShards {
        tenant_id: String,
        namespace: String,
        cache: String,
        request_id: u64,
    },
    /// How many shards that cache has, as this broker's routing snapshot sees
    /// it. `0` means the broker knows nothing of the cache.
    CacheShardsView { shards: u32, request_id: u64 },
    /// Ask which broker owns each shard of a stream or cache.
    ///
    /// Only ever sent to a broker that advertised `FEATURE_SHARD_OWNERS`.
    ShardOwners {
        tenant_id: String,
        namespace: String,
        /// The stream or cache. A stream and a cache may share a name, which
        /// is why `kind` is required.
        name: String,
        kind: ShardKind,
        request_id: u64,
    },
    /// One entry per shard, in shard order, as this broker's routing snapshot
    /// sees it. Empty means the broker knows nothing of the stream or cache.
    ShardOwnersView {
        owners: Vec<ShardOwner>,
        request_id: u64,
    },

    // Publishing, including idempotent producers.
    /// Publish a single payload to a stream.
    Publish {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes")]
        payload: Vec<u8>,
        /// Which shard this record belongs to, resolved by hashing.
        ///
        /// Absent means shard 0, which is what every record did before routing
        /// keys existed and what a single-shard stream does regardless. Present
        /// means the broker hashes it against the stream's shard count.
        ///
        /// **Ordering is per key, not per stream.** Two records with the same
        /// key are ordered with respect to each other; two with different keys
        /// may be applied by different brokers in either order. A stream with
        /// one shard keeps total order whatever keys are used, which is what
        /// makes this safe to add.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::client::message::base64_serde::base64_option_bytes"
        )]
        key: Option<Bytes>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    /// Publish a batch of payloads in a single request.
    PublishBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_vec")]
        payloads: Vec<Vec<u8>>,
        /// One key for the whole batch, with the same semantics as
        /// `Publish.key`. A batch is routed as a unit, so every record in it
        /// shares a shard — splitting a batch across shards would make it
        /// several batches with several acknowledgements, which is not what
        /// the caller asked for.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::client::message::base64_serde::base64_option_bytes"
        )]
        key: Option<Bytes>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ack: Option<AckMode>,
    },
    /// Publish ack with request id.
    PublishOk {
        request_id: u64,
        /// The log offset of the batch's first record; the rest follow it
        /// contiguously. Sent only to a client that offered
        /// `FLAG_BINARY_PUBLISH_ACK_OFFSET` in `Auth.client_flags`, and only
        /// when the broker answered after writing the batch to a log.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset: Option<u64>,
    },
    /// Publish error with request id. The optional fields are `Error`'s, under
    /// the same negotiation.
    PublishError {
        request_id: u64,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<ErrorCode>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry: Option<RetryClass>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ErrorDetail>,
    },
    /// Ask the broker for a producer id.
    ///
    /// Only ever sent to a broker that advertised `FEATURE_IDEMPOTENT_PRODUCER`.
    /// The id is the broker's to assign, so two producers can never pick the
    /// same one and have their sequences confused for each other's.
    ProducerInit { request_id: u64 },
    /// The producer id the broker assigned.
    ProducerInitOk { request_id: u64, producer_id: u64 },
    /// A batch the broker appends once, however many times it arrives.
    ///
    /// `sequence` counts this producer's batches on this shard from zero, one
    /// per batch whatever its size. The broker appends a batch whose sequence
    /// is the next it expects, answers a re-send of one it already holds with
    /// `publish_ok` and no second append, and refuses anything else with
    /// `publish_refused`. Only ever sent to a broker that advertised
    /// `FEATURE_IDEMPOTENT_PRODUCER`, and always acknowledged: a producer that
    /// never learns the answer cannot know what to send next.
    PublishIdempotent {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_vec")]
        payloads: Vec<Vec<u8>>,
        /// Routes the batch like `PublishBatch.key`. The sequence is per
        /// shard, so a producer keeps one counter per key's shard.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::client::message::base64_serde::base64_option_bytes"
        )]
        key: Option<Bytes>,
        request_id: u64,
        producer_id: u64,
        sequence: u64,
    },
    /// A `publish_idempotent` the broker would not append, with a reason the
    /// producer can act on rather than prose it would have to parse.
    ///
    /// Only ever sent to a client that offered `FEATURE_IDEMPOTENT_PRODUCER`,
    /// which it did by sending `publish_idempotent` at all.
    PublishRefused {
        request_id: u64,
        reason: PublishRefusalReason,
        message: String,
    },

    // Subscribing, and the events a subscription delivers.
    /// Subscribe to a stream; server responds with Subscribed.
    Subscribe {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        subscription_id: Option<u64>,
        /// Where to begin delivering from.
        ///
        /// Absent means the tail, which is what every client sent before this
        /// field existed — so an old client and a new broker still produce
        /// byte-for-byte the frames they always did, and a new client talking
        /// to an old broker has its position ignored rather than misread.
        #[serde(skip_serializing_if = "Option::is_none")]
        start: Option<StartPosition>,
        /// Which shard of the stream to read.
        ///
        /// Absent means shard 0, which is every record of a single-shard stream
        /// and is what a client that predates sharding expects.
        ///
        /// A subscription reads **one** shard. A stream's shards can have
        /// different owners, and a subscription is bound to one connection to
        /// one broker, so reading a whole multi-shard stream means one
        /// subscription per shard. `ClusterClient::subscribe_sharded` opens one
        /// per shard and merges them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        shard: Option<u32>,
    },
    /// Subscription confirmation with server-assigned ID.
    Subscribed {
        subscription_id: u64,
        /// The first offset this subscription delivers. Sent only for a
        /// subscribe with a `start`, on a durable stream, to a client that
        /// negotiated offsets, so a plain subscribe gets the frame it always did.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_offset: Option<u64>,
        /// The stream's tail when the subscriber was registered. Records below
        /// it are catch-up, records from it on are live, and none are skipped
        /// in between. Equal to `start_offset` for `latest`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        live_offset: Option<u64>,
    },
    /// A subscribe could not start where it was asked to.
    ///
    /// Distinct from `Error` because the client must be able to *act* on it:
    /// `too_old` means pick a newer offset (or `earliest`), `in_future` means
    /// the log has not reached that offset yet. A generic string forces every
    /// client to parse prose to tell those apart, and a client that guesses
    /// wrong silently restarts at the tail -- the failure resume exists to
    /// remove.
    SubscribeCursorError {
        reason: CursorErrorReason,
        /// The offset that was asked for.
        requested: u64,
        /// For `too_old`, the oldest offset still retained. For `in_future`,
        /// the current tail. Either way: the nearest offset that would work.
        available: u64,
    },
    /// First message on the event stream for a subscription.
    EventStreamHello { subscription_id: u64 },
    /// Last message on the event stream of a subscription or cache watch whose
    /// shard this broker has stopped serving: the shard moved, and this says
    /// where to pick it up. The broker finishes the stream after it.
    ///
    /// Sent only to a client that offered `FEATURE_SHARD_MOVED`; any other
    /// client sees the stream end after its last event, as it always has.
    ShardMoved {
        subscription_id: u64,
        /// The first offset this broker did not hand to the subscription:
        /// every record below it was sent to this subscriber or dropped by its
        /// queue, and none at or above it was. Resuming at
        /// `max(last delivered + 1, resume_from)` neither repeats nor skips a
        /// record. Absent for an in-memory stream, whose offsets mean nothing
        /// on another broker, and for a watch whose shard was still taking
        /// writes when it ended; resume after the last offset seen then.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resume_from: Option<u64>,
        /// The broker taking the shard, when this one knows. A hint: the shard
        /// may have moved again by the time the client gets there, and that
        /// broker then redirects like any other.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        node_id: Option<String>,
        /// That broker's client address, when the cluster publishes one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        addr: Option<String>,
        /// The assignment generation that moved the shard.
        generation: u64,
    },
    /// Last message on the event stream of a durable-stream subscription
    /// whose queue on the broker dropped records: the subscriber fell behind.
    /// The broker finishes the stream after it.
    ///
    /// Sent only to a client that offered `FEATURE_SUBSCRIPTION_LAGGED`; any
    /// other client keeps its subscription and sees the drop only as a jump
    /// in offsets, once a later event arrives.
    SubscriptionLagged {
        subscription_id: u64,
        /// The first dropped offset. Every event below it was sent before
        /// this message and none at or above it was, so resubscribing from
        /// here neither repeats nor skips a record.
        resume_from: u64,
    },
    /// Single event delivered to a subscriber.
    Event {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes")]
        payload: Vec<u8>,
        /// Log offset of this event, for durable streams.
        ///
        /// Absent for in-memory streams, which have no durable position to
        /// checkpoint against, and absent from every frame a pre-offsets broker
        /// sends.
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<u64>,
    },
    /// JSON event batch (binary batch uses FLAG_BINARY_EVENT_BATCH).
    EventBatch {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_vec")]
        payloads: Vec<Vec<u8>>,
        /// Offset of the first payload. The rest are contiguous from there, so
        /// payload `i` sits at `base_offset + i`.
        #[serde(skip_serializing_if = "Option::is_none")]
        base_offset: Option<u64>,
    },

    // Cache reads, writes, and watches.
    /// Cache set operation; may include TTL and request id.
    CachePut {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
        value: Bytes,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
        ttl_ms: Option<u64>,
    },
    /// Cache get operation; request id is echoed in responses.
    CacheGet {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    /// Cache delete; answered with `CacheValue` carrying whatever was removed.
    ///
    /// Sent only to a broker that advertised `FEATURE_CACHE_DELETE`: an older one
    /// has no arm for this variant, and an unrecognised message type ends its
    /// control loop.
    CacheDelete {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    /// Cache read response (value is optional for misses).
    CacheValue {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        #[serde(with = "crate::client::message::base64_serde::base64_option_bytes")]
        value: Option<Bytes>,
        #[serde(skip_serializing_if = "Option::is_none")]
        request_id: Option<u64>,
    },
    /// Cache write response with request id.
    CacheOk { request_id: u64 },
    /// Watch a cache key or key prefix for changes; answered with
    /// `CacheWatchStarted` and a uni event stream carrying `CacheEvent`s.
    ///
    /// Sent only to a broker that advertised `FEATURE_CACHE_WATCH`: an older one
    /// has no arm for this variant, and an unrecognised message type ends its
    /// control loop.
    CacheWatch {
        tenant_id: String,
        namespace: String,
        cache: String,
        /// Watch exactly this key. Exactly one of `key`/`prefix` must be set;
        /// both or neither is refused rather than guessed at.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key: Option<String>,
        /// Watch every key beginning with this prefix. `""` is every key in
        /// the shard.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        prefix: Option<String>,
        /// Which shard of the cache a *prefix* watch reads. A watch reads one
        /// shard, exactly as a stream subscription does: keys sharing a prefix
        /// hash to different shards, so a whole multi-shard cache is one watch
        /// per shard. Ignored for a `key` watch — the key names its shard by
        /// hashing, the same resolution a `cache_get` uses.
        ///
        /// Absent means shard 0, which is every key of a single-shard cache;
        /// on a multi-shard cache the broker refuses an absent shard.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        shard: Option<u32>,
        /// Resume from this cache-log offset — the first change the client has
        /// *not* seen, so a client checkpoints the offset it last handled plus
        /// one. Absent means from now: live changes only.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from_offset: Option<u64>,
        /// Deliver each matching key's *current* value first, then live
        /// changes — MQTT's retained message. Refused alongside `from_offset`,
        /// whose replay already reconstructs the state this shortcuts.
        ///
        /// Sent only to a broker that advertised
        /// `FEATURE_CACHE_WATCH_RETAINED`. An older watch-capable broker would
        /// ignore this unknown field and serve a live-only watch — a client
        /// silently missing the state it joined for — so the client must not
        /// send it on the strength of `FEATURE_CACHE_WATCH` alone.
        #[serde(default, skip_serializing_if = "is_false")]
        retained: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subscription_id: Option<u64>,
    },
    /// Watch confirmation. The uni event stream is bound by an
    /// `EventStreamHello` carrying the same `subscription_id`, exactly as a
    /// stream subscription's is.
    CacheWatchStarted {
        subscription_id: u64,
        /// The offset live delivery begins at: every change at or past it is
        /// delivered, and everything before it was covered by replay or by the
        /// snapshot. This is the client's first checkpoint.
        resume_offset: u64,
        /// True when `from_offset` names history that compaction has already
        /// collapsed. The watch then begins with each matching key's *current*
        /// value instead of the collapsed history — the same resnapshot
        /// contract the control plane's assignment watch uses. Never true for
        /// a watch that did not ask to resume.
        #[serde(default)]
        resnapshot: bool,
        /// How many retained values follow before live delivery, present
        /// exactly when the watch asked for retained delivery. `Some(0)` is
        /// the defined "no retained value" signal: joining an empty key is an
        /// answer, not silence, so it cannot be mistaken for a slow one. Once
        /// this many changes have arrived, the client holds the current state.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retained_count: Option<u64>,
    },
    /// One cache change, delivered on a watch's event stream.
    CacheEvent {
        key: String,
        /// The value the key now holds; absent means the key was deleted.
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            with = "crate::client::message::base64_serde::base64_option_bytes"
        )]
        value: Option<Bytes>,
        /// The cache-log offset of the change. Offsets are naturally sparse on
        /// a filtered watch — other keys' changes consume them — so a gap here
        /// is *not* a drop signal; `CacheWatchLagged` is.
        offset: u64,
        /// Absolute Unix milliseconds this value expires at; `0` means never.
        /// `0` for a delete.
        #[serde(default)]
        expires_at_millis: u64,
    },
    /// The watch fell behind and its queue dropped changes; the broker ends the
    /// event stream after sending this. Filtering makes offsets sparse, so a
    /// drop on a watch is not visible as an offset jump the way a stream
    /// subscriber's is — this signal is what makes the loss loud. Re-watching
    /// with `from_offset = resume_from` is gapless.
    CacheWatchLagged {
        /// The offset of the first change this watch missed.
        resume_from: u64,
    },

    // Counters.
    /// Apply a signed delta to a counter; answered with `CounterValue` carrying
    /// the sum including this delta.
    ///
    /// Sent only to a broker that advertised `FEATURE_COUNTERS`: an older one
    /// has no arm for this variant, and an unrecognised message type ends its
    /// control loop.
    ///
    /// A counter is scoped exactly as a cache key is — the same registered
    /// cache scope, the same key-to-shard hash, the same owner — but lives in a
    /// store of its own: a counter and a cache value may share a key and are
    /// unrelated. **Delivery is at least once**: a retried add after a lost
    /// acknowledgement counts twice, and deltas carry no dedupe identity. See
    /// `docs/projections.md` for the decision.
    CounterAdd {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        delta: i64,
        request_id: u64,
    },
    /// Read a counter's current sum; answered with `CounterValue`.
    CounterGet {
        tenant_id: String,
        namespace: String,
        cache: String,
        key: String,
        request_id: u64,
    },
    /// A counter's sum. Absent means the counter has never been written —
    /// which is a different answer from a sum of zero, exactly as a cache miss
    /// differs from a stored empty value.
    CounterValue {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        value: Option<i64>,
        request_id: u64,
    },

    // Atomic commits and the state they write.
    /// Append `event` and apply `changes` to one shard of `stream` as one
    /// record: every reader sees all of it or none of it. `entity_key` picks
    /// the shard the way a publish's routing key does. Answered with
    /// `commit_ok`, or `error`. Sent only to a broker that advertised
    /// `FEATURE_ATOMIC_COMMIT`.
    Commit {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
        entity_key: Bytes,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
        event: Bytes,
        #[serde(default)]
        changes: Vec<StateChange>,
        request_id: u64,
    },
    /// The commit is durable (on a `Quorum` stream, on a majority) at
    /// `offset`, which is also the version of every key it wrote.
    CommitOk { request_id: u64, offset: u64 },
    /// Read `key` in the state of the shard `entity_key` routes to. Answered
    /// with `state_value`. Sent only to a broker that advertised
    /// `FEATURE_ATOMIC_COMMIT`.
    StateGet {
        tenant_id: String,
        namespace: String,
        stream: String,
        #[serde(with = "crate::client::message::base64_serde::base64_bytes_bytes")]
        entity_key: Bytes,
        key: String,
        request_id: u64,
    },
    /// A key's value and the offset of the commit that wrote it, as of
    /// `as_of`: every commit at or below it and no later one.
    StateValue {
        #[serde(with = "crate::client::message::base64_serde::base64_option_bytes")]
        value: Option<Bytes>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        as_of: Option<u64>,
        request_id: u64,
    },

    // Consumer groups and their dead letters.
    /// Take records for a consumer group, claimed until the visibility timeout.
    ///
    /// A poll rather than a subscription: a queue consumer takes work when it
    /// has capacity for it, and the broker cannot know that. Sent only to a
    /// broker that advertised `FEATURE_CONSUMER_GROUP`.
    GroupPoll {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        /// Most records to take. The broker may return fewer, including none.
        max_records: u32,
        /// How long the broker may hold the request open waiting for work,
        /// in milliseconds.
        ///
        /// Omitted or `0` answers immediately with whatever is available, which
        /// is what a broker that predates this does — so an older peer degrades
        /// to a plain poll rather than misreading the request. The broker caps
        /// it; a client cannot hold a stream open indefinitely.
        #[serde(default)]
        wait_ms: u64,
        request_id: u64,
    },
    /// Records claimed by a `GroupPoll`, in offset order.
    ///
    /// Empty means nothing was available, which is an answer rather than an
    /// error: the log has no unclaimed records for this group right now.
    GroupRecords {
        records: Vec<GroupRecord>,
        request_id: u64,
    },
    /// Finish one record. Everything below the group's cursor stays finished.
    GroupAck {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: u64,
        request_id: u64,
    },
    /// Hand one record back without finishing it, to be redelivered at once
    /// rather than after the visibility timeout.
    GroupNack {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: u64,
        request_id: u64,
    },
    /// Offsets this group gave up on. Answered with `GroupDeadLetterList`.
    ///
    /// Sent only to a broker that advertised `FEATURE_GROUP_DEAD_LETTERS`.
    GroupDeadLetters {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        request_id: u64,
    },
    /// Offsets the group gave up on, lowest first.
    ///
    /// The records are still in the stream's log at these offsets: this is a
    /// list of what to look at, not a copy of it.
    GroupDeadLetterList { offsets: Vec<u64>, request_id: u64 },
    /// Drop one dead letter from the list, having decided the record is not
    /// worth reprocessing. Does not touch the record itself.
    GroupDiscard {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: u64,
        request_id: u64,
    },
    /// Put one dead letter back in the queue, its attempt count reset.
    ///
    /// For when the reason it failed has been fixed. The group's cursor is not
    /// moved backwards — the record is owed again, which is a different thing:
    /// everything the group finished stays finished.
    GroupRedrive {
        tenant_id: String,
        namespace: String,
        stream: String,
        shard: u32,
        group: String,
        offset: u64,
        request_id: u64,
    },

    // Last, because serde requires its catch-all to be.
    /// A `type` this build does not know. Never sent, and never produced by
    /// anything but decoding.
    ///
    /// Decoding lands here rather than failing so a broker can tell "a
    /// request from a newer client" from "bytes that are not a message", and
    /// answer the first with `unsupported`. [`Message::unknown_request`] reads
    /// the type and request id back out of the frame.
    #[serde(other, skip_serializing)]
    Unknown,
}

impl Message {
    /// JSON-encode into an unflagged frame.
    pub fn encode(&self) -> Result<Frame> {
        let payload = serde_json::to_vec(self).map_err(Error::Serialize)?;
        Frame::new(0, Bytes::from(payload))
    }

    /// Decode a frame's JSON payload.
    pub fn decode(frame: Frame) -> Result<Self> {
        serde_json::from_slice(&frame.payload).map_err(Error::Deserialize)
    }

    /// An `Error` with no code: the shape every client understands.
    pub fn error(message: impl Into<String>) -> Self {
        Message::Error {
            message: message.into(),
            code: None,
            retry: None,
            detail: None,
        }
    }

    /// A `PublishError` with no code.
    pub fn publish_error(request_id: u64, message: impl Into<String>) -> Self {
        Message::PublishError {
            request_id,
            message: message.into(),
            code: None,
            retry: None,
            detail: None,
        }
    }

    /// The `type` and `request_id` of a frame that decoded as
    /// [`Message::Unknown`], for the `unsupported` answer. `None` when the
    /// payload is not a JSON object with a string `type`.
    pub fn unknown_request(frame: &Frame) -> Option<UnknownRequest> {
        serde_json::from_slice(&frame.payload).ok()
    }

    /// Drop a `PublishOk`'s offset, for a client that did not offer
    /// `FLAG_BINARY_PUBLISH_ACK_OFFSET`. Anything else is returned unchanged.
    pub fn without_ack_offset(self) -> Self {
        match self {
            Message::PublishOk { request_id, .. } => Message::PublishOk {
                request_id,
                offset: None,
            },
            other => other,
        }
    }

    /// Drop the error-code fields, for a client that did not offer
    /// `FEATURE_ERROR_CODES`. Anything but an `Error` or `PublishError` is
    /// returned unchanged.
    pub fn without_error_code(self) -> Self {
        match self {
            Message::Error { message, .. } => Message::error(message),
            Message::PublishError {
                request_id,
                message,
                ..
            } => Message::publish_error(request_id, message),
            other => other,
        }
    }
}

/// What can be read of a request whose `type` this build does not know.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct UnknownRequest {
    #[serde(rename = "type")]
    pub request_type: String,
    /// Absent, or not a number, reads as `None`: the answer is still owed.
    #[serde(default, deserialize_with = "lenient_request_id")]
    pub request_id: Option<u64>,
}

fn lenient_request_id<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    Ok(value.as_u64())
}

/// For `skip_serializing_if`: an unset flag stays off the wire entirely, so a
/// message that does not use it is byte-identical to one that predates it.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests;
