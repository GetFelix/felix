//! Feature bits: optional requests a broker may serve, advertised in `AuthOk`.
//!
//! A *feature* bit is not a frame flag. Frame flags say how a payload is laid
//! out and travel on every frame; these say only that a request exists, and
//! never appear on a frame at all. They are numbered separately for that
//! reason -- sharing the space would have a client offering to receive a frame
//! shape it has no encoder for.
//!
//! A client must not send a featured request to a broker that did not advertise
//! the bit: to a broker without `FEATURE_UNSUPPORTED`, an unrecognised message
//! type is a fatal protocol error, so probing costs the connection. A broker
//! that advertises `FEATURE_UNSUPPORTED` answers one with `unsupported`
//! instead, which is what lets a request added from here on go without a bit
//! of its own.

/// The broker answers `topology`: which brokers a client may connect to.
pub const FEATURE_TOPOLOGY: u32 = 0x0000_0001;

/// The peer understands `NotLeader`.
///
/// Offered by a *client*, and read by the broker, which is the direction that
/// matters here: `NotLeader` travels broker to client, and a client that cannot
/// decode it would lose the connection to a message meant to help it. A broker
/// talking to a client that did not offer this bit answers with an ordinary
/// error instead.
pub const FEATURE_REDIRECT: u32 = 0x0000_0002;

/// The broker understands `CacheDelete`.
///
/// Advertised by a *broker*, because this is a request rather than a response:
/// a client that sent it to a broker predating it would be sending an
/// unrecognised message type, which is fatal to the broker's control loop. A
/// client that does not see this bit reports that the broker cannot delete
/// rather than trying and losing the connection.
pub const FEATURE_CACHE_DELETE: u32 = 0x0000_0004;

/// The broker serves consumer groups: `group_poll`, `group_ack`, `group_nack`.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_DELETE` and for the same
/// reason: these are requests, and sending one to a broker that has no arm for
/// it ends that broker's control loop rather than returning an error.
///
/// A broker with no durable storage never advertises it. A group whose position
/// is lost on restart redelivers everything it had already finished, so there is
/// nothing useful to offer.
pub const FEATURE_CONSUMER_GROUP: u32 = 0x0000_0008;

/// The broker serves the dead-letter requests: `group_dead_letters`,
/// `group_discard`, `group_redrive`.
///
/// A bit of its own rather than folded into `FEATURE_CONSUMER_GROUP`. That bit
/// already means "serves poll, ack and nack" to every broker that advertises
/// it, and a broker built before these requests existed would have no arm for
/// them — which ends its control loop rather than returning an error. A feature
/// bit says one set of requests exists, and widening what an existing bit
/// promises is the one thing that cannot be done safely.
pub const FEATURE_GROUP_DEAD_LETTERS: u32 = 0x0000_0010;

/// The broker answers `stream_shards`: how many shards a stream was placed with.
///
/// A subscription reads one shard, so a client that wants a whole multi-shard
/// stream has to know how many there are. Nothing else on the wire tells it:
/// `topology` names brokers, not streams.
///
/// Advertised by a *broker*, and a separate bit rather than folded into
/// `FEATURE_TOPOLOGY` for the usual reason — that bit already means "names the
/// brokers" to every broker that advertises it, and a broker built before this
/// request existed has no arm for it.
pub const FEATURE_STREAM_SHARDS: u32 = 0x0000_0020;

/// The broker serves `cache_watch`: a subscription to changes for one cache
/// key or key prefix.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_DELETE` and for the same
/// reason: this is a request, and sending it to a broker that has no arm for
/// it ends that broker's control loop rather than returning an error.
///
/// Only a broker whose cache is log-backed advertises it. A watch's contract is
/// built on log offsets — resume, duplicate detection, and the lag signal all
/// name them — and a cache with no log has none to offer.
pub const FEATURE_CACHE_WATCH: u32 = 0x0000_0040;

/// The broker serves *retained* delivery on a `cache_watch`: each matching
/// key's current value first, then live changes.
///
/// A bit of its own rather than folded into `FEATURE_CACHE_WATCH`, for the
/// reason the dead-letter bit is not folded into the consumer-group bit: a bit
/// says which requests exist, and widening what an existing bit promises is
/// the one change that cannot be made safely. A broker built when
/// `FEATURE_CACHE_WATCH` meant live-and-resume only would ignore the unknown
/// `retained` field and serve a live-only watch — the client silently missing
/// exactly the state it joined for.
pub const FEATURE_CACHE_WATCH_RETAINED: u32 = 0x0000_0080;

/// The broker serves counters: `counter_add` and `counter_get`.
///
/// Advertised by a *broker*, like every request-shaped feature: sending either
/// to a broker with no arm for it ends that broker's control loop rather than
/// returning an error.
///
/// Only a broker with durable storage advertises it. A counter is a fold over
/// a log — the sum is rebuilt from the deltas on recovery — and a broker with
/// nowhere to write the log would be offering a sum that any restart resets,
/// which is worse than refusing to count at all.
pub const FEATURE_COUNTERS: u32 = 0x0000_0100;

/// The broker assigns producer ids (`producer_init`) and accepts
/// `publish_idempotent`: a batch carrying a producer id and a sequence number,
/// which it appends once however many times it arrives. A re-send of a batch
/// the broker already holds is acknowledged with the original's outcome rather
/// than appended again, and a refusal comes back as `publish_refused` with a
/// reason a client can act on.
///
/// Both messages exist only under this bit: a client offers the bit in its own
/// features to say it can decode `publish_refused`, and sends the requests
/// only to a broker that advertised it.
pub const FEATURE_IDEMPOTENT_PRODUCER: u32 = 0x0000_0200;

/// The broker answers `cache_shards`: how many shards a cache was placed with.
///
/// A prefix watch reads one shard, so watching a prefix across a multi-shard
/// cache means one watch per shard, and the client has to know how many there
/// are. A bit of its own rather than a field on `stream_shards`: a broker that
/// predates it would ignore the field and answer for a stream of the same name.
pub const FEATURE_CACHE_SHARDS: u32 = 0x0000_0400;

/// The client can read typed error codes: `code`, `retry` and `detail` on
/// `error` and `publish_error`.
///
/// Offered by a *client*, like `FEATURE_REDIRECT`: the fields travel broker to
/// client, and a broker sends them only when this bit was offered, so every
/// other client keeps getting byte-identical frames. Serde would ignore the
/// fields anyway; the bit exists so the frames stay the same, not to avoid a
/// decode failure. A broker advertises it too, so a client knows whether an
/// error without a code means "no code applies" or "this broker predates them".
pub const FEATURE_ERROR_CODES: u32 = 0x0000_0800;

/// The client can read `shard_moved` on an event stream.
///
/// Offered by a *client*, like `FEATURE_REDIRECT`: the message travels broker
/// to client, as the last frame of a subscription or cache watch whose shard
/// this broker stopped serving, and says where to resume. A client that did
/// not offer the bit gets what it always got: the stream ends after the last
/// event, with nothing after it. A broker advertises it too, so a client knows
/// a stream that ends without one did not end because its shard moved.
pub const FEATURE_SHARD_MOVED: u32 = 0x0000_1000;

/// The peer answers a request type it does not know with `unsupported`, and
/// carries on serving the stream, instead of ending the control loop.
///
/// Advertised by a *broker* to say it answers that way; offered by a *client*
/// to say it can decode `unsupported`. A broker only sends `unsupported` to a
/// client that offered the bit: any other client gets what it always got, a
/// closed stream. With both bits set a client may send a request the broker
/// may not know -- including anything in the `extension` area -- and read a
/// typed refusal rather than lose the connection, so a new request no longer
/// needs a feature bit before a client can try it.
pub const FEATURE_UNSUPPORTED: u32 = 0x0000_2000;

/// The client can read `publish_refused` with reason `sequence_reused`.
///
/// Offered by a *client*, like `FEATURE_ERROR_CODES`. A leader keeps a digest
/// of each remembered batch's payloads, and a batch under a remembered
/// sequence whose payloads differ is refused with that reason, for a client
/// that offered the bit. Any other client gets what it always got: the batch
/// is answered as a duplicate of the one held, and not written. A broker
/// advertises the bit too, so a client knows whether a duplicate answer was
/// checked.
pub const FEATURE_SEQUENCE_REUSED: u32 = 0x0000_4000;

/// Acknowledged publishes on a stream are pipelined: the client may have
/// several unanswered, and the broker answers them in the order the stream
/// carried them.
///
/// Offered by a *client*; a broker that grants it answers `publish_window`
/// in `AuthOk`, the most acknowledged publishes that may be unanswered at
/// once: across the whole connection, or on each stream when the broker also
/// advertises `FEATURE_STREAM_PUBLISH_WINDOW`. The broker stops reading
/// publishes while that many are outstanding, so a client that sends more is
/// slowed by flow control rather than refused. Without the
/// bit, answers come in completion order and the request id is the only
/// correlation, as before.
///
/// The order is what an idempotent producer needs to keep several batches in
/// flight: when one fails, every batch behind it on the stream is answered
/// after it, so the producer learns about the failure before it acts on any
/// later answer.
pub const FEATURE_PUBLISH_PIPELINE: u32 = 0x0000_8000;

/// The broker serves `commit` and `state_get`: an event and state updates
/// written to one stream shard as one record, and reads of that state.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_DELETE`. Advertised by a
/// broker that implements it even before its fleet enables `atomic_commit`;
/// until then a commit is refused with an error that says so.
pub const FEATURE_ATOMIC_COMMIT: u32 = 0x0001_0000;
/// The broker's `publish_window` is per stream, not per connection.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_DELETE`, alongside
/// `FEATURE_PUBLISH_PIPELINE`. Each pipelining stream then has its own window,
/// so a stream whose publishes are stuck behind a stalled shard holds only its
/// own slots and the connection's other streams keep publishing. Without the
/// bit the window is shared by every stream on the connection, which is what a
/// client must assume of a broker that predates it.
pub const FEATURE_STREAM_PUBLISH_WINDOW: u32 = 0x0002_0000;

/// The broker answers `shard_owners`: which broker owns each shard of a
/// stream or cache.
///
/// Advertised by a *broker*, like `FEATURE_CACHE_SHARDS`. A client learns an
/// owner otherwise only by sending something to the shard and being
/// redirected or forwarded.
pub const FEATURE_SHARD_OWNERS: u32 = 0x0004_0000;

/// The client wants each acknowledged publish answered after its write, with
/// its offset, rather than when it is queued.
///
/// Offered by a *client*, which is the side asking, and advertised by a
/// broker that honours it, which is how the client learns its acks mean what
/// it asked. The same answer a broker gives everyone under
/// `FELIX_ACK_ON_COMMIT=true`, for this connection only. A client offers it
/// only when its application asked: it trades latency for the offset, so
/// clients do not offer every bit they know here, as they do elsewhere.
pub const FEATURE_ACK_ON_COMMIT: u32 = 0x0008_0000;

/// The client reads `skipped_before` on a `GroupRecord`.
///
/// Offered by a *client* and advertised by a broker that reports it, which
/// is how the client learns that an absent field means `0` rather than "not
/// reported". A client that did not offer it gets the record without the
/// field, so its frames are byte-identical to the ones it always got.
pub const FEATURE_GROUP_SKIPPED: u32 = 0x0010_0000;

/// Every feature bit this version implements.
pub const KNOWN_FEATURES: u32 = FEATURE_TOPOLOGY
    | FEATURE_REDIRECT
    | FEATURE_CACHE_DELETE
    | FEATURE_CONSUMER_GROUP
    | FEATURE_GROUP_DEAD_LETTERS
    | FEATURE_STREAM_SHARDS
    | FEATURE_CACHE_WATCH
    | FEATURE_CACHE_WATCH_RETAINED
    | FEATURE_COUNTERS
    | FEATURE_IDEMPOTENT_PRODUCER
    | FEATURE_CACHE_SHARDS
    | FEATURE_ERROR_CODES
    | FEATURE_SHARD_MOVED
    | FEATURE_UNSUPPORTED
    | FEATURE_SEQUENCE_REUSED
    | FEATURE_PUBLISH_PIPELINE
    | FEATURE_ATOMIC_COMMIT
    | FEATURE_STREAM_PUBLISH_WINDOW
    | FEATURE_SHARD_OWNERS
    | FEATURE_ACK_ON_COMMIT
    | FEATURE_GROUP_SKIPPED;

/// True if `features` advertises `feature`.
pub fn supports_feature(features: u32, feature: u32) -> bool {
    features & feature == feature
}

#[cfg(test)]
mod tests;
