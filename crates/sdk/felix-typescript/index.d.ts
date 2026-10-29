// Hand-written to stay readable. `napi build` also emits a generated
// `index.d.ts`; this file is the checked-in contract and the generated one is
// expected to agree with it. `task ts:check` is where a drift shows up.

/// <reference types="node" />

/** One delivered record. */
export interface Event {
  tenantId: string;
  namespace: string;
  stream: string;
  payload: Buffer;
  /**
   * The record's log offset on a durable stream, absent on an ephemeral one.
   *
   * Subscriber queues shed under the default policy rather than blocking the
   * publisher, so a gap here is the signal that it happened:
   * `offset - previous - 1n - skippedBefore` records were dropped.
   */
  offset: bigint | null;
  /**
   * How many offsets just before `offset` hold no event. Non-zero only on the
   * first event after a leader change, whose generation-start record took an
   * offset; `0n` from a broker that predates it.
   */
  skippedBefore: bigint;
}

/** The delivery `groupNack` hands back. A `GroupRecord` is one. */
export interface GroupDelivery {
  offset: bigint;
  attempts: number;
}

/** One record handed out by a consumer group. */
export interface GroupRecord {
  /** What to pass to `groupAck` to settle this record. */
  offset: bigint;
  payload: Buffer;
  /**
   * How many times this record has been handed out, this delivery included.
   * `1` is a first attempt; anything higher is a redelivery, so a consumer can
   * treat a retry differently. `0` means the broker did not report it.
   */
  attempts: number;
  /**
   * How many offsets directly below this one were settled without being
   * delivered (generation starts, records retention removed). A hole with this
   * count will not fill. `0n` from a broker that predates it.
   */
  skippedBefore: bigint;
}

/** One change observed by a cache watch. */
export interface CacheChange {
  key: string;
  /** `null` when the key was deleted or expired, which is not `Buffer.alloc(0)`. */
  value: Buffer | null;
  /** This change's cache-log offset. Re-watch from `offset + 1n` to resume. */
  offset: bigint;
  expiresAtMillis: bigint;
}

/**
 * An item from a cache watch: a change, or notice that the watch fell behind.
 *
 * Exactly one field is set. Lag is a value rather than a rejection because it
 * is not a failure — the watch did its job by saying so — and a filtered
 * watch's offsets are sparse by construction, so loss cannot be inferred the
 * way a stream subscriber infers it.
 */
export interface CacheWatchItem {
  change: CacheChange | null;
  /**
   * Set when the watch lagged and the broker ended it. Re-watching with
   * `start = laggedResumeFrom` is gapless.
   */
  laggedResumeFrom: bigint | null;
  /**
   * Set when the watch's shard moved to another broker. The watch follows it
   * there on its own and carries on where it left off.
   */
  shardMoved: ShardMoved | null;
}

/**
 * Where a shard went when it moved to another broker. Cache watches and
 * sharded subscriptions follow it on their own; this is a notice.
 */
export interface ShardMoved {
  resumeFrom: bigint | null;
  nodeId: string | null;
  addr: string | null;
  generation: bigint;
}

/**
 * An item from a sharded subscription.
 *
 * Exactly one of `event`, `lostError`, `recovered`, `shardMoved` and
 * `streamDeleted` is set, and `shard` says which shard it concerns. A lost
 * shard does not affect the others: they keep delivering while that one is
 * re-established, and it resumes from its own last offset so nothing is
 * skipped. A moved shard is followed: its records carry on from the new owner,
 * or a loss comes next. A deleted stream ends the shard for good.
 */
export interface ShardEvent {
  shard: number;
  event: Event | null;
  lostError: string | null;
  recovered: boolean | null;
  shardMoved: ShardMoved | null;
  streamDeleted: boolean | null;
}

/** How much the broker must have done before a publish resolves. */
export type AckMode = "none" | "per_message" | "per_batch";

/** Where a new subscription begins. */
export type StartPosition = "latest" | "earliest" | bigint;

/** Per-shard offsets, keyed by shard number. */
export type ShardPositions = Record<string, bigint>;

/**
 * What the broker says a caller may do after an error. `retry`, `retry_after`
 * and `redirect` mean nothing was applied; `outcome_unknown` means it may have
 * been, so only an idempotent request is safe to send again.
 */
export type RetryClass = "retry" | "retry_after" | "redirect" | "outcome_unknown" | "fatal";

/** Extra facts the broker sent with an error. Every field is optional. */
export interface ErrorDetail {
  /**
   * For `shard_unavailable`: `not_assigned`, `owner_unavailable`, `not_ready`,
   * `stale`, `fenced` or `moving`.
   */
  reason?: string;
  /**
   * How long the broker suggests waiting: for `retry_after`, and as a hint with
   * `retry` for a shard that is `moving`.
   */
  retry_after_ms?: number;
}

/** Base class for every error this client raises. */
export declare class FelixError extends Error {
  /**
   * Which kind of failure this is (`FELIX_AUTH`, `FELIX_SHARD_UNAVAILABLE`,
   * ...), the same thing the class says. Always set.
   */
  readonly kind: string;
  /**
   * The broker's error code, such as `shard_unavailable` or `quorum_timeout`.
   * `undefined` when the broker predates error codes or the failure was local.
   */
  readonly code: string | undefined;
  /** The broker's retry class, or `undefined` when it sent no code. */
  readonly retry: RetryClass | undefined;
  /** Extra facts from the broker, or `undefined`. */
  readonly detail: ErrorDetail | undefined;
  /**
   * Whether sending the same request again could succeed without applying it
   * twice. Decided by `retry` when the broker sent one, and otherwise by the
   * class: `ConnectionError`, `ShardUnavailableError` and `OverloadedError`.
   */
  readonly retryable: boolean;
}

/**
 * The broker could not be reached, the connection was lost mid-call, or the
 * broker is shutting down (`draining`).
 */
export declare class ConnectionError extends FelixError {}
/** The token was rejected, or lacks the permission this call needs. */
export declare class AuthError extends FelixError {}
/** The tenant, namespace, stream or cache does not exist on the broker. */
export declare class NotFoundError extends FelixError {}
/** The requested start offset is gone — retention discarded it. */
export declare class CursorError extends FelixError {}
/** A bad argument to this client, rather than a failure of the call. */
export declare class InvalidArgumentError extends FelixError {}
/**
 * Nobody can serve the shard right now, typically while it moves
 * (`shard_unavailable`), or another broker owns it (`not_leader`). Nothing was
 * applied; retrying is safe.
 */
export declare class ShardUnavailableError extends FelixError {}
/** The broker is shedding load (`overloaded`). Nothing was applied. */
export declare class OverloadedError extends FelixError {}
/**
 * The write may or may not have been applied: `quorum_timeout`,
 * `leadership_lost`, `unacknowledged`, or any error whose retry class is
 * `outcome_unknown`. Only an idempotent request is safe to send again.
 */
export declare class OutcomeUnknownError extends FelixError {}

/** A commit was refused before it was sent. Nothing was written. */
export declare class CommitError extends FelixError {}
/**
 * An op names a stream other than the commit's: a different stream is a
 * different log, and a commit writes one.
 */
export declare class NotOnOwningShardError extends CommitError {
  /** Which op, counting from 0. */
  readonly index: number;
  /** The stream that op named. */
  readonly stream: string;
  /** The commit's own stream. */
  readonly owner: string;
}
/** A commit carries exactly one event (publish or enqueue). */
export declare class EventCountError extends CommitError {
  readonly count: number;
}

/**
 * The broker would not append an idempotent publish, and said why. Nothing was
 * written, and the producer is finished on that stream.
 */
export declare class PublishRefusedError extends FelixError {
  /**
   * `sequence_gap` (the producer skipped ahead), `unknown_producer` (the shard
   * holds nothing for it), `sequence_expired` (a re-send from further back than
   * the broker remembers), `sequence_reused` (a different batch under a spent
   * sequence), `offset_mismatch` or `not_leader`.
   */
  readonly reason:
    | "sequence_gap"
    | "unknown_producer"
    | "sequence_expired"
    | "sequence_reused"
    | "offset_mismatch"
    | "not_leader";
  /** For `sequence_gap`: the sequence the broker would have appended. */
  readonly expected?: bigint;
  /** For `offset_mismatch`: the shard's next offset. */
  readonly tail?: bigint;
  /** For `not_leader`: the node that leads the shard. */
  readonly nodeId?: string;
  /** For `not_leader`: where that node serves clients, when known. */
  readonly addr?: string;
}

/**
 * One part of an atomic commit. Exactly one op per commit is an event
 * (`publish` or `enqueue`); every op names the same stream.
 */
export type CommitOp =
  | { op: "publish"; stream: string; payload: Buffer }
  | { op: "enqueue"; queue: string; payload: Buffer }
  | { op: "put"; stream: string; key: string; value: Buffer }
  | { op: "delete"; stream: string; key: string };

/** Builders for `CommitOp`s; each returns the plain object. */
export declare const CommitOp: {
  publish(stream: string, payload: Buffer | string): CommitOp;
  enqueue(queue: string, payload: Buffer | string): CommitOp;
  put(stream: string, key: string, value: Buffer | string): CommitOp;
  delete(stream: string, key: string): CommitOp;
};

/** A commit the broker made durable. */
export interface CommitReceipt {
  /** Where the event is read, and the version of every key the commit wrote. */
  offset: bigint;
}

/**
 * What a conditional cache put requires of the key's current entry: no live
 * entry (never written, deleted or expired), or exactly this version.
 */
export type CacheCondition = { kind: "absent" } | { kind: "version"; version: bigint };

/** Builders for `CacheCondition`s; each returns the plain object. */
export declare const CacheCondition: {
  /** Write only if the key has no live entry: take a lease, claim a key. */
  absent(): CacheCondition;
  /** Write only if the key is at this version, from `cacheGetVersioned`. */
  version(version: bigint | number): CacheCondition;
};

/** A cache value and the version a conditional write compares against. */
export interface VersionedValue {
  value: Buffer;
  version: bigint;
}

/** The answer to `cachePutIf` or `cacheDeleteIf`. A refusal is not an error. */
export interface CacheConditionResult {
  /** The condition held and the write was made. */
  applied: boolean;
  /**
   * After an applied put, the version it wrote. Otherwise the key's current
   * version, `null` when it has no live entry.
   */
  version: bigint | null;
}

/**
 * The answer to `publishIf`, `commitIf` or their `OnShard` forms. A refusal is
 * not an error: nothing was written and no offset was consumed.
 */
export type ConditionalWrite =
  | { written: true; offset: bigint; tail: null }
  | { written: false; offset: null; tail: bigint };

/** Where `groupSeek` or `groupCreate` left a group on one shard. */
export interface GroupPosition {
  /** The offset the group resumes from. */
  offset: bigint;
  /** `false` when `groupCreate` found the group already there and left it alone. */
  moved: boolean;
}

/** Where a group stands on one shard, from `groupDescribe`. */
export interface GroupInfo {
  /**
   * Everything below this is finished. `null` for a group with no cursor on
   * the shard, which starts at the beginning of the log when polled.
   */
  committed: bigint | null;
  /** The shard's committed tail. */
  tail: bigint;
  /**
   * Records handed out and not yet settled. The shard leader's memory, so it
   * restarts from zero when the leader changes.
   */
  inFlight: bigint;
  /** Records owed again after a nack or a lapsed claim. Leader memory, like `inFlight`. */
  owed: bigint;
  /** Records the group gave up on. */
  deadLetters: bigint;
  /** Records published to the shard that the group has not finished. */
  lag: bigint;
}

/**
 * An item from a prefix watch across every shard of a cache.
 *
 * Exactly one of `change`, `stateComplete`, `laggedResumeFrom`, `shardMoved`
 * and `shardClosed` is set. Losing a shard is an item and not silence: the
 * other shards keep delivering, and a consumer that ignores these is choosing
 * to watch part of the prefix. `shard` is `null` only for `stateComplete`,
 * which concerns the whole watch.
 */
export interface ShardedCacheWatchItem {
  shard: number | null;
  change: CacheChange | null;
  /**
   * Every shard has delivered its current state; everything further is live.
   * Only on a retained watch, at most once, and never if a shard ended first.
   */
  stateComplete: boolean | null;
  /** This shard fell behind and its watch ended. `resumeOffsets()` already accounts for it. */
  laggedResumeFrom: bigint | null;
  /** This shard moved to another broker and its watch is following it there. */
  shardMoved: ShardMoved | null;
  /** This shard's watch ended without a lag signal. */
  shardClosed: boolean | null;
  /** With `shardClosed`, why the shard could not be reopened on another broker in time, when that is why it ended. */
  shardClosedError: string | null;
}

/** One committed record from `read`. */
export interface StreamRecord {
  offset: bigint;
  payload: Buffer;
  /** The principal that published it, when the broker recorded one. */
  publisher: string | null;
  /** When the record was appended, in microseconds since the Unix epoch. */
  timestampMicros: bigint;
}

/** One page of a shard's committed records. */
export interface StreamPage {
  records: StreamRecord[];
  /**
   * Where the next page starts. Offsets that hold no record are passed over,
   * so this is not always the last offset plus one.
   */
  nextOffset: bigint;
}

/** A record for `publishRecords`: the routing key it hashes by, and its payload. */
export interface KeyedRecord {
  key: Buffer;
  payload: Buffer;
}

/** How one shard's piece of a `publishRecords` call fared. */
export interface ShardPublish {
  shard: number;
  /** Positions, in the order passed, of the records in this piece. */
  records: number[];
  /** The log offset of the piece's first record, when the broker reported one. */
  offset: bigint | null;
  /** Why the piece was not accepted, as the typed error a rejected call would carry. A piece is all or nothing. */
  error: FelixError | Error | null;
}

/** What `publishRecords` did, shard by shard. Every record is in exactly one entry. */
export interface BatchReport {
  shards: ShardPublish[];
  /** Whether every piece was accepted. */
  ok: boolean;
  /** Positions of the records in failed pieces, ascending. */
  failedRecords: number[];
}

/** One broker's connection count, from `connectionsPerNode`. */
export interface ConnectionCount {
  addr: string;
  connections: number;
}

/** A record from `ShardedGroupHandle.poll`. */
export interface ShardedGroupRecord {
  /** The shard it came from, which settling it has to reach. */
  shard: number;
  offset: bigint;
  payload: Buffer;
  /** How many times this record has been handed out, this delivery included. */
  attempts: number;
  skippedBefore: bigint;
}

/** What settling a sharded group record needs. A `ShardedGroupRecord` is one. */
export interface ShardedGroupDelivery {
  shard: number;
  offset: bigint;
  attempts: number;
}

/** A key in a stream shard's state. */
export interface StateValue {
  /** Absent when the key was never written or was deleted. */
  value: Buffer | null;
  /** Offset of the commit that wrote `value`. */
  version: bigint | null;
  /** Offset of the last commit the answer reflects. */
  asOf: bigint | null;
}

/** A live subscription. Read it with `nextEvent`, and `close` it when done. */
export declare class SubscriptionHandle {
  /**
   * The offset the subscription began at when it opened, or `null` for an
   * in-memory stream. It does not change if the shard later moves.
   */
  readonly startOffset: bigint | null;
  /** The offset live delivery began at when it opened, or `null` for an in-memory stream. */
  readonly liveOffset: bigint | null;
  /**
   * The next record, or `null` once the subscription has ended.
   *
   * There is no timeout argument because a caller that wants one can race this
   * promise against a timer — but the losing `nextEvent` stays in flight and
   * will resolve with the next record, so keep the promise rather than calling
   * again.
   */
  nextEvent(): Promise<Event | null>;
  /** Release the subscription. Idempotent. */
  close(): Promise<void>;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/** A subscription across every shard of a stream. */
export declare class ShardedSubscriptionHandle {
  /** How many shards this subscription covers. */
  readonly shards: number;
  /** The next item, or `null` once every shard has ended. */
  nextEvent(): Promise<ShardEvent | null>;
  /**
   * The last offset seen from each shard, for resuming.
   *
   * Only shards that have delivered something appear. Pass it back as
   * `resume`: each listed shard continues at `offset + 1`, and the rest start
   * wherever `start` says.
   */
  positions(): Promise<ShardPositions>;
  close(): Promise<void>;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/**
 * A producer whose publishes land once, however many times they are sent.
 * Made by `client.producer()`.
 *
 * Any rejection but `PublishRefusedError` leaves the batch in doubt: the
 * sequence has not advanced and the batch may have landed. Call again with the
 * same payloads and key to settle it; a different batch is rejected without
 * being sent until then. A promise you stop waiting for (a timeout race) does
 * not cancel the batch, which is then in doubt too. A `PublishRefusedError`
 * ends the producer on that stream.
 */
export declare class Producer {
  /** The id the broker assigned this producer. */
  readonly id: bigint;
  /** Publish one payload, once. Resolves to its log offset, or `null`. */
  publish(
    tenantId: string,
    namespace: string,
    stream: string,
    payload: Buffer,
    key?: Buffer,
  ): Promise<bigint | null>;
  /**
   * Publish a batch (at least one payload), once. It takes one sequence
   * whatever its size. `key` picks the shard as for `client.publish`; without
   * one it goes to shard 0.
   */
  publishBatch(
    tenantId: string,
    namespace: string,
    stream: string,
    payloads: Buffer[],
    key?: Buffer,
  ): Promise<bigint | null>;
  /**
   * Publish several batches to shard 0, once each, under consecutive
   * sequences. After a failure, call again with the same batches to send only
   * those not yet acknowledged.
   */
  publishBatches(
    tenantId: string,
    namespace: string,
    stream: string,
    batches: Buffer[][],
  ): Promise<void>;
  /**
   * Give up on the stream's batches in doubt and number the stream from 0 under
   * a fresh producer id. **A batch given up may land twice.**
   */
  abandonInDoubt(tenantId: string, namespace: string, stream: string): Promise<void>;
  /** The id this producer's batches to `stream` go out under. */
  idFor(tenantId: string, namespace: string, stream: string): Promise<bigint>;
  /**
   * Wait for the batches already handed over, then stop. `timeoutMs` bounds the
   * wait; on a timeout it rejects and the producer stays closed. Idempotent.
   */
  close(timeoutMs?: number): Promise<void>;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/** A live cache watch. Close it when done. */
export declare class CacheWatchHandle {
  /**
   * The offset live delivery began at. Everything the broker sent before it —
   * replay, or a retained snapshot — was already reflected there.
   */
  readonly resumeOffset: bigint;
  /**
   * True when the requested start predated what compaction kept, so the watch
   * began from each key's current value instead of replaying history.
   */
  readonly resnapshot: boolean;
  /**
   * How many retained values arrive before live delivery on a retained watch,
   * so an application knows the exact moment its state is complete.
   *
   * `0n` is a definite answer — the key or prefix held nothing at join — not a
   * silence to wait through. `null` on a watch that did not ask for retained
   * delivery.
   */
  readonly retainedCount: bigint | null;
  /**
   * The `fromOffset` that resumes this watch without a gap, counting
   * everything handed out so far. Readable while a `recv` is in flight.
   */
  readonly resumeFrom: bigint;
  /** How many times this watch has followed its shard to a new owner. */
  readonly moves: bigint;
  /** How many times this watch was reopened after losing its connection. */
  readonly reconnects: bigint;
  /**
   * Why the watch ended, when it could not be reopened on another broker
   * within the reconnect deadline; otherwise `null`.
   */
  readonly error: string | null;
  /** The next item, or `null` once the watch has ended. */
  recv(): Promise<CacheWatchItem | null>;
  close(): Promise<void>;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/** A prefix watch across every shard of a cache. Close it when done. */
export declare class ShardedCacheWatchHandle {
  /** How many shards this watch covers. */
  readonly shards: number;
  /**
   * How many retained values arrive before live delivery, summed across
   * shards, on a retained watch. `0n` means the prefix held nothing on any
   * shard; `stateComplete` marks the moment the state is whole. `null` on a
   * watch that did not ask for retained delivery.
   */
  readonly retainedCount: bigint | null;
  /**
   * The next item from any shard, or `null` once every shard has ended.
   * Changes to one key arrive in write order; changes to keys on different
   * shards have no order between them, and each `offset` belongs to its own
   * shard's log.
   */
  recv(): Promise<ShardedCacheWatchItem | null>;
  /**
   * Where to resume each shard: pass it back as `resume`. Every shard is
   * listed, and one still delivering its state resumes at `0n` so no key is
   * missed.
   */
  resumeOffsets(): Promise<ShardPositions>;
  close(): Promise<void>;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/** A consumer group across every shard of a stream. */
export declare class ShardedGroupHandle {
  /** How many shards the group spans. */
  readonly shards: number;
  /**
   * Take up to `maxRecords` (32 by default) from the next shard that has any.
   * The shards are not merged into one order; each record says which shard it
   * is from.
   */
  poll(maxRecords?: number): Promise<ShardedGroupRecord[]>;
  /** Finish a record on its shard. */
  ack(record: ShardedGroupDelivery): Promise<void>;
  /** Hand a record back for redelivery at once. */
  nack(record: ShardedGroupDelivery): Promise<void>;
  /** Hand a record back to be redelivered after `delayMs`. */
  nackAfter(record: ShardedGroupDelivery, delayMs: number): Promise<void>;
  /** Keep a record's claim standing for `extendMs` from now. Resolves with how many milliseconds it now stands. */
  extend(record: ShardedGroupDelivery, extendMs: number): Promise<number>;
  /** Give up on a record: list it as a dead letter of the group. */
  deadLetter(record: ShardedGroupDelivery): Promise<void>;
}

/**
 * Returns a fresh Felix token, or a promise of one. See `Client.connect` for
 * when it is called and what counts as a failure.
 *
 * `signal` aborts when the client gives up waiting (30 s) and once the last
 * handle using the token is released: the client, its `withIdentity` clients and
 * open subscriptions. Pass it to `fetch` or anything else that takes one to stop the work.
 * A provider that ignores it still works: the client does not call it again
 * while the call is pending, and a late answer is used.
 */
export type TokenProvider = (signal: AbortSignal) => string | Promise<string>;

/** Options for `Client.connect`. Unknown keys throw a `TypeError`. */
export interface ConnectOptions {
  /** The token the client starts with. Required unless `tokenProvider` is set. */
  token?: string | null;
  /** Returns fresh tokens; see `Client.connect`. */
  tokenProvider?: TokenProvider;
  /** The TLS server name to check the broker's certificate against. Default `localhost`. */
  serverName?: string;
  /** A CA file to trust instead of the operating system's store. */
  caFile?: string;
  /** Offer the `felix/1` ALPN. */
  offerAlpn?: boolean;
  /**
   * How many times a call tries to reach a broker before it gives up; each
   * attempt tries every known endpoint. A whole number of at least 1. Default 5.
   */
  reconnectAttempts?: number;
  /**
   * A ceiling in milliseconds on one call, across every attempt and every wait
   * between them. Default none. A deadline shorter than one attempt's own
   * timeout leaves the call a single attempt, since that attempt spends the
   * whole budget. `withIdentity` clients inherit it.
   */
  reconnectDeadline?: number;
  /**
   * The ceiling in milliseconds of the first wait between attempts. It doubles
   * each attempt up to `reconnectMaxBackoff`, and each wait is a random
   * fraction of its ceiling. At least 1. Default 200.
   */
  reconnectBackoff?: number;
  /**
   * The longest any one wait can be, in milliseconds. At least 1 and not below
   * `reconnectBackoff`. Default 2000.
   */
  reconnectMaxBackoff?: number;
}

/** Options for `Client.withIdentity`. Unknown keys throw a `TypeError`. */
export interface IdentityOptions {
  token?: string | null;
  tokenProvider?: TokenProvider;
}

/** A connected Felix client. */
export declare class Client {
  /**
   * Connect to a cluster.
   *
   * Any reachable seed address is enough; the client discovers the rest. A
   * `host:port` name is resolved again whenever no broker the client knows
   * answers. A name that does not resolve now is an `InvalidArg` error.
   * TLS is not optional — QUIC has no unencrypted mode. Pass `caFile` to trust
   * a specific CA (what a self-signed development broker needs), or omit it to
   * use the operating system's trust store.
   *
   * The options go in the last argument: `{ token, tokenProvider, serverName,
   * caFile, offerAlpn, reconnectAttempts, reconnectDeadline, reconnectBackoff, reconnectMaxBackoff }`. `offerAlpn` offers the `felix/1` ALPN. A broker with
   * `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker older
   * than ALPN support refuses them, which is why it is off by default.
   *
   * Pass `token`, a `tokenProvider`, or both. A client with a `tokenProvider`
   * asks it for a token only when a stream is about to authenticate and the
   * client holds none, or one past two thirds of its life (its JWT `exp`), or
   * one the broker just refused. An idle client makes no calls. It is called
   * on the JS thread, one call at a time; return the token or a promise of it.
   * A throw, a rejection, a result that is not a non-empty string, or no
   * answer within 30 s is a failed fetch: the client keeps a token that is
   * still valid and retries a few seconds later, and otherwise the call fails
   * with an `AuthError` carrying your message. After a timeout the provider is
   * not called again until the pending call settles; a late answer is used. The
   * provider receives an `AbortSignal` that aborts at that timeout and once
   * the last handle using the token is released (the client, its `withIdentity`
   * clients, open subscriptions); pass it to `fetch` to stop the request. `token`
   * is what the client starts with; without it the first stream authentication calls the
   * provider. The provider does not keep the Node process alive.
   */
  static connect(
    addrs: string | string[],
    tenantId: string,
    options?: ConnectOptions,
  ): Promise<Client>;

  /**
   * A client that acts as another user over this client's connections.
   *
   * Every stream it opens authenticates with `tenantId` and `options.token`, and the
   * broker checks its requests against that token's grants alone. It follows
   * shards and failovers as this client does, reaching each broker over this
   * client's connection to it, so a gateway serving many users holds one
   * connection per broker. Pass a fixed `options.token`, or an `options.tokenProvider` that
   * behaves as for `connect` (`token` then only seeds it). A refused token
   * rejects here.
   */
  withIdentity(tenantId: string, options?: IdentityOptions): Promise<Client>;

  /**
   * Publish one record.
   *
   * `key` is the routing key, and it decides the shard. Without one every
   * record lands on shard 0, so a multi-shard stream behaves like a
   * single-shard one. Records sharing a key stay ordered with respect to each
   * other; records with different keys do not.
   *
   * `atLeastOnce` **may duplicate the record.** By default a publish that
   * fails after the broker may already have written it is reported, not
   * re-sent, because nothing downstream can tell the copies apart. With
   * `atLeastOnce` it is re-sent to another broker instead: the record is then
   * certain to land, and may land twice. That is a delivery guarantee you
   * choose, never one this client assumes. With a `key` the record is re-sent
   * to the key's shard.
   *
   * Resolves to the record's log offset, or `null` when the broker
   * acknowledged before writing it, the stream has no log, the broker is too
   * old to say, or `ack` is `"none"`.
   */
  publish(
    tenantId: string,
    namespace: string,
    stream: string,
    payload: Buffer,
    key?: Buffer,
    ack?: AckMode,
    atLeastOnce?: boolean,
  ): Promise<bigint | null>;

  /**
   * Publish `payloads` (at least one) as one batch to the named shard, rather
   * than the shard a key would hash to. A shard the stream does not have is
   * refused with `invalid_request` and nothing is written; a broker that does
   * not advertise explicit shards rejects the call without sending anything.
   * Resolves to the batch's first log offset, or `null` as `publish` does.
   */
  publishOnShard(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    payloads: Buffer[],
    ack?: AckMode,
  ): Promise<bigint | null>;

  /**
   * Ask the broker for a producer id and return a `Producer` that sends under
   * it. Its publishes land once, however many times they are sent.
   */
  producer(): Promise<Producer>;

  /**
   * Subscribe to a stream.
   *
   * `start` is the first record you have *not* seen, so a resuming client
   * passes the offset it last handled plus one.
   *
   * A subscription reads **one shard**: `shard`, which is shard 0 by default.
   * `subscribeSharded` is what reads the whole stream.
   */
  subscribe(
    tenantId: string,
    namespace: string,
    stream: string,
    start?: StartPosition,
    shard?: number,
  ): Promise<SubscriptionHandle>;

  /**
   * Subscribe to **every** shard of a stream and merge them.
   *
   * Per-shard ordering only — that is all a sharded stream has. A lost shard
   * does not disturb the others, and resumes from its own offset.
   *
   * `resume` is a `positions()` result. Offsets are per shard, so resuming is
   * a map rather than a number: one number carried across shards replays on
   * all but one of them.
   */
  subscribeSharded(
    tenantId: string,
    namespace: string,
    stream: string,
    start?: StartPosition,
    resume?: ShardPositions,
  ): Promise<ShardedSubscriptionHandle>;

  /**
   * How many shards a stream was placed with.
   *
   * `0` means the broker knows nothing of the stream, which is deliberately
   * not `1`: told "one shard" for a stream that does not exist, a consumer
   * would read shard 0, report success, and find out later as missing data.
   */
  streamShards(tenantId: string, namespace: string, stream: string): Promise<number>;

  /** Every broker this client would try, seeds included. */
  endpoints(): Promise<string[]>;

  /** Store a value, optionally with a time-to-live in seconds. */
  cachePut(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
    value: Buffer,
    ttlSeconds?: number,
  ): Promise<void>;

  /** Read a value, or `null` if the key is absent or expired. */
  cacheGet(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
  ): Promise<Buffer | null>;

  /**
   * Remove a key, returning what it held, or `null` if it held nothing.
   *
   * The previous value is the return rather than a discard: it is what lets a
   * caller tell "I deleted something" from "it was already gone" without a
   * second round trip.
   */
  cacheDelete(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
  ): Promise<Buffer | null>;

  /**
   * Commit `ops` as one record on the shard `entityKey` routes to. Every
   * reader sees all of it or none of it. Rejects with `EventCountError` or
   * `NotOnOwningShardError` before anything is sent when `ops` does not carry
   * exactly one event or names two streams. See docs/atomic-commit.md.
   */
  commit(
    tenantId: string,
    namespace: string,
    entityKey: Buffer,
    ops: CommitOp[],
  ): Promise<CommitReceipt>;

  /** `key` in the state of `stream`'s shard that `entityKey` routes to. */
  stateGet(
    tenantId: string,
    namespace: string,
    stream: string,
    entityKey: Buffer,
    key: string,
  ): Promise<StateValue>;

  /** Read a value with its version, or `null` if the key is absent or expired. */
  cacheGetVersioned(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
  ): Promise<VersionedValue | null>;

  /**
   * Store a value only if the key's entry meets `condition`. A refusal
   * resolves with `applied: false` and the key's current version. Never sent
   * twice: a lost answer rejects with an error (possibly `ConnectionError`),
   * so read the key back to learn whether the write landed.
   */
  cachePutIf(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
    value: Buffer,
    condition: CacheCondition,
    ttlSeconds?: number,
  ): Promise<CacheConditionResult>;

  /** Remove a key only if its version is `version`. */
  cacheDeleteIf(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
    version: bigint,
  ): Promise<CacheConditionResult>;

  /**
   * Commit `ops` only if the shard's next offset is `expectedOffset`, a
   * compare-and-set on the whole shard. A refusal resolves with the shard's
   * `tail`.
   */
  commitIf(
    tenantId: string,
    namespace: string,
    entityKey: Buffer,
    ops: CommitOp[],
    expectedOffset: bigint,
  ): Promise<ConditionalWrite>;

  /** `commit`, to the named shard instead of the one `entityKey` hashes to. */
  commitOnShard(
    tenantId: string,
    namespace: string,
    shard: number,
    entityKey: Buffer,
    ops: CommitOp[],
  ): Promise<CommitReceipt>;

  /** `commitIf`, to the named shard. `expectedOffset` is that shard's next offset. */
  commitIfOnShard(
    tenantId: string,
    namespace: string,
    shard: number,
    entityKey: Buffer,
    ops: CommitOp[],
    expectedOffset: bigint,
  ): Promise<ConditionalWrite>;

  /** `key` in the state of the named shard of `stream`; takes no entity key. */
  stateGetOnShard(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    key: string,
  ): Promise<StateValue>;

  /**
   * Publish `payloads` (at least one) only if the shard's next offset is
   * `expectedOffset`. `key` picks the shard as a plain publish does; without
   * one the shard is 0.
   */
  publishIf(
    tenantId: string,
    namespace: string,
    stream: string,
    payloads: Buffer[],
    expectedOffset: bigint,
    key?: Buffer,
  ): Promise<ConditionalWrite>;

  /** `publishIf`, to the named shard. */
  publishIfOnShard(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    payloads: Buffer[],
    expectedOffset: bigint,
  ): Promise<ConditionalWrite>;

  /** Add to a counter and return its new value. `delta` may be negative. */
  counterAdd(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
    delta: number,
  ): Promise<number>;

  /**
   * Read a counter's current value, or `null` if it does not exist.
   *
   * Absent is not zero: a counter nobody has added to has never been written,
   * and the distinction is the caller's to make.
   */
  counterGet(
    tenantId: string,
    namespace: string,
    cache: string,
    key: string,
  ): Promise<number | null>;

  /**
   * Watch one cache for changes.
   *
   * Exactly one of `key` or `prefix` selects what to watch; a `prefix` of `""`
   * is every key in the shard.
   *
   * `start` is the first cache-log offset you have *not* seen, so a resuming
   * watcher passes the offset it last handled plus one. `retained` instead
   * delivers each matching key's current value first and then live changes —
   * join a room and immediately hold the roster. The two are mutually
   * exclusive, and asking for both is refused rather than resolved.
   */
  watchCache(
    tenantId: string,
    namespace: string,
    cache: string,
    key?: string,
    prefix?: string,
    start?: bigint,
    retained?: boolean,
  ): Promise<CacheWatchHandle>;

  /**
   * Claim up to `maxRecords` from a consumer group, waiting up to `waitMs` for
   * one to appear.
   *
   * Each record stays claimed until acked or the visibility timeout lapses, at
   * which point it is handed to someone else — which is why `attempts` is
   * worth reading. An empty array is an answer, not a failure: the group is
   * owed nothing right now.
   */
  groupPoll(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    maxRecords?: number,
    waitMs?: number,
    /**
     * This member's name, stable across its restarts, 1 to 128 bytes. With
     * `reclaim`, the records a previous process under the same name still
     * held come back first, instead of after the visibility timeout. Only a
     * connection's first such poll reclaims, so it is safe to leave set.
     */
    consumer?: string,
    reclaim?: boolean,
    /**
     * How long this poll's claims stand before the records are owed to the
     * group again, in milliseconds (1 to a year). The broker's own visibility
     * timeout when absent; the broker caps it, and a broker that cannot honour
     * it refuses the poll.
     */
    visibilityMs?: number,
  ): Promise<GroupRecord[]>;

  /** Finish a record: it will not be handed out again. */
  groupAck(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    offset: bigint,
  ): Promise<void>;

  /**
   * Return a record for redelivery without waiting out its timeout. Pass the
   * record `groupPoll` returned: it names that delivery, and is refused once
   * its claim has lapsed or the record has gone out again.
   */
  groupNack(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    record: GroupDelivery,
  ): Promise<void>;

  /** Offsets this group gave up on after exhausting their attempts. */
  groupDeadLetters(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
  ): Promise<bigint[]>;

  /** Drop a dead-lettered record permanently. */
  groupDiscard(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    offset: bigint,
  ): Promise<void>;

  /** Put a dead-lettered record back into the group for another attempt. */
  groupRedrive(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    offset: bigint,
  ): Promise<void>;

  /**
   * Keep the claim on `record` standing for `extendMs` from now. Resolves with
   * how many milliseconds it now stands, which is less than asked when the
   * broker capped it. Pass the record `groupPoll` returned: it names that
   * delivery, and is refused once the claim has lapsed or the record has gone
   * out again, even to this consumer.
   */
  groupExtend(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    record: GroupDelivery,
    extendMs: number,
  ): Promise<number>;

  /**
   * Move a group's cursor on one shard to `start`. Records the group had
   * handed out are void and delivered again from the new position; dead
   * letters are kept. Needs the `group.manage` permission.
   */
  groupSeek(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    start: StartPosition,
  ): Promise<GroupPosition>;

  /**
   * Create a group on one shard at `start`, so its first poll begins there. A
   * group that already exists is left where it is and `moved` is `false`, so
   * this is safe to call on every start. Needs the `group.manage` permission.
   */
  groupCreate(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    start: StartPosition,
  ): Promise<GroupPosition>;

  /** Where a group stands on one shard. */
  groupDescribe(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
  ): Promise<GroupInfo>;

  /**
   * Delete a group on one shard: its cursor, its dead letters and the records
   * it has handed out. Resolves with whether there was anything to delete.
   * Needs the `group.manage` permission.
   */
  groupDelete(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
  ): Promise<boolean>;

  /**
   * Hand `record` back to be redelivered once `delayMs` has passed rather than
   * at once, for a consumer that backs off before retrying. `0` is
   * `groupNack`. The broker caps the delay, and it is the leader's memory: a
   * failover redelivers the record sooner. Pass the record `groupPoll`
   * returned: it names that delivery, and is refused once the claim has lapsed
   * or the record has gone out again.
   */
  groupNackAfter(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    record: GroupDelivery,
    delayMs: number,
  ): Promise<void>;

  /**
   * Give up on one record: list it as a dead letter of the group and finish
   * it, as the group does once a record runs out of attempts. `groupRedrive`
   * puts it back. Refused for a record already finished.
   */
  groupDeadLetter(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    offset: bigint,
  ): Promise<void>;

  /**
   * `groupSeek` on every shard of the stream, in shard order. Resolves with
   * each shard's position, indexed by shard.
   *
   * One request per shard, not one cut across them: `"latest"` is each
   * shard's tail when its own seek lands. An offset is refused for a stream
   * with more than one shard, since offsets are per shard. A failure partway
   * leaves the earlier shards moved; the call is safe to repeat. Needs the
   * `group.manage` permission.
   */
  groupSeekStream(
    tenantId: string,
    namespace: string,
    stream: string,
    group: string,
    start: StartPosition,
  ): Promise<GroupPosition[]>;

  /**
   * `groupCreate` on every shard of the stream, as `groupSeekStream` does. A
   * shard where the group already exists is left alone.
   */
  groupCreateStream(
    tenantId: string,
    namespace: string,
    stream: string,
    group: string,
    start: StartPosition,
  ): Promise<GroupPosition[]>;

  /** `groupDescribe` for every shard of the stream, indexed by shard. */
  groupDescribeStream(
    tenantId: string,
    namespace: string,
    stream: string,
    group: string,
  ): Promise<GroupInfo[]>;

  /**
   * `groupDelete` on every shard of the stream. Resolves with whether any
   * shard had anything to delete. Safe to repeat after a failure partway.
   */
  groupDeleteStream(
    tenantId: string,
    namespace: string,
    stream: string,
    group: string,
  ): Promise<boolean>;

  /**
   * A consumer group across every shard of a stream, for a consumer that wants
   * the whole stream through one handle.
   */
  groupSharded(
    tenantId: string,
    namespace: string,
    stream: string,
    group: string,
  ): Promise<ShardedGroupHandle>;

  /**
   * The first offset on one shard of a durable stream whose record was
   * appended at or after `atMicros` (microseconds since the Unix epoch), or
   * `null` when no committed record is that recent yet: subscribe at `"latest"`
   * to wait for one. A time older than every record the shard holds answers
   * with the oldest. Times are the leading broker's clock at append, so if
   * that clock stepped back the answer is near the first such record rather
   * than exactly it. Subscribe at the offset to replay from a time. Needs the
   * `stream.subscribe` permission.
   */
  offsetForTime(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    atMicros: bigint,
  ): Promise<bigint | null>;

  /**
   * One page of a durable stream shard's committed records, from `from` and
   * stopping before `end`, without subscribing. Read the next page from
   * `nextOffset`; the range is done once it reaches `end`. `maxRecords` of `0`
   * (the default) takes the broker's cap, which also caps a page's size. A
   * page that reaches what is committed comes back short or empty rather than
   * waiting. A `from` below what retention kept, or past the tail, is a
   * `CursorError`. Needs the `stream.subscribe` permission, and only the
   * shard's leader answers.
   */
  read(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    from: bigint,
    end?: bigint,
    maxRecords?: number,
  ): Promise<StreamPage>;

  /**
   * Publish `payloads` as one batch to the shard `key` hashes to, or to shard
   * 0 without a key. Retries and failures are those of `publish`. Resolves
   * with the first record's log offset, or `null` as `publish` does.
   */
  publishBatch(
    tenantId: string,
    namespace: string,
    stream: string,
    payloads: Buffer[],
    key?: Buffer,
    ack?: AckMode,
  ): Promise<bigint | null>;

  /**
   * Publish records that each carry a routing key, split by shard and sent to
   * each shard's owner. A shard's records go as one batch, in the order given,
   * accepted or refused as a unit; there is no order between shards.
   *
   * Failure is per shard and does not reject: the report says which records
   * each piece held and why a piece failed, and a failed piece does not stop
   * the others. Without `atLeastOnce` a piece that may have been written is
   * not sent again. With it, such a piece is re-sent after the reconnect
   * policy's backoff, so a record may land twice.
   */
  publishRecords(
    tenantId: string,
    namespace: string,
    stream: string,
    records: KeyedRecord[],
    ack?: AckMode,
    atLeastOnce?: boolean,
  ): Promise<BatchReport>;

  /**
   * Watch a key prefix across **every** shard of a cache, merged into one
   * handle. Keys sharing a prefix hash to different shards and a watch reads
   * one, so this opens one prefix watch per shard and follows each shard to its
   * owner. It fails if any shard cannot be opened.
   *
   * `resume` is a `resumeOffsets()` result: a listed shard resumes at that
   * offset and the rest watch from now. `retained` instead delivers each
   * matching key's current value first, from every shard; the two are mutually
   * exclusive.
   */
  watchCacheSharded(
    tenantId: string,
    namespace: string,
    cache: string,
    prefix: string,
    resume?: ShardPositions,
    retained?: boolean,
  ): Promise<ShardedCacheWatchHandle>;

  /**
   * Ask the broker in use which brokers a client may connect to, and add them
   * to the endpoints this client will try. Resolves with how many the cluster
   * named, which may be zero. Rejects when the broker predates discovery.
   */
  refreshTopology(): Promise<number>;

  /**
   * Live connections to each broker this client holds one for. A broker
   * starts at one connection and gains more only when its streams are
   * saturated.
   */
  connectionsPerNode(): Promise<ConnectionCount[]>;

  /**
   * Release the client. Idempotent.
   *
   * Later calls fail rather than quietly using a connection that was meant to
   * be gone. Subscriptions already handed out hold their own connection and
   * keep running — closing the client is not a way to stop them, and `close`
   * on the subscription is.
   */
  close(): void;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/** The unwrapped addon, for anyone who wants it. Errors are untyped there. */
export declare const native: unknown;
