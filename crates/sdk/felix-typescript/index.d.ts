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

/** One record handed out by a consumer group. */
export interface GroupRecord {
  /** What to pass to `groupAck` or `groupNack` to settle this record. */
  offset: bigint;
  payload: Buffer;
  /**
   * How many times this record has been handed out, this delivery included.
   * `1` is a first attempt; anything higher is a redelivery, so a consumer can
   * treat a retry differently. `0` means the broker did not report it.
   */
  attempts: number;
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
 * Exactly one of `event`, `lostError`, `recovered` and `shardMoved` is set, and
 * `shard` says which shard it concerns. A lost shard does not affect the
 * others: they keep delivering while that one is re-established, and it
 * resumes from its own last offset so nothing is skipped. A moved shard is
 * followed: its records carry on from the new owner, or a loss comes next.
 */
export interface ShardEvent {
  shard: number;
  event: Event | null;
  lostError: string | null;
  recovered: boolean | null;
  shardMoved: ShardMoved | null;
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
  /** The next item, or `null` once the watch has ended. */
  recv(): Promise<CacheWatchItem | null>;
  close(): Promise<void>;
  readonly closed: boolean;
  [Symbol.asyncDispose](): Promise<void>;
}

/** A connected Felix client. */
export declare class Client {
  /**
   * Connect to a cluster.
   *
   * Any reachable seed address is enough; the client discovers the rest.
   * TLS is not optional — QUIC has no unencrypted mode. Pass `caFile` to trust
   * a specific CA (what a self-signed development broker needs), or omit it to
   * use the operating system's trust store.
   *
   * `offerAlpn` offers the `felix/1` ALPN. A broker with
   * `FELIX_TLS_REQUIRE_ALPN=true` serves only clients that do; a broker older
   * than ALPN support refuses them, which is why it is off by default.
   */
  static connect(
    addrs: string | string[],
    tenantId: string,
    token: string,
    serverName?: string,
    caFile?: string,
    offerAlpn?: boolean,
  ): Promise<Client>;

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
   * choose, never one this client assumes — and it cannot be combined with
   * `key`, which the re-send path does not yet carry.
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
   * Subscribe to a stream.
   *
   * `start` is the first record you have *not* seen, so a resuming client
   * passes the offset it last handled plus one.
   *
   * A subscription reads **one shard**. For a multi-shard stream that is shard
   * 0; `subscribeSharded` is what reads the whole stream.
   */
  subscribe(
    tenantId: string,
    namespace: string,
    stream: string,
    start?: StartPosition,
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

  /** Return a record for redelivery without waiting out its timeout. */
  groupNack(
    tenantId: string,
    namespace: string,
    stream: string,
    shard: number,
    group: string,
    offset: bigint,
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
