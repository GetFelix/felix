// The error classes, and turning a native error into one of them.
//
// The native layer cannot set properties on its errors: napi errors carry only
// a status from napi's own fixed enum and a message. So it prefixes the message
// with a kind, and with the broker's code, retry class and detail when the
// broker sent them:
//
//   FELIX_AUTH: <text>
//   FELIX_SHARD_UNAVAILABLE {"code":"shard_unavailable","retry":"retry"}\n<text>
//
// and this file lifts that onto a typed error. Compact JSON never holds a raw
// newline, which is what makes the second form safe to split. Both halves ship
// as one package, so the prefix is an internal detail rather than something a
// caller parses. Kept apart from `index.js` so it can be tested without the
// addon.
//
// The class hierarchy mirrors the Python binding's exceptions, because the
// distinction is the same one in both languages: what an application can
// *decide* from a failure.

"use strict";

/** Retry classes under which sending the request again cannot duplicate it. */
const SAFE_TO_RETRY = new Set(["retry", "retry_after", "redirect"]);

/** Base class for every error this client raises. */
class FelixError extends Error {
  constructor(kind, message, meta = null) {
    super(message);
    this.name = new.target.name;
    /**
     * Which kind of failure this is (`FELIX_AUTH`, `FELIX_SHARD_UNAVAILABLE`,
     * ...), the same thing the class says. Always set.
     */
    this.kind = kind;
    /**
     * The broker's error code, such as `shard_unavailable`. `undefined` when
     * the broker sent none: an older broker, or a failure in the client.
     */
    this.code = meta?.code;
    /**
     * What the broker says the caller may do: `retry`, `retry_after`,
     * `redirect`, `outcome_unknown` or `fatal`. `undefined` without a code.
     */
    this.retry = meta?.retry;
    /** Extra facts, such as `reason` or `retry_after_ms`, or `undefined`. */
    this.detail = meta?.detail;
  }

  /**
   * Whether sending the same request again could succeed without applying
   * it twice.
   *
   * The broker's retry class decides when it sent one. Otherwise the class
   * does: only a connection failure, an unavailable shard and an overloaded
   * broker say yes.
   */
  get retryable() {
    if (this.retry !== undefined) return SAFE_TO_RETRY.has(this.retry);
    return this.constructor.retryableByDefault;
  }

  static get retryableByDefault() {
    return false;
  }
}

/**
 * The broker could not be reached, the connection was lost mid-call, or the
 * broker is shutting down.
 */
class ConnectionError extends FelixError {
  static get retryableByDefault() {
    return true;
  }
}

/** The token was rejected, or lacks the permission this call needs. */
class AuthError extends FelixError {}

/** The tenant, namespace, stream or cache does not exist on the broker. */
class NotFoundError extends FelixError {}

/** The requested start offset is gone — retention discarded it. */
class CursorError extends FelixError {}

/** A bad argument to this client, rather than a failure of the call. */
class InvalidArgumentError extends FelixError {}

/**
 * Nobody can serve the shard right now, typically while it moves. Nothing was
 * applied.
 */
class ShardUnavailableError extends FelixError {
  static get retryableByDefault() {
    return true;
  }
}

/** The broker is shedding load. Nothing was applied; retry after a pause. */
class OverloadedError extends FelixError {
  static get retryableByDefault() {
    return true;
  }
}

/**
 * The write may or may not have been applied. Only an idempotent request is
 * safe to send again.
 */
class OutcomeUnknownError extends FelixError {}

/** A commit was refused before it was sent. Nothing was written. */
class CommitError extends FelixError {}

/**
 * An op names a stream other than the commit's. A different stream is a
 * different log, and a commit writes one. `index` is the op, `stream` what it
 * named, `owner` the commit's stream.
 */
class NotOnOwningShardError extends CommitError {
  constructor(kind, message, meta = null) {
    super(kind, message, null);
    this.index = meta?.index;
    this.stream = meta?.stream;
    this.owner = meta?.owner;
  }
}

/** A commit carries exactly one event (publish or enqueue); this had `count`. */
class EventCountError extends CommitError {
  constructor(kind, message, meta = null) {
    super(kind, message, null);
    this.count = meta?.count;
  }
}

const CLASSES = new Map([
  ["FELIX_CONNECTION", ConnectionError],
  ["FELIX_AUTH", AuthError],
  ["FELIX_NOT_FOUND", NotFoundError],
  ["FELIX_CURSOR", CursorError],
  ["FELIX_INVALID", InvalidArgumentError],
  ["FELIX_SHARD_UNAVAILABLE", ShardUnavailableError],
  ["FELIX_OVERLOADED", OverloadedError],
  ["FELIX_OUTCOME_UNKNOWN", OutcomeUnknownError],
  ["FELIX_COMMIT", CommitError],
  ["FELIX_NOT_ON_OWNING_SHARD", NotOnOwningShardError],
  ["FELIX_EVENT_COUNT", EventCountError],
  ["FELIX_ERROR", FelixError],
]);

const PREFIX = /^(FELIX_[A-Z_]+)(?:: | (\{[^\n]*\})\n)/;

/** Lift a native error into a typed one, leaving anything else alone. */
function typed(err) {
  const message = err && typeof err.message === "string" ? err.message : "";
  const match = PREFIX.exec(message);
  const Class = match && CLASSES.get(match[1]);
  if (!Class) return err;
  let meta = null;
  if (match[2]) {
    try {
      meta = JSON.parse(match[2]);
    } catch {
      return err;
    }
  }
  const out = new Class(match[1], message.slice(match[0].length), meta);
  // Keep the native stack: it names the call that failed.
  if (err.stack) out.stack = err.stack.replace(message, out.message);
  return out;
}

module.exports = {
  FelixError,
  ConnectionError,
  AuthError,
  NotFoundError,
  CursorError,
  InvalidArgumentError,
  ShardUnavailableError,
  OverloadedError,
  OutcomeUnknownError,
  CommitError,
  NotOnOwningShardError,
  EventCountError,
  typed,
};
