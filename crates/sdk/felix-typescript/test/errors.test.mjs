// How a native error becomes a typed one. No addon and no cluster: the
// messages below are the ones `src/errors.rs` writes, and its own tests pin
// that side of the format.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
const errors = require("../errors.js");

function native(message) {
  return new Error(message);
}

test("a broker code picks the class and rides along", () => {
  const err = errors.typed(
    native(
      'FELIX_SHARD_UNAVAILABLE {"code":"shard_unavailable","detail":{"reason":"fenced"},"retry":"retry"}\n' +
        "publish failed: shard is fenced",
    ),
  );
  assert.ok(err instanceof errors.ShardUnavailableError);
  assert.ok(err instanceof errors.FelixError);
  assert.equal(err.kind, "FELIX_SHARD_UNAVAILABLE");
  assert.equal(err.code, "shard_unavailable");
  assert.equal(err.retry, "retry");
  assert.deepEqual(err.detail, { reason: "fenced" });
  assert.equal(err.retryable, true);
  assert.equal(err.message, "publish failed: shard is fenced");
});

test("an outcome-unknown error is not retryable", () => {
  const err = errors.typed(
    native('FELIX_OUTCOME_UNKNOWN {"code":"quorum_timeout","retry":"outcome_unknown"}\nquorum'),
  );
  assert.ok(err instanceof errors.OutcomeUnknownError);
  assert.equal(err.code, "quorum_timeout");
  assert.equal(err.retryable, false);
});

test("the broker's retry class decides retryable over the class default", () => {
  // `not_found` is sent as retry_after: a promoted broker may not know the
  // stream yet.
  const err = errors.typed(
    native('FELIX_NOT_FOUND {"code":"not_found","retry":"retry_after"}\nunknown stream'),
  );
  assert.ok(err instanceof errors.NotFoundError);
  assert.equal(err.retryable, true);
});

test("without a code the old prefix still types it", () => {
  const err = errors.typed(native("FELIX_AUTH: publish failed: forbidden: a: b"));
  assert.ok(err instanceof errors.AuthError);
  assert.equal(err.kind, "FELIX_AUTH");
  assert.equal(err.code, undefined);
  assert.equal(err.retry, undefined);
  assert.equal(err.detail, undefined);
  assert.equal(err.retryable, false);
  assert.equal(err.message, "publish failed: forbidden: a: b");

  const lost = errors.typed(native("FELIX_CONNECTION: connection lost"));
  assert.ok(lost instanceof errors.ConnectionError);
  assert.equal(lost.retryable, true);
});

test("anything else is left alone", () => {
  const plain = native("something else: entirely");
  assert.equal(errors.typed(plain), plain);
  const unknownKind = native("FELIX_FROM_THE_FUTURE: text");
  assert.equal(errors.typed(unknownKind), unknownKind);
  assert.equal(errors.typed(undefined), undefined);
});

test("a commit refusal carries which op was wrong", () => {
  const split = errors.typed(
    native(
      'FELIX_NOT_ON_OWNING_SHARD {"index":1,"owner":"orders","stream":"inventory"}\n' +
        "operation 1 is on stream \"inventory\"",
    ),
  );
  assert.ok(split instanceof errors.NotOnOwningShardError);
  assert.ok(split instanceof errors.CommitError);
  assert.equal(split.index, 1);
  assert.equal(split.stream, "inventory");
  assert.equal(split.owner, "orders");
  assert.equal(split.code, undefined);

  const count = errors.typed(native('FELIX_EVENT_COUNT {"count":2}\ntwo events'));
  assert.ok(count instanceof errors.EventCountError);
  assert.equal(count.count, 2);

  const old = errors.typed(native("FELIX_COMMIT: this broker does not support atomic commits"));
  assert.ok(old instanceof errors.CommitError);
  assert.equal(old.retryable, false);
});
