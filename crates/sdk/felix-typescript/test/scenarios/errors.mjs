// Error classification and lifecycle.
//
// The scenarios here are about what an application can *decide* from a
// failure. A client that reports everything as one error type forces callers to
// match on message text, which breaks the first time a message is reworded — so
// these assert on error identity, never on wording.

import assert from "node:assert/strict";
import { it } from "node:test";

import { scenario, sleep, startFixture, take, unique, withClient } from "../harness.mjs";

export default function register(ctx) {
  it("publishing to an unknown stream fails distinguishably", () =>
    scenario("error.unknown_stream_is_typed", async () => {
      const { client, fixture } = ctx;
      const { FelixError, ConnectionError, NotFoundError } = ctx.felix;

      const caught = await client
        .publish(
          fixture.tenant_id,
          fixture.namespace,
          fixture.missing_stream,
          Buffer.from("nowhere"),
        )
        .then(
          () => null,
          (err) => err,
        );

      assert.ok(caught instanceof FelixError, `publishing to an unknown stream resolved or threw ${caught}`);
      // Distinguishable from a transport failure, which is the decision this
      // supports: an unknown stream will not start existing because you retried.
      assert.ok(
        !(caught instanceof ConnectionError),
        "an unknown stream was reported as a connection failure, which tells " +
          "an application to retry something that cannot succeed",
      );
      // Typed either way. A broker with no assignment for the shard cannot
      // tell an unregistered stream from one not placed yet, and says the latter.
      assert.ok(["not_found", "shard_unavailable"].includes(caught.code), caught.message);
      if (caught.code === "not_found") assert.ok(caught instanceof NotFoundError);
    }));

  it("an unauthorized publish is typed, and is not retried", () =>
    scenario(
      ["error.unauthorized_is_typed", "retry.terminal_failures_are_not_retried"],
      async () => {
        // Promptly is half the assertion: a client that retried this would take
        // its full backoff budget to arrive at the same answer, so the elapsed
        // time is evidence that it did not.
        const { fixture } = ctx;
        const { AuthError } = ctx.felix;

        await withClient(fixture, { token: fixture.unauthorized_token }, async (restricted) => {
          const started = process.hrtime.bigint();
          const caught = await restricted
            .publish(
              fixture.tenant_id,
              fixture.namespace,
              fixture.durable_stream,
              Buffer.from("not allowed"),
            )
            .then(
              () => null,
              (err) => err,
            );
          const elapsed = Number(process.hrtime.bigint() - started) / 1e9;

          assert.ok(
            caught instanceof AuthError,
            `an authorization failure surfaced as ${caught?.constructor?.name}; ` +
              "an application cannot tell it from a retryable fault",
          );
          assert.equal(caught.retryable, false);
          assert.equal(caught.code, "forbidden");
          assert.equal(caught.retry, "fatal");
          assert.ok(
            elapsed < 5.0,
            `the refusal took ${elapsed.toFixed(2)}s — it was retried, and no ` +
              "amount of retrying grants a permission",
          );
        });
      },
    ));

  it("a publish refused during a move is retryable", { timeout: 300_000 }, () =>
    scenario("error.shard_unavailable_is_retryable", async () => {
      // Its own cluster: fencing drains a broker, which would move shards out
      // from under every test after this one.
      const { ShardUnavailableError } = ctx.felix;
      const own = await startFixture();
      try {
        const { fixture } = own;
        const owner = await control(fixture, "/fence");
        const stream = fixture.movable_stream;

        // Straight to the fenced owner: through another broker the answer
        // would be about the forward rather than the shard.
        const caught = await withClient(fixture, { addrs: [owner.addr] }, (client) =>
          client
            .publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from("mid-move"))
            .then(
              () => null,
              (err) => err,
            ),
        );
        assert.ok(
          caught instanceof ShardUnavailableError,
          `a refusal during a move surfaced as ${caught?.constructor?.name} ` +
            `(${caught?.code}/${caught?.retry}): ${caught?.message}`,
        );
        assert.ok(["shard_unavailable", "not_leader"].includes(caught.code));
        assert.ok(["retry", "redirect"].includes(caught.retry));
        assert.equal(caught.retryable, true);

        // Retryable means the same publish lands once the move finishes.
        await control(fixture, "/heal");
        const deadline = Date.now() + 60_000;
        await withClient(fixture, { addrs: [owner.addr] }, async (client) => {
          for (;;) {
            try {
              await client.publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from("mid-move"));
              return;
            } catch (err) {
              if (Date.now() > deadline) {
                throw new Error(`the move never finished; last refusal: ${err.message}`);
              }
              await sleep(250);
            }
          }
        });
      } finally {
        await own.stop();
      }
    }));

  it("a write no majority confirmed is outcome-unknown", { timeout: 300_000 }, () =>
    scenario("error.quorum_timeout_is_outcome_unknown", async () => {
      const { OutcomeUnknownError } = ctx.felix;
      const own = await startFixture();
      try {
        const { fixture } = own;
        const leader = await control(fixture, "/partition");

        const caught = await withClient(fixture, { addrs: [leader.addr] }, (client) =>
          client
            .publish(
              fixture.tenant_id,
              fixture.namespace,
              fixture.quorum_stream,
              Buffer.from("no majority"),
            )
            .then(
              () => null,
              (err) => err,
            ),
        );
        assert.ok(
          caught instanceof OutcomeUnknownError,
          `a write no majority confirmed surfaced as ${caught?.constructor?.name} ` +
            `(${caught?.code}/${caught?.retry}): ${caught?.message}`,
        );
        // A Raft leader that steps down while the appended batch waits reports
        // leadership_lost.
        assert.ok(
          ["quorum_timeout", "leadership_lost"].includes(caught.code),
          `unexpected outcome-unknown code ${caught.code}`,
        );
        assert.equal(caught.retry, "outcome_unknown");
        assert.equal(caught.retryable, false);
      } finally {
        await own.stop();
      }
    }));

  it("closing a client or a subscription twice is safe", () =>
    scenario("client.close_is_idempotent", async () => {
      // Cleanup runs on paths that may already have cleaned up — a `finally`
      // inside an error handler, a disposal after an explicit close.
      const { client, fixture } = ctx;
      const subscription = await client.subscribe(
        fixture.tenant_id,
        fixture.namespace,
        fixture.durable_stream,
      );
      await subscription.close();
      assert.equal(subscription.closed, true);
      await subscription.close(); // must not reject
      assert.equal(subscription.closed, true);

      // And through disposal, over an already-closed subscription. Called
      // rather than written as `await using`: the syntax needs a newer Node
      // than this package asks for, and it is this method it desugars to.
      await subscription[Symbol.asyncDispose]();
      assert.equal(subscription.closed, true);

      // A client's own close is idempotent for the same reason. Its own
      // client, because closing the shared one would end the suite.
      await withClient(fixture, {}, async (own) => {
        own.close();
        assert.equal(own.closed, true);
        own.close();
        assert.equal(own.closed, true);
      });
    }));

  it("one client serves concurrent callers", () =>
    scenario("client.concurrent_calls", async () => {
      // Every publish must land, and none may corrupt another. napi runs each
      // of these on its own Tokio task, so they are genuinely in flight
      // together — which is the condition under which shared mutable state
      // would show.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("concurrent");
      const payloads = Array.from({ length: 24 }, (_, i) => `${prefix}-${i}`);

      const results = await Promise.allSettled(
        payloads.map((payload) =>
          client.publish(fixture.tenant_id, fixture.namespace, stream, Buffer.from(payload)),
        ),
      );
      const failed = results.filter((result) => result.status === "rejected");
      assert.deepEqual(
        failed.map((result) => String(result.reason)),
        [],
      );

      const events = await client.subscribe(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        "earliest",
      );
      let seen;
      try {
        seen = new Set((await take(events, 2_000, 8_000)).map((e) => e.payload.toString()));
      } finally {
        await events.close();
      }

      const missing = payloads.filter((payload) => !seen.has(payload));
      assert.deepEqual(missing, [], `${missing.length} concurrent publishes were lost`);
    }));

  it("a closed subscription reports the end rather than hanging", () =>
    scenario("client.subscription_ends_cleanly", async () => {
      // A consumer whose loop never returns after a shutdown is a hung process,
      // and the cause is invisible: nothing errors, nothing logs, the loop is
      // simply still waiting. So a closed subscription reports the end at once.
      const { client, fixture } = ctx;
      const subscription = await client.subscribe(
        fixture.tenant_id,
        fixture.namespace,
        fixture.durable_stream,
      );
      await subscription.close();

      const started = process.hrtime.bigint();
      assert.equal(await subscription.nextEvent(), null);
      const elapsed = Number(process.hrtime.bigint() - started) / 1e9;
      assert.ok(
        elapsed < 2.0,
        `a closed subscription took ${elapsed.toFixed(2)}s to report the end; a ` +
          "consumer loop would sit there rather than shutting down",
      );

      // And repeatedly, which is how a `while` loop actually reads it.
      assert.equal(await subscription.nextEvent(), null);
    }));

  it("a call on a closed client fails as a bad argument, not as a transport fault", () =>
    scenario("client.close_is_idempotent", async () => {
      // Closing is the caller's own doing, so it is not something to retry
      // against another broker — and reporting it as a connection failure
      // would say exactly that.
      const { fixture } = ctx;
      const { InvalidArgumentError } = ctx.felix;
      const client = await withClient(fixture, {}, async (own) => {
        own.close();
        return own;
      });
      await assert.rejects(
        client.cacheGet(fixture.tenant_id, fixture.namespace, fixture.cache, unique("closed")),
        InvalidArgumentError,
      );
    }));
}

/**
 * Ask the fixture for a fault, resolving to the broker to publish through. A
 * fixture that cannot produce it fails the scenario rather than passing it.
 */
async function control(fixture, path) {
  if (!fixture.control_url) {
    throw new Error("this fixture has no control endpoint, so the scenario cannot run");
  }
  const response = await fetch(fixture.control_url + path, { method: "POST" });
  const body = await response.text();
  if (!response.ok) throw new Error(`${path}: ${response.status}: ${body}`);
  return JSON.parse(body);
}
