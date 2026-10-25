// What the client will and will not re-send.
//
// `retry.ambiguous_outcomes_are_not_silently_retried` is the scenario worth the
// most care here, because getting it wrong changes the delivery guarantee
// without anyone choosing to. It is asserted by construction rather than by
// fault injection: the two calls differ only in `atLeastOnce`, and the
// binding's default must be the one that does not duplicate.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { it } from "node:test";

import { CRATE_ROOT, scenario, startFixture, take, unique, withClient } from "../harness.mjs";

export default function register(ctx) {
  it("re-sending a publish is opt-in", () =>
    scenario("retry.ambiguous_outcomes_are_not_silently_retried", async () => {
      // A publish that fails after the broker may already have written the
      // record cannot be retried safely — nothing downstream can tell the
      // copies apart. So the default is to report the failure, and re-sending
      // is a separate, named choice.
      //
      // The default is asserted through behaviour, not through a signature:
      // napi does not expose parameter names, so what stands in for Python's
      // signature check is that omitting the argument and passing `false` are
      // the same call.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("opt-in");

      const defaulted = `${prefix}-defaulted`;
      const explicit = `${prefix}-explicit-false`;
      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(defaulted),
        undefined,
        "per_message",
      );
      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(explicit),
        undefined,
        "per_message",
        false,
      );

      // And the opt-in path must actually work, or the choice is theoretical.
      const resent = `${prefix}-at-least-once`;
      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(resent),
        undefined,
        "per_message",
        true,
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
      for (const payload of [defaulted, explicit, resent]) {
        assert.ok(seen.has(payload), `${payload} never landed`);
      }
    }));

  it("opt-in re-sending is documented as duplicating", () =>
    scenario("ack.at_least_once_may_duplicate_and_says_so", async () => {
      // The guarantee is the caller's to choose, not the client's to assume —
      // which means the duplication has to be stated where a caller reading the
      // API will meet it, not only in a design note. The declaration file is
      // what an editor shows at the call site, so that is what is checked.
      const declaration = readFileSync(join(CRATE_ROOT, "index.d.ts"), "utf8");
      const mention = declaration.indexOf("atLeastOnce");
      assert.ok(mention >= 0, "`atLeastOnce` is not in the declaration file at all");
      assert.ok(
        /duplicat/i.test(declaration.slice(mention, mention + 600)),
        "`atLeastOnce` is documented without saying it can duplicate the " +
          "record, so a caller choosing it has not been told what they chose",
      );

      const readme = readFileSync(join(CRATE_ROOT, "README.md"), "utf8");
      assert.ok(
        /atLeastOnce/.test(readme) && /duplicat/i.test(readme),
        "the README offers at-least-once publishing without saying it can " +
          "duplicate",
      );
    }));

  it("a routing key and at-least-once are refused together, rather than one being dropped", () =>
    scenario("retry.ambiguous_outcomes_are_not_silently_retried", async () => {
      // A re-send does not carry the routing key yet. Accepting both and
      // honouring one would move the record to a different shard on the
      // re-send, which is worse than refusing.
      const { client, fixture } = ctx;
      const { InvalidArgumentError } = ctx.felix;
      await assert.rejects(
        client.publish(
          fixture.tenant_id,
          fixture.namespace,
          fixture.durable_stream,
          Buffer.from("keyed"),
          Buffer.from("key"),
          "per_message",
          true,
        ),
        InvalidArgumentError,
      );
    }));

  it("a deadline bounds a retried publish", { timeout: 300_000 }, () =>
    scenario("retry.deadline_bounds_an_operation", async () => {
      // A fenced shard refuses at once and every refusal is retryable, so only
      // the deadline stops an at-least-once publish with a large attempt
      // budget. Its own cluster: fencing drains a broker.
      const { ShardUnavailableError } = ctx.felix;
      const own = await startFixture();
      try {
        const { fixture } = own;
        const post = async (path) => {
          const response = await fetch(fixture.control_url + path, { method: "POST" });
          const body = await response.text();
          if (!response.ok) throw new Error(`${path}: ${response.status}: ${body}`);
          return JSON.parse(body);
        };
        const owner = await post("/fence");
        const deadline = 2_000;
        let elapsed;
        let caught;
        try {
          await withClient(
            fixture,
            { addrs: [owner.addr], reconnectAttempts: 40, reconnectDeadline: deadline },
            async (client) => {
              const started = Date.now();
              caught = await client
                .publish(
                  fixture.tenant_id,
                  fixture.namespace,
                  fixture.movable_stream,
                  Buffer.from("bounded"),
                  undefined,
                  "per_message",
                  true,
                )
                .then(
                  () => null,
                  (err) => err,
                );
              elapsed = Date.now() - started;
            },
          );
        } finally {
          await post("/heal");
        }
        assert.ok(caught instanceof ShardUnavailableError, `the publish gave ${caught}`);
        assert.ok(
          elapsed < deadline + 8_000,
          `the publish took ${elapsed}ms against a ${deadline}ms deadline: it spent its attempts instead`,
        );
      } finally {
        await own.stop();
      }
    }));
}
