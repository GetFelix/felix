// Multi-shard subscriptions.
//
// A subscription reads one shard. Consuming a whole stream means one per shard,
// each following its own shard's owner — which is why this is the client's job
// and not something the broker can do on the caller's behalf. A client that
// opens shard 0 and calls it the stream loses the rest with no error anywhere.

import assert from "node:assert/strict";
import { it } from "node:test";

import { reader, scenario, unique } from "../harness.mjs";

/**
 * A reader that separates records from shard lifecycle items.
 *
 * One reader per subscription, reused across takes: building a fresh one each
 * time would abandon the read already in flight, and the item it is about to
 * resolve with would be lost — reported here as a record the client never
 * delivered, which is a false failure of exactly the kind these tests exist to
 * rule out.
 */
function shardReader(subscription) {
  const read = reader(subscription);
  const lifecycle = [];
  return {
    lifecycle,
    /** Up to `wanted` records, or fewer if the subscription goes quiet. */
    async take(wanted, timeoutMs = 15_000) {
      const records = [];
      while (records.length < wanted) {
        const item = await read.next(timeoutMs);
        if (item === null) break;
        if (item.event) records.push(item);
        else lifecycle.push(item);
      }
      return records;
    },
  };
}

/** Publish keyed, so records hash across shards instead of all landing on 0. */
async function publishKeyed(client, fixture, stream, payloads) {
  for (const payload of payloads) {
    await client.publish(
      fixture.tenant_id,
      fixture.namespace,
      stream,
      Buffer.from(payload),
      Buffer.from(payload),
      "per_message",
    );
  }
}

export default function register(ctx) {
  it("a client can ask how many shards a stream has", () =>
    scenario("sharded.reports_the_shard_count", async () => {
      const { client, fixture } = ctx;
      const shards = await client.streamShards(
        fixture.tenant_id,
        fixture.namespace,
        fixture.durable_stream,
      );
      assert.ok(
        shards > 1,
        `the fixture stream reports ${shards} shards; the sharded scenarios ` +
          "cannot mean anything against a single-shard stream",
      );

      // Zero, not one: an unknown stream has to be distinguishable from a real
      // single-shard one, or subscribing to a stream that does not exist reads
      // shard 0 and calls it the stream.
      const absent = await client.streamShards(
        fixture.tenant_id,
        fixture.namespace,
        fixture.missing_stream,
      );
      assert.equal(
        absent,
        0,
        `an unknown stream reported ${absent} shards, so a client cannot tell ` +
          "it from a stream that really has that many",
      );
    }));

  it("a sharded subscription receives records from every shard", () =>
    scenario("sharded.delivers_every_shard", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("sharded");
      const payloads = Array.from({ length: 24 }, (_, i) => `${prefix}-${i}`);

      const subscription = await client.subscribeSharded(
        fixture.tenant_id,
        fixture.namespace,
        stream,
      );
      let records;
      try {
        assert.ok(subscription.shards > 1);
        await publishKeyed(client, fixture, stream, payloads);
        records = await shardReader(subscription).take(payloads.length);
      } finally {
        await subscription.close();
      }

      const received = new Set(records.map((record) => record.event.payload.toString()));
      const missing = payloads.filter((payload) => !received.has(payload));
      assert.deepEqual(
        missing,
        [],
        `${missing.length} of ${payloads.length} records never arrived. A ` +
          "subscription that reads one shard sees only its share, and the rest " +
          "go missing with no error.",
      );

      // More than one shard actually contributed, or this proved nothing about
      // merging.
      const contributing = new Set(records.map((record) => record.shard));
      assert.ok(
        contributing.size > 1,
        "every record came from one shard; the payloads did not spread, so this " +
          "did not exercise the merge",
      );
    }));

  it("a sharded subscription resumes each shard at its own offset", () =>
    scenario("sharded.resumes_per_shard", async () => {
      // Offsets are per shard, so resuming is a map rather than a number.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const prefix = unique("resume-sharded");
      const second = Array.from({ length: 12 }, (_, i) => `${prefix}-b${i}`);

      const subscription = await client.subscribeSharded(
        fixture.tenant_id,
        fixture.namespace,
        stream,
      );
      const first = [];
      let positions = {};
      try {
        // Publish until *every* shard has handed something over. A shard that
        // delivered nothing has no position, so it resumes wherever `start`
        // says — the live tail — and would legitimately miss records published
        // while away. That is the client behaving as specified, so a test that
        // resumed with a partial map would be asserting against its own setup
        // rather than against the resume.
        const reading = shardReader(subscription);
        for (let round = 0; round < 8; round++) {
          const batch = Array.from({ length: 12 }, (_, i) => `${prefix}-a${round}_${i}`);
          await publishKeyed(client, fixture, stream, batch);
          first.push(...batch);
          await reading.take(batch.length);
          positions = await subscription.positions();
          if (Object.keys(positions).length >= subscription.shards) break;
        }
      } finally {
        await subscription.close();
      }

      assert.equal(
        Object.keys(positions).length,
        subscription.shards,
        "some shard never delivered, so there is nothing to resume it from",
      );

      // Published while nothing is subscribed: only a correct resume sees them.
      await publishKeyed(client, fixture, stream, second);

      const resumed = await client.subscribeSharded(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        undefined,
        positions,
      );
      let records;
      try {
        records = await shardReader(resumed).take(second.length);
      } finally {
        await resumed.close();
      }

      const received = new Set(records.map((record) => record.event.payload.toString()));
      // Nothing already handled comes back: a client carrying one offset across
      // shards would replay on every shard but one.
      const replayed = first.filter((payload) => received.has(payload));
      assert.deepEqual(replayed, [], `${replayed.length} already-handled records were delivered again`);
      for (const payload of second) {
        assert.ok(received.has(payload), `resume skipped ${payload}`);
      }
    }));

  it("losing one shard is surfaced, not swallowed", () =>
    scenario("sharded.a_lost_shard_is_surfaced", async () => {
      // Losing a shard needs a broker to go away, which belongs to the
      // destructive tests and their own cluster. What this asserts is the part
      // a client owns: that a lifecycle item is tellable from a record, so a
      // consumer can branch on it at all.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const subscription = await client.subscribeSharded(
        fixture.tenant_id,
        fixture.namespace,
        stream,
      );
      let item;
      try {
        await publishKeyed(client, fixture, stream, [unique("lifecycle")]);
        item = await reader(subscription).next(15_000);
      } finally {
        await subscription.close();
      }

      assert.ok(item !== null, "a sharded subscription yielded nothing");
      assert.equal(typeof item.shard, "number", "every item must name its shard");
      const kinds = [
        item.event !== null ? "record" : null,
        item.lostError !== null ? "lost" : null,
        item.recovered !== null ? "recovered" : null,
        item.shardMoved !== null ? "moved" : null,
      ].filter(Boolean);
      assert.equal(
        kinds.length,
        1,
        `a sharded item must be exactly one of a record, a loss, a recovery or a move; ` +
          `got ${kinds.length}`,
      );
    }));
}
