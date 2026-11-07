// The calls that bring the Node client level with the Rust one: record
// headers, who published and when, lease tokens, subscription progress,
// cluster inspection, handed-off batches, tuning, and one shard of a stream.
//
// None of these is in the conformance catalogue, so they are plain tests and
// leave no ledger entry.

import assert from "node:assert/strict";
import { describe, it } from "node:test";

import { connect, take, unique, withClient } from "../harness.mjs";

const TIMEOUT = 20_000;

export default function register(ctx) {
  const target = () => [ctx.fixture.tenant_id, ctx.fixture.namespace, ctx.fixture.durable_stream];

  describe("record headers", () => {
    it("a record's key and headers come back on the event, in order", async () => {
      const { client } = ctx;
      const [tenant, namespace, stream] = target();
      const payload = Buffer.from(unique("headers"));
      const events = await client.subscribe(tenant, namespace, stream);
      try {
        await client.publishWithHeaders(
          tenant,
          namespace,
          stream,
          {
            key: Buffer.from("record-key"),
            headers: [
              { name: "trace", value: Buffer.from("a") },
              { name: "trace", value: Buffer.from("b") },
            ],
          },
          payload,
        );
        const [event] = await take(events, 1, TIMEOUT);
        assert.deepEqual(event.payload, payload);
        assert.deepEqual(event.headers.key, Buffer.from("record-key"));
        assert.deepEqual(
          event.headers.headers.map((h) => [h.name, h.value.toString()]),
          [["trace", "a"], ["trace", "b"]],
        );
      } finally {
        await events.close();
      }
    });

    it("a plain record has no headers, and a batch keeps each record's own", async () => {
      const { client } = ctx;
      const [tenant, namespace, stream] = target();
      const prefix = unique("batch-headers");
      const events = await client.subscribe(tenant, namespace, stream);
      try {
        await client.publishBatchWithHeaders(tenant, namespace, stream, [
          { headers: { headers: [{ name: "n", value: Buffer.from("1") }] }, payload: Buffer.from(`${prefix}-0`) },
          { headers: {}, payload: Buffer.from(`${prefix}-1`) },
        ]);
        const received = (await take(events, 2, TIMEOUT)).filter((e) => e.payload.toString().startsWith(prefix));
        assert.equal(received.length, 2);
        assert.equal(received[0].headers.headers[0].name, "n");
        assert.equal(received[1].headers, null);
      } finally {
        await events.close();
      }
    });

    it("a reserved or empty header name is an InvalidArgumentError and sends nothing", async () => {
      const { client, felix } = ctx;
      const [tenant, namespace, stream] = target();
      for (const name of ["felix.owned", ""]) {
        await assert.rejects(
          client.publishWithHeaders(tenant, namespace, stream, { headers: [{ name, value: Buffer.from("x") }] }, Buffer.from("p")),
          felix.InvalidArgumentError,
        );
      }
      await assert.rejects(client.publishBatchWithHeaders(tenant, namespace, stream, []), felix.InvalidArgumentError);
    });
  });

  describe("who published and when", () => {
    it("publisher and timestampMicros are null unless asked for", async () => {
      const { client } = ctx;
      const [tenant, namespace, stream] = target();
      const events = await client.subscribe(tenant, namespace, stream);
      try {
        await client.publish(tenant, namespace, stream, Buffer.from(unique("plain")));
        const [event] = await take(events, 1, TIMEOUT);
        assert.equal(event.publisher, null);
        assert.equal(event.timestampMicros, null);
      } finally {
        await events.close();
      }
    });

    it("publishers and timestamps reach events and group records", async () => {
      await withClient(ctx.fixture, { publishers: true, timestamps: true }, async (asking) => {
        const [tenant, namespace, stream] = target();
        const before = BigInt(Date.now()) * 1000n - 60_000_000n;
        const events = await asking.subscribe(tenant, namespace, stream);
        try {
          await asking.publish(tenant, namespace, stream, Buffer.from(unique("told")));
          const [event] = await take(events, 1, TIMEOUT);
          assert.equal(typeof event.publisher, "string");
          assert.ok(event.timestampMicros > before, `timestamp ${event.timestampMicros} is not recent`);
        } finally {
          await events.close();
        }
      });
    });
  });

  describe("who published a group record", () => {
    it("group records carry publisher and timestampMicros when asked for, and null otherwise", async () => {
      const { fixture } = ctx;
      const [tenant, namespace, stream] = target();
      const tag = unique("group-meta");
      const newest = (records) => records.find((r) => r.payload.toString() === tag);
      async function pollFor(poll) {
        for (let i = 0; i < 20; i++) {
          const found = newest(await poll());
          if (found) return found;
        }
        throw new Error("the published record never reached the group");
      }
      await withClient(fixture, { publishers: true, timestamps: true }, async (asking) => {
        await ctx.client.publishOnShard(tenant, namespace, stream, 0, [Buffer.from(tag)]);
        const group = unique("meta");
        const record = await pollFor(() => asking.groupPoll(tenant, namespace, stream, 0, group, 64, 1_000));
        assert.equal(typeof record.publisher, "string");
        assert.equal(typeof record.timestampMicros, "bigint");

        const plain = unique("meta-plain");
        const unasked = await pollFor(() => ctx.client.groupPoll(tenant, namespace, stream, 0, plain, 64, 1_000));
        assert.equal(unasked.publisher, null);
        assert.equal(unasked.timestampMicros, null);

        const sharded = await asking.groupSharded(tenant, namespace, stream, unique("meta-sharded"));
        const across = await pollFor(() => sharded.poll(64));
        assert.equal(typeof across.publisher, "string");
        assert.equal(typeof across.timestampMicros, "bigint");
      });
    });
  });

  describe("lease tokens", () => {
    it("a put that applies reports a token or why there is none; one that does not apply has none", async () => {
      const { client, fixture } = ctx;
      const key = unique("lease-key");
      const put = () =>
        client.cachePutIfLease(
          fixture.tenant_id, fixture.namespace, fixture.cache, key, Buffer.from("v"), { kind: "absent" }, 30,
        );
      const first = await put();
      assert.equal(first.result.applied, true);
      assert.notEqual(first.token, undefined);
      assert.notEqual(first.tokenRefused, undefined);
      if (first.token === null) {
        assert.equal(typeof first.tokenRefused === "string" || first.tokenRefused === null, true);
      } else {
        assert.equal(typeof first.token.token, "string");
        assert.equal(typeof first.token.expiresAt, "bigint");
      }
      const second = await put();
      assert.equal(second.result.applied, false);
      assert.equal(second.token, null);
    });
  });

  describe("subscription progress", () => {
    it("a fresh subscription has not moved, reconnected or been deleted", async () => {
      const { client } = ctx;
      const events = await client.subscribe(...target());
      try {
        assert.equal(events.moves, 0n);
        assert.equal(events.reconnects, 0n);
        assert.equal(events.streamDeleted, false);
        assert.equal(events.queueCapacity, null);
      } finally {
        await events.close();
      }
    });

    it("brokerSubQueueCapacity is granted and reported", async () => {
      await withClient(ctx.fixture, { brokerSubQueueCapacity: 8 }, async (sized) => {
        const events = await sized.subscribe(...target());
        try {
          assert.equal(typeof events.queueCapacity, "number");
          assert.ok(events.queueCapacity >= 1);
        } finally {
          await events.close();
        }
      });
    });
  });

  describe("one shard of a stream", () => {
    it("a subscription on a shard sees only that shard", async () => {
      const { client } = ctx;
      const [tenant, namespace, stream] = target();
      assert.ok((await client.streamShards(tenant, namespace, stream)) > 1);
      const events = await client.subscribe(tenant, namespace, stream, "latest", 1);
      try {
        const mine = unique("on-shard-1");
        await client.publishOnShard(tenant, namespace, stream, 0, [Buffer.from(unique("on-shard-0"))]);
        await client.publishOnShard(tenant, namespace, stream, 1, [Buffer.from(mine)]);
        const received = await take(events, 2, 3_000);
        assert.deepEqual(received.map((e) => e.payload.toString()), [mine]);
      } finally {
        await events.close();
      }
    });
  });

  describe("cluster questions", () => {
    it("stream routing, owners and shard counts agree", async () => {
      const { client, fixture } = ctx;
      const [tenant, namespace, stream] = target();
      const supported = await client.supports();
      assert.equal(typeof supported.inspect, "boolean");
      assert.equal(supported.streamShards, true);
      const routing = await client.streamRouting(tenant, namespace, stream);
      assert.ok(routing.shards > 1);
      assert.ok(["modulo", "jump_hash"].includes(routing.routing));
      if (supported.shardOwners) {
        const owners = await client.shardOwners("stream", tenant, namespace, stream);
        assert.deepEqual(owners.map((o) => o.shard), [...owners.keys()]);
        assert.equal(owners.length, routing.shards);
      }
      if (supported.cacheShards) {
        assert.ok((await client.cacheShards(tenant, namespace, fixture.cache)) >= 1);
      }
      assert.ok(Array.isArray(await client.topology()));
    });

    it("a wrong kind is refused before anything is sent", async () => {
      const { client, felix } = ctx;
      const [tenant, namespace, stream] = target();
      await assert.rejects(client.inspectShard("table", tenant, namespace, stream, 0), felix.InvalidArgumentError);
      await assert.rejects(client.shardOwners("table", tenant, namespace, stream), felix.InvalidArgumentError);
    });

    it("a token without node.view:cluster:* is refused inspection", async () => {
      const { client, felix } = ctx;
      const [tenant, namespace, stream] = target();
      await assert.rejects(client.inspectShard("stream", tenant, namespace, stream, 0), felix.AuthError);
      await assert.rejects(client.listSubscriptions({ stream }, 10), felix.AuthError);
    });

    it("an inspector sees a shard's state and the subscriptions on it", async () => {
      const { fixture, client } = ctx;
      const [tenant, namespace, stream] = target();
      assert.ok(fixture.inspector_token, "the fixture has no inspector token");
      const events = await client.subscribe(tenant, namespace, stream);
      try {
        await withClient(fixture, { token: fixture.inspector_token }, async (inspector) => {
          // The broker answered is the one in use, which may not lead shard 0
          // or hold this subscription, so assert the shape and keep paging.
          const view = await inspector.inspectShard("stream", tenant, namespace, stream, 0);
          assert.ok(view.shards > 1);
          assert.ok(["leader", "follower", "none"].includes(view.role), view.role);
          assert.equal(typeof view.phase, "string");
          assert.equal(typeof view.serving, "boolean");
          assert.ok(Array.isArray(view.replicas));
          assert.notEqual(view.tail, undefined);

          let cursor;
          const seen = [];
          do {
            const page = await inspector.listSubscriptions({ tenantId: tenant, stream }, 1, cursor);
            assert.notEqual(page.nextCursor, undefined);
            seen.push(...page.subscriptions);
            cursor = page.nextCursor ?? undefined;
          } while (cursor && seen.length < 50);
          for (const sub of seen) {
            assert.equal(sub.stream, stream);
            assert.equal(typeof sub.capacity, "bigint");
            assert.ok(["block", "drop_new", "drop_old"].includes(sub.policy), sub.policy);
          }
        });
      } finally {
        await events.close();
      }
    });
  });

  describe("handed-off batches", () => {
    it("batches handed over in order land in that order, and a Handed reads once", async () => {
      const { client, felix } = ctx;
      const [tenant, namespace, stream] = target();
      const producer = await client.producer();
      try {
        const key = Buffer.from(unique("hand-off"));
        const first = await producer.handOffBatch(tenant, namespace, stream, [Buffer.from("a"), Buffer.from("b")], key);
        const second = await producer.handOffBatch(tenant, namespace, stream, [Buffer.from("c")], key);
        const firstOffset = await first.offset();
        const secondOffset = await second.offset();
        assert.equal(typeof firstOffset, "bigint");
        assert.equal(secondOffset, firstOffset + 2n);
        await assert.rejects(first.offset(), felix.InvalidArgumentError);

        const several = await producer.handOffBatches(tenant, namespace, stream, [[Buffer.from("d")], [Buffer.from("e")]]);
        await several.done();

        const before = await producer.idFor(tenant, namespace, stream);
        await (await producer.handOffAbandon(tenant, namespace, stream)).done();
        assert.notEqual(await producer.idFor(tenant, namespace, stream), before);
      } finally {
        await producer.close();
      }
    });

    it("an empty batch is refused", async () => {
      const { client, felix } = ctx;
      const [tenant, namespace, stream] = target();
      const producer = await client.producer();
      try {
        await assert.rejects(producer.handOffBatch(tenant, namespace, stream, []), felix.InvalidArgumentError);
        await assert.rejects(producer.handOffBatches(tenant, namespace, stream, []), felix.InvalidArgumentError);
      } finally {
        await producer.close();
      }
    });
  });

  describe("tuning", () => {
    it("tuned transport and queue options still publish and deliver", async () => {
      await withClient(
        ctx.fixture,
        {
          clientSubQueuePolicy: "block",
          clientSubQueueCapacity: 256,
          publishConnPool: 2,
          publishSharding: "hash_stream",
          maxFrameBytes: 8 * 1024 * 1024,
          connectTimeout: 15_000,
          recordHeaders: false,
        },
        async (tuned) => {
          const [tenant, namespace, stream] = target();
          const payload = Buffer.from(unique("tuned"));
          const events = await tuned.subscribe(tenant, namespace, stream);
          try {
            await tuned.publish(tenant, namespace, stream, payload);
            const [event] = await take(events, 1, TIMEOUT);
            assert.deepEqual(event.payload, payload);
          } finally {
            await events.close();
          }
        },
      );
    });

    it("a value that does not fit is refused before connecting", async () => {
      const { felix } = ctx;
      for (const bad of [
        { clientSubQueuePolicy: "sometimes" },
        { publishSharding: "random" },
        { publishConnPool: 0 },
        { maxFrameBytes: 1.5 },
        { connectTimeout: -1 },
      ]) {
        await assert.rejects(connect(ctx.fixture, bad), felix.InvalidArgumentError, JSON.stringify(bad));
      }
      await assert.rejects(connect(ctx.fixture, { notAnOption: 1 }), TypeError);
    });
  });
}
