// Atomic commits: an event and the state it changes, as one record.
//
// The refusals are asserted as specifically as the successes. A client that
// split a two-stream commit into two writes, or sent one with no event, would
// look fine until the first failure left half of it behind.

import assert from "node:assert/strict";
import { it } from "node:test";

import { reader, scenario, unique } from "../harness.mjs";

/** The shard event carrying `payload`, or `null`. */
async function find(subscription, payload, attempts = 200) {
  const read = reader(subscription);
  for (let i = 0; i < attempts; i++) {
    const item = await read.next(15_000);
    if (item === null) return null;
    if (item.event && item.event.payload.equals(payload)) return item;
  }
  return null;
}

export default function register(ctx) {
  it("a commit is read at the offset it returns", () =>
    scenario(["commit.returns_the_offset", "commit.negotiated"], async () => {
      const { client, fixture, felix } = ctx;
      const stream = fixture.durable_stream;
      const key = unique("commit");
      const payload = Buffer.from(`${key}-placed`);
      const subscription = await client.subscribeSharded(
        fixture.tenant_id,
        fixture.namespace,
        stream,
      );
      let receipt;
      let record;
      try {
        receipt = await client.commit(fixture.tenant_id, fixture.namespace, Buffer.from(key), [
          felix.CommitOp.publish(stream, payload),
          felix.CommitOp.put(stream, key, "placed"),
        ]);
        record = await find(subscription, payload);
      } finally {
        subscription.close();
      }
      assert.equal(typeof receipt.offset, "bigint");
      assert.ok(record, "the commit's event never reached a subscriber");
      assert.equal(record.event.offset, receipt.offset);
    }));

  it("state reads back at the commit's version", () =>
    scenario("commit.state_get_returns_the_version", async () => {
      const { client, fixture, felix } = ctx;
      const stream = fixture.durable_stream;
      const key = unique("state");
      const entity = Buffer.from(key);
      const receipt = await client.commit(fixture.tenant_id, fixture.namespace, entity, [
        felix.CommitOp.enqueue(stream, "placed"),
        felix.CommitOp.put(stream, key, "placed"),
      ]);
      const state = await client.stateGet(fixture.tenant_id, fixture.namespace, stream, entity, key);
      assert.deepEqual(state.value, Buffer.from("placed"));
      assert.equal(state.version, receipt.offset);
      assert.ok(state.asOf >= receipt.offset);

      // The object literal works as well as the builder.
      await client.commit(fixture.tenant_id, fixture.namespace, entity, [
        { op: "publish", stream, payload: Buffer.from("cancelled") },
        { op: "delete", stream, key },
      ]);
      const gone = await client.stateGet(fixture.tenant_id, fixture.namespace, stream, entity, key);
      assert.equal(gone.value, null);
    }));

  it("an op on another stream is refused", () =>
    scenario("commit.another_stream_is_refused", async () => {
      const { client, fixture, felix } = ctx;
      const stream = fixture.durable_stream;
      const key = unique("split");
      await assert.rejects(
        client.commit(fixture.tenant_id, fixture.namespace, Buffer.from(key), [
          felix.CommitOp.publish(stream, "placed"),
          felix.CommitOp.put(fixture.movable_stream, key, "placed"),
        ]),
        (err) => {
          assert.ok(err instanceof felix.NotOnOwningShardError, `${err}`);
          assert.ok(err instanceof felix.CommitError);
          assert.equal(err.index, 1);
          assert.equal(err.stream, fixture.movable_stream);
          assert.equal(err.owner, stream);
          return true;
        },
      );
    }));

  it("a commit needs exactly one event", () =>
    scenario("commit.exactly_one_event", async () => {
      const { client, fixture, felix } = ctx;
      const stream = fixture.durable_stream;
      const key = unique("events");
      for (const [ops, count] of [
        [[felix.CommitOp.put(stream, key, "v")], 0],
        [[felix.CommitOp.publish(stream, "a"), felix.CommitOp.enqueue(stream, "b")], 2],
      ]) {
        await assert.rejects(
          client.commit(fixture.tenant_id, fixture.namespace, Buffer.from(key), ops),
          (err) => {
            assert.ok(err instanceof felix.EventCountError, `${err}`);
            assert.equal(err.count, count);
            return true;
          },
        );
      }
    }));
}
