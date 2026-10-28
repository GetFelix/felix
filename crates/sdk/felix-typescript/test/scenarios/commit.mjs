// Atomic commits: an event and the state it changes, as one record.
//
// The refusals are asserted as specifically as the successes. A client that
// split a two-stream commit into two writes, or sent one with no event, would
// look fine until the first failure left half of it behind.

import assert from "node:assert/strict";
import { it } from "node:test";

import { connect, reader, scenario, skipScenarios, unique } from "../harness.mjs";

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
    scenario("commit.returns_the_offset", async () => {
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

  it("a broker that advertised no commits is sent none", () =>
    scenario("commit.negotiated", async () => {
      const { fixture, felix } = ctx;
      if (!fixture.no_commit_addrs?.length || !fixture.control_url) {
        skipScenarios(
          "commit.negotiated",
          "the fixture file has no no_commit_addrs or control_url",
        );
        return;
      }
      const stream = fixture.durable_stream;
      const key = unique("negotiated");
      const bare = await connect(fixture, { addrs: fixture.no_commit_addrs });
      try {
        // The plain base class: a typed refusal (wrong shard, event count)
        // would mean the call reached the broker.
        await assert.rejects(
          bare.commit(fixture.tenant_id, fixture.namespace, Buffer.from(key), [
            felix.CommitOp.publish(stream, "x"),
          ]),
          (err) => err.constructor === felix.CommitError,
        );
        await assert.rejects(
          bare.stateGet(fixture.tenant_id, fixture.namespace, stream, Buffer.from(key), key),
          (err) => err.constructor === felix.CommitError,
        );
      } finally {
        bare.close();
      }
      const sent = await fetch(`${fixture.control_url}/no-commit/requests`);
      assert.equal(Number(await sent.text()), 0, "the broker was sent a commit or state read");
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

  // A write at an offset no shard has reached is refused with the tail, one at
  // the tail lands there, and the same offset again is refused past it.
  // `write(expected)` runs the form under test and returns its answer.
  async function checkConditional(what, write) {
    const stale = await write(2n ** 64n - 1n);
    assert.equal(stale.written, false, `${what} at a stale offset was written`);
    assert.equal(stale.offset, null);
    assert.equal(typeof stale.tail, "bigint");
    const tail = stale.tail;

    const landed = await write(tail);
    assert.equal(landed.written, true, `${what} at the tail ${tail} was refused`);
    assert.equal(landed.offset, tail);
    assert.equal(landed.tail, null);

    const again = await write(tail);
    assert.equal(again.written, false, `${what} again at ${tail} was written`);
    assert.ok(again.tail > landed.offset, `${what} refused with ${again.tail}, not past ${landed.offset}`);
  }

  it("a conditional commit is refused once the shard has moved on", () =>
    scenario("commit.expected_offset_refuses_a_stale_write", async () => {
      const { client, fixture, felix } = ctx;
      const stream = fixture.durable_stream;
      const { tenant_id: tenant, namespace } = fixture;
      for (const [what, shard] of [["commitIf", null], ["commitIfOnShard", 2]]) {
        const tag = unique("cond");
        const ops = [felix.CommitOp.publish(stream, tag), felix.CommitOp.put(stream, tag, "cond")];
        await checkConditional(what, (expected) =>
          shard === null
            ? client.commitIf(tenant, namespace, Buffer.from(tag), ops, expected)
            : client.commitIfOnShard(tenant, namespace, shard, Buffer.from(tag), ops, expected),
        );
      }
    }));

  it("a commit to a named shard is read back from that shard", () =>
    scenario("explicit_shard.commit_lands_on_the_named_shard", async () => {
      const { client, fixture, felix } = ctx;
      const stream = fixture.durable_stream;
      const { tenant_id: tenant, namespace } = fixture;
      const key = unique("shard");
      const receipt = await client.commitOnShard(tenant, namespace, 2, Buffer.from(key), [
        felix.CommitOp.publish(stream, `${key}-event`),
        felix.CommitOp.put(stream, key, "shard-value"),
      ]);
      assert.equal(typeof receipt.offset, "bigint");
      const state = await client.stateGetOnShard(tenant, namespace, stream, 2, key);
      assert.deepEqual(state.value, Buffer.from("shard-value"));
      assert.equal(state.version, receipt.offset);
    }));

  it("of two conditional publishes at one offset, the second is refused", () =>
    scenario(
      ["publish_if.lands_at_the_expected_offset", "publish_if.refused_with_the_tail"],
      async () => {
        const { client, fixture } = ctx;
        const stream = fixture.durable_stream;
        const { tenant_id: tenant, namespace } = fixture;
        const payloads = [Buffer.from("pi-1"), Buffer.from("pi-2")];
        const key = Buffer.from(unique("pub"));
        await checkConditional("publishIf", (expected) =>
          client.publishIf(tenant, namespace, stream, payloads, expected, key),
        );
        await checkConditional("publishIfOnShard", (expected) =>
          client.publishIfOnShard(tenant, namespace, stream, 3, payloads, expected),
        );
      },
    ));

  it("a conditional publish with no payload is refused before it is sent", async () => {
    const { client, fixture, felix } = ctx;
    await assert.rejects(
      client.publishIf(fixture.tenant_id, fixture.namespace, fixture.durable_stream, [], 0n),
      (err) => err instanceof felix.FelixError,
    );
  });
}
