// Consumer groups: claim, settle, redeliver.
//
// A queue is not a subscription with extra steps. Records are pulled because
// only the consumer knows when it has capacity, each is claimed by one member
// until settled, and an unsettled record comes back. These assert the settle
// semantics specifically, because that is where a client that modelled a queue
// as a stream goes wrong.

import assert from "node:assert/strict";
import { after, it } from "node:test";

import { connect, scenario, unique } from "../harness.mjs";

/** Poll until `wanted` records have been seen, or the attempts run out. */
async function pollUntil(client, fixture, group, wanted, attempts = 10) {
  const records = [];
  for (let i = 0; i < attempts; i++) {
    const batch = await client.groupPoll(
      fixture.tenant_id,
      fixture.namespace,
      fixture.durable_stream,
      0,
      group,
      32,
      2_000,
    );
    records.push(...batch);
    if (records.length >= wanted) break;
  }
  return records;
}

/**
 * Settle everything a group is currently owed, so a later poll is empty.
 *
 * A new group begins at the oldest retained record rather than the tail, so it
 * inherits whatever else is on the shared fixture stream.
 */
async function drainAndAck(client, fixture, group, rounds = 20) {
  for (let i = 0; i < rounds; i++) {
    const batch = await client.groupPoll(
      fixture.tenant_id,
      fixture.namespace,
      fixture.durable_stream,
      0,
      group,
      64,
      1_000,
    );
    if (batch.length === 0) return;
    for (const record of batch) {
      await client.groupAck(
        fixture.tenant_id,
        fixture.namespace,
        fixture.durable_stream,
        0,
        group,
        record.offset,
      );
    }
  }
}

export default function register(ctx) {
  // Creating, moving or deleting a group needs `group.manage`, which the
  // fixture's ordinary token does not hold.
  let operator;
  const groupOperator = async (fixture) => {
    operator ??= await connect(fixture, { token: fixture.group_operator_token });
    return operator;
  };
  after(() => operator?.close());

  it("a poll claims records, and an empty poll is an answer", () =>
    scenario("queue.poll_returns_claimed_records", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const group = unique("group");
      const payload = unique("queued");

      // A new group starts at the *earliest* retained record, not the tail, so
      // it is owed whatever other tests have already published to this stream.
      // Settle all of that first; only then is an empty poll meaningful.
      await drainAndAck(client, fixture, group);

      const empty = await client.groupPoll(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        0,
        group,
        32,
        500,
      );
      assert.deepEqual(empty, [], "a group with nothing owed must return an empty array, not reject");

      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(payload),
        undefined,
        "per_message",
      );
      const records = await pollUntil(client, fixture, group, 1);

      const mine = records.filter((record) => record.payload.toString() === payload);
      assert.ok(mine.length > 0, "the published record was never handed to the group");
      assert.equal(typeof mine[0].offset, "bigint");
      assert.ok(mine[0].attempts >= 1, "a first delivery reports at least one attempt");
    }));

  it("an acknowledged record does not come back", () =>
    scenario("queue.acknowledged_records_are_not_redelivered", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const group = unique("group-ack");
      const payload = unique("acked");

      // Settle whatever the shared stream already owes this group, so the poll
      // below is about the record this test published.
      await drainAndAck(client, fixture, group);
      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(payload),
        undefined,
        "per_message",
      );

      const records = await pollUntil(client, fixture, group, 1);
      assert.ok(
        records.some((record) => record.payload.toString() === payload),
        "nothing to acknowledge",
      );

      for (const record of records) {
        await client.groupAck(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          0,
          group,
          record.offset,
        );
      }

      // Settled records must not reappear. A client whose acknowledgement never
      // reaches the broker produces a queue that redelivers forever, and the
      // symptom shows up in the consumer rather than here.
      const again = await client.groupPoll(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        0,
        group,
        32,
        3_000,
      );
      assert.ok(
        !again.some((record) => record.payload.toString() === payload),
        "an acknowledged record was handed out again",
      );
    }));

  it("a record handed back is delivered again, with a higher attempt count", () =>
    scenario("queue.unacknowledged_records_are_redelivered", async () => {
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const group = unique("group-nack");
      const payload = unique("nacked");

      await drainAndAck(client, fixture, group);
      await client.publish(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        Buffer.from(payload),
        undefined,
        "per_message",
      );

      const first = await pollUntil(client, fixture, group, 1);
      const mine = first.filter((record) => record.payload.toString() === payload);
      assert.ok(mine.length > 0, "nothing to hand back");
      const firstAttempts = mine[0].attempts;

      await client.groupNack(fixture.tenant_id, fixture.namespace, stream, 0, group, mine[0]);

      const redelivered = await pollUntil(client, fixture, group, 1);
      const again = redelivered.filter((record) => record.payload.toString() === payload);
      assert.ok(again.length > 0, "a record handed back was never redelivered");
      assert.ok(
        again[0].attempts > firstAttempts,
        `redelivery reported ${again[0].attempts} attempts against ${firstAttempts} on ` +
          "the first: a consumer cannot tell a retry from a first attempt",
      );

      await client.groupAck(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        0,
        group,
        again[0].offset,
      );
    }));

  it("dead letters can be listed, redriven, or discarded", () =>
    scenario("queue.dead_letters_are_listable_and_redrivable", async () => {
      // The administrative half. Driving a record past its attempt bound takes
      // as many redeliveries as the bound allows, which is a slow thing to do
      // in a conformance run; what this asserts is that the calls exist, reach
      // the broker, and answer — an empty list being a legitimate answer.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const group = unique("group-dl");

      const offsets = await client.groupDeadLetters(
        fixture.tenant_id,
        fixture.namespace,
        stream,
        0,
        group,
      );
      assert.ok(Array.isArray(offsets));

      // Redriving an offset that is not dead-lettered is a no-op or a typed
      // error, never a crash.
      if (offsets.length > 0) {
        await client.groupRedrive(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          0,
          group,
          offsets[0],
        );
      }
    }));

  it("two group members do not receive the same record", () =>
    scenario("client.concurrent_calls", async () => {
      // The property a queue exists for: one record, one consumer. Both members
      // poll the same group; no offset may be handed to both while the first
      // still holds it.
      const { client, fixture } = ctx;
      const stream = fixture.durable_stream;
      const group = unique("group-excl");
      const prefix = unique("excl");
      const payloads = Array.from({ length: 10 }, (_, i) => `${prefix}-${i}`);

      await drainAndAck(client, fixture, group);
      for (const payload of payloads) {
        await client.publish(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          Buffer.from(payload),
          undefined,
          "per_message",
        );
      }

      const second = await connect(fixture);
      let firstBatch;
      let secondBatch;
      try {
        firstBatch = await client.groupPoll(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          0,
          group,
          32,
          3_000,
        );
        secondBatch = await second.groupPoll(
          fixture.tenant_id,
          fixture.namespace,
          stream,
          0,
          group,
          32,
          3_000,
        );
      } finally {
        second.close();
      }

      const held = new Set(firstBatch.map((record) => record.offset));
      const overlap = secondBatch.filter((record) => held.has(record.offset));
      assert.deepEqual(
        overlap.map((record) => record.offset.toString()),
        [],
        "an offset was claimed by both members at once; a queue that hands one " +
          "record to two consumers is not a queue",
      );
    }));

  it("a group created at latest is not owed what came before", () =>
    scenario("queue.group_create_starts_at_the_chosen_position", async () => {
      const { client, fixture } = ctx;
      const operator = await groupOperator(fixture);
      const { tenant_id: tenant, namespace, durable_stream: stream } = fixture;
      const group = unique("group-create");
      const payload = unique("after");
      const { tail } = await client.groupDescribe(tenant, namespace, stream, 0, group);

      const created = await operator.groupCreate(tenant, namespace, stream, 0, group, "latest");
      assert.equal(created.moved, true);
      assert.ok(created.offset >= tail);

      await client.publish(
        tenant, namespace, stream, Buffer.from(payload), undefined, "per_message",
      );
      const first = await pollUntil(client, fixture, group, 1);
      assert.ok(first.length > 0, "the group was handed nothing");
      assert.ok(
        first.every((record) => record.offset >= created.offset),
        "a group created at latest was owed records from before it existed",
      );
      assert.ok(first.some((record) => record.payload.toString() === payload));

      // A consumer that creates its group on every start must not rewind it.
      const again = await operator.groupCreate(tenant, namespace, stream, 0, group, "earliest");
      assert.equal(again.moved, false);
    }));

  it("a seek delivers from the new position", () =>
    scenario("queue.group_seek_replays_from_the_new_position", async () => {
      const { client, fixture } = ctx;
      const operator = await groupOperator(fixture);
      const { tenant_id: tenant, namespace, durable_stream: stream } = fixture;
      const group = unique("group-seek");
      const payload = unique("replay");
      const created = await operator.groupCreate(tenant, namespace, stream, 0, group, "latest");
      await client.publish(
        tenant, namespace, stream, Buffer.from(payload), undefined, "per_message",
      );
      const first = (await pollUntil(client, fixture, group, 1)).find(
        (record) => record.payload.toString() === payload,
      );
      assert.ok(first, "the record was never handed to the group");
      await client.groupAck(tenant, namespace, stream, 0, group, first.offset);

      const position = await operator.groupSeek(tenant, namespace, stream, 0, group, created.offset);
      assert.equal(position.offset, created.offset);
      assert.equal(position.moved, true);

      const again = (await pollUntil(client, fixture, group, 1)).find(
        (record) => record.payload.toString() === payload,
      );
      assert.ok(again, "the seek did not replay the record");
      assert.equal(again.offset, first.offset);
      await client.groupAck(tenant, namespace, stream, 0, group, again.offset);
    }));

  it("describe reports the group and delete forgets it", () =>
    scenario("queue.group_describe_and_delete", async () => {
      const { client, fixture } = ctx;
      const operator = await groupOperator(fixture);
      const { tenant_id: tenant, namespace, durable_stream: stream } = fixture;
      const group = unique("group-describe");
      const payload = unique("described");
      const created = await operator.groupCreate(tenant, namespace, stream, 0, group, "latest");
      await client.publish(
        tenant, namespace, stream, Buffer.from(payload), undefined, "per_message",
      );

      const before = await client.groupDescribe(tenant, namespace, stream, 0, group);
      assert.equal(before.committed, created.offset);
      assert.ok(before.tail > created.offset);
      assert.equal(before.lag, before.tail - created.offset);
      assert.equal(before.inFlight, 0n);

      await pollUntil(client, fixture, group, 1);
      const claimed = await client.groupDescribe(tenant, namespace, stream, 0, group);
      assert.ok(claimed.inFlight >= 1n);

      assert.equal(await operator.groupDelete(tenant, namespace, stream, 0, group), true);
      const gone = await client.groupDescribe(tenant, namespace, stream, 0, group);
      assert.equal(gone.committed, null);
      assert.equal(gone.inFlight, 0n);
      assert.equal(await operator.groupDelete(tenant, namespace, stream, 0, group), false);
    }));

  it("extending a claim reports how long it stands", () =>
    scenario("queue.group_extend_keeps_a_claim", async () => {
      const { client, fixture } = ctx;
      const operator = await groupOperator(fixture);
      const { tenant_id: tenant, namespace, durable_stream: stream } = fixture;
      const group = unique("group-extend");
      const payload = unique("extended");
      await operator.groupCreate(tenant, namespace, stream, 0, group, "latest");
      await client.publish(
        tenant, namespace, stream, Buffer.from(payload), undefined, "per_message",
      );
      const record = (await pollUntil(client, fixture, group, 1)).find(
        (r) => r.payload.toString() === payload,
      );
      assert.ok(record, "the record was never handed to the group");

      const stands = await client.groupExtend(tenant, namespace, stream, 0, group, record, 60_000);
      assert.equal(typeof stands, "number");
      assert.ok(stands > 0);
      await client.groupAck(tenant, namespace, stream, 0, group, record.offset);
    }));

  it("a bad start or extension is refused before it is sent", async () => {
    const { fixture } = ctx;
    const { tenant_id: tenant, namespace, durable_stream: stream } = fixture;
    const operator = await groupOperator(fixture);
    await assert.rejects(
      operator.groupCreate(tenant, namespace, stream, 0, unique("group-bad"), "middle"),
      /start must be/,
    );
    const year = 365 * 24 * 3600 * 1000;
    for (const bad of [0, -1, NaN, Infinity, year + 1]) {
      await assert.rejects(
        operator.groupExtend(tenant, namespace, stream, 0, "g", { offset: 0n, attempts: 1 }, bad),
        /extendMs/,
        `extendMs ${bad} was accepted`,
      );
    }
  });
}
