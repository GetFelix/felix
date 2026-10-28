// Idempotent producers and named-shard publishes.
//
// The refusals are asserted as specifically as the successes. A producer that
// advanced its sequence on a failure, or a named-shard publish that fell back
// to hashing a key, would look fine until the first retry.

import assert from "node:assert/strict";
import { it } from "node:test";

import { connect, reader, scenario, skipScenarios, sleep, unique } from "../harness.mjs";

const SETTLE = 45_000;
const POLL = 250;
const PENDING = Symbol("pending");

/** `promise`'s outcome if it settles within `ms`, else `PENDING`. */
async function settleWithin(promise, ms) {
  const controller = new AbortController();
  const timer = sleep(ms, { kind: PENDING }, { signal: controller.signal, ref: false });
  timer.catch(() => {});
  try {
    return await Promise.race([
      promise.then(
        (value) => ({ kind: "value", value }),
        (error) => ({ kind: "error", error }),
      ),
      timer,
    ]);
  } finally {
    controller.abort();
  }
}

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

async function post(fixture, path, body = {}) {
  const response = await fetch(fixture.control_url + path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${path}: ${response.status}: ${text}`);
}

export default function register(ctx) {
  /** The stream's last shard, so named-shard tests never name one it lacks. */
  async function shardCount() {
    const { client, fixture } = ctx;
    const count = await client.streamShards(fixture.tenant_id, fixture.namespace, fixture.durable_stream);
    assert.ok(count >= 1, `the stream reports ${count} shards`);
    return count;
  }

  /** A named-shard publish, retried while the entry broker's routing catches up. */
  async function publishOnShard(shard, payloads) {
    const { client, fixture, felix } = ctx;
    const until = Date.now() + 30_000;
    for (;;) {
      try {
        return await client.publishOnShard(
          fixture.tenant_id,
          fixture.namespace,
          fixture.durable_stream,
          shard,
          payloads,
        );
      } catch (err) {
        if (!(err instanceof felix.ShardUnavailableError) || Date.now() >= until) throw err;
        await sleep(200);
      }
    }
  }

  it("a producer reports where each batch landed", () =>
    scenario("ack.carries_the_offset", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace } = fixture;
      const stream = fixture.durable_stream;
      const producer = await client.producer();
      let first;
      let second;
      try {
        assert.equal(typeof producer.id, "bigint");
        assert.notEqual(producer.id, 0n);
        assert.equal(await producer.idFor(tenant, namespace, stream), producer.id);
        first = await producer.publishBatch(tenant, namespace, stream, [Buffer.from("a"), Buffer.from("b")]);
        second = await producer.publish(tenant, namespace, stream, Buffer.from("c"));
      } finally {
        await producer.close();
      }
      assert.equal(typeof first, "bigint", "the broker reported no offset for an idempotent batch");
      assert.equal(second, first + 2n, `a batch after two records landed at ${second}`);

      const key = unique("offset");
      const one = await client.publish(tenant, namespace, stream, Buffer.from(`${key}-1`));
      const two = await client.publish(tenant, namespace, stream, Buffer.from(`${key}-2`));
      if (one !== null) assert.equal(two, one + 1n);
    }));

  it("a producer takes keyed batches and several batches", async () => {
    const { client, fixture } = ctx;
    const { tenant_id: tenant, namespace } = fixture;
    const stream = fixture.durable_stream;
    const key = Buffer.from(unique("keyed"));
    const payload = Buffer.from(`${key}-keyed`);
    const subscription = await client.subscribeSharded(tenant, namespace, stream);
    let landed;
    let plain;
    try {
      const producer = await client.producer();
      try {
        const offset = await producer.publishBatch(tenant, namespace, stream, [payload], key);
        assert.equal(typeof offset, "bigint");
        await producer.publishBatches(tenant, namespace, stream, [
          [Buffer.from("x"), Buffer.from("y")],
          [Buffer.from("z")],
        ]);
      } finally {
        await producer.close();
      }
      landed = await find(subscription, payload);
      const after = Buffer.from(`${key}-after`);
      await client.publish(tenant, namespace, stream, after, key);
      plain = await find(subscription, after);
    } finally {
      subscription.close();
    }
    assert.ok(landed, "the keyed batch never reached a subscriber");
    // The producer routes a key as a plain publish does.
    assert.equal(landed.shard, plain.shard);
  });

  it("a producer refuses empty input before sending", async () => {
    const { client, fixture, felix } = ctx;
    const { tenant_id: tenant, namespace } = fixture;
    const stream = fixture.durable_stream;
    const producer = await client.producer();
    try {
      await assert.rejects(producer.publishBatch(tenant, namespace, stream, []), felix.InvalidArgumentError);
      await assert.rejects(
        producer.publishBatches(tenant, namespace, stream, [[Buffer.from("a")], []]),
        felix.InvalidArgumentError,
      );
      // Nothing was numbered, so the next batch still goes through, and
      // giving up with nothing in doubt is a no-op.
      await producer.abandonInDoubt(tenant, namespace, stream);
      assert.equal(typeof (await producer.publish(tenant, namespace, stream, Buffer.from("after"))), "bigint");
    } finally {
      await producer.close();
    }
  });

  it("a closed producer refuses calls and closes again quietly", async () => {
    const { client, fixture, felix } = ctx;
    const { tenant_id: tenant, namespace } = fixture;
    const stream = fixture.durable_stream;
    const producer = await client.producer();
    await producer.publish(tenant, namespace, stream, Buffer.from("before"));
    assert.equal(producer.closed, false);
    await producer.close(5_000);
    assert.equal(producer.closed, true);
    await assert.rejects(
      producer.publish(tenant, namespace, stream, Buffer.from("late")),
      (err) => err instanceof felix.FelixError,
    );
    await producer.close();
  });

  it("await using closes a producer", async () => {
    const producer = await ctx.client.producer();
    await producer[Symbol.asyncDispose]();
    assert.equal(producer.closed, true);
  });

  it("a named shard is sent only to a broker that advertised it", () =>
    scenario("explicit_shard.negotiated", async () => {
      const { fixture, felix } = ctx;
      if (!fixture.no_commit_addrs?.length || !fixture.control_url) {
        skipScenarios(
          "explicit_shard.negotiated",
          "the fixture file has no no_commit_addrs or control_url",
        );
        return;
      }
      const bare = await connect(fixture, { addrs: fixture.no_commit_addrs });
      try {
        await assert.rejects(
          bare.publishOnShard(fixture.tenant_id, fixture.namespace, fixture.durable_stream, 0, [
            Buffer.from(unique("negotiated")),
          ]),
          (err) => err instanceof felix.FelixError,
        );
      } finally {
        bare.close();
      }
      const sent = await fetch(`${fixture.control_url}/no-commit/requests`);
      assert.equal(Number(await sent.text()), 0, "the broker was sent a named-shard publish");
    }));

  it("a shard the stream lacks is refused and nothing is written", () =>
    scenario("explicit_shard.out_of_range_is_refused", async () => {
      const { client, fixture, felix } = ctx;
      const { tenant_id: tenant, namespace } = fixture;
      const stream = fixture.durable_stream;
      const count = await shardCount();
      const key = unique("range");
      const probe = await publishOnShard(count - 1, [Buffer.from(`${key}-probe`)]);
      await assert.rejects(
        client.publishOnShard(tenant, namespace, stream, count, [Buffer.from("lost")]),
        (err) => {
          assert.ok(err instanceof felix.FelixError, `${err}`);
          assert.equal(err.code, "invalid_request");
          return true;
        },
      );
      await assert.rejects(
        client.commitOnShard(tenant, namespace, count, Buffer.from(key), [
          felix.CommitOp.publish(stream, "lost"),
        ]),
        (err) => {
          assert.ok(err instanceof felix.FelixError, `${err}`);
          assert.equal(err.code, "invalid_request");
          return true;
        },
      );
      // The shard's next offset did not move.
      if (probe !== null) {
        const again = await publishOnShard(count - 1, [Buffer.from(`${key}-again`)]);
        assert.equal(again, probe + 1n);
      }
    }));

  it("a batch on a named shard lands on that shard", async () => {
    const { client, fixture } = ctx;
    const { tenant_id: tenant, namespace } = fixture;
    const shard = (await shardCount()) - 1;
    const key = unique("onshard");
    const payloads = [Buffer.from(`${key}-1`), Buffer.from(`${key}-2`)];
    const subscription = await client.subscribeSharded(tenant, namespace, fixture.durable_stream);
    let landed;
    let first;
    try {
      first = await publishOnShard(shard, payloads);
      landed = await find(subscription, payloads[1]);
    } finally {
      subscription.close();
    }
    assert.ok(landed, "the batch never reached a subscriber");
    assert.equal(landed.shard, shard);
    if (first !== null) assert.equal(landed.event.offset, first + 1n);
  });

  it("a batch on a named shard needs a payload", async () => {
    const { client, fixture, felix } = ctx;
    await assert.rejects(
      client.publishOnShard(fixture.tenant_id, fixture.namespace, fixture.durable_stream, 0, []),
      felix.InvalidArgumentError,
    );
  });

  it("a keyed record can be sent at least once", async () => {
    const { client, fixture } = ctx;
    const { tenant_id: tenant, namespace } = fixture;
    const stream = fixture.durable_stream;
    const key = Buffer.from(unique("alo"));
    const plain = Buffer.from(`${key}-plain`);
    const resent = Buffer.from(`${key}-resent`);
    const subscription = await client.subscribeSharded(tenant, namespace, stream);
    let first;
    let second;
    try {
      await client.publish(tenant, namespace, stream, plain, key);
      await client.publish(tenant, namespace, stream, resent, key, "per_message", true);
      first = await find(subscription, plain);
      second = await find(subscription, resent);
    } finally {
      subscription.close();
    }
    assert.ok(first && second, "a record never reached a subscriber");
    // The key still decides the shard on the re-send path.
    assert.equal(second.shard, first.shard);
  });

  it(
    "an idempotent producer settles a publish cut by a dropped link",
    { timeout: 180_000 },
    () =>
      scenario("retry.idempotent_producers_re_send_ambiguous_outcomes", async () => {
        const { client: direct, fixture, felix } = ctx;
        assert.ok(fixture.link_addr && fixture.control_url, "this fixture has no link interposer");
        const step = (fixture.faults ?? []).find((fault) => fault.id === "fault.publish_through_a_dropped_link");
        assert.ok(step, "the fixture has no dropped-link step");
        const { tenant_id: tenant, namespace, link_stream: stream } = fixture;
        const stamp = `idempotent-${process.hrtime.bigint()}`;
        const payloads = Array.from({ length: step.records }, (_, index) => Buffer.from(`${stamp}-${index}`));

        const linked = await connect(fixture, { addrs: [fixture.link_addr] });
        try {
          const producer = await linked.producer();
          try {
            for (const [index, payload] of payloads.entries()) {
              if (index === step.after_records) {
                await post(fixture, "/link", { fault: step.fault, hold_ms: step.hold_ms });
              }
              const until = Date.now() + step.hold_ms + SETTLE;
              for (;;) {
                const outcome = await settleWithin(
                  producer.publish(tenant, namespace, stream, payload),
                  step.hold_ms + SETTLE,
                );
                if (outcome.kind === PENDING) assert.fail(`publish ${index} neither returned nor failed`);
                if (outcome.kind === "value") break;
                const err = outcome.error;
                assert.ok(err instanceof felix.FelixError, `publish ${index} threw ${err}`);
                assert.ok(
                  !(err instanceof felix.PublishRefusedError),
                  `publish ${index} was refused: ${err.reason}`,
                );
                // In doubt: the same call again, never a different batch.
                if (Date.now() >= until) assert.fail(`publish ${index} was never settled: ${err}`);
                await sleep(POLL);
              }
            }
          } finally {
            await producer.close(5_000).catch(() => {});
          }
        } finally {
          linked.close();
          await post(fixture, "/heal");
        }

        const seen = new Map(payloads.map((payload) => [payload.toString(), 0]));
        const check = await direct.subscribe(tenant, namespace, stream, "earliest");
        try {
          let pending = null;
          const until = Date.now() + SETTLE;
          while (Date.now() < until) {
            pending ??= check.nextEvent();
            const outcome = await settleWithin(pending, 1_000);
            if (outcome.kind === PENDING) {
              if ([...seen.values()].every((count) => count > 0)) break;
              continue;
            }
            pending = null;
            if (outcome.kind === "error") throw outcome.error;
            if (outcome.value === null) break;
            const text = outcome.value.payload.toString();
            if (seen.has(text)) seen.set(text, seen.get(text) + 1);
          }
        } finally {
          await check.close();
        }
        assert.deepEqual([...seen.values()], payloads.map(() => 1), "copies of each batch");
      }));
}
