// Connection faults: the link to a broker drops, resets or stalls mid-publish,
// mid-subscribe and between cache calls.
//
// The steps come from the fixture, which copies them out of the catalogue, and
// the fault comes from its `/link` endpoint: an interposer between this client
// and the broker that owns `link_stream`. See the "Connection faults" section
// of `scenarios.toml` for what each scenario must show.

import assert from "node:assert/strict";
import { it } from "node:test";

import { connect, scenario, sleep } from "../harness.mjs";

// Room for the idle timeout to fire and a reconnect to land, on top of the
// fault's own hold.
const SETTLE = 45_000;
const POLL = 250;

const FAULT_IDS = [
  "fault.publish_through_a_dropped_link",
  "fault.publish_through_a_reset_link",
  "fault.publish_through_a_stalled_link",
  "fault.subscription_through_a_dropped_link",
  "fault.subscription_through_a_reset_link",
  "fault.subscription_through_a_stalled_link",
  "fault.cache_through_a_reset_link",
];

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

async function post(fixture, path, body = {}) {
  const response = await fetch(fixture.control_url + path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${path}: ${response.status}: ${text}`);
}

function inject(fixture, step) {
  return post(fixture, "/link", { fault: step.fault, hold_ms: step.hold_ms });
}

export default function register(ctx) {
  for (const id of FAULT_IDS) {
    it(id, { timeout: 180_000 }, () =>
      scenario(id, async () => {
        const { fixture } = ctx;
        assert.ok(fixture.link_addr && fixture.control_url, "this fixture has no link interposer");
        const step = (fixture.faults ?? []).find((fault) => fault.id === id);
        assert.ok(step, `the fixture has no fault step for ${id}`);

        const linked = await connect(fixture, { addrs: [fixture.link_addr] });
        try {
          if (step.during === "publish") await publishThroughAFault(ctx, step, linked);
          else if (step.during === "cache") await cacheThroughAFault(ctx, step, linked);
          else await subscribeThroughAFault(ctx, step, linked);
        } finally {
          linked.close();
          // A client that resumed elsewhere can finish inside a drop's hold,
          // and the next test's client would connect into it.
          await post(fixture, "/heal");
        }
      }));
  }
}

async function publishThroughAFault(ctx, step, linked) {
  const { client: direct, fixture, felix } = ctx;
  const { tenant_id: tenant, namespace, link_stream: stream } = fixture;
  const stamp = `${step.id}-${process.hrtime.bigint()}`;
  const publish = (target, payload) =>
    target.publish(tenant, namespace, stream, payload, undefined, "per_message");

  const acked = [];
  let healedAt = null;
  for (let index = 0; index < step.records; index++) {
    if (index === step.after_records) {
      await inject(fixture, step);
      healedAt = Date.now() + step.hold_ms;
    }
    const payload = Buffer.from(`${stamp}-${index}`);
    const outcome = await settleWithin(publish(linked, payload), step.hold_ms + SETTLE);
    if (outcome.kind === PENDING) assert.fail(`publish ${index} neither returned nor failed`);
    if (outcome.kind === "value") {
      acked.push(payload.toString());
    } else {
      assert.ok(outcome.error instanceof felix.FelixError, `publish ${index} threw ${outcome.error}`);
      if (step.fault === "stall") assert.fail(`publish ${index} failed during a stall: ${outcome.error}`);
    }
  }

  // Once the fault is over, the client has to be able to publish again.
  if (healedAt !== null) await sleep(Math.max(0, healedAt - Date.now()));
  const deadline = Date.now() + SETTLE;
  for (;;) {
    const payload = Buffer.from(`${stamp}-after`);
    try {
      await publish(linked, payload);
      acked.push(payload.toString());
      break;
    } catch (err) {
      if (Date.now() >= deadline) assert.fail(`the client never published again after the fault: ${err}`);
      await sleep(POLL);
    }
  }

  // Read back on an untouched connection: an acknowledged record that is not
  // there was lost.
  const missing = new Set(acked);
  const check = await direct.subscribe(tenant, namespace, stream, "earliest");
  try {
    let pending = null;
    const until = Date.now() + SETTLE;
    while (missing.size > 0 && Date.now() < until) {
      pending ??= check.nextEvent();
      const outcome = await settleWithin(pending, 1_000);
      if (outcome.kind === PENDING) continue;
      pending = null;
      if (outcome.kind === "error") throw outcome.error;
      if (outcome.value === null) break;
      missing.delete(outcome.value.payload.toString());
    }
  } finally {
    await check.close();
  }
  assert.equal(missing.size, 0, `${missing.size} acknowledged records were not in the stream`);
}

async function cacheThroughAFault(ctx, step, linked) {
  const { fixture } = ctx;
  const { tenant_id: tenant, namespace, cache } = fixture;
  const stamp = `${step.id}-${process.hrtime.bigint()}`;
  const counter = `${stamp}-count`;
  const call = async (label, promise) => {
    const outcome = await settleWithin(promise, step.hold_ms + SETTLE);
    if (outcome.kind === PENDING) assert.fail(`${label} neither returned nor failed`);
    return outcome;
  };

  const acked = [];
  let added = 0;
  for (let index = 0; index < step.records; index++) {
    if (index === step.after_records) await inject(fixture, step);
    const key = `${stamp}-${index}`;
    const put = await call(`put ${index}`, linked.cachePut(tenant, namespace, cache, key, Buffer.from(key)));
    if (put.kind === "value") acked.push(key);
    const add = await call(`add ${index}`, linked.counterAdd(tenant, namespace, cache, counter, 1));
    if (add.kind === "value") added += 1;
  }

  const after = `${stamp}-after`;
  const deadline = Date.now() + SETTLE;
  for (;;) {
    try {
      await linked.cachePut(tenant, namespace, cache, after, Buffer.from(after));
      acked.push(after);
      break;
    } catch (err) {
      if (Date.now() >= deadline) assert.fail(`the client never wrote to the cache again after the fault: ${err}`);
      await sleep(POLL);
    }
  }

  for (const key of acked) {
    const value = await linked.cacheGet(tenant, namespace, cache, key);
    assert.equal(value?.toString(), key, `the acknowledged put of ${key} did not read back`);
  }
  const count = Number((await linked.counterGet(tenant, namespace, cache, counter)) ?? 0);
  assert.ok(
    count >= added && count <= step.records,
    `the counter is ${count} after ${added} acknowledged adds of ${step.records}`,
  );
}

async function subscribeThroughAFault(ctx, step, linked) {
  const { client: direct, fixture, felix } = ctx;
  const { tenant_id: tenant, namespace, link_stream: stream } = fixture;
  const stamp = `${step.id}-${process.hrtime.bigint()}`;
  const payloads = Array.from({ length: step.records }, (_, index) => `${stamp}-${index}`);
  const before = payloads.slice(0, step.after_records);
  const rest = payloads.slice(step.after_records);
  const publish = (payload) =>
    direct.publish(tenant, namespace, stream, Buffer.from(payload), undefined, "per_message");

  const events = await linked.subscribe(tenant, namespace, stream);
  const received = [];
  try {
    for (const payload of before) await publish(payload);

    let injected = false;
    let deadline = Date.now() + SETTLE;
    let pending = null;
    while (received.length < payloads.length) {
      if (!injected && received.length === before.length) {
        await inject(fixture, step);
        injected = true;
        deadline = Date.now() + step.hold_ms + SETTLE;
        for (const payload of rest) await publish(payload);
      }
      if (Date.now() >= deadline) {
        assert.fail(
          `neither resumed nor reported the loss: ${received.length} of ${payloads.length} records, then nothing`,
        );
      }
      // The read stays in flight across polls rather than being abandoned,
      // which would drop the record it is about to resolve with.
      pending ??= events.nextEvent();
      const outcome = await settleWithin(pending, POLL);
      if (outcome.kind === PENDING) continue;
      pending = null;
      if (outcome.kind === "error") {
        assert.ok(outcome.error instanceof felix.FelixError, `the subscription threw ${outcome.error}`);
        if (!injected || step.fault === "stall") {
          assert.fail(`the subscription failed with no loss to report: ${outcome.error}`);
        }
        // Reported the loss, which is one of the two right answers.
        checkDelivered(received, payloads);
        return;
      }
      assert.notEqual(
        outcome.value,
        null,
        `the subscription ended cleanly after ${received.length} of ${payloads.length} records; ` +
          "a dead connection must be resumed or reported, not read as the end of the stream",
      );
      received.push(outcome.value);
    }
  } finally {
    await events.close();
  }
  checkDelivered(received, payloads);
}

/** What arrived is a prefix of what was published, at contiguous offsets. */
function checkDelivered(received, payloads) {
  const got = received.map((event) => event.payload.toString());
  assert.deepEqual(got, payloads.slice(0, got.length), "a gap, a duplicate or a reordering");
  for (let index = 1; index < received.length; index++) {
    const earlier = received[index - 1].offset;
    const later = received[index].offset;
    assert.equal(typeof later, "bigint", "a durable stream delivered a record without an offset");
    assert.equal(later, earlier + 1n, `offsets jumped: ${earlier} then ${later}`);
  }
}
