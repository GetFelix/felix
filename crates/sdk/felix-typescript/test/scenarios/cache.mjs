// Cache reads, writes, counters, and watches.
//
// The watch scenarios carry the weight here. A cache watch is what makes Felix
// usable for state synchronisation rather than notification, and the parts that
// make it so — resume by offset, loss reported rather than inferred — are
// exactly the parts a client can appear to implement without implementing.

import assert from "node:assert/strict";
import { it } from "node:test";

import { reader, scenario, sleep, unique } from "../harness.mjs";

/** The next item matching `predicate`, or `null`. A lag ends the watch and is returned. */
async function waitForChange(watch, predicate, attempts = 20, timeoutMs = 3_000) {
  const read = reader(watch, "recv");
  for (let i = 0; i < attempts; i++) {
    const item = await read.next(timeoutMs);
    if (item === null) return null;
    if (item.laggedResumeFrom !== null) return item;
    if (item.change && predicate(item.change)) return item;
  }
  return null;
}

export default function register(ctx) {
  it("a cached value reads back", () =>
    scenario("cache.put_get", async () => {
      const { client, fixture } = ctx;
      const key = unique("value");
      await client.cachePut(
        fixture.tenant_id,
        fixture.namespace,
        fixture.cache,
        key,
        Buffer.from("value"),
      );
      const got = await client.cacheGet(
        fixture.tenant_id,
        fixture.namespace,
        fixture.cache,
        key,
      );
      assert.deepEqual(got, Buffer.from("value"));
    }));

  it("a missing key reads as absent, not as an error", () =>
    scenario("cache.miss_is_absent", async () => {
      // `null`, not a rejection: a miss is the most common cache outcome, and
      // forcing a try/catch around it is a poor trade.
      const { client, fixture } = ctx;
      const got = await client.cacheGet(
        fixture.tenant_id,
        fixture.namespace,
        fixture.cache,
        unique("absent"),
      );
      assert.equal(got, null);
    }));

  it("delete reports the value it removed", () =>
    scenario("cache.delete_returns_previous", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      const key = unique("doomed");
      await client.cachePut(tenant, namespace, cache, key, Buffer.from("doomed"));

      assert.deepEqual(
        await client.cacheDelete(tenant, namespace, cache, key),
        Buffer.from("doomed"),
      );
      // Deleting what is not there is an answer, not an error.
      assert.equal(await client.cacheDelete(tenant, namespace, cache, key), null);
      assert.equal(await client.cacheGet(tenant, namespace, cache, key), null);
    }));

  it("a value with a TTL stops being readable after it", () =>
    scenario("cache.ttl_expires", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      const key = unique("brief");
      await client.cachePut(tenant, namespace, cache, key, Buffer.from("brief"), 1.0);
      assert.deepEqual(
        await client.cacheGet(tenant, namespace, cache, key),
        Buffer.from("brief"),
      );

      // Expiry is lazy — the record is reclaimed at compaction, not at the
      // deadline — so what matters is that it reads as absent, not that
      // storage shrank.
      await sleep(2_000);
      assert.equal(await client.cacheGet(tenant, namespace, cache, key), null);
    }));

  it("adding to a counter returns the sum including the delta", () =>
    scenario("counter.add_returns_sum", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      const counter = unique("counter");

      assert.equal(await client.counterAdd(tenant, namespace, cache, counter, 5), 5);
      // The sum *including* this delta, in one round trip — not an add
      // followed by a read, which another writer could interleave with.
      assert.equal(await client.counterAdd(tenant, namespace, cache, counter, 3), 8);
      assert.equal(await client.counterAdd(tenant, namespace, cache, counter, -8), 0);
    }));

  it("an unwritten counter reads as absent, not zero", () =>
    scenario("counter.get_absent_is_none", async () => {
      // Absent and zero are different: a counter decremented to zero has been
      // written; one that never existed has not.
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      assert.equal(
        await client.counterGet(tenant, namespace, cache, unique("never")),
        null,
      );

      const counter = unique("zeroed");
      await client.counterAdd(tenant, namespace, cache, counter, 1);
      await client.counterAdd(tenant, namespace, cache, counter, -1);
      assert.equal(await client.counterGet(tenant, namespace, cache, counter), 0);
    }));

  it("a watch delivers matching changes and ignores the rest", () =>
    scenario("watch.delivers_changes_for_the_filter", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      const watched = unique("watched");
      const ignored = unique("other");

      const watch = await client.watchCache(tenant, namespace, cache, watched);
      let item;
      try {
        await client.cachePut(tenant, namespace, cache, ignored, Buffer.from("no"));
        await client.cachePut(tenant, namespace, cache, watched, Buffer.from("yes"));
        item = await waitForChange(watch, (change) => change.key === watched);
      } finally {
        await watch.close();
      }

      assert.ok(item !== null, "the watched key's change never arrived");
      assert.ok(item.change, "a lag ended the watch before the change arrived");
      assert.deepEqual(item.change.value, Buffer.from("yes"));
      assert.equal(item.change.key, watched, "a change for an unwatched key was delivered");
    }));

  it("a delete arrives as a change carrying no value", () =>
    scenario("watch.deletes_arrive_as_changes_without_values", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      const watched = unique("deleted");

      const watch = await client.watchCache(tenant, namespace, cache, watched);
      let item;
      try {
        // A write first, and waited for: it is what proves the watch is live.
        // Deleting the moment the handle resolves would make a missed delete
        // and a watch that had not finished registering look the same.
        await client.cachePut(tenant, namespace, cache, watched, Buffer.from("here"));
        const present = await waitForChange(watch, (change) => change.key === watched);
        assert.ok(present?.change, "the watch never went live");

        await client.cacheDelete(tenant, namespace, cache, watched);
        item = await waitForChange(
          watch,
          (change) => change.key === watched && change.value === null,
        );
      } finally {
        await watch.close();
      }

      assert.ok(item !== null, "the delete never arrived");
      // Absent rather than an empty buffer: a watcher mirroring the cache has
      // to tell 'removed' from 'set to empty'.
      assert.equal(item.change.value, null);
      assert.ok(
        !Buffer.isBuffer(item.change.value),
        "a delete arrived carrying a value, so it cannot be told from a write",
      );
    }));

  it("a watch resumes at a requested offset with no gap", () =>
    scenario("watch.resumes_from_an_offset", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace, cache } = fixture;
      const watched = unique("resumed");

      const watch = await client.watchCache(tenant, namespace, cache, watched);
      let first;
      try {
        await client.cachePut(tenant, namespace, cache, watched, Buffer.from("first"));
        first = await waitForChange(
          watch,
          (change) => change.value?.toString() === "first",
        );
      } finally {
        await watch.close();
      }
      assert.ok(first?.change, "nothing to resume from");

      // Written while nothing is watching: only a resume can see it.
      await client.cachePut(tenant, namespace, cache, watched, Buffer.from("second"));

      const resumed = await client.watchCache(
        tenant,
        namespace,
        cache,
        watched,
        undefined,
        first.change.offset + 1n,
      );
      let replayed;
      try {
        replayed = await waitForChange(
          resumed,
          (change) => change.value?.toString() === "second",
        );
      } finally {
        await resumed.close();
      }

      assert.ok(
        replayed?.change,
        "a watch resuming from the next offset did not replay the change made " +
          "while it was away — a reconnecting watcher would have to re-read the " +
          "whole cache and hope",
      );
    }));

  it("retained and start are mutually exclusive", () =>
    scenario("watch.resumes_from_an_offset", async () => {
      // Asking for both is a mistake the client should name, not resolve.
      const { client, fixture } = ctx;
      const { FelixError } = ctx.felix;
      await assert.rejects(
        client.watchCache(
          fixture.tenant_id,
          fixture.namespace,
          fixture.cache,
          unique("both"),
          undefined,
          0n,
          true,
        ),
        FelixError,
      );
    }));

  it("a watch that falls behind says so", () =>
    scenario("watch.lag_is_reported_rather_than_silent", async () => {
      // Driving a real overflow needs a watcher slower than a flood, which is
      // not a thing to do reliably in a conformance run. What this asserts is
      // that the signal is a first-class value the client hands back — carrying
      // the offset to resume from — rather than a rejection or a silent close,
      // because a filtered watch's sparse offsets mean loss cannot be inferred.
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace } = fixture;
      // The single-shard cache: a prefix watch reads one shard, so over a
      // multi-shard cache this would watch a shard the key never lands on.
      const cache = fixture.single_shard_cache;
      const prefix = unique("lag");

      const watch = await client.watchCache(tenant, namespace, cache, undefined, prefix);
      let item;
      try {
        await client.cachePut(tenant, namespace, cache, `${prefix}-a`, Buffer.from("v"));
        item = await reader(watch, "recv").next(5_000);
      } finally {
        await watch.close();
      }

      // A healthy watch yields changes; a lagging one yields the lag. Both come
      // back through the same call, which is what lets an application branch
      // rather than guess.
      assert.ok(item !== null, "the watch yielded nothing at all");
      const isChange = item.change !== null;
      const isLag = item.laggedResumeFrom !== null;
      assert.ok(
        isChange !== isLag,
        "a watch item must be exactly one of a change or a lag notice, so an " +
          `application can branch on it; got change=${isChange} lag=${isLag}`,
      );
    }));

  it("a retained watch delivers current state, then live changes", () =>
    scenario("watch.retained_delivers_state_before_changes", async () => {
      const { client, fixture } = ctx;
      const { tenant_id: tenant, namespace } = fixture;
      // A retained *prefix* watch reads one shard, so the roster has to live on
      // a cache that has only one.
      const cache = fixture.single_shard_cache;
      const prefix = `${unique("room")}:`;
      for (const member of ["alice", "bob"]) {
        await client.cachePut(tenant, namespace, cache, `${prefix}${member}`, Buffer.from("here"));
      }

      const watch = await client.watchCache(
        tenant,
        namespace,
        cache,
        undefined,
        prefix,
        undefined,
        true,
      );
      const roster = new Map();
      try {
        // The count is the point: an application knows the exact moment its
        // state is complete, and zero is a definite answer rather than a
        // silence to wait through.
        const expected = watch.retainedCount;
        assert.ok(
          typeof expected === "bigint",
          "a retained watch must report how many values to expect",
        );

        const read = reader(watch, "recv");
        for (let i = 0n; i < expected; i++) {
          const item = await read.next(10_000);
          assert.ok(item?.change, "a retained value was not delivered as a change");
          roster.set(item.change.key, item.change.value);
        }
      } finally {
        await watch.close();
      }

      assert.ok(roster.has(`${prefix}alice`));
      assert.ok(roster.has(`${prefix}bob`));
    }));
}
