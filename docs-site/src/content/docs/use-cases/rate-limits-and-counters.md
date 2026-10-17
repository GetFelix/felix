---
title: "Rate Limits and Counters"
description: "Per-user rate limits, quotas and live tallies from a durable counter that answers with the new total in one round trip."
---

An API gateway runs on twelve instances and has to allow each API key 1,000
requests a minute across all of them. A SaaS product meters how many seats
each customer has invoked this month and cuts off a free plan at its quota. A
voting page shows a running tally that every server increments. Each case
needs a number that many processes add to, that is correct when they do so at
the same time, and that the caller learns in the same breath as it changes it.

## How this is usually built

The usual tool is Redis. `INCR key` is atomic and returns the new value, and
a limiter sets an expiry on the key when the result is 1 so the window resets
itself. The pair is not atomic on its own, so careful implementations use a
short Lua script or `SET key 0 NX EX 60` followed by `INCR`. It is fast and
simple, and for a limit whose only job is to protect a backend it is a fine
answer. The caveat is durability. Redis acknowledges before it persists unless
the append-only file is set to sync every write, which most deployments do not
do, and replication is asynchronous. A restart or a failover can lose the last
second or so of increments, and the count then runs behind. For a rate limit
that is a few extra requests. For a quota that is billed, it is an under-billed customer.

A relational database is the other common choice for quotas:
`UPDATE usage SET n = n + 1 WHERE key = $1 RETURNING n`. It is durable and
transactional and the count can be joined with everything else. It is also a
row lock per increment, so a hot key serializes, and every request pays a
database round trip.

Some teams avoid the network and keep a token bucket in each process. It adds
no latency, but with twelve instances each enforcing 1,000 a minute the real
limit is twelve times that, and a load balancer that sends one key's traffic
unevenly makes it unpredictable.

## How it works on Felix

`counter_add(tenant, namespace, cache, key, delta)` appends a signed delta
to a log, the shard's owner folds the running sum, and the answer is the sum
including your delta. Incrementing and learning the result is one round trip,
so there is no read-modify-write to race. `counter_get` reads the sum, and
returns nothing for a counter that was never written, which is different from
zero.

A counter is scoped and routed like a cache key: the same
`(tenant, namespace, cache, key)` address, the same key-to-shard hash and the
same owner, though it is stored beside the cache and not in it. A counter and
a cache value with the same key are unrelated, and a cache watch does not see
counter changes. Because the key picks the shard, a hot key is served by one
owner, and many keys spread over the cache's shards.

The sum is durable. It survives a restart (rebuilt by folding the log),
compaction (deltas collapse into a checkpoint) and leader failover (the
counter log replicates with its cache shard, and the promoted replica keeps
counting from the true sum). The cache's `Leader` or `Quorum` level applies
to counter adds as to puts: on `Quorum` an add is acknowledged once a majority
of the shard's replicas hold it, and a counter get waits for a majority and
for the owner to confirm it still leads, by lease or, once `lease_free_reads` is
finalized, by a round of fences. Counters need a durable broker
(`FELIX_DURABLE_STORAGE_DIR`).

A window is a key. A fixed-window limiter uses one counter per principal per
window, so the window needs no expiry logic: when the clock moves to the next
window the key changes and the count starts from nothing.

## Example

Create a cache to hold the counters, then try it from a terminal against a
local broker:

```bash
felixctl cache create limits --shards 4 --replication 3 --consistency quorum
felixctl counter add limits user:42:2026-10-17T09:15 1
felixctl counter add limits user:42:2026-10-17T09:15 1
felixctl counter get limits user:42:2026-10-17T09:15
```

Each `add` prints the sum including the add, and `get` exits with status 5
for a counter that was never written. `felixctl` sends an add once and does
not retry it.

A fixed-window limiter in Rust:

```rust
use felix_client::Client;

struct RateLimiter<'a> {
    client: &'a Client,
    limit: i64,
    window_ms: u64,
}

impl RateLimiter<'_> {
    /// True if the request is within the limit for this window.
    async fn allow(&self, api_key: &str, now_ms: u64) -> anyhow::Result<bool> {
        let window = now_ms / self.window_ms;
        let key = format!("{api_key}:{window}");
        let used = self.client.counter_add("acme", "prod", "limits", &key, 1).await?;
        Ok(used <= self.limit)
    }
}
```

The same in Python, which wraps the same client:

```python
def allow(client, api_key, now, limit=1000, window=60):
    key = f"{api_key}:{int(now) // window}"
    used = client.counter_add("acme", "prod", "limits", key, 1)
    return used <= limit
```

A quota that counts something you can undo uses a negative delta:

```rust
// Reserve a seat; give it back if the invite is declined.
let seats = client.counter_add("acme", "prod", "usage", "customer:7:seats", 1).await?;
if seats > plan_limit {
    client.counter_add("acme", "prod", "usage", "customer:7:seats", -1).await?;
    return Err(OverQuota);
}
```

Counters are at-least-once. If the add was sent and the answer was lost, the
client returns the error and does not resend, because the add may have
landed. Retrying it yourself counts twice. A limiter can accept that, since an
extra count only tightens the limit by one. A billed quota should keep its own
idempotency key, for example by claiming it first with a
[conditional cache write](/use-cases/leases-and-locks/), and only then adding.

For a live tally that watchers see change, read the counter and write the
total into a watched cache key, or poll `counter_get`. A cache watch does not
carry counter changes.

## Compared with Redis, SQL and local buckets

Redis `INCR` is lower friction and, in a single instance, very fast. Felix's
counter costs a network round trip to the key's owner and, on a `Quorum`
cache, a wait for a majority, so for a limiter on a hot path in front of a
cheap backend, Redis or a local bucket will respond sooner. What Felix adds
is a sum that survives restart and failover without a persistence mode to get
right, and that sits in the same system, with the same tenant scoping and
permissions, as the rest of your state. If you already run Felix, that is one
fewer thing to operate. If you do not, the argument is weaker.

Against `UPDATE ... RETURNING`, a Felix counter gives up joins and
transactions across rows in exchange for no row lock and no query planning:
an add is an append to a log. Use the database when the count must change in
the same transaction as other data.

A local token bucket is the right answer when a per-instance limit is
acceptable, and it has no network cost at all. It is not a way to enforce a
single limit across a fleet, which is what a shared counter is for.

## Where it stops

A counter has no expiry and no delete. A window key is never removed, so a
per-minute limiter leaves one counter per principal per minute behind. Use
a coarser window where you can, or accept the growth. The
limiter above is a fixed window, so a client can send up to twice the limit
across a window boundary. A sliding window or token bucket needs a different
shape, and Felix has no server-side script to build one atomically.

Adds are not exactly-once. There is no multi-key transaction, so a counter and
a cache value cannot be changed together. A counter is not watchable. Counters
need a broker with durable storage.

Felix also limits its own tenants: a tenant's publish bytes and messages per
second can be capped per broker with `FELIX_TENANT_PUBLISH_BYTES_PER_SEC` and
`FELIX_TENANT_PUBLISH_MSGS_PER_SEC`. That protects the cluster from one tenant
and is a different thing from limiting your API's users. It is partial: only
publishes are limited, and the rates live in each broker's environment. See
[Multi-tenant SaaS](/use-cases/multi-tenant-saas/).

## Related

- [Counters in the cache features](/features/cache/#8-counters)
- [Rust client counters](/clients/rust/#counters)
- [Python cache and counters](/clients/python/#cache-and-counters)
- [felixctl counters](/getting-started/felixctl/#counters)
- [Consistency levels](/features/cache/#6-consistency-levels)
