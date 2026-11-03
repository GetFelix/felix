---
title: "Rust: Cache Operations"
description: "Key-value reads and writes with TTL, conditional writes, leases, watches, and counters."
---

Part of the [Rust client guide](/clients/rust/). Setup and configuration are on that page.

## Cache Operations

### Put and Get

```rust
use anyhow::bail;

// tenant, namespace, cache, key, value (`Bytes`), TTL in milliseconds
client
    .cache_put("acme", "prod", "sessions", "user-abc", session_data, Some(60 * 60 * 1000))
    .await?;

let Some(data) = client.cache_get("acme", "prod", "sessions", "user-abc").await? else {
    bail!("session user-abc expired or never existed");
};
let session: Session = serde_json::from_slice(&data)?;
```

### Without TTL

```rust
// Store permanently (until evicted or restart)
client
    .cache_put("acme", "prod", "config", "app-settings", config_data, None)
    .await?;
```

### Delete

```rust
// Answers with the value that was removed, or `None` if the key was not there,
// so a caller can tell a delete that did something from one that did not.
match client.cache_delete("acme", "prod", "sessions", "user-abc").await? {
    Some(removed) => audit_log("session revoked", removed),
    None => { /* already gone, or never there */ }
}
```

Needs a broker advertising `FEATURE_CACHE_DELETE`. The client returns an error
rather than probing, because a broker that does not advertise
`FEATURE_UNSUPPORTED` ends its control loop on an unrecognised message type.

### Conditional writes

```rust
use felix_client::CacheCondition;

// Read, change, and write back only if nobody wrote in between.
loop {
    let Some(current) = client
        .cache_get_versioned("acme", "prod", "config", "endpoints")
        .await?
    else {
        break;
    };
    let edited = edit(&current.value);
    let result = client
        .cache_put_if("acme", "prod", "config", "endpoints", edited, None,
            CacheCondition::Version(current.version))
        .await?;
    if result.applied {
        break;
    }
}
```

`CacheCondition::Absent` writes only if the key has no live entry, which is
how to take a lease or claim an idempotency key. `cache_delete_if` removes a
key only at a given version. A refusal is `applied: false` with the key's
current version, not an error. Needs a broker advertising
`FEATURE_CACHE_CONDITIONAL`; `Client` has the same three calls and never
resends a conditional write after a lost answer. See
[Conditional writes](/features/cache/#9-conditional-writes).

### Leases and lease tokens

A node that holds a lease can ask for a *lease token* with the same write: a
short-lived token that grants what its lease templates name for the key, such
as publishing to `wl.w7.out` while it holds lease `w7`. The broker must have a
lease key, and the caller's token must hold the templates; see
[Lease tokens](/features/security/#lease-tokens).

```rust
use bytes::Bytes;
use felix_client::{AckMode, CacheCondition};

// Claim workload w7's lease for 30 seconds. Only a write that applies comes
// back with a token.
let claim = client
    .cache_put_if_lease("acme", "prod", "leases", "w7", Bytes::from_static(b"node-a"),
        Some(30_000), CacheCondition::Absent)
    .await?;
if !claim.result.applied {
    return Ok(()); // another node holds it
}
let lease = claim
    .token
    .with_context(|| format!("claimed without a token: {:?}", claim.token_refused))?;
let version = claim.result.version.context("an applied write has a version")?;

// Publish the workload's output as the holder, on a handle of its own.
let holder = client.with_identity_token("acme", lease.token).await?;
holder
    .publish("acme", "prod", "wl.w7.out", b"started".to_vec(), AckMode::PerMessage)
    .await?;

// Renew before the TTL runs out, at the version held. A renewal that applies
// carries a fresh token; one that does not means the lease is gone.
let renewed = client
    .cache_put_if_lease("acme", "prod", "leases", "w7", Bytes::from_static(b"node-a"),
        Some(30_000), CacheCondition::Version(version))
    .await?;
drop(holder); // its streams end at the old token's expiry anyway
if let Some(fresh) = renewed.token {
    let _holder = client.with_identity_token("acme", fresh.token).await?;
}
```

`cache_put_if_lease` fails without writing when the broker did not advertise
`FEATURE_LEASE_TOKEN`, or refuses the mint: the caller's token is itself a
lease or delegated token, holds no template naming this cache, or the key
cannot be part of a name. An applied write whose token would already be
expired comes back with `token_refused` set; the write stands. That happens
when the token the cache stream authenticated with has expired, since a lease
token never outlives it. If the key lives on another broker, the claim can
instead fail with `unauthorized` and write nothing, once that token is more
than a minute past its expiry. A connection authenticates its cache stream
once, so on a long-lived connection claim through a handle opened with a
fresh token (`with_identity`). Streams opened with a lease token end when it expires, so
switch handles on each renewal.
`LeaseToken` leaves the token out of its `Debug` output. `Connection` has the
same call.

### Watch

Subscribe to changes for one key or key prefix. Each change carries its
cache-log offset, so a watch can be resumed exactly where it left off:

```rust
use felix_client::{CacheWatchFilter, CacheWatchItem};

let mut watch = client
    .watch_cache(
        "acme",
        "prod",
        "sessions",
        CacheWatchFilter::Prefix("user:".into()),
        None,          // from now; Some(offset) resumes gaplessly
    )
    .await?;

let mut checkpoint = watch.resume_offset();
while let Some(item) = watch.recv().await {
    match item {
        CacheWatchItem::Change(change) => {
            match &change.value {
                Some(value) => apply_update(&change.key, value),
                None => remove(&change.key),   // a delete
            }
            checkpoint = change.offset + 1;
        }
        CacheWatchItem::Lagged { resume_from } => {
            // The watch fell behind and was ended; re-watch from
            // `resume_from` to replay everything missed.
            checkpoint = resume_from;
            break;
        }
        CacheWatchItem::ShardMoved(moved) => {
            // The shard moved to another broker, which ended this `Connection`
            // watch. Re-watch there; without a `resume_from`, `checkpoint` is
            // right. A `Client` watch follows on its own instead.
            checkpoint = moved.resume_from.unwrap_or(checkpoint);
            break;
        }
    }
}
```

When `recv` returns `None` without either, the broker either closed the
stream or ended the watch with an error, and `watch.refusal()` holds that error
as a `BrokerError`. A `revoked` code there means the credential the watch was
opened with was revoked; watch again with a fresh token. A subscription ended
for the same reason returns the `BrokerError` from `next_event`. When the
connection broke instead, because the broker died or the connection timed
out, `watch.lost()` is set; watch again from the offset after the last change.

A resume whose history compaction has collapsed begins with each matching
key's current value instead, and `watch.resnapshot()` says so. Needs a broker
advertising `FEATURE_CACHE_WATCH`, which only brokers with a log-backed cache send.
`Client::watch_cache` returns a `ClusterCacheWatch`, which follows a
moved shard itself: `ShardMoved` arrives as a notice and the changes carry on
from the new owner, none repeated or skipped outside a retained watch's state
phase. A lost connection is followed the same way, without a notice: the watch
is reopened on whichever broker owns the shard now, under the client's
`ReconnectPolicy`, and `watch.reconnects()` counts how often. If none takes it
before the deadline, `recv` returns `None` and `watch.error()` says why. A prefix watch reads one shard; on a multi-shard cache use
`Client::watch_cache_sharded` (see [Clusters](/clients/rust/operations/#clusters)).
See [Cache Features](/features/cache/#7-keyed-watch) for the full
contract.

### Retained Watch

Start from current state instead of from now: each matching key's current
value first, then live changes. Use it to join presence or state sync:

```rust
let watch = client
    .watch_cache_retained(
        "acme",
        "prod",
        "presence",
        CacheWatchFilter::Prefix("room:7:".into()),
    )
    .await?;

// The state phase is exactly this many changes; 0 means empty, definitively.
let state_size = watch
    .retained_count()
    .context("the broker did not report the retained count")?;
```

Needs `FEATURE_CACHE_WATCH_RETAINED`, a separate bit so an older watch-capable
broker is never asked for state it would silently not deliver. Through a
`Client`, a move or a lost connection inside the state phase reopens the
watch retained and the state phase starts over on the new owner, so values
already handed out may arrive again. `watch.in_state_phase()` turns false once
the state is complete; count against `retained_count()` only on a bare
`Connection` watch. Mutually
exclusive with `from_offset`, because a resume already replays what a retained start
shortcuts.

### Counters

```rust
// Apply a delta and learn the sum including it, in one round trip.
let after = client
    .counter_add("acme", "prod", "limits", "user:42:reqs", 1)
    .await?;

// Read; None means never written, which differs from a sum of zero.
let views = client
    .counter_get("acme", "prod", "metrics", "page:home")
    .await?;
```

Scoped and routed like cache keys, stored beside the cache; durable and
replicated with the shard. At-least-once: a retry after a lost ack counts
twice. Needs a broker advertising `FEATURE_COUNTERS` (durable brokers only).

### Concurrent Cache Operations

Requests issued together share the client's connections and run
concurrently:

```rust
use tokio::task::JoinSet;

let mut lookups = JoinSet::new();
for i in 0..10 {
    let client = Arc::clone(&client);
    lookups.spawn(async move {
        let key = format!("key-{i}");
        client.cache_get("acme", "prod", "data", &key).await
    });
}

// Answers arrive in completion order. The first failure stops the loop, and
// dropping the set cancels the rest.
while let Some(answer) = lookups.join_next().await {
    if let Some(value) = answer?? {
        process(value);
    }
}
```

### Cache Namespacing

Cache keys are scoped to prevent collisions:

```rust
// These are independent entries
client
    .cache_put("acme", "prod", "sessions", "user-123", data1, ttl)
    .await?;
client
    .cache_put("acme", "prod", "profiles", "user-123", data2, ttl)
    .await?;
client
    .cache_put("acme", "prod", "temp", "user-123", data3, ttl)
    .await?;
```
