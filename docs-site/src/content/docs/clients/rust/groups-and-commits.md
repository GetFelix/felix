---
title: "Rust: Consumer Groups and Commits"
description: "Work queues with consumer groups, atomic commits across streams and cache keys, and trimming a shard's head."
---

Part of the [Rust client guide](/clients/rust/). Setup and configuration are on that page.

## Consumer Groups

The other way to read a stream. `subscribe` pushes every record to every
subscriber; a **consumer group** hands each record to one consumer and takes it
back if nobody says it was handled.

Records are **pulled**, because only the consumer knows when it has capacity:

```rust
loop {
    // Waits up to five seconds for work rather than spinning on empty polls.
    let records = client
        .group_poll_wait("acme", "prod", "jobs", 0, "fulfilment", 32, Duration::from_secs(5))
        .await?;

    for record in records {
        // `attempts` is 1 on a first delivery and higher on a redelivery, so a
        // consumer can treat a retry differently. 0 means the broker did not
        // report it, which is not the same as a first attempt.
        match handle(&record.payload, record.attempts).await {
            Ok(()) => {
                client
                    .group_ack("acme", "prod", "jobs", 0, "fulfilment", record.offset)
                    .await?;
            }
            Err(err) => {
                tracing::warn!(offset = record.offset, "job failed: {err:#}");
                // Hand it back for immediate redelivery rather than waiting
                // out the visibility timeout.
                client
                    .group_nack("acme", "prod", "jobs", 0, "fulfilment", &record)
                    .await?;
            }
        }
    }
}
```

An empty batch means nothing was available. It is not an error.

### Dead letters

Past `FELIX_GROUP_MAX_ATTEMPTS` a record is dead-lettered, so one poison record
cannot stall the queue behind it. These need `FEATURE_GROUP_DEAD_LETTERS`, a
separate bit from `FEATURE_CONSUMER_GROUP`:

```rust
let offsets = client
    .group_dead_letters("acme", "prod", "jobs", 0, "fulfilment")
    .await?;
for offset in offsets {
    if worth_retrying(offset) {
        client.group_redrive("acme", "prod", "jobs", 0, "fulfilment", offset).await?;
    } else {
        client.group_discard("acme", "prod", "jobs", 0, "fulfilment", offset).await?;
    }
}
```

A dead letter is a **pointer, not a copy**: the record is still in the stream's
log at that offset, readable by an ordinary replay.

### Creating, moving and deleting a group

A group starts at offset 0 on its first poll unless it was created first. These
need `FEATURE_GROUP_ADMIN` (`Connection::supports_group_admin`):

```rust
use felix_client::StartPosition;

// Left alone if the group already exists, so safe on every start.
client
    .group_create("acme", "prod", "jobs", 0, "fulfilment", StartPosition::Latest)
    .await?;

// Replay everything the shard still holds.
client
    .group_seek("acme", "prod", "jobs", 0, "fulfilment", StartPosition::Earliest)
    .await?;

let info = client
    .group_describe("acme", "prod", "jobs", 0, "fulfilment")
    .await?;
println!("committed {:?}, {} behind", info.committed, info.lag());

// `false` if there was no such group.
let existed = client
    .group_delete("acme", "prod", "jobs", 0, "fulfilment")
    .await?;
```

A seek voids the claims standing when it lands, so an ack for a record the
group now owes fails with `stale_claim`. `Client` has the same calls per
shard, and `group_create_stream`, `group_seek_stream`, `group_describe_stream`
and `group_delete_stream` for every shard of a stream. The Python and TypeScript
bindings wrap the per-shard calls and the stream-wide forms
([Python](/clients/python/#the-whole-stream-at-once),
[TypeScript](/clients/typescript/#the-whole-stream-at-once)).

Just after a failover, the new leader of a `Quorum` stream has no committed
tail yet. Until it does, it refuses a seek, a describe and a group's first poll
with `shard_unavailable` (reason `not_ready`). `Client` backs off and asks again
under its reconnect policy; if the call still fails, nothing changed, so call it
again.

A consumer can also manage each claim itself on a broker that advertises
`FEATURE_GROUP_CLAIM_CONTROL` (`Connection::supports_group_claim_control`):

```rust
use std::time::Duration;

use felix_client::GroupPollOptions;

// Claims from this poll stand for five minutes instead of the broker's default.
let options = GroupPollOptions {
    visibility: Some(Duration::from_secs(300)),
    ..Default::default()
};
let records = client
    .group_poll_with("acme", "prod", "jobs", 0, "fulfilment", 10, &options)
    .await?;

for record in &records {
    match check(record) {
        Job::Done => {
            client
                .group_ack("acme", "prod", "jobs", 0, "fulfilment", record.offset)
                .await?;
        }
        // Still working: keep the claim for another minute.
        Job::Running => {
            client
                .group_extend("acme", "prod", "jobs", 0, "fulfilment", record, Duration::from_secs(60))
                .await?;
        }
        // Failed for now: back off before the next try.
        Job::Failed => {
            client
                .group_nack_after("acme", "prod", "jobs", 0, "fulfilment", record, Duration::from_secs(30))
                .await?;
        }
        // Will never work: listed as a dead letter at once, and can be redriven.
        Job::Poison => {
            client
                .group_dead_letter("acme", "prod", "jobs", 0, "fulfilment", record.offset)
                .await?;
        }
    }
}
```

An extension or a nack, delayed or not, is refused with `stale_claim` once the
claim has lapsed and the record has been handed out again: each names the
delivery by `record.attempts`. The client refuses a delay or a visibility
to a broker without the bit, which would ignore it. `Client` has the
same calls, and `ShardedGroup` has `extend`, `nack_after` and `dead_letter`. The Python and TypeScript
bindings wrap all of them: `group_poll` takes a `visibility` in place of `group_poll_with`'s options, and
`group_sharded` returns the `ShardedGroup`.

### What a group needs

- **Durable storage on the broker.** A group's position lives in a log, so a
  broker without `FELIX_DURABLE_STORAGE_DIR` serves no groups and does not
  advertise `FEATURE_CONSUMER_GROUP`.
- **The shard's leader.** A poll is refused rather than forwarded, because
  relaying would put the claim and the acknowledgement on different brokers.
  Another broker answers with `NotLeaderError`, and so does the old leader once
  a shard move cuts over. `Connection` hands that back; the same calls on
  `Client` (`group_poll`, `group_ack`, ...) follow it to the leader.
- **Idempotent handling.** This is at-least-once: a crash after handling and
  before acknowledging is indistinguishable from a crash before handling, so the
  record comes back.

## Atomic commits

`commit` writes an event and the state it changes as one record on the shard
an entity key routes to; `state_get` reads that state back with the commit's
offset as its version. `Connection` answers a shard led elsewhere with
`NotLeaderError`, and `Client` follows it.

```rust
use felix_client::{CommitError, CommitOp};

let receipt = client
    .commit("acme", "orders", b"order-42", vec![
        CommitOp::enqueue("order-events", r#"{"type":"placed"}"#),
        CommitOp::put("order-events", "order-42", r#"{"status":"placed"}"#),
    ])
    .await?;
let state = client
    .state_get("acme", "orders", "order-events", b"order-42", "order-42")
    .await?;
// The state's version is the offset of the commit that wrote it.
println!("committed at {}, state version {:?}", receipt.offset, state.version);

// Refused before anything is sent: another stream is another log.
let refused = client
    .commit("acme", "orders", b"order-42", vec![
        CommitOp::publish("order-events", "placed"),
        CommitOp::put("inventory", "sku-1", "3"),
    ])
    .await;
if let Err(err) = refused {
    match err.downcast_ref::<CommitError>() {
        // Op 1 names a stream the entity key does not route to.
        Some(CommitError::NotOnOwningShard { index, .. }) => {
            tracing::error!(index, "commit spans two streams");
        }
        _ => return Err(err),
    }
}
```

`CommitError::EventCount` refuses a commit without exactly one event, and
`CommitError::Unsupported` a broker that did not advertise
`FEATURE_ATOMIC_COMMIT`.

`commit_if` and `publish_if` write only at an expected offset, the shard's
next, for a single writer that must not append after it has been replaced:

```rust
use anyhow::bail;
use felix_client::ConditionalWrite;

// The offset this writer expects to write next, starting at an empty shard.
let mut next = 0;
for tick in ticks {
    match client
        .publish_if("acme", "games", "match-7", None, vec![tick], next)
        .await?
    {
        ConditionalWrite::Written { offset } => next = offset + 1,
        ConditionalWrite::Refused { tail } => {
            bail!("another writer has the stream; its next offset is {tail}")
        }
    }
}
```

A refusal is an answer, not an error, and writes nothing. Both need
`FEATURE_PUBLISH_CONDITIONAL`, and fail without sending anything against a
broker that did not advertise it. `Client` follows the leader; neither
is forwarded between brokers. Do not resend one whose answer was lost: read
the shard at the expected offset instead.

`commit_on_shard`, `commit_if_on_shard`, `publish_if_on_shard` and
`state_get_on_shard` take a shard instead of hashing the entity key, for a
writer that keeps one shard per partition:

```rust
let ops = vec![
    CommitOp::publish("ticks", tick),
    CommitOp::put("ticks", "district", state),
];
let written = client
    .commit_if_on_shard("acme", "city", district, b"district", ops, next)
    .await?;
let current = client
    .state_get_on_shard("acme", "city", "ticks", district, "district")
    .await?;
```

A keyed `state_get(entity_key)` reads the shard the key hashes to, so it does
not find state committed to another named shard; read it with
`state_get_on_shard`. A subscriber that picks its shard by hashing a key and
filters by key misses those records the same way: subscribe to the named shard.

What atomic does and does not cover is in
[`docs/atomic-commit.md`](https://github.com/GetFelix/felix/blob/main/docs/atomic-commit.md).

## Trimming a shard's head

`trim_before` hides the records of one shard of a durable stream below an
offset, for a single writer that owns the shard's history, such as a state
machine that has snapshotted. It is conditional like `publish_if`: it writes
only if the shard's next offset is the one the caller expects, and the trim is
a record of its own that lands there.

```rust
use anyhow::bail;
use felix_client::{BrokerError, ConditionalTrim, RetryClass, TrimError};

// `next` is the shard's next offset as this writer last saw it, and `floor`
// the first offset it still needs. `floor` can be at most `next`.
let trimmed = client
    .trim_before("acme", "games", "match-7", 0, floor, next, Some(b"writer-7"))
    .await;
match trimmed {
    Ok(ConditionalTrim::Trimmed { offset, oldest }) => {
        // Reads of the shard start at `oldest` now. A trim that landed took
        // an offset, so the next write expects the one after it.
        if let Some(offset) = offset {
            next = offset + 1;
        }
        tracing::info!(oldest, "trimmed");
    }
    Ok(ConditionalTrim::Refused { tail }) => {
        bail!("the shard moved on; its next offset is {tail}")
    }
    Err(err) if err.is::<TrimError>() => bail!("this broker cannot trim: {err}"),
    Err(err) => match err.downcast_ref::<BrokerError>() {
        // The broker stopped waiting. The trim may have landed.
        Some(refusal) if matches!(refusal.retry, RetryClass::OutcomeUnknown) => {
            tracing::warn!("trim outcome unknown; read the shard before trimming again");
        }
        _ => return Err(err),
    },
}
```

`ConditionalTrim::Trimmed` carries where the trim record landed, absent when
the floor already stood at or past `before` and nothing was written, and the
oldest offset reads start at now. A stale tail is an answer, `Refused`, and
writes nothing. `trim_before` is not retried on it.

Every other refusal is a `BrokerError`:

| Code | Meaning | Retry |
| --- | --- | --- |
| `feature_not_enabled` | the fleet has not finalized head trims | not until it has |
| `stream_has_commits` | the stream holds atomic commits | never |
| `invalid_request` | `before` is past `expected_offset` (`trim_past_tail`), or the stream is not durable (`not_durable`) | never |
| `forbidden` | the token lacks `stream.trim` | never |
| `unacknowledged` | the broker stopped waiting for the trim to commit (`trim_timed_out`) | outcome unknown |

Do not send a timed-out trim again as it is. If it landed, the same call is
refused with a tail one past the trim, which looks like a lost race. Read the
shard's earliest offset, or read below the floor you asked for, to tell the two
apart. `Client` follows the leader and asks again only after a coded
`not_leader`. A `shard_unavailable` or `not_found` from the entry broker comes
back without a backoff; nothing was written, so it is safe to call again.

A read below the floor fails with `SubscribeCursorError` (`TooOld`), whose
`available` is the new floor and whose `trim_headers` are the bytes passed as
`headers`, so a writer can see who trimmed past it. The call needs the
`stream.trim` permission, which `stream.manage` includes, and fails with
`TrimError::Unsupported`, sending nothing, against a broker that did not
advertise `FEATURE_HEAD_TRIM`.
