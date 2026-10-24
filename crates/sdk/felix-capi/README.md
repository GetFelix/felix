# felix-capi

A C ABI over the Felix Rust client, and the base the C# SDK will bind to. Like the Python and Node bindings it wraps `felix-client`, so
reconnection, redirect-following and error classification are the Rust
client's.

It builds `libfelix` as a shared library (`.so`, `.dylib`, `.dll`) and a
static one (`.a`), with the header in [`include/felix.h`](include/felix.h).

```bash
cargo build --release -p felix-capi
cc app.c -I crates/sdk/felix-capi/include target/release/libfelix.a <native libs>
```

`cargo rustc -p felix-capi --crate-type staticlib -- --print native-static-libs`
prints the system libraries a static link needs on your platform.

## The API

Every call that talks to the broker is asynchronous: it returns at once and
reports its result to a callback. The C# SDK completes a `Task` from that
callback, so no thread waits on a pending publish or read.

```c
static void on_published(void *user_data, FelixStatus status, const char *message,
                         bool has_offset, uint64_t offset) {
    /* runs on a library thread */
}

FelixClient *client = /* from felix_client_connect's callback */;
felix_client_publish(client, "t1", "default", "events",
                     (const uint8_t *)"hello", 5, NULL, 0, FELIX_ACK_PER_MESSAGE,
                     FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_published, my_state);
```

The calls are `felix_client_connect`, the publishes (`felix_client_publish`,
`felix_client_publish_batch`, `felix_client_publish_on_shard` and
`felix_client_publish_records`), the subscribes (`felix_client_subscribe` for one
shard and `felix_client_subscribe_sharded` for all of them),
`felix_subscription_next`, `felix_sharded_next` and
`felix_client_offset_for_time`, the group calls (`felix_client_group_*` and
`felix_client_stream_shards`), and the cache calls (`felix_client_cache_*`,
`felix_client_counter_*`, `felix_client_watch_cache*` and `felix_cache_watch_*`), the commit calls (`felix_client_commit*`,
`felix_client_publish_if*` and `felix_client_state_get*`), and the producer calls
(`felix_client_idempotent_producer` and `felix_producer_*`). Each takes a callback, a `void *user_data`
passed back to it untouched, and an optional `FelixCancelToken`.

The rules are the same for every function:

- Handles (`FelixClient`, `FelixSubscription`,
  `FelixShardedSubscription`, `FelixCacheWatch`, `FelixProducer`, `FelixEvent`, `FelixCancelToken`) are opaque. Everything the library hands out has a
  `*_free` function, which accepts null. A handle may be used from several
  threads, but never freed while another thread is using it, a callback
  included.
- A call returns a `FelixStatus` for the call itself. `FELIX_STATUS_OK`
  means it was accepted and its callback will run exactly once. Anything
  else means it was refused before starting (a null pointer, bad UTF-8, an
  out-of-range value), the callback will not run, and
  `felix_last_error_message()` says why. That message is per thread and valid
  until the next Felix call on that thread.
- The callback gets the result's status and, for a failure (status 10 and
  above), a message valid only while the callback runs. On success it may
  own a new handle, such as the client or the event.
- The error classes match the other SDKs: `FELIX_STATUS_CONNECTION` and
  `FELIX_STATUS_SHARD_UNAVAILABLE` are worth retrying, `FELIX_STATUS_AUTH`
  is not, and `FELIX_STATUS_OUTCOME_UNKNOWN` means a write may have landed.
- Callbacks run on one of the client's two library threads, and may run
  before the call that started them returns. Keep them short: a callback that
  blocks stalls the client. A callback may call any Felix function, including
  freeing the handle it was called for. It must not unwind or throw back into
  the library.
- Calls in flight at the same time run concurrently and may reach the broker
  in any order, as concurrent futures do in the Rust client. To keep records
  in order, start each publish from the previous one's callback.
- Cancelling a token, or freeing the client or subscription a call was made
  on, completes the call with `FELIX_STATUS_CANCELLED`, unless it already
  finished. A cancelled connect or subscribe never hands out a handle. A
  publish cancelled after it may have been sent completes with
  `FELIX_STATUS_OUTCOME_UNKNOWN`. A token can be freed while calls hold it.
- Callbacks for calls cancelled by a free may run after the free returns, so
  `user_data` must stay valid until they do. Freeing the last handle on a
  client's runtime waits up to a few seconds for them, except from a callback
  or another async runtime, where a library thread finishes the teardown
  after the free returns; do not unload the library before those callbacks
  arrive. A process that exits with handles still live gets no callbacks for
  their pending calls.
- A panic inside the library becomes `FELIX_STATUS_PANIC`, from the call or
  through the callback.
- Each client owns a small Tokio runtime, so the caller needs no async
  runtime of its own.
- A subscription is read one event at a time with `felix_subscription_next`,
  which takes a timeout in milliseconds (negative waits indefinitely) and
  completes with an event, `FELIX_STATUS_TIMEOUT`, or `FELIX_STATUS_CLOSED`
  once the broker ends the stream. One read may be pending; a second is
  refused. A timed-out or cancelled read loses no event. Events reach
  callbacks in order when each read starts from the previous one's callback
  or after it ran.

### Publishing

`felix_client_publish` sends one record. A non-null `key` routes it, so
records sharing a key share a shard and stay in order; a null key sends it to
shard 0. `FELIX_DELIVERY_AT_MOST_ONCE` reports a lost connection rather than
risk writing the record twice; `FELIX_DELIVERY_AT_LEAST_ONCE` reconnects and
sends it again.

The batch calls take the payloads end to end in one buffer with an array of
lengths, so a managed caller pins one byte array and one length array instead
of a pointer per record:

```c
const uint8_t data[] = "onetwothree";
const size_t lens[] = {3, 3, 5};
felix_client_publish_batch(client, "t1", "default", "events", data, lens, 3,
                           NULL, 0, FELIX_ACK_PER_BATCH, NULL, on_batch, my_state);
```

`felix_client_publish_batch` sends the batch to one shard, by key or to
shard 0, and `felix_client_publish_on_shard` to a shard the caller names.
Either is one frame the broker appends contiguously and accepts or refuses
whole. `felix_client_publish_records` takes a key per record (laid out the
same way), splits the records by shard and sends each shard's share to its
owner; some shards can succeed while others fail. It can resend with
`FELIX_DELIVERY_AT_LEAST_ONCE`; the two single-shard batch calls never
resend a batch that may have been written.

A batch callback gets a `FelixRecordResult` per record, in the order passed:
its status, its log offset when the broker reported one, and the failure's
message. The callback's own status is `FELIX_STATUS_OK` only when every
record was accepted. The results are null when no record has an outcome of
its own: the batch was empty, or the call was cancelled or panicked.

For a C# binding: `bool` in a callback or in `FelixRecordResult` is one
byte, so declare it `byte` in an `[UnmanagedCallersOnly]` signature or a
`[StructLayout(LayoutKind.Sequential)]` struct; `size_t` is `nuint`. A
`fixed` empty array is a null pointer, which `felix_client_publish` reads as
no key, so pass the empty key from a non-empty buffer. Catch every exception inside the
callback, since one that escapes ends the process.

### Subscribing

`felix_client_subscribe` reads one shard, named by `shard`; a shard past the
stream's width is refused by the broker. The subscription follows the shard
to its owner and resubscribes after the last event when it falls behind, so
no lag item reaches C. `felix_subscription_start_offset` returns where it
began, for counting a drop before the first event as
`offset - start_offset - skipped_before`. Once a read reports
`FELIX_STATUS_CLOSED`, `felix_subscription_close_reason` says whether the
broker ended the stream (`FELIX_CLOSE_ENDED`) or it was deleted
(`FELIX_CLOSE_STREAM_DELETED`); a live one is `FELIX_CLOSE_OPEN`.

`felix_client_subscribe_sharded` opens every shard and merges them, failing
if any cannot be opened. `felix_sharded_next` completes with an item: a
record (`FELIX_SHARD_RECORD`, with an event to free) or a notice about one
shard (`FELIX_SHARD_LOST` with the error text, `FELIX_SHARD_RECOVERED`,
`FELIX_SHARD_MOVED`, `FELIX_SHARD_STREAM_DELETED`). Order holds within a shard
only. A caller that ignores `FELIX_SHARD_LOST` reads an incomplete stream
without knowing it. `felix_sharded_positions` writes the last offset handed
out per shard, never ahead of what the caller has been handed, and may be called while a
read is pending; pass the result to `felix_client_resubscribe_sharded` to
resume, where listed shards continue after their offset and the rest start at
`start`.

`felix_client_offset_for_time` finds the first offset on a shard at or after a
time in microseconds, to subscribe from with `FELIX_START_OFFSET`. No offset
means nothing is that recent: subscribe at `FELIX_START_LATEST`.

For a C# binding, the item callback's `FelixEvent *` is null unless the kind is
`FELIX_SHARD_RECORD`, and its message is read only for `FELIX_SHARD_LOST`.

### Consumer groups

Every group call names one shard and goes to the broker that leads it,
following a move. `felix_client_stream_shards` gives the shard count, so a
caller runs one group per shard; composing a group over all shards
(`ShardedGroup`) and the `*_stream` admin calls are later work, as are the
record publisher and timestamp fields, which need connect flags.

- `felix_client_group_create` and `felix_client_group_seek` take a
  `FELIX_START_*` position and report where the group resumes. Create leaves
  an existing group alone (`moved` false).
- `felix_client_group_poll` returns up to `max_records` as `FelixGroupRecord`
  values (offset, attempts, skipped-before, payload), valid only during the
  callback. An empty poll is OK with a count of zero. `consumer` names the
  member, `reclaim` takes back what that member held before a restart, and
  `visibility_ms` of zero is the broker's timeout. A poll cancelled while it
  waits completes with `FELIX_STATUS_CANCELLED`; records it claimed come back
  after the visibility timeout.
- Records are not handles. `felix_client_group_ack`, `_nack`, `_extend`,
  `_dead_letter`, `_discard` and `_redrive` take the `offset`, and nack and
  extend also the `attempts` the poll reported, which the broker checks: a
  claim that lapsed or moved on is refused. Settles and creates are writes, so
  a cancel after one may have been sent completes with
  `FELIX_STATUS_OUTCOME_UNKNOWN`; reads (`_dead_letters`, `_describe`,
  `felix_client_stream_shards`) report `FELIX_STATUS_CANCELLED`.
- `felix_client_group_describe` fills a `FelixGroupInfo`, and
  `felix_client_group_delete` says whether the group existed.

A broker without claim control refuses a delayed nack, an extend and a
visibility override with the same error the Rust client raises. The status is
the broker's error class; a `stale_claim` refusal is `FELIX_STATUS_ERROR` with
the reason in the message.

Creating, seeking and deleting a group, and discarding or redriving a dead
letter, need the `group.manage` permission; polling, settling, extending,
listing dead letters and describing need only consumer access. The C test
uses a second client with the fixture's group operator token for the former.

For a C# binding, copy the poll's records inside the callback and keep
`(offset, attempts)` as a plain struct to settle with.

### Caches, counters and watches

Keys and filters are UTF-8 C strings; values are a pointer and a length, copied
before the call returns (null with length zero is the empty value). Values
handed to a callback are valid only while it runs. Any broker serves a cache
request, forwarding it to the key's owner, so there is no shard to name.

- `felix_client_cache_put` takes a `ttl_ms` (zero is no expiry).
  `felix_client_cache_get` and `_delete` report `found` and the value (for a
  delete, the one removed); `felix_client_cache_get_versioned` adds the entry's
  version.
- `felix_client_cache_put_if` takes `FELIX_IF_ABSENT`, or `FELIX_IF_VERSION`
  with a version from a versioned get; `felix_client_cache_delete_if` takes a
  version. A condition that does not hold is `FELIX_STATUS_OK` with `applied`
  false, not an error.
- `felix_client_counter_add` returns the new value; `felix_client_counter_get`
  reports `has_value` false for a counter that does not exist. An add that is
  cancelled after it may have been sent is `FELIX_STATUS_OUTCOME_UNKNOWN`:
  read the counter before repeating it.
- Puts, deletes and adds are writes (`FELIX_STATUS_OUTCOME_UNKNOWN` on a late
  cancel); gets report `FELIX_STATUS_CANCELLED`.

A watch is one handle, `FelixCacheWatch`, read with `felix_cache_watch_next`
like a subscription: one read pending at a time, a timeout that leaves it
usable, a cancelled read that loses nothing, and a free that cancels a pending
read. `felix_client_watch_cache` watches a key or prefix on one shard (a key is
routed to its own shard, a prefix reads one, so use it on a single-shard cache).
`felix_client_watch_cache_sharded` watches a prefix on every shard and fails
rather than cover fewer shards than the cache has. Both take `retained` to
deliver the stored entries first; it cannot be combined with an offset to
resume from.

A `FelixWatchItem` is borrowed for the callback and its `kind` says what it is:
`FELIX_WATCH_CHANGE` (a deleted key has `has_value` false; `expires_at_millis`
is zero for never), `FELIX_WATCH_LAGGED` (the broker ended the watch;
`resume_from` is where a new one continues; a single watch's next read is
`FELIX_STATUS_CLOSED`, while a sharded watch loses only that shard), `FELIX_WATCH_SHARD_MOVED` (followed automatically),
`FELIX_WATCH_STATE_COMPLETE` (every shard's stored state delivered, sharded
retained only) and `FELIX_WATCH_SHARD_CLOSED`. Keys are not NUL-terminated;
use `key_len`. `felix_cache_watch_resume_offsets` writes each shard's offset for
a sharded watch, never ahead of what reads have handed out, and
`felix_cache_watch_retained_count` reports how many entries a retained watch
started from.

For a C# binding, copy the key and value inside the item callback, and treat an
unknown `kind` as skip-and-read-again so a newer library does not break an older
binding. After `FELIX_WATCH_LAGGED` on a sharded watch the other shards keep
delivering; to get the lagged shard back, reopen with the offsets from
`felix_cache_watch_resume_offsets`, which already account for the lag.

### Atomic commits

A commit is an array of `FelixCommitOp` (`FELIX_COMMIT_PUBLISH`,
`FELIX_COMMIT_ENQUEUE`, `FELIX_COMMIT_PUT`, `FELIX_COMMIT_DELETE`) written as one
record on the shard that owns an entity: every reader sees the event at the
commit's offset and the state it wrote at the same version. It must hold exactly
one event and every operation must name that event's stream. Anything else is
`FELIX_STATUS_INVALID_ARGUMENT` at the call, with the callback never run. An
operation's `key` is read only for a put or delete, and its `data` only for a
publish, enqueue or put. Everything is copied before the call returns.

- `felix_client_commit` routes by `entity_key`; `felix_client_commit_on_shard`
  names the shard. The callback gets the commit's `offset`.
- `felix_client_commit_if`, `_commit_if_on_shard`, `felix_client_publish_if` and
  `_publish_if_on_shard` write only if the shard's next offset is
  `expected_offset`. A refusal is an answer: `FELIX_STATUS_OK`, `written` false,
  and the shard's tail in `offset`. A conditional publish takes the payload
  arrays of `felix_client_publish_batch` and at least one record.
- `felix_client_state_get` and `_state_get_on_shard` fill a borrowed
  `FelixStateValue`: `has_value` is false for a key never written or deleted,
  `version` is the offset of the commit that wrote it, and `as_of` the last
  commit the answer reflects. `felix_client_state_get` takes the `entity_key`
  the commits were routed by.
- Commits are writes: `FELIX_STATUS_OUTCOME_UNKNOWN` on a late cancel, and not
  sent again. `FELIX_STATUS_UNSUPPORTED` means the broker does not advertise
  commits; nothing was sent.

For a C# binding, build the `FelixCommitOp` array in pinned memory for the call
only, since the library copies it, and copy the state's value inside the
callback.

### Idempotent producers

`felix_client_idempotent_producer` asks the broker for a producer id and hands
back a `FelixProducer`. Its batches are numbered per shard and the shard's leader
answers a number it already holds without appending again, so a batch that got
no answer can be sent again and cannot land twice, across a reconnection or a
leader change. Calls on one producer are handed to it in the order the C
calls returned, so they take sequences in that order, and they do not wait for
each other's answers.

- `felix_producer_publish` sends a batch of one or more records as one
  sequence, to shard 0 or, with a `key`, to the key's shard. It takes the
  payload arrays of `felix_client_publish_batch` and reports the batch's first
  offset. `felix_producer_publish_batches` takes several batches in one call,
  split by `batch_sizes`, each at least one record and summing to the record
  count, and reports only success.
- A failure other than a broker refusal leaves the batch in doubt: the
  sequence has not advanced and the batch may have landed. Call again with the
  same payloads and key to settle it; a different batch is refused without
  being sent until then. A cancel after the batch was handed over reports
  `FELIX_STATUS_OUTCOME_UNKNOWN` and the batch is still sent, so settle it the
  same way rather than treating it as lost.
- A refusal (for example, the shard forgot the producer) ends the producer on
  that stream, and its message says why. `felix_producer_abandon_in_doubt`
  numbers the stream afresh under a new id, and a batch given up that way may
  land twice.
- `felix_producer_id` and `felix_producer_id_for` read the id a producer, or one
  stream of it, sends under.
- `felix_producer_close` waits for what was handed over, then stops the
  producer; a non-negative `timeout_ms` bounds the wait. After it, async calls
  on the handle complete with `FELIX_STATUS_CLOSED` and the id calls return it.
  `felix_producer_free` drops what is left without waiting; use close to drain.

For a C# binding, keep one producer per logical writer and start its calls from
one thread or under a lock: the order is the order the C calls return, so
calls racing on several threads have no order of their own. Retry a failed
publish with the same buffers rather than building a new batch.

### Why callbacks, and why pull

A pollable completion handle would still need a notification to complete a
`Task` without a parked thread, so it is a callback with more steps.
Temporal's sdk-core C bridge does the same under its .NET SDK: each async
call takes a callback, `user_data` and a native cancellation token, and the
callback completes a `TaskCompletionSource`.
A C caller that wants a poll loop can signal an eventfd or pipe from the
callback.

Subscriptions are pulled, not pushed. A pushed event has to be buffered
somewhere when the caller is slow; a pull gives the broker-side queue and
its overflow policy the only buffer, the same as the Rust client. Re-arming
the next read from inside the callback is the expected pattern.

There are no blocking variants. Blocking on a callback is a few lines over a
mutex and condition variable, as [`tests/c/pubsub.c`](tests/c/pubsub.c)
shows, and one surface keeps every later operation to one function.

This covers connect, every publish the Rust cluster client offers except the
idempotent ones, subscribing to one shard or to all of them,
groups on a shard, caches, counters and cache watches, and atomic commits with
conditional publishes. Event publisher and timestamp fields, idempotent producers
and the sharded group come later.

## The header

`include/felix.h` is generated by cbindgen and checked in. After changing an
exported function, run `task capi:header` and commit the result; the `header`
test fails while the two differ.

## Tests

`task capi:test` compiles [`tests/c/pubsub.c`](tests/c/pubsub.c) against the
header, links it against both the shared and the static library, and runs the
checks that need no broker. `task capi:test -- fixture` also runs the publish,
batch, subscribe, group, cache and commit scenarios against `felix-cluster client-fixture`, which CI does
on every pull request.

The fixture run is also the C conformance harness. `pubsub.c` tags each
catalogue scenario in `felix-conformance` with `SCENARIO_BEGIN` and
`SCENARIO_END` (a scenario passes when no check inside it failed) and records
`SKIP` with a reason for the ones it does not run yet. `run.sh` merges the
shared and static builds, the worst outcome winning, and writes the results
document to `$FELIX_CONFORMANCE_RESULTS` when that is set:

```bash
FELIX_CONFORMANCE_RESULTS=c-conformance.json crates/sdk/felix-capi/tests/c/run.sh fixture
cargo run -p felix-conformance -- verify c-conformance.json
```

The scenarios that break a cluster (connection faults, losing a broker or a
shard) are skipped until the harness can start a cluster of its own. The
shard-move and lost-quorum scenarios use the fixture's `/fence`, `/partition`
and `/heal` endpoint, so they need `curl`.
