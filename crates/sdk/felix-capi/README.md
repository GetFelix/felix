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
`felix_client_offset_for_time`, and the group calls (`felix_client_group_*` and
`felix_client_stream_shards`). Each takes a callback, a `void *user_data`
passed back to it untouched, and an optional `FelixCancelToken`.

The rules are the same for every function:

- Handles (`FelixClient`, `FelixSubscription`,
  `FelixShardedSubscription`, `FelixEvent`, `FelixCancelToken`) are opaque. Everything the library hands out has a
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

For a C# binding, copy the poll's records inside the callback and keep
`(offset, attempts)` as a plain struct to settle with.

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
idempotent and conditional ones, and subscribing to one shard or to all of
them. Event publisher and timestamp fields, groups, caches and commits come
later.

## The header

`include/felix.h` is generated by cbindgen and checked in. After changing an
exported function, run `task capi:header` and commit the result; the `header`
test fails while the two differ.

## Tests

`task capi:test` compiles [`tests/c/pubsub.c`](tests/c/pubsub.c) against the
header, links it against both the shared and the static library, and runs the
checks that need no broker. `task capi:test -- fixture` also runs the publish,
batch, subscribe and group scenarios against `felix-cluster client-fixture`, which CI does
on every pull request.
