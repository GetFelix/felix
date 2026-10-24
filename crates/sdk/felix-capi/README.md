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
                     (const uint8_t *)"hello", 5, FELIX_ACK_PER_MESSAGE,
                     NULL, on_published, my_state);
```

The calls are `felix_client_connect`, `felix_client_publish`,
`felix_client_subscribe` and `felix_subscription_next`. Each takes a
callback, a `void *user_data` passed back to it untouched, and an optional
`FelixCancelToken`.

The rules are the same for every function:

- Handles (`FelixClient`, `FelixSubscription`, `FelixEvent`,
  `FelixCancelToken`) are opaque. Everything the library hands out has a
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

For a C# binding: `bool` in a callback is one byte, so declare it `byte` in
an `[UnmanagedCallersOnly]` signature, and catch every exception inside the
callback, since one that escapes ends the process.

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

This first cut covers connect, publish and subscribe on shard 0 of a stream.
Keyed publish, groups, caches and multi-shard subscribe come later.

## The header

`include/felix.h` is generated by cbindgen and checked in. After changing an
exported function, run `task capi:header` and commit the result; the `header`
test fails while the two differ.

## Tests

`task capi:test` compiles [`tests/c/pubsub.c`](tests/c/pubsub.c) against the
header, links it against both the shared and the static library, and runs the
checks that need no broker. `task capi:test -- fixture` also runs the publish
and subscribe scenarios against `felix-cluster client-fixture`, which CI does
on every pull request.
