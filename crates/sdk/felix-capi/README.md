# libfelix

[Felix](https://github.com/GetFelix/felix) is a broker that serves streams, work queues and a cache from one replicated log, over QUIC. This is the C client: a C ABI over the Rust client, built as a shared library, a static library and the header `felix.h`. Reconnection, redirects, retry classification and offsets behave as they do in the Rust client. The C# client binds this library, and any language with a C FFI can.

It is not published to crates.io. It ships as a native library with its header.

Each call returns at once and its result arrives in a callback. These are the calls, taken from the full program under [Quick start](#quick-start):

```c
felix_client_connect("127.0.0.1:5000", "t1", token, "localhost", NULL, NULL, on_connect, &w);
felix_client_publish(client, "t1", "default", "events", payload, sizeof payload - 1,
                     NULL, 0, FELIX_ACK_PER_MESSAGE, FELIX_DELIVERY_AT_MOST_ONCE,
                     NULL, on_publish, &w);
felix_subscription_next(sub, 5000 /* ms */, NULL, on_event, &w);
```

## Install

Every GitHub release attaches an archive per platform, with a `.sha256` file beside it. With the GitHub CLI, for Linux x64:

```bash
gh release download -R GetFelix/felix -p 'libfelix-*-linux-x64.*'
```

The platforms are `linux-x64`, `linux-arm64`, `osx-x64`, `osx-arm64` (`.tar.gz`) and `win-x64`, `win-arm64` (`.zip`). Check the checksum, unpack, and from the unpacked directory link:

```bash
cc -I include app.c -L lib -lfelix -Wl,-rpath,"$PWD/lib"
```

The archive's `LINKING.txt` has the exact commands for your platform, including static linking and `cl /MD` on Windows. To build the library from this repository instead:

```bash
cargo build --release -p felix-capi
```

## Quick start

Connect, subscribe, publish one record and read it back. Every call that talks to the broker returns at once and reports its result to a callback, so this program wraps each one in a small `waiter` that blocks until the callback runs:

```c
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "felix.h"

struct waiter {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool done;
    FelixStatus status;
    void *handle;      /* the new client, subscription or event */
    char message[256]; /* copied: the callback's message dies with the callback */
};

static void finish(struct waiter *w, FelixStatus status, const char *message, void *handle) {
    pthread_mutex_lock(&w->mu);
    w->done = true;
    w->status = status;
    w->handle = handle;
    snprintf(w->message, sizeof w->message, "%s", message ? message : "");
    pthread_cond_signal(&w->cv);
    pthread_mutex_unlock(&w->mu);
}

/* Waits for the callback of a call that returned `started`. A refused call
   never runs its callback, so its error is taken from the thread instead. */
static FelixStatus wait_for(struct waiter *w, FelixStatus started) {
    if (started != FELIX_STATUS_OK) {
        snprintf(w->message, sizeof w->message, "%s", felix_last_error_message());
        return started;
    }
    pthread_mutex_lock(&w->mu);
    while (!w->done) pthread_cond_wait(&w->cv, &w->mu);
    w->done = false;
    FelixStatus status = w->status;
    pthread_mutex_unlock(&w->mu);
    return status;
}

static void on_connect(void *user_data, FelixStatus status, const char *message,
                       FelixClient *client) {
    finish(user_data, status, message, client);
}
static void on_subscribe(void *user_data, FelixStatus status, const char *message,
                         FelixSubscription *subscription) {
    finish(user_data, status, message, subscription);
}
static void on_publish(void *user_data, FelixStatus status, const char *message,
                       bool has_offset, uint64_t offset) {
    (void)has_offset;
    (void)offset;
    finish(user_data, status, message, NULL);
}
static void on_event(void *user_data, FelixStatus status, const char *message,
                     FelixEvent *event) {
    finish(user_data, status, message, event);
}

int main(void) {
    const char *token = getenv("FELIX_TOKEN");
    if (!token || !*token) {
        fprintf(stderr, "set FELIX_TOKEN\n");
        return 1;
    }

    struct waiter w = {.done = false};
    pthread_mutex_init(&w.mu, NULL);
    pthread_cond_init(&w.cv, NULL);
    FelixClient *client = NULL;
    FelixSubscription *sub = NULL;
    FelixEvent *event = NULL;
    const uint8_t *data = NULL;
    size_t len = 0;
    int exit_code = 1;
    FelixStatus status;

    status = felix_client_connect("127.0.0.1:5000", "t1", token, "localhost",
                                  NULL /* system CAs */, NULL, on_connect, &w);
    if (wait_for(&w, status) != FELIX_STATUS_OK) {
        fprintf(stderr, "connect: %s\n", w.message);
        goto cleanup;
    }
    client = w.handle;

    /* Subscribe first, so the publish below is not missed. */
    status = felix_client_subscribe(client, "t1", "default", "events", 0,
                                    FELIX_START_LATEST, 0, NULL, on_subscribe, &w);
    if (wait_for(&w, status) != FELIX_STATUS_OK) {
        fprintf(stderr, "subscribe: %s\n", w.message);
        goto cleanup;
    }
    sub = w.handle;

    static const uint8_t payload[] = "hello";
    status = felix_client_publish(client, "t1", "default", "events", payload,
                                  sizeof payload - 1, NULL, 0, FELIX_ACK_PER_MESSAGE,
                                  FELIX_DELIVERY_AT_MOST_ONCE, NULL, on_publish, &w);
    if (wait_for(&w, status) != FELIX_STATUS_OK) {
        fprintf(stderr, "publish: %s\n", w.message);
        goto cleanup;
    }

    status = felix_subscription_next(sub, 5000 /* ms */, NULL, on_event, &w);
    status = wait_for(&w, status);
    if (status != FELIX_STATUS_OK) {
        fprintf(stderr, "read: %s\n", status == FELIX_STATUS_TIMEOUT ? "timed out" : w.message);
        goto cleanup;
    }
    event = w.handle;
    if (felix_event_payload(event, &data, &len) != FELIX_STATUS_OK) {
        fprintf(stderr, "payload: %s\n", felix_last_error_message());
        goto cleanup;
    }
    printf("%.*s\n", (int)len, (const char *)data);
    exit_code = 0;

cleanup:
    /* Every call has completed by here, so no callback still holds &w. */
    felix_event_free(event);
    felix_subscription_free(sub);
    felix_client_free(client);
    pthread_cond_destroy(&w.cv);
    pthread_mutex_destroy(&w.mu);
    return exit_code;
}
```

To run it you need a running broker, a token for tenant `t1`, and the stream `t1/default/events`. The [quick start](https://docs.getfelix.dev/getting-started/quickstart/) shows how to start a broker, [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) covers tokens, and [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/) makes the stream.

## What it covers

- Publish and subscribe, with a log offset on every event of a durable stream
- Sharded streams: routing keys, named shards, and subscribing to every shard
- Consumer groups: poll, acknowledge, retry later, dead letters
- Cache, counters and cache watches
- Atomic commits, conditional writes and head trims
- Idempotent producers
- Token refresh, cluster discovery, failover and following a shard that moves

Record headers and client certificates (mutual TLS) are available in the Rust client only. A publish with a null key puts every record on shard 0.

## Configuration

`felix_client_connect` takes the settings as arguments. `felix_client_connect_with` also takes a `FelixConnectOptions`, whose `size` field is `sizeof(FelixConnectOptions)`.

| Setting | Where |
| --- | --- |
| Broker addresses | `addrs`: one `host:port` or several separated by commas. Any reachable one is enough. |
| Tenant | `tenant_id` |
| Token | `token` |
| Token provider | `token_fetch`, `token_user_data` and `token_release` in `FelixConnectOptions`. `token` is then optional. |
| CA file | `ca_file`: a PEM file of CAs to trust. The system store when null. |
| Server name | `server_name`: the name the broker's certificate carries. `localhost` when null. |
| Reconnect policy | `reconnect_attempts`, `reconnect_deadline_ms`, `reconnect_backoff_ms` and `reconnect_max_backoff_ms` in `FelixConnectOptions`. Times are in milliseconds. Zero is the default. |

TLS is always on, and there is no switch to skip verification.

A token lasts 15 minutes by default. A process that runs longer sets `token_fetch` so the client can get a new one. [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) lists the same options for every client.

## Errors

A call returns a `FelixStatus` for the call itself. `FELIX_STATUS_OK` means the callback will run exactly once. Anything else means the call was refused before it started, the callback will not run, and `felix_last_error_message()` says why. The callback then gets the result's status.

- `FELIX_STATUS_CONNECTION` and `FELIX_STATUS_SHARD_UNAVAILABLE`: nothing was applied. Send it again. `FELIX_STATUS_OVERLOADED` is the same after a pause.
- `FELIX_STATUS_AUTH`: the token was refused. Retrying will not help.
- `FELIX_STATUS_OUTCOME_UNKNOWN`: a write may have landed. Reconcile before sending it again.

The client already retries what is safe to retry. [Status codes](https://docs.getfelix.dev/clients/c/#status-codes) lists them all, and [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps them to the other clients' types.

## Supported platforms

Linux, macOS and Windows, each on x64 and arm64. The Linux library is built for glibc. Callbacks run on a library thread, so they must be short and must not unwind into the library.

## Documentation

- [C client guide](https://docs.getfelix.dev/clients/c/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)

## Building and testing

`include/felix.h` is generated by cbindgen and checked in. After changing an exported function, run `task capi:header` and commit the result. `task capi:test` compiles `tests/c/pubsub.c` against the header and runs it against the shared and static libraries; `task capi:test -- fixture` also runs the broker scenarios against `felix-cluster client-fixture`.

## License

Apache-2.0.
