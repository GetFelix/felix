# felix-client for Python

[Felix](https://github.com/GetFelix/felix) is a broker that serves streams, work queues and a cache from one replicated log, over QUIC. This is the Python client. It publishes to and reads streams, runs consumer groups, uses the cache and counters, and follows a cluster as brokers fail and shards move. It is a binding over the Rust client, so reconnection, redirects, retry classification and offsets behave as they do there.

## Install

```bash
pip install felix-client
```

You import it as `felix`.

## Quick start

Connect, subscribe, publish one record and read it back:

```python
import os

import felix

with felix.Client(
    "127.0.0.1:5000", tenant_id="t1", token=os.environ["FELIX_TOKEN"]
) as client:
    # Subscribe before publishing: a subscription starts at the stream's tail.
    with client.subscribe("t1", "default", "events") as events:
        client.publish("t1", "default", "events", b"hello")
        event = events.next_event(timeout=5)
        if event is None:
            raise TimeoutError("no event within 5 seconds")
        print(event.payload, event.offset)
```

To run it you need a running broker, a token for tenant `t1`, and the stream `t1/default/events`. The [quick start](https://docs.getfelix.dev/getting-started/quickstart/) shows how to start a broker, [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) covers tokens, and [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/) makes the stream.

`AsyncClient` is the same client for `asyncio`. Its calls are awaitable and its subscriptions are read with `async for`:

```python
import os

import felix


async def main() -> None:
    async with await felix.AsyncClient.connect(
        "127.0.0.1:5000", tenant_id="t1", token=os.environ["FELIX_TOKEN"]
    ) as client:
        async with await client.subscribe("t1", "default", "events") as events:
            await client.publish("t1", "default", "events", b"hello")
            event = await events.next_event(timeout=5)
            if event is None:
                raise TimeoutError("no event within 5 seconds")
            print(event.payload, event.offset)
```

`Client` blocks with the GIL released, which suits threads and scripts. `AsyncClient` yields to your event loop.

## What it covers

- Publish and subscribe, with a log offset on every event of a durable stream
- Sharded streams: routing keys, named shards, and subscribing to every shard
- Consumer groups: poll, acknowledge, retry later, dead letters
- Cache, counters and cache watches
- Atomic commits, conditional writes and head trims
- Idempotent producers, including `hand_off_batch` to queue a batch without waiting for the leader
- Record headers, and `publisher` and `timestamp_micros` on events
- Lease writes (`cache_put_if_lease`)
- Asking a broker about itself: `inspect_shard`, `list_subscriptions`, `topology`, `shard_owners`, `stream_routing`, `cache_shards`, `supports`
- Token refresh, cluster discovery, failover and following a shard that moves

Two things in the Rust client are not wrapped: `publish_json` (Python serializes JSON itself) and a custom `quinn` transport. A publish without `key=` puts every record on shard 0.

## Configuration

`Client(...)` and `AsyncClient.connect(...)` take the same keyword arguments:

| Argument | Meaning |
| --- | --- |
| `addrs` | One `host:port` or a list. Any reachable broker is enough. |
| `tenant_id` | The tenant to authenticate as. Required. |
| `token` | A Felix token. |
| `token_provider` | A function that returns a fresh token. `Client` takes a plain function or an `async def`; `AsyncClient` takes an `async def`. `token` is then optional. |
| `ca_file` | A PEM file of CAs to trust. The system store when omitted. |
| `server_name` | The name the broker's certificate carries. Default `localhost`. |
| `client_cert_file`, `client_key_file` | The client certificate and its key, as PEM files. |
| `reconnect_attempts`, `reconnect_deadline`, `reconnect_backoff`, `reconnect_max_backoff` | Retry policy. Times are in seconds. |
| `publishers`, `timestamps`, `record_headers` | Ask brokers for `Event.publisher`, `Event.timestamp_micros`, and record headers (on by default). |
| `tuning` | A `ClientTuning` with connection pools, windows, queue sizes and timeouts (in seconds). |

TLS is always on, and there is no switch to skip verification. A development broker with a self-signed certificate needs its CA file: set `FELIX_TLS_CERT_EXPORT=/path/ca.pem` on the broker and pass that path as `ca_file`. A broker started with `FELIX_TLS_CLIENT_CA` also requires a client certificate. Give the certificate and its key as two PEM files, both or neither.

A token lasts 15 minutes by default. A process that runs longer passes `token_provider` so the client can fetch a new one. [Auth and TLS](https://docs.getfelix.dev/clients/auth-and-tls/) lists the same options for every client.

## Errors

Every error is a `felix.FelixError`, with `code`, `retry` and `detail` when the broker sent them:

```python
try:
    client.publish("t1", "default", "events", b"hello")
except felix.OutcomeUnknownError:
    ...  # the record may have been written; reconcile before sending it again
except felix.AuthError:
    ...  # the token was refused; retrying will not help
except felix.FelixError as err:
    if err.retry in ("retry", "retry_after", "redirect"):
        ...  # nothing was applied
```

The other classes are `ConnectionError`, `ShardUnavailableError`, `OverloadedError`, `NotFoundError`, `CursorError`, `CommitError` (with `NotOnOwningShardError` and `EventCountError`), `TrimError` and `PublishRefusedError`. The client already retries what is safe to retry. Do not match on the message text. [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps every code to the type each client raises.

## Supported platforms

Python 3.9 or newer. Wheels are built for Linux (x86-64 and arm64), macOS (Intel and Apple silicon) and Windows (x86-64), and need no Rust toolchain. Other platforms build from the source distribution, which does.

## Documentation

- [Python client guide](https://docs.getfelix.dev/clients/python/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)

## License

Apache-2.0.
