# felix-client for Python

The Python client for [Felix](https://github.com/GetFelix/felix). It publishes to and reads from streams, runs consumer groups, uses the cache and counters, and follows a cluster as brokers fail and shards move. It is a binding over the Rust client, so reconnection, redirects, retry classification and offsets behave as they do there.

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

The stream has to exist first; create it with [`felixctl stream create`](https://docs.getfelix.dev/getting-started/felixctl/).

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
- Idempotent producers
- Token refresh, cluster discovery, failover and following a shard that moves

Record headers and client certificates (mutual TLS) are Rust only. A publish without `key=` puts every record on shard 0.

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
| `reconnect_attempts`, `reconnect_deadline`, `reconnect_backoff`, `reconnect_max_backoff` | Retry policy. Times are in seconds. |

TLS is always on, and there is no switch to skip verification. A development broker with a self-signed certificate needs its CA file: set `FELIX_TLS_CERT_EXPORT=/path/ca.pem` on the broker and pass that path as `ca_file`.

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

The other classes are `ConnectionError`, `ShardUnavailableError`, `OverloadedError`, `NotFoundError` and `CursorError`. The client already retries what is safe to retry. Do not match on the message text. [Errors across clients](https://docs.getfelix.dev/clients/errors/) maps every code to the type each client raises.

## Supported platforms

Python 3.9 or newer. Wheels are built for Linux (x86-64 and arm64), macOS (Intel and Apple silicon) and Windows (x86-64), and need no Rust toolchain. Other platforms build from the source distribution, which does.

## Documentation

- [Python client guide](https://docs.getfelix.dev/clients/python/)
- [Choosing a client](https://docs.getfelix.dev/clients/overview/)
- [Delivery semantics](https://docs.getfelix.dev/architecture/semantics/)

## License

Apache-2.0.
