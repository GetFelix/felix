---
title: "Python Client"
description: "Installing and using the Felix Python client: both surfaces, streams, queues, cache watches, multi-shard consumption, and the failure modes worth writing code for."
---

`felix` is a binding over the Rust client. Reconnection,
redirect-following, retry classification and offset bookkeeping live in
`felix-client` and are shared, so Python gets the same failover behaviour as
Rust. See
[Choosing a Client](/clients/overview/) for why that choice was made and
what the conformance suite does about it.

## Installing

```bash
pip install felix-client
```

Wheels ship compiled, one per platform, for every Python from 3.9 up, so
installing needs no Rust toolchain. Linux (x86-64 and arm64), macOS (Intel and
Apple silicon) and Windows x86-64 are covered; anything else builds from the
sdist and does need one.

Building from the repository needs a Rust toolchain either way:

```bash
pip install ./crates/sdk/felix-python
```

## Two surfaces

Both wrap the same Rust client and fail over identically. Pick by how your
application is already written; performance is the same.

| | `Client` | `AsyncClient` |
| --- | --- | --- |
| Calls | block, GIL released | awaitable |
| Subscriptions | `for event in events` | `async for event in events` |
| Suits | threads, `asyncio.to_thread`, scripts | an existing event loop |

```python
import felix

with felix.Client("127.0.0.1:5000", tenant_id="t1", token=tok) as client:
    client.publish("t1", "default", "events", b"hello")
```

```python
import felix


async def main(tok):
    async with await felix.AsyncClient.connect(
        "127.0.0.1:5000", tenant_id="t1", token=tok
    ) as client:
        await client.publish("t1", "default", "events", b"hello")
```

`tok` is a Felix token; [Getting and refreshing a
token](#getting-and-refreshing-a-token) covers where it comes from and how a
client that outlives it gets the next one. These examples pass one and no
`token_provider`, so the client holds it until it expires: streams opened after
expiry, including the ones a reconnect opens, are refused.

### Getting and refreshing a token

A token lasts 15 minutes by default, and the broker checks it each time a stream
authenticates, including the streams a reconnect opens. [Getting a
token](/clients/overview/#getting-a-token) covers where one comes from. A
process that outlives one gives `Client`, `AsyncClient.connect` and
`with_identity` a `token_provider`, and the client asks it for a token when it
needs one. It asks lazily: when no token is held, when the held one has passed
two thirds of its life, or when a broker has just refused it (one refetch per refused stream, retried only if the provider returns a different token, then the operation fails with the auth error). If a fetch fails
while the held token is still valid, the client keeps using that one and
asks again about five seconds later.

The provider is a callable that takes no arguments and returns the token as a
`str`, passed as `token_provider=`; `token=` is then optional, so leave it out
to have the first connect call the provider (`None` or `""` counts as none), or
pass a token you already hold to have it used first.

**When it is called.** When a stream is about to authenticate and the client
holds no token, the held one is due for replacement, or a broker just refused
it. An idle client makes no calls, and there is no timer. The client never runs
two calls at once: streams that open together wait on one and share its token.
That is also what makes it safe to rotate a refresh token inside the provider.
`with_identity` authenticates its streams before it returns, so it calls the
provider when no valid token is held.

**Threading.** On `Client`, a plain function runs on a Felix worker thread, with
the GIL, and never on a thread that is waiting on the broker. Blocking in it is
fine, though it delays the streams waiting for the token. An `async def` runs on
that same worker thread with `asyncio.run`, on a fresh event loop of its own, so
do not use an object bound to another loop in it (an `aiohttp` session, say).
The provider can be called while your own event loop is blocked in a synchronous
call, because it does not depend on that loop.

**Failure.** The provider fails when it raises, returns something that is not a
non-empty string, or does not answer within 30 seconds. While the held token is
still valid the client keeps it and asks again about five seconds
later. Otherwise the operation that needed the token fails with
`AuthError` carrying the provider's message unchanged. A provider failure is
final for that operation: it is not retried, because each call can rotate a
refresh token. The next operation asks again. The 30 second timeout fails the
operations waiting on the call, but the call itself keeps running: the client
never starts a second one while it is outstanding, the next refresh joins it,
and a late answer is used. The message is the exception's `str()`, or its type
name when that is empty. A `str` that is not valid UTF-8 counts as not a string.

**Cancellation.** On `AsyncClient` the provider's task is cancelled at the 30
second timeout and when the client, its `with_identity` handles and its open
subscriptions and in-flight calls are all gone; closing a client while a
subscription or call still uses the token source does not cancel it. A provider
that ignores it still works: it is not called again while pending, and its late
answer is used. The provider sees an `asyncio.CancelledError` at its current
`await`, so a `finally` block can release a connection. Let it propagate: the
cancelled call counts as a failed fetch, and the next fetch calls the provider
again. If the loop is already closed there is nothing to cancel. On `Client`
the provider is your Python code running on a thread and cannot be cancelled,
so it is joined, and an `async def` run there with `asyncio.run` is joined the
same way. Give a blocking provider its own timeout
(`urllib.request.urlopen(..., timeout=20)`) so it returns before the client
gives up on it.

**Async variant.** `AsyncClient` takes an `async def`, or any callable that
returns an awaitable (a lambda or decorator around one). Whatever the provider
returns is awaited; a `str` returned directly is a failure (`AuthError`), since
a plain function belongs on `Client`. A non-callable raises `TypeError` at
`connect` or `with_identity`. The coroutine is awaited on the event loop that
was running at `connect` or `with_identity`, so a session bound to that loop
works. That loop is captured once and must outlive the client, every
`with_identity` handle and their subscriptions; after it closes, calls that need
a token fail with "the event loop the token provider was bound to is closed".
While that loop is blocked, for instance in a synchronous call, the provider
cannot run and the stream waits for it, up to the 30 seconds.

The example below does the control plane calls with `urllib` and keeps the
rotating refresh token. The refresh token works once: every refresh returns a
replacement, so the provider stores it before it returns. A 403 means the
refresh token expired or was revoked: run the exchange again (fresh OIDC login)
and connect a new client; the client keeps the last token until it expires.
`urllib` raises `HTTPError` with the status in its message, so that is what the
provider's failure says.

```python
import json
import urllib.request

import felix

CONTROLPLANE = "https://controlplane:8443/v1/tenants/t1"


def post_json(path, body, bearer=None):
    headers = {"Content-Type": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    request = urllib.request.Request(
        CONTROLPLANE + path, data=json.dumps(body).encode(), headers=headers
    )
    # Under the client's 30 s, so a hung request fails as itself.
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def connect(oidc_token):
    minted = post_json(
        "/token/exchange",
        {"requested": ["stream.publish"], "resources": ["stream:t1/default/*"]},
        bearer=oidc_token,
    )
    refresh_token = minted["refresh_token"]

    def fetch_token():
        nonlocal refresh_token
        minted = post_json("/token/refresh", {"refresh_token": refresh_token})
        refresh_token = minted["refresh_token"]
        return minted["felix_token"]

    return felix.Client(
        "127.0.0.1:5000",
        tenant_id="t1",
        token=minted["felix_token"],
        token_provider=fetch_token,
    )
```

With `AsyncClient` the provider is an `async def`. Use an async HTTP client
such as `aiohttp` for it. The loop stays free without a thread, and when the
client cancels the provider the request stops with it. A blocking call moved to
`asyncio.to_thread` would keep running after the cancel.

The `aiohttp` session has to outlive everything that can call the provider, so
the example is an async context manager that closes the session after the
client block, on every path. Open subscriptions and `with_identity` handles
inside that block: one that outlives it keeps the provider alive with a closed
session. `raise_for_status=True` makes a 403 raise with the status in the
message, and the session timeout stays under the client's 30 seconds.

```python
import contextlib

import aiohttp

import felix

CONTROLPLANE = "https://controlplane:8443/v1/tenants/t1"


@contextlib.asynccontextmanager
async def connect_async(oidc_token):
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(raise_for_status=True, timeout=timeout) as session:

        async def post_json(path, body, bearer=None):
            headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
            async with session.post(CONTROLPLANE + path, json=body, headers=headers) as response:
                return await response.json()

        minted = await post_json(
            "/token/exchange",
            {"requested": ["stream.publish"], "resources": ["stream:t1/default/*"]},
            bearer=oidc_token,
        )
        refresh_token = minted["refresh_token"]

        async def fetch_token():
            nonlocal refresh_token
            minted = await post_json("/token/refresh", {"refresh_token": refresh_token})
            refresh_token = minted["refresh_token"]
            return minted["felix_token"]

        async with await felix.AsyncClient.connect(
            "127.0.0.1:5000",
            tenant_id="t1",
            token=minted["felix_token"],
            token_provider=fetch_token,
        ) as client:
            yield client


async def main(oidc_token):
    async with connect_async(oidc_token) as client:
        await client.publish("t1", "default", "events", b"hello")
```

`with_identity` takes the same argument, so a gateway can give each user a
handle that renews its own token: `client.with_identity("t1",
token_provider=fetch_token)`. A handle with no `token` calls the provider for
its first stream.

## Connecting

```python
import felix

felix.Client(
    addrs,                    # "host:port", or a list of them
    tenant_id="t1",
    token=tok,                # or token_provider=fn; see above
    server_name="localhost",  # the name the broker's certificate carries
    ca_file=None,             # trust a specific CA; omit for the system store
    offer_alpn=False,         # offer the felix/1 ALPN
)
```

One reachable address is enough. The client discovers the rest of the cluster
and will use brokers it was never told about. It is built on the Rust
`ClusterClient`, so it works the same against a single broker.
[Connecting to a cluster](#connecting-to-a-cluster) says what it does with
several.

Give a name rather than an IP where you can. The client resolves a name again
whenever no broker it knows answers, which is how it gets back in after a
rollout has moved every broker at once. A name that does not resolve when
you connect raises `ValueError`.

TLS is always on. QUIC has no unencrypted mode, so there are two trust
choices: the platform trust store, or an explicit `ca_file` for a self-signed
development broker. There is no "skip verification" switch. It would silently
turn a secure deployment insecure, and a CA file covers development without it.

`offer_alpn=True` makes the client offer the `felix/1` ALPN, which a broker
running with `FELIX_TLS_REQUIRE_ALPN=true` insists on. It is off by default
because a broker older than ALPN support refuses a client that offers it.

### Acting for many users

A gateway serving many users can hold one client and give each user a client
that shares its connections:

```python
with client.with_identity("t1", alice_token) as alice:
    alice.publish("t1", "app", "alice-feed", b"hello")
```

Every stream `alice` opens authenticates with her token, so the broker checks
her requests against her grants, not the gateway's. She follows moved shards
and failed brokers like any client, over the gateway's connection to each
broker. A fixed token stays until it expires; pass `token_provider` instead
to renew it. `AsyncClient.with_identity` is the same, awaited. The limits and the
certificate-bound token rules are the Rust client's; see
[Acting for many users](/clients/rust/#acting-for-many-users).

## Connecting to a cluster

In every language the client is a cluster client; see
[Choosing a Client](/clients/overview/#every-client-is-a-cluster-client).

```python
import felix

with felix.Client(
    ["felix-0:5000", "felix-1:5000", "felix-2:5000"],
    tenant_id="t1",
    token=tok,
) as client:
    client.publish("t1", "default", "events", b"hello")
```

`AsyncClient.connect` takes the same list. These are seeds: the client asks
whichever answers which brokers exist and adds them, and it keeps your seeds
whatever the answer, so a stale one cannot leave it worse off. List more than
one so the first connection survives a seed being down.

- **A broker fails.** A publish waiting on it fails once the connection has been
  silent for the idle timeout (6 s by default), with the connection already
  rebuilt from the brokers that remain. `publish` raises; the next call goes to
  a live broker. An at-least-once publish resends instead, and may duplicate
  ([At-least-once may duplicate](#at-least-once-may-duplicate)).
- **A shard moves.** Publishes go to the broker that now leads it, and a
  subscription follows it ([Subscribing](#subscribing)).
- **A subscription's broker fails.** On a durable stream it resubscribes from
  the offset after the last one it delivered, and where it cannot it raises
  rather than ending as though the stream had finished.
- **Knobs.** [Reconnecting](#reconnecting) sets the attempt count and a deadline. `client.endpoints()` (awaited on `AsyncClient`) lists the brokers it would try, discovered
  ones included.

### Reconnecting

Publishes, opening and following subscriptions, commits, `publish_if`, state reads, cache calls and consumer-group calls retry with a backoff when no broker can be reached. Two options tune the retrying, and both default to the Rust client's own policy, so a client that sets neither behaves as it always did.

- **Attempts** is how many times a call tries to reach a broker before it gives up. Each attempt tries every broker the client knows. The default is 5, and the minimum is 1.
- **Deadline** is a ceiling on one call across every attempt and every wait between them. It is off by default, and it is checked between attempts, before each backoff sleep, so a call can last the deadline plus one attempt's own timeout. Attempt counts alone do not bound time: five attempts against a broker that takes its full publish timeout to answer is minutes.

Pass them as `reconnect_attempts` (an int) and `reconnect_deadline` (seconds, a float) to `Client(...)` or `AsyncClient.connect(...)`. The attempt count must be at least 1 and the deadline above 0 and at most one year; anything else, including an int too large for a machine word, raises `ValueError`.

```python
import felix

with felix.Client(
    ["felix-0:5000", "felix-1:5000", "felix-2:5000"],
    tenant_id="t1",
    token=tok,
    reconnect_attempts=10,
    reconnect_deadline=30.0,
) as client:
    try:
        # Stops retrying once 30 s have passed, checked between attempts, and raises the last error.
        client.publish("t1", "default", "orders", b"hello", at_least_once=True)
    except felix.FelixError as err:
        print(f"publish gave up: {err}")
```

A deadline shorter than one attempt's own timeout means no retry happens: the first attempt spends the whole budget and the call returns after trying once. Set it to cover at least one attempt plus the backoff you can afford, and leave it off if you would rather wait out a failover. A client from `with_identity` inherits both.

The Rust page covers [clusters](/clients/rust/#clusters) in full.

## Publishing

```python
client.publish("t1", "default", "orders", payload)                      # acked
client.publish("t1", "default", "orders", payload, ack="none")          # fire and forget
client.publish("t1", "default", "orders", payload, key=customer_id.encode())  # routed
```

`ack` selects what the broker must have done before the call returns:
`"per_message"` (the default), `"per_batch"`, or `"none"`, which promises
nothing and does not wait to find out.

An acked `publish` returns the offset the record landed at, or `None` when the
broker acknowledged it before writing it (a broker that acks on accept:
`FELIX_ACK_ON_COMMIT=0`, or an in-memory stream), the stream has no log, or the broker is too old to say.
`"none"` always returns `None`. Only the broker that owns the shard
acknowledges before writing; a publish forwarded through another broker is
answered after the write and has its offset, so one stream can return both.

### The routing key decides the shard

**Without a key every record lands on shard 0**, so a multi-shard stream
behaves like a single-shard one. If you created a stream with several shards to
get throughput, and you are not passing a key, you are not getting it.

```python
import json

for order in orders:
    client.publish(
        "t1", "default", "orders",
        json.dumps(order).encode(),
        key=order["customer_id"].encode(),
    )
```

Records sharing a key share a shard and stay ordered with respect to each
other. Records with different keys do not, once a stream has more than one
shard. A consumer needing total order wants a single-shard stream.

### At-least-once may duplicate

By default a publish whose outcome was ambiguous (the broker may or may not
have written it before the connection went) is **reported, not re-sent**,
because nothing downstream can tell two copies apart.

```python
client.publish("t1", "default", "orders", payload, at_least_once=True)
```

That is the opt-in. The record is then certain to land and **may land twice**.
It cannot be combined with `key`: the re-send path does not carry one, and
honouring the key on the first attempt but not the re-send would put the
duplicate on a different shard, so the combination raises rather than
resolving.

## Subscribing

```python
with client.subscribe("t1", "default", "events") as events:
    for event in events:
        handle(event.payload)
```

`start` is `"latest"` (the default), `"earliest"`, or an integer offset: **the
first record you have not seen**, so a resuming consumer passes the offset it
last handled *plus one*.

`next_event(timeout=...)` returns `None` both when the timeout passes and when
the broker has ended the stream; `closed` is `True` only after the second, so
check it to tell them apart. A lost connection is neither: on a durable stream
the subscription resubscribes from the offset after the last one it delivered,
and where it cannot, it raises a `FelixError`.

**A subscription follows its shard when a rebalance moves it.** The old owner
ends it after delivering what it committed and says where to resume; the client
resubscribes on the new owner and iteration carries on. On a durable stream
nothing is repeated or skipped; an in-memory stream resumes at the new owner's
tail.

### Offsets are how you notice a drop

Subscriber queues shed under the default policy rather than blocking the
publisher. On a durable stream a reader that falls behind does not lose
records: the broker or this client ends the subscription at the first drop,
and the subscription resubscribes after the last record it delivered,
catching up from the log. A shard reader of a sharded subscription reports the
shard lost and recovered while it does.

Against a broker that predates that, or on an in-memory stream, a subscriber
can still silently miss records. On a durable stream every
delivered event carries its log offset, and **a jump in them is a drop**. The
exception is a new leader's generation-start record, which takes an offset; the
next event reports it in `skipped_before`:

```python
import logging

log = logging.getLogger(__name__)

expected = None
with client.subscribe("t1", "default", "events") as events:
    for event in events:
        start = event.offset - event.skipped_before
        if expected is not None and start != expected:
            log.warning("dropped %d records", start - expected)
        expected = event.offset + 1
        handle(event.payload)
```

That is worth writing even when you do not resume from offsets. It is the only
signal that the queue overflowed.

### A consumer that survives a restart

The pattern the offset semantics exist for: checkpoint what you handled, and
resume at the next one.

```python
import logging
import time

import felix

log = logging.getLogger(__name__)


def run(client, checkpoint, handle):
    last = checkpoint.load()             # None on a cold start
    start = last + 1 if last is not None else "earliest"

    while True:
        try:
            with client.subscribe("t1", "default", "events", start=start) as events:
                for event in events:
                    handle(event.payload)
                    checkpoint.save(event.offset)
                    start = event.offset + 1
            return                       # the broker ended the stream
        except felix.ConnectionError:
            # Worth retrying, and the client has already tried other brokers.
            # Resuming from `start` is what makes it lossless.
            log.warning("subscription lost; resuming at %s", start, exc_info=True)
            time.sleep(1)
        except felix.CursorError:
            # Retention discarded the offset. The only recovery is to accept
            # the gap and say so. Silently restarting at the tail would lose
            # records without telling anyone.
            log.error("checkpoint %s is past retention; restarting at earliest", start)
            start = "earliest"
```

`handle` runs before the checkpoint is saved, so a crash between the two
repeats one record rather than losing it. The two `except` clauses do
different things. The recovery differs, so the error types differ.

## Errors you can act on

```python
import felix

try:
    client.publish("t1", "default", "orders", payload)
except felix.ShardUnavailableError:
    retry()          # the shard is moving; nothing was written
except felix.OutcomeUnknownError:
    reconcile()      # it may have been written; resend only if idempotent
except felix.ConnectionError:
    retry()          # worth another attempt, against another broker
except felix.AuthError:
    give_up()        # no amount of retrying grants a permission
except felix.NotFoundError:
    create_stream()  # or wait: a broker promoted a moment ago may not know it yet
```

| Exception | What it means | Retry? |
| --- | --- | --- |
| `ConnectionError` | broker unreachable, the connection died mid-call, or the broker is shutting down (`draining`) | yes, elsewhere |
| `ShardUnavailableError` | nobody can serve the shard right now, usually because it is moving (`shard_unavailable`), or another broker owns it (`not_leader`); nothing was written | yes |
| `OverloadedError` | the broker is shedding load (`overloaded`); nothing was written | yes, after a pause |
| `OutcomeUnknownError` | the write may or may not have happened (`quorum_timeout`, `leadership_lost`, `unacknowledged`, or any error sent as `outcome_unknown`) | only if idempotent |
| `AuthError` | token rejected or revoked, or missing the permission (`unauthenticated`, `forbidden`, `revoked`) | no |
| `NotFoundError` | no such tenant, namespace, stream or cache (`not_found`) | see `retry` |
| `CursorError` | the start offset is gone; retention discarded it | no; restart at `earliest` |
| `FelixError` | the base, and anything else (`invalid_request`, `limit_exceeded`, a code this client does not know) | see `retry` |

Every exception carries three attributes from the broker:

- `code`: the broker's [error code](https://github.com/GetFelix/felix/blob/main/docs/protocol.md#error-codes),
  such as `"shard_unavailable"` or `"quorum_timeout"`.
- `retry`: what you may do about it: `"retry"`, `"retry_after"`,
  `"redirect"`, `"outcome_unknown"` or `"fatal"`.
- `detail`: a dict of extra facts, such as `{"reason": "fenced"}` for an
  unavailable shard or `retry_after_ms`, or `None`.

The class is picked from `code`, except that an `outcome_unknown` retry class
always makes an `OutcomeUnknownError`: whatever went wrong, "this may have been
written" decides what you do next. When the two disagree, act on `retry`. A
code this client does not know still carries a retry class.

All three are `None` when the broker predates error codes, or when the failure
happened in the client (a lost connection, say). Then the class is chosen from
the message.

Never match on the message. It is prose and it will be reworded; match on the
exception type or `code` instead.

## Queues

A queue works differently from a subscription. Records are **pulled**, because
only the consumer knows when it has capacity; each is claimed by one member
until settled; and an unsettled record comes back.

```python
while True:
    records = client.group_poll(
        "t1", "default", "orders", shard, "billing",
        max_records=32,
        wait=5.0,          # long poll; an empty list is an answer, not an error
    )
    for record in records:
        try:
            charge(record.payload)
            client.group_ack("t1", "default", "orders", shard, "billing", record.offset)
        except RetryableError:
            # Back to the queue now, rather than after the visibility timeout.
            client.group_nack("t1", "default", "orders", shard, "billing", record)
```

A nack takes the record, not its offset, because it names that delivery: once
the claim has lapsed and the record has gone out again, the nack is refused
with `stale_claim` rather than taking it from the consumer now holding it.

`record.attempts` counts deliveries **including this one**, so `1` is a first
attempt and anything higher is a redelivery. It is how a consumer tells a retry
from a first try, which is worth branching on before doing anything expensive or
side-effecting:

```python
for record in records:
    if record.attempts > 3:
        quarantine(record)
        # Settle it, so it is not delivered again.
        client.group_ack("t1", "default", "orders", shard, "billing", record.offset)
        continue
    charge(record.payload)
    client.group_ack("t1", "default", "orders", shard, "billing", record.offset)
```

**A group is bound to one shard.** Consuming a multi-shard stream means polling
each shard's group; `stream_shards` says how many there are. Only the shard's leader
serves its group; the client follows the broker's redirect there, including
after a rebalance moves the shard, so any broker address works.

### Dead letters

Past the attempt bound a record is set aside rather than retried forever:

```python
for offset in client.group_dead_letters("t1", "default", "orders", shard, "billing"):
    if fixed_the_cause:
        client.group_redrive("t1", "default", "orders", shard, "billing", offset)
    else:
        client.group_discard("t1", "default", "orders", shard, "billing", offset)
```

## Cache and counters

```python
client.cache_put("t1", "default", "sessions", "user-abc", data, ttl=3600.0)
value = client.cache_get("t1", "default", "sessions", "user-abc")      # bytes or None
removed = client.cache_delete("t1", "default", "sessions", "user-abc") # bytes or None
```

`ttl` is in seconds and takes a float, so `ttl=0.5` is half a second. Leave it
out and the entry has no time-to-live. `cache_get` returns `None` for a key that is
missing or expired. `cache_delete` returns the value it removed, or `None` if
the key was not there, so you can tell a delete that did something from one
that did not.

Counters live beside the cache and use the same scoping:

```python
total = client.counter_add("t1", "default", "limits", "user:42:reqs", 1)  # sum after the add
current = client.counter_get("t1", "default", "limits", "user:42:reqs")   # int or None
```

`counter_add` takes a signed delta and returns the sum including it.
`counter_get` returns `None` for a counter that was never written, which is not
the same as zero. A retry after a lost acknowledgement counts twice. Counters
need a durable broker that advertises `FEATURE_COUNTERS`.

Cache and counter calls go to the key's shard owner when the client knows it,
like a publish. If the broker the client entered by goes away, the next call
moves to another one: a read (a get) is asked again there, while a write that
may have landed (a put, delete or add) returns its error with the client
already moved, and is not sent twice. `stream_shards` moves the same way.

`AsyncClient` has the same methods as coroutines.

## Cache watches

Watches can resume by offset and report loss explicitly, which makes the cache
usable for state synchronisation.

```python
import felix

def mirror(client, roster):
    """Keep `roster` equal to the room's entries until the broker ends the watch."""
    resume_from = None
    while True:
        with client.watch_cache(
            "t1", "default", "sessions",
            felix.CacheWatchFilter.prefix("room:42:"),
            start=resume_from,
            retained=resume_from is None,
        ) as watch:
            if resume_from is None:
                # Retained values arrive first, and the count says exactly how
                # many. Zero is a definite answer (the prefix is empty), not a
                # silence to wait through.
                roster.clear()
                for _ in range(watch.retained_count):
                    change = watch.recv()
                    roster[change.key] = change.value
            elif watch.resnapshot:
                # Compaction passed resume_from, so deletes since then are gone.
                # Start over from current state.
                resume_from = None
                continue
            resume_from = None

            # State is now complete. Everything after this is live.
            for item in watch:
                if isinstance(item, felix.CacheWatchLagged):
                    # Not an error: the watch did its job by saying so. Offsets
                    # on a filtered watch are sparse, so loss cannot be inferred
                    # the way a stream subscriber infers it. This is the only
                    # signal.
                    resume_from = item.resume_from
                    break
                if isinstance(item, felix.CacheWatchShardMoved):
                    # The shard moved to another broker; the watch follows it.
                    continue
                if item.value is None:
                    roster.pop(item.key, None)   # a delete is a change with no value
                else:
                    roster[item.key] = item.value
        if resume_from is None:
            return                               # the broker ended the watch
```

Three things in that example matter:

- **`retained_count`** tells you the moment your state is complete. Without it
  you are guessing when to start trusting the map.
- **`value is None` means removed**, and is deliberately distinguishable from
  an empty value. A watcher mirroring a cache has to tell those apart.
- **`CacheWatchLagged` is a value, not an exception.** Re-watching from
  `resume_from` is gapless. `CacheWatchShardMoved` is only a notice: the watch
  follows its shard to the new owner and carries on, with no change repeated
  or skipped.

A prefix watch reads **one shard**, so a prefix spanning a multi-shard cache
needs one watch per shard. On a multi-shard cache the broker refuses a prefix
watch that names no shard, rather than quietly reading shard 0. The merged
helper, `watch_cache_sharded`, is Rust-only for now.

## Atomic commits

`commit` writes an event and the state it changes as one record on the shard
an entity key routes to. A subscriber, a consumer group and `state_get` all
see the whole commit or none of it, at the same offset.

```python
import felix

stream = "order-events"
receipt = client.commit(
    "acme",
    "orders",
    b"order-42",
    [
        felix.CommitOp.enqueue(stream, b'{"type":"placed","id":42}'),
        felix.CommitOp.put(stream, "order-42", b'{"status":"placed"}'),
        felix.CommitOp.delete(stream, "cart-42"),
    ],
)
print(receipt.offset)  # where the event is read, and the state's version

state = client.state_get("acme", "orders", stream, b"order-42", "order-42")
print(state.value, state.version)  # the version is receipt.offset
```

`AsyncClient` has the same two methods, awaited.

| Call | Returns |
| --- | --- |
| `commit(tenant_id, namespace, entity_key, ops)` | `CommitReceipt(offset)` |
| `state_get(tenant_id, namespace, stream, entity_key, key)` | `StateValue(value, version, as_of)`; `value` is `None` for a key never written or deleted |
| `CommitOp.publish(stream, payload)`, `CommitOp.enqueue(queue, payload)` | the commit's one event, for subscribers or for consumer groups (the same record: a queue is a stream read through a group) |
| `CommitOp.put(stream, key, value)`, `CommitOp.delete(stream, key)` | state changes, applied in order |

A commit is refused before anything is sent when it cannot be one record:

```python
import logging

import felix

log = logging.getLogger(__name__)

try:
    client.commit("acme", "orders", b"order-42", [
        felix.CommitOp.publish("order-events", b"placed"),
        felix.CommitOp.put("inventory", "sku-1", b"3"),   # another stream
    ])
except felix.NotOnOwningShardError as err:
    # err.index == 1, err.stream == "inventory", err.owner == "order-events"
    log.error("op %d writes %s, not %s", err.index, err.stream, err.owner)
    raise
except felix.EventCountError as err:
    log.error("a commit needs exactly one event, not %d", err.count)
    raise
```

Both are mistakes in the ops, so the example logs and re-raises rather than
retrying. Both are subclasses of `CommitError`, itself a `FelixError`. Failures after the
commit was sent are the ordinary classes above; note that a commit is **not
idempotent**, so an `OutcomeUnknownError` means it may already be written.

**What atomic covers:** every part of one commit, on one shard's log, across a
failover. **What it does not:** another stream or shard, a Felix cache (a
commit's state is the stream shard's own, read with `state_get`), or a retry.
State is rebuilt from the retained log, so keep an entity stream's retention
long enough. The stream must be durable, and in a cluster the operator must
finalize the `atomic_commit` fleet feature first. The full semantics are in
[`docs/atomic-commit.md`](https://github.com/GetFelix/felix/blob/main/docs/atomic-commit.md).

## Multi-shard streams

A subscription reads one shard. `subscribe_sharded` opens one per shard,
follows each shard's own owner, and merges them:

```python
import logging

import felix

log = logging.getLogger(__name__)

with client.subscribe_sharded("t1", "default", "orders") as stream:
    for item in stream:
        if isinstance(item, felix.ShardRecord):
            handle(item.event.payload)
        elif isinstance(item, felix.ShardLost):
            # Surfaced rather than swallowed: the other shards carry on, so a
            # consumer that ignored this would be reading part of the stream
            # while believing it read all of it.
            alert(item.shard, item.error)
        elif isinstance(item, felix.ShardRecovered):
            log.info("shard %d back", item.shard)
        elif isinstance(item, felix.ShardMoved):
            # Followed to its new owner; its records carry on from there.
            log.info("shard %d moved to %s", item.shard, item.node_id)
        elif isinstance(item, felix.StreamDeleted):
            # The stream is gone; this shard is not reopened.
            log.info("shard %d: stream deleted", item.shard)
```

Resuming is a **map**, not a number, because offsets are per shard.
`positions()` returns `{shard: last offset handled}`; pass it back as `resume=`
and each listed shard continues after its offset:

```python
import felix

resume = checkpoint.load()             # None on a cold start
with client.subscribe_sharded("t1", "default", "orders", resume=resume) as stream:
    for item in stream:
        if isinstance(item, felix.ShardRecord):
            handle(item.event.payload)
            checkpoint.save(stream.positions())
```

Read `positions()` while the subscription is open: a closed one returns an
empty map.

A single offset carried across shards would replay on every shard but one.

:::caution[A shard that delivered nothing has no position]
`positions()` only lists shards that handed something over. On resume, a shard
not in the map starts wherever `start` says. That is the live tail by
default, so records published to it while you were away are missed. If that
matters, pass `start="earliest"` alongside `resume`, or make sure every shard
has reported before you checkpoint.
:::

## Concurrency

One client is meant to be shared. The synchronous surface blocks with the GIL
released, so a thread pool over one client runs in parallel:

```python
import concurrent.futures


def publish(payload):
    return client.publish("t1", "default", "events", payload)


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    # Reading the results re-raises the first failed publish.
    offsets = list(pool.map(publish, payloads))
```

Creating a client per thread is the thing to avoid: each is a QUIC endpoint
with its own connection pool, and they do not share discovery.

## What is not wrapped

These are in the [Rust client](/clients/rust/) and not in this binding yet:

- **Conditional writes.** `cache_put_if`, `cache_delete_if` and
  `cache_get_versioned` (compare-and-set on a cache key), and the conditional
  `publish_if` and `commit_if`. Without them a lease or lock cannot be built
  from Python.
- **Atomic commits on a named shard** (`commit_on_shard`). `commit` routes by
  key only.
- **Merged prefix watches** (`watch_cache_sharded`). A prefix on a
  multi-shard cache needs one `watch_cache` per shard.
- **Idempotent producers.** At-least-once without the duplicate; see
  [the Rust page](/clients/rust/#idempotent-producers).
- **`at_least_once` with a routing key.** The re-send path does not carry one,
  and the client refuses the combination rather than silently dropping the key.
- **Group lease extension and administration.** `group_extend`, `group_seek`,
  and creating, describing or deleting a group. Groups are created with
  `felixctl`.
- **Starting from a time** (`offset_for_time`).

The ones the conformance catalogue covers are marked there, so the binding
reports them as unclaimed.

## Conformance

Python passes every required scenario in the
[client conformance catalogue](/clients/overview/#the-conformance-suite),
and CI is gated on it. The suite runs against a real three-node cluster rather
than a mock, because what it checks (reconnection, redirect-following, offset
accounting) only exists in a cluster.
