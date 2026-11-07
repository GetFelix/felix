"""Features the Rust client has that Python gained to match it: record headers,
publisher and timestamp on events, lease writes, broker inspection,
subscription counters, client tuning and producer hand-off.

The argument checks need no cluster; the rest run against the shared fixture.
"""

from __future__ import annotations

import asyncio

import pytest

import felix

TIMEOUT = 20.0


def connect(fixture, **options):
    return felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
        **options,
    )


def scope(fixture):
    return fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]


def find(subscription, payload, attempts=200):
    for _ in range(attempts):
        event = subscription.next_event(timeout=TIMEOUT)
        if event is None:
            return None
        if event.payload == payload:
            return event
    return None


# ---------------------------------------------------------------------------
# Record headers
# ---------------------------------------------------------------------------


def test_record_headers_keep_order_repeats_and_the_record_key():
    headers = felix.RecordHeaders([("a", b"1"), ("b", b"2"), ("a", b"3")], key=b"k")
    assert headers.headers == [("a", b"1"), ("b", b"2"), ("a", b"3")]
    assert headers.key == b"k"
    assert headers.get("a") == b"1"
    assert headers.get("missing") is None
    assert len(headers) == 3
    assert felix.RecordHeaders({"x": b"y"}).headers == [("x", b"y")]


def test_record_headers_reject_what_is_not_names_and_bytes():
    with pytest.raises(TypeError):
        felix.RecordHeaders([("a", 1)])
    with pytest.raises(TypeError):
        felix.RecordHeaders(5)


def test_headers_and_a_routing_key_are_refused_together(client, fixture, key):
    with pytest.raises(ValueError, match="headers"):
        client.publish(*scope(fixture), b"x", key=b"route", headers={"a": b"1"})
    with pytest.raises(ValueError, match="headers"):
        client.publish(*scope(fixture), b"x", at_least_once=True, headers={"a": b"1"})
    with pytest.raises(ValueError, match="payloads"):
        client.publish_batch(*scope(fixture), [b"a", b"b"], headers=[{"a": b"1"}])
    with pytest.raises(ValueError, match="headers"):
        client.publish_batch(*scope(fixture), [b"a"], key=b"route", headers=[{"a": b"1"}])


def test_a_reserved_header_name_is_refused_and_nothing_is_sent(client, fixture, key):
    with pytest.raises(felix.FelixError, match="reserved"):
        client.publish(*scope(fixture), key.encode(), headers={"felix.mine": b"1"})


def test_headers_reach_the_subscriber(client, fixture, key):
    payload = f"headers-{key}".encode()
    with client.subscribe(*scope(fixture)) as events:
        client.publish(
            *scope(fixture),
            payload,
            headers=felix.RecordHeaders({"trace": b"abc"}, key=b"own-key"),
        )
        event = find(events, payload)
    assert event is not None, "nothing arrived"
    assert event.headers is not None
    assert event.headers.get("trace") == b"abc"
    assert event.headers.key == b"own-key"


def test_a_batch_carries_headers_per_record(client, fixture, key):
    plain, tagged = f"{key}-plain".encode(), f"{key}-tagged".encode()
    with client.subscribe(*scope(fixture)) as events:
        client.publish_batch(*scope(fixture), [plain, tagged], headers=[None, {"n": b"2"}])
        first, second = find(events, plain), find(events, tagged)
    assert first is not None and second is not None
    assert first.headers is None
    assert second.headers.get("n") == b"2"


def test_without_headers_an_event_has_none(client, fixture, key):
    payload = f"bare-{key}".encode()
    with client.subscribe(*scope(fixture)) as events:
        client.publish(*scope(fixture), payload)
        event = find(events, payload)
    assert event is not None and event.headers is None


# ---------------------------------------------------------------------------
# Publisher and timestamp
# ---------------------------------------------------------------------------


def test_publisher_and_timestamp_are_off_unless_asked_for(client, fixture, key):
    payload = f"plain-{key}".encode()
    with client.subscribe(*scope(fixture)) as events:
        client.publish(*scope(fixture), payload)
        event = find(events, payload)
    assert event is not None
    assert event.publisher is None
    assert event.timestamp_micros is None


def test_a_client_that_asks_gets_the_timestamp_and_publisher(fixture, key):
    payload = f"stamped-{key}".encode()
    with connect(fixture, publishers=True, timestamps=True) as stamped:
        with stamped.subscribe(*scope(fixture)) as events:
            stamped.publish(*scope(fixture), payload)
            event = find(events, payload)
    assert event is not None
    assert event.timestamp_micros is not None and event.timestamp_micros > 0
    assert event.publisher is None or isinstance(event.publisher, str)


def test_a_group_record_carries_the_timestamp_when_asked(fixture, key):
    group = f"stamped-{key}"
    tenant, namespace, stream = scope(fixture)
    payload = key.encode()
    with connect(fixture, timestamps=True) as stamped:
        stamped.publish(tenant, namespace, stream, payload)
        mine = []
        for _ in range(10):
            records = stamped.group_poll(
                tenant, namespace, stream, 0, group, max_records=64, wait=2.0
            )
            mine = [record for record in records if record.payload == payload]
            for record in records:
                stamped.group_ack(tenant, namespace, stream, 0, group, record.offset)
            if mine:
                break
    assert mine, "the group never handed out the record"
    assert mine[0].timestamp_micros is not None and mine[0].timestamp_micros > 0


# ---------------------------------------------------------------------------
# Tuning
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [
        {"publish_conn_pool": 0},
        {"client_sub_queue_capacity": 0},
        {"publish_sharding": "random"},
        {"client_sub_queue_policy": "newest"},
        {"connect_timeout": 0.0},
        {"finish_timeout": -1.0},
    ],
)
def test_tuning_refuses_values_the_client_cannot_use(bad):
    with pytest.raises(ValueError):
        felix.ClientTuning(**bad)


def test_a_tuned_client_publishes_and_reads_back(fixture, key):
    tuning = felix.ClientTuning(
        publish_conn_pool=2,
        publish_sharding="round_robin",
        client_sub_queue_capacity=64,
        client_sub_queue_policy="block",
        max_frame_bytes=8 * 1024 * 1024,
        finish_timeout=30.0,
    )
    payload = f"tuned-{key}".encode()
    with connect(fixture, tuning=tuning) as tuned:
        with tuned.subscribe(*scope(fixture)) as events:
            tuned.publish(*scope(fixture), payload)
            assert find(events, payload) is not None


# ---------------------------------------------------------------------------
# Subscription counters
# ---------------------------------------------------------------------------


def test_a_subscription_reports_its_counters(client, fixture, key):
    payload = f"counters-{key}".encode()
    with client.subscribe(*scope(fixture)) as events:
        assert (events.moves, events.reconnects, events.stream_deleted) == (0, 0, False)
        client.publish(*scope(fixture), payload)
        assert find(events, payload) is not None
        assert events.moves == 0 and events.reconnects == 0
        assert events.queue_capacity is None or events.queue_capacity > 0


def test_the_async_subscription_reports_its_counters(fixture, key):
    payload = f"async-counters-{key}".encode()

    async def run():
        async with await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        ) as client:
            async with await client.subscribe(*scope(fixture)) as events:
                await client.publish(*scope(fixture), payload)
                event = await events.next_event(timeout=TIMEOUT)
                return event, events.moves, events.reconnects, events.stream_deleted

    event, moves, reconnects, deleted = asyncio.run(run())
    assert event is not None
    assert (moves, reconnects, deleted) == (0, 0, False)


# ---------------------------------------------------------------------------
# Lease writes
# ---------------------------------------------------------------------------


def test_a_lease_write_claims_a_key_once(client, fixture, key):
    cache = (fixture["tenant_id"], fixture["namespace"], fixture["cache"])
    first = client.cache_put_if_lease(*cache, key, b"holder-a", felix.CacheCondition.absent())
    assert first.result.applied and first.result.version is not None
    assert first.token is None or isinstance(first.token, felix.LeaseToken)
    if first.token is not None:
        assert first.token.token not in repr(first) and first.token.token not in repr(first.token)

    second = client.cache_put_if_lease(*cache, key, b"holder-b", felix.CacheCondition.absent())
    assert not second.result.applied
    assert second.token is None
    assert client.cache_get(*cache, key) == b"holder-a"


def test_the_async_client_writes_a_lease_too(fixture, key):
    async def run():
        async with await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        ) as client:
            cache = (fixture["tenant_id"], fixture["namespace"], fixture["cache"])
            return await client.cache_put_if_lease(
                *cache, key, b"v", felix.CacheCondition.absent()
            )

    assert asyncio.run(run()).result.applied


# ---------------------------------------------------------------------------
# Inspection
# ---------------------------------------------------------------------------


def test_supports_names_every_question(client):
    assert set(client.supports()) == {
        "inspect",
        "topology",
        "ack_on_commit",
        "stream_shards",
        "cache_shards",
        "shard_owners",
    }


def test_shard_counts_owners_and_routing_agree(client, fixture):
    tenant, namespace, stream = scope(fixture)
    shards, routing = client.stream_routing(tenant, namespace, stream)
    assert shards >= 1 and routing in ("modulo", "jump_hash")
    assert shards == client.stream_shards(tenant, namespace, stream)
    owners = client.shard_owners("stream", tenant, namespace, stream)
    assert sorted(owner["shard"] for owner in owners) == list(range(shards))
    assert client.cache_shards(tenant, namespace, fixture["cache"]) >= 1
    assert isinstance(client.topology(), list)


def test_a_kind_must_be_stream_or_cache(client, fixture):
    tenant, namespace, stream = scope(fixture)
    with pytest.raises(ValueError, match="kind"):
        client.shard_owners("queue", tenant, namespace, stream)
    with pytest.raises(ValueError, match="kind"):
        client.inspect_shard("queue", tenant, namespace, stream, 0)


def test_a_tenant_token_may_not_inspect(client, fixture):
    tenant, namespace, stream = scope(fixture)
    with pytest.raises(felix.FelixError):
        client.inspect_shard("stream", tenant, namespace, stream, 0)
    with pytest.raises(felix.FelixError):
        client.list_subscriptions(stream=stream)


def test_a_cursor_must_be_the_dict_a_page_returned(client):
    with pytest.raises(ValueError, match="cursor"):
        client.list_subscriptions(cursor={"tenant_id": "t"})


def test_the_async_client_asks_the_same_questions(fixture):
    async def run():
        async with await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        ) as client:
            tenant, namespace, stream = scope(fixture)
            return (
                await client.supports(),
                await client.stream_routing(tenant, namespace, stream),
                await client.shard_owners("stream", tenant, namespace, stream),
                await client.cache_shards(tenant, namespace, fixture["cache"]),
            )

    supported, routing, owners, cache_shards = asyncio.run(run())
    assert "inspect" in supported
    assert routing[0] == len(owners) and cache_shards >= 1


# ---------------------------------------------------------------------------
# Producer hand-off
# ---------------------------------------------------------------------------


def test_handed_off_batches_keep_their_order_and_answer_once(client, fixture, key):
    tenant, namespace, stream = scope(fixture)
    with client.producer() as producer:
        handed = [
            producer.hand_off_batch(tenant, namespace, stream, [f"{key}-{n}".encode()] * 2)
            for n in range(3)
        ]
        offsets = [h.offset() for h in handed]
        assert offsets[0] is not None
        assert offsets == [offsets[0], offsets[0] + 2, offsets[0] + 4]
        with pytest.raises(RuntimeError, match="already"):
            handed[0].offset()
        assert producer.hand_off_batches(tenant, namespace, stream, [[b"a"], [b"b"]]).done() is None


def test_an_async_producer_hands_off_too(fixture, key):
    async def run():
        async with await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        ) as client:
            producer = await client.producer()
            async with producer:
                handed = await producer.hand_off_batch(*scope(fixture), [key.encode()])
                return await handed.offset()

    assert asyncio.run(run()) is not None
