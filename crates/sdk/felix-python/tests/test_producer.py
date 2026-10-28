"""Idempotent producers and named-shard publishes.

The refusals are asserted as specifically as the successes. A producer that
advanced its sequence on a failure, or a named-shard publish that fell back to
hashing a key, would look fine until the first retry.
"""

from __future__ import annotations

import asyncio
import time
import urllib.request

import pytest

import felix

STREAM = "durable_stream"


def last_shard(client, fixture):
    """The stream's last shard, so named-shard tests never name one it lacks."""
    count = client.stream_shards(fixture["tenant_id"], fixture["namespace"], fixture[STREAM])
    assert count >= 1, f"the stream reports {count} shards"
    return count - 1


def publish_on_shard(client, fixture, shard, payloads):
    """A named-shard publish, retried while the entry broker's routing catches up."""
    deadline = time.monotonic() + 30
    while True:
        try:
            return client.publish_on_shard(
                fixture["tenant_id"], fixture["namespace"], fixture[STREAM], shard, payloads
            )
        except felix.ShardUnavailableError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.2)


def find(subscription, payload, attempts=200):
    for _ in range(attempts):
        item = subscription.next_event(timeout=15.0)
        if item is None:
            return None
        if isinstance(item, felix.ShardRecord) and item.event.payload == payload:
            return item
    return None


@pytest.mark.scenario("ack.carries_the_offset")
def test_a_producer_reports_where_each_batch_landed(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    with client.producer() as producer:
        assert producer.id != 0
        assert producer.id_for(tenant, namespace, stream) == producer.id
        first = producer.publish_batch(tenant, namespace, stream, [b"a", b"b"])
        second = producer.publish(tenant, namespace, stream, b"c")
    assert first is not None, "the broker reported no offset for an idempotent batch"
    assert second == first + 2, f"a batch after two records landed at {second}, not {first + 2}"

    one = client.publish(tenant, namespace, stream, f"{key}-1".encode())
    two = client.publish(tenant, namespace, stream, f"{key}-2".encode())
    if one is not None:
        assert two == one + 1


def test_a_producer_takes_keyed_batches_and_several_batches(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    payload = f"{key}-keyed".encode()
    with client.subscribe_sharded(tenant, namespace, stream) as subscription:
        with client.producer() as producer:
            assert producer.publish_batch(
                tenant, namespace, stream, [payload], key=key.encode()
            ) is not None
            producer.publish_batches(tenant, namespace, stream, [[b"x", b"y"], [b"z"]])
        landed = find(subscription, payload)
        client.publish(tenant, namespace, stream, f"{key}-plain".encode(), key=key.encode())
    assert landed is not None, "the keyed batch never reached a subscriber"
    # The producer routes a key as a plain publish does.
    with client.subscribe_sharded(tenant, namespace, stream) as subscription:
        plain = f"{key}-after".encode()
        client.publish(tenant, namespace, stream, plain, key=key.encode())
        assert find(subscription, plain).shard == landed.shard


def test_a_producer_refuses_empty_input_before_sending(client, fixture):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    with client.producer() as producer:
        with pytest.raises(ValueError):
            producer.publish_batch(tenant, namespace, stream, [])
        with pytest.raises(ValueError):
            producer.publish_batches(tenant, namespace, stream, [[b"a"], []])
        # Nothing was numbered, so the next batch still goes through.
        assert producer.publish(tenant, namespace, stream, b"after") is not None


def test_abandoning_numbers_the_stream_afresh_under_a_new_id(client, fixture):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    with client.producer() as producer:
        producer.publish(tenant, namespace, stream, b"before")
        before = producer.id_for(tenant, namespace, stream)
        # Every sequence on the stream restarts, whether or not a batch was in doubt.
        producer.abandon_in_doubt(tenant, namespace, stream)
        assert producer.id_for(tenant, namespace, stream) != before
        assert producer.id == before
        assert producer.publish(tenant, namespace, stream, b"after") is not None


def test_a_closed_producer_refuses_calls_and_closes_again_quietly(client, fixture):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    producer = client.producer()
    producer.publish(tenant, namespace, stream, b"before")
    assert not producer.closed
    producer.close(timeout=5.0)
    assert producer.closed
    with pytest.raises(felix.FelixError):
        producer.publish(tenant, namespace, stream, b"late")
    producer.close()
    with pytest.raises(ValueError):
        client.producer().close(timeout=-1.0)


@pytest.mark.scenario("explicit_shard.negotiated")
def test_a_named_shard_is_sent_only_to_a_broker_that_advertised_it(fixture, key):
    addrs = fixture.get("no_commit_addrs")
    control_url = fixture.get("control_url")
    if not addrs or not control_url:
        pytest.skip("the fixture file has no no_commit_addrs or control_url")
    with felix.Client(
        addrs,
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    ) as bare:
        with pytest.raises(felix.FelixError):
            bare.publish_on_shard(
                fixture["tenant_id"], fixture["namespace"], fixture[STREAM], 0, [key.encode()]
            )
    with urllib.request.urlopen(control_url + "/no-commit/requests", timeout=10) as response:
        assert int(response.read()) == 0, "the broker was sent a named-shard publish"


@pytest.mark.scenario("explicit_shard.out_of_range_is_refused")
def test_a_shard_the_stream_lacks_is_refused_and_nothing_is_written(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    count = client.stream_shards(tenant, namespace, stream)
    probe = publish_on_shard(client, fixture, count - 1, [f"{key}-probe".encode()])
    with pytest.raises(felix.FelixError) as refused:
        client.publish_on_shard(tenant, namespace, stream, count, [b"lost"])
    assert refused.value.code == "invalid_request", f"{refused.value!r}"
    with pytest.raises(felix.FelixError) as refused:
        client.commit_on_shard(
            tenant, namespace, count, key.encode(), [felix.CommitOp.publish(stream, b"lost")]
        )
    assert refused.value.code == "invalid_request", f"{refused.value!r}"
    # The shard's next offset did not move.
    if probe is not None:
        again = publish_on_shard(client, fixture, count - 1, [f"{key}-again".encode()])
        assert again == probe + 1


def test_a_batch_on_a_named_shard_lands_on_that_shard(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    shard = last_shard(client, fixture)
    payloads = [f"{key}-1".encode(), f"{key}-2".encode()]
    with client.subscribe_sharded(tenant, namespace, stream) as subscription:
        first = publish_on_shard(client, fixture, shard, payloads)
        landed = find(subscription, payloads[1])
    assert landed is not None, "the batch never reached a subscriber"
    assert landed.shard == shard
    if first is not None:
        assert landed.event.offset == first + 1


def test_a_batch_on_a_named_shard_needs_a_payload(client, fixture):
    with pytest.raises(ValueError):
        client.publish_on_shard(
            fixture["tenant_id"], fixture["namespace"], fixture[STREAM], 0, []
        )


def test_a_keyed_record_can_be_sent_at_least_once(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture[STREAM]
    plain, resent = f"{key}-plain".encode(), f"{key}-resent".encode()
    with client.subscribe_sharded(tenant, namespace, stream) as subscription:
        client.publish(tenant, namespace, stream, plain, key=key.encode())
        client.publish(tenant, namespace, stream, resent, key=key.encode(), at_least_once=True)
        first = find(subscription, plain)
        second = find(subscription, resent)
    assert first is not None and second is not None, "a record never reached a subscriber"
    # The key still decides the shard on the re-send path.
    assert second.shard == first.shard


def test_the_async_client_publishes_idempotently_and_on_a_shard(fixture, key):
    stream = fixture[STREAM]

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        scope = (fixture["tenant_id"], fixture["namespace"])
        async with await client.producer() as producer:
            first = await producer.publish_batch(*scope, stream, [b"a", b"b"], key=key.encode())
            await producer.publish_batches(*scope, stream, [[b"c"], [b"d"]])
            before = await producer.id_for(*scope, stream)
            await producer.abandon_in_doubt(*scope, stream)
            after = await producer.id_for(*scope, stream)
            ident = (producer.id, before, after)
        assert await producer.closed
        with pytest.raises(felix.FelixError):
            await producer.publish(*scope, stream, b"late")
        shard = await client.stream_shards(*scope, stream) - 1
        on_shard = await client.publish_on_shard(*scope, stream, shard, [b"e"])
        with pytest.raises(felix.FelixError) as refused:
            await client.publish_on_shard(*scope, stream, shard + 1, [b"lost"])
        keyed = await client.publish(
            *scope, stream, b"f", key=key.encode(), at_least_once=True
        )
        return first, ident, on_shard, refused.value, keyed

    first, ident, on_shard, refused, keyed = asyncio.run(run())
    assert first is not None
    # A stream given up is numbered afresh under an id of its own.
    assert ident[0] == ident[1] and ident[2] != ident[0]
    assert refused.code == "invalid_request"
