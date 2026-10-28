"""Reading a stream's past: where a time begins, and pages of committed records."""

from __future__ import annotations

import time

import pytest

import felix


def now_micros():
    return time.time_ns() // 1000


def shard0(fixture):
    return fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]


@pytest.mark.scenario("stream.offset_for_time_finds_the_first_record_at_or_after")
def test_a_time_resolves_to_the_first_record_at_or_after_it(client, fixture, key):
    tenant, namespace, stream = shard0(fixture)
    earlier = client.publish(tenant, namespace, stream, f"a-{key}".encode(), ack="per_message")
    assert earlier is not None, "the fixture stream reports no offsets"
    time.sleep(0.1)
    boundary = now_micros()
    time.sleep(0.1)
    later = client.publish(tenant, namespace, stream, f"b-{key}".encode(), ack="per_message")

    assert client.offset_for_time(tenant, namespace, stream, 0, boundary) == later
    # A time before everything answers with the oldest record, not nothing.
    oldest = client.offset_for_time(tenant, namespace, stream, 0, 1)
    assert oldest is not None and oldest <= earlier
    # No committed record is that recent yet.
    future = now_micros() + 3_600_000_000
    assert client.offset_for_time(tenant, namespace, stream, 0, future) is None


@pytest.mark.scenario("stream.read_returns_committed_pages")
def test_a_range_can_be_read_in_pages(client, fixture, key):
    tenant, namespace, stream = shard0(fixture)
    payloads = [f"{key}-{index}".encode() for index in range(3)]
    first = client.publish_batch(tenant, namespace, stream, payloads, ack="per_message")
    assert first is not None

    page = client.read(tenant, namespace, stream, 0, first, end=first + 3, max_records=2)
    assert [r.payload for r in page.records] == payloads[:2]
    assert [r.offset for r in page.records] == [first, first + 1]
    assert all(r.timestamp_micros > 0 for r in page.records)
    assert page.next_offset == first + 2

    rest = client.read(tenant, namespace, stream, 0, page.next_offset, end=first + 3)
    assert [r.payload for r in rest.records] == payloads[2:]
    assert rest.next_offset == first + 3


def test_a_subscription_reports_where_it_began(client, fixture, key):
    tenant, namespace, stream = shard0(fixture)
    with client.subscribe(tenant, namespace, stream, start="earliest", shard=0) as sub:
        assert sub.start_offset is not None


def test_a_named_shard_can_be_subscribed_alone(client, fixture, key):
    tenant, namespace, stream = shard0(fixture)
    payload = f"only-shard-1-{key}".encode()
    with client.subscribe(tenant, namespace, stream, shard=1) as sub:
        client.publish_on_shard(tenant, namespace, stream, 1, [payload], ack="per_message")
        for _ in range(20):
            event = sub.next_event(timeout=3.0)
            if event is not None and event.payload == payload:
                return
    pytest.fail("a record published to shard 1 never reached a shard 1 subscription")
