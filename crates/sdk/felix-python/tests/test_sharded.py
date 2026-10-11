"""Multi-shard subscriptions.

A subscription reads one shard. Consuming a whole stream means one per shard,
each following its own shard's owner — which is why this is the client's job
and not something the broker can do on the caller's behalf. A client that opens
shard 0 and calls it the stream loses the rest with no error anywhere.
"""

from __future__ import annotations

import pytest

import felix


def drain_shards(subscription, wanted, timeout=15.0, attempts=200):
    """Collect records (ignoring shard lifecycle events) until `wanted` seen."""
    records = []
    lifecycle = []
    for _ in range(attempts):
        item = subscription.next_event(timeout=timeout)
        if item is None:
            break
        if isinstance(item, felix.ShardRecord):
            records.append(item)
            if len(records) >= wanted:
                break
        else:
            lifecycle.append(item)
    return records, lifecycle


@pytest.mark.scenario("sharded.reports_the_shard_count")
def test_a_client_can_ask_how_many_shards(client, fixture):
    shards = client.stream_shards(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    )
    assert shards > 1, (
        f"the fixture stream reports {shards} shards; the sharded scenarios "
        "cannot mean anything against a single-shard stream"
    )

    # Zero, not one: an unknown stream has to be distinguishable from a real
    # single-shard one, or subscribing to a stream that does not exist reads
    # shard 0 and calls it the stream.
    absent = client.stream_shards(
        fixture["tenant_id"], fixture["namespace"], fixture["missing_stream"]
    )
    assert absent == 0, (
        f"an unknown stream reported {absent} shards, so a client cannot tell "
        "it from a stream that really has that many"
    )


@pytest.mark.scenario("sharded.delivers_every_shard")
def test_a_sharded_subscription_receives_from_every_shard(client, fixture, key):
    """Records keyed to different shards must all arrive through one handle.

    Published with distinct keys so they hash across shards; asserting that
    every payload arrives is what catches a client reading shard 0 only.
    """
    stream = fixture["durable_stream"]
    payloads = [f"{key}-{index}".encode() for index in range(24)]

    with client.subscribe_sharded(
        fixture["tenant_id"], fixture["namespace"], stream
    ) as subscription:
        assert subscription.shards > 1

        # Keyed, because the key is what picks the shard: without one every
        # record lands on shard 0 and the merge is never exercised.
        for payload in payloads:
            client.publish(
                fixture["tenant_id"],
                fixture["namespace"],
                stream,
                payload,
                key=payload,
                ack="per_message",
            )

        records, _lifecycle = drain_shards(subscription, len(payloads))

    received = {record.event.payload for record in records}
    missing = [payload for payload in payloads if payload not in received]
    assert not missing, (
        f"{len(missing)} of {len(payloads)} records never arrived. A "
        "subscription that reads one shard sees only its share, and the rest "
        "go missing with no error."
    )

    # More than one shard actually contributed, or the test proved nothing
    # about merging.
    assert len({record.shard for record in records}) > 1, (
        "every record came from one shard; the payloads did not spread, so "
        "this did not exercise the merge"
    )


@pytest.mark.scenario("sharded.resumes_per_shard")
def test_a_sharded_subscription_resumes_per_shard(client, fixture, key):
    """Offsets are per shard, so resuming is a map rather than a number."""
    stream = fixture["durable_stream"]
    first = [f"{key}-a{index}".encode() for index in range(12)]
    second = [f"{key}-b{index}".encode() for index in range(12)]

    with client.subscribe_sharded(
        fixture["tenant_id"], fixture["namespace"], stream
    ) as subscription:
        for payload in first:
            client.publish(
                fixture["tenant_id"],
                fixture["namespace"],
                stream,
                payload,
                key=payload,
                ack="per_message",
            )
        drain_shards(subscription, len(first))
        positions = subscription.positions()

    assert positions, "no per-shard positions were reported, so nothing can resume"
    assert isinstance(positions, dict)

    # Published while nothing is subscribed: only a correct resume sees them.
    for payload in second:
        client.publish(
            fixture["tenant_id"],
            fixture["namespace"],
            stream,
            payload,
            key=payload,
            ack="per_message",
        )

    with client.subscribe_sharded(
        fixture["tenant_id"], fixture["namespace"], stream, resume=positions
    ) as resumed:
        records, _lifecycle = drain_shards(resumed, len(second))

    received = {record.event.payload for record in records}
    # Nothing already handled comes back: a client carrying one offset across
    # shards would replay on every shard but one.
    replayed = [payload for payload in first if payload in received]
    assert not replayed, f"{len(replayed)} already-handled records were delivered again"
    for payload in second:
        assert payload in received, f"resume skipped {payload!r}"


@pytest.mark.scenario("sharded.a_lost_shard_is_surfaced")
def test_shard_lifecycle_events_are_a_distinct_type(client, fixture, key):
    """A lost shard has to be tellable from a record.

    Losing a shard needs a broker to go away, which belongs to the destructive
    tests and their own cluster. What this asserts is the part a client owns:
    that the lifecycle events are their own types rather than being folded
    into records or swallowed, so a consumer can branch on them at all.
    """
    assert hasattr(felix.ShardLost, "shard")
    assert hasattr(felix.ShardLost, "error")
    assert hasattr(felix.ShardRecovered, "shard")
    for field in ("shard", "resume_from", "node_id", "addr", "generation"):
        assert hasattr(felix.ShardMoved, field)
    assert hasattr(felix.StreamDeleted, "shard")

    stream = fixture["durable_stream"]
    with client.subscribe_sharded(
        fixture["tenant_id"], fixture["namespace"], stream
    ) as subscription:
        client.publish(
            fixture["tenant_id"],
            fixture["namespace"],
            stream,
            f"{key}-lifecycle".encode(),
            ack="per_message",
        )
        item = subscription.next_event(timeout=15.0)

    assert isinstance(
        item,
        (
            felix.ShardRecord,
            felix.ShardLost,
            felix.ShardRecovered,
            felix.ShardMoved,
            felix.StreamDeleted,
        ),
    ), f"a sharded subscription yielded {type(item).__name__}"
