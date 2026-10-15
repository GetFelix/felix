"""Publish and subscribe, against a real cluster.

Each test tags the conformance scenario it demonstrates. The tags are what
turn this file into evidence rather than assertion: a scenario with no passing
test here is reported missing by `felix-conformance verify`.
"""

from __future__ import annotations

import pytest

TIMEOUT = 20.0


def drain(subscription, count, timeout=TIMEOUT):
    """The next `count` events, or fewer if the stream goes quiet."""
    events = []
    while len(events) < count:
        event = subscription.next_event(timeout=timeout)
        if event is None:
            break
        events.append(event)
    return events


@pytest.mark.scenario("pubsub.roundtrip")
def test_a_published_record_reaches_a_subscriber(client, fixture, key):
    stream = fixture["durable_stream"]
    payload = f"roundtrip-{key}".encode()

    with client.subscribe(fixture["tenant_id"], fixture["namespace"], stream) as events:
        client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        received = drain(events, 1)

    assert received, "nothing arrived"
    # Byte for byte: a client that coerces to text or wraps in JSON fails here,
    # which is the point of asserting on bytes rather than on a decoded value.
    assert received[0].payload == payload
    assert isinstance(received[0].payload, bytes)


@pytest.mark.scenario("pubsub.order")
def test_records_arrive_in_publish_order(client, fixture, key):
    stream = fixture["durable_stream"]
    payloads = [f"{key}-{index}".encode() for index in range(20)]

    with client.subscribe(fixture["tenant_id"], fixture["namespace"], stream) as events:
        for payload in payloads:
            client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        received = drain(events, len(payloads))

    mine = [event.payload for event in received if event.payload in payloads]
    assert mine == payloads, "records were reordered"


@pytest.mark.scenario("pubsub.offsets_are_contiguous")
def test_offsets_increase_by_one(client, fixture, key):
    stream = fixture["durable_stream"]
    payloads = [f"{key}-{index}".encode() for index in range(10)]

    with client.subscribe(fixture["tenant_id"], fixture["namespace"], stream) as events:
        for payload in payloads:
            client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        received = drain(events, len(payloads))

    offsets = [event.offset for event in received]
    assert all(offset is not None for offset in offsets), (
        "a durable stream must deliver offsets; without them an application "
        "cannot detect a drop or resume after one"
    )
    for earlier, later in zip(offsets, offsets[1:]):
        assert later == earlier + 1, f"offsets jumped: {earlier} then {later}"


@pytest.mark.scenario(
    "pubsub.resume_from_offset",
    "reconnect.subscription_resumes_at_the_next_offset",
)
def test_a_subscription_resumes_at_the_next_offset(client, fixture, key):
    """Resume at offset + 1: no gap, no duplicate.

    The single most expensive client bug is off-by-one here — one direction
    loses records silently, the other duplicates them — so this asserts both
    directions rather than only that something arrived.
    """
    stream = fixture["durable_stream"]
    first_half = [f"{key}-a{index}".encode() for index in range(5)]
    second_half = [f"{key}-b{index}".encode() for index in range(5)]

    with client.subscribe(fixture["tenant_id"], fixture["namespace"], stream) as events:
        for payload in first_half:
            client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        received = drain(events, len(first_half))

    mine = [event for event in received if event.payload in first_half]
    assert mine, "nothing to resume from"
    last_handled = mine[-1].offset
    assert last_handled is not None

    # Published while nothing is subscribed: these exist only in the log, so a
    # resume that silently started at the tail would miss all of them.
    for payload in second_half:
        client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)

    with client.subscribe(
        fixture["tenant_id"],
        fixture["namespace"],
        stream,
        start=last_handled + 1,
    ) as resumed:
        after = drain(resumed, len(second_half))

    payloads = [event.payload for event in after]
    assert all(payload not in first_half for payload in payloads), (
        "a record already handled was delivered again"
    )
    for payload in second_half:
        assert payload in payloads, f"resume skipped {payload!r}"


@pytest.mark.scenario("ack.per_message_means_the_broker_accepted_it")
def test_an_acknowledged_publish_returns_after_the_broker_accepted_it(
    client, fixture, key
):
    stream = fixture["durable_stream"]
    payload = f"acked-{key}".encode()

    client.publish(
        fixture["tenant_id"],
        fixture["namespace"],
        stream,
        payload,
        ack="per_message",
    )

    # The acknowledgement is the claim that the broker has the record, so it
    # must be readable from the log immediately afterwards — without any
    # further wait on the test's part.
    with client.subscribe(
        fixture["tenant_id"], fixture["namespace"], stream, start="earliest"
    ) as events:
        received = drain(events, 200, timeout=5.0)
    assert payload in [event.payload for event in received], (
        "a record the broker acknowledged was not in the log"
    )


@pytest.mark.scenario("ack.none_does_not_wait_for_the_broker")
def test_an_unacknowledged_publish_does_not_wait(client, fixture, key):
    """`none` must not block on a broker round trip.

    Timing is the only observable difference, so this asserts a bound loose
    enough not to flake on a loaded machine and tight enough to catch a client
    that waits for an acknowledgement it was told not to want.
    """
    import time

    stream = fixture["durable_stream"]

    # Warm the connection so the measurement is not dominated by setup.
    client.publish(fixture["tenant_id"], fixture["namespace"], stream, b"warm")

    started = time.monotonic()
    for index in range(20):
        client.publish(
            fixture["tenant_id"],
            fixture["namespace"],
            stream,
            f"{key}-{index}".encode(),
            ack="none",
        )
    elapsed = time.monotonic() - started

    assert elapsed < 2.0, (
        f"20 unacknowledged publishes took {elapsed:.2f}s; a client that waits "
        "for the broker has turned the cheapest ack mode into the dearest"
    )


@pytest.mark.scenario("pubsub.start_earliest")
def test_a_subscription_can_start_at_the_oldest_retained_record(client, fixture, key):
    """`earliest` is "as far back as you can", not offset 0.

    A trimmed log has no offset 0 to ask for, and asking is an error — which is
    why this is a separate start position rather than a number a caller could
    have written themselves.
    """
    stream = fixture["durable_stream"]
    payload = f"earliest-{key}".encode()
    client.publish(
        fixture["tenant_id"], fixture["namespace"], stream, payload, ack="per_message"
    )

    with client.subscribe(
        fixture["tenant_id"], fixture["namespace"], stream, start="earliest"
    ) as events:
        received = drain(events, 500, timeout=5.0)

    payloads = [event.payload for event in received]
    assert payload in payloads, (
        "a subscription starting at the earliest retained record did not "
        "replay history, so it started somewhere else"
    )
    # History, not just the live tail: the record was published before the
    # subscription existed.
    assert len(payloads) > 1


def test_an_identity_publishes_and_subscribes_over_shared_connections(
    client, fixture, key
):
    """A client made by `with_identity` publishes and subscribes on its own
    token, over the parent's connections."""
    stream = fixture["durable_stream"]
    payload = f"identity-{key}".encode()
    identity = client.with_identity(fixture["tenant_id"], fixture["token"])

    with identity.subscribe(fixture["tenant_id"], fixture["namespace"], stream) as events:
        identity.publish(fixture["tenant_id"], fixture["namespace"], stream, payload)
        received = drain(events, 1)

    assert payload in [event.payload for event in received], "nothing arrived"
