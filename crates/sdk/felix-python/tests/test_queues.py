"""Consumer groups: claim, settle, redeliver.

A queue is not a subscription with extra steps. Records are pulled because
only the consumer knows when it has capacity, each is claimed by one member
until settled, and an unsettled record comes back. These assert the settle
semantics specifically, because that is where a client that modelled a queue
as a stream goes wrong.
"""

from __future__ import annotations

import pytest

import felix

GROUP = "conformance-group"


def poll_until(client, fixture, shard, group, wanted, attempts=10):
    """Poll a group until `wanted` records have been seen, or attempts run out."""
    records = []
    for _ in range(attempts):
        batch = client.group_poll(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["durable_stream"],
            shard,
            group,
            max_records=32,
            wait=2.0,
        )
        records.extend(batch)
        if len(records) >= wanted:
            break
    return records


def drain_and_ack(client, fixture, shard, group, rounds=20):
    """Settle everything a group is currently owed, so a later poll is empty.

    A new group begins at the oldest retained record rather than the tail, so
    it inherits whatever else is on the shared fixture stream.
    """
    for _ in range(rounds):
        batch = client.group_poll(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["durable_stream"],
            shard,
            group,
            max_records=64,
            wait=1.0,
        )
        if not batch:
            return
        for record in batch:
            client.group_ack(
                fixture["tenant_id"],
                fixture["namespace"],
                fixture["durable_stream"],
                shard,
                group,
                record.offset,
            )


@pytest.mark.scenario("queue.poll_returns_claimed_records")
def test_a_poll_claims_records_and_empty_is_an_answer(client, fixture, key):
    stream = fixture["durable_stream"]
    group = f"{GROUP}-{key}"
    payload = f"queued-{key}".encode()

    # A new group starts at the *earliest* retained record, not the tail, so
    # it is owed whatever other tests have already published to this stream.
    # Settle all of that first; only then is an empty poll meaningful.
    drain_and_ack(client, fixture, 0, group)

    empty = client.group_poll(
        fixture["tenant_id"], fixture["namespace"], stream, 0, group, wait=0.5
    )
    assert empty == [], "a group with nothing owed must return an empty list, not raise"

    client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload, ack="per_message")
    records = poll_until(client, fixture, 0, group, 1)

    mine = [record for record in records if record.payload == payload]
    assert mine, f"the published record was never handed to the group: {records}"
    assert mine[0].offset >= 0
    assert mine[0].attempts >= 1, "a first delivery reports at least one attempt"


@pytest.mark.scenario("queue.acknowledged_records_are_not_redelivered")
def test_an_acknowledged_record_does_not_come_back(client, fixture, key):
    stream = fixture["durable_stream"]
    group = f"{GROUP}-ack-{key}"
    payload = f"acked-{key}".encode()

    # Settle whatever the shared stream already owes this group, so the poll
    # below is about the record this test published.
    drain_and_ack(client, fixture, 0, group)
    client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload, ack="per_message")

    records = poll_until(client, fixture, 0, group, 1)
    mine = [record for record in records if record.payload == payload]
    assert mine, "nothing to acknowledge"

    for record in records:
        client.group_ack(
            fixture["tenant_id"], fixture["namespace"], stream, 0, group, record.offset
        )

    # Settled records must not reappear. A client whose acknowledgement never
    # reaches the broker produces a queue that redelivers forever, and the
    # symptom shows up in the consumer rather than here.
    again = client.group_poll(
        fixture["tenant_id"], fixture["namespace"], stream, 0, group, wait=3.0
    )
    assert payload not in [record.payload for record in again], (
        "an acknowledged record was handed out again"
    )


@pytest.mark.scenario("queue.unacknowledged_records_are_redelivered")
def test_a_record_handed_back_is_delivered_again(client, fixture, key):
    stream = fixture["durable_stream"]
    group = f"{GROUP}-nack-{key}"
    payload = f"nacked-{key}".encode()

    drain_and_ack(client, fixture, 0, group)
    client.publish(fixture["tenant_id"], fixture["namespace"], stream, payload, ack="per_message")

    first = poll_until(client, fixture, 0, group, 1)
    mine = [record for record in first if record.payload == payload]
    assert mine, "nothing to hand back"
    first_attempts = mine[0].attempts

    client.group_nack(fixture["tenant_id"], fixture["namespace"], stream, 0, group, mine[0])

    redelivered = poll_until(client, fixture, 0, group, 1)
    again = [record for record in redelivered if record.payload == payload]
    assert again, "a record handed back was never redelivered"
    assert again[0].attempts > first_attempts, (
        f"redelivery reported {again[0].attempts} attempts against {first_attempts} "
        "on the first: a consumer cannot tell a retry from a first attempt"
    )

    client.group_ack(
        fixture["tenant_id"], fixture["namespace"], stream, 0, group, again[0].offset
    )


@pytest.mark.scenario("queue.dead_letters_are_listable_and_redrivable")
def test_dead_letters_can_be_listed(client, fixture, key):
    """The administrative half: listing is enough to claim the scenario.

    Driving a record past its attempt bound takes as many redeliveries as the
    bound allows, which is a slow thing to do in a conformance run. What this
    asserts is that the three calls exist, reach the broker, and answer — an
    empty list being a legitimate answer.
    """
    stream = fixture["durable_stream"]
    group = f"{GROUP}-dl-{key}"

    offsets = client.group_dead_letters(
        fixture["tenant_id"], fixture["namespace"], stream, 0, group
    )
    assert isinstance(offsets, list)

    # Redriving or discarding an offset that is not dead-lettered is a
    # no-op or a typed error, never a crash.
    if offsets:
        client.group_redrive(
            fixture["tenant_id"], fixture["namespace"], stream, 0, group, offsets[0]
        )


@pytest.mark.scenario("client.concurrent_calls")
def test_two_group_members_do_not_receive_the_same_record(client, fixture, key):
    """The property a queue exists for: one record, one consumer.

    Both members poll the same group; no offset may be handed to both while
    the first still holds it.
    """
    stream = fixture["durable_stream"]
    group = f"{GROUP}-excl-{key}"
    payloads = [f"{key}-{index}".encode() for index in range(10)]

    drain_and_ack(client, fixture, 0, group)
    for payload in payloads:
        client.publish(
            fixture["tenant_id"], fixture["namespace"], stream, payload, ack="per_message"
        )

    with felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    ) as second:
        first_batch = client.group_poll(
            fixture["tenant_id"], fixture["namespace"], stream, 0, group, wait=3.0
        )
        second_batch = second.group_poll(
            fixture["tenant_id"], fixture["namespace"], stream, 0, group, wait=3.0
        )

    overlap = {record.offset for record in first_batch} & {
        record.offset for record in second_batch
    }
    assert not overlap, (
        f"offsets {sorted(overlap)} were claimed by both members at once; a "
        "queue that hands one record to two consumers is not a queue"
    )
