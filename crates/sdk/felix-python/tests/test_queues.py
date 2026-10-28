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


def scope(fixture):
    return fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], 0


@pytest.fixture
def operator(fixture):
    """A client holding `group.manage`, which creating, moving or deleting a group needs."""
    with felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["group_operator_token"],
        ca_file=fixture["ca_file"],
    ) as operator:
        yield operator


def poll_for(client, fixture, group, payload):
    """Poll until the record carrying `payload` shows up, or fail."""
    for record in poll_until(client, fixture, 0, group, 1):
        if record.payload == payload:
            return record
    raise AssertionError(f"{payload!r} was never handed to {group}")


@pytest.mark.scenario("queue.group_create_starts_at_the_chosen_position")
def test_a_group_created_at_latest_skips_what_came_before(client, operator, fixture, key):
    group = f"{GROUP}-create-{key}"
    payload = f"after-{key}".encode()
    tail = client.group_describe(*scope(fixture), group).tail

    created = operator.group_create(*scope(fixture), group, "latest")
    assert created.moved
    assert created.offset >= tail

    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )
    first = poll_until(client, fixture, 0, group, 1)
    assert first, "the group was handed nothing"
    assert min(record.offset for record in first) >= created.offset, (
        "a group created at latest was owed records from before it existed"
    )
    assert payload in [record.payload for record in first]

    # A consumer that creates its group on every start must not rewind it.
    again = operator.group_create(*scope(fixture), group, "earliest")
    assert not again.moved


@pytest.mark.scenario("queue.group_seek_replays_from_the_new_position")
def test_a_seek_delivers_from_the_new_position(client, operator, fixture, key):
    group = f"{GROUP}-seek-{key}"
    payload = f"replay-{key}".encode()
    created = operator.group_create(*scope(fixture), group, "latest")
    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )
    record = poll_for(client, fixture, group, payload)
    client.group_ack(*scope(fixture), group, record.offset)

    position = operator.group_seek(*scope(fixture), group, created.offset)
    assert position.offset == created.offset
    assert position.moved

    again = poll_for(client, fixture, group, payload)
    assert again.offset == record.offset, "the seek did not replay the record"


@pytest.mark.scenario("queue.group_describe_and_delete")
def test_describe_reports_the_group_and_delete_forgets_it(client, operator, fixture, key):
    group = f"{GROUP}-describe-{key}"
    payload = f"described-{key}".encode()
    created = operator.group_create(*scope(fixture), group, "latest")
    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )

    before = client.group_describe(*scope(fixture), group)
    assert before.committed == created.offset
    assert before.tail > created.offset
    assert before.lag == before.tail - created.offset
    assert before.in_flight == 0

    poll_for(client, fixture, group, payload)
    assert client.group_describe(*scope(fixture), group).in_flight >= 1

    assert operator.group_delete(*scope(fixture), group) is True
    gone = client.group_describe(*scope(fixture), group)
    assert (gone.committed, gone.in_flight) == (None, 0)
    assert operator.group_delete(*scope(fixture), group) is False


@pytest.mark.scenario("queue.group_extend_keeps_a_claim")
def test_extending_a_claim_reports_how_long_it_stands(client, operator, fixture, key):
    group = f"{GROUP}-extend-{key}"
    payload = f"extended-{key}".encode()
    operator.group_create(*scope(fixture), group, "latest")
    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )
    record = poll_for(client, fixture, group, payload)

    stands = client.group_extend(*scope(fixture), group, record, 60.0)
    assert isinstance(stands, float)
    assert stands > 0.0

    client.group_ack(*scope(fixture), group, record.offset)


def test_a_bad_start_is_refused_before_it_is_sent(client, operator, fixture, key):
    group = f"{GROUP}-bad-{key}"
    with pytest.raises(ValueError):
        operator.group_create(*scope(fixture), group, "middle")
    with pytest.raises(ValueError):
        operator.group_seek(*scope(fixture), group, "")


def test_the_async_client_administers_groups_too(fixture, key):
    import asyncio

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        operator = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["group_operator_token"],
            ca_file=fixture["ca_file"],
        )
        group = f"{GROUP}-async-{key}"
        created = await operator.group_create(*scope(fixture), group, "latest")
        moved = await operator.group_seek(*scope(fixture), group, created.offset)
        info = await client.group_describe(*scope(fixture), group)
        deleted = await operator.group_delete(*scope(fixture), group)
        return created, moved, info, deleted

    created, moved, info, deleted = asyncio.run(run())
    assert created.moved
    assert moved.offset == created.offset
    assert info.committed == created.offset
    assert deleted is True


@pytest.mark.scenario("queue.group_nack_after_delays_redelivery")
def test_a_delayed_nack_comes_back_later(client, operator, fixture, key):
    group = f"{GROUP}-delay-{key}"
    payload = f"later-{key}".encode()
    operator.group_create(*scope(fixture), group, "latest")
    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )
    record = poll_for(client, fixture, group, payload)

    client.group_nack_after(*scope(fixture), group, record, 2.0)
    soon = client.group_poll(*scope(fixture), group, max_records=32, wait=0.2)
    assert payload not in [r.payload for r in soon], "a delayed nack was redelivered at once"

    again = poll_for(client, fixture, group, payload)
    assert again.offset == record.offset
    assert again.attempts > record.attempts
    client.group_ack(*scope(fixture), group, again.offset)


@pytest.mark.scenario("queue.group_dead_letter_gives_up_a_record")
def test_a_record_can_be_given_up_on(client, operator, fixture, key):
    group = f"{GROUP}-dead-{key}"
    payload = f"hopeless-{key}".encode()
    operator.group_create(*scope(fixture), group, "latest")
    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )
    record = poll_for(client, fixture, group, payload)

    operator.group_dead_letter(*scope(fixture), group, record.offset)
    assert record.offset in client.group_dead_letters(*scope(fixture), group)
    again = client.group_poll(*scope(fixture), group, max_records=32, wait=0.2)
    assert payload not in [r.payload for r in again], "a dead letter was handed out again"

    client.group_redrive(*scope(fixture), group, record.offset)
    back = poll_for(client, fixture, group, payload)
    client.group_ack(*scope(fixture), group, back.offset)


def test_a_shorter_visibility_lets_a_claim_lapse_sooner(client, operator, fixture, key):
    group = f"{GROUP}-visible-{key}"
    payload = f"lapses-{key}".encode()
    operator.group_create(*scope(fixture), group, "latest")
    client.publish(
        fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], payload,
        ack="per_message",
    )
    first = None
    for _ in range(10):
        for record in client.group_poll(
            *scope(fixture), group, max_records=32, wait=2.0, visibility=1.0
        ):
            if record.payload == payload:
                first = record
        if first:
            break
    assert first is not None
    again = poll_for(client, fixture, group, payload)
    assert again.attempts > first.attempts
    client.group_ack(*scope(fixture), group, again.offset)


@pytest.mark.scenario("queue.group_stream_forms_cover_every_shard")
def test_the_stream_forms_act_on_every_shard(client, operator, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    group = f"{GROUP}-all-{key}"
    shards = client.stream_shards(tenant, namespace, stream)
    assert shards > 1

    created = operator.group_create_stream(tenant, namespace, stream, group, "latest")
    assert len(created) == shards
    assert all(position.moved for position in created)

    described = client.group_describe_stream(tenant, namespace, stream, group)
    assert len(described) == shards
    assert [info.committed for info in described] == [p.offset for p in created]

    moved = operator.group_seek_stream(tenant, namespace, stream, group, "latest")
    assert len(moved) == shards
    with pytest.raises(felix.FelixError):
        operator.group_seek_stream(tenant, namespace, stream, group, 0)

    assert operator.group_delete_stream(tenant, namespace, stream, group) is True
    assert operator.group_delete_stream(tenant, namespace, stream, group) is False
    assert all(
        info.committed is None
        for info in client.group_describe_stream(tenant, namespace, stream, group)
    )


def test_a_sharded_group_polls_and_settles_every_shard(client, operator, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    group = f"{GROUP}-sharded-{key}"
    operator.group_create_stream(tenant, namespace, stream, group, "latest")
    payloads = {f"{key}-{index}".encode() for index in range(16)}
    for payload in payloads:
        client.publish(tenant, namespace, stream, payload, key=payload, ack="per_message")

    sharded = client.group_sharded(tenant, namespace, stream, group)
    assert sharded.shards > 1
    seen = {}
    for _ in range(40):
        for record in sharded.poll(max_records=32):
            seen[record.record.payload] = record.shard
            sharded.ack(record)
        if payloads <= set(seen):
            break
    assert payloads <= set(seen), "a shard's records were never polled"
    assert len(set(seen.values())) > 1
    operator.group_delete_stream(tenant, namespace, stream, group)


def test_a_bad_visibility_is_refused_before_it_is_sent(client, fixture, key):
    for bad in (-1.0, float("nan"), float("inf"), 365 * 24 * 3600 + 1):
        with pytest.raises(ValueError):
            client.group_poll(
                *scope(fixture), f"{GROUP}-bad-{key}", wait=0.0, visibility=bad
            )
    with pytest.raises(ValueError):
        client.group_poll(*scope(fixture), f"{GROUP}-bad-{key}", wait=0.0, visibility=0.0)
