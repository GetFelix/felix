"""Atomic commits: an event and the state it changes, as one record.

The refusals are asserted as specifically as the successes. A client that
split a two-stream commit into two writes, or sent a commit with no event,
would appear to work until the first failure left half of it behind.
"""

from __future__ import annotations

import asyncio
import urllib.request

import pytest

import felix


def find(subscription, payload, attempts=200, timeout=15.0):
    """The shard record carrying `payload`, or None."""
    for _ in range(attempts):
        item = subscription.next_event(timeout=timeout)
        if item is None:
            return None
        if isinstance(item, felix.ShardRecord) and item.event.payload == payload:
            return item
    return None


@pytest.mark.scenario("commit.returns_the_offset")
def test_a_commit_is_read_at_its_offset(client, fixture, key):
    stream = fixture["durable_stream"]
    payload = f"{key}-placed".encode()
    with client.subscribe_sharded(
        fixture["tenant_id"], fixture["namespace"], stream
    ) as subscription:
        receipt = client.commit(
            fixture["tenant_id"],
            fixture["namespace"],
            key.encode(),
            [
                felix.CommitOp.publish(stream, payload),
                felix.CommitOp.put(stream, key, b"placed"),
            ],
        )
        record = find(subscription, payload)

    assert isinstance(receipt, felix.CommitReceipt)
    assert record is not None, "the commit's event never reached a subscriber"
    assert record.event.offset == receipt.offset


@pytest.mark.scenario("commit.negotiated")
def test_commits_are_refused_by_a_broker_that_advertised_none(fixture, key):
    addrs = fixture.get("no_commit_addrs")
    control_url = fixture.get("control_url")
    if not addrs or not control_url:
        pytest.skip("the fixture file has no no_commit_addrs or control_url")
    tenant, namespace = fixture["tenant_id"], fixture["namespace"]
    stream = fixture["durable_stream"]

    with felix.Client(
        addrs,
        tenant_id=tenant,
        token=fixture["token"],
        ca_file=fixture["ca_file"],
    ) as bare:
        # The plain base class: a typed refusal (wrong shard, event count) would
        # mean the call reached the broker.
        with pytest.raises(felix.CommitError) as refused:
            bare.commit(tenant, namespace, key.encode(), [felix.CommitOp.publish(stream, b"x")])
        assert type(refused.value) is felix.CommitError
        with pytest.raises(felix.CommitError) as refused:
            bare.state_get(tenant, namespace, stream, key.encode(), key)
        assert type(refused.value) is felix.CommitError

    with urllib.request.urlopen(control_url + "/no-commit/requests", timeout=10) as response:
        assert int(response.read()) == 0, "the broker was sent a commit or state read"


@pytest.mark.scenario("commit.state_get_returns_the_version")
def test_state_reads_back_at_the_commits_version(client, fixture, key):
    stream = fixture["durable_stream"]
    tenant, namespace = fixture["tenant_id"], fixture["namespace"]
    receipt = client.commit(
        tenant,
        namespace,
        key.encode(),
        [
            felix.CommitOp.enqueue(stream, b"placed"),
            felix.CommitOp.put(stream, key, b"placed"),
        ],
    )
    state = client.state_get(tenant, namespace, stream, key.encode(), key)
    assert state.value == b"placed"
    assert state.version == receipt.offset
    assert state.as_of >= receipt.offset

    client.commit(
        tenant,
        namespace,
        key.encode(),
        [felix.CommitOp.publish(stream, b"cancelled"), felix.CommitOp.delete(stream, key)],
    )
    # Absent, not empty: a deleted key has no value at all.
    assert client.state_get(tenant, namespace, stream, key.encode(), key).value is None


@pytest.mark.scenario("commit.another_stream_is_refused")
def test_an_op_on_another_stream_is_refused(client, fixture, key):
    stream = fixture["durable_stream"]
    with pytest.raises(felix.NotOnOwningShardError) as refused:
        client.commit(
            fixture["tenant_id"],
            fixture["namespace"],
            key.encode(),
            [
                felix.CommitOp.publish(stream, b"placed"),
                felix.CommitOp.put(fixture["movable_stream"], key, b"placed"),
            ],
        )
    assert isinstance(refused.value, felix.CommitError)
    assert refused.value.index == 1
    assert refused.value.stream == fixture["movable_stream"]
    assert refused.value.owner == stream


@pytest.mark.scenario("commit.exactly_one_event")
def test_a_commit_needs_exactly_one_event(client, fixture, key):
    stream = fixture["durable_stream"]
    for ops, count in [
        ([felix.CommitOp.put(stream, key, b"v")], 0),
        ([felix.CommitOp.publish(stream, b"a"), felix.CommitOp.enqueue(stream, b"b")], 2),
    ]:
        with pytest.raises(felix.EventCountError) as refused:
            client.commit(fixture["tenant_id"], fixture["namespace"], key.encode(), ops)
        assert refused.value.count == count


def check_conditional(what, write):
    """A write at an offset no shard has reached is refused with the tail, one
    at the tail lands there, and the same offset again is refused past it.
    `write(expected)` runs the form under test."""
    stale = write(2**64 - 1)
    assert isinstance(stale, felix.ConditionalWrite)
    assert not stale.written, f"{what} at a stale offset was written"
    assert stale.offset is None
    tail = stale.tail
    assert isinstance(tail, int)

    landed = write(tail)
    assert landed.written, f"{what} at the tail {tail} was refused"
    assert (landed.offset, landed.tail) == (tail, None)

    again = write(tail)
    assert not again.written, f"{what} again at {tail} was written"
    assert again.tail > landed.offset


@pytest.mark.scenario("commit.expected_offset_refuses_a_stale_write")
def test_a_conditional_commit_is_refused_once_the_shard_has_moved_on(client, fixture, key):
    stream = fixture["durable_stream"]
    tenant, namespace = fixture["tenant_id"], fixture["namespace"]
    ops = [felix.CommitOp.publish(stream, key.encode()), felix.CommitOp.put(stream, key, b"cond")]
    check_conditional(
        "commit_if",
        lambda expected: client.commit_if(tenant, namespace, key.encode(), ops, expected),
    )
    check_conditional(
        "commit_if_on_shard",
        lambda expected: client.commit_if_on_shard(
            tenant, namespace, 2, key.encode(), ops, expected
        ),
    )


@pytest.mark.scenario("explicit_shard.commit_lands_on_the_named_shard")
def test_a_commit_to_a_named_shard_is_read_back_from_that_shard(client, fixture, key):
    stream = fixture["durable_stream"]
    tenant, namespace = fixture["tenant_id"], fixture["namespace"]
    receipt = client.commit_on_shard(
        tenant,
        namespace,
        2,
        key.encode(),
        [felix.CommitOp.publish(stream, b"event"), felix.CommitOp.put(stream, key, b"shard-value")],
    )
    assert isinstance(receipt, felix.CommitReceipt)
    state = client.state_get_on_shard(tenant, namespace, stream, 2, key)
    assert state.value == b"shard-value"
    assert state.version == receipt.offset


@pytest.mark.scenario(
    "publish_if.lands_at_the_expected_offset", "publish_if.refused_with_the_tail"
)
def test_of_two_conditional_publishes_at_one_offset_the_second_is_refused(client, fixture, key):
    stream = fixture["durable_stream"]
    tenant, namespace = fixture["tenant_id"], fixture["namespace"]
    payloads = [b"pi-1", b"pi-2"]
    check_conditional(
        "publish_if",
        lambda expected: client.publish_if(
            tenant, namespace, stream, payloads, expected, key=key.encode()
        ),
    )
    check_conditional(
        "publish_if_on_shard",
        lambda expected: client.publish_if_on_shard(
            tenant, namespace, stream, 3, payloads, expected
        ),
    )


def test_a_conditional_publish_with_no_payload_is_refused(client, fixture):
    with pytest.raises(felix.FelixError):
        client.publish_if(
            fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], [], 0
        )


def test_the_async_client_commits_conditionally_too(fixture, key):
    stream = fixture["durable_stream"]

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        scope = (fixture["tenant_id"], fixture["namespace"])
        stale = await client.publish_if(*scope, stream, [b"a"], 2**64 - 1)
        landed = await client.publish_if(*scope, stream, [b"a"], stale.tail)
        ops = [felix.CommitOp.publish(stream, b"x"), felix.CommitOp.put(stream, key, b"x")]
        on_shard = await client.commit_on_shard(*scope, 2, key.encode(), ops)
        state = await client.state_get_on_shard(*scope, stream, 2, key)
        return stale, landed, on_shard, state

    stale, landed, on_shard, state = asyncio.run(run())
    assert not stale.written
    assert (landed.written, landed.offset) == (True, stale.tail)
    assert state.version == on_shard.offset


def test_the_async_client_commits_too(fixture, key):
    stream = fixture["durable_stream"]

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        receipt = await client.commit(
            fixture["tenant_id"],
            fixture["namespace"],
            key.encode(),
            [felix.CommitOp.publish(stream, b"placed"), felix.CommitOp.put(stream, key, b"x")],
        )
        state = await client.state_get(
            fixture["tenant_id"], fixture["namespace"], stream, key.encode(), key
        )
        return receipt, state

    receipt, state = asyncio.run(run())
    assert state.version == receipt.offset
