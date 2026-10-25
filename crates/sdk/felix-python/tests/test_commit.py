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
