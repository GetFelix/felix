"""The reconnect knobs: validation without a broker, and the deadline against one."""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request

import pytest

import felix

BAD_ATTEMPTS = [0, -1, 10**30, 10**100]
BAD_DEADLINES = [0, 0.0, -1.0, float("nan"), float("inf"), 1e19, 400 * 24 * 3600.0]


def _connect(addrs="127.0.0.1:1", **options):
    return felix.Client(addrs, tenant_id="t", token="x", **options)


@pytest.mark.parametrize("attempts", BAD_ATTEMPTS)
def test_reconnect_attempts_must_be_at_least_one(attempts):
    with pytest.raises(ValueError, match="reconnect_attempts"):
        _connect(reconnect_attempts=attempts)


@pytest.mark.parametrize("deadline", BAD_DEADLINES)
def test_reconnect_deadline_must_be_a_positive_number(deadline):
    with pytest.raises(ValueError, match="reconnect_deadline"):
        _connect(reconnect_deadline=deadline)


@pytest.mark.parametrize("deadline", BAD_DEADLINES)
def test_the_async_client_validates_too(deadline):
    async def connect(**options):
        await felix.AsyncClient.connect(
            "127.0.0.1:1", tenant_id="t", token="x", **options
        )

    with pytest.raises(ValueError, match="reconnect_deadline"):
        asyncio.run(connect(reconnect_deadline=deadline))
    with pytest.raises(ValueError, match="reconnect_attempts"):
        asyncio.run(connect(reconnect_attempts=0))


@pytest.mark.scenario("retry.deadline_bounds_an_operation")
def test_a_deadline_bounds_a_retried_publish(disposable_fixture):
    """A fenced shard refuses at once and every refusal is retryable, so only
    the deadline stops an at-least-once publish with a large attempt budget.
    The same deadline bounds a group call to that owner."""
    fixture = disposable_fixture
    if not fixture.get("control_url"):
        pytest.skip("this fixture has no control endpoint")
    # Longer than the policy's largest backoff step (2 s), so a client that made
    # one attempt and gave up is told apart from one that kept going.
    deadline = 6.0
    request = urllib.request.Request(
        fixture["control_url"] + "/fence", data=b"", method="POST"
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        owner = json.loads(response.read())
    try:
        with felix.Client(
            owner["addr"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
            reconnect_attempts=40,
            reconnect_deadline=deadline,
        ) as client:
            started = time.monotonic()
            with pytest.raises(felix.ShardUnavailableError):
                client.publish(
                    fixture["tenant_id"],
                    fixture["namespace"],
                    fixture["movable_stream"],
                    b"bounded",
                    at_least_once=True,
                )
            elapsed = time.monotonic() - started
            # A settle is refused by the fenced owner before it looks the group
            # up, so the group need not exist.
            began = time.monotonic()
            with pytest.raises(felix.ShardUnavailableError):
                client.group_ack(
                    fixture["tenant_id"],
                    fixture["namespace"],
                    fixture["movable_stream"],
                    0,
                    "bounded-group",
                    0,
                )
            group_elapsed = time.monotonic() - began
    finally:
        heal = urllib.request.Request(
            fixture["control_url"] + "/heal", data=b"", method="POST"
        )
        urllib.request.urlopen(heal, timeout=120).read()
    assert elapsed < deadline + 8.0, (
        f"the publish took {elapsed:.1f}s against a {deadline}s deadline: "
        "it spent its attempts instead"
    )
    assert group_elapsed >= deadline - 2.0 - 0.25, (
        f"the group call took {group_elapsed:.1f}s: it gave up before the "
        f"{deadline}s deadline was in reach"
    )
    assert group_elapsed < deadline + 2.0, (
        f"the group call took {group_elapsed:.1f}s against a {deadline}s deadline: "
        "it spent its attempts instead"
    )
