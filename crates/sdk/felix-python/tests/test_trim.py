"""Conditional head trims: hiding a durable shard's oldest records.

A stale tail is an answer, not an error, like a conditional publish. The
refusals that are errors are asserted by class: a client that folded them into
one exception would send a caller retrying a trim the broker will never take.
"""

from __future__ import annotations

import asyncio
import urllib.request

import pytest

import felix

STALE = 2**64 - 1


def tail_of(client, fixture):
    """The trim stream's next offset, learned from a stale trim's refusal."""
    refused = client.trim_before(
        fixture["tenant_id"], fixture["namespace"], fixture["trim_stream"], 0, 0, STALE
    )
    assert not refused.trimmed
    return refused.tail


@pytest.fixture
def trimmer(fixture):
    if not fixture.get("trimmer_token") or not fixture.get("trim_stream"):
        pytest.skip("the fixture file has no trimmer_token or trim_stream")
    with felix.Client(
        fixture["addrs"],
        tenant_id=fixture["tenant_id"],
        token=fixture["trimmer_token"],
        ca_file=fixture["ca_file"],
    ) as connected:
        yield connected


@pytest.mark.scenario(
    "head_trim.lands_at_the_expected_offset", "head_trim.refused_with_the_tail"
)
def test_a_trim_at_the_tail_lands_there_and_a_stale_one_is_refused(trimmer, fixture):
    scope = (fixture["tenant_id"], fixture["namespace"], fixture["trim_stream"])
    tail = tail_of(trimmer, fixture)
    written = trimmer.publish_if_on_shard(*scope, 0, [b"t-1", b"t-2", b"t-3"], tail)
    assert written.written
    tail += 3

    trimmed = trimmer.trim_before(*scope, 0, tail - 1, tail, headers=b"who")
    assert isinstance(trimmed, felix.ConditionalTrim)
    assert trimmed.trimmed
    assert (trimmed.offset, trimmed.oldest, trimmed.tail) == (tail, tail - 1, None)

    # The trim took the offset the first one expected.
    again = trimmer.trim_before(*scope, 0, tail - 1, tail, headers=b"who")
    assert not again.trimmed
    assert (again.offset, again.oldest, again.tail) == (None, None, tail + 1)

    # At or below the floor nothing is written, but the tail must still match.
    noop = trimmer.trim_before(*scope, 0, tail - 2, tail + 1)
    assert noop.trimmed
    assert (noop.offset, noop.oldest) == (None, tail - 1)

    with pytest.raises(felix.CursorError):
        trimmer.read(*scope, 0, tail - 2)


def test_a_trim_past_its_own_record_is_a_plain_error(trimmer, fixture):
    scope = (fixture["tenant_id"], fixture["namespace"], fixture["trim_stream"])
    tail = tail_of(trimmer, fixture)
    with pytest.raises(felix.FelixError) as refused:
        trimmer.trim_before(*scope, 0, tail + 5, tail)
    assert type(refused.value) is felix.FelixError
    assert refused.value.code == "invalid_request"
    assert refused.value.detail["reason"] == "trim_past_tail"


@pytest.mark.scenario("head_trim.requires_permission")
def test_a_trim_without_the_grant_is_an_authorization_failure(client, fixture):
    with pytest.raises(felix.AuthError):
        client.trim_before(
            fixture["tenant_id"], fixture["namespace"], fixture["trim_stream"], 0, 0, STALE
        )


@pytest.mark.scenario("head_trim.negotiated")
def test_a_trim_is_refused_by_a_broker_that_advertised_none(fixture):
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
        with pytest.raises(felix.TrimError):
            bare.trim_before(
                fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], 0, 0, 0
            )

    with urllib.request.urlopen(control_url + "/no-commit/requests", timeout=10) as response:
        assert int(response.read()) == 0, "the broker was sent a trim"


def test_the_async_client_trims_too(trimmer, fixture):
    scope = (fixture["tenant_id"], fixture["namespace"], fixture["trim_stream"])
    tail = tail_of(trimmer, fixture)

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["trimmer_token"],
            ca_file=fixture["ca_file"],
        )
        stale = await client.trim_before(*scope, 0, 0, STALE)
        landed = await client.trim_before(*scope, 0, tail, tail, headers=b"async")
        return stale, landed

    stale, landed = asyncio.run(run())
    assert not stale.trimmed
    assert stale.tail == tail
    assert (landed.trimmed, landed.offset, landed.oldest) == (True, tail, tail)
