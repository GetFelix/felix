"""Batches that are not idempotent-producer batches, and what the client knows of its cluster."""

from __future__ import annotations

import pytest

import felix


@pytest.mark.scenario("publish.records_land_on_their_keys_shards")
def test_keyed_records_are_split_by_shard_and_reported(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    records = [(f"k-{key}-{i}".encode(), f"v-{key}-{i}".encode()) for i in range(24)]

    report = client.publish_records(tenant, namespace, stream, records, ack="per_message")

    assert report.ok, [piece.error for piece in report.shards if piece.error]
    assert report.failed_records() == []
    assert len(report.shards) > 1, "the keys did not spread across shards"
    assert sorted(i for piece in report.shards for i in piece.records) == list(range(24))

    # Each piece is one contiguous batch on its own shard, in the order given.
    for piece in report.shards:
        assert piece.offset is not None
        page = client.read(
            tenant, namespace, stream, piece.shard, piece.offset, max_records=len(piece.records)
        )
        assert [r.payload for r in page.records] == [records[i][1] for i in piece.records]


def test_a_keyed_batch_stays_on_one_shard(client, fixture, key):
    tenant, namespace, stream = fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"]
    payloads = [f"{key}-{i}".encode() for i in range(4)]
    assert client.publish_batch(tenant, namespace, stream, [], ack="per_message") is None
    first = client.publish_batch(
        tenant, namespace, stream, payloads, key=key.encode(), ack="per_message"
    )
    assert first is not None


def test_the_client_can_describe_its_cluster(client):
    assert client.refresh_topology() >= 0
    counts = client.connections_per_node()
    assert counts and all(isinstance(addr, str) and n >= 1 for addr, n in counts)
