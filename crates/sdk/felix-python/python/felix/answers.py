"""The dicts the broker-inspection calls return.

They are plain dicts with the wire's field names, typed here so a checker knows
the keys. A field the broker has no value for is absent rather than ``None``.
"""

from __future__ import annotations

from typing import Any, TypedDict

class Supports(TypedDict):
    inspect: bool
    topology: bool
    ack_on_commit: bool
    stream_shards: bool
    cache_shards: bool
    shard_owners: bool

class BrokerEndpoint(TypedDict):
    node_id: str
    addr: str

class ShardOwner(TypedDict, total=False):
    shard: int
    node_id: str
    addr: str
    generation: int
    unavailable: str

class SubscriptionCursor(TypedDict):
    tenant_id: str
    namespace: str
    stream: str
    shard: int
    subscriber_id: int

class InspectedSubscription(TypedDict, total=False):
    tenant_id: str
    namespace: str
    stream: str
    shard: int
    subscriber_id: int
    subscription_id: int
    connection: int
    peer: str
    principal: str
    policy: str
    depth: int
    capacity: int
    dropped: int

class SubscriptionsPage(TypedDict):
    node_id: str
    subscriptions: list[InspectedSubscription]
    next_cursor: SubscriptionCursor | None

class ShardInspection(TypedDict, total=False):
    """One broker's view of one shard; fields a broker has no value for are absent."""

    node_id: str
    shards: int
    role: str
    phase: str
    serving: bool
    reason: str
    detail: str
    generation: int
    assignment: dict[str, Any]
    fence: dict[str, Any]
    lease: dict[str, Any]
    tail: int
    committed: int
    accepted_generation: int
    replicas: list[dict[str, Any]]
