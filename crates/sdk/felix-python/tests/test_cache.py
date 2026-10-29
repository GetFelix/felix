"""Cache reads, writes, counters, and watches.

The watch scenarios carry the weight here. A cache watch is what makes Felix
usable for state synchronisation rather than notification, and the parts that
make it so — resume by offset, loss reported rather than inferred — are exactly
the parts a client can appear to implement without implementing.
"""

from __future__ import annotations

import time

import pytest

import felix


def wait_for_change(watch, predicate, attempts=20, timeout=3.0):
    """The next matching change, or None. Lag ends the watch and is returned."""
    for _ in range(attempts):
        item = watch.recv(timeout=timeout)
        if item is None:
            return None
        if isinstance(item, felix.CacheWatchLagged):
            return item
        if predicate(item):
            return item
    return None


@pytest.mark.scenario("cache.put_get")
def test_a_value_reads_back(client, fixture, key):
    cache = fixture["cache"]
    client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, key, b"value")
    assert (
        client.cache_get(fixture["tenant_id"], fixture["namespace"], cache, key) == b"value"
    )


@pytest.mark.scenario("cache.miss_is_absent")
def test_a_missing_key_is_absent_not_an_error(client, fixture, key):
    # None, not an exception: a miss is the most common cache outcome, and
    # forcing exception handling around it is a poor trade.
    assert (
        client.cache_get(
            fixture["tenant_id"], fixture["namespace"], fixture["cache"], f"absent-{key}"
        )
        is None
    )


@pytest.mark.scenario("cache.delete_returns_previous")
def test_delete_reports_what_it_removed(client, fixture, key):
    cache = fixture["cache"]
    client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, key, b"doomed")

    assert (
        client.cache_delete(fixture["tenant_id"], fixture["namespace"], cache, key) == b"doomed"
    )
    # Deleting what is not there is an answer, not an error.
    assert client.cache_delete(fixture["tenant_id"], fixture["namespace"], cache, key) is None
    assert client.cache_get(fixture["tenant_id"], fixture["namespace"], cache, key) is None


@pytest.mark.scenario("cache.ttl_expires")
def test_a_value_with_a_ttl_stops_reading(client, fixture, key):
    cache = fixture["cache"]
    client.cache_put(
        fixture["tenant_id"], fixture["namespace"], cache, key, b"brief", ttl=1.0
    )
    assert client.cache_get(fixture["tenant_id"], fixture["namespace"], cache, key) == b"brief"

    # Expiry is lazy — the record is reclaimed at compaction, not at the
    # deadline — so what matters is that it reads as absent, not that storage
    # shrank.
    time.sleep(2.0)
    assert client.cache_get(fixture["tenant_id"], fixture["namespace"], cache, key) is None


@pytest.mark.scenario("counter.add_returns_sum")
def test_adding_to_a_counter_returns_the_sum(client, fixture, key):
    cache = fixture["cache"]
    counter = f"counter-{key}"

    assert client.counter_add(fixture["tenant_id"], fixture["namespace"], cache, counter, 5) == 5
    # The sum *including* this delta, in one round trip — not an add followed
    # by a read, which another writer could interleave with.
    assert client.counter_add(fixture["tenant_id"], fixture["namespace"], cache, counter, 3) == 8
    assert client.counter_add(fixture["tenant_id"], fixture["namespace"], cache, counter, -8) == 0


@pytest.mark.scenario("counter.get_absent_is_none")
def test_an_unwritten_counter_is_absent_not_zero(client, fixture, key):
    # Absent and zero are different: a counter decremented to zero has been
    # written; one that never existed has not.
    assert (
        client.counter_get(
            fixture["tenant_id"], fixture["namespace"], fixture["cache"], f"never-{key}"
        )
        is None
    )

    counter = f"zeroed-{key}"
    client.counter_add(fixture["tenant_id"], fixture["namespace"], fixture["cache"], counter, 1)
    client.counter_add(fixture["tenant_id"], fixture["namespace"], fixture["cache"], counter, -1)
    assert (
        client.counter_get(
            fixture["tenant_id"], fixture["namespace"], fixture["cache"], counter
        )
        == 0
    )


@pytest.mark.scenario("watch.delivers_changes_for_the_filter")
def test_a_watch_delivers_matching_changes_only(client, fixture, key):
    cache = fixture["cache"]
    watched = f"watched-{key}"
    ignored = f"other-{key}"

    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        cache,
        felix.CacheWatchFilter.key(watched),
    ) as watch:
        client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, ignored, b"no")
        client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, watched, b"yes")

        change = wait_for_change(watch, lambda item: item.key == watched)

    assert change is not None, "the watched key's change never arrived"
    assert isinstance(change, felix.CacheChange)
    assert change.value == b"yes"
    assert change.key == watched, "a change for an unwatched key was delivered"


@pytest.mark.scenario("watch.deletes_arrive_as_changes_without_values")
def test_a_delete_arrives_as_a_change_with_no_value(client, fixture, key):
    cache = fixture["cache"]
    watched = f"deleted-{key}"
    client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, watched, b"here")

    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        cache,
        felix.CacheWatchFilter.key(watched),
    ) as watch:
        client.cache_delete(fixture["tenant_id"], fixture["namespace"], cache, watched)
        change = wait_for_change(
            watch, lambda item: item.key == watched and item.value is None
        )

    assert change is not None, "the delete never arrived"
    # None rather than b"": a watcher mirroring the cache has to tell 'removed'
    # from 'set to empty'.
    assert change.value is None


@pytest.mark.scenario("watch.resumes_from_an_offset")
def test_a_watch_resumes_from_an_offset(client, fixture, key):
    cache = fixture["cache"]
    watched = f"resumed-{key}"

    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        cache,
        felix.CacheWatchFilter.key(watched),
    ) as watch:
        client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, watched, b"first")
        first = wait_for_change(watch, lambda item: item.value == b"first")

    assert first is not None, "nothing to resume from"

    # Written while nothing is watching: only a resume can see it.
    client.cache_put(fixture["tenant_id"], fixture["namespace"], cache, watched, b"second")

    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        cache,
        felix.CacheWatchFilter.key(watched),
        start=first.offset + 1,
    ) as resumed:
        replayed = wait_for_change(resumed, lambda item: item.value == b"second")

    assert replayed is not None, (
        "a watch resuming from the next offset did not replay the change made "
        "while it was away — a reconnecting watcher would have to re-read the "
        "whole cache and hope"
    )


@pytest.mark.scenario("watch.lag_is_reported_rather_than_silent")
def test_lag_is_a_value_the_application_can_act_on(client, fixture, key):
    """Lag must be distinguishable from an ordinary end.

    Driving a real overflow needs a watcher slower than a flood, which is not
    a thing to do reliably in a conformance run. What this asserts is that the
    signal is a first-class value the client can hand back — carrying the
    offset to resume from — rather than an exception or a silent close, because
    a filtered watch's sparse offsets mean loss cannot be inferred.
    """
    assert hasattr(felix.CacheWatchLagged, "resume_from")
    for field in ("resume_from", "node_id", "addr", "generation"):
        assert hasattr(felix.CacheWatchShardMoved, field)

    # The single-shard cache: a prefix watch reads one shard, so over a
    # multi-shard cache this would watch a shard the key never lands on.
    cache = fixture["single_shard_cache"]
    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        cache,
        felix.CacheWatchFilter.prefix(f"lag-{key}"),
    ) as watch:
        client.cache_put(
            fixture["tenant_id"],
            fixture["namespace"],
            cache,
            f"lag-{key}-a",
            b"v",
        )
        item = watch.recv(timeout=5.0)

    # A healthy watch yields changes; a lagging one yields the lag value. Both
    # come back through the same call, which is what lets an application branch
    # rather than guess.
    assert isinstance(
        item, (felix.CacheChange, felix.CacheWatchLagged, felix.CacheWatchShardMoved)
    )


@pytest.mark.scenario("watch.retained_delivers_state_before_changes")
def test_a_retained_watch_delivers_current_state_first(client, fixture, key):
    # A retained *prefix* watch reads one shard, so the roster has to live on
    # a cache that has only one.
    cache = fixture["single_shard_cache"]
    prefix = f"room-{key}:"
    for member in ("alice", "bob"):
        client.cache_put(
            fixture["tenant_id"], fixture["namespace"], cache, f"{prefix}{member}", b"here"
        )

    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        cache,
        felix.CacheWatchFilter.prefix(prefix),
        retained=True,
    ) as watch:
        # The count is the point: an application knows the exact moment its
        # state is complete, and zero is a definite answer rather than a
        # silence to wait through.
        expected = watch.retained_count
        assert expected is not None, "a retained watch must report how many values to expect"

        roster = {}
        for _ in range(expected):
            item = watch.recv(timeout=10.0)
            assert isinstance(item, felix.CacheChange)
            roster[item.key] = item.value

    assert f"{prefix}alice" in roster
    assert f"{prefix}bob" in roster


@pytest.mark.scenario("watch.resumes_from_an_offset")
def test_retained_and_start_are_mutually_exclusive(client, fixture, key):
    """Asking for both is a mistake the client should name, not resolve."""
    with pytest.raises(ValueError):
        client.watch_cache(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["cache"],
            felix.CacheWatchFilter.key(key),
            start=0,
            retained=True,
        )


# The conditional calls have no catalogued scenario of their own, so these run
# against the fixture without a ledger entry.


def test_a_conditional_put_writes_only_at_the_version_read(client, fixture, key):
    tenant, namespace, cache = fixture["tenant_id"], fixture["namespace"], fixture["cache"]
    assert client.cache_get_versioned(tenant, namespace, cache, key) is None

    first = client.cache_put_if(tenant, namespace, cache, key, b"one", felix.CacheCondition.absent())
    assert isinstance(first, felix.CacheConditionResult)
    assert first.applied
    assert isinstance(first.version, int)

    # Taking an absent key twice: the second is refused with the live version.
    taken = client.cache_put_if(tenant, namespace, cache, key, b"two", felix.CacheCondition.absent())
    assert (taken.applied, taken.version) == (False, first.version)

    read = client.cache_get_versioned(tenant, namespace, cache, key)
    assert isinstance(read, felix.VersionedValue)
    assert (read.value, read.version) == (b"one", first.version)

    stale = client.cache_put_if(
        tenant, namespace, cache, key, b"x", felix.CacheCondition.version(read.version + 1)
    )
    assert not stale.applied
    swapped = client.cache_put_if(
        tenant, namespace, cache, key, b"two", felix.CacheCondition.version(read.version), ttl=60
    )
    assert swapped.applied
    assert swapped.version > read.version

    # A delete at the old version is refused; at the new one it removes the key.
    refused = client.cache_delete_if(tenant, namespace, cache, key, read.version)
    assert (refused.applied, refused.version) == (False, swapped.version)
    assert client.cache_delete_if(tenant, namespace, cache, key, swapped.version).applied
    assert client.cache_get(tenant, namespace, cache, key) is None
    gone = client.cache_delete_if(tenant, namespace, cache, key, swapped.version)
    assert (gone.applied, gone.version) == (False, None)


def test_the_async_client_writes_conditionally_too(fixture, key):
    import asyncio

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        scope = (fixture["tenant_id"], fixture["namespace"], fixture["cache"])
        first = await client.cache_put_if(*scope, key, b"one", felix.CacheCondition.absent())
        read = await client.cache_get_versioned(*scope, key)
        stale = await client.cache_delete_if(*scope, key, read.version + 1)
        removed = await client.cache_delete_if(*scope, key, read.version)
        return first, read, stale, removed

    first, read, stale, removed = asyncio.run(run())
    assert first.applied
    assert (read.value, read.version) == (b"one", first.version)
    assert not stale.applied
    assert removed.applied


def test_a_bad_version_is_refused_before_it_is_sent(client, fixture, key):
    tenant, namespace, cache = fixture["tenant_id"], fixture["namespace"], fixture["cache"]
    with pytest.raises(OverflowError):
        client.cache_delete_if(tenant, namespace, cache, key, -1)
    with pytest.raises(OverflowError):
        felix.CacheCondition.version(-1)


def put_spread(client, fixture, prefix, count=12):
    """Write `count` keys under `prefix`, enough to land on several shards."""
    keys = [f"{prefix}{index}" for index in range(count)]
    for name in keys:
        client.cache_put(
            fixture["tenant_id"], fixture["namespace"], fixture["cache"], name, b"v"
        )
    return keys


@pytest.mark.scenario("watch.sharded_prefix_covers_every_shard")
def test_a_sharded_prefix_watch_sees_every_shard(client, fixture, key):
    prefix = f"spread-{key}:"
    with client.watch_cache_sharded(
        fixture["tenant_id"], fixture["namespace"], fixture["cache"], prefix
    ) as watch:
        assert watch.shards > 1
        keys = put_spread(client, fixture, prefix)

        seen = {}
        for _ in range(60):
            if len(seen) == len(keys):
                break
            item = watch.recv(timeout=5.0)
            if item is None:
                break
            if isinstance(item, felix.ShardedCacheChange):
                seen[item.change.key] = item.shard
        offsets = watch.resume_offsets()

    missing = sorted(set(keys) - set(seen))
    assert not missing, (
        f"{len(missing)} of {len(keys)} changes never arrived; a prefix watch "
        "on one shard sees only the keys that hash there"
    )
    assert len(set(seen.values())) > 1, "every key came from one shard"
    assert sorted(offsets) == list(range(watch.shards)), "resume_offsets omits a shard"


@pytest.mark.scenario("watch.sharded_retained_marks_state_complete")
def test_a_sharded_retained_watch_marks_when_state_is_whole(client, fixture, key):
    prefix = f"roster-{key}:"
    keys = put_spread(client, fixture, prefix)

    with client.watch_cache_sharded(
        fixture["tenant_id"], fixture["namespace"], fixture["cache"], prefix, retained=True
    ) as watch:
        assert watch.retained_count is not None
        assert watch.retained_count >= len(keys)
        state = set()
        complete = False
        for _ in range(80):
            item = watch.recv(timeout=10.0)
            assert item is not None, "the watch ended before the state was complete"
            if isinstance(item, felix.CacheStateComplete):
                complete = True
                break
            if isinstance(item, felix.ShardedCacheChange):
                state.add(item.change.key)

    assert complete
    assert set(keys) <= state, "state was marked complete with keys still to come"


def test_a_sharded_watch_cannot_resume_and_retain(client, fixture, key):
    with pytest.raises(ValueError):
        client.watch_cache_sharded(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["cache"],
            key,
            resume={0: 0},
            retained=True,
        )


def test_the_async_client_watches_every_shard_too(fixture, key):
    import asyncio

    async def run():
        client = await felix.AsyncClient.connect(
            fixture["addrs"],
            tenant_id=fixture["tenant_id"],
            token=fixture["token"],
            ca_file=fixture["ca_file"],
        )
        prefix = f"async-spread-{key}:"
        for index in range(6):
            await client.cache_put(
                fixture["tenant_id"],
                fixture["namespace"],
                fixture["cache"],
                f"{prefix}{index}",
                b"v",
            )
        async with await client.watch_cache_sharded(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["cache"],
            prefix,
            retained=True,
        ) as watch:
            seen = set()
            while True:
                item = await watch.recv(timeout=10.0)
                if item is None or isinstance(item, felix.CacheStateComplete):
                    break
                if isinstance(item, felix.ShardedCacheChange):
                    seen.add(item.change.key)
            return seen, watch.shards

    seen, shards = asyncio.run(run())
    assert len(seen) == 6
    assert shards > 1


def _closes_while_a_reader_is_parked(make_handle, read):
    import threading

    handle = make_handle()
    stopped = threading.Event()

    def reader():
        try:
            read(handle)
        finally:
            stopped.set()

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    time.sleep(0.5)
    assert not stopped.is_set(), "the reader returned before anything was written"

    started = time.monotonic()
    closer = threading.Thread(target=handle.close, daemon=True)
    closer.start()
    closer.join(timeout=5.0)
    assert not closer.is_alive(), "close() waited on the parked reader"
    assert time.monotonic() - started < 5.0
    assert stopped.wait(timeout=5.0), "the reader never stopped after close()"
    assert handle.closed


def test_closing_a_sharded_watch_releases_a_parked_reader(client, fixture, key):
    _closes_while_a_reader_is_parked(
        lambda: client.watch_cache_sharded(
            fixture["tenant_id"], fixture["namespace"], fixture["cache"], f"idle-{key}:"
        ),
        lambda watch: watch.recv(),
    )


def test_closing_a_watch_releases_a_parked_reader(client, fixture, key):
    _closes_while_a_reader_is_parked(
        lambda: client.watch_cache(
            fixture["tenant_id"],
            fixture["namespace"],
            fixture["cache"],
            felix.CacheWatchFilter.key(f"idle-{key}"),
        ),
        lambda watch: watch.recv(),
    )


def test_closing_a_subscription_releases_a_parked_reader(client, fixture, key):
    _closes_while_a_reader_is_parked(
        lambda: client.subscribe(
            fixture["tenant_id"], fixture["namespace"], fixture["durable_stream"], start="latest"
        ),
        lambda sub: sub.next_event(),
    )


def test_an_infinite_or_negative_timeout_is_refused(client, fixture, key):
    with client.watch_cache_sharded(
        fixture["tenant_id"], fixture["namespace"], fixture["cache"], f"idle-{key}:"
    ) as watch:
        for bad in (float("inf"), float("nan"), -1.0):
            with pytest.raises(ValueError):
                watch.recv(timeout=bad)


def test_a_watch_reports_how_far_to_resume_and_how_often_it_moved(client, fixture, key):
    name = f"progress-{key}"
    with client.watch_cache(
        fixture["tenant_id"],
        fixture["namespace"],
        fixture["cache"],
        felix.CacheWatchFilter.key(name),
    ) as watch:
        before = watch.resume_from
        client.cache_put(fixture["tenant_id"], fixture["namespace"], fixture["cache"], name, b"v")
        change = wait_for_change(watch, lambda item: item.key == name)
        assert change is not None
        assert watch.resume_from > before
        assert watch.moves == 0
