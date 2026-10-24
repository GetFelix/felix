using System.Text;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Caches, counters and watches against the fixture. Each test keys under its own run-unique prefix,
/// so a prefix watch sees only what the test itself wrote.
/// </summary>
public class CacheFixtureTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte>? value) => Encoding.UTF8.GetString(value!.Value.Span);

    /// <summary>Best-effort cleanup that never masks the failure being reported.</summary>
    private static async Task Cleanup(FelixCache cache, params string[] keys)
    {
        foreach (var key in keys)
        {
            try
            {
                await cache.DeleteAsync(key);
            }
            catch (Exception)
            {
            }
        }
    }

    private static async Task<WatchItem> Next(FelixCacheWatch watch)
    {
        var item = await watch.ReadAsync(Wait);
        Assert.NotNull(item);
        return item!;
    }

    [FixtureFact]
    [Scenario("cache.put_get", "cache.miss_is_absent", "cache.ttl_expires", "cache.delete_returns_previous")]
    public async Task ValuesReadBackExpireAndDeleteReturnsThePrevious()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var pfx = $"cs-{Run()}-";
        try
        {
            await cache.PutAsync(pfx + "a", Bytes("hello"));
            Assert.Equal("hello", Text(await cache.GetAsync(pfx + "a")));
            var versioned = await cache.GetVersionedAsync(pfx + "a");
            Assert.Equal("hello", Text(versioned!.Value));

            // A key never written is absent, not an error; an empty value is present.
            Assert.Null(await cache.GetAsync(pfx + "missing"));
            Assert.Null(await cache.GetVersionedAsync(pfx + "missing"));
            await cache.PutAsync(pfx + "empty", default);
            Assert.True((await cache.GetAsync(pfx + "empty"))!.Value.IsEmpty);

            // Expiry is lazy: the value reads as absent once the TTL has passed.
            await cache.PutAsync(pfx + "brief", Bytes("brief"), TimeSpan.FromSeconds(1));
            Assert.Equal("brief", Text(await cache.GetAsync(pfx + "brief")));
            await Task.Delay(TimeSpan.FromSeconds(2));
            Assert.Null(await cache.GetAsync(pfx + "brief"));

            // Delete returns what it removed, then absent.
            await cache.PutAsync(pfx + "gone", Bytes("new"));
            Assert.Equal("new", Text(await cache.DeleteAsync(pfx + "gone")));
            Assert.Null(await cache.DeleteAsync(pfx + "gone"));
        }
        finally
        {
            await Cleanup(cache, pfx + "a", pfx + "empty", pfx + "brief", pfx + "gone");
        }
    }

    [FixtureFact]
    public async Task ConditionalWritesReportRefusalsAsAnswers()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-cond";
        try
        {
            Assert.True((await cache.PutIfAbsentAsync(key, Bytes("one"))).Applied);
            Assert.False((await cache.PutIfAbsentAsync(key, Bytes("again"))).Applied);

            var v1 = (await cache.GetVersionedAsync(key))!.Version;
            Assert.True((await cache.PutIfVersionAsync(key, Bytes("two"), v1)).Applied);
            Assert.False((await cache.PutIfVersionAsync(key, Bytes("three"), v1)).Applied);
            Assert.False((await cache.DeleteIfVersionAsync(key, v1)).Applied);

            var now = (await cache.GetVersionedAsync(key))!;
            Assert.Equal("two", Text(now.Value));
            Assert.NotEqual(v1, now.Version);
            Assert.True((await cache.DeleteIfVersionAsync(key, now.Version)).Applied);
            Assert.Null(await cache.GetAsync(key));
        }
        finally
        {
            await Cleanup(cache, key);
        }
    }

    [FixtureFact]
    [Scenario("counter.add_returns_sum", "counter.get_absent_is_none")]
    public async Task CountersAddAndAnUnwrittenOneIsAbsent()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-count";
        try
        {
            Assert.Null(await cache.CounterGetAsync(key));
            Assert.Equal(5, await cache.CounterAddAsync(key, 5));
            Assert.Equal(3, await cache.CounterAddAsync(key, -2));
            Assert.Equal(3L, await cache.CounterGetAsync(key));
            // Brought back to zero, it has been written: zero, not absent.
            Assert.Equal(0, await cache.CounterAddAsync(key, -3));
            Assert.Equal(0L, await cache.CounterGetAsync(key));
        }
        finally
        {
            await Cleanup(cache, key);
        }
    }

    [FixtureFact]
    [Scenario("watch.delivers_changes_for_the_filter", "watch.deletes_arrive_as_changes_without_values")]
    public async Task AKeyWatchSeesItsKeyAndItsDeleteButNotItsNeighbours()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-watched";
        try
        {
            await using var watch = await cache.WatchKeyAsync(key);
            Assert.Null(watch.RetainedCount);

            // A neighbour's change comes first and must not arrive.
            await cache.PutAsync(key + "-not", Bytes("other"));
            await cache.PutAsync(key, Bytes("w1"), TimeSpan.FromSeconds(60));
            var put = await Next(watch);
            Assert.Equal(WatchItemKind.Change, put.Kind);
            Assert.Equal(key, put.Key);
            Assert.Equal("w1", Text(put.Value));
            Assert.Null(put.Shard);
            Assert.NotNull(put.ExpiresAt);

            // A delete is a change with no value, which an empty value is not.
            await cache.PutAsync(key, default);
            var empty = await Next(watch);
            Assert.False(empty.IsDelete);
            Assert.True(empty.Value!.Value.IsEmpty);
            await cache.DeleteAsync(key);
            var deleted = await Next(watch);
            Assert.Equal(key, deleted.Key);
            Assert.True(deleted.IsDelete);
            Assert.True(deleted.Offset > put.Offset);

            // Nothing more is waiting, and a timeout leaves the watch usable.
            Assert.Null(await watch.ReadAsync(TimeSpan.FromMilliseconds(150)));
            var refused = Assert.Throws<FelixException>(() => watch.ResumeOffsets());
            Assert.Equal(FelixStatus.InvalidArgument, refused.Status);
        }
        finally
        {
            await Cleanup(cache, key, key + "-not");
        }
    }

    [FixtureFact]
    [Scenario("watch.retained_delivers_state_before_changes")]
    public async Task ARetainedWatchDeliversStoredStateThenLiveChanges()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var pfx = $"cs-{Run()}-";
        try
        {
            // An empty prefix is a definite zero, not a silence to wait through.
            await using (var none = await cache.WatchPrefixAsync(pfx + "none-", retained: true))
            {
                Assert.Equal(0L, none.RetainedCount);
            }

            await cache.PutAsync(pfx + "k1", Bytes("stored"));
            await cache.PutAsync(pfx + "k2", Bytes("stored"));

            await using var watch = await cache.WatchPrefixAsync(pfx, retained: true);
            Assert.Equal(2L, watch.RetainedCount);
            var seen = new HashSet<string>();
            for (var i = 0; i < 2; i++)
            {
                var item = await Next(watch);
                Assert.Equal(WatchItemKind.Change, item.Kind);
                Assert.Equal("stored", Text(item.Value));
                seen.Add(item.Key!);
            }

            Assert.Equal(new HashSet<string> { pfx + "k1", pfx + "k2" }, seen);
            await cache.PutAsync(pfx + "k3", Bytes("live"));
            var live = await Next(watch);
            Assert.Equal(pfx + "k3", live.Key);
            Assert.Equal("live", Text(live.Value));
        }
        finally
        {
            await Cleanup(cache, pfx + "k1", pfx + "k2", pfx + "k3");
        }
    }

    // A lag cannot be forced here (no suite forces one: it takes a flood past the broker's queue),
    // so this carries no scenario tag. It checks that a healthy watch delivers a change and that a
    // lag item, if the broker did end the watch, names where to resume.
    [FixtureFact]
    public async Task ALagIsAnItemNamingWhereToResume()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var key = $"cs-{Run()}-lag";
        try
        {
            await using var watch = await cache.WatchPrefixAsync(key);
            await cache.PutAsync(key, Bytes("v"));
            var item = await Next(watch);
            Assert.True(item.Kind is WatchItemKind.Change or WatchItemKind.Lagged or WatchItemKind.ShardMoved, $"kind {item.Kind}");
            if (item.Kind == WatchItemKind.Lagged)
            {
                Assert.NotNull(item.ResumeFrom);
            }
        }
        finally
        {
            await Cleanup(cache, key);
        }
    }

    [FixtureFact]
    [Scenario("watch.resumes_from_an_offset")]
    public async Task AShardedWatchReplaysWhatItMissedThenJoinsLiveDelivery()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var pfx = $"cs-{Run()}-";
        var keys = new List<string>();
        async Task Put(string name)
        {
            keys.Add(pfx + name);
            await cache.PutAsync(pfx + name, Bytes(name));
        }

        try
        {
            ShardOffset[] at;
            await using (var watch = await cache.WatchShardedAsync(pfx))
            {
                for (var i = 0; i < 8; i++)
                {
                    await Put($"s{i}");
                }

                var seen = new HashSet<string>();
                for (var i = 0; i < 8; i++)
                {
                    var item = await Next(watch);
                    Assert.Equal(WatchItemKind.Change, item.Kind);
                    Assert.NotNull(item.Shard);
                    seen.Add(item.Key!);
                }

                Assert.Equal(8, seen.Count);
                at = watch.ResumeOffsets();
                Assert.NotEmpty(at);
                Assert.Equal(at.Select(o => o.Shard).Order().ToArray(), at.Select(o => o.Shard).ToArray());
            }

            // Written while no watch is open: resuming must replay these, and only these.
            await Put("missed0");
            await Put("missed1");

            await using (var resumed = await cache.WatchShardedAsync(pfx, resume: at))
            {
                var replayed = new HashSet<string>();
                for (var i = 0; i < 2; i++)
                {
                    replayed.Add((await Next(resumed)).Key!);
                }

                Assert.Equal(new HashSet<string> { pfx + "missed0", pfx + "missed1" }, replayed);
                // Then it joins live delivery, with no repeat of what it replayed.
                await Put("live");
                var live = await Next(resumed);
                Assert.Equal(pfx + "live", live.Key);
                Assert.Null(await resumed.ReadAsync(TimeSpan.FromMilliseconds(300)));
            }

            // A sharded retained watch delivers every shard's state, then says so.
            await using (var retained = await cache.WatchShardedAsync(pfx, retained: true))
            {
                var changes = 0;
                WatchItem item;
                do
                {
                    item = await Next(retained);
                    if (item.Kind == WatchItemKind.Change)
                    {
                        changes++;
                    }
                }
                while (item.Kind != WatchItemKind.StateComplete);

                Assert.Equal(11, changes);
                Assert.Null(item.Shard);
            }
        }
        finally
        {
            await Cleanup(cache, [.. keys]);
        }
    }

    [FixtureFact]
    [Scenario("watch.resumes_from_an_offset")]
    public async Task AWatchReplaysFromAnOffsetOnASingleShardCache()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var pfx = $"cs-{Run()}-";
        try
        {
            WatchItem seen;
            await using (var first = await cache.WatchPrefixAsync(pfx))
            {
                await cache.PutAsync(pfx + "a", Bytes("1"));
                seen = await Next(first);
            }

            // Written while no watch is open. The first offset not yet seen is the last handled plus one.
            await cache.PutAsync(pfx + "b", Bytes("2"));
            await using var resumed = await cache.WatchPrefixAsync(pfx, fromOffset: seen.Offset + 1);
            var replayed = await Next(resumed);
            Assert.Equal(pfx + "b", replayed.Key);
            Assert.True(replayed.Offset > seen.Offset);

            // Then live delivery, and nothing is repeated.
            await cache.PutAsync(pfx + "c", Bytes("3"));
            Assert.Equal(pfx + "c", (await Next(resumed)).Key);
            Assert.Null(await resumed.ReadAsync(TimeSpan.FromMilliseconds(300)));
        }
        finally
        {
            await Cleanup(cache, pfx + "a", pfx + "b", pfx + "c");
        }
    }

    [FixtureFact]
    public async Task APendingReadCanBeCancelledAndLosesNothing()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-cancel";
        var watch = await cache.WatchKeyAsync(key);
        try
        {
            using (var cts = new CancellationTokenSource())
            {
                var pending = watch.ReadAsync(Timeout.InfiniteTimeSpan, cts.Token);
                await Task.Delay(100);
                // One read at a time.
                await Assert.ThrowsAsync<InvalidOperationException>(() => watch.ReadAsync(TimeSpan.Zero));
                cts.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            }

            await cache.PutAsync(key, Bytes("after"));
            Assert.Equal("after", Text((await Next(watch)).Value));

            // Freeing the watch under a pending read cancels it.
            var last = watch.ReadAsync(Timeout.InfiniteTimeSpan);
            await Task.Delay(100);
            watch.Dispose();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => last);
        }
        finally
        {
            watch.Dispose();
            await Cleanup(cache, key);
        }
    }

    [FixtureFact]
    public async Task EnumeratingAWatchYieldsItsItemsUntilCancelled()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-enum";
        try
        {
            await using var watch = await cache.WatchKeyAsync(key);
            await cache.PutAsync(key, Bytes("e"));

            using var cts = new CancellationTokenSource(Wait);
            var got = new List<string>();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await foreach (var item in watch.WithCancellation(cts.Token))
                {
                    got.Add(Text(item.Value));
                    cts.CancelAfter(TimeSpan.FromMilliseconds(200));
                }
            });
            Assert.Equal(["e"], got);
        }
        finally
        {
            await Cleanup(cache, key);
        }
    }

    [FixtureFact]
    public async Task WatchArgumentsTheCallRefusesAreTyped()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var pfx = $"cs-{Run()}-";

        var retainedFrom = await Assert.ThrowsAsync<FelixException>(() => cache.WatchPrefixAsync(pfx, retained: true, fromOffset: 5));
        Assert.Equal(FelixStatus.InvalidArgument, retainedFrom.Status);
        var duplicate = await Assert.ThrowsAsync<FelixException>(
            () => cache.WatchShardedAsync(pfx, resume: [new ShardOffset(1, 0), new ShardOffset(1, 0)]));
        Assert.Equal(FelixStatus.InvalidArgument, duplicate.Status);
        var retainedResume = await Assert.ThrowsAsync<FelixException>(
            () => cache.WatchShardedAsync(pfx, retained: true, resume: [new ShardOffset(1, 0)]));
        Assert.Equal(FelixStatus.InvalidArgument, retainedResume.Status);
    }
}
