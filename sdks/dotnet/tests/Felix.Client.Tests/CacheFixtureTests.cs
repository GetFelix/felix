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

        await cache.DeleteAsync(pfx + "a");
        await cache.DeleteAsync(pfx + "empty");
    }

    [FixtureFact]
    public async Task ConditionalWritesReportRefusalsAsAnswers()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-cond";

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

    [FixtureFact]
    [Scenario("counter.add_returns_sum", "counter.get_absent_is_none")]
    public async Task CountersAddAndAnUnwrittenOneIsAbsent()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-count";

        Assert.Null(await cache.CounterGetAsync(key));
        Assert.Equal(5, await cache.CounterAddAsync(key, 5));
        Assert.Equal(3, await cache.CounterAddAsync(key, -2));
        Assert.Equal(3L, await cache.CounterGetAsync(key));
        // Brought back to zero, it has been written: zero, not absent.
        Assert.Equal(0, await cache.CounterAddAsync(key, -3));
        Assert.Equal(0L, await cache.CounterGetAsync(key));
    }

    [FixtureFact]
    [Scenario("watch.delivers_changes_for_the_filter", "watch.deletes_arrive_as_changes_without_values")]
    public async Task AKeyWatchSeesItsKeyAndItsDeleteButNotItsNeighbours()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-watched";

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
        await cache.DeleteAsync(key + "-not");
    }

    [FixtureFact]
    [Scenario("watch.retained_delivers_state_before_changes")]
    public async Task ARetainedWatchDeliversStoredStateThenLiveChanges()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var pfx = $"cs-{Run()}-";
        await cache.PutAsync(pfx + "k1", Bytes("stored"));
        await cache.PutAsync(pfx + "k2", Bytes("stored"));

        await using var watch = await cache.WatchPrefixAsync(pfx, retained: true);
        Assert.True(watch.RetainedCount >= 2);
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

        foreach (var key in new[] { "k1", "k2", "k3" })
        {
            await cache.DeleteAsync(pfx + key);
        }
    }

    // A flood that overflows a watch cannot be forced reliably, so this asserts what a healthy
    // watch delivers and that a lag, if the broker did end the watch, names where to resume.
    [FixtureFact]
    [Scenario("watch.lag_is_reported_rather_than_silent")]
    public async Task ALagIsAnItemNamingWhereToResume()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var key = $"cs-{Run()}-lag";
        await using var watch = await cache.WatchPrefixAsync(key);
        await cache.PutAsync(key, Bytes("v"));
        var item = await Next(watch);
        Assert.True(item.Kind is WatchItemKind.Change or WatchItemKind.Lagged or WatchItemKind.ShardMoved, $"kind {item.Kind}");
        if (item.Kind == WatchItemKind.Lagged)
        {
            Assert.NotNull(item.ResumeFrom);
        }

        await cache.DeleteAsync(key);
    }

    [FixtureFact]
    [Scenario("watch.resumes_from_an_offset")]
    public async Task AShardedWatchResumesFromItsOffsetsWithNoGap()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var pfx = $"cs-{Run()}-";

        ShardOffset[] at;
        await using (var watch = await cache.WatchShardedAsync(pfx))
        {
            for (var i = 0; i < 8; i++)
            {
                await cache.PutAsync($"{pfx}s{i}", Bytes("spread"));
            }

            var keys = new HashSet<string>();
            for (var i = 0; i < 8; i++)
            {
                var item = await Next(watch);
                Assert.Equal(WatchItemKind.Change, item.Kind);
                Assert.NotNull(item.Shard);
                keys.Add(item.Key!);
            }

            Assert.Equal(8, keys.Count);
            at = watch.ResumeOffsets();
            Assert.NotEmpty(at);
            Assert.Equal(at.Select(o => o.Shard).Order().ToArray(), at.Select(o => o.Shard).ToArray());
        }

        // Resumed, it sees only what came after.
        await using (var resumed = await cache.WatchShardedAsync(pfx, resume: at))
        {
            await cache.PutAsync($"{pfx}later0", Bytes("later"));
            await cache.PutAsync($"{pfx}later1", Bytes("later"));
            var seen = new HashSet<string>();
            for (var i = 0; i < 2; i++)
            {
                seen.Add((await Next(resumed)).Key!);
            }

            Assert.Equal(new HashSet<string> { $"{pfx}later0", $"{pfx}later1" }, seen);
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

            Assert.Equal(10, changes);
            Assert.Null(item.Shard);
        }

        for (var i = 0; i < 8; i++)
        {
            await cache.DeleteAsync($"{pfx}s{i}");
        }

        await cache.DeleteAsync($"{pfx}later0");
        await cache.DeleteAsync($"{pfx}later1");
    }

    [FixtureFact]
    public async Task AWatchResumesFromAnOffsetOnASingleShardCache()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var pfx = $"cs-{Run()}-";

        await using var first = await cache.WatchPrefixAsync(pfx);
        await cache.PutAsync(pfx + "a", Bytes("1"));
        var seen = await Next(first);

        // The first offset not yet seen is the last handled plus one.
        await cache.PutAsync(pfx + "b", Bytes("2"));
        await using var resumed = await cache.WatchPrefixAsync(pfx, fromOffset: seen.Offset + 1);
        var item = await Next(resumed);
        Assert.Equal(pfx + "b", item.Key);

        await cache.DeleteAsync(pfx + "a");
        await cache.DeleteAsync(pfx + "b");
    }

    [FixtureFact]
    public async Task APendingReadCanBeCancelledAndLosesNothing()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-cancel";
        var watch = await cache.WatchKeyAsync(key);

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
        await cache.DeleteAsync(key);
    }

    [FixtureFact]
    public async Task EnumeratingAWatchYieldsItsItemsUntilCancelled()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.Cache);
        var key = $"cs-{Run()}-enum";
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
        await cache.DeleteAsync(key);
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
