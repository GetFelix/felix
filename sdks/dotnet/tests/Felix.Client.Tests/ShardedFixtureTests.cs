using System.Text;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Sharded subscribe and resume, shard-addressed publishes, endpoints and offset lookups against
/// the fixture. The stream is shared, so each test publishes under its own payload prefix and reads
/// only that.
/// </summary>
public class ShardedFixtureTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static string Text(FelixEvent ev) => Encoding.UTF8.GetString(ev.Payload.Span);

    private static ReadOnlyMemory<byte> Bytes(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>Reads record items until <paramref name="count"/> carry the prefix; returns shard and offset of each.</summary>
    private static async Task<List<(uint Shard, long Offset, string Text)>> ReadPrefixed(
        FelixShardedSubscription sub, string prefix, int count)
    {
        var got = new List<(uint, long, string)>();
        var deadline = DateTime.UtcNow + Wait;
        while (got.Count < count && DateTime.UtcNow < deadline)
        {
            var item = await sub.ReadAsync(TimeSpan.FromSeconds(2));
            if (item is { Kind: ShardItemKind.Record, Event: { } ev } && Text(ev).StartsWith(prefix, StringComparison.Ordinal))
            {
                got.Add((item.Shard, ev.Offset ?? throw new Xunit.Sdk.XunitException("a durable stream gave no offset"), Text(ev)));
            }
        }

        Assert.Equal(count, got.Count);
        return got;
    }

    private static async Task PublishKeyed(FelixClient client, Fixture f, string prefix, int count)
    {
        for (var i = 0; i < count; i++)
        {
            await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"{prefix}{i}"), Bytes($"key-{i}"));
        }
    }

    [FixtureFact]
    [Scenario("sharded.delivers_every_shard")]
    public async Task ASubscriptionDeliversRecordsFromMoreThanOneShard()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeShardedAsync(f.Tenant, f.Namespace, f.Stream, StartPosition.Latest);
        Assert.True(sub.ShardCount > 1, $"the subscription covers {sub.ShardCount} shard(s)");

        var prefix = $"cs-sh-{Run()}-";
        await PublishKeyed(client, f, prefix, 8);
        var got = await ReadPrefixed(sub, prefix, 8);

        // More than one shard contributed, or the merge was never exercised.
        Assert.True(got.Select(g => g.Shard).Distinct().Count() > 1, "every record came from one shard");
        Assert.Equal(Enumerable.Range(0, 8).Select(i => $"{prefix}{i}").Order(), got.Select(g => g.Text).Order());
    }

    [FixtureFact]
    [Scenario("sharded.resumes_per_shard")]
    public async Task PositionsResumeEachShardAfterItsLastRecord()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var first = $"cs-pos-{run}-a";
        var second = $"cs-pos-{run}-b";

        IReadOnlyList<ShardPosition> positions;
        var seen = new Dictionary<uint, long>();
        await using (var sub = await client.SubscribeShardedAsync(f.Tenant, f.Namespace, f.Stream, StartPosition.Latest))
        {
            Assert.Empty(sub.Positions());
            await PublishKeyed(client, f, first, 8);
            foreach (var (shard, offset, _) in await ReadPrefixed(sub, first, 8))
            {
                seen[shard] = Math.Max(seen.GetValueOrDefault(shard, -1), offset);
            }

            // What was handed out, per shard that delivered, in ascending shard order.
            positions = sub.Positions();
            Assert.Equal(seen.OrderBy(s => s.Key).Select(s => new ShardPosition(s.Key, s.Value)), positions);
        }

        await PublishKeyed(client, f, second, 4);
        await using var resumed = await client.ResubscribeShardedAsync(
            f.Tenant, f.Namespace, f.Stream, positions, StartPosition.Earliest);

        // Only the later records arrive on shards that delivered; a replay of the first batch is a bug.
        var got = await ReadPrefixed(resumed, second, 4);
        Assert.All(got, g => Assert.True(g.Offset > seen.GetValueOrDefault(g.Shard, -1)));
        var replayed = 0;
        for (var i = 0; i < 200; i++)
        {
            var item = await resumed.ReadAsync(TimeSpan.FromMilliseconds(100));
            if (item is null)
            {
                break;
            }

            if (item.Event is { } ev && Text(ev).StartsWith(first, StringComparison.Ordinal))
            {
                replayed++;
            }
        }

        Assert.Equal(0, replayed);
    }

    [FixtureFact]
    public async Task AShardListedTwiceIsRefusedBeforeAnythingStarts()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var error = await Assert.ThrowsAsync<FelixException>(() => client.ResubscribeShardedAsync(
            f.Tenant, f.Namespace, f.Stream, [new ShardPosition(1, 5), new ShardPosition(1, 6)], StartPosition.Latest));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
    }

    [FixtureFact]
    public async Task ASecondPendingReadIsRefusedAndACancelledReadLosesNothing()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeShardedAsync(f.Tenant, f.Namespace, f.Stream, StartPosition.Latest);

        using var cts = new CancellationTokenSource();
        var pending = sub.ReadAsync(Timeout.InfiniteTimeSpan, cts.Token);
        await Assert.ThrowsAsync<InvalidOperationException>(() => sub.ReadAsync(TimeSpan.FromMilliseconds(10)));
        // Positions can be read while a read is pending.
        Assert.Empty(sub.Positions());
        cts.CancelAfter(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);

        Assert.Null(await sub.ReadAsync(TimeSpan.FromMilliseconds(100)));
        var prefix = $"cs-cancel-{Run()}-";
        await PublishKeyed(client, f, prefix, 1);
        await ReadPrefixed(sub, prefix, 1);
    }

    [FixtureFact]
    public async Task DisposingASubscriptionWithAReadPendingCancelsTheRead()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var sub = await client.SubscribeShardedAsync(f.Tenant, f.Namespace, f.Stream, StartPosition.Latest);
        var pending = sub.ReadAsync(Timeout.InfiniteTimeSpan);
        await Task.Delay(50);
        sub.Dispose();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
    }

    [FixtureFact]
    public async Task APublishOnAShardIsReadBackFromThatShard()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        for (uint shard = 1; shard <= 3; shard++)
        {
            await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, shard, StartPosition.Latest);
            var prefix = $"cs-on-shard-{Run()}-{shard}-";
            var offsets = await client.PublishOnShardAsync(
                f.Tenant, f.Namespace, f.Stream, shard, [Bytes(prefix + "0"), Bytes(prefix + "1")]);
            Assert.Equal(2, offsets.Length);

            for (var i = 0; i < 2; i++)
            {
                var ev = await sub.ReadAsync(Wait);
                Assert.NotNull(ev);
                Assert.Equal(prefix + i, Text(ev!));
                Assert.Equal(offsets[i], ev!.Offset);
            }
        }
    }

    [FixtureFact]
    public async Task AShardTheStreamDoesNotHaveIsRefused()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var error = await Assert.ThrowsAsync<FelixException>(() => client.PublishOnShardAsync(
            f.Tenant, f.Namespace, f.Stream, 1000, [Bytes("never"), Bytes("written")]));
        Assert.NotEqual(FelixStatus.Ok, error.Status);
    }

    [FixtureFact]
    public async Task PublishRecordsRoutesEachRecordByItsKey()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeShardedAsync(f.Tenant, f.Namespace, f.Stream, StartPosition.Latest);
        var prefix = $"cs-records-{Run()}-";
        var records = Enumerable.Range(0, 6)
            .Select(i => (Payload: Bytes($"{prefix}{i}"), Key: Bytes($"key-{i}")))
            .ToList();

        var results = await client.PublishRecordsAsync(f.Tenant, f.Namespace, f.Stream, records);
        Assert.Equal(6, results.Length);
        Assert.All(results, r => Assert.True(r.Accepted, r.Message));

        var got = await ReadPrefixed(sub, prefix, 6);
        Assert.Equal(Enumerable.Range(0, 6).Select(i => $"{prefix}{i}").Order(), got.Select(g => g.Text).Order());
    }

    [FixtureFact]
    public async Task EndpointsListAddressesWithPorts()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var endpoints = await client.GetEndpointsAsync();
        Assert.NotEmpty(endpoints);
        Assert.All(endpoints, e => Assert.Contains(':', e));
    }

    [FixtureFact]
    public async Task OffsetForTimeFindsARecentRecordAndNoFutureOne()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        // A record exists by now: an hour ago has an offset, an hour ahead none.
        await client.PublishOnShardAsync(f.Tenant, f.Namespace, f.Stream, 0, [Bytes($"cs-time-{Run()}")]);
        var now = DateTimeOffset.UtcNow;
        Assert.NotNull(await client.OffsetForTimeAsync(f.Tenant, f.Namespace, f.Stream, 0, now.AddHours(-1)));
        Assert.Null(await client.OffsetForTimeAsync(f.Tenant, f.Namespace, f.Stream, 0, now.AddHours(1)));

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => client.OffsetForTimeAsync(f.Tenant, f.Namespace, f.Stream, 0, now, cts.Token));
    }
}
