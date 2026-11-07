using System.Text;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Inspection, counters, queue and transport options and hand-offs against the fixture.</summary>
public class ParityMoreFixtureTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    [FixtureFact]
    public async Task TheBrokerNamesWhatItAnswers()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var supports = await client.GetSupportsAsync();
        Assert.True(supports.Inspect && supports.Topology && supports.StreamShards && supports.CacheShards && supports.ShardOwners);
    }

    [FixtureFact]
    public async Task ShardCountsOwnersAndRoutingAgree()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var routing = await client.GetStreamRoutingAsync(f.Tenant, f.Namespace, f.Stream);
        Assert.True(routing.Shards >= 1);
        Assert.True(routing.Routing is ShardRouting.Modulo or ShardRouting.JumpHash);

        var owners = await client.GetShardOwnersAsync(ShardKind.Stream, f.Tenant, f.Namespace, f.Stream);
        Assert.Equal(Enumerable.Range(0, (int)routing.Shards).Select(i => (uint)i), owners.Select(o => o.Shard).Order());

        Assert.True(await client.GetCacheShardsAsync(f.Tenant, f.Namespace, f.Cache) >= 1);
        var brokers = await client.GetTopologyAsync();
        Assert.NotEmpty(brokers);
        Assert.All(brokers, b => Assert.False(string.IsNullOrEmpty(b.Addr)));
    }

    [FixtureFact]
    public async Task ATenantTokenMayNotInspect()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        Assert.IsType<FelixException>(await Record.ExceptionAsync(() =>
            client.InspectShardAsync(ShardKind.Stream, f.Tenant, f.Namespace, f.Stream, 0)));
        Assert.IsType<FelixException>(await Record.ExceptionAsync(() =>
            client.ListSubscriptionsAsync(new SubscriptionFilter { Stream = f.Stream })));
    }

    private static async Task<FelixClient> ConnectToAsync(Fixture f, string addr, string token) =>
        await FelixClient.ConnectAsync(new FelixClientOptions { Addrs = [addr], TenantId = f.Tenant, Token = token, CaFile = f.CaFile });

    [InspectorFixtureFact]
    public async Task AnInspectorSeesTheLeadersViewOfAShard()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var owner = (await client.GetShardOwnersAsync(ShardKind.Stream, f.Tenant, f.Namespace, f.Stream)).First(o => o.Shard == 0);
        Assert.NotNull(owner.Addr);

        await using var ops = await ConnectToAsync(f, owner.Addr!, f.InspectorToken!);
        var view = await ops.InspectShardAsync(ShardKind.Stream, f.Tenant, f.Namespace, f.Stream, 0);
        Assert.Equal(owner.NodeId, view.NodeId);
        Assert.Equal("leader", view.Role);
        Assert.True(view.Serving);
        Assert.Equal("active", view.Phase);
        Assert.True(view.Tail >= 0);
    }

    [InspectorFixtureFact]
    public async Task AnInspectorPagesThroughABrokersSubscriptions()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var owner = (await client.GetShardOwnersAsync(ShardKind.Stream, f.Tenant, f.Namespace, f.Stream)).First(o => o.Shard == 0);
        await using var subscriber = await ConnectToAsync(f, owner.Addr!, f.Token);
        await using var ops = await ConnectToAsync(f, owner.Addr!, f.InspectorToken!);
        var held = new List<FelixSubscription>();
        try
        {
            for (var i = 0; i < 3; i++)
            {
                held.Add(await subscriber.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest));
            }

            var filter = new SubscriptionFilter { TenantId = f.Tenant, Namespace = f.Namespace, Stream = f.Stream, Shard = 0 };
            var seen = new List<long>();
            SubscriptionCursor? cursor = null;
            var pages = 0;
            do
            {
                var page = await ops.ListSubscriptionsAsync(filter, 2, cursor);
                pages++;
                Assert.Equal(owner.NodeId, page.NodeId);
                Assert.True(page.Subscriptions.Count <= 2);
                seen.AddRange(page.Subscriptions.Select(s => s.SubscriberId));
                cursor = page.NextCursor;
            }
            while (cursor is not null && pages < 50);

            Assert.Equal(seen.Count, seen.Distinct().Count());
            Assert.True(seen.Count >= 3 && pages >= 2);

            var first = await ops.ListSubscriptionsAsync(filter, 1);
            Assert.NotNull(first.NextCursor);
            Assert.NotEmpty(first.Subscriptions[0].Policy);
            Assert.True(first.Subscriptions[0].Capacity > 0);
        }
        finally
        {
            foreach (var subscription in held)
            {
                subscription.Dispose();
            }
        }
    }

    [FixtureFact]
    public async Task TheQueueAndTransportOptionsAreAppliedAndTheBrokerReportsItsGrant()
    {
        var f = Fixture.Load();
        await using var tuned = await FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = f.Addrs,
            TenantId = f.Tenant,
            Token = f.Token,
            CaFile = f.CaFile,
            ClientSubQueuePolicy = ClientSubQueuePolicy.Block,
            ClientSubQueueCapacity = 64,
            BrokerSubQueueCapacity = 8,
            PublishConnPool = 2,
            PublishStreamsPerConn = 2,
            PublishQueueDepth = 32,
            PublishInflightBytes = 2 << 20,
            PublishSharding = PublishSharding.RoundRobin,
            PublishShardStreams = 0,
            CacheConnPool = 2,
            CacheStreamsPerConn = 2,
            EventConnPool = 2,
            ClusterConnPool = 4,
            ClusterStreamsPerConn = 512,
            EventRouterMaxPending = 4096,
            MaxFrameBytes = 8 << 20,
            EventConnRecvWindow = 64 << 20,
            EventStreamRecvWindow = 16 << 20,
            EventSendWindow = 64 << 20,
            CacheConnRecvWindow = 64 << 20,
            CacheStreamRecvWindow = 16 << 20,
            CacheSendWindow = 64 << 20,
            AckOnCommit = true,
            ConnectTimeout = TimeSpan.FromSeconds(8),
            FinishTimeout = TimeSpan.FromSeconds(30),
        });
        await using var sub = await tuned.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        Assert.Equal(8u, sub.QueueCapacity);

        var body = $"cs-{Run()}-tuned";
        await tuned.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes(body));
        var ev = await sub.ReadAsync(Wait);
        Assert.Equal(body, Text(ev!.Payload));

        // Nothing moved the shard or dropped the connection.
        Assert.Equal(0, sub.Moves);
        Assert.Equal(0, sub.Reconnects);
        Assert.False(sub.StreamDeleted);
    }

    [FixtureFact]
    public async Task WithoutARequestTheBrokerQueueSizeIsNotReported()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        Assert.Null(sub.QueueCapacity);
    }

    [FixtureFact]
    public async Task HandedOffBatchesAreSentInOrderAndAnsweredOnce()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var producer = await client.CreateProducerAsync();
        var run = Run();

        var handed = new List<Handed>();
        for (var i = 0; i < 4; i++)
        {
            ReadOnlyMemory<byte>[] payloads = [Encoding.UTF8.GetBytes($"cs-{run}-h{i}-0"), Encoding.UTF8.GetBytes($"cs-{run}-h{i}-1")];
            handed.Add(await producer.HandOffBatchAsync(f.Tenant, f.Namespace, f.Stream, payloads));
        }

        long? previous = null;
        foreach (var h in handed)
        {
            var offset = await h.OffsetAsync();
            if (offset is { } now && previous is { } before)
            {
                Assert.True(now > before, $"offset {now} did not follow {before}");
            }

            previous = offset ?? previous;
            Assert.Throws<InvalidOperationException>(() => { _ = h.OffsetAsync(); });
            Assert.Throws<InvalidOperationException>(() => { _ = h.DoneAsync(); });
            h.Dispose();
        }
    }

    [FixtureFact]
    public async Task SeveralBatchesAndAnAbandonAreHandedOffAndDone()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var producer = await client.CreateProducerAsync();
        var run = Run();
        ReadOnlyMemory<byte>[] two = [Encoding.UTF8.GetBytes($"cs-{run}-a"), Encoding.UTF8.GetBytes($"cs-{run}-b")];
        ReadOnlyMemory<byte>[] one = [Encoding.UTF8.GetBytes($"cs-{run}-c")];

        using (var many = await producer.HandOffBatchesAsync(f.Tenant, f.Namespace, f.Stream, [two, one]))
        {
            await many.DoneAsync();
            Assert.Throws<InvalidOperationException>(() => { _ = many.DoneAsync(); });
        }

        // Nothing is in doubt, so giving up is a no-op that succeeds.
        using var abandon = await producer.HandOffAbandonAsync(f.Tenant, f.Namespace, f.Stream);
        await abandon.DoneAsync();
    }

    [FixtureFact]
    public async Task AClosedProducerTakesNoMoreHandOffs()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var producer = await client.CreateProducerAsync();
        await producer.CloseAsync(TimeSpan.FromSeconds(5));
        var error = await Assert.ThrowsAsync<FelixException>(() =>
            producer.HandOffBatchAsync(f.Tenant, f.Namespace, f.Stream, ["late"u8.ToArray()]));
        Assert.Equal(FelixStatus.Closed, error.Status);
    }
}
