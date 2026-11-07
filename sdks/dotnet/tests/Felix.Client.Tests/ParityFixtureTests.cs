using System.Text;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Record headers, page reads, identities, topology and the live offset against the fixture.</summary>
public class ParityFixtureTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static string Text(ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);

    [FixtureFact]
    public async Task ARecordKeepsItsKeyAndHeadersInOrder()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        var body = $"cs-{Run()}-headered";

        var offset = await client.PublishWithHeadersAsync(
            f.Tenant, f.Namespace, f.Stream,
            new HeaderedRecord(
                Encoding.UTF8.GetBytes(body),
                "k-1"u8.ToArray(),
                [new Header("trace", "t-1"u8.ToArray()), new Header("trace", ReadOnlyMemory<byte>.Empty), new Header("kind", "order"u8.ToArray())]));

        var ev = await sub.ReadAsync(Wait);
        Assert.NotNull(ev);
        Assert.Equal(body, Text(ev!.Payload));
        Assert.Equal(offset, ev.Offset);
        Assert.Equal("k-1", Text(ev.Key!.Value));
        Assert.NotNull(ev.Headers);
        Assert.Equal(["trace", "trace", "kind"], ev.Headers!.Select(h => h.Name));
        Assert.Equal("t-1", Text(ev.Headers[0].Value));
        Assert.True(ev.Headers[1].Value.IsEmpty);
        Assert.Equal("order", Text(ev.Headers[2].Value));
    }

    [FixtureFact]
    public async Task APlainEventHasNoKeyOrHeaders()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "plain"u8.ToArray());

        var ev = await sub.ReadAsync(Wait);
        Assert.NotNull(ev);
        Assert.Null(ev!.Key);
        Assert.True(ev.Headers is null || ev.Headers.Count == 0);
    }

    [FixtureFact]
    public async Task AReservedHeaderNameIsRefusedBeforeAnythingIsSent()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var error = await Assert.ThrowsAsync<FelixException>(() =>
            client.PublishWithHeadersAsync(
                f.Tenant, f.Namespace, f.Stream,
                new HeaderedRecord("x"u8.ToArray(), null, [new Header("felix.x", ReadOnlyMemory<byte>.Empty)])));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
    }

    [FixtureFact]
    public async Task ABatchWithHeadersTakesConsecutiveOffsetsAndIsReadBack()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var records = Enumerable.Range(0, 3)
            .Select(i => new HeaderedRecord(Encoding.UTF8.GetBytes($"cs-{run}-b{i}"), null, [new Header("n", Encoding.UTF8.GetBytes($"{i}"))]))
            .ToArray();

        var offsets = await client.PublishBatchWithHeadersAsync(f.Tenant, f.Namespace, f.Stream, records);
        Assert.Equal(3, offsets.Length);
        Assert.All(offsets, o => Assert.NotNull(o));
        Assert.Equal(offsets[0] + 1, offsets[1]);
        Assert.Equal(offsets[1] + 1, offsets[2]);

        // The page read returns the same records without a subscription.
        var page = await client.ReadRangeAsync(f.Tenant, f.Namespace, f.Stream, 0, offsets[0]!.Value, maxRecords: 3);
        Assert.Equal(3, page.Records.Count);
        Assert.Equal($"cs-{run}-b0", Text(page.Records[0].Payload));
        Assert.Equal(offsets[0], page.Records[0].Offset);
        Assert.Equal(page.Records[^1].Offset + 1, page.NextOffset);

        // An end bound stops the page.
        var bounded = await client.ReadRangeAsync(f.Tenant, f.Namespace, f.Stream, 0, offsets[0]!.Value, end: offsets[1]!.Value);
        Assert.Equal([offsets[0]!.Value], bounded.Records.Select(r => r.Offset));
    }

    [FixtureFact]
    public async Task ALiveOffsetFromLatestIsTheStartOffset()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        Assert.NotNull(sub.LiveOffset);
        Assert.Equal(sub.StartOffset, sub.LiveOffset);
    }

    [FixtureFact]
    public async Task TheClusterNamesItsBrokersAndTheClientCountsItsConnections()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        Assert.True(await client.RefreshTopologyAsync() >= 1);
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "warm"u8.ToArray());
        var nodes = await client.GetConnectionsPerNodeAsync();
        Assert.NotEmpty(nodes);
        Assert.All(nodes, n => Assert.True(n.Connections >= 1));
    }

    [FixtureFact]
    public async Task AnIdentityPublishesOverTheSameConnectionsAndOutlivesItsParent()
    {
        var f = Fixture.Load();
        var client = await f.ConnectAsync();
        await using var identity = await client.WithIdentityAsync(f.Tenant, f.Token);
        await client.DisposeAsync();

        var offset = await identity.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes($"cs-{Run()}-identity"));
        Assert.NotNull(offset);
    }

    [FixtureFact]
    public async Task AnIdentityOutlivesAParentWhoseProviderItShares()
    {
        var f = Fixture.Load();
        var fetches = 0;
        var client = await FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = f.Addrs,
            TenantId = f.Tenant,
            CaFile = f.CaFile,
            TokenProvider = _ =>
            {
                Interlocked.Increment(ref fetches);
                return ValueTask.FromResult(f.Token);
            },
        });
        var source = client.Tokens!;
        await using var identity = await client.WithIdentityAsync(f.Tenant, f.Token);
        Assert.Same(source, identity.Tokens);
        await client.DisposeAsync();

        Assert.False(source.IsCancelled);
        var offset = await identity.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes($"cs-{Run()}-shared"));
        Assert.NotNull(offset);
        await using var sub = await identity.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        Assert.False(source.IsCancelled);
        Assert.True(fetches >= 1);
    }

    [FixtureFact]
    public async Task AnIdentityWithAProviderAsksItForTheToken()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var fetches = 0;
        await using var identity = await client.WithIdentityAsync(
            f.Tenant,
            _ =>
            {
                Interlocked.Increment(ref fetches);
                return ValueTask.FromResult(f.Token);
            });

        var offset = await identity.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes($"cs-{Run()}-provider"));
        Assert.NotNull(offset);
        Assert.True(fetches >= 1, "with no first token the provider supplies it");

        await using var first = await client.WithIdentityAsync(f.Tenant, _ => throw new InvalidOperationException("not needed"), f.Token);
        Assert.NotNull(await first.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes($"cs-{Run()}-first")));
    }

    [FixtureFact]
    public async Task AProviderIdentityWhoseTokenIsRefusedHandsOutNoClient()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await Assert.ThrowsAsync<FelixException>(() =>
            client.WithIdentityAsync(f.Tenant, _ => ValueTask.FromResult("not-a-token")));
    }

    [FixtureFact]
    public async Task ALeaseTokenMintedByTheBrokerActsForItsOwnOutputOnly()
    {
        var f = Fixture.Load();
        Assert.NotNull(f.LeaseNodeToken);
        Assert.NotNull(f.LeaseCache);
        var (key, other) = ("cs", "cs2");
        Assert.Contains(key, f.LeaseKeys);
        Assert.Contains(other, f.LeaseKeys);

        await using var node = await f.ConnectAsync(f.LeaseNodeToken);
        var cache = node.Cache(f.Tenant, f.Namespace, f.LeaseCache!);
        var output = $"wl.{key}.out";

        // The node's own token cannot publish to the output; only a lease token can.
        await Assert.ThrowsAsync<FelixException>(() =>
            node.PublishAsync(f.Tenant, f.Namespace, output, "denied"u8.ToArray()));

        var lease = await cache.PutIfAbsentLeaseAsync(key, "node"u8.ToArray(), ttl: TimeSpan.FromMilliseconds(2000));
        Assert.True(lease.Result.Applied);
        var token = lease.Token;
        Assert.NotNull(token);

        await using var holder = await node.WithIdentityAsync(f.Tenant, token!.Value);
        Assert.NotNull(await holder.PublishAsync(f.Tenant, f.Namespace, output, "held"u8.ToArray()));

        // A second claim loses the write and gets no token.
        var lost = await cache.PutIfAbsentLeaseAsync(key, "node"u8.ToArray(), ttl: TimeSpan.FromMilliseconds(2000));
        Assert.False(lost.Result.Applied);
        Assert.Null(lost.Token);

        // The token names this key's output and no other.
        await using var wrong = await node.WithIdentityAsync(f.Tenant, token.Value);
        await Assert.ThrowsAsync<FelixException>(() =>
            wrong.PublishAsync(f.Tenant, f.Namespace, $"wl.{other}.out", "other"u8.ToArray()));

        // Once the token expires, the identity it opened stops being served and no new one opens.
        var ended = false;
        for (var i = 0; i < 80 && !ended; i++)
        {
            try
            {
                await holder.PublishAsync(f.Tenant, f.Namespace, output, "late"u8.ToArray());
                await Task.Delay(100);
            }
            catch (FelixException)
            {
                ended = true;
            }
        }

        Assert.True(ended, "a stale lease token kept publishing");
        await Assert.ThrowsAsync<FelixException>(() => node.WithIdentityAsync(f.Tenant, token.Value));
    }

    [FixtureFact]
    public async Task ARefusedIdentityTokenHandsOutNoClient()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await Assert.ThrowsAsync<FelixException>(() => client.WithIdentityAsync(f.Tenant, "not-a-token"));
    }
}
