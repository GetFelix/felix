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
    public async Task ARefusedIdentityTokenHandsOutNoClient()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await Assert.ThrowsAsync<FelixException>(() => client.WithIdentityAsync(f.Tenant, "not-a-token"));
    }
}
