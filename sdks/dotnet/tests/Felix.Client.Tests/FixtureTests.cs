using System.Text.Json;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Skips with a reason when the broker fixture is not available.</summary>
public class FixtureFactAttribute : FactAttribute
{
    public FixtureFactAttribute()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FELIX_FIXTURE")))
        {
            Skip = "FELIX_FIXTURE is not set; run against `felix-cluster client-fixture` (see sdks/dotnet/README.md)";
        }
        else if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FELIX_NATIVE_LIB")))
        {
            Skip = "FELIX_NATIVE_LIB is not set; build libfelix with `cargo build --release -p felix-capi`";
        }
    }
}

/// <summary>As <see cref="FixtureFactAttribute"/>, and skips when the fixture has no group operator token.</summary>
public sealed class GroupFixtureFactAttribute : FixtureFactAttribute
{
    public GroupFixtureFactAttribute()
    {
        if (Skip is null && Fixture.Load().GroupOperatorToken is null)
        {
            Skip = "the fixture has no group_operator_token (an older client-fixture)";
        }
    }
}

internal sealed record Fixture(
    string[] Addrs,
    string Tenant,
    string Namespace,
    string Token,
    string CaFile,
    string Stream,
    string MissingStream,
    string UnauthorizedToken,
    string? GroupOperatorToken,
    bool RecordsPublishers)
{
    internal static Fixture Load()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Environment.GetEnvironmentVariable("FELIX_FIXTURE")!));
        var root = doc.RootElement;
        string Text(string name) => root.GetProperty(name).GetString()!;
        return new Fixture(
            root.GetProperty("addrs").EnumerateArray().Select(a => a.GetString()!).ToArray(),
            Text("tenant_id"),
            Text("namespace"),
            Text("token"),
            Text("ca_file"),
            Text("durable_stream"),
            Text("missing_stream"),
            Text("unauthorized_token"),
            root.TryGetProperty("group_operator_token", out var op) && op.ValueKind == JsonValueKind.String ? op.GetString() : null,
            root.TryGetProperty("records_publishers", out var rp) && rp.ValueKind == JsonValueKind.True);
    }

    internal Task<FelixClient> ConnectAsync(string? token = null, CancellationToken ct = default) =>
        ConnectAsync(token, publishers: false, timestamps: false, ct);

    internal Task<FelixClient> ConnectAsync(string? token, bool publishers, bool timestamps, CancellationToken ct = default) =>
        FelixClient.ConnectAsync(
            new FelixClientOptions
            {
                Addrs = Addrs,
                TenantId = Tenant,
                Token = token ?? Token,
                CaFile = CaFile,
                Publishers = publishers,
                Timestamps = timestamps,
            },
            ct);
}

public class FixtureTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    [FixtureFact]
    [Scenario("pubsub.roundtrip", "ack.carries_the_offset")]
    public async Task PublishedRecordIsDeliveredWithItsOffset()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        var offset = await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "hello"u8.ToArray());
        var ev = await sub.ReadAsync(Wait);

        Assert.NotNull(ev);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(ev!.Payload.Span));
        Assert.NotNull(offset);
        Assert.Equal(offset, ev.Offset);
    }

    [FixtureFact]
    [Scenario("pubsub.order")]
    public async Task BatchRecordsArriveInOrder()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        var payloads = new ReadOnlyMemory<byte>[] { "a"u8.ToArray(), "bb"u8.ToArray(), "ccc"u8.ToArray() };
        var offsets = await client.PublishBatchAsync(f.Tenant, f.Namespace, f.Stream, payloads);
        Assert.Equal(3, offsets.Length);

        for (var i = 0; i < payloads.Length; i++)
        {
            var ev = await sub.ReadAsync(Wait);
            Assert.True(payloads[i].Span.SequenceEqual(ev!.Payload.Span));
        }
    }

    [FixtureFact]
    public async Task ReadTimesOutWithoutLosingAnything()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        Assert.Null(await sub.ReadAsync(TimeSpan.FromMilliseconds(100)));
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "late"u8.ToArray());
        Assert.NotNull(await sub.ReadAsync(Wait));
    }

    [FixtureFact]
    public async Task CancellingARead()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sub.ReadAsync(Timeout.InfiniteTimeSpan, cts.Token));
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "after"u8.ToArray());
        Assert.NotNull(await sub.ReadAsync(Wait));
    }

    [FixtureFact]
    public async Task AsyncEnumerationYieldsEvents()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "one"u8.ToArray());

        using var cts = new CancellationTokenSource(Wait);
        await foreach (var ev in sub.WithCancellation(cts.Token))
        {
            Assert.Equal(3, ev.Payload.Length);
            break;
        }
    }

    [FixtureFact]
    [Scenario("error.unknown_stream_is_typed")]
    public async Task UnknownStreamIsTyped()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var error = await Assert.ThrowsAsync<FelixException>(
            () => client.PublishAsync(f.Tenant, f.Namespace, f.MissingStream, "x"u8.ToArray()));
        Assert.True(error.Status is FelixStatus.NotFound or FelixStatus.ShardUnavailable, error.Status.ToString());
        Assert.False(string.IsNullOrEmpty(error.Message));
    }

    [FixtureFact]
    [Scenario("error.unauthorized_is_typed")]
    public async Task UnauthorizedIsTypedAndNotRetryable()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync(f.UnauthorizedToken);
        var error = await Assert.ThrowsAsync<FelixException>(
            () => client.PublishAsync(f.Tenant, f.Namespace, f.Stream, "x"u8.ToArray()));
        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.False(error.IsRetryable);
    }

    [FixtureFact]
    public async Task CancelledConnect()
    {
        var f = Fixture.Load();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => f.ConnectAsync(ct: cts.Token));
    }
}
