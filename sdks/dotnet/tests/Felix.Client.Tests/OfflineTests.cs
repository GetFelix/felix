using Xunit;

namespace Felix.Client.Tests;

public class OptionsTests
{
    [Fact]
    public async Task ConnectNeedsAnAddress()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = [], TenantId = "t", Token = "x" }));
    }

    [Fact]
    public async Task ConnectNeedsOptions()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => FelixClient.ConnectAsync(null!));
    }

    public static TheoryData<int?, double?> BadReconnectOptions => new()
    {
        { 0, null },
        { -1, null },
        { null, 0 },
        { null, -1000 },
        { null, 400.0 * 24 * 3600 * 1000 },
    };

    [Theory]
    [MemberData(nameof(BadReconnectOptions))]
    public async Task ReconnectOptionsAreValidatedBeforeAnythingIsSent(int? attempts, double? deadlineMs)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = ["127.0.0.1:1"],
            TenantId = "t",
            Token = "x",
            ReconnectAttempts = attempts,
            ReconnectDeadline = deadlineMs is { } ms ? TimeSpan.FromMilliseconds(ms) : null,
        }));
    }

    public static TheoryData<double?, double?> BadBackoffs => new()
    {
        { 0, null },
        { -1, null },
        { 400.0 * 24 * 3600 * 1000, null },
        { null, 0 },
        { null, -1 },
        { null, 400.0 * 24 * 3600 * 1000 },
    };

    [Theory]
    [MemberData(nameof(BadBackoffs))]
    public async Task BackoffOptionsAreValidatedBeforeAnythingIsSent(double? backoffMs, double? maxBackoffMs)
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(new FelixClientOptions
        {
            Addrs = ["127.0.0.1:1"],
            TenantId = "t",
            Token = "x",
            ReconnectBackoff = backoffMs is { } b ? TimeSpan.FromMilliseconds(b) : null,
            ReconnectMaxBackoff = maxBackoffMs is { } m ? TimeSpan.FromMilliseconds(m) : null,
        }));
    }

    [Fact]
    public void TheNativeOptionsCarryEachConfiguredValueInItsOwnField()
    {
        var native = FelixClient.NativeOptionsOf(new FelixClientOptions
        {
            Addrs = ["127.0.0.1:1"],
            TenantId = "t",
            Token = "x",
            ReconnectAttempts = 7,
            ReconnectDeadline = TimeSpan.FromMilliseconds(1500),
            ReconnectBackoff = TimeSpan.FromMilliseconds(50),
            ReconnectMaxBackoff = TimeSpan.FromMilliseconds(400),
        });
        Assert.Equal(7u, native.ReconnectAttempts);
        Assert.Equal(1500ul, native.ReconnectDeadlineMs);
        Assert.Equal(50ul, native.ReconnectBackoffMs);
        Assert.Equal(400ul, native.ReconnectMaxBackoffMs);

        var unset = FelixClient.NativeOptionsOf(new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "t", Token = "x" });
        Assert.Equal(0ul, unset.ReconnectBackoffMs);
        Assert.Equal(0ul, unset.ReconnectMaxBackoffMs);
    }

    [Fact]
    public async Task ACancelledTokenFailsBeforeAnythingIsSent()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "t", Token = "x" }, cts.Token));
    }
}
