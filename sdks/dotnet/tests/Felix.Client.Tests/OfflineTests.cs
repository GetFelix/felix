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

    [Fact]
    public async Task ACancelledTokenFailsBeforeAnythingIsSent()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["127.0.0.1:1"], TenantId = "t", Token = "x" }, cts.Token));
    }
}
