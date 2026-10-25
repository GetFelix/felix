using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>The sharded and shard-addressed calls with no broker: argument checks and ownership.</summary>
public class ShardedTests
{
    private static FelixClient DisposedClient()
    {
        var client = new FelixClient(new FelixClientHandle(0));
        client.Dispose();
        return client;
    }

    [Fact]
    public void ShardItemKindsMatchTheHeader()
    {
        Assert.Equal(1, (int)ShardItemKind.Record);
        Assert.Equal(2, (int)ShardItemKind.Lost);
        Assert.Equal(3, (int)ShardItemKind.Recovered);
        Assert.Equal(4, (int)ShardItemKind.Moved);
        Assert.Equal(5, (int)ShardItemKind.StreamDeleted);
    }

    [Fact]
    public async Task EveryNewCallOnADisposedClientThrowsAndLeaksNothing()
    {
        var client = DisposedClient();
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;
        ReadOnlyMemory<byte> one = new byte[] { 1 };

        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.GetEndpointsAsync(cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.PublishOnShardAsync("t", "n", "s", 1, [one], cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.PublishRecordsAsync("t", "n", "s", [(one, one)], cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.OffsetForTimeAsync("t", "n", "s", 0, DateTimeOffset.UnixEpoch, cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.SubscribeShardedAsync("t", "n", "s", StartPosition.Latest, cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            client.ResubscribeShardedAsync("t", "n", "s", [new ShardPosition(0, 4)], StartPosition.Latest, cts.Token));

        Assert.Equal(before, CallStateBase.Live);
    }

    [Fact]
    public async Task ACancelledTokenStopsOffsetForTimeBeforeAnythingIsSent()
    {
        var client = DisposedClient();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // A time before the epoch is also refused; the cancelled token is checked first, like the other calls.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.OffsetForTimeAsync("t", "n", "s", 0, DateTimeOffset.UnixEpoch.AddDays(-1), cts.Token));
    }

    [Fact]
    public unsafe void APartlyFailedPublishKeepsEveryRecordsOutcome()
    {
        var refused = "shard moved"u8.ToArray();
        fixed (byte* message = refused)
        {
            var native = stackalloc NativeRecordResult[2];
            native[0] = new NativeRecordResult { Status = 0, HasOffset = 1, Offset = 7 };
            native[1] = new NativeRecordResult { Status = (int)FelixStatus.ShardUnavailable, Message = message };

            var outcomes = Callbacks.CopyRecordResults(native, 2);

            Assert.Equal(2, outcomes.Length);
            Assert.Equal(7, outcomes[0].Offset);
            Assert.Equal(FelixStatus.Ok, outcomes[0].Status);
            Assert.Null(outcomes[0].Message);
            Assert.Null(outcomes[1].Offset);
            Assert.Equal(FelixStatus.ShardUnavailable, outcomes[1].Status);
            Assert.Equal("shard moved", outcomes[1].Message);
        }
    }

    [Fact]
    public async Task ArgumentsAreCheckedBeforeAnythingIsSent()
    {
        var client = DisposedClient();
        ReadOnlyMemory<byte> one = new byte[] { 1 };

        await Assert.ThrowsAsync<ArgumentException>(() => client.PublishOnShardAsync("t", "n", "s", 0, []));
        await Assert.ThrowsAsync<ArgumentException>(() => client.PublishRecordsAsync("t", "n", "s", []));
        await Assert.ThrowsAsync<ArgumentException>(() => client.PublishOnShardAsync("t\0", "n", "s", 0, [one]));
        await Assert.ThrowsAsync<ArgumentException>(() => client.SubscribeShardedAsync("t", "n", "s\0", StartPosition.Latest));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.ResubscribeShardedAsync("t", "n\0", "s", [], StartPosition.Latest));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            client.OffsetForTimeAsync("t", "n", "s", 0, DateTimeOffset.UnixEpoch.AddSeconds(-1)));
    }

    [Fact]
    public void APublishResultIsAcceptedOnlyWhenOk()
    {
        Assert.True(new PublishResult(3, FelixStatus.Ok, null).Accepted);
        Assert.False(new PublishResult(null, FelixStatus.NotFound, "no such stream").Accepted);
    }
}
