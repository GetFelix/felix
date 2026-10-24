using Felix.Native;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Felix.Client.Tests;

public class LeakTests
{
    // A client whose handle is already closed, so no native library is involved.
    private static FelixClient DisposedClient()
    {
        var client = new FelixClient(new FelixClientHandle(0));
        client.Dispose();
        return client;
    }

    [Fact]
    public async Task PublishOnADisposedClientThrowsAndLeaksNothing()
    {
        var client = DisposedClient();
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PublishAsync("t", "n", "s", new byte[] { 1 }, cancellationToken: cts.Token));

        Assert.Equal(before, CallStateBase.Live);
    }

    [Fact]
    public async Task BatchOnADisposedClientLeaksNothing()
    {
        var client = DisposedClient();
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.PublishBatchAsync("t", "n", "s", [new byte[] { 1 }], cancellationToken: cts.Token));

        Assert.Equal(before, CallStateBase.Live);
    }

    [Fact]
    public async Task SubscribeOnADisposedClientLeaksNothing()
    {
        var client = DisposedClient();
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await client.SubscribeAsync("t", "n", "s", 0, StartPosition.Latest, cts.Token));

        Assert.Equal(before, CallStateBase.Live);
    }
}

public class CancelRaceTests
{
    private static unsafe void Connect(nint userData) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnConnect)(userData, 0, null, 0);

    private static unsafe void Subscribe(nint userData) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnSubscribe)(userData, 0, null, 0);

    // A connect that succeeds as the token is cancelled must not hand out a client.
    [NativeFact]
    public async Task ConnectCompletingAfterCancelHandsOutNothing()
    {
        using var cts = new CancellationTokenSource();
        var state = new CallState<FelixClientHandle>(cts.Token);
        var userData = state.Pin();
        cts.Cancel();
        Connect(userData);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => state.Task);
        state.Abandon();
    }

    [NativeFact]
    public async Task SubscribeCompletingAfterCancelHandsOutNothing()
    {
        using var cts = new CancellationTokenSource();
        var state = new CallState<FelixSubscriptionHandle>(cts.Token);
        var userData = state.Pin();
        cts.Cancel();
        Subscribe(userData);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => state.Task);
        state.Abandon();
    }
}
