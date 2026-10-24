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

public class HandleOwnershipTests
{
    // The failure behind the net8.0 test host crash: a test ran a callback by hand (which
    // freed the call's GCHandle) and then Abandoned the call (which freed it again). The
    // double free leaves a slot in the runtime's handle table twice, so later calls that are
    // live together are given the same slot, and one callback completes another call's state
    // or finds none (a NullReferenceException in OnPublish). Fails on the old code.
    [Fact]
    public void ADoubleFreeNeverMakesLiveCallsShareAHandle()
    {
        for (var round = 0; round < 200; round++)
        {
            for (var i = 0; i < 70; i++)
            {
                var done = new CallState<long?>(default);
                var userData = done.Pin();
                CallState<long?>.Take(userData);
                done.Abandon();
            }

            var live = new List<(CallState<long?> State, nint Data)>();
            for (var i = 0; i < 150; i++)
            {
                var state = new CallState<long?>(default);
                live.Add((state, state.Pin()));
            }

            Assert.Equal(live.Count, live.Select(l => l.Data).Distinct().Count());
            foreach (var (state, data) in live)
            {
                Assert.Same(state, CallState<long?>.Take(data));
            }
        }
    }

    // The callback frees the handle through the state, so a later Abandon, or a second
    // callback path, cannot free the same slot again.
    [Fact]
    public void TheHandleIsFreedExactlyOnceWhoeverGetsThereFirst()
    {
        var state = new CallState<long?>(default);
        var userData = state.Pin();
        var before = CallStateBase.HandlesFreed;

        Assert.Same(state, CallState<long?>.Take(userData));
        Assert.Equal(before + 1, CallStateBase.HandlesFreed);
        state.Abandon();
        state.Abandon();
        Assert.Equal(before + 1, CallStateBase.HandlesFreed);
    }

    [Fact]
    public void TakingAnUnknownHandleReturnsNullInsteadOfThrowing()
    {
        var state = new CallState<long?>(default);
        var userData = state.Pin();
        Assert.Same(state, CallState<long?>.Take(userData));
        Assert.Null(CallState<long?>.Take(0));
    }
}
