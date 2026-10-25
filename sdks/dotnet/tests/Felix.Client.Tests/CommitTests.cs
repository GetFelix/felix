using System.Runtime.InteropServices;
using System.Text;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Commits, conditional writes, state reads and producers with no broker.</summary>
public class CommitTests
{
    private static FelixClient DisposedClient()
    {
        var client = new FelixClient(new FelixClientHandle(0));
        client.Dispose();
        return client;
    }

    private static FelixProducer ClosedHandleProducer()
    {
        var producer = new FelixProducer(new FelixProducerHandle(0));
        producer.Dispose();
        return producer;
    }

    private static readonly byte[] One = [1];

    [Fact]
    public unsafe void NativeStructsMatchTheHeader()
    {
        if (nint.Size != 8)
        {
            return;
        }

        Assert.Equal(40, sizeof(NativeCommitOp));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeCommitOp>(nameof(NativeCommitOp.Stream)));
        Assert.Equal(16, (int)Marshal.OffsetOf<NativeCommitOp>(nameof(NativeCommitOp.Key)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeCommitOp>(nameof(NativeCommitOp.Data)));
        Assert.Equal(32, (int)Marshal.OffsetOf<NativeCommitOp>(nameof(NativeCommitOp.DataLen)));
        Assert.Equal(56, sizeof(NativeStateValue));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeStateValue>(nameof(NativeStateValue.Value)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeStateValue>(nameof(NativeStateValue.HasVersion)));
        Assert.Equal(32, (int)Marshal.OffsetOf<NativeStateValue>(nameof(NativeStateValue.Version)));
        Assert.Equal(40, (int)Marshal.OffsetOf<NativeStateValue>(nameof(NativeStateValue.HasAsOf)));
        Assert.Equal(48, (int)Marshal.OffsetOf<NativeStateValue>(nameof(NativeStateValue.AsOf)));
    }

    [Fact]
    public unsafe void OpsAreLaidOutForTheLibraryAndCopied()
    {
        var payload = new byte[] { 9, 8, 7 };
        var ops = new[]
        {
            CommitOp.Publish("orders", payload),
            CommitOp.Put("orders", "kéy", new byte[] { 5 }),
            CommitOp.Delete("orders", "gone"),
        };
        using var native = new NativeCommitOps(ops);
        payload[0] = 0; // the library's copy must not follow the caller's buffer

        Assert.Equal((nuint)3, native.Count);
        var first = native.Pointer[0];
        Assert.Equal((int)CommitOpKind.Publish, first.Kind);
        Assert.Equal("orders", Marshal.PtrToStringUTF8((nint)first.Stream));
        Assert.True(first.Key is null);
        Assert.Equal((nuint)3, first.DataLen);
        Assert.Equal(new byte[] { 9, 8, 7 }, new ReadOnlySpan<byte>(first.Data, 3).ToArray());

        var put = native.Pointer[1];
        Assert.Equal((int)CommitOpKind.Put, put.Kind);
        Assert.Equal("kéy", Marshal.PtrToStringUTF8((nint)put.Key));
        // NUL-terminated: the library reads the key as a C string.
        Assert.Equal(0, put.Key[Encoding.UTF8.GetByteCount("kéy")]);

        var delete = native.Pointer[2];
        Assert.Equal((int)CommitOpKind.Delete, delete.Kind);
        Assert.Equal((nuint)0, delete.DataLen);
        Assert.True(delete.Data is null);
    }

    [Fact]
    public void AnEmptyOpListIsLeftForTheLibraryToRefuse()
    {
        using var native = new NativeCommitOps([]);
        Assert.Equal((nuint)0, native.Count);
    }

    [Fact]
    public void OpsRefuseAnEmbeddedNul()
    {
        Assert.Throws<ArgumentException>(() => CommitOp.Publish("a\0b", One));
        Assert.Throws<ArgumentException>(() => CommitOp.Put("s", "k\0", One));
        Assert.Throws<ArgumentException>(() => CommitOp.Delete("s", "k\0"));
        Assert.Throws<ArgumentNullException>(() => CommitOp.Enqueue(null!, One));
    }

    [Fact]
    public async Task ACommitOnADisposedClientThrowsAndLeaksNothing()
    {
        var client = DisposedClient();
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;
        var ops = new[] { CommitOp.Publish("s", One) };

        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.CommitAsync("t", "n", One, ops, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.CommitAsync("t", "n", One, ops, shard: 1, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.CommitIfAsync("t", "n", One, ops, 0, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.CommitIfAsync("t", "n", One, ops, 0, 1, cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.PublishIfAsync("t", "n", "s", [One], 0, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.PublishIfAsync("t", "n", "s", [One], 0, shard: 2, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.GetStateAsync("t", "n", "s", "k", cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.GetStateAsync("t", "n", "s", "k", shard: 2, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.CreateProducerAsync(cts.Token));

        Assert.Equal(before, CallStateBase.Live);
    }

    [Fact]
    public async Task BadArgumentsAreRefusedBeforeAnyCall()
    {
        var client = DisposedClient();
        var ops = new[] { CommitOp.Publish("s", One) };

        await Assert.ThrowsAsync<ArgumentNullException>(() => client.CommitAsync("t", "n", One, null!));
        await Assert.ThrowsAsync<ArgumentException>(() => client.CommitAsync("t\0", "n", One, ops));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.CommitIfAsync("t", "n", One, ops, -1));
        await Assert.ThrowsAsync<ArgumentException>(() => client.PublishIfAsync("t", "n", "s", [], 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.PublishIfAsync("t", "n", "s", [One], -1));
        await Assert.ThrowsAsync<ArgumentException>(() => client.PublishIfAsync("t", "n", "s", [One], 0, One, 1));
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetStateAsync("t", "n", "s", "k\0"));
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetStateAsync("t", "n", "s", "k", One, 1));
    }

    [Fact]
    public async Task ProducerArgumentsAreCheckedBeforeAnyCall()
    {
        var producer = ClosedHandleProducer();
        var batch = (IReadOnlyList<ReadOnlyMemory<byte>>)[One];

        await Assert.ThrowsAsync<ArgumentException>(() => producer.PublishBatchAsync("t", "n", "s", []));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.PublishAsync("t", "n\0", "s", One));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.PublishBatchesAsync("t", "n", "s", []));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.PublishBatchesAsync("t", "n", "s", [batch, []]));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.AbandonInDoubtAsync("t", "n", "s\0"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => producer.CloseAsync(TimeSpan.FromSeconds(-2)));
        Assert.Throws<ArgumentException>(() => producer.GetIdFor("t", "n\0", "s"));
    }

    [Fact]
    public async Task AFreedProducerThrowsAndLeaksNothing()
    {
        var producer = ClosedHandleProducer();
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.PublishAsync("t", "n", "s", One, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.PublishBatchesAsync("t", "n", "s", [[One]], cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.AbandonInDoubtAsync("t", "n", "s", cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.CloseAsync(null, cts.Token));
        Assert.Throws<ObjectDisposedException>(() => producer.GetId());
        Assert.Throws<ObjectDisposedException>(() => producer.GetIdFor("t", "n", "s"));

        Assert.Equal(before, CallStateBase.Live);
    }

    [Fact]
    public async Task DisposingAProducerTwiceIsSafe()
    {
        var producer = ClosedHandleProducer();
        await producer.DisposeAsync();
        await producer.DisposeAsync();
        producer.Dispose();
    }

    private static unsafe void Call(delegate* unmanaged[Cdecl]<nint, int, byte*, ulong, void> callback, nint userData, int status, ulong offset) =>
        callback(userData, status, null, offset);

    [Fact]
    public async Task ACommitOffsetIsDelivered()
    {
        var state = new CallState<long>(default);
        var userData = state.Pin();
        unsafe { Call(&Callbacks.OnCommit, userData, 0, 42); }
        Assert.Equal(42L, await state.Task);
    }

    [Fact]
    public async Task ACommitFailureIsTyped()
    {
        var state = new CallState<long>(default);
        var userData = state.Pin();
        unsafe { Call(&Callbacks.OnCommit, userData, (int)FelixStatus.Unsupported, 0); }
        var error = await Assert.ThrowsAsync<FelixException>(() => state.Task);
        Assert.Equal(FelixStatus.Unsupported, error.Status);
    }

    [Fact]
    public async Task ARefusedConditionalWriteIsAnAnswerWithTheTail()
    {
        var state = new CallState<ConditionalWrite>(default);
        var userData = state.Pin();
        unsafe { ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, ulong, void>)&Callbacks.OnConditional)(userData, 0, null, 0, 17); }
        Assert.Equal(new ConditionalWrite(false, 17), await state.Task);

        state = new CallState<ConditionalWrite>(default);
        userData = state.Pin();
        unsafe { ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, ulong, void>)&Callbacks.OnConditional)(userData, 0, null, 1, 17); }
        Assert.Equal(new ConditionalWrite(true, 17), await state.Task);
    }

    [Fact]
    public async Task StateIsCopiedAndAbsentIsNotEmpty()
    {
        var (present, absent, empty) = await Deliveries();

        Assert.Equal("v", Encoding.UTF8.GetString(present.Value!.Value.Span));
        Assert.Equal(5, present.Version);
        Assert.Equal(9, present.AsOf);
        Assert.Null(absent.Value);
        Assert.Null(absent.Version);
        Assert.Equal(9, absent.AsOf);
        Assert.NotNull(empty.Value);
        Assert.True(empty.Value!.Value.IsEmpty);
    }

    private static unsafe Task<(StateValue, StateValue, StateValue)> Deliveries()
    {
        var value = "v"u8.ToArray();
        Task<StateValue> present;
        fixed (byte* pointer = value)
        {
            var native = new NativeStateValue { HasValue = 1, Value = pointer, ValueLen = 1, HasVersion = 1, Version = 5, HasAsOf = 1, AsOf = 9 };
            present = Deliver(&native);
            value[0] = (byte)'X'; // the copy must not follow native memory
        }

        var none = new NativeStateValue { HasAsOf = 1, AsOf = 9 };
        var absent = Deliver(&none);
        var blank = new NativeStateValue { HasValue = 1, HasVersion = 1, Version = 6 };
        var empty = Deliver(&blank);
        return Collect(present, absent, empty);
    }

    private static async Task<(StateValue, StateValue, StateValue)> Collect(Task<StateValue> a, Task<StateValue> b, Task<StateValue> c) =>
        (await a, await b, await c);

    private static unsafe Task<StateValue> Deliver(NativeStateValue* native)
    {
        var state = new CallState<StateValue>(default);
        var userData = state.Pin();
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, NativeStateValue*, void>)&Callbacks.OnState)(userData, 0, null, native);
        return state.Task;
    }

    [Fact]
    public async Task AProducerCancelledAsItIsHandedOutIsNotLeaked()
    {
        using var cts = new CancellationTokenSource();
        var state = new CallState<FelixProducerHandle>(cts.Token);
        var userData = state.Pin();
        cts.Cancel();
        // A failed status hands out no handle, so nothing native is freed here.
        unsafe { ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnProducer)(userData, (int)FelixStatus.Cancelled, null, 0); }
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => state.Task);
    }
}
