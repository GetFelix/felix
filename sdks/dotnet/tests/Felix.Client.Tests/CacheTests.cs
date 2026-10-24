using System.Text;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

// Drives the real cache callbacks through their function pointers on fabricated call states,
// as the library does, without a native call behind them.
public class CacheMarshalTests
{
    private static unsafe void Value(nint userData, int status, byte found, byte[] value)
    {
        fixed (byte* data = value)
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte*, nuint, void>)&Callbacks.OnValue)(
                userData, status, null, found, value.Length == 0 ? null : data, (nuint)value.Length);
        }
    }

    [Fact]
    public async Task AFoundValueIsCopied()
    {
        var state = new CallState<ReadOnlyMemory<byte>?>(default);
        var buffer = "hello"u8.ToArray();
        Value(state.Pin(), 0, 1, buffer);
        Array.Clear(buffer);
        Assert.Equal("hello"u8.ToArray(), (await state.Task)!.Value.ToArray());
    }

    [Fact]
    public async Task AMissIsNullAndAnEmptyValueIsNot()
    {
        var miss = new CallState<ReadOnlyMemory<byte>?>(default);
        Value(miss.Pin(), 0, 0, []);
        Assert.Null(await miss.Task);

        var empty = new CallState<ReadOnlyMemory<byte>?>(default);
        Value(empty.Pin(), 0, 1, []);
        var value = await empty.Task;
        Assert.NotNull(value);
        Assert.True(value!.Value.IsEmpty);
    }

    [Fact]
    public async Task AFailedReadIsTyped()
    {
        var state = new CallState<ReadOnlyMemory<byte>?>(default);
        Value(state.Pin(), 23, 0, []);
        var error = await Assert.ThrowsAsync<FelixException>(() => state.Task);
        Assert.Equal(FelixStatus.NotFound, error.Status);
    }

    private static unsafe void Versioned(nint userData, byte found, byte[] value, ulong version)
    {
        fixed (byte* data = value)
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte*, nuint, ulong, void>)&Callbacks.OnVersioned)(
                userData, 0, null, found, data, (nuint)value.Length, version);
        }
    }

    private static unsafe void Condition(nint userData, byte applied, byte hasVersion, ulong version) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte, ulong, void>)&Callbacks.OnCondition)(
            userData, 0, null, applied, hasVersion, version);

    private static unsafe void Counter(nint userData, byte hasValue, long value) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, long, void>)&Callbacks.OnCounter)(userData, 0, null, hasValue, value);

    private static unsafe void WatchStatus(nint userData, int status) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, NativeWatchItem*, void>)&Callbacks.OnWatchItem)(userData, status, null, null);

    private static unsafe void WatchOpened(nint userData) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnWatchOpen)(userData, 0, null, 0);

    // Not async, so the pins hold while the callback copies; the callback completes the task before this returns.
    private static unsafe Task<WatchItem?> Deliver(NativeWatchItem item, byte[]? key = null, byte[]? value = null)
    {
        var state = new CallState<WatchItem?>(default);
        fixed (byte* k = key)
        fixed (byte* v = value)
        {
            item.Key = key is null ? null : k;
            item.KeyLen = (nuint)(key?.Length ?? 0);
            item.Value = value is null ? null : v;
            item.ValueLen = (nuint)(value?.Length ?? 0);
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, NativeWatchItem*, void>)&Callbacks.OnWatchItem)(state.Pin(), 0, null, &item);
        }

        return state.Task;
    }

    [Fact]
    public async Task AVersionedValueCarriesItsVersion()
    {
        var state = new CallState<VersionedValue?>(default);
        Versioned(state.Pin(), 1, "v"u8.ToArray(), 77);
        var got = (await state.Task)!;
        Assert.Equal(77, got.Version);
        Assert.Equal("v"u8.ToArray(), got.Value.ToArray());
    }

    [Fact]
    public async Task AVersionedMissIsNull()
    {
        var state = new CallState<VersionedValue?>(default);
        Versioned(state.Pin(), 0, [], 0);
        Assert.Null(await state.Task);
    }

    [Theory]
    [InlineData((byte)1, (byte)1, 5UL, true, 5L)]
    [InlineData((byte)0, (byte)1, 9UL, false, 9L)]
    [InlineData((byte)0, (byte)0, 9UL, false, null)]
    public async Task AConditionIsAnAnswerNotAnError(byte applied, byte hasVersion, ulong version, bool expectApplied, long? expectVersion)
    {
        var state = new CallState<ConditionalResult>(default);
        Condition(state.Pin(), applied, hasVersion, version);
        Assert.Equal(new ConditionalResult(expectApplied, expectVersion), await state.Task);
    }

    [Fact]
    public async Task ACounterThatDoesNotExistIsNullNotZero()
    {
        var missing = new CallState<long?>(default);
        Counter(missing.Pin(), 0, 0);
        Assert.Null(await missing.Task);

        var negative = new CallState<long?>(default);
        Counter(negative.Pin(), 1, -3);
        Assert.Equal(-3L, await negative.Task);
    }

    [Fact]
    public async Task AChangeIsCopiedWithItsShardKeyValueAndExpiry()
    {
        var value = "val"u8.ToArray();
        var got = (await Deliver(
            new NativeWatchItem { Kind = 1, HasShard = 1, Shard = 3, HasValue = 1, Offset = 12, ExpiresAtMillis = 99 },
            Encoding.UTF8.GetBytes("k\u00e9y"),
            value))!;
        Array.Clear(value);

        Assert.Equal(WatchItemKind.Change, got.Kind);
        Assert.Equal(3u, got.Shard);
        Assert.Equal("k\u00e9y", got.Key);
        Assert.Equal("val"u8.ToArray(), got.Value!.Value.ToArray());
        Assert.Equal(12, got.Offset);
        Assert.Equal(99, got.ExpiresAtMillis);
        Assert.Null(got.ResumeFrom);
        Assert.False(got.IsDelete);
    }

    [Fact]
    public async Task ADeleteHasNoValueAndAnEmptyValueIsNotADelete()
    {
        var deleted = (await Deliver(new NativeWatchItem { Kind = 1 }, "k"u8.ToArray()))!;
        Assert.True(deleted.IsDelete);
        Assert.Null(deleted.Shard);
        Assert.Null(deleted.ExpiresAtMillis);

        var empty = (await Deliver(new NativeWatchItem { Kind = 1, HasValue = 1 }, "k"u8.ToArray(), []))!;
        Assert.False(empty.IsDelete);
        Assert.True(empty.Value!.Value.IsEmpty);
    }

    [Fact]
    public async Task ALagCarriesWhereToResume()
    {
        var got = (await Deliver(new NativeWatchItem { Kind = 2, HasShard = 1, Shard = 1, HasResumeFrom = 1, ResumeFrom = 40 }))!;
        Assert.Equal(WatchItemKind.Lagged, got.Kind);
        Assert.Equal(40, got.ResumeFrom);
        Assert.Null(got.Key);
        Assert.False(got.IsDelete);
    }

    [Fact]
    public async Task AWatchReadTimeoutIsNullAndClosedIsTyped()
    {
        var timeout = new CallState<WatchItem?>(default);
        WatchStatus(timeout.Pin(), 1);
        Assert.Null(await timeout.Task);

        var closed = new CallState<WatchItem?>(default);
        WatchStatus(closed.Pin(), 2);
        var error = await Assert.ThrowsAsync<FelixException>(() => closed.Task);
        Assert.Equal(FelixStatus.Closed, error.Status);
    }

    [Fact]
    public async Task AWatchOpenedAfterCancelHandsOutNothing()
    {
        using var cts = new CancellationTokenSource();
        var state = new CallState<FelixCacheWatchHandle>(cts.Token);
        var userData = state.Pin();
        cts.Cancel();
        WatchOpened(userData);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => state.Task);
        state.Abandon();
    }

    [Fact]
    public void ExpiryReadsAsATimeOrNull()
    {
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_000), new WatchItem(WatchItemKind.Change, null, "k", null, 0, 1_000, null).ExpiresAt);
        Assert.Null(new WatchItem(WatchItemKind.Change, null, "k", null, 0, null, null).ExpiresAt);
        Assert.Null(new WatchItem(WatchItemKind.Change, null, "k", null, 0, long.MaxValue, null).ExpiresAt);
    }
}

public class CacheCallTests
{
    // A client whose handle is already closed, so no native library is involved.
    private static FelixClient DisposedClient()
    {
        var client = new FelixClient(new FelixClientHandle(0));
        client.Dispose();
        return client;
    }

    [Fact]
    public async Task CallsOnADisposedClientThrowAndLeakNothing()
    {
        var cache = DisposedClient().Cache("t", "n", "c");
        using var cts = new CancellationTokenSource();
        var before = CallStateBase.Live;

        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.PutAsync("k", new byte[] { 1 }, cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.GetAsync("k", cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.CounterAddAsync("k", 1, cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.WatchKeyAsync("k", cancellationToken: cts.Token));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => cache.WatchShardedAsync("p", resume: [new ShardOffset(0, 1)], cancellationToken: cts.Token));

        Assert.Equal(before, CallStateBase.Live);
    }

    [Fact]
    public async Task ArgumentsAreCheckedBeforeAnythingIsSent()
    {
        var cache = DisposedClient().Cache("t", "n", "c");
        await Assert.ThrowsAsync<ArgumentNullException>(() => cache.GetAsync(null!));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.PutAsync("k", default, TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.PutIfAbsentAsync("k", default, TimeSpan.FromSeconds(-1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => cache.WatchKeyAsync("k", fromOffset: -1));
        Assert.Throws<ArgumentNullException>(() => new FelixClient(new FelixClientHandle(0)).Cache(null!, "n", "c"));
    }
}
