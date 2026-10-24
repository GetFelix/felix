using System.Runtime.InteropServices;
using System.Text;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

// Drives the real callbacks through their function pointers, as the library does.
public class MarshalTests
{
    private static unsafe void Publish(nint userData, int status, string? message, byte has, ulong offset)
    {
        var pin = default(GCHandle);
        try
        {
            byte* text = null;
            if (message is not null)
            {
                text = (byte*)Utf8(message, out pin);
            }

            ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, ulong, void>)&Callbacks.OnPublish)(userData, status, text, has, offset);
        }
        finally
        {
            if (pin.IsAllocated)
            {
                pin.Free();
            }
        }
    }

    private static unsafe void Batch(nint userData, int status, (byte Has, ulong Offset)[] records)
    {
        var results = new NativeRecordResult[records.Length];
        for (var i = 0; i < records.Length; i++)
        {
            results[i] = new NativeRecordResult { HasOffset = records[i].Has, Offset = records[i].Offset };
        }

        fixed (NativeRecordResult* pointer = results)
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, NativeRecordResult*, nuint, void>)&Callbacks.OnBatch)(
                userData, status, null, records.Length == 0 ? null : pointer, (nuint)records.Length);
        }
    }

    private static unsafe void Event(nint userData, int status) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnEvent)(userData, status, null, 0);

    private static IntPtr Utf8(string text, out GCHandle pin)
    {
        pin = GCHandle.Alloc(Encoding.UTF8.GetBytes(text + "\0"), GCHandleType.Pinned);
        return pin.AddrOfPinnedObject();
    }

    [Fact]
    public async Task PublishOffsetIsDelivered()
    {
        var state = new CallState<long?>(default);
        var userData = state.Pin();
        Publish(userData, 0, null, 1, 42);
        Assert.Equal(42L, await state.Task);
    }

    [Fact]
    public async Task PublishWithoutOffsetIsNull()
    {
        var state = new CallState<long?>(default);
        var userData = state.Pin();
        Publish(userData, 0, null, 0, 7);
        Assert.Null(await state.Task);
    }

    [Fact]
    public async Task FailureCopiesTheMessageAndTypesTheStatus()
    {
        var state = new CallState<long?>(default);
        var userData = state.Pin();
        Publish(userData, 22, "not allowed", 0, 0);

        var error = await Assert.ThrowsAsync<FelixException>(() => state.Task);
        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.Equal("not allowed", error.Message);
    }

    [Fact]
    public async Task CancelledStatusCancelsTheTask()
    {
        var state = new CallState<long?>(default);
        var userData = state.Pin();
        Publish(userData, 3, null, 0, 0);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => state.Task);
    }

    [Fact]
    public async Task BatchResultsAreCopiedInOrder()
    {
        var state = new CallState<long?[]>(default);
        var userData = state.Pin();
        Batch(userData, 0, [(1, 10), (0, 0)]);
        Assert.Equal(new long?[] { 10, null }, await state.Task);
    }

    [Fact]
    public async Task BatchFailureReportsTheFailedRecordStatus()
    {
        var state = new CallState<long?[]>(default);
        var userData = state.Pin();
        Batch(userData, 25, []);
        var error = await Assert.ThrowsAsync<FelixException>(() => state.Task);
        Assert.True(error.IsRetryable);
    }

    [Fact]
    public async Task ReadTimeoutIsNullNotAnError()
    {
        var state = new CallState<FelixEvent?>(default);
        var userData = state.Pin();
        Event(userData, 1);
        Assert.Null(await state.Task);
    }

    [Fact]
    public async Task ClosedReadIsATypedFailure()
    {
        var state = new CallState<FelixEvent?>(default);
        var userData = state.Pin();
        Event(userData, 2);
        var error = await Assert.ThrowsAsync<FelixException>(() => state.Task);
        Assert.Equal(FelixStatus.Closed, error.Status);
    }

    [Fact]
    public void AbandonFreesTheHandleOnce()
    {
        var state = new CallState<long?>(default);
        state.Pin();
        state.Abandon();
        state.Abandon();
    }

    [Fact]
    public void StartPositionsCarryTheirKind()
    {
        Assert.Equal(0, StartPosition.Latest.Kind);
        Assert.Equal(1, StartPosition.Earliest.Kind);
        Assert.Equal(2, StartPosition.FromOffset(9).Kind);
        Assert.Equal(9UL, StartPosition.FromOffset(9).Offset);
    }

    [Fact]
    public void NativeLibraryIsSearchedNextToTheAssemblyThenUnderRuntimes()
    {
        var found = NativeLoader.Candidates(null, "/app").ToList();
        Assert.Equal(Path.Combine("/app", NativeLoader.FileName), found[0]);
        Assert.Contains(found, p => p.Contains("runtimes"));
        Assert.Equal(["/x/libfelix.so"], NativeLoader.Candidates("/x/libfelix.so", "/app").ToList());
    }
}
