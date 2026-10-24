using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary>
/// The native callbacks. Each runs on a library thread, copies every borrowed
/// buffer before returning, and never lets an exception escape: an unwinding
/// exception would end the process, so a failure faults the call's task instead.
/// </summary>
internal static unsafe class Callbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnConnect(nint userData, int status, byte* message, nint client)
    {
        var state = CallState<FelixClientHandle>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0 && state.Token.IsCancellationRequested)
            {
                // The call finished as the caller cancelled: hand out nothing.
                TryFree(() => NativeMethods.felix_client_free(client));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixClientHandle(client));
            }
            else
            {
                state.Fail(status, NativeMethods.Utf8(message));
            }
        }
        catch (Exception e)
        {
            if (status == 0)
            {
                TryFree(() => NativeMethods.felix_client_free(client));
            }

            CallState<FelixClientHandle>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnPublish(nint userData, int status, byte* message, byte hasOffset, ulong offset)
    {
        var state = CallState<long?>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0)
            {
                state.Succeed(hasOffset != 0 ? unchecked((long)offset) : null);
            }
            else
            {
                state.Fail(status, NativeMethods.Utf8(message));
            }
        }
        catch (Exception e)
        {
            CallState<long?>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnBatch(nint userData, int status, byte* message, NativeRecordResult* results, nuint count)
    {
        var state = CallState<long?[]>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status != 0)
            {
                state.Fail(status, NativeMethods.Utf8(message));
                return;
            }

            var offsets = new long?[(int)count];
            for (var i = 0; i < offsets.Length; i++)
            {
                var record = results[i];
                if (record.Status != 0)
                {
                    state.Fail(record.Status, NativeMethods.Utf8(record.Message));
                    return;
                }

                offsets[i] = record.HasOffset != 0 ? unchecked((long)record.Offset) : null;
            }

            state.Succeed(offsets);
        }
        catch (Exception e)
        {
            CallState<long?[]>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnSubscribe(nint userData, int status, byte* message, nint subscription)
    {
        var state = CallState<FelixSubscriptionHandle>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0 && state.Token.IsCancellationRequested)
            {
                // The call finished as the caller cancelled: hand out nothing.
                TryFree(() => NativeMethods.felix_subscription_free(subscription));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixSubscriptionHandle(subscription));
            }
            else
            {
                state.Fail(status, NativeMethods.Utf8(message));
            }
        }
        catch (Exception e)
        {
            if (status == 0)
            {
                TryFree(() => NativeMethods.felix_subscription_free(subscription));
            }

            CallState<FelixSubscriptionHandle>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnEvent(nint userData, int status, byte* message, nint ev)
    {
        var state = CallState<FelixEvent?>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0)
            {
                state.Succeed(CopyEvent(ev));
            }
            else if (status == (int)FelixStatus.Timeout)
            {
                state.Succeed(null);
            }
            else
            {
                state.Fail(status, NativeMethods.Utf8(message));
            }
        }
        catch (Exception e)
        {
            CallState<FelixEvent?>.TryFail(state, e);
        }
    }

    /// <summary>
    /// A callback with no pending call means the handle table is corrupt: completing some
    /// other call, or none, would hang or misreport. Ending the process is the honest option.
    /// </summary>
    private static void Lost() =>
        Environment.FailFast("felix: a native callback arrived for a call that is no longer pending; the SDK's call table is corrupt");

    private static void TryFree(Action free)
    {
        try
        {
            free();
        }
        catch
        {
            // A callback must not throw.
        }
    }

    /// <summary>Copies the event out of native memory and frees it.</summary>
    private static FelixEvent CopyEvent(nint ev)
    {
        using var handle = new FelixEventHandle(ev);
        byte* data;
        nuint length;
        Check(NativeMethods.felix_event_payload(ev, &data, &length));
        var payload = length == 0 ? Array.Empty<byte>() : new ReadOnlySpan<byte>(data, checked((int)length)).ToArray();

        ulong offset;
        byte hasOffset;
        Check(NativeMethods.felix_event_offset(ev, &offset, &hasOffset));

        ulong skipped;
        Check(NativeMethods.felix_event_skipped_before(ev, &skipped));

        return new FelixEvent(payload, hasOffset != 0 ? unchecked((long)offset) : null, unchecked((long)skipped));
    }

    private static void Check(int status)
    {
        if (status != 0)
        {
            throw FelixException.FromStatus(status, NativeMethods.LastError());
        }
    }
}
