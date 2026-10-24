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
        try
        {
            if (status == 0)
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
                NativeMethods.felix_client_free(client);
            }

            state.Fail(e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnPublish(nint userData, int status, byte* message, byte hasOffset, ulong offset)
    {
        var state = CallState<long?>.Take(userData);
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
            state.Fail(e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnBatch(nint userData, int status, byte* message, NativeRecordResult* results, nuint count)
    {
        var state = CallState<long?[]>.Take(userData);
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
            state.Fail(e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnSubscribe(nint userData, int status, byte* message, nint subscription)
    {
        var state = CallState<FelixSubscriptionHandle>.Take(userData);
        try
        {
            if (status == 0)
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
                NativeMethods.felix_subscription_free(subscription);
            }

            state.Fail(e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnEvent(nint userData, int status, byte* message, nint ev)
    {
        var state = CallState<FelixEvent?>.Take(userData);
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
            state.Fail(e);
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
