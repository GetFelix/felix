using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Felix.Native;

internal static unsafe partial class Callbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnShardedSubscribe(nint userData, int status, byte* message, nint subscription)
    {
        var state = CallState<FelixShardedSubscriptionHandle>.Take(userData);
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
                TryFree(() => NativeMethods.felix_sharded_subscription_free(subscription));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixShardedSubscriptionHandle(subscription));
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
                TryFree(() => NativeMethods.felix_sharded_subscription_free(subscription));
            }

            CallState<FelixShardedSubscriptionHandle>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnShardItem(nint userData, int status, byte* message, int kind, uint shard, nint ev)
    {
        var state = CallState<ShardItem?>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        // CopyEvent frees the event whether or not it succeeds, and only a record item carries one.
        try
        {
            if (status == 0)
            {
                var text = NativeMethods.Utf8(message);
                var item = kind == (int)ShardItemKind.Record
                    ? new ShardItem(ShardItemKind.Record, shard, CopyEvent(ev), null)
                    : new ShardItem((ShardItemKind)kind, shard, null, text);
                state.Succeed(item);
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
            CallState<ShardItem?>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnEndpoints(nint userData, int status, byte* message, byte** addrs, nuint count)
    {
        var state = CallState<string[]>.Take(userData);
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

            var result = new string[(int)count];
            for (var i = 0; i < result.Length; i++)
            {
                result[i] = NativeMethods.Utf8(addrs[i]) ?? string.Empty;
            }

            state.Succeed(result);
        }
        catch (Exception e)
        {
            CallState<string[]>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnOffset(nint userData, int status, byte* message, byte hasOffset, ulong offset)
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

    /// <summary>The per-record outcomes of a publish that can partly succeed.</summary>
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnRecords(nint userData, int status, byte* message, NativeRecordResult* results, nuint count)
    {
        var state = CallState<PublishResult[]>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            // A partly failed publish still reports each record: status is the first failure's, and the
            // caller needs the rest to know what landed.
            if (status != 0 && (results == null || count == 0))
            {
                state.Fail(status, NativeMethods.Utf8(message));
                return;
            }

            state.Succeed(CopyRecordResults(results, count));
        }
        catch (Exception e)
        {
            CallState<PublishResult[]>.TryFail(state, e);
        }
    }

    /// <summary>Copies the per-record outcomes out of native memory.</summary>
    internal static PublishResult[] CopyRecordResults(NativeRecordResult* results, nuint count)
    {
        var outcomes = new PublishResult[(int)count];
        for (var i = 0; i < outcomes.Length; i++)
        {
            var record = results[i];
            outcomes[i] = new PublishResult(
                record.HasOffset != 0 ? unchecked((long)record.Offset) : null,
                (FelixStatus)record.Status,
                record.Status == 0 ? null : NativeMethods.Utf8(record.Message));
        }

        return outcomes;
    }
}
