using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Felix.Native;

// The consumer-group callbacks. Each copies what it borrows and never throws.
internal static unsafe partial class Callbacks
{
    /// <summary>Completes a call whose callback carries a value that is copied on success.</summary>
    private static void Complete<T>(nint userData, int status, byte* message, Func<T> read)
    {
        var state = CallState<T>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0)
            {
                state.Succeed(read());
            }
            else
            {
                state.Fail(status, NativeMethods.Utf8(message));
            }
        }
        catch (Exception e)
        {
            CallState<T>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnDone(nint userData, int status, byte* message) =>
        Complete(userData, status, message, static () => true);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnGroupPoll(nint userData, int status, byte* message, NativeGroupRecord* records, nuint count) =>
        Complete(userData, status, message, () => CopyRecords(records, count));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnShardedPoll(nint userData, int status, byte* message, NativeShardedGroupRecord* records, nuint count) =>
        Complete(userData, status, message, () => CopyShardedRecords(records, count));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnExtend(nint userData, int status, byte* message, ulong visibleMs) =>
        Complete(userData, status, message, () => TimeSpan.FromMilliseconds(visibleMs));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnOffsets(nint userData, int status, byte* message, ulong* offsets, nuint count) =>
        Complete(userData, status, message, () => CopyOffsets(offsets, count));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnPosition(nint userData, int status, byte* message, ulong offset, byte moved) =>
        Complete(userData, status, message, () => new GroupPosition(unchecked((long)offset), moved != 0));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnInfo(nint userData, int status, byte* message, NativeGroupInfo* info) =>
        Complete(userData, status, message, () => ToInfo(info));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnDeleted(nint userData, int status, byte* message, byte existed) =>
        Complete(userData, status, message, () => existed != 0);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnShards(nint userData, int status, byte* message, uint shards) =>
        Complete(userData, status, message, () => shards);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnPositions(nint userData, int status, byte* message, NativeGroupPosition* positions, nuint count) =>
        Complete(userData, status, message, () => CopyPositions(positions, count));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnInfos(nint userData, int status, byte* message, NativeGroupInfo* infos, nuint count) =>
        Complete(userData, status, message, () => CopyInfos(infos, count));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnShardedGroup(nint userData, int status, byte* message, nint group)
    {
        var state = CallState<FelixShardedGroupHandle>.Take(userData);
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
                TryFree(() => NativeMethods.felix_sharded_group_free(group));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixShardedGroupHandle(group));
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
                TryFree(() => NativeMethods.felix_sharded_group_free(group));
            }

            CallState<FelixShardedGroupHandle>.TryFail(state, e);
        }
    }

    internal static GroupRecord CopyRecord(NativeGroupRecord* record, uint shard = 0)
    {
        var payload = record->PayloadLen == 0 || record->Payload is null
            ? Array.Empty<byte>()
            : new ReadOnlySpan<byte>(record->Payload, checked((int)record->PayloadLen)).ToArray();
        var publisher = record->Publisher is null || record->PublisherLen == 0
            ? null
            : Encoding.UTF8.GetString(record->Publisher, checked((int)record->PublisherLen));
        return new GroupRecord(
            shard,
            unchecked((long)record->Offset),
            record->Attempts,
            unchecked((long)record->SkippedBefore),
            payload,
            publisher,
            record->HasTimestamp != 0 ? unchecked((long)record->TimestampMicros) : null);
    }

    private static GroupRecord[] CopyRecords(NativeGroupRecord* records, nuint count)
    {
        var copied = new GroupRecord[checked((int)count)];
        for (var i = 0; i < copied.Length; i++)
        {
            copied[i] = CopyRecord(&records[i]);
        }

        return copied;
    }

    private static GroupRecord[] CopyShardedRecords(NativeShardedGroupRecord* records, nuint count)
    {
        var copied = new GroupRecord[checked((int)count)];
        for (var i = 0; i < copied.Length; i++)
        {
            copied[i] = CopyRecord(&records[i].Record, records[i].Shard);
        }

        return copied;
    }

    private static long[] CopyOffsets(ulong* offsets, nuint count)
    {
        var copied = new long[checked((int)count)];
        for (var i = 0; i < copied.Length; i++)
        {
            copied[i] = unchecked((long)offsets[i]);
        }

        return copied;
    }

    private static GroupPosition[] CopyPositions(NativeGroupPosition* positions, nuint count)
    {
        var copied = new GroupPosition[checked((int)count)];
        for (var i = 0; i < copied.Length; i++)
        {
            copied[i] = new GroupPosition(unchecked((long)positions[i].Offset), positions[i].Moved != 0);
        }

        return copied;
    }

    private static GroupInfo[] CopyInfos(NativeGroupInfo* infos, nuint count)
    {
        var copied = new GroupInfo[checked((int)count)];
        for (var i = 0; i < copied.Length; i++)
        {
            copied[i] = ToInfo(&infos[i]);
        }

        return copied;
    }

    internal static GroupInfo ToInfo(NativeGroupInfo* info) =>
        new(
            info->HasCommitted != 0 ? unchecked((long)info->Committed) : null,
            unchecked((long)info->Tail),
            unchecked((long)info->InFlight),
            unchecked((long)info->Owed),
            unchecked((long)info->DeadLetters));
}
