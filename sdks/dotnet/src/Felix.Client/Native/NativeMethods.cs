using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary>One slot of <c>FelixRecordResult</c>.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeRecordResult
{
    public int Status;
    public byte HasOffset;
    public ulong Offset;
    public byte* Message;
}

/// <summary><c>FelixConnectOptions</c>: <c>size</c> says which fields the caller knows.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct NativeConnectOptions
{
    public nuint Size;
    public byte Publishers;
    public byte Timestamps;
}

/// <summary><c>FelixGroupRecord</c>, borrowed for the length of a callback.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeGroupRecord
{
    public ulong Offset;
    public uint Attempts;
    public ulong SkippedBefore;
    public byte* Payload;
    public nuint PayloadLen;
    public byte* Publisher;
    public nuint PublisherLen;
    public byte HasTimestamp;
    public ulong TimestampMicros;
}

/// <summary><c>FelixShardedGroupRecord</c>.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct NativeShardedGroupRecord
{
    public uint Shard;
    public NativeGroupRecord Record;
}

/// <summary><c>FelixGroupInfo</c>.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct NativeGroupInfo
{
    public byte HasCommitted;
    public ulong Committed;
    public ulong Tail;
    public ulong InFlight;
    public ulong Owed;
    public ulong DeadLetters;
}

/// <summary><c>FelixGroupPosition</c>.</summary>
[StructLayout(LayoutKind.Sequential)]
internal struct NativeGroupPosition
{
    public ulong Offset;
    public byte Moved;
}

/// <summary><c>FelixWatchItem</c>, borrowed for the length of a callback.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeWatchItem
{
    public int Kind;
    public byte HasShard;
    public uint Shard;
    public byte* Key;
    public nuint KeyLen;
    public byte HasValue;
    public byte* Value;
    public nuint ValueLen;
    public ulong Offset;
    public ulong ExpiresAtMillis;
    public byte HasResumeFrom;
    public ulong ResumeFrom;
}

/// <summary>The C ABI of <c>felix.h</c>. Callbacks are unmanaged function pointers.</summary>
internal static unsafe partial class NativeMethods
{
    private const string Lib = "felix";

    static NativeMethods() => NativeLoader.EnsureInstalled();

    [LibraryImport(Lib)]
    internal static partial nint felix_cancel_token_new();

    [LibraryImport(Lib)]
    internal static partial void felix_cancel_token_cancel(nint token);

    [LibraryImport(Lib)]
    internal static partial void felix_cancel_token_free(nint token);

    [LibraryImport(Lib)]
    internal static partial byte* felix_last_error_message();

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_connect(
        string addrs,
        string tenantId,
        string token,
        string? serverName,
        string? caFile,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial void felix_client_free(nint client);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_publish(
        nint client,
        string tenantId,
        string ns,
        string stream,
        byte* payload,
        nuint payloadLen,
        byte* key,
        nuint keyLen,
        int ack,
        int delivery,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_publish_batch(
        nint client,
        string tenantId,
        string ns,
        string stream,
        byte* data,
        nuint* lens,
        nuint count,
        byte* key,
        nuint keyLen,
        int ack,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeRecordResult*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_subscribe(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        int start,
        ulong startOffset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_next(
        nint subscription,
        long timeoutMs,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_close_reason(nint subscription, int* outReason);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_start_offset(nint subscription, ulong* outOffset, byte* outHas);

    [LibraryImport(Lib)]
    internal static partial void felix_subscription_free(nint subscription);

    [LibraryImport(Lib)]
    internal static partial int felix_event_payload(nint ev, byte** outData, nuint* outLen);

    [LibraryImport(Lib)]
    internal static partial int felix_event_offset(nint ev, ulong* outOffset, byte* outHas);

    [LibraryImport(Lib)]
    internal static partial int felix_event_skipped_before(nint ev, ulong* outSkipped);

    [LibraryImport(Lib)]
    internal static partial void felix_event_free(nint ev);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_connect_with(
        string addrs,
        string tenantId,
        string token,
        string? serverName,
        string? caFile,
        NativeConnectOptions* options,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_event_publisher(nint ev, byte** outData, nuint* outLen);

    [LibraryImport(Lib)]
    internal static partial int felix_event_timestamp(nint ev, ulong* outMicros, byte* outHas);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_poll(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        uint maxRecords,
        ulong waitMs,
        ulong visibilityMs,
        string? consumer,
        byte reclaim,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeGroupRecord*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_ack(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        ulong offset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_nack(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        ulong offset,
        uint attempts,
        ulong delayMs,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_dead_letter(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        ulong offset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_discard(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        ulong offset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_redrive(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        ulong offset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_extend(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        ulong offset,
        uint attempts,
        ulong extendMs,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_dead_letters(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, ulong*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_seek(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        int start,
        ulong startOffset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, ulong, byte, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_create(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        int start,
        ulong startOffset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, ulong, byte, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_describe(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeGroupInfo*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_delete(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        string group,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_stream_shards(
        nint client,
        string tenantId,
        string ns,
        string stream,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, uint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_sharded(
        nint client,
        string tenantId,
        string ns,
        string stream,
        string group,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_create_stream(
        nint client,
        string tenantId,
        string ns,
        string stream,
        string group,
        int start,
        ulong startOffset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeGroupPosition*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_seek_stream(
        nint client,
        string tenantId,
        string ns,
        string stream,
        string group,
        int start,
        ulong startOffset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeGroupPosition*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_describe_stream(
        nint client,
        string tenantId,
        string ns,
        string stream,
        string group,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeGroupInfo*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_group_delete_stream(
        nint client,
        string tenantId,
        string ns,
        string stream,
        string group,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_sharded_group_shards(nint group, uint* outShards);

    [LibraryImport(Lib)]
    internal static partial int felix_sharded_group_poll(
        nint group,
        uint maxRecords,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeShardedGroupRecord*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial void felix_sharded_group_free(nint group);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_put(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        byte* value,
        nuint len,
        ulong ttlMs,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_get(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_delete(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_get_versioned(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte*, nuint, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_put_if(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        byte* value,
        nuint len,
        ulong ttlMs,
        int kind,
        ulong version,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_delete_if(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        ulong version,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_counter_add(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        long delta,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, long, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_counter_get(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string key,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, long, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_watch_cache(
        nint client,
        string tenantId,
        string ns,
        string cache,
        int kind,
        string filter,
        byte retained,
        byte hasFromOffset,
        ulong fromOffset,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_watch_cache_sharded(
        nint client,
        string tenantId,
        string ns,
        string cache,
        string prefix,
        byte retained,
        uint* shards,
        ulong* offsets,
        nuint count,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_cache_watch_next(
        nint watch,
        long timeoutMs,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeWatchItem*, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_cache_watch_retained_count(nint watch, ulong* count, byte* has);

    [LibraryImport(Lib)]
    internal static partial int felix_cache_watch_resume_offsets(nint watch, uint* shards, ulong* offsets, nuint cap, nuint* count);

    [LibraryImport(Lib)]
    internal static partial void felix_cache_watch_free(nint watch);

    /// <summary>The library's message for the last failed call on this thread, copied.</summary>
    internal static string? LastError()
    {
        var ptr = felix_last_error_message();
        return ptr is null ? null : Marshal.PtrToStringUTF8((nint)ptr);
    }

    internal static string? Utf8(byte* ptr) => ptr is null ? null : Marshal.PtrToStringUTF8((nint)ptr);
}
