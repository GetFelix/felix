using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary><c>FelixHeader</c>; the pointers are owned by <see cref="RecordArgs"/>.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeHeader
{
    public byte* Name;
    public nuint NameLen;
    public byte* Value;
    public nuint ValueLen;
}

/// <summary><c>FelixRecord</c>; <c>size</c> is how the library steps through an array of them.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeRecord
{
    public nuint Size;
    public byte* Payload;
    public nuint PayloadLen;
    public byte* Key;
    public nuint KeyLen;
    public NativeHeader* Headers;
    public nuint HeaderCount;
}

/// <summary><c>FelixStreamRecord</c>, borrowed for the length of a callback.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeStreamRecord
{
    public ulong Offset;
    public byte* Payload;
    public nuint PayloadLen;
    public byte* Publisher;
    public nuint PublisherLen;
    public ulong TimestampMicros;
}

internal static unsafe partial class NativeMethods
{
    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_publish_with_headers(
        nint client,
        string tenantId,
        string ns,
        string stream,
        NativeRecord* record,
        int ack,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_publish_batch_with_headers(
        nint client,
        string tenantId,
        string ns,
        string stream,
        NativeRecord* records,
        nuint count,
        int ack,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeRecordResult*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_event_key(nint ev, byte** outData, nuint* outLen);

    [LibraryImport(Lib)]
    internal static partial int felix_event_header_count(nint ev, nuint* outCount);

    [LibraryImport(Lib)]
    internal static partial int felix_event_header(
        nint ev, nuint index, byte** outName, nuint* outNameLen, byte** outValue, nuint* outValueLen);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_put_if_lease(
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
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte, ulong, byte*, ulong, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_with_identity(
        nint client,
        string tenantId,
        string token,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_read(
        nint client,
        string tenantId,
        string ns,
        string stream,
        uint shard,
        ulong fromOffset,
        byte hasEnd,
        ulong end,
        uint maxRecords,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, NativeStreamRecord*, nuint, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_client_refresh_topology(
        nint client,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_client_connections_per_node(
        nint client,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte**, nuint*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_live_offset(nint subscription, ulong* outOffset, byte* outHas);
}
