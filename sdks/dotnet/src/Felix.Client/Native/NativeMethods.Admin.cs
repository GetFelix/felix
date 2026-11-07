using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary><c>FelixSubscriptionFilter</c>; the strings are owned by the caller for the call.</summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeSubscriptionFilter
{
    public nuint Size;
    public byte* TenantId;
    public byte* Ns;
    public byte* Stream;
    public byte* Principal;
    public uint Shard;
    public byte HasShard;
    public byte Dropping;
}

internal static unsafe partial class NativeMethods
{
    [LibraryImport(Lib)]
    internal static partial int felix_client_supports(
        nint client,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, uint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_inspect_shard(
        nint client,
        int kind,
        string tenantId,
        string ns,
        string name,
        uint shard,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_client_list_subscriptions(
        nint client,
        NativeSubscriptionFilter* filter,
        uint limit,
        byte* cursor,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_client_topology(
        nint client,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_stream_routing(
        nint client,
        string tenantId,
        string ns,
        string stream,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, uint, int, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_shard_owners(
        nint client,
        int kind,
        string tenantId,
        string ns,
        string name,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_client_cache_shards(
        nint client,
        string tenantId,
        string ns,
        string cache,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nuint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_moves(nint subscription, ulong* outMoves);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_reconnects(nint subscription, ulong* outReconnects);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_stream_deleted(nint subscription, byte* outDeleted);

    [LibraryImport(Lib)]
    internal static partial int felix_subscription_queue_capacity(nint subscription, uint* outCapacity, byte* outHas);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_producer_hand_off_batch(
        nint producer,
        string tenantId,
        string ns,
        string stream,
        byte* data,
        nuint* lens,
        nuint count,
        byte* key,
        nuint keyLen,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_producer_hand_off_batches(
        nint producer,
        string tenantId,
        string ns,
        string stream,
        byte* data,
        nuint* lens,
        nuint count,
        nuint* batchSizes,
        nuint batchCount,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial int felix_producer_hand_off_abandon(
        nint producer,
        string tenantId,
        string ns,
        string stream,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_handed_offset(
        nint handed,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, byte, ulong, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial int felix_handed_done(
        nint handed,
        nint cancel,
        delegate* unmanaged[Cdecl]<nint, int, byte*, void> callback,
        nint userData);

    [LibraryImport(Lib)]
    internal static partial void felix_handed_free(nint handed);
}
