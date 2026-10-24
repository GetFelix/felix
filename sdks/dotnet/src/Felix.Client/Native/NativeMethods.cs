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

    /// <summary>The library's message for the last failed call on this thread, copied.</summary>
    internal static string? LastError()
    {
        var ptr = felix_last_error_message();
        return ptr is null ? null : Marshal.PtrToStringUTF8((nint)ptr);
    }

    internal static string? Utf8(byte* ptr) => ptr is null ? null : Marshal.PtrToStringUTF8((nint)ptr);
}
