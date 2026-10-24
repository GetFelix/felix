using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Felix.Native;

// The cache, counter and watch callbacks. Each copies what it borrows and never throws.
internal static unsafe partial class Callbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnValue(nint userData, int status, byte* message, byte found, byte* value, nuint len) =>
        Complete<ReadOnlyMemory<byte>?>(userData, status, message, () => CopyOptional(found != 0, value, len));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnVersioned(nint userData, int status, byte* message, byte found, byte* value, nuint len, ulong version) =>
        Complete<VersionedValue?>(userData, status, message, () => found != 0 ? new VersionedValue(CopyBytes(value, len), unchecked((long)version)) : null);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnCondition(nint userData, int status, byte* message, byte applied, byte hasVersion, ulong version) =>
        Complete(userData, status, message, () => new ConditionalResult(applied != 0, hasVersion != 0 ? unchecked((long)version) : null));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnCounter(nint userData, int status, byte* message, byte hasValue, long value) =>
        Complete<long?>(userData, status, message, () => hasValue != 0 ? value : null);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnWatchOpen(nint userData, int status, byte* message, nint watch)
    {
        var state = CallState<FelixCacheWatchHandle>.Take(userData);
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
                TryFree(() => NativeMethods.felix_cache_watch_free(watch));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixCacheWatchHandle(watch));
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
                TryFree(() => NativeMethods.felix_cache_watch_free(watch));
            }

            CallState<FelixCacheWatchHandle>.TryFail(state, e);
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnWatchItem(nint userData, int status, byte* message, NativeWatchItem* item)
    {
        var state = CallState<WatchItem?>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0)
            {
                state.Succeed(CopyWatchItem(item));
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
            CallState<WatchItem?>.TryFail(state, e);
        }
    }

    private static byte[] CopyBytes(byte* data, nuint length) =>
        length == 0 || data is null ? Array.Empty<byte>() : new ReadOnlySpan<byte>(data, checked((int)length)).ToArray();

    // Not `present ? bytes : null`: that has type byte[], and a null byte[] converts to an empty
    // (non-null) ReadOnlyMemory<byte>?, which would turn "absent" into "empty".
    private static ReadOnlyMemory<byte>? CopyOptional(bool present, byte* data, nuint length)
    {
        if (!present)
        {
            return null;
        }

        return CopyBytes(data, length);
    }

    internal static WatchItem CopyWatchItem(NativeWatchItem* item) =>
        new(
            (WatchItemKind)item->Kind,
            item->HasShard != 0 ? item->Shard : null,
            item->Key is null || item->KeyLen == 0 ? null : Encoding.UTF8.GetString(item->Key, checked((int)item->KeyLen)),
            CopyOptional(item->HasValue != 0, item->Value, item->ValueLen),
            unchecked((long)item->Offset),
            item->ExpiresAtMillis != 0 ? unchecked((long)item->ExpiresAtMillis) : null,
            item->HasResumeFrom != 0 ? unchecked((long)item->ResumeFrom) : null);
}
