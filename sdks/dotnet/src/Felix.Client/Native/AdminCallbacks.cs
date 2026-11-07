using System.Runtime.InteropServices;
using System.Runtime.CompilerServices;

namespace Felix.Native;

// The callbacks for the inspect and admin answers and for hand-offs. Each copies what it borrows and never throws.
internal static unsafe partial class Callbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnJson(nint userData, int status, byte* message, byte* json) =>
        Complete(userData, status, message, () => NativeMethods.Utf8(json) ?? string.Empty);

    // The page's JSON holds next_cursor, so the separate cursor string the C callback offers is not needed.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnSubscriptions(nint userData, int status, byte* message, byte* json, byte* nextCursor) =>
        Complete(userData, status, message, () => NativeMethods.Utf8(json) ?? string.Empty);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnSupports(nint userData, int status, byte* message, uint flags) =>
        Complete(userData, status, message, () => SupportsOf(flags));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnRouting(nint userData, int status, byte* message, uint shards, int routing) =>
        Complete(userData, status, message, () => new StreamRouting(shards, routing == 1 ? ShardRouting.JumpHash : ShardRouting.Modulo));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnHanded(nint userData, int status, byte* message, nint handed)
    {
        var state = CallState<FelixHandedHandle>.Take(userData);
        if (state is null)
        {
            Lost();
            return;
        }

        try
        {
            if (status == 0 && state.Token.IsCancellationRequested)
            {
                // The call finished as the caller cancelled: hand out nothing. The batch is sent regardless.
                TryFree(() => NativeMethods.felix_handed_free(handed));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixHandedHandle(handed));
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
                TryFree(() => NativeMethods.felix_handed_free(handed));
            }

            CallState<FelixHandedHandle>.TryFail(state, e);
        }
    }

    /// <summary>The <c>FELIX_SUPPORTS_*</c> bits as a record.</summary>
    internal static FelixSupports SupportsOf(uint flags) => new(
        Inspect: (flags & 1u << 0) != 0,
        Topology: (flags & 1u << 1) != 0,
        AckOnCommit: (flags & 1u << 2) != 0,
        StreamShards: (flags & 1u << 3) != 0,
        CacheShards: (flags & 1u << 4) != 0,
        ShardOwners: (flags & 1u << 5) != 0);
}
