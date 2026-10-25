using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Felix.Native;

// The commit, conditional-write, state-read and producer callbacks. Each copies what it borrows and never throws.
internal static unsafe partial class Callbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnCommit(nint userData, int status, byte* message, ulong offset) =>
        Complete<long>(userData, status, message, () => unchecked((long)offset));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnConditional(nint userData, int status, byte* message, byte written, ulong offset) =>
        Complete(userData, status, message, () => new ConditionalWrite(written != 0, unchecked((long)offset)));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnState(nint userData, int status, byte* message, NativeStateValue* state)
    {
        var at = (nint)state;
        Complete<StateValue>(userData, status, message, () => CopyState((NativeStateValue*)at));
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnProducer(nint userData, int status, byte* message, nint producer)
    {
        var state = CallState<FelixProducerHandle>.Take(userData);
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
                TryFree(() => NativeMethods.felix_producer_free(producer));
                state.Fail(new OperationCanceledException(state.Token));
            }
            else if (status == 0)
            {
                state.Succeed(new FelixProducerHandle(producer));
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
                TryFree(() => NativeMethods.felix_producer_free(producer));
            }

            CallState<FelixProducerHandle>.TryFail(state, e);
        }
    }

    private static StateValue CopyState(NativeStateValue* state) =>
        new(
            CopyOptional(state->HasValue != 0, state->Value, state->ValueLen),
            state->HasVersion != 0 ? unchecked((long)state->Version) : null,
            state->HasAsOf != 0 ? unchecked((long)state->AsOf) : null);
}
