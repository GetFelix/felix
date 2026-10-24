using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary>
/// The caller's side of one native call: the task the callback completes and
/// the cancel token the call holds. Its GCHandle is <c>user_data</c>, freed by
/// the callback, or by the caller if the call is refused and no callback runs.
/// </summary>
internal abstract class CallStateBase
{
    private GCHandle self;
    private int allocated;
    private int finished;

    protected CallStateBase(CancellationToken token)
    {
        Token = token;
        Cancel = new NativeCancel(token);
        Interlocked.Increment(ref live);
    }

    private static int live;
    private static int handlesFreed;

    /// <summary>GCHandles freed so far; a test hook for double frees.</summary>
    internal static int HandlesFreed => Volatile.Read(ref handlesFreed);

    /// <summary>Calls whose cancel token has not been released yet; a test hook for leaks.</summary>
    internal static int Live => Volatile.Read(ref live);

    internal CancellationToken Token { get; }

    internal NativeCancel Cancel { get; }

    /// <summary>The <c>user_data</c> to pass to the native call.</summary>
    internal nint Pin()
    {
        self = GCHandle.Alloc(this);
        Volatile.Write(ref allocated, 1);
        return GCHandle.ToIntPtr(self);
    }

    /// <summary>The call was refused, so its callback will never run.</summary>
    internal void Abandon()
    {
        FreeHandle();
        Finish();
    }

    /// <summary>
    /// Frees the GCHandle once, whoever gets here first: freeing a slot twice would free
    /// whatever call has been given it since, and that call's callback would find nothing.
    /// </summary>
    private void FreeHandle()
    {
        if (Interlocked.Exchange(ref allocated, 0) == 1)
        {
            self.Free();
            Interlocked.Increment(ref handlesFreed);
        }
    }

    /// <summary>Releases the cancel token and its registration, once.</summary>
    protected void Finish()
    {
        if (Interlocked.Exchange(ref finished, 1) == 0)
        {
            Cancel.Dispose();
            Interlocked.Decrement(ref live);
        }
    }

    /// <summary>Frees the handle: the callback is running, once.</summary>
    protected static T Release<T>(nint userData)
        where T : CallStateBase
    {
        var state = (T)GCHandle.FromIntPtr(userData).Target!;
        state.FreeHandle();
        return state;
    }
}

internal sealed class CallState<T> : CallStateBase
{
    private readonly TaskCompletionSource<T> source = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal CallState(CancellationToken token)
        : base(token)
    {
    }

    internal Task<T> Task => source.Task;

    /// <summary>Called by the callback, once: recovers the state and frees its handle.</summary>
    internal static CallState<T>? Take(nint userData)
    {
        // Never throws: it runs inside a native callback, where an exception ends the process.
        try
        {
            return Release<CallState<T>>(userData);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>Faults the task from a callback's catch block, without throwing.</summary>
    internal static void TryFail(CallState<T>? state, Exception error)
    {
        try
        {
            state?.Fail(error);
        }
        catch
        {
            // Nothing more can be done, and nothing may escape.
        }
    }

    internal void Succeed(T value) => source.TrySetResult(value);

    internal void Fail(Exception error) => source.TrySetException(error);

    internal void Fail(int status, string? message) =>
        source.TrySetException(FelixException.FromStatus(status, message, Token));

    /// <summary>Waits for the callback, then releases the cancel token.</summary>
    internal async Task<T> WaitAsync()
    {
        try
        {
            return await source.Task.ConfigureAwait(false);
        }
        finally
        {
            Finish();
        }
    }
}

/// <summary>A native cancel token wired to a <see cref="CancellationToken"/>.</summary>
internal sealed class NativeCancel : IDisposable
{
    private CancellationTokenRegistration registration;
    private int disposed;

    internal NativeCancel(CancellationToken token)
    {
        if (!token.CanBeCanceled)
        {
            return;
        }

        Pointer = NativeMethods.felix_cancel_token_new();
        var pointer = Pointer;
        registration = token.UnsafeRegister(static state => NativeMethods.felix_cancel_token_cancel((nint)state!), pointer);
    }

    /// <summary>Zero when the caller's token can never cancel.</summary>
    internal nint Pointer { get; }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        // Dispose waits for a cancel in progress, so the token is not freed under it.
        registration.Dispose();
        if (Pointer != 0)
        {
            NativeMethods.felix_cancel_token_free(Pointer);
        }
    }
}
