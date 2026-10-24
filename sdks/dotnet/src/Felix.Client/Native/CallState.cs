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
    private int finished;

    protected CallStateBase(CancellationToken token)
    {
        Token = token;
        Cancel = new NativeCancel(token);
        Interlocked.Increment(ref live);
    }

    private static int live;

    /// <summary>Calls whose cancel token has not been released yet; a test hook for leaks.</summary>
    internal static int Live => Volatile.Read(ref live);

    internal CancellationToken Token { get; }

    internal NativeCancel Cancel { get; }

    /// <summary>The <c>user_data</c> to pass to the native call.</summary>
    internal nint Pin()
    {
        self = GCHandle.Alloc(this);
        return GCHandle.ToIntPtr(self);
    }

    /// <summary>The call was refused, so its callback will never run.</summary>
    internal void Abandon()
    {
        if (self.IsAllocated)
        {
            self.Free();
        }

        Finish();
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
        var handle = GCHandle.FromIntPtr(userData);
        var state = (T)handle.Target!;
        handle.Free();
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
    internal static CallState<T> Take(nint userData)
    {
        return Release<CallState<T>>(userData);
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
