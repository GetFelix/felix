using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary>Starts a native call on a handle and returns the task its callback completes.</summary>
internal static class NativeCall
{
    /// <summary>
    /// Runs <paramref name="begin"/> with the handle, the cancel token and the callback's
    /// <c>user_data</c>. The library's error message is per thread, so it is read right
    /// after the call returns, on this thread.
    /// </summary>
    internal static Task<T> Start<T>(SafeHandle handle, CancellationToken cancellationToken, Func<nint, nint, nint, int> begin)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var call = new HandleLease(handle);
        var state = new CallState<T>(cancellationToken);
        int status;
        string? error;
        try
        {
            var userData = state.Pin();
            status = begin(call.Pointer, state.Cancel.Pointer, userData);
            error = status != 0 ? NativeMethods.LastError() : null;
        }
        catch
        {
            state.Abandon();
            throw;
        }

        return status != 0 ? Refused<T>(state, status, error) : state.WaitAsync();
    }

    /// <summary>The call returned an error, so its callback never runs.</summary>
    internal static Task<T> Refused<T>(CallStateBase state, int status, string? message)
    {
        state.Abandon();
        return Task.FromException<T>(FelixException.FromStatus(status, message));
    }
}
