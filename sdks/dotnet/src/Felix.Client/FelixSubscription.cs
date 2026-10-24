using System.Runtime.CompilerServices;
using Felix.Native;

namespace Felix;

/// <summary>
/// A live subscription to one shard of a stream. One read may be pending at a time.
/// Enumerating it reads until the broker ends the stream or the token is cancelled.
/// </summary>
public sealed class FelixSubscription : IAsyncEnumerable<FelixEvent>, IAsyncDisposable, IDisposable
{
    private readonly FelixSubscriptionHandle handle;
    private int reading;

    internal unsafe FelixSubscription(FelixSubscriptionHandle handle)
    {
        this.handle = handle;
        using var call = new HandleLease(handle);
        ulong offset;
        byte has;
        var status = NativeMethods.felix_subscription_start_offset(call.Pointer, &offset, &has);
        if (status != 0)
        {
            var message = NativeMethods.LastError();
            handle.Dispose();
            throw FelixException.FromStatus(status, message);
        }

        StartOffset = has != 0 ? unchecked((long)offset) : null;
    }

    /// <summary>
    /// Where the subscription began: no event below it is delivered. Null on a stream
    /// without a log. Count the drops before the first event as
    /// <c>offset - StartOffset - SkippedBefore</c>.
    /// </summary>
    public long? StartOffset { get; }

    /// <summary>Why the subscription ended, or <see cref="Felix.CloseReason.Open"/> while it is live.</summary>
    public CloseReason CloseReason
    {
        get
        {
            return Query();
        }
    }

    private unsafe CloseReason Query()
    {
        {
            using var call = new HandleLease(handle);
            int reason;
            var status = NativeMethods.felix_subscription_close_reason(call.Pointer, &reason);
            if (status != 0)
            {
                throw FelixException.FromStatus(status, NativeMethods.LastError());
            }

            return (CloseReason)reason;
        }
    }

    /// <summary>
    /// Reads the next event, or returns null if <paramref name="timeout"/> passes with
    /// none (the subscription stays usable, and a timed-out or cancelled read loses nothing).
    /// </summary>
    /// <param name="timeout">How long to wait; <see cref="Timeout.InfiniteTimeSpan"/> waits indefinitely.</param>
    /// <exception cref="FelixException">With <see cref="FelixStatus.Closed"/> once the broker has ended the stream; see <see cref="CloseReason"/>.</exception>
    /// <exception cref="InvalidOperationException">Another read is pending.</exception>
    public async Task<FelixEvent?> ReadAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        long timeoutMs;
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            timeoutMs = -1;
        }
        else if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
        else
        {
            timeoutMs = (long)Math.Ceiling(timeout.TotalMilliseconds);
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.CompareExchange(ref reading, 1, 0) != 0)
        {
            throw new InvalidOperationException("a read is already pending on this subscription");
        }

        try
        {
            return await StartRead(timeoutMs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref reading, 0);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<FelixEvent> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            FelixEvent? next;
            try
            {
                next = await ReadAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
            catch (FelixException e) when (e.Status == FelixStatus.Closed)
            {
                yield break;
            }

            if (next is not null)
            {
                yield return next;
            }
        }
    }

    /// <summary>Frees the subscription, which stops it. A pending read completes as cancelled.</summary>
    public void Dispose() => handle.Dispose();

    /// <inheritdoc cref="Dispose" />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    private unsafe Task<FelixEvent?> StartRead(long timeoutMs, CancellationToken cancellationToken)
    {
        var state = new CallState<FelixEvent?>(cancellationToken);
        var userData = state.Pin();
        int status;
        string? error;
        using (var call = new HandleLease(handle))
        {
            status = NativeMethods.felix_subscription_next(
                call.Pointer,
                timeoutMs,
                state.Cancel.Pointer,
                &Callbacks.OnEvent,
                userData);
            error = status != 0 ? NativeMethods.LastError() : null;
        }

        if (status != 0)
        {
            state.Abandon();
            return Task.FromException<FelixEvent?>(FelixException.FromStatus(status, error));
        }

        return state.WaitAsync();
    }
}
