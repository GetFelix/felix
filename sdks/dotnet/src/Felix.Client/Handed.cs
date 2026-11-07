using Felix.Native;

namespace Felix;

/// <summary>
/// A producer call the producer's task has been handed and not yet answered, from one of the
/// <see cref="FelixProducer"/> <c>HandOff*</c> methods. Its answer is read once, by <see cref="OffsetAsync"/>
/// or <see cref="DoneAsync"/>. Disposing it does not stop the call: the batch is sent whether or not the
/// answer is ever read.
/// </summary>
public sealed class Handed : IDisposable
{
    private readonly FelixHandedHandle handle;
    private int read;

    internal Handed(FelixHandedHandle handle)
    {
        this.handle = handle;
    }

    /// <summary>
    /// Waits for the leader's answer to <see cref="FelixProducer.HandOffBatchAsync"/>: the log offset of the
    /// batch's first record, or null when the broker reported none.
    /// </summary>
    /// <exception cref="InvalidOperationException">The answer was already read.</exception>
    /// <remarks>
    /// A cancel while waiting fails with <see cref="FelixStatus.OutcomeUnknown"/>, since the batch is sent
    /// regardless and its answer is lost with the wait. Any other failure leaves the batch in doubt as
    /// <see cref="FelixProducer"/> describes.
    /// </remarks>
    public unsafe Task<long?> OffsetAsync(CancellationToken cancellationToken = default)
    {
        Take();
        return NativeCall.Start<long?>(handle, cancellationToken, (handed, cancel, userData) =>
            NativeMethods.felix_handed_offset(handed, cancel, &Callbacks.OnPublish, userData));
    }

    /// <summary>
    /// Waits for the leader's answer to <see cref="FelixProducer.HandOffBatchesAsync"/> or
    /// <see cref="FelixProducer.HandOffAbandonAsync"/>, which is whether the call succeeded. The cancel and
    /// failure rules are those of <see cref="OffsetAsync"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The answer was already read.</exception>
    public unsafe Task DoneAsync(CancellationToken cancellationToken = default)
    {
        Take();
        return NativeCall.Start<bool>(handle, cancellationToken, (handed, cancel, userData) =>
            NativeMethods.felix_handed_done(handed, cancel, &Callbacks.OnDone, userData));
    }

    /// <summary>
    /// Frees the handle. A wait still pending completes as <see cref="FelixStatus.OutcomeUnknown"/>;
    /// the batch is sent either way.
    /// </summary>
    public void Dispose() => handle.Dispose();

    private void Take()
    {
        ObjectDisposedException.ThrowIf(handle.IsClosed, this);
        if (Interlocked.Exchange(ref read, 1) != 0)
        {
            throw new InvalidOperationException("this hand-off's answer was already read");
        }
    }
}
