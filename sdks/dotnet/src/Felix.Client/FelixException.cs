namespace Felix;

/// <summary>A failure reported by the Felix client, typed by <see cref="Status"/>.</summary>
public sealed class FelixException : Exception
{
    /// <summary>Creates an exception for <paramref name="status"/>.</summary>
    public FelixException(FelixStatus status, string message)
        : base(message)
    {
        Status = status;
    }

    /// <summary>The class of the failure.</summary>
    public FelixStatus Status { get; }

    /// <summary>
    /// For <see cref="FelixStatus.Cursor"/>: where the reader can resume, and the head trim's
    /// headers when the floor is a trim's. Null for any other status.
    /// </summary>
    public CursorDetails? Cursor { get; private init; }

    /// <summary>
    /// Whether the same call can be tried again and nothing was applied: the
    /// broker was unreachable, the shard was moving, or the broker was shedding load.
    /// </summary>
    public bool IsRetryable =>
        Status is FelixStatus.Connection or FelixStatus.ShardUnavailable or FelixStatus.Overloaded;

    /// <summary>
    /// Whether a write may have been applied. Retry only if the write is idempotent.
    /// </summary>
    public bool OutcomeUnknown => Status == FelixStatus.OutcomeUnknown;

    /// <summary>
    /// The exception a call reports for a native status: cancellation becomes
    /// <see cref="OperationCanceledException"/>, everything else a <see cref="FelixException"/>.
    /// </summary>
    internal static Exception FromStatus(int status, string? message, CancellationToken token = default)
    {
        var code = (FelixStatus)status;
        if (code == FelixStatus.Cancelled)
        {
            return new OperationCanceledException(message ?? "the call was cancelled", token);
        }

        // Read here, where the status is seen: the details are the calling thread's, valid in the
        // native callback or right after the call that returned the status.
        var cursor = code == FelixStatus.Cursor ? Native.NativeMethods.LastCursor() : null;
        return string.IsNullOrEmpty(message)
            ? new FelixException(code, $"felix call failed ({code})") { Cursor = cursor }
            : new FelixException(code, message) { FromNative = true, Cursor = cursor };
    }

    /// <summary>Whether the message came from the native call rather than being made up here.</summary>
    internal bool FromNative { get; private init; }
}

/// <summary>Why a start offset was refused.</summary>
public enum CursorReason
{
    /// <summary>Retention or a head trim discarded the offset.</summary>
    TooOld,
    /// <summary>The offset is past the end of the stream.</summary>
    InFuture,
}

/// <summary>What a reader refused with <see cref="FelixStatus.Cursor"/> needs to recover.</summary>
/// <param name="Reason">Whether the offset was too old or not yet written.</param>
/// <param name="Requested">The offset that was asked for.</param>
/// <param name="Available">The nearest offset that would have worked: the oldest retained for
/// <see cref="CursorReason.TooOld"/>, the tail for <see cref="CursorReason.InFuture"/>.</param>
/// <param name="TrimHeaders">For <see cref="CursorReason.TooOld"/> below a head trim's floor, the headers the
/// trim was written with, so a reader can tell who trimmed past it. Null for a floor retention set.</param>
public sealed record CursorDetails(
    CursorReason Reason,
    long Requested,
    long Available,
    ReadOnlyMemory<byte>? TrimHeaders);
