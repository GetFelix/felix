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

        return new FelixException(code, string.IsNullOrEmpty(message) ? $"felix call failed ({code})" : message);
    }
}
