namespace Felix;

/// <summary>How a publish is acknowledged.</summary>
public enum AckMode
{
    /// <summary>No acknowledgement and no offset.</summary>
    None = 0,
    /// <summary>The broker acknowledges each record.</summary>
    PerMessage = 1,
    /// <summary>The broker acknowledges the batch.</summary>
    PerBatch = 2,
}

/// <summary>What a publish does when its outcome is in doubt.</summary>
public enum Delivery
{
    /// <summary>A record whose outcome is unknown is not sent again.</summary>
    AtMostOnce = 0,
    /// <summary>A record whose outcome is unknown is sent again, which can write it twice.</summary>
    AtLeastOnce = 1,
}

/// <summary>Why a subscription ended, or that it has not.</summary>
public enum CloseReason
{
    /// <summary>The subscription is live.</summary>
    Open = 0,
    /// <summary>The broker ended the stream.</summary>
    Ended = 1,
    /// <summary>The stream was deleted.</summary>
    StreamDeleted = 2,
}

/// <summary>Where a subscription starts reading.</summary>
public readonly record struct StartPosition
{
    private StartPosition(int kind, ulong offset)
    {
        Kind = kind;
        Offset = offset;
    }

    /// <summary>Only records published after the subscribe.</summary>
    public static StartPosition Latest => new(0, 0);

    /// <summary>From the oldest retained record.</summary>
    public static StartPosition Earliest => new(1, 0);

    /// <summary>From a log offset.</summary>
    public static StartPosition FromOffset(ulong offset) => new(2, offset);

    internal int Kind { get; }

    internal ulong Offset { get; }
}

/// <summary>What <see cref="FelixClient.ConnectAsync"/> needs.</summary>
public sealed class FelixClientOptions
{
    /// <summary>Broker addresses as <c>host:port</c>; any reachable one is enough.</summary>
    public required IReadOnlyList<string> Addrs { get; init; }

    /// <summary>The tenant to authenticate as.</summary>
    public required string TenantId { get; init; }

    /// <summary>
    /// The tenant's token. Optional when <see cref="TokenProvider"/> is set: leave it null (or
    /// empty) and the first connect calls the provider. With a provider this is only the first
    /// token. Without one it is the only token, and the client cannot refresh it.
    /// </summary>
    public string? Token { get; init; }

    /// <summary>
    /// Returns a fresh Felix token, usually from a closure over the refresh token. The client
    /// calls it when a stream is about to authenticate and it holds no token, the held one is
    /// past two thirds of its life, or the broker just refused it; an idle client makes no
    /// calls. It is never called concurrently, and a failure (an exception, a null or empty
    /// result, or no answer within 30 seconds) keeps a still-valid token or fails the operation
    /// with a <see cref="FelixException"/> of status <see cref="FelixStatus.Auth"/> carrying the
    /// exception's message. The <see cref="CancellationToken"/> fires after 30 seconds and when
    /// the client is disposed. At least one of <see cref="Token"/> and this is required.
    /// </summary>
    public Func<CancellationToken, ValueTask<string>>? TokenProvider { get; init; }

    /// <summary>The name the broker certificate must carry; <c>localhost</c> when null.</summary>
    public string? ServerName { get; init; }

    /// <summary>A PEM file of CA certificates to trust; the system store when null.</summary>
    public string? CaFile { get; init; }

    /// <summary>
    /// Ask brokers to say who published each record, for <see cref="FelixEvent.Publisher"/>
    /// and <see cref="GroupRecord.Publisher"/>. Adds the principal to every event batch.
    /// A broker that does not record it leaves it null.
    /// </summary>
    public bool Publishers { get; init; }

    /// <summary>
    /// Ask brokers for each record's append time, for <see cref="FelixEvent.Timestamp"/>
    /// and <see cref="GroupRecord.Timestamp"/>. Adds eight bytes per event.
    /// </summary>
    public bool Timestamps { get; init; }
}

/// <summary>One record delivered to a subscription, copied out of the native event.</summary>
/// <param name="Payload">The record's bytes as published.</param>
/// <param name="Offset">The log offset, or null on a stream without a log.</param>
/// <param name="SkippedBefore">
/// How many offsets just below this one hold no event. A larger jump than this
/// explains is a drop.
/// </param>
/// <param name="Publisher">
/// The principal that published the record. Null unless the client was connected with
/// <see cref="FelixClientOptions.Publishers"/> and the broker recorded one.
/// </param>
/// <param name="TimestampMicros">
/// When the broker appended the record, in microseconds since the Unix epoch by the clock of
/// the broker that led the shard. Null unless the client was connected with
/// <see cref="FelixClientOptions.Timestamps"/> and the broker supplied one.
/// </param>
public sealed record FelixEvent(
    ReadOnlyMemory<byte> Payload,
    long? Offset,
    long SkippedBefore,
    string? Publisher = null,
    long? TimestampMicros = null)
{
    /// <summary><see cref="TimestampMicros"/> as a time; null when it is absent or out of range.</summary>
    public DateTimeOffset? Timestamp => Time.FromMicros(TimestampMicros);
}

internal static class Time
{
    internal static DateTimeOffset? FromMicros(long? micros)
    {
        // DateTimeOffset ends in year 9999; a broker clock that far out is not a time.
        if (micros is not { } value || value < 0 || value / 1_000_000 > 253_402_300_799)
        {
            return null;
        }

        return DateTimeOffset.UnixEpoch.AddTicks(value * 10);
    }
}
