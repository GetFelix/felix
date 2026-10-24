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

    /// <summary>The tenant's token.</summary>
    public required string Token { get; init; }

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
