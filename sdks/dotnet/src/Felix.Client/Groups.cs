namespace Felix;

/// <summary>One record a consumer group handed out, copied out of the native poll.</summary>
/// <param name="Shard">The shard it was claimed from; settle it there.</param>
/// <param name="Offset">The offset to settle.</param>
/// <param name="Attempts">
/// How many times it has been handed out, this one included. 1 is a first attempt; 0 means
/// the broker did not report it. Nack and extend name the delivery by this count.
/// </param>
/// <param name="SkippedBefore">
/// How many offsets directly below this one the broker settled without delivering. Zero from
/// a broker that does not report it.
/// </param>
/// <param name="Payload">The record's bytes as published.</param>
/// <param name="Publisher">The principal that published it; see <see cref="FelixClientOptions.Publishers"/>.</param>
/// <param name="TimestampMicros">When the broker appended it, in microseconds since the Unix epoch; see <see cref="FelixClientOptions.Timestamps"/>.</param>
public sealed record GroupRecord(
    uint Shard,
    long Offset,
    uint Attempts,
    long SkippedBefore,
    ReadOnlyMemory<byte> Payload,
    string? Publisher,
    long? TimestampMicros)
{
    /// <summary><see cref="TimestampMicros"/> as a time; null when it is absent or out of range.</summary>
    public DateTimeOffset? Timestamp => Time.FromMicros(TimestampMicros);
}

/// <summary>Where a group resumes on one shard.</summary>
/// <param name="Offset">The offset the group resumes from.</param>
/// <param name="Moved">
/// False when creating the group found it already on this shard and left it alone.
/// </param>
public readonly record struct GroupPosition(long Offset, bool Moved);

/// <summary>Where a group stands on one shard.</summary>
/// <param name="Committed">Everything below this is finished. Null for a group with no cursor on the shard, which starts at the beginning of the log.</param>
/// <param name="Tail">The shard's committed tail.</param>
/// <param name="InFlight">Records handed out and not yet settled. Leader memory: it restarts from zero when the leader changes.</param>
/// <param name="Owed">Records owed again after a nack or a lapsed claim. Leader memory.</param>
/// <param name="DeadLetters">Records the group gave up on.</param>
public readonly record struct GroupInfo(long? Committed, long Tail, long InFlight, long Owed, long DeadLetters);
