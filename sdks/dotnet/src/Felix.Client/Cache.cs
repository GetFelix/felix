namespace Felix;

/// <summary>A cache entry read with the version a conditional write can name.</summary>
/// <param name="Value">A copy of the value.</param>
/// <param name="Version">The entry's version; pass it to <see cref="FelixCache.PutIfVersionAsync"/> or <see cref="FelixCache.DeleteIfVersionAsync"/>.</param>
public sealed record VersionedValue(ReadOnlyMemory<byte> Value, long Version);

/// <summary>The answer to a conditional cache write. A condition that did not hold is an answer, not an error.</summary>
/// <param name="Applied">Whether the write happened.</param>
/// <param name="Version">The key's version if the broker reported it: the new one when applied, the current one when not.</param>
public readonly record struct ConditionalResult(bool Applied, long? Version);

/// <summary>A shard and the cache-log offset a watch resumes it from.</summary>
public readonly record struct ShardOffset(uint Shard, long Offset);

/// <summary>What a <see cref="WatchItem"/> reports.</summary>
public enum WatchItemKind
{
    /// <summary>A key took a new value or was deleted.</summary>
    Change = 1,
    /// <summary>
    /// The watch fell behind and the broker ended it. <see cref="WatchItem.ResumeFrom"/> is where
    /// a new watch picks up without a gap. A single watch then ends; on a sharded watch only
    /// <see cref="WatchItem.Shard"/> has ended and the others carry on.
    /// </summary>
    Lagged = 2,
    /// <summary>The shard moved to another broker. The watch follows it by itself.</summary>
    ShardMoved = 3,
    /// <summary>Every shard's retained state has been delivered; live changes follow. Sharded retained watches only.</summary>
    StateComplete = 4,
    /// <summary>One shard's feed has ended and delivers no more. Other shards go on.</summary>
    ShardClosed = 5,
}

/// <summary>One item of a cache watch, copied out of the native read.</summary>
/// <param name="Kind">What the item reports; the other fields are set as each kind describes.</param>
/// <param name="Shard">The shard the item came from. Null on a single watch and for <see cref="WatchItemKind.StateComplete"/>.</param>
/// <param name="Key">The changed key, for <see cref="WatchItemKind.Change"/>.</param>
/// <param name="Value">The new value; null when the change deleted the key, which is not the same as an empty value.</param>
/// <param name="Offset">
/// The change's offset in the cache's log. A filtered watch's offsets are sparse, since other keys'
/// changes consume them, so a gap is not a drop: <see cref="WatchItemKind.Lagged"/> is.
/// </param>
/// <param name="ExpiresAtMillis">Unix milliseconds the value expires at; null for a value that never does.</param>
/// <param name="ResumeFrom">An offset to resume from, on <see cref="WatchItemKind.Lagged"/> and sometimes <see cref="WatchItemKind.ShardMoved"/>.</param>
public sealed record WatchItem(
    WatchItemKind Kind,
    uint? Shard,
    string? Key,
    ReadOnlyMemory<byte>? Value,
    long Offset,
    long? ExpiresAtMillis,
    long? ResumeFrom)
{
    /// <summary>Whether this is a change that removed its key.</summary>
    public bool IsDelete => Kind == WatchItemKind.Change && Value is null;

    /// <summary><see cref="ExpiresAtMillis"/> as a time; null for a value that never expires or an out-of-range time.</summary>
    public DateTimeOffset? ExpiresAt =>
        ExpiresAtMillis is { } ms && ms >= 0 && ms <= 253_402_300_799_999 ? DateTimeOffset.FromUnixTimeMilliseconds(ms) : null;
}
