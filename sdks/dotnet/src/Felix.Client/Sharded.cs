namespace Felix;

/// <summary>What a sharded read delivered.</summary>
public enum ShardItemKind
{
    /// <summary>A record, in <see cref="ShardItem.Event"/>.</summary>
    Record = 1,
    /// <summary>The shard's owner stopped answering and the shard is being re-established; the others are unaffected.</summary>
    Lost = 2,
    /// <summary>The shard is delivering again, resuming after the last offset seen from it.</summary>
    Recovered = 3,
    /// <summary>The shard moved to another broker and is being followed there.</summary>
    Moved = 4,
    /// <summary>The stream was deleted. This shard's feed has ended, and every other shard ends the same way.</summary>
    StreamDeleted = 5,
}

/// <summary>One thing a sharded subscription reports: a record, or a change in a shard's feed.</summary>
/// <param name="Kind">What arrived.</param>
/// <param name="Shard">The shard it concerns.</param>
/// <param name="Event">The record; set only for <see cref="ShardItemKind.Record"/>.</param>
/// <param name="Message">The error text; set for <see cref="ShardItemKind.Lost"/>.</param>
public sealed record ShardItem(ShardItemKind Kind, uint Shard, FelixEvent? Event, string? Message);

/// <summary>Where a sharded subscription has reached on one shard.</summary>
/// <param name="Shard">The shard.</param>
/// <param name="Offset">The offset of the last record delivered from it.</param>
public readonly record struct ShardPosition(uint Shard, long Offset);

/// <summary>One record's outcome from <see cref="FelixClient.PublishRecordsAsync"/>.</summary>
/// <param name="Offset">The record's offset, when it was accepted and the broker reported one.</param>
/// <param name="Status"><see cref="FelixStatus.Ok"/> when the record was accepted.</param>
/// <param name="Message">Why it was not, when <paramref name="Status"/> is not ok.</param>
public sealed record PublishResult(long? Offset, FelixStatus Status, string? Message)
{
    /// <summary>Whether the record was accepted.</summary>
    public bool Accepted => Status == FelixStatus.Ok;
}
