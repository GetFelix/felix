using System.Text.Json;
using System.Text.Json.Serialization;

namespace Felix;

/// <summary>Whether a shard belongs to a stream or a cache.</summary>
public enum ShardKind
{
    /// <summary>A stream's shard.</summary>
    Stream = 0,
    /// <summary>A cache's shard.</summary>
    Cache = 1,
}

/// <summary>How a stream maps routing keys onto its shards.</summary>
public enum ShardRouting
{
    /// <summary><c>hash(key) % shards</c>. What a stream created before routing modes has.</summary>
    Modulo = 0,
    /// <summary>Jump consistent hash of the same key hash.</summary>
    JumpHash = 1,
}

/// <summary>Which questions the broker in use answers; see <see cref="FelixClient.GetSupportsAsync"/>.</summary>
/// <param name="Inspect"><see cref="FelixClient.InspectShardAsync"/> and <see cref="FelixClient.ListSubscriptionsAsync"/>.</param>
/// <param name="Topology"><see cref="FelixClient.GetTopologyAsync"/>.</param>
/// <param name="AckOnCommit">The broker honours <see cref="FelixClientOptions.AckOnCommit"/>.</param>
/// <param name="StreamShards"><see cref="FelixClient.GetStreamRoutingAsync"/>.</param>
/// <param name="CacheShards"><see cref="FelixClient.GetCacheShardsAsync"/>.</param>
/// <param name="ShardOwners"><see cref="FelixClient.GetShardOwnersAsync"/>.</param>
public sealed record FelixSupports(
    bool Inspect,
    bool Topology,
    bool AckOnCommit,
    bool StreamShards,
    bool CacheShards,
    bool ShardOwners);

/// <summary>A stream's shard count and how it maps routing keys onto them.</summary>
/// <param name="Shards">How many shards the stream has.</param>
/// <param name="Routing">The mapping from a key to its shard.</param>
public readonly record struct StreamRouting(uint Shards, ShardRouting Routing);

/// <summary>A broker a client may connect to.</summary>
/// <param name="NodeId">The broker's node id.</param>
/// <param name="Addr"><c>host:port</c>, as the broker advertises itself to clients.</param>
public sealed record BrokerEndpoint(string NodeId, string Addr);

/// <summary>Who owns one shard, as the answering broker's routing sees it. A snapshot that can be stale.</summary>
public sealed record ShardOwner
{
    /// <summary>The shard.</summary>
    public uint Shard { get; init; }

    /// <summary>The owning broker. Null when no broker can serve the shard now (see <see cref="Unavailable"/>), or the answering broker is not in a cluster.</summary>
    public string? NodeId { get; init; }

    /// <summary><c>host:port</c> the owner serves clients on; null when the cluster has not been told.</summary>
    public string? Addr { get; init; }

    /// <summary>The ownership epoch this describes; 0 from a broker not in a cluster.</summary>
    public long Generation { get; init; }

    /// <summary>Why no broker can serve the shard now, as the reason of a shard-unavailable error.</summary>
    public string? Unavailable { get; init; }
}

/// <summary>One broker's view of one shard. Never forwarded: ask each broker for its own.</summary>
public sealed record ShardInspection
{
    /// <summary>The broker answering; empty when it is not in a cluster.</summary>
    public string NodeId { get; init; } = string.Empty;

    /// <summary>How many shards the stream or cache has, as this broker sees it; 0 when it knows nothing of it.</summary>
    public uint Shards { get; init; }

    /// <summary><c>leader</c>, <c>follower</c> or <c>none</c>.</summary>
    public string Role { get; init; } = string.Empty;

    /// <summary><c>unassigned</c>, <c>opening</c>, <c>fencing</c>, <c>active</c>, <c>draining</c>, <c>closed</c> or <c>failed</c>.</summary>
    public string Phase { get; init; } = string.Empty;

    /// <summary>Whether this broker takes writes for the shard right now.</summary>
    public bool Serving { get; init; }

    /// <summary>Why not, when <see cref="Serving"/> is false.</summary>
    public string? Reason { get; init; }

    /// <summary>The reason in a sentence.</summary>
    public string? Detail { get; init; }

    /// <summary>The generation this broker holds the shard at.</summary>
    public long? Generation { get; init; }

    /// <summary>The assignment as this broker last received it.</summary>
    public InspectedAssignment? Assignment { get; init; }

    /// <summary>The promotion fence, while this broker waits for its replicas to take it.</summary>
    public InspectedFence? Fence { get; init; }

    /// <summary>This broker's lease; null when it has none.</summary>
    public InspectedLease? Lease { get; init; }

    /// <summary>One past the last record in this broker's copy of the log; null when the log is not open here.</summary>
    public long? Tail { get; init; }

    /// <summary>The leader's commit mark: everything below it is held by a majority.</summary>
    public long? Committed { get; init; }

    /// <summary>The newest generation this broker's log has accepted a leader at.</summary>
    public long? AcceptedGeneration { get; init; }

    /// <summary>The leader's view of every other replica; empty from a follower.</summary>
    public IReadOnlyList<InspectedReplica> Replicas
    {
        get => replicas ?? [];
        init => replicas = value;
    }

    private IReadOnlyList<InspectedReplica>? replicas;

}

/// <summary>A shard's assignment: who leads it, who holds copies, and where a move is taking it.</summary>
public sealed record InspectedAssignment
{
    /// <summary>The assignment's generation.</summary>
    public long Generation { get; init; }

    /// <summary>The leader's node id.</summary>
    public string Leader { get; init; } = string.Empty;

    /// <summary>Every replica other than the leader.</summary>
    public IReadOnlyList<string> Replicas
    {
        get => replicas ?? [];
        init => replicas = value;
    }

    private IReadOnlyList<string>? replicas;

    /// <summary>The leader has been told to stop serving so the shard can move.</summary>
    public bool Draining { get; init; }

    /// <summary>The broker a move in progress is handing the shard to.</summary>
    public string? Successor { get; init; }
}

/// <summary>A promoted leader's fence: which replicas have taken its generation in the latest attempt.</summary>
public sealed record InspectedFence
{
    /// <summary>Replicas that took the fence.</summary>
    public IReadOnlyList<string> Took
    {
        get => took ?? [];
        init => took = value;
    }

    private IReadOnlyList<string>? took;

    /// <summary>Replicas still to answer.</summary>
    public IReadOnlyList<string> Pending
    {
        get => pending ?? [];
        init => pending = value;
    }

    private IReadOnlyList<string>? pending;

    /// <summary>Attempts in a row at this generation that left the shard closed.</summary>
    public uint Attempts { get; init; }

    /// <summary>When the next attempt starts, in milliseconds.</summary>
    public long? RetryInMs { get; init; }
}

/// <summary>The broker's lease, which a leader needs to serve writes.</summary>
/// <param name="Held">Whether the lease is held.</param>
/// <param name="RemainingMs">Milliseconds left on it.</param>
public sealed record InspectedLease(bool Held, long RemainingMs);

/// <summary>One replica as the leader sees it.</summary>
public sealed record InspectedReplica
{
    /// <summary>The replica's node id.</summary>
    public string NodeId { get; init; } = string.Empty;

    /// <summary><c>follower</c>, or <c>learner</c> for a move's destination still copying.</summary>
    public string Role { get; init; } = string.Empty;

    /// <summary>The next offset the leader will ship to it.</summary>
    public long? NextOffset { get; init; }

    /// <summary>Records it is behind the leader's tail.</summary>
    public long? Lag { get; init; }

    /// <summary>Whether it took the fence, while the leader is fencing.</summary>
    public bool? Fence { get; init; }

    /// <summary><c>shipping</c>, <c>stalled</c>, <c>copying</c>, <c>rebuilding</c>, <c>halted</c> or <c>fencing</c>.</summary>
    public string State { get; init; } = string.Empty;

    /// <summary>Why replication to it stopped, when <see cref="State"/> is <c>halted</c>.</summary>
    public string? Halted { get; init; }
}

/// <summary>Which subscriptions <see cref="FelixClient.ListSubscriptionsAsync"/> returns. Every property set narrows the list.</summary>
public sealed record SubscriptionFilter
{
    /// <summary>Only this tenant's subscriptions.</summary>
    public string? TenantId { get; init; }

    /// <summary>Only this namespace.</summary>
    public string? Namespace { get; init; }

    /// <summary>Only this stream.</summary>
    public string? Stream { get; init; }

    /// <summary>Only this shard.</summary>
    public uint? Shard { get; init; }

    /// <summary>Only subscriptions made under this principal (the token's subject).</summary>
    public string? Principal { get; init; }

    /// <summary>Only subscriptions whose queue has dropped records.</summary>
    public bool Dropping { get; init; }
}

/// <summary>Where a page of subscriptions ended; pass it back to read the next page.</summary>
/// <param name="TenantId">The tenant of the last subscription listed.</param>
/// <param name="Namespace">Its namespace.</param>
/// <param name="Stream">Its stream.</param>
/// <param name="Shard">Its shard.</param>
/// <param name="SubscriberId">Its subscriber id on that shard.</param>
public sealed record SubscriptionCursor(string TenantId, string Namespace, string Stream, uint Shard, long SubscriberId);

/// <summary>One subscription as the broker serving it sees it.</summary>
public sealed record InspectedSubscription
{
    /// <summary>The subscription's tenant.</summary>
    public string TenantId { get; init; } = string.Empty;

    /// <summary>Its namespace.</summary>
    public string Namespace { get; init; } = string.Empty;

    /// <summary>Its stream.</summary>
    public string Stream { get; init; } = string.Empty;

    /// <summary>Its shard.</summary>
    public uint Shard { get; init; }

    /// <summary>The shard's own id for the subscriber.</summary>
    public long SubscriberId { get; init; }

    /// <summary>The id the client sees on its events.</summary>
    public long? SubscriptionId { get; init; }

    /// <summary>The broker's connection id; null for a subscriber inside the broker or one still being set up.</summary>
    public long? Connection { get; init; }

    /// <summary>The connection's remote address.</summary>
    public string? Peer { get; init; }

    /// <summary>The principal the connection authenticated as.</summary>
    public string? Principal { get; init; }

    /// <summary>What a publish does when the queue is full: <c>block</c>, <c>drop_new</c> or <c>drop_old</c>.</summary>
    public string Policy { get; init; } = string.Empty;

    /// <summary>Batches waiting in the queue.</summary>
    public long Depth { get; init; }

    /// <summary>Batches the queue holds at most.</summary>
    public long Capacity { get; init; }

    /// <summary>Records the queue has dropped since the subscription started.</summary>
    public long Dropped { get; init; }

    /// <summary>One past the last offset taken from the queue for delivery; null on a stream without offsets and until the first live batch.</summary>
    public long? Position { get; init; }

    /// <summary>The shard's next offset.</summary>
    public long Tail { get; init; }

    /// <summary>How long ago it subscribed, in milliseconds.</summary>
    public long AgeMs { get; init; }
}

/// <summary>One page of <see cref="FelixClient.ListSubscriptionsAsync"/>.</summary>
public sealed record SubscriptionsPage
{
    /// <summary>The broker that answered; empty when it is not in a cluster.</summary>
    public string NodeId { get; init; } = string.Empty;

    /// <summary>The subscriptions on this page.</summary>
    public IReadOnlyList<InspectedSubscription> Subscriptions
    {
        get => subscriptions ?? [];
        init => subscriptions = value;
    }

    private IReadOnlyList<InspectedSubscription>? subscriptions;

    /// <summary>Pass this back for the next page; null on the last one.</summary>
    public SubscriptionCursor? NextCursor { get; init; }
}

/// <summary>The wire's JSON, snake_case, parsed without reflection.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(ShardInspection))]
[JsonSerializable(typeof(SubscriptionsPage))]
[JsonSerializable(typeof(SubscriptionCursor))]
[JsonSerializable(typeof(BrokerEndpoint[]))]
[JsonSerializable(typeof(ShardOwner[]))]
internal sealed partial class WireJson : JsonSerializerContext
{
    internal static T Read<T>(string json, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> info) =>
        JsonSerializer.Deserialize(json, info) ?? throw new FelixException(FelixStatus.Error, "the broker's answer was empty");
}
