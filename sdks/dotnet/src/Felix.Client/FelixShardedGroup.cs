using Felix.Native;

namespace Felix;

/// <summary>
/// A consumer group across every shard of a stream. Polling visits the shards in turn;
/// order holds within a shard and nowhere across shards. Settle each record on its own shard
/// with the methods here, which route by <see cref="GroupRecord.Shard"/>. Disposing it ends
/// a pending poll, which completes as cancelled.
/// </summary>
public sealed class FelixShardedGroup : IAsyncDisposable, IDisposable
{
    private readonly FelixClient client;
    private readonly FelixShardedGroupHandle handle;

    internal unsafe FelixShardedGroup(FelixClient client, FelixShardedGroupHandle handle, string tenant, string ns, string stream, string group)
    {
        this.client = client;
        this.handle = handle;
        Tenant = tenant;
        Namespace = ns;
        Stream = stream;
        Name = group;
        using var call = new HandleLease(handle);
        uint shards;
        var status = NativeMethods.felix_sharded_group_shards(call.Pointer, &shards);
        if (status != 0)
        {
            var message = NativeMethods.LastError();
            handle.Dispose();
            throw FelixException.FromStatus(status, message);
        }

        ShardCount = shards;
    }

    /// <summary>The tenant the group belongs to.</summary>
    public string Tenant { get; }

    /// <summary>The stream's namespace.</summary>
    public string Namespace { get; }

    /// <summary>The stream.</summary>
    public string Stream { get; }

    /// <summary>The group's name.</summary>
    public string Name { get; }

    /// <summary>How many shards the group spans.</summary>
    public uint ShardCount { get; }

    /// <summary>
    /// Takes up to <paramref name="maxRecords"/> (at least one) from the group's shards, visiting
    /// them in turn from where the last poll left off, and returns the first non-empty batch. Empty
    /// means no shard had anything. A shard that cannot be polled fails the whole poll.
    /// </summary>
    public unsafe Task<GroupRecord[]> PollAsync(uint maxRecords, CancellationToken cancellationToken = default) =>
        NativeCall.Start<GroupRecord[]>(handle, cancellationToken, (group, cancel, userData) =>
            NativeMethods.felix_sharded_group_poll(group, maxRecords, cancel, &Callbacks.OnShardedPoll, userData));

    /// <summary>The group's handle on one shard, for the per-shard calls.</summary>
    public FelixGroup OnShard(uint shard) => client.JoinGroup(Tenant, Namespace, Stream, shard, Name);

    /// <inheritdoc cref="FelixGroup.AckAsync(GroupRecord, CancellationToken)" />
    public Task AckAsync(GroupRecord record, CancellationToken cancellationToken = default) =>
        OnShard(record.Shard).AckAsync(record, cancellationToken);

    /// <inheritdoc cref="FelixGroup.NackAsync" />
    public Task NackAsync(GroupRecord record, TimeSpan delay = default, CancellationToken cancellationToken = default) =>
        OnShard(record.Shard).NackAsync(record, delay, cancellationToken);

    /// <inheritdoc cref="FelixGroup.ExtendAsync" />
    public Task<TimeSpan> ExtendAsync(GroupRecord record, TimeSpan extension, CancellationToken cancellationToken = default) =>
        OnShard(record.Shard).ExtendAsync(record, extension, cancellationToken);

    /// <inheritdoc cref="FelixGroup.DeadLetterAsync(GroupRecord, CancellationToken)" />
    public Task DeadLetterAsync(GroupRecord record, CancellationToken cancellationToken = default) =>
        OnShard(record.Shard).DeadLetterAsync(record, cancellationToken);

    /// <summary>Frees the group.</summary>
    public void Dispose() => handle.Dispose();

    /// <inheritdoc cref="Dispose" />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }
}
