using Felix.Native;

namespace Felix;

public sealed partial class FelixClient
{
    /// <summary>
    /// A consumer group on one shard. Nothing is sent: the group is made, if it is not
    /// there, by <see cref="FelixGroup.CreateAsync"/>.
    /// </summary>
    /// <param name="consumer">The polling member's name, or null for an unnamed one.</param>
    public FelixGroup JoinGroup(string tenant, string ns, string stream, uint shard, string group, string? consumer = null) =>
        new(this, tenant, ns, stream, shard, group, consumer);

    /// <summary>
    /// How many shards a stream was placed with, so a caller can run one group per shard.
    /// Zero means the broker does not know the stream, which is not the same as one shard.
    /// </summary>
    public unsafe Task<uint> GetShardCountAsync(string tenant, string ns, string stream, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        return Call<uint>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_stream_shards(client, tenant, ns, stream, cancel, &Callbacks.OnShards, userData));
    }

    /// <summary>
    /// Opens a consumer group across every shard of a stream. The shard count is asked of the
    /// broker, so an unknown stream fails here. This creates no group; see <see cref="CreateStreamGroupAsync"/>.
    /// </summary>
    public async Task<FelixShardedGroup> OpenShardedGroupAsync(
        string tenant,
        string ns,
        string stream,
        string group,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(group);
        var native = await StartShardedGroup(tenant, ns, stream, group, cancellationToken).ConfigureAwait(false);
        return new FelixShardedGroup(this, native, tenant, ns, stream, group);
    }

    private unsafe Task<FelixShardedGroupHandle> StartShardedGroup(string tenant, string ns, string stream, string group, CancellationToken cancellationToken) =>
        Call<FelixShardedGroupHandle>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_group_sharded(client, tenant, ns, stream, group, cancel, &Callbacks.OnShardedGroup, userData));

    /// <summary>
    /// Creates a group on every shard of a stream at <paramref name="start"/>, leaving a shard
    /// where it already exists alone. Returns one position per shard, indexed by shard.
    /// </summary>
    /// <remarks>
    /// An offset start is refused for a stream with more than one shard, since an offset means
    /// something different on each. It is one request per shard, so a failure partway leaves the
    /// earlier shards created; the call is safe to repeat.
    /// </remarks>
    public unsafe Task<GroupPosition[]> CreateStreamGroupAsync(string tenant, string ns, string stream, string group, StartPosition start, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(group);
        return Call<GroupPosition[]>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_group_create_stream(
                client, tenant, ns, stream, group, start.Kind, start.Offset, cancel, &Callbacks.OnPositions, userData));
    }

    /// <summary>
    /// Moves a group's cursor on every shard of a stream. <see cref="StartPosition.Latest"/> is each
    /// shard's tail when its own request lands, not one cut across them. Safe to repeat.
    /// </summary>
    public unsafe Task<GroupPosition[]> SeekStreamGroupAsync(string tenant, string ns, string stream, string group, StartPosition start, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(group);
        return Call<GroupPosition[]>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_group_seek_stream(
                client, tenant, ns, stream, group, start.Kind, start.Offset, cancel, &Callbacks.OnPositions, userData));
    }

    /// <summary>Where a group stands on every shard of a stream, indexed by shard.</summary>
    public unsafe Task<GroupInfo[]> DescribeStreamGroupAsync(string tenant, string ns, string stream, string group, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(group);
        return Call<GroupInfo[]>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_group_describe_stream(
                client, tenant, ns, stream, group, cancel, &Callbacks.OnInfos, userData));
    }

    /// <summary>
    /// Deletes a group, with its cursor and dead letters, on every shard of a stream. Returns whether
    /// any shard had it. A failure partway leaves the earlier shards deleted; the call is safe to repeat.
    /// </summary>
    public unsafe Task<bool> DeleteStreamGroupAsync(string tenant, string ns, string stream, string group, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(group);
        return Call<bool>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_group_delete_stream(
                client, tenant, ns, stream, group, cancel, &Callbacks.OnDeleted, userData));
    }

    /// <summary>
    /// A cache of a tenant's namespace. Nothing is sent: the cache is made by the control plane, and
    /// a cache that does not exist fails its first call with <see cref="FelixStatus.NotFound"/>.
    /// </summary>
    public FelixCache Cache(string tenant, string ns, string cache) => new(this, tenant, ns, cache);

    internal Task<T> Call<T>(CancellationToken cancellationToken, Func<nint, nint, nint, int> begin) =>
        NativeCall.Start<T>(handle, cancellationToken, begin);
}
