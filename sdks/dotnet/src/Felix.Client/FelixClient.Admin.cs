using System.Runtime.InteropServices;
using System.Text.Json;
using Felix.Native;

namespace Felix;

public sealed partial class FelixClient
{
    /// <summary>
    /// Which questions the broker in use answers. A call the broker does not support fails without being
    /// sent, so check here to tell that apart from other failures. Older brokers answer fewer.
    /// </summary>
    public unsafe Task<FelixSupports> GetSupportsAsync(CancellationToken cancellationToken = default) =>
        Call<FelixSupports>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_supports(client, cancel, &Callbacks.OnSupports, userData));

    /// <summary>
    /// The broker's own view of one shard: its phase and generation, the fence, its lease and tail, and,
    /// where it leads, every replica's position. It is the view of the broker the client is connected to,
    /// not forwarded: connect to the broker you want to ask.
    /// </summary>
    /// <param name="kind">Whether <paramref name="name"/> is a stream or a cache.</param>
    /// <param name="name">The stream or cache.</param>
    /// <remarks>
    /// Needs the <c>node.view:cluster:*</c> permission, which a tenant's token usually lacks (the call then
    /// fails with <see cref="FelixStatus.Auth"/>), and a broker that advertises inspection
    /// (<see cref="FelixSupports.Inspect"/>).
    /// </remarks>
    public unsafe Task<ShardInspection> InspectShardAsync(
        ShardKind kind,
        string tenant,
        string ns,
        string name,
        uint shard,
        CancellationToken cancellationToken = default)
    {
        CheckKind(kind);
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(name);
        return Parse(
            Call<string>(cancellationToken, (client, cancel, userData) =>
                NativeMethods.felix_client_inspect_shard(client, (int)kind, tenant, ns, name, shard, cancel, &Callbacks.OnJson, userData)),
            WireJson.Default.ShardInspection);
    }

    /// <summary>
    /// One page of the subscriptions the connected broker serves: each one's queue depth and capacity,
    /// overflow policy, records dropped, position against the shard's tail, connection and principal.
    /// Not forwarded: ask each broker for its own. Page by passing <see cref="SubscriptionsPage.NextCursor"/> back.
    /// </summary>
    /// <param name="filter">Narrows the list; null lists every subscription.</param>
    /// <param name="limit">The most to return. Null takes the broker's default, 100; the broker caps a page at 1000.</param>
    /// <param name="cursor">Null for the first page.</param>
    /// <remarks>Needs <c>node.view:cluster:*</c> and a broker that advertises inspection.</remarks>
    public unsafe Task<SubscriptionsPage> ListSubscriptionsAsync(
        SubscriptionFilter? filter = null,
        uint? limit = null,
        SubscriptionCursor? cursor = null,
        CancellationToken cancellationToken = default)
    {
        if (limit is 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "a limit must be at least 1; pass null for the broker's default");
        }

        Arg.OptionalText(filter?.TenantId, nameof(filter));
        Arg.OptionalText(filter?.Namespace, nameof(filter));
        Arg.OptionalText(filter?.Stream, nameof(filter));
        Arg.OptionalText(filter?.Principal, nameof(filter));
        if (cursor is not null)
        {
            Arg.Text(cursor.TenantId, nameof(cursor));
            Arg.Text(cursor.Namespace, nameof(cursor));
            Arg.Text(cursor.Stream, nameof(cursor));
        }

        var cursorJson = cursor is null ? null : JsonSerializer.Serialize(cursor, WireJson.Default.SubscriptionCursor);
        return ParsePage(Call<string>(cancellationToken, (client, cancel, userData) =>
        {
            nint tenantPtr = 0, nsPtr = 0, streamPtr = 0, principalPtr = 0, cursorPtr = 0;
            try
            {
                tenantPtr = Utf8(filter?.TenantId);
                nsPtr = Utf8(filter?.Namespace);
                streamPtr = Utf8(filter?.Stream);
                principalPtr = Utf8(filter?.Principal);
                cursorPtr = Utf8(cursorJson);
                var native = new NativeSubscriptionFilter
                {
                    Size = (nuint)sizeof(NativeSubscriptionFilter),
                    TenantId = (byte*)tenantPtr,
                    Ns = (byte*)nsPtr,
                    Stream = (byte*)streamPtr,
                    Principal = (byte*)principalPtr,
                    Shard = filter?.Shard ?? 0,
                    HasShard = filter?.Shard is null ? (byte)0 : (byte)1,
                    Dropping = filter?.Dropping == true ? (byte)1 : (byte)0,
                };
                // The library copies the filter and the cursor before the call returns.
                return NativeMethods.felix_client_list_subscriptions(
                    client, filter is null ? null : &native, limit ?? 0, (byte*)cursorPtr,
                    cancel, &Callbacks.OnSubscriptions, userData);
            }
            finally
            {
                Marshal.FreeCoTaskMem(tenantPtr);
                Marshal.FreeCoTaskMem(nsPtr);
                Marshal.FreeCoTaskMem(streamPtr);
                Marshal.FreeCoTaskMem(principalPtr);
                Marshal.FreeCoTaskMem(cursorPtr);
            }
        }));
    }

    /// <summary>
    /// The brokers a client may connect to, as the connected broker names them. This only reads the list;
    /// <see cref="RefreshTopologyAsync"/> adds them to the endpoints the client tries.
    /// </summary>
    /// <remarks>Needs a broker that advertises topology (<see cref="FelixSupports.Topology"/>).</remarks>
    public unsafe Task<IReadOnlyList<BrokerEndpoint>> GetTopologyAsync(CancellationToken cancellationToken = default) =>
        ParseList(
            Call<string>(cancellationToken, (client, cancel, userData) =>
                NativeMethods.felix_client_topology(client, cancel, &Callbacks.OnJson, userData)),
            WireJson.Default.BrokerEndpointArray);

    /// <summary>How many shards a stream has and how it maps routing keys onto them.</summary>
    /// <remarks>Needs a broker that reports stream shards (<see cref="FelixSupports.StreamShards"/>).</remarks>
    public unsafe Task<StreamRouting> GetStreamRoutingAsync(string tenant, string ns, string stream, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        return Call<StreamRouting>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_stream_routing(client, tenant, ns, stream, cancel, &Callbacks.OnRouting, userData));
    }

    /// <summary>
    /// Which broker serves each shard of a stream or cache, in shard order. A shard nobody can serve has no
    /// node and says why in <see cref="ShardOwner.Unavailable"/>. A snapshot that can be stale the way a
    /// redirect can; a request sent on it is still routed correctly.
    /// </summary>
    /// <param name="kind">Whether <paramref name="name"/> is a stream or a cache.</param>
    /// <param name="name">The stream or cache.</param>
    /// <remarks>Needs a broker that reports shard owners (<see cref="FelixSupports.ShardOwners"/>).</remarks>
    public unsafe Task<IReadOnlyList<ShardOwner>> GetShardOwnersAsync(
        ShardKind kind,
        string tenant,
        string ns,
        string name,
        CancellationToken cancellationToken = default)
    {
        CheckKind(kind);
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(name);
        return ParseList(
            Call<string>(cancellationToken, (client, cancel, userData) =>
                NativeMethods.felix_client_shard_owners(client, (int)kind, tenant, ns, name, cancel, &Callbacks.OnJson, userData)),
            WireJson.Default.ShardOwnerArray);
    }

    /// <summary>How many shards a cache has.</summary>
    /// <remarks>Needs a broker that reports cache shards (<see cref="FelixSupports.CacheShards"/>).</remarks>
    public unsafe Task<int> GetCacheShardsAsync(string tenant, string ns, string cache, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(cache);
        return Call<int>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_cache_shards(client, tenant, ns, cache, cancel, &Callbacks.OnCount, userData));
    }

    private static void CheckKind(ShardKind kind)
    {
        if (kind is not (ShardKind.Stream or ShardKind.Cache))
        {
            throw new ArgumentOutOfRangeException(nameof(kind), kind, "kind must be Stream or Cache");
        }
    }

    private static unsafe nint Utf8(string? text) => text is null ? 0 : Marshal.StringToCoTaskMemUTF8(text);

    private static async Task<T> Parse<T>(Task<string> answer, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T> info) =>
        WireJson.Read(await answer.ConfigureAwait(false), info);

    private static async Task<IReadOnlyList<T>> ParseList<T>(Task<string> answer, System.Text.Json.Serialization.Metadata.JsonTypeInfo<T[]> info) =>
        WireJson.Read(await answer.ConfigureAwait(false), info);

    private static async Task<SubscriptionsPage> ParsePage(Task<string> answer) =>
        WireJson.Read(await answer.ConfigureAwait(false), WireJson.Default.SubscriptionsPage);
}
