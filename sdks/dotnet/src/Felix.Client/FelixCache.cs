using Felix.Native;

namespace Felix;

/// <summary>
/// One cache of a tenant's namespace: values by key with an optional lifetime, conditional
/// writes, counters and watches. Cheap to make and holds no native state, so it is as live as
/// its client; calls after the client is disposed throw <see cref="ObjectDisposedException"/>.
/// Keys are UTF-8 strings. Writes cancelled after they may have been sent fail with
/// <see cref="FelixStatus.OutcomeUnknown"/>; reads cancelled fail with <see cref="OperationCanceledException"/>.
/// </summary>
public sealed class FelixCache
{
    private readonly FelixClient client;

    internal FelixCache(FelixClient client, string tenant, string ns, string cache)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(cache);
        this.client = client;
        Tenant = tenant;
        Namespace = ns;
        Name = cache;
    }

    /// <summary>The tenant the cache belongs to.</summary>
    public string Tenant { get; }

    /// <summary>The cache's namespace.</summary>
    public string Namespace { get; }

    /// <summary>The cache's name.</summary>
    public string Name { get; }

    /// <summary>Whole milliseconds for a lifetime, rounded up so a short one never becomes "for ever" (zero).</summary>
    private static ulong TtlMillis(TimeSpan? ttl)
    {
        if (ttl is not { } value)
        {
            return 0;
        }

        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(ttl), "a lifetime must be positive; pass null for none");
        }

        return (ulong)Math.Ceiling(value.TotalMilliseconds);
    }

    /// <summary>Stores <paramref name="value"/> under <paramref name="key"/>. Safe to repeat.</summary>
    /// <param name="ttl">How long it lives; null is for ever.</param>
    public unsafe Task PutAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? ttl = null, CancellationToken cancellationToken = default)
    {
        Arg.Text(key);
        var ttlMs = TtlMillis(ttl);
        return client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
        {
            // The library copies the value before the call returns.
            fixed (byte* data = value.Span)
            {
                return NativeMethods.felix_client_cache_put(
                    handle, Tenant, Namespace, Name, key, data, (nuint)value.Length, ttlMs, cancel, &Callbacks.OnDone, userData);
            }
        });
    }

    /// <summary>Reads the value under <paramref name="key"/>. A missing or expired key is null, not an error.</summary>
    public unsafe Task<ReadOnlyMemory<byte>?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Arg.Text(key);
        return client.Call<ReadOnlyMemory<byte>?>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_cache_get(handle, Tenant, Namespace, Name, key, cancel, &Callbacks.OnValue, userData));
    }

    /// <summary>Reads the value under <paramref name="key"/> with the version a conditional write can name. Null when there is none.</summary>
    public unsafe Task<VersionedValue?> GetVersionedAsync(string key, CancellationToken cancellationToken = default)
    {
        Arg.Text(key);
        return client.Call<VersionedValue?>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_cache_get_versioned(handle, Tenant, Namespace, Name, key, cancel, &Callbacks.OnVersioned, userData));
    }

    /// <summary>Removes <paramref name="key"/>. Returns the value it held, or null if there was none (an answer, not an error).</summary>
    public unsafe Task<ReadOnlyMemory<byte>?> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        Arg.Text(key);
        return client.Call<ReadOnlyMemory<byte>?>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_cache_delete(handle, Tenant, Namespace, Name, key, cancel, &Callbacks.OnValue, userData));
    }

    /// <summary>Stores <paramref name="value"/> only if <paramref name="key"/> has no live entry.</summary>
    public Task<ConditionalResult> PutIfAbsentAsync(string key, ReadOnlyMemory<byte> value, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        PutIf(key, value, ttl, Absent, 0, cancellationToken);

    /// <summary>
    /// Stores <paramref name="value"/> only if the key's live entry has exactly <paramref name="version"/>,
    /// a version from <see cref="GetVersionedAsync"/>.
    /// </summary>
    public Task<ConditionalResult> PutIfVersionAsync(string key, ReadOnlyMemory<byte> value, long version, TimeSpan? ttl = null, CancellationToken cancellationToken = default) =>
        PutIf(key, value, ttl, AtVersion, version, cancellationToken);

    private const int Absent = 0;
    private const int AtVersion = 1;

    private unsafe Task<ConditionalResult> PutIf(string key, ReadOnlyMemory<byte> value, TimeSpan? ttl, int kind, long version, CancellationToken cancellationToken)
    {
        Arg.Text(key);
        var ttlMs = TtlMillis(ttl);
        return client.Call<ConditionalResult>(cancellationToken, (handle, cancel, userData) =>
        {
            fixed (byte* data = value.Span)
            {
                return NativeMethods.felix_client_cache_put_if(
                    handle, Tenant, Namespace, Name, key, data, (nuint)value.Length, ttlMs, kind, unchecked((ulong)version),
                    cancel, &Callbacks.OnCondition, userData);
            }
        });
    }

    /// <summary>Removes <paramref name="key"/> only if its entry has exactly <paramref name="version"/>.</summary>
    public unsafe Task<ConditionalResult> DeleteIfVersionAsync(string key, long version, CancellationToken cancellationToken = default)
    {
        Arg.Text(key);
        return client.Call<ConditionalResult>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_cache_delete_if(
                handle, Tenant, Namespace, Name, key, unchecked((ulong)version), cancel, &Callbacks.OnCondition, userData));
    }

    /// <summary>
    /// Adds <paramref name="delta"/> to the counter under <paramref name="key"/>, creating it at zero,
    /// and returns the new value. The counter saturates at <see cref="long.MaxValue"/> and <see cref="long.MinValue"/>
    /// instead of wrapping. Not idempotent: an add cancelled after it may have been sent fails with
    /// <see cref="FelixStatus.OutcomeUnknown"/> and may or may not have counted, so read the counter before repeating it.
    /// </summary>
    public async Task<long> CounterAddAsync(string key, long delta, CancellationToken cancellationToken = default) =>
        await StartCounterAdd(key, delta, cancellationToken).ConfigureAwait(false)
            ?? throw new FelixException(FelixStatus.Error, "the library reported no value for a counter add");

    private unsafe Task<long?> StartCounterAdd(string key, long delta, CancellationToken cancellationToken)
    {
        Arg.Text(key);
        return client.Call<long?>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_counter_add(handle, Tenant, Namespace, Name, key, delta, cancel, &Callbacks.OnCounter, userData));
    }

    /// <summary>Reads the counter under <paramref name="key"/>. A counter never written is null, which is not zero.</summary>
    public unsafe Task<long?> CounterGetAsync(string key, CancellationToken cancellationToken = default)
    {
        Arg.Text(key);
        return client.Call<long?>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_counter_get(handle, Tenant, Namespace, Name, key, cancel, &Callbacks.OnCounter, userData));
    }

    /// <summary>
    /// Watches one key, which is routed to its own shard. <paramref name="retained"/> first delivers
    /// the stored value as a change. <paramref name="fromOffset"/> is the first offset not yet seen: the
    /// <see cref="WatchItem.Offset"/> of the last change handled plus one, or a <see cref="WatchItemKind.Lagged"/>
    /// item's <see cref="WatchItem.ResumeFrom"/>. Retained with an offset is refused with
    /// <see cref="FelixStatus.InvalidArgument"/>.
    /// </summary>
    public Task<FelixCacheWatch> WatchKeyAsync(string key, bool retained = false, long? fromOffset = null, CancellationToken cancellationToken = default) =>
        Watch(WatchKey, key, retained, fromOffset, cancellationToken);

    /// <summary>
    /// Watches every key that starts with <paramref name="prefix"/> on one shard. A single watch reads
    /// one shard, so on a cache with several use <see cref="WatchShardedAsync"/>. Arguments as for <see cref="WatchKeyAsync"/>.
    /// </summary>
    public Task<FelixCacheWatch> WatchPrefixAsync(string prefix, bool retained = false, long? fromOffset = null, CancellationToken cancellationToken = default) =>
        Watch(WatchPrefix, prefix, retained, fromOffset, cancellationToken);

    private const int WatchKey = 0;
    private const int WatchPrefix = 1;

    private async Task<FelixCacheWatch> Watch(int kind, string filter, bool retained, long? fromOffset, CancellationToken cancellationToken)
    {
        Arg.Text(filter);
        if (fromOffset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(fromOffset));
        }

        var native = await OpenWatch(kind, filter, retained, fromOffset, cancellationToken).ConfigureAwait(false);
        return new FelixCacheWatch(native);
    }

    private unsafe Task<FelixCacheWatchHandle> OpenWatch(int kind, string filter, bool retained, long? fromOffset, CancellationToken cancellationToken) =>
        client.Call<FelixCacheWatchHandle>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_watch_cache(
                handle, Tenant, Namespace, Name, kind, filter,
                retained ? (byte)1 : (byte)0,
                fromOffset is null ? (byte)0 : (byte)1,
                unchecked((ulong)(fromOffset ?? 0)),
                cancel, &Callbacks.OnWatchOpen, userData));

    /// <summary>
    /// Watches every shard of the cache for keys that start with <paramref name="prefix"/>. It fails,
    /// and hands out no watch, if any shard cannot be opened, so a watch never covers fewer shards than
    /// the cache has. <paramref name="resume"/> restarts the listed shards at their offsets, as
    /// <see cref="FelixCacheWatch.ResumeOffsets"/> returns them; null or empty starts from now.
    /// <paramref name="retained"/> first delivers each shard's stored entries, then
    /// <see cref="WatchItemKind.StateComplete"/>, and cannot be combined with resume offsets. A shard listed twice is refused.
    /// </summary>
    public async Task<FelixCacheWatch> WatchShardedAsync(
        string prefix,
        bool retained = false,
        IReadOnlyList<ShardOffset>? resume = null,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(prefix);
        var shards = new uint[resume?.Count ?? 0];
        var offsets = new ulong[shards.Length];
        for (var i = 0; i < shards.Length; i++)
        {
            if (resume![i].Offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(resume), "an offset cannot be negative");
            }

            shards[i] = resume[i].Shard;
            offsets[i] = (ulong)resume[i].Offset;
        }

        var native = await OpenShardedWatch(prefix, retained, shards, offsets, cancellationToken).ConfigureAwait(false);
        return new FelixCacheWatch(native);
    }

    private unsafe Task<FelixCacheWatchHandle> OpenShardedWatch(string prefix, bool retained, uint[] shards, ulong[] offsets, CancellationToken cancellationToken) =>
        client.Call<FelixCacheWatchHandle>(cancellationToken, (handle, cancel, userData) =>
        {
            // The library copies the arrays before the call returns.
            fixed (uint* shardPointer = shards)
            fixed (ulong* offsetPointer = offsets)
            {
                return NativeMethods.felix_client_watch_cache_sharded(
                    handle, Tenant, Namespace, Name, prefix, retained ? (byte)1 : (byte)0,
                    shards.Length == 0 ? null : shardPointer,
                    shards.Length == 0 ? null : offsetPointer,
                    (nuint)shards.Length, cancel, &Callbacks.OnWatchOpen, userData);
            }
        });
}
