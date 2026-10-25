using Felix.Native;

namespace Felix;

/// <summary>
/// A live watch on a cache, on one key or prefix of a shard or on a prefix across every shard.
/// One read may be pending at a time. Enumerating it yields every item, including
/// <see cref="WatchItemKind.Lagged"/> (which the application must act on, by re-watching from
/// <see cref="WatchItem.ResumeFrom"/>) and ends when the feed does, which is after a lag on a single
/// watch and once every shard of a sharded one has ended.
/// </summary>
public sealed class FelixCacheWatch : IAsyncEnumerable<WatchItem>, IAsyncDisposable, IDisposable
{
    private readonly FelixCacheWatchHandle handle;
    private int reading;

    internal unsafe FelixCacheWatch(FelixCacheWatchHandle handle)
    {
        this.handle = handle;
        using var call = new HandleLease(handle);
        ulong count;
        byte has;
        var status = NativeMethods.felix_cache_watch_retained_count(call.Pointer, &count, &has);
        if (status != 0)
        {
            var message = NativeMethods.LastError();
            handle.Dispose();
            throw FelixException.FromStatus(status, message);
        }

        RetainedCount = has != 0 ? unchecked((long)count) : null;
    }

    /// <summary>
    /// How many entries a retained watch started from, so an application knows when its state is
    /// complete; zero means the prefix was empty. Null for a watch that is not retained.
    /// </summary>
    public long? RetainedCount { get; }

    /// <summary>
    /// Where each shard has reached, in ascending shard order and never ahead of what reads have
    /// handed out, for <see cref="FelixCache.WatchShardedAsync"/>. Sharded watches only; a single watch fails
    /// with <see cref="FelixStatus.InvalidArgument"/>. May be called while a read is pending.
    /// </summary>
    public unsafe ShardOffset[] ResumeOffsets()
    {
        using var call = new HandleLease(handle);
        var capacity = 8;
        while (true)
        {
            var shards = new uint[capacity];
            var offsets = new ulong[capacity];
            nuint count;
            int status;
            fixed (uint* shardPointer = shards)
            fixed (ulong* offsetPointer = offsets)
            {
                status = NativeMethods.felix_cache_watch_resume_offsets(call.Pointer, shardPointer, offsetPointer, (nuint)capacity, &count);
            }

            if (status != 0)
            {
                throw FelixException.FromStatus(status, NativeMethods.LastError());
            }

            // The total is reported even when the buffers were too small: ask again with room.
            if ((int)count > capacity)
            {
                capacity = checked((int)count);
                continue;
            }

            var result = new ShardOffset[(int)count];
            for (var i = 0; i < result.Length; i++)
            {
                result[i] = new ShardOffset(shards[i], unchecked((long)offsets[i]));
            }

            return result;
        }
    }

    /// <summary>
    /// Reads the next item, or returns null if <paramref name="timeout"/> passes with none (the watch
    /// stays usable, and a timed-out or cancelled read loses nothing).
    /// </summary>
    /// <param name="timeout">How long to wait; <see cref="Timeout.InfiniteTimeSpan"/> waits indefinitely.</param>
    /// <exception cref="FelixException">With <see cref="FelixStatus.Closed"/> once the feed has ended.</exception>
    /// <exception cref="InvalidOperationException">Another read is pending.</exception>
    public async Task<WatchItem?> ReadAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        long timeoutMs;
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            timeoutMs = -1;
        }
        else if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
        else
        {
            timeoutMs = (long)Math.Ceiling(timeout.TotalMilliseconds);
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (Interlocked.CompareExchange(ref reading, 1, 0) != 0)
        {
            throw new InvalidOperationException("a read is already pending on this watch");
        }

        try
        {
            return await StartRead(timeoutMs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref reading, 0);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<WatchItem> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            WatchItem? next;
            try
            {
                next = await ReadAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
            }
            catch (FelixException e) when (e.Status == FelixStatus.Closed)
            {
                yield break;
            }

            if (next is not null)
            {
                yield return next;
            }
        }
    }

    /// <summary>Frees the watch, which stops every shard. A pending read completes as cancelled.</summary>
    public void Dispose() => handle.Dispose();

    /// <inheritdoc cref="Dispose" />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    private unsafe Task<WatchItem?> StartRead(long timeoutMs, CancellationToken cancellationToken) =>
        NativeCall.Start<WatchItem?>(handle, cancellationToken, (watch, cancel, userData) =>
            NativeMethods.felix_cache_watch_next(watch, timeoutMs, cancel, &Callbacks.OnWatchItem, userData));
}
