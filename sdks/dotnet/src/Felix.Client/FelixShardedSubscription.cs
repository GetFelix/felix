using Felix.Native;

namespace Felix;

/// <summary>
/// A live subscription to every shard of a stream. Order holds within a shard and nowhere across
/// shards. One read may be pending at a time. Enumerating it reads until every shard's feed has
/// ended or the token is cancelled.
/// </summary>
public sealed class FelixShardedSubscription : IAsyncEnumerable<ShardItem>, IAsyncDisposable, IDisposable
{
    private readonly FelixShardedSubscriptionHandle handle;
    private int reading;

    internal unsafe FelixShardedSubscription(FelixShardedSubscriptionHandle handle)
    {
        this.handle = handle;
        using var call = new HandleLease(handle);
        uint shards;
        var status = NativeMethods.felix_sharded_shards(call.Pointer, &shards);
        if (status != 0)
        {
            var message = NativeMethods.LastError();
            handle.Dispose();
            throw FelixException.FromStatus(status, message);
        }

        ShardCount = shards;
    }

    /// <summary>How many shards the subscription covers.</summary>
    public uint ShardCount { get; }

    /// <summary>
    /// Reads the next item from any shard, or returns null if <paramref name="timeout"/> passes with
    /// none (the subscription stays usable, and a timed-out or cancelled read loses nothing).
    /// </summary>
    /// <exception cref="FelixException">With <see cref="FelixStatus.Closed"/> once every shard's feed has ended.</exception>
    /// <exception cref="InvalidOperationException">Another read is pending.</exception>
    public async Task<ShardItem?> ReadAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
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
            throw new InvalidOperationException("a read is already pending on this subscription");
        }

        try
        {
            return await Next(timeoutMs, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref reading, 0);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerator<ShardItem> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            ShardItem? next;
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

    /// <summary>
    /// Where each shard has reached, for <see cref="FelixClient.ResubscribeShardedAsync"/>. Only shards
    /// that have delivered a record to the caller appear, in ascending shard order. Safe to call while a read is pending.
    /// </summary>
    public unsafe IReadOnlyList<ShardPosition> Positions()
    {
        using var call = new HandleLease(handle);
        var capacity = (nuint)Math.Max(ShardCount, 1u);
        while (true)
        {
            var shards = new uint[(int)capacity];
            var offsets = new ulong[(int)capacity];
            nuint total;
            int status;
            fixed (uint* shardPointer = shards)
            fixed (ulong* offsetPointer = offsets)
            {
                status = NativeMethods.felix_sharded_positions(call.Pointer, shardPointer, offsetPointer, capacity, &total);
            }

            if (status != 0)
            {
                throw FelixException.FromStatus(status, NativeMethods.LastError());
            }

            // The library says how many there are; a buffer that was too small is asked again, larger.
            if (total > capacity)
            {
                capacity = total;
                continue;
            }

            var result = new ShardPosition[(int)total];
            for (var i = 0; i < result.Length; i++)
            {
                result[i] = new ShardPosition(shards[i], unchecked((long)offsets[i]));
            }

            return result;
        }
    }

    /// <summary>Frees the subscription, which stops every shard. A pending read completes as cancelled.</summary>
    public void Dispose() => handle.Dispose();

    /// <inheritdoc cref="Dispose" />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return default;
    }

    private unsafe Task<ShardItem?> Next(long timeoutMs, CancellationToken cancellationToken) =>
        NativeCall.Start<ShardItem?>(handle, cancellationToken, (subscription, cancel, userData) =>
            NativeMethods.felix_sharded_next(subscription, timeoutMs, cancel, &Callbacks.OnShardItem, userData));
}
