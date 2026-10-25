using Felix.Native;

namespace Felix;

public sealed partial class FelixClient
{
    /// <summary>
    /// Everywhere this client would try to reach the cluster: the seeds it was given and the
    /// brokers it learned from them, as <c>host:port</c>. A client given one seed that lists more
    /// than one address has discovered the cluster.
    /// </summary>
    public unsafe Task<string[]> GetEndpointsAsync(CancellationToken cancellationToken = default) =>
        Call<string[]>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_endpoints(client, cancel, &Callbacks.OnEndpoints, userData));

    /// <summary>
    /// Publishes a batch to <paramref name="shard"/>, rather than the shard a key would hash to, for a
    /// producer that does its own partitioning. Returns each record's offset, in order, null where the
    /// broker reported none. A shard the stream does not have is refused and nothing is written.
    /// </summary>
    /// <exception cref="FelixException">The broker refused the batch, or is too old to take a shard number.</exception>
    public unsafe Task<long?[]> PublishOnShardAsync(
        string tenant,
        string ns,
        string stream,
        uint shard,
        IReadOnlyList<ReadOnlyMemory<byte>> payloads,
        AckMode ack = AckMode.PerBatch,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentNullException.ThrowIfNull(payloads);
        if (payloads.Count == 0)
        {
            throw new ArgumentException("a batch needs at least one record", nameof(payloads));
        }

        cancellationToken.ThrowIfCancellationRequested();
        var (data, lengths) = Pack(payloads);
        return Call<long?[]>(cancellationToken, (client, cancel, userData) =>
        {
            // The library copies both buffers before the call returns.
            fixed (byte* dataPointer = data)
            fixed (nuint* lengthPointer = lengths)
            {
                return NativeMethods.felix_client_publish_on_shard(
                    client, tenant, ns, stream, shard, dataPointer, lengthPointer, (nuint)lengths.Length, (int)ack, cancel, &Callbacks.OnBatch, userData);
            }
        });
    }

    /// <summary>
    /// Publishes records that each carry their own routing key. They are split by shard and each shard's
    /// records go to its owner as one batch, in the order given; there is no order between shards. A
    /// shard's batch is accepted or refused as a unit and does not stop the others, so the result can
    /// report some records accepted and others not. It is indexed like <paramref name="records"/>.
    /// </summary>
    /// <remarks>
    /// With <see cref="Delivery.AtLeastOnce"/> a shard's batch whose outcome is in doubt is sent again,
    /// which can write it twice, and a resent batch lands after records sent later.
    /// </remarks>
    public unsafe Task<PublishResult[]> PublishRecordsAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<(ReadOnlyMemory<byte> Payload, ReadOnlyMemory<byte> Key)> records,
        AckMode ack = AckMode.PerBatch,
        Delivery delivery = Delivery.AtMostOnce,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
        {
            throw new ArgumentException("at least one record is required", nameof(records));
        }

        cancellationToken.ThrowIfCancellationRequested();
        var (data, lengths) = Pack(records.Select(r => r.Payload).ToList());
        var (keys, keyLengths) = Pack(records.Select(r => r.Key).ToList());
        return Call<PublishResult[]>(cancellationToken, (client, cancel, userData) =>
        {
            fixed (byte* dataPointer = data)
            fixed (nuint* lengthPointer = lengths)
            fixed (byte* keyPointer = keys)
            fixed (nuint* keyLengthPointer = keyLengths)
            {
                return NativeMethods.felix_client_publish_records(
                    client, tenant, ns, stream, dataPointer, lengthPointer, keyPointer, keyLengthPointer, (nuint)lengths.Length,
                    (int)ack, (int)delivery, cancel, &Callbacks.OnRecords, userData);
            }
        });
    }

    /// <summary>
    /// The first offset on one shard whose record was appended at or after <paramref name="at"/>, by the
    /// leading broker's clock; subscribe with <see cref="StartPosition.FromOffset"/> at it to replay from
    /// a time. Null when nothing is that recent yet. A time older than every record answers with the oldest.
    /// </summary>
    public unsafe Task<long?> OffsetForTimeAsync(
        string tenant,
        string ns,
        string stream,
        uint shard,
        DateTimeOffset at,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        cancellationToken.ThrowIfCancellationRequested();
        var micros = (at.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10;
        if (micros < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(at), "must not be before the Unix epoch");
        }

        return Call<long?>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_offset_for_time(
                client, tenant, ns, stream, shard, (ulong)micros, cancel, &Callbacks.OnOffset, userData));
    }

    /// <summary>
    /// Subscribes to every shard of a stream. The call fails, and nothing is handed out, if any shard
    /// cannot be opened, so a subscription never quietly covers fewer shards than the stream has.
    /// <paramref name="start"/> applies to every shard.
    /// </summary>
    public async Task<FelixShardedSubscription> SubscribeShardedAsync(
        string tenant,
        string ns,
        string stream,
        StartPosition start,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        return await ResubscribeShardedAsync(tenant, ns, stream, [], start, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resumes a sharded subscription from where each shard had reached, as
    /// <see cref="FelixShardedSubscription.Positions"/> reports it. Each listed shard resumes just after
    /// its offset; a shard not listed starts at <paramref name="start"/>, which is what a shard that had
    /// delivered nothing should do.
    /// </summary>
    /// <exception cref="ArgumentException">A shard is listed twice.</exception>
    public async Task<FelixShardedSubscription> ResubscribeShardedAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<ShardPosition> positions,
        StartPosition start,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentNullException.ThrowIfNull(positions);
        var shards = new uint[positions.Count];
        var offsets = new ulong[positions.Count];
        for (var i = 0; i < shards.Length; i++)
        {
            shards[i] = positions[i].Shard;
            offsets[i] = unchecked((ulong)positions[i].Offset);
        }

        var handle = await Call<FelixShardedSubscriptionHandle>(cancellationToken, (client, cancel, userData) =>
            StartResubscribe(client, tenant, ns, stream, shards, offsets, start, cancel, userData)).ConfigureAwait(false);
        return new FelixShardedSubscription(handle);
    }

    private static unsafe int StartResubscribe(
        nint client,
        string tenant,
        string ns,
        string stream,
        uint[] shards,
        ulong[] offsets,
        StartPosition start,
        nint cancel,
        nint userData)
    {
        // The library copies the positions before the call returns.
        fixed (uint* shardPointer = shards)
        fixed (ulong* offsetPointer = offsets)
        {
            return shards.Length == 0
                ? NativeMethods.felix_client_subscribe_sharded(
                    client, tenant, ns, stream, start.Kind, start.Offset, cancel, &Callbacks.OnShardedSubscribe, userData)
                : NativeMethods.felix_client_resubscribe_sharded(
                    client, tenant, ns, stream, shardPointer, offsetPointer, (nuint)shards.Length, start.Kind, start.Offset, cancel, &Callbacks.OnShardedSubscribe, userData);
        }
    }

    /// <summary>Lays the buffers end to end, with each one's length; empty buffers are valid.</summary>
    private static (byte[] Data, nuint[] Lengths) Pack(IReadOnlyList<ReadOnlyMemory<byte>> buffers)
    {
        var lengths = new nuint[buffers.Count];
        long total = 0;
        for (var i = 0; i < lengths.Length; i++)
        {
            lengths[i] = (nuint)buffers[i].Length;
            total += buffers[i].Length;
        }

        var data = new byte[total];
        var at = 0;
        foreach (var buffer in buffers)
        {
            buffer.Span.CopyTo(data.AsSpan(at));
            at += buffer.Length;
        }

        return (data, lengths);
    }
}
