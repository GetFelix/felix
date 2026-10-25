using Felix.Native;

namespace Felix;

public sealed partial class FelixClient
{
    /// <summary>
    /// Commits <paramref name="ops"/> as one record: every reader sees all of it or none of it.
    /// Returns the offset the commit's event is read at, which is also the version of every key it wrote.
    /// </summary>
    /// <param name="entityKey">Routes the commit to a shard, and is what a later <see cref="GetStateAsync"/> reads by. Empty for none.</param>
    /// <param name="ops">Exactly one event (<see cref="CommitOp.Publish"/> or <see cref="CommitOp.Enqueue"/>), and state changes, all on one stream.</param>
    /// <param name="shard">Commits to this shard rather than the one <paramref name="entityKey"/> routes to.</param>
    /// <remarks>
    /// The library refuses a commit with no event, two events or two streams as <see cref="FelixStatus.InvalidArgument"/>
    /// before anything is sent. A broker that does not serve commits fails with <see cref="FelixStatus.Unsupported"/>.
    /// Cancelled after it may have been sent, the call fails with <see cref="FelixStatus.OutcomeUnknown"/>.
    /// </remarks>
    public unsafe Task<long> CommitAsync(
        string tenant,
        string ns,
        ReadOnlyMemory<byte> entityKey,
        IReadOnlyList<CommitOp> ops,
        uint? shard = null,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        ArgumentNullException.ThrowIfNull(ops);
        using var native = new NativeCommitOps(ops);
        return Call<long>(cancellationToken, (client, cancel, userData) =>
        {
            // The library copies the key and the operations before the call returns.
            fixed (byte* key = entityKey.Span)
            {
                return shard is { } s
                    ? NativeMethods.felix_client_commit_on_shard(
                        client, tenant, ns, s, key, (nuint)entityKey.Length, native.Pointer, native.Count, cancel, &Callbacks.OnCommit, userData)
                    : NativeMethods.felix_client_commit(
                        client, tenant, ns, key, (nuint)entityKey.Length, native.Pointer, native.Count, cancel, &Callbacks.OnCommit, userData);
            }
        });
    }

    /// <summary>
    /// As <see cref="CommitAsync"/>, only if the commit would land at exactly <paramref name="expectedOffset"/>,
    /// the shard's next offset: nothing has been appended since the caller saw the tail. A compare-and-set on the whole shard.
    /// A refusal is <see cref="ConditionalWrite.Written"/> false with the tail, not an exception.
    /// </summary>
    public unsafe Task<ConditionalWrite> CommitIfAsync(
        string tenant,
        string ns,
        ReadOnlyMemory<byte> entityKey,
        IReadOnlyList<CommitOp> ops,
        long expectedOffset,
        uint? shard = null,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        ArgumentNullException.ThrowIfNull(ops);
        var expected = Offset(expectedOffset);
        using var native = new NativeCommitOps(ops);
        return Call<ConditionalWrite>(cancellationToken, (client, cancel, userData) =>
        {
            fixed (byte* key = entityKey.Span)
            {
                return shard is { } s
                    ? NativeMethods.felix_client_commit_if_on_shard(
                        client, tenant, ns, s, key, (nuint)entityKey.Length, native.Pointer, native.Count, expected, cancel, &Callbacks.OnConditional, userData)
                    : NativeMethods.felix_client_commit_if(
                        client, tenant, ns, key, (nuint)entityKey.Length, native.Pointer, native.Count, expected, cancel, &Callbacks.OnConditional, userData);
            }
        });
    }

    /// <summary>
    /// Publishes a batch to one shard, only if it would land at exactly <paramref name="expectedOffset"/>,
    /// the shard's next offset. The batch is written whole or not at all; a refusal is
    /// <see cref="ConditionalWrite.Written"/> false with the tail, not an exception.
    /// </summary>
    /// <param name="key">Routes the batch to its key's shard; null means shard 0. Not allowed with <paramref name="shard"/>.</param>
    /// <param name="shard">Publishes to this shard rather than the one a key would hash to.</param>
    public unsafe Task<ConditionalWrite> PublishIfAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<ReadOnlyMemory<byte>> payloads,
        long expectedOffset,
        ReadOnlyMemory<byte>? key = null,
        uint? shard = null,
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

        if (shard is not null && key is not null)
        {
            throw new ArgumentException("name a shard or a key, not both", nameof(key));
        }

        var expected = Offset(expectedOffset);
        var (data, lengths) = Payloads.Flatten(payloads);
        return Call<ConditionalWrite>(cancellationToken, (client, cancel, userData) =>
        {
            byte empty = 0;
            fixed (byte* dataPointer = data)
            fixed (nuint* lengthPointer = lengths)
            fixed (byte* keyBytes = (key ?? default).Span)
            {
                if (shard is { } s)
                {
                    return NativeMethods.felix_client_publish_if_on_shard(
                        client, tenant, ns, stream, s, dataPointer, lengthPointer, (nuint)lengths.Length, expected,
                        cancel, &Callbacks.OnConditional, userData);
                }

                var keyPointer = key is null ? null : key.Value.IsEmpty ? &empty : keyBytes;
                return NativeMethods.felix_client_publish_if(
                    client, tenant, ns, stream, keyPointer, key is null ? 0 : (nuint)key.Value.Length, dataPointer, lengthPointer,
                    (nuint)lengths.Length, expected, cancel, &Callbacks.OnConditional, userData);
            }
        });
    }

    /// <summary>
    /// Reads the state under <paramref name="key"/>, as written by commits. A key that was never written,
    /// or was deleted, has a null <see cref="StateValue.Value"/>; that is an answer, not an error.
    /// </summary>
    /// <param name="entityKey">What the commits that wrote the key were routed by. Not allowed with <paramref name="shard"/>.</param>
    /// <param name="shard">Reads this shard, for state written with a shard named on the commit.</param>
    public unsafe Task<StateValue> GetStateAsync(
        string tenant,
        string ns,
        string stream,
        string key,
        ReadOnlyMemory<byte> entityKey = default,
        uint? shard = null,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(key);
        if (shard is not null && !entityKey.IsEmpty)
        {
            throw new ArgumentException("name a shard or an entity key, not both", nameof(entityKey));
        }

        return Call<StateValue>(cancellationToken, (client, cancel, userData) =>
        {
            if (shard is { } s)
            {
                return NativeMethods.felix_client_state_get_on_shard(
                    client, tenant, ns, stream, s, key, cancel, &Callbacks.OnState, userData);
            }

            fixed (byte* entity = entityKey.Span)
            {
                return NativeMethods.felix_client_state_get(
                    client, tenant, ns, stream, entity, (nuint)entityKey.Length, key, cancel, &Callbacks.OnState, userData);
            }
        });
    }

    /// <summary>
    /// Asks the broker for a producer id and makes an <see cref="FelixProducer"/> under it,
    /// whose publishes cannot land twice. Like a subscription, it keeps working after
    /// the client is disposed, until it is disposed itself.
    /// </summary>
    public async Task<FelixProducer> CreateProducerAsync(CancellationToken cancellationToken = default) =>
        new(await StartProducer(cancellationToken).ConfigureAwait(false));

    private unsafe Task<FelixProducerHandle> StartProducer(CancellationToken cancellationToken) =>
        Call<FelixProducerHandle>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_idempotent_producer(client, cancel, &Callbacks.OnProducer, userData));

    private static ulong Offset(long offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        return (ulong)offset;
    }
}
