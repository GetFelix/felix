using Felix.Native;

namespace Felix;

/// <summary>
/// A producer whose publishes are numbered, so a batch the caller never got an answer for can be
/// sent again and still land once. Calls on one producer reach the broker in the order they were made.
/// </summary>
/// <remarks>
/// <para>
/// A publish can fail with the batch in doubt: any failure but a refusal. The producer's sequence has
/// not advanced then and the batch may have landed. To settle it, call again with the same payloads and
/// key: the broker answers a batch it holds without appending it, so it cannot land twice. A different
/// batch is refused without being sent until then. A cancel after the batch was handed over fails with
/// <see cref="FelixStatus.OutcomeUnknown"/>; the batch is still sent, so settle it the same way.
/// </para>
/// <para>
/// A refusal by the broker (for example, the shard no longer knows this producer) ends the producer on
/// that stream: every later call fails with the same reason. Start a new producer, or call
/// <see cref="AbandonInDoubtAsync"/>, after deciding whether the last batch landed.
/// </para>
/// <para>
/// <see cref="DisposeAsync"/> closes gracefully and frees it. <see cref="Dispose"/> only frees it, without
/// waiting for anything: calls pending on it fail as cancelled, and batches already handed over may
/// still be sent. After <see cref="CloseAsync"/> every call fails with <see cref="FelixStatus.Closed"/>.
/// </para>
/// </remarks>
public sealed class FelixProducer : IAsyncDisposable, IDisposable
{
    /// <summary>How long <see cref="DisposeAsync"/> waits for a graceful close.</summary>
    private static readonly TimeSpan DisposeTimeout = TimeSpan.FromSeconds(30);

    private readonly FelixProducerHandle handle;

    internal FelixProducer(FelixProducerHandle handle)
    {
        this.handle = handle;
    }

    /// <summary>The producer id the broker assigned, which its batches go out under unless a stream has been given one of its own.</summary>
    /// <exception cref="FelixException"><see cref="FelixStatus.Closed"/> once the producer has been closed.</exception>
    public unsafe ulong GetId()
    {
        using var call = new HandleLease(handle);
        ulong id;
        Callbacks.Check(NativeMethods.felix_producer_id(call.Pointer, &id));
        return id;
    }

    /// <summary>
    /// The id a stream's batches go out under: the producer's own, unless the stream was created again
    /// at another width or given up with <see cref="AbandonInDoubtAsync"/>, which numbers it afresh under an id of its own.
    /// </summary>
    /// <exception cref="FelixException"><see cref="FelixStatus.Closed"/> once the producer has been closed.</exception>
    public unsafe ulong GetIdFor(string tenant, string ns, string stream)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        using var call = new HandleLease(handle);
        ulong id;
        Callbacks.Check(NativeMethods.felix_producer_id_for(call.Pointer, tenant, ns, stream, &id));
        return id;
    }

    /// <summary>
    /// Publishes one record exactly once. Returns its log offset, or null when the broker reported none.
    /// </summary>
    /// <param name="key">Routes the record to its key's shard; null means shard 0.</param>
    public Task<long?> PublishAsync(
        string tenant,
        string ns,
        string stream,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<byte>? key = null,
        CancellationToken cancellationToken = default) =>
        PublishBatchAsync(tenant, ns, stream, [payload], key, cancellationToken);

    /// <summary>
    /// Publishes a batch of records exactly once, under one sequence whatever its size. Returns the
    /// offset of the batch's first record, or null when the broker reported none.
    /// </summary>
    /// <param name="key">Routes the batch to its key's shard; null means shard 0.</param>
    public unsafe Task<long?> PublishBatchAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<ReadOnlyMemory<byte>> payloads,
        ReadOnlyMemory<byte>? key = null,
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

        var (data, lengths) = Payloads.Flatten(payloads);
        return NativeCall.Start<long?>(handle, cancellationToken, (producer, cancel, userData) =>
        {
            byte empty = 0;
            fixed (byte* dataPointer = data)
            fixed (nuint* lengthPointer = lengths)
            fixed (byte* keyBytes = (key ?? default).Span)
            {
                var keyPointer = key is null ? null : key.Value.IsEmpty ? &empty : keyBytes;
                return NativeMethods.felix_producer_publish(
                    producer, tenant, ns, stream, dataPointer, lengthPointer, (nuint)lengths.Length,
                    keyPointer, key is null ? 0 : (nuint)key.Value.Length, cancel, &Callbacks.OnPublish, userData);
            }
        });
    }

    /// <summary>
    /// Publishes several batches exactly once each, under consecutive sequences, all to shard 0.
    /// Against a broker that pipelines publishes they are in flight together.
    /// </summary>
    /// <remarks>
    /// On a failure the batches already acknowledged stay acknowledged and the rest are in doubt
    /// together. Call again with the same batches to settle them: only the unacknowledged ones are sent.
    /// </remarks>
    public unsafe Task PublishBatchesAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<IReadOnlyList<ReadOnlyMemory<byte>>> batches,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentNullException.ThrowIfNull(batches);
        if (batches.Count == 0)
        {
            throw new ArgumentException("at least one batch is required", nameof(batches));
        }

        var all = new List<ReadOnlyMemory<byte>>();
        var sizes = new nuint[batches.Count];
        for (var i = 0; i < sizes.Length; i++)
        {
            var batch = batches[i];
            ArgumentNullException.ThrowIfNull(batch, nameof(batches));
            if (batch.Count == 0)
            {
                throw new ArgumentException("a batch needs at least one record", nameof(batches));
            }

            sizes[i] = (nuint)batch.Count;
            all.AddRange(batch);
        }

        var (data, lengths) = Payloads.Flatten(all);
        return NativeCall.Start<bool>(handle, cancellationToken, (producer, cancel, userData) =>
        {
            fixed (byte* dataPointer = data)
            fixed (nuint* lengthPointer = lengths)
            fixed (nuint* sizePointer = sizes)
            {
                return NativeMethods.felix_producer_publish_batches(
                    producer, tenant, ns, stream, dataPointer, lengthPointer, (nuint)lengths.Length,
                    sizePointer, (nuint)sizes.Length, cancel, &Callbacks.OnDone, userData);
            }
        });
    }

    /// <summary>
    /// Gives up on the stream's batches in doubt and numbers the stream from zero under a fresh producer id.
    /// For a stream that cannot settle them: one deleted and created again at another width while a batch
    /// was in doubt is refused every re-send. A batch given up may land twice: it may have been appended
    /// before, and sent again it is a new record under the fresh id.
    /// </summary>
    public unsafe Task AbandonInDoubtAsync(string tenant, string ns, string stream, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        return NativeCall.Start<bool>(handle, cancellationToken, (producer, cancel, userData) =>
            NativeMethods.felix_producer_abandon_in_doubt(producer, tenant, ns, stream, cancel, &Callbacks.OnDone, userData));
    }

    /// <summary>
    /// Waits for every publish already handed to the producer to be answered, then stops it. The
    /// producer is closed as soon as this succeeds: later calls fail with <see cref="FelixStatus.Closed"/>,
    /// a second close included.
    /// </summary>
    /// <param name="timeout">How long to wait; null waits as long as it takes.</param>
    /// <remarks>
    /// When the time runs out the call fails with <see cref="FelixStatus.Timeout"/> and the producer stays
    /// closed. If it ran out waiting for calls still being handed over, batches already handed to the
    /// producer may still be sent or re-sent. If it ran out waiting for the producer's task, the task is
    /// stopped and nothing more is sent; a batch already sent may or may not land. A cancelled close fails
    /// as cancelled and the producer is closed all the same.
    /// </remarks>
    public unsafe Task CloseAsync(TimeSpan? timeout = null, CancellationToken cancellationToken = default)
    {
        var millis = -1L;
        if (timeout is { } bound && bound != Timeout.InfiniteTimeSpan)
        {
            if (bound < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeout), "a timeout must not be negative; pass null for none");
            }

            millis = (long)Math.Ceiling(bound.TotalMilliseconds);
        }

        return NativeCall.Start<bool>(handle, cancellationToken, (producer, cancel, userData) =>
            NativeMethods.felix_producer_close(producer, millis, cancel, &Callbacks.OnDone, userData));
    }

    /// <summary>
    /// Frees the producer without waiting for anything: calls pending on it fail as cancelled
    /// (or <see cref="FelixStatus.OutcomeUnknown"/> for a send that may be out), and batches already
    /// handed over may still be sent. Use <see cref="CloseAsync"/> or <see cref="DisposeAsync"/> to wait for them.
    /// </summary>
    public void Dispose() => handle.Dispose();

    /// <summary>
    /// Closes the producer, waiting up to 30 seconds for what it has sent, then frees it. A close that
    /// fails (a timeout, a producer already closed) is not reported; call <see cref="CloseAsync"/> first
    /// to see it. Safe to call more than once.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (handle.IsClosed)
        {
            return;
        }

        try
        {
            await CloseAsync(DisposeTimeout).ConfigureAwait(false);
        }
        catch (Exception e) when (e is FelixException or ObjectDisposedException or OperationCanceledException)
        {
            // Disposal reports nothing: the handle is freed either way.
        }

        // Freeing the last handle on the runtime can wait on its tasks, so keep it off the caller's thread.
        await Task.Run(handle.Dispose).ConfigureAwait(false);
    }
}
