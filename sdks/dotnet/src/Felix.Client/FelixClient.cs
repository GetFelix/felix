using System.Runtime.InteropServices;
using Felix.Native;

namespace Felix;

/// <summary>
/// A connection to a Felix cluster. Safe to use from several threads; disposing it
/// cancels its pending calls, and subscriptions made from it keep working until
/// they are disposed.
/// </summary>
public sealed class FelixClient : IAsyncDisposable, IDisposable
{
    private readonly FelixClientHandle handle;

    private FelixClient(FelixClientHandle handle)
    {
        this.handle = handle;
    }

    /// <summary>Connects to a cluster.</summary>
    /// <exception cref="FelixException">The connect failed.</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    public unsafe static Task<FelixClient> ConnectAsync(FelixClientOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Addrs);
        ArgumentNullException.ThrowIfNull(options.TenantId);
        ArgumentNullException.ThrowIfNull(options.Token);
        if (options.Addrs.Count == 0)
        {
            throw new ArgumentException("at least one broker address is required", nameof(options));
        }

        cancellationToken.ThrowIfCancellationRequested();
        var state = new CallState<FelixClientHandle>(cancellationToken);
        var userData = state.Pin();
        var status = NativeMethods.felix_client_connect(
            string.Join(',', options.Addrs),
            options.TenantId,
            options.Token,
            options.ServerName,
            options.CaFile,
            state.Cancel.Pointer,
            &Callbacks.OnConnect,
            userData);
        var error = status != 0 ? NativeMethods.LastError() : null;
        if (status != 0)
        {
            return Refused<FelixClient>(state, status, error);
        }

        return ConnectedAsync(state);
    }

    /// <summary>
    /// Publishes one record. Returns its log offset, or null when the stream has no
    /// log, <paramref name="ack"/> is <see cref="AckMode.None"/>, or the broker acknowledged before writing.
    /// </summary>
    /// <param name="key">Routes the record: records sharing a key share a shard and stay in order. Null means shard 0.</param>
    public unsafe Task<long?> PublishAsync(
        string tenant,
        string ns,
        string stream,
        ReadOnlyMemory<byte> payload,
        ReadOnlyMemory<byte>? key = null,
        AckMode ack = AckMode.PerMessage,
        Delivery delivery = Delivery.AtMostOnce,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(ns);
        ArgumentNullException.ThrowIfNull(stream);
        cancellationToken.ThrowIfCancellationRequested();

        var state = new CallState<long?>(cancellationToken);
        var userData = state.Pin();
        int status;
        string? error;
        // The library copies the payload and key before the call returns.
        using (var pinnedPayload = payload.Pin())
        using (var pinnedKey = (key ?? default).Pin())
        using (var call = new HandleLease(handle))
        {
            byte empty = 0;
            var keyPointer = key is null ? null : key.Value.IsEmpty ? &empty : (byte*)pinnedKey.Pointer;
            status = NativeMethods.felix_client_publish(
                call.Pointer,
                tenant,
                ns,
                stream,
                (byte*)pinnedPayload.Pointer,
                (nuint)payload.Length,
                keyPointer,
                key is null ? 0 : (nuint)key.Value.Length,
                (int)ack,
                (int)delivery,
                state.Cancel.Pointer,
                &Callbacks.OnPublish,
                userData);
            error = status != 0 ? NativeMethods.LastError() : null;
        }

        return status != 0 ? Refused<long?>(state, status, error) : state.WaitAsync();
    }

    /// <summary>
    /// Publishes a batch to one shard as a unit. Returns each record's offset, in order,
    /// null where the broker reported none.
    /// </summary>
    /// <param name="key">Routes the batch; null means shard 0.</param>
    public unsafe Task<long?[]> PublishBatchAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<ReadOnlyMemory<byte>> payloads,
        ReadOnlyMemory<byte>? key = null,
        AckMode ack = AckMode.PerBatch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(ns);
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(payloads);
        if (payloads.Count == 0)
        {
            throw new ArgumentException("a batch needs at least one record", nameof(payloads));
        }

        cancellationToken.ThrowIfCancellationRequested();

        var lengths = new nuint[payloads.Count];
        long total = 0;
        for (var i = 0; i < lengths.Length; i++)
        {
            lengths[i] = (nuint)payloads[i].Length;
            total += payloads[i].Length;
        }

        var data = new byte[total];
        var at = 0;
        foreach (var payload in payloads)
        {
            payload.Span.CopyTo(data.AsSpan(at));
            at += payload.Length;
        }

        var state = new CallState<long?[]>(cancellationToken);
        var userData = state.Pin();
        int status;
        string? error;
        using (var pinnedKey = (key ?? default).Pin())
        using (var call = new HandleLease(handle))
        fixed (byte* dataPointer = data)
        fixed (nuint* lengthPointer = lengths)
        {
            byte empty = 0;
            var keyPointer = key is null ? null : key.Value.IsEmpty ? &empty : (byte*)pinnedKey.Pointer;
            status = NativeMethods.felix_client_publish_batch(
                call.Pointer,
                tenant,
                ns,
                stream,
                dataPointer,
                lengthPointer,
                (nuint)lengths.Length,
                keyPointer,
                key is null ? 0 : (nuint)key.Value.Length,
                (int)ack,
                state.Cancel.Pointer,
                &Callbacks.OnBatch,
                userData);
            error = status != 0 ? NativeMethods.LastError() : null;
        }

        return status != 0 ? Refused<long?[]>(state, status, error) : state.WaitAsync();
    }

    /// <summary>
    /// Subscribes to one shard of a stream. Subscribe before publishing if the
    /// subscription must not miss the publish.
    /// </summary>
    public unsafe Task<FelixSubscription> SubscribeAsync(
        string tenant,
        string ns,
        string stream,
        uint shard,
        StartPosition start,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tenant);
        ArgumentNullException.ThrowIfNull(ns);
        ArgumentNullException.ThrowIfNull(stream);
        cancellationToken.ThrowIfCancellationRequested();

        var state = new CallState<FelixSubscriptionHandle>(cancellationToken);
        var userData = state.Pin();
        int status;
        string? error;
        using (var call = new HandleLease(handle))
        {
            status = NativeMethods.felix_client_subscribe(
                call.Pointer,
                tenant,
                ns,
                stream,
                shard,
                start.Kind,
                start.Offset,
                state.Cancel.Pointer,
                &Callbacks.OnSubscribe,
                userData);
            error = status != 0 ? NativeMethods.LastError() : null;
        }

        return status != 0 ? Refused<FelixSubscription>(state, status, error) : SubscribedAsync(state);
    }

    /// <summary>Frees the client, cancelling its pending calls.</summary>
    public void Dispose() => handle.Dispose();

    /// <summary>
    /// As <see cref="Dispose"/>, off the calling thread: freeing the last handle on the
    /// client's runtime can wait a few seconds for pending calls to report.
    /// </summary>
    public ValueTask DisposeAsync() => new(Task.Run(handle.Dispose));

    private static async Task<FelixClient> ConnectedAsync(CallState<FelixClientHandle> state) =>
        new(await state.WaitAsync().ConfigureAwait(false));

    private static async Task<FelixSubscription> SubscribedAsync(CallState<FelixSubscriptionHandle> state) =>
        new(await state.WaitAsync().ConfigureAwait(false));

    /// <summary>
    /// The call returned an error, so its callback never runs. The library's message
    /// is per thread, so the caller reads it right after the call.
    /// </summary>
    private static Task<T> Refused<T>(CallStateBase state, int status, string? message)
    {
        state.Abandon();
        return Task.FromException<T>(FelixException.FromStatus(status, message));
    }
}
