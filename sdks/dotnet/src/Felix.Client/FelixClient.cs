using System.Runtime.InteropServices;
using Felix.Native;

namespace Felix;

/// <summary>
/// A connection to a Felix cluster. Safe to use from several threads; disposing it
/// cancels its pending calls, and subscriptions made from it keep working until
/// they are disposed. Calls on a disposed client throw <see cref="ObjectDisposedException"/>; calls
/// pending when it is disposed fail with <see cref="OperationCanceledException"/>.
/// </summary>
public sealed partial class FelixClient : IAsyncDisposable, IDisposable
{
    private readonly FelixClientHandle handle;
    private readonly TokenSource? tokens;

    internal FelixClient(FelixClientHandle handle, TokenSource? tokens = null)
    {
        this.handle = handle;
        this.tokens = tokens;
    }

    /// <summary>Connects to a cluster.</summary>
    /// <exception cref="FelixException">The connect failed.</exception>
    /// <exception cref="OperationCanceledException">The token was cancelled; no client is handed out.</exception>
    public unsafe static Task<FelixClient> ConnectAsync(FelixClientOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Addrs);
        Arg.Text(options.TenantId, nameof(options.TenantId));
        Arg.OptionalText(options.Token, nameof(options.Token));
        if (string.IsNullOrEmpty(options.Token) && options.TokenProvider is null)
        {
            throw new ArgumentException("a token or a TokenProvider is required", nameof(options));
        }

        Arg.OptionalText(options.ServerName, nameof(options.ServerName));
        Arg.OptionalText(options.CaFile, nameof(options.CaFile));
        foreach (var addr in options.Addrs)
        {
            Arg.Text(addr, nameof(options.Addrs));
        }

        if (options.Addrs.Count == 0)
        {
            throw new ArgumentException("at least one broker address is required", nameof(options));
        }

        cancellationToken.ThrowIfCancellationRequested();
        var state = new CallState<FelixClientHandle>(cancellationToken);
        var tokens = options.TokenProvider is { } provider ? new TokenSource(provider) : null;
        int status;
        string? error;
        try
        {
            var userData = state.Pin();
            // The library reads the struct during the call; its size says which fields we know.
            NativeConnectOptions native = new()
            {
                Size = (nuint)sizeof(NativeConnectOptions),
                Publishers = options.Publishers ? (byte)1 : (byte)0,
                Timestamps = options.Timestamps ? (byte)1 : (byte)0,
            };
            if (tokens is not null)
            {
                native.TokenFetch = (nint)(delegate* unmanaged[Cdecl]<nint, nint, void>)&Callbacks.OnTokenFetch;
                native.TokenUserData = tokens.Pin();
                native.TokenRelease = (nint)(delegate* unmanaged[Cdecl]<nint, void>)&Callbacks.OnTokenRelease;
            }

            status = NativeMethods.felix_client_connect_with(
                string.Join(',', options.Addrs),
                options.TenantId,
                options.Token,
                options.ServerName,
                options.CaFile,
                options.Publishers || options.Timestamps || tokens is not null ? &native : null,
                state.Cancel.Pointer,
                &Callbacks.OnConnect,
                userData);
            error = status != 0 ? NativeMethods.LastError() : null;
        }
        catch
        {
            // The call did not start, so token_release will never run.
            tokens?.Abandon();
            state.Abandon();
            throw;
        }

        if (status != 0)
        {
            // Not OK: the library did not keep the token source, so token_release never runs.
            tokens?.Abandon();
            return Refused<FelixClient>(state, status, error);
        }

        return ConnectedAsync(state, tokens);
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
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        cancellationToken.ThrowIfCancellationRequested();

        CallState<long?> state;
        int status;
        string? error;
        // The library copies the payload and key before the call returns.
        using (var pinnedPayload = payload.Pin())
        using (var pinnedKey = (key ?? default).Pin())
        using (var call = new HandleLease(handle))
        {
            byte empty = 0;
            var keyPointer = key is null ? null : key.Value.IsEmpty ? &empty : (byte*)pinnedKey.Pointer;
            state = new CallState<long?>(cancellationToken);
            try
            {
                var userData = state.Pin();
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
            catch
            {
                state.Abandon();
                throw;
            }
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
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
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

        CallState<long?[]> state;
        int status;
        string? error;
        using (var pinnedKey = (key ?? default).Pin())
        using (var call = new HandleLease(handle))
        fixed (byte* dataPointer = data)
        fixed (nuint* lengthPointer = lengths)
        {
            byte empty = 0;
            var keyPointer = key is null ? null : key.Value.IsEmpty ? &empty : (byte*)pinnedKey.Pointer;
            state = new CallState<long?[]>(cancellationToken);
            try
            {
                var userData = state.Pin();
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
            catch
            {
                state.Abandon();
                throw;
            }
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
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        cancellationToken.ThrowIfCancellationRequested();

        CallState<FelixSubscriptionHandle> state;
        int status;
        string? error;
        using (var call = new HandleLease(handle))
        {
            state = new CallState<FelixSubscriptionHandle>(cancellationToken);
            try
            {
                var userData = state.Pin();
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
            catch
            {
                state.Abandon();
                throw;
            }
        }

        return status != 0 ? Refused<FelixSubscription>(state, status, error) : SubscribedAsync(state);
    }

    /// <summary>Frees the client, cancelling its pending calls.</summary>
    public void Dispose()
    {
        tokens?.Cancel();
        handle.Dispose();
    }

    /// <summary>
    /// As <see cref="Dispose"/>, off the calling thread: freeing the last handle on the
    /// client's runtime can wait a few seconds for pending calls to report.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        tokens?.Cancel();
        return new(Task.Run(handle.Dispose));
    }

    private static async Task<FelixClient> ConnectedAsync(CallState<FelixClientHandle> state, TokenSource? tokens)
    {
        try
        {
            return new(await state.WaitAsync().ConfigureAwait(false), tokens);
        }
        catch
        {
            // No client will be handed out, so a provider still running has no one to serve.
            tokens?.Cancel();
            throw;
        }
    }

    private static async Task<FelixSubscription> SubscribedAsync(CallState<FelixSubscriptionHandle> state) =>
        new(await state.WaitAsync().ConfigureAwait(false));

    /// <summary>
    /// The call returned an error, so its callback never runs. The library's message
    /// is per thread, so the caller reads it right after the call.
    /// </summary>
    private static Task<T> Refused<T>(CallStateBase state, int status, string? message) =>
        NativeCall.Refused<T>(state, status, message);
}
