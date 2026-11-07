using Felix.Native;

namespace Felix;

public sealed partial class FelixClient
{
    /// <summary>
    /// Publishes one record with a key and headers. Returns its log offset, or null when the stream
    /// has no log or <paramref name="ack"/> is <see cref="AckMode.None"/>.
    /// </summary>
    /// <remarks>
    /// The record's key does not route; it goes where an unkeyed <see cref="PublishAsync"/> sends it.
    /// A header name that is empty or starts with <c>felix.</c> fails with <see cref="FelixStatus.InvalidArgument"/>
    /// and sends nothing; a broker that did not accept record headers, or a client connected with
    /// <see cref="FelixClientOptions.DisableRecordHeaders"/>, fails with <see cref="FelixStatus.Unsupported"/>.
    /// </remarks>
    public Task<long?> PublishWithHeadersAsync(
        string tenant,
        string ns,
        string stream,
        HeaderedRecord record,
        AckMode ack = AckMode.PerMessage,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentNullException.ThrowIfNull(record);
        return Call<long?>(cancellationToken, (client, cancel, userData) =>
        {
            using var args = new RecordArgs([record]);
            unsafe
            {
                return NativeMethods.felix_client_publish_with_headers(
                    client, tenant, ns, stream, args.Records, (int)ack, cancel, &Callbacks.OnPublish, userData);
            }
        });
    }

    /// <summary>
    /// Publishes a batch of records with keys and headers to shard 0's owner as one frame. Returns each
    /// record's offset, in order, null where the broker reported none. Errors are those of
    /// <see cref="PublishWithHeadersAsync"/>; a refused header fails the whole batch before anything is sent.
    /// An empty batch throws <see cref="ArgumentException"/>, as the C call refuses a count of zero.
    /// </summary>
    public Task<long?[]> PublishBatchWithHeadersAsync(
        string tenant,
        string ns,
        string stream,
        IReadOnlyList<HeaderedRecord> records,
        AckMode ack = AckMode.PerBatch,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
        {
            throw new ArgumentException("a batch needs at least one record", nameof(records));
        }

        return Call<long?[]>(cancellationToken, (client, cancel, userData) =>
        {
            using var args = new RecordArgs(records);
            unsafe
            {
                return NativeMethods.felix_client_publish_batch_with_headers(
                    client, tenant, ns, stream, args.Records, (nuint)args.Count, (int)ack, cancel, &Callbacks.OnBatch, userData);
            }
        });
    }

    /// <summary>
    /// A client that acts as <paramref name="tenant"/> with <paramref name="token"/> over this client's
    /// connections, for a gateway serving many principals from one pool. Its requests are checked against
    /// the token's grants alone. Dispose it like any client; it shares this client's runtime and
    /// connections, so disposing this one first does not stop it.
    /// </summary>
    /// <remarks>
    /// The token is fixed for the new client's life; use the overload that takes a provider for one that
    /// expires. A <see cref="LeaseToken"/> from <see cref="FelixCache.PutIfAbsentLeaseAsync"/> is a valid
    /// token. The new client keeps this client's <see cref="FelixClientOptions.TokenProvider"/> running
    /// until it and everything made from it are disposed. Keep it alive while subscriptions made from it
    /// are in use. A refused token fails with <see cref="FelixStatus.Auth"/>.
    /// </remarks>
    public async Task<FelixClient> WithIdentityAsync(string tenant, string token, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(token);
        // Taken before the call, so disposing this client meanwhile cannot cancel a fetch the new one needs.
        var held = ShareTokens();
        try
        {
            var native = await StartIdentity(tenant, token, cancellationToken).ConfigureAwait(false);
            return new FelixClient(native, held);
        }
        catch
        {
            held?.ReleaseHolder();
            throw;
        }
    }

    /// <summary>
    /// As <see cref="WithIdentityAsync(string, string, CancellationToken)"/>, for a token that expires.
    /// <paramref name="tokenProvider"/> behaves as <see cref="FelixClientOptions.TokenProvider"/> does, and
    /// runs until the new client and everything made from it are disposed.
    /// </summary>
    /// <param name="initialToken">The first token, or null to call the provider before the first request.</param>
    public async Task<FelixClient> WithIdentityAsync(
        string tenant,
        Func<CancellationToken, ValueTask<string>> tokenProvider,
        string? initialToken = null,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        ArgumentNullException.ThrowIfNull(tokenProvider);
        Arg.OptionalText(initialToken);
        var source = new TokenSource(tokenProvider);
        try
        {
            var native = await StartIdentityProvider(tenant, initialToken, source, cancellationToken).ConfigureAwait(false);
            return new FelixClient(native, source);
        }
        catch
        {
            // No client will be handed out, so a provider still running has no one to serve.
            source.Cancel();
            throw;
        }
    }

    /// <summary>A reference on this client's token source for a client that outlives it; null without a provider.</summary>
    internal TokenSource? ShareTokens() => tokens?.Retain() == true ? tokens : null;

    private unsafe Task<FelixClientHandle> StartIdentity(string tenant, string token, CancellationToken cancellationToken) =>
        Call<FelixClientHandle>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_with_identity(client, tenant, token, cancel, &Callbacks.OnConnect, userData));

    private unsafe Task<FelixClientHandle> StartIdentityProvider(
        string tenant, string? initialToken, TokenSource source, CancellationToken cancellationToken) =>
        Call<FelixClientHandle>(cancellationToken, (client, cancel, userData) =>
        {
            int status;
            try
            {
                status = NativeMethods.felix_client_with_identity_provider(
                    client,
                    tenant,
                    initialToken,
                    (nint)(delegate* unmanaged[Cdecl]<nint, nint, void>)&Callbacks.OnTokenFetch,
                    source.Pin(),
                    (nint)(delegate* unmanaged[Cdecl]<nint, void>)&Callbacks.OnTokenRelease,
                    cancel,
                    &Callbacks.OnConnect,
                    userData);
            }
            catch
            {
                source.Abandon();
                throw;
            }

            if (status != 0)
            {
                // Not OK: the library did not keep the source, so token_release never runs.
                source.Abandon();
            }

            return status;
        });

    /// <summary>
    /// Reads one page of a durable stream shard's committed records, from <paramref name="from"/> and stopping
    /// before <paramref name="end"/>, without subscribing. Read the next page from <see cref="StreamPage.NextOffset"/>.
    /// </summary>
    /// <param name="maxRecords">Zero takes the broker's cap; the broker also caps a page's size.</param>
    /// <param name="end">Null may run to the committed tail. A page that reaches what is committed comes back short or empty.</param>
    /// <remarks>
    /// A <paramref name="from"/> below what retention kept, or past the tail, fails with
    /// <see cref="FelixStatus.Cursor"/>. Needs the stream's subscribe permission. This is not
    /// <see cref="FelixSubscription.ReadAsync"/>, which waits for the next live event.
    /// </remarks>
    public unsafe Task<StreamPage> ReadRangeAsync(
        string tenant,
        string ns,
        string stream,
        uint shard,
        long from,
        long? end = null,
        uint maxRecords = 0,
        CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        ArgumentOutOfRangeException.ThrowIfNegative(from);
        if (end is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(end));
        }

        return Call<StreamPage>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_read(
                client, tenant, ns, stream, shard, (ulong)from,
                end is null ? (byte)0 : (byte)1, (ulong)(end ?? 0), maxRecords,
                cancel, &Callbacks.OnPage, userData));
    }

    /// <summary>
    /// Asks a broker which brokers a client may connect to and adds them to the endpoints this client tries
    /// (see <see cref="GetEndpointsAsync"/>). Returns how many the cluster named; zero is a legitimate
    /// answer for a broker with no cluster behind it. A broker that predates discovery fails the call.
    /// </summary>
    public unsafe Task<int> RefreshTopologyAsync(CancellationToken cancellationToken = default) =>
        Call<int>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_refresh_topology(client, cancel, &Callbacks.OnCount, userData));

    /// <summary>
    /// The live connections the client holds to each broker. A broker starts at one connection, shared
    /// by all traffic, and gains more only when its streams are saturated.
    /// </summary>
    public unsafe Task<NodeConnections[]> GetConnectionsPerNodeAsync(CancellationToken cancellationToken = default) =>
        Call<NodeConnections[]>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_connections_per_node(client, cancel, &Callbacks.OnConnections, userData));
}
