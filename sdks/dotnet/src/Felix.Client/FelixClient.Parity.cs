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
    /// The token is fixed for the new client's life; make another when it expires. A
    /// <see cref="LeaseToken"/> from <see cref="FelixCache.PutIfAbsentLeaseAsync"/> is a valid token. Keep the
    /// new client alive while subscriptions made from it are in use. A refused token fails with
    /// <see cref="FelixStatus.Auth"/>.
    /// </remarks>
    public async Task<FelixClient> WithIdentityAsync(string tenant, string token, CancellationToken cancellationToken = default)
    {
        Arg.Text(tenant);
        Arg.Text(token);
        var native = await StartIdentity(tenant, token, cancellationToken).ConfigureAwait(false);
        return new FelixClient(native);
    }

    private unsafe Task<FelixClientHandle> StartIdentity(string tenant, string token, CancellationToken cancellationToken) =>
        Call<FelixClientHandle>(cancellationToken, (client, cancel, userData) =>
            NativeMethods.felix_client_with_identity(client, tenant, token, cancel, &Callbacks.OnConnect, userData));

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
