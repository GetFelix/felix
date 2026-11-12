namespace Felix;

/// <summary>How a publish is acknowledged.</summary>
public enum AckMode
{
    /// <summary>No acknowledgement and no offset.</summary>
    None = 0,
    /// <summary>The broker acknowledges each record.</summary>
    PerMessage = 1,
    /// <summary>The broker acknowledges the batch.</summary>
    PerBatch = 2,
}

/// <summary>What a publish does when its outcome is in doubt.</summary>
public enum Delivery
{
    /// <summary>A record whose outcome is unknown is not sent again.</summary>
    AtMostOnce = 0,
    /// <summary>A record whose outcome is unknown is sent again, which can write it twice.</summary>
    AtLeastOnce = 1,
}

/// <summary>Why a subscription ended, or that it has not.</summary>
public enum CloseReason
{
    /// <summary>The subscription is live.</summary>
    Open = 0,
    /// <summary>The broker ended the stream.</summary>
    Ended = 1,
    /// <summary>The stream was deleted.</summary>
    StreamDeleted = 2,
}

/// <summary>What a subscription does when its client-side queue is full.</summary>
public enum ClientSubQueuePolicy
{
    /// <summary>Stop reading the event stream until the application catches up.</summary>
    Block = 1,
    /// <summary>Drop the arriving item. The default.</summary>
    DropNew = 2,
    /// <summary>Meant to drop the oldest queued item. It currently drops the arriving one, exactly as <see cref="DropNew"/> does.</summary>
    DropOld = 3,
}

/// <summary>How publishes are spread across the publish streams.</summary>
public enum PublishSharding
{
    /// <summary>
    /// Pick the stream by hashing the stream's name, so each stream's publishes share one writer and stay
    /// in order. The default.
    /// </summary>
    HashStream = 1,
    /// <summary>Rotate through the streams. Publishes to one stream can reach the broker out of order.</summary>
    RoundRobin = 2,
}

/// <summary>Where a subscription starts reading.</summary>
public readonly record struct StartPosition
{
    private StartPosition(int kind, ulong offset)
    {
        Kind = kind;
        Offset = offset;
    }

    /// <summary>Only records published after the subscribe.</summary>
    public static StartPosition Latest => new(0, 0);

    /// <summary>From the oldest retained record.</summary>
    public static StartPosition Earliest => new(1, 0);

    /// <summary>From a log offset.</summary>
    public static StartPosition FromOffset(ulong offset) => new(2, offset);

    internal int Kind { get; }

    internal ulong Offset { get; }
}

/// <summary>What <see cref="FelixClient.ConnectAsync"/> needs.</summary>
public sealed class FelixClientOptions
{
    /// <summary>Broker addresses as <c>host:port</c>; any reachable one is enough.</summary>
    public required IReadOnlyList<string> Addrs { get; init; }

    /// <summary>The tenant to authenticate as.</summary>
    public required string TenantId { get; init; }

    /// <summary>
    /// The tenant's token. Optional when <see cref="TokenProvider"/> is set: leave it null (or
    /// empty) and the first connect calls the provider. With a provider this is only the first
    /// token. Without one it is the only token, and the client cannot refresh it.
    /// </summary>
    public string? Token { get; init; }

    /// <summary>
    /// Returns a fresh Felix token, usually from a closure over the refresh token. The client
    /// calls it when a stream is about to authenticate and it holds no token, the held one is
    /// past two thirds of its life, or the broker just refused it; an idle client makes no
    /// calls. It is never called concurrently, and a failure (an exception, a null or empty
    /// result, or no answer within 30 seconds) keeps a still-valid token or fails the operation
    /// with a <see cref="FelixException"/> of status <see cref="FelixStatus.Auth"/> carrying the
    /// exception's message. The <see cref="CancellationToken"/> fires after 30 seconds, when the
    /// connect fails, or once the client and every subscription, sharded subscription, sharded
    /// group, cache watch and producer made from it are disposed. At least one of
    /// <see cref="Token"/> and this is required.
    /// </summary>
    public Func<CancellationToken, ValueTask<string>>? TokenProvider { get; init; }

    /// <summary>The name the broker certificate must carry; <c>localhost</c> when null.</summary>
    public string? ServerName { get; init; }

    /// <summary>A PEM file of CA certificates to trust; the system store when null.</summary>
    public string? CaFile { get; init; }

    /// <summary>
    /// A PEM file holding the client certificate (leaf first, then any intermediates) to present
    /// to a broker started with <c>FELIX_TLS_CLIENT_CA</c>, which refuses a client without one.
    /// Needs <see cref="ClientKeyFile"/>; a missing or malformed file fails the connect with
    /// <see cref="FelixStatus.InvalidArgument"/>.
    /// </summary>
    public string? ClientCertFile { get; init; }

    /// <summary>A PEM file holding the private key of <see cref="ClientCertFile"/>: PKCS#8, PKCS#1 or SEC1.</summary>
    public string? ClientKeyFile { get; init; }

    /// <summary>
    /// Ask brokers to say who published each record, for <see cref="FelixEvent.Publisher"/>
    /// and <see cref="GroupRecord.Publisher"/>. Adds the principal to every event batch.
    /// A broker that does not record it leaves it null.
    /// </summary>
    public bool Publishers { get; init; }

    /// <summary>
    /// Ask brokers for each record's append time, for <see cref="FelixEvent.Timestamp"/>
    /// and <see cref="GroupRecord.Timestamp"/>. Adds eight bytes per event.
    /// </summary>
    public bool Timestamps { get; init; }

    /// <summary>
    /// A ceiling on one retried call, across every attempt and every wait between them. Null,
    /// the default, is no ceiling. A deadline shorter than one attempt's own timeout leaves the
    /// call a single attempt, since that attempt spends the whole budget. Whole milliseconds,
    /// at least one.
    /// </summary>
    public TimeSpan? ReconnectDeadline { get; init; }

    /// <summary>
    /// How many times a retried call tries to reach a broker before it gives up; each attempt
    /// tries every known endpoint. Null is the default, 5. At least 1.
    /// </summary>
    public int? ReconnectAttempts { get; init; }

    /// <summary>
    /// The ceiling of the first wait between attempts. It doubles each attempt up to
    /// <see cref="ReconnectMaxBackoff"/>, and each wait is a random fraction of its ceiling.
    /// Null is the default, 200 ms. Whole milliseconds, at least one.
    /// </summary>
    public TimeSpan? ReconnectBackoff { get; init; }

    /// <summary>
    /// The longest any one wait can be. Null is the default, 2 s. Whole milliseconds, at least
    /// one. Every wait is capped at it, even when the backoff is larger.
    /// </summary>
    public TimeSpan? ReconnectMaxBackoff { get; init; }

    /// <summary>
    /// Do not offer record headers to brokers. Off, the default, offers them, so
    /// <see cref="FelixEvent.Headers"/> is filled and <see cref="FelixClient.PublishWithHeadersAsync"/> works against a
    /// broker that takes them. On, a publish with headers fails with <see cref="FelixStatus.Unsupported"/>
    /// and events carry none.
    /// </summary>
    public bool DisableRecordHeaders { get; init; }

    /// <summary>
    /// What a subscription does when its client-side queue is full. Null is the default,
    /// <see cref="Felix.ClientSubQueuePolicy.DropNew"/>.
    /// </summary>
    public ClientSubQueuePolicy? ClientSubQueuePolicy { get; init; }

    /// <summary>Events each subscription's client-side queue holds. Null is the default, 256. At least 1.</summary>
    public int? ClientSubQueueCapacity { get; init; }

    /// <summary>
    /// Batches the broker's queue for each subscription should hold, instead of the stream's default. The
    /// broker clamps it to its maximum; <see cref="FelixSubscription.QueueCapacity"/> reports what was
    /// granted. When the queue is full the stream's policy applies, which by default drops. At least 1.
    /// </summary>
    public int? BrokerSubQueueCapacity { get; init; }

    /// <summary>Publish connections per client. Null is the default, 4. At least 1.</summary>
    public int? PublishConnPool { get; init; }

    /// <summary>Publish streams per publish connection. Null is the default, 2. At least 1.</summary>
    public int? PublishStreamsPerConn { get; init; }

    /// <summary>Publishes queued per publish stream before the next waits for room. Null is the default, 64. At least 1.</summary>
    public int? PublishQueueDepth { get; init; }

    /// <summary>Bytes of publishes sent but not yet answered, across the client. Null is the default, 4 MiB. At least 1.</summary>
    public long? PublishInflightBytes { get; init; }

    /// <summary>
    /// How publishes are spread across the publish streams. Null is the default,
    /// <see cref="Felix.PublishSharding.HashStream"/>, which keeps a stream's publishes in order.
    /// </summary>
    public PublishSharding? PublishSharding { get; init; }

    /// <summary>
    /// Most publish streams opened per broker for one shard each, beside the pool. Null is the default, 4;
    /// 0 puts every publish on the pool.
    /// </summary>
    public int? PublishShardStreams { get; init; }

    /// <summary>Cache connections per client. Null is the default, 8. At least 1.</summary>
    public int? CacheConnPool { get; init; }

    /// <summary>Cache streams per cache connection. Null is the default, 4. At least 1.</summary>
    public int? CacheStreamsPerConn { get; init; }

    /// <summary>Connections that carry subscriptions and cache watches. Null is the default, 8. At least 1.</summary>
    public int? EventConnPool { get; init; }

    /// <summary>Most connections opened to one broker. Null is the default, 8. At least 1.</summary>
    public int? ClusterConnPool { get; init; }

    /// <summary>Streams one connection carries before another is opened beside it. Null is the default, 1024. At least 1.</summary>
    public int? ClusterStreamsPerConn { get; init; }

    /// <summary>QUIC receive window per event connection, in bytes. Null is the default, 256 MiB. Between 1 and 2^62 - 1, as are the five windows below.</summary>
    public long? EventConnRecvWindow { get; init; }

    /// <summary>QUIC receive window per event stream, in bytes. Null is the default, 64 MiB.</summary>
    public long? EventStreamRecvWindow { get; init; }

    /// <summary>QUIC send window per event connection, in bytes. Null is the default, 256 MiB.</summary>
    public long? EventSendWindow { get; init; }

    /// <summary>QUIC receive window per cache connection, in bytes. Null is the default, 256 MiB.</summary>
    public long? CacheConnRecvWindow { get; init; }

    /// <summary>QUIC receive window per cache stream, in bytes. Null is the default, 64 MiB.</summary>
    public long? CacheStreamRecvWindow { get; init; }

    /// <summary>QUIC send window per cache connection, in bytes. Null is the default, 256 MiB.</summary>
    public long? CacheSendWindow { get; init; }

    /// <summary>
    /// Registrations and unclaimed event streams one event connection holds before refusing more. Null is
    /// the default, 16384. At least 1.
    /// </summary>
    public int? EventRouterMaxPending { get; init; }

    /// <summary>Largest frame the client will read; a bigger one fails the stream. Null is the default, 16 MiB. At least 1.</summary>
    public long? MaxFrameBytes { get; init; }

    /// <summary>
    /// Ask brokers to answer acknowledged publishes after the write, with the offset, as the broker-wide
    /// <c>FELIX_ACK_ON_COMMIT=true</c> does. Slower per publish. <see cref="FelixSupports.AckOnCommit"/>
    /// says whether a broker honours it.
    /// </summary>
    public bool AckOnCommit { get; init; }

    /// <summary>
    /// Longest a connect to one broker may take, handshake included, before it fails. Null is the default,
    /// 10 s. Whole milliseconds, between one and a year.
    /// </summary>
    public TimeSpan? ConnectTimeout { get; init; }

    /// <summary>
    /// Longest closing a producer waits for queued publishes to be written and acknowledged before it
    /// aborts. Null is the default, 60 s. Whole milliseconds, between one and a year.
    /// </summary>
    public TimeSpan? FinishTimeout { get; init; }

    /// <summary>Whether any queue or transport setting is set, so the connect passes the options struct.</summary>
    internal bool HasTuning =>
        ClientSubQueuePolicy is not null || ClientSubQueueCapacity is not null || BrokerSubQueueCapacity is not null
        || PublishConnPool is not null || PublishStreamsPerConn is not null || PublishQueueDepth is not null
        || PublishInflightBytes is not null || PublishSharding is not null || PublishShardStreams is not null
        || CacheConnPool is not null || CacheStreamsPerConn is not null || EventConnPool is not null
        || ClusterConnPool is not null || ClusterStreamsPerConn is not null
        || EventConnRecvWindow is not null || EventStreamRecvWindow is not null || EventSendWindow is not null
        || CacheConnRecvWindow is not null || CacheStreamRecvWindow is not null || CacheSendWindow is not null
        || EventRouterMaxPending is not null || MaxFrameBytes is not null || AckOnCommit
        || ConnectTimeout is not null || FinishTimeout is not null;

    /// <summary>Refuses a tuning value the library would refuse, before anything is started.</summary>
    internal void ValidateTuning()
    {
        static void AtLeastOne(long? value, string name)
        {
            if (value is < 1)
            {
                throw new ArgumentOutOfRangeException("options", $"{name} must be at least 1");
            }
        }

        static void Window(long? value, string name)
        {
            // QUIC windows are variable-length integers, which top out below 2^62.
            if (value is < 1 || value > (1L << 62) - 1)
            {
                throw new ArgumentOutOfRangeException("options", $"{name} must be between 1 and 2^62 - 1");
            }
        }

        static void Timeout(TimeSpan? value, string name)
        {
            if (value is { } span && (span < TimeSpan.FromMilliseconds(1) || span > TimeSpan.FromDays(365)))
            {
                throw new ArgumentOutOfRangeException("options", $"{name} must be between 1 millisecond and one year");
            }
        }

        AtLeastOne(ClientSubQueueCapacity, nameof(ClientSubQueueCapacity));
        AtLeastOne(BrokerSubQueueCapacity, nameof(BrokerSubQueueCapacity));
        AtLeastOne(PublishConnPool, nameof(PublishConnPool));
        AtLeastOne(PublishStreamsPerConn, nameof(PublishStreamsPerConn));
        AtLeastOne(PublishQueueDepth, nameof(PublishQueueDepth));
        AtLeastOne(PublishInflightBytes, nameof(PublishInflightBytes));
        if (PublishShardStreams is < 0)
        {
            throw new ArgumentOutOfRangeException("options", "PublishShardStreams must be at least 0");
        }

        AtLeastOne(CacheConnPool, nameof(CacheConnPool));
        AtLeastOne(CacheStreamsPerConn, nameof(CacheStreamsPerConn));
        AtLeastOne(EventConnPool, nameof(EventConnPool));
        AtLeastOne(ClusterConnPool, nameof(ClusterConnPool));
        AtLeastOne(ClusterStreamsPerConn, nameof(ClusterStreamsPerConn));
        AtLeastOne(EventRouterMaxPending, nameof(EventRouterMaxPending));
        AtLeastOne(MaxFrameBytes, nameof(MaxFrameBytes));
        Window(EventConnRecvWindow, nameof(EventConnRecvWindow));
        Window(EventStreamRecvWindow, nameof(EventStreamRecvWindow));
        Window(EventSendWindow, nameof(EventSendWindow));
        Window(CacheConnRecvWindow, nameof(CacheConnRecvWindow));
        Window(CacheStreamRecvWindow, nameof(CacheStreamRecvWindow));
        Window(CacheSendWindow, nameof(CacheSendWindow));
        Timeout(ConnectTimeout, nameof(ConnectTimeout));
        Timeout(FinishTimeout, nameof(FinishTimeout));
        if (ClientSubQueuePolicy is { } policy && !Enum.IsDefined(policy))
        {
            throw new ArgumentOutOfRangeException("options", $"ClientSubQueuePolicy {policy} is not a defined value");
        }

        if (PublishSharding is { } sharding && !Enum.IsDefined(sharding))
        {
            throw new ArgumentOutOfRangeException("options", $"PublishSharding {sharding} is not a defined value");
        }
    }
}

/// <summary>One record delivered to a subscription, copied out of the native event.</summary>
/// <param name="Payload">The record's bytes as published.</param>
/// <param name="Offset">The log offset, or null on a stream without a log.</param>
/// <param name="SkippedBefore">
/// How many offsets just below this one hold no event. A larger jump than this
/// explains is a drop.
/// </param>
/// <param name="Publisher">
/// The principal that published the record. Null unless the client was connected with
/// <see cref="FelixClientOptions.Publishers"/> and the broker recorded one.
/// </param>
/// <param name="TimestampMicros">
/// When the broker appended the record, in microseconds since the Unix epoch by the clock of
/// the broker that led the shard. Null unless the client was connected with
/// <see cref="FelixClientOptions.Timestamps"/> and the broker supplied one.
/// </param>
/// <param name="Key">
/// The key the publisher stored with the record (see <see cref="HeaderedRecord.Key"/>). Null when it
/// set none, and unless the broker took record headers.
/// </param>
/// <param name="Headers">The headers the publisher gave the record, in order; null or empty for a plain record.</param>
public sealed record FelixEvent(
    ReadOnlyMemory<byte> Payload,
    long? Offset,
    long SkippedBefore,
    string? Publisher = null,
    long? TimestampMicros = null,
    ReadOnlyMemory<byte>? Key = null,
    IReadOnlyList<Header>? Headers = null)
{
    /// <summary><see cref="TimestampMicros"/> as a time; null when it is absent or out of range.</summary>
    public DateTimeOffset? Timestamp => Time.FromMicros(TimestampMicros);
}

internal static class Time
{
    internal static DateTimeOffset? FromMicros(long? micros)
    {
        // DateTimeOffset ends in year 9999; a broker clock that far out is not a time.
        if (micros is not { } value || value < 0 || value / 1_000_000 > 253_402_300_799)
        {
            return null;
        }

        return DateTimeOffset.UnixEpoch.AddTicks(value * 10);
    }
}
