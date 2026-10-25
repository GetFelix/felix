using Felix.Native;

namespace Felix;

/// <summary>
/// A consumer group on one shard of a stream: poll for records, settle each one, and manage
/// the group. Cheap to make and holds no native state, so it is as live as its client;
/// calls after the client is disposed throw <see cref="ObjectDisposedException"/>.
/// Polling and settling need the consume permission; creating, seeking, deleting, discarding
/// and redriving need the group-management permission.
/// </summary>
public sealed class FelixGroup
{
    private readonly FelixClient client;

    internal FelixGroup(FelixClient client, string tenant, string ns, string stream, uint shard, string group, string? consumer)
    {
        Arg.Text(tenant);
        Arg.Text(ns);
        Arg.Text(stream);
        Arg.Text(group);
        Arg.OptionalText(consumer);
        this.client = client;
        Tenant = tenant;
        Namespace = ns;
        Stream = stream;
        Shard = shard;
        Name = group;
        Consumer = consumer;
    }

    /// <summary>The tenant the group belongs to.</summary>
    public string Tenant { get; }

    /// <summary>The stream's namespace.</summary>
    public string Namespace { get; }

    /// <summary>The stream.</summary>
    public string Stream { get; }

    /// <summary>The shard this group object works on.</summary>
    public uint Shard { get; }

    /// <summary>The group's name.</summary>
    public string Name { get; }

    /// <summary>The polling member's name (1 to 128 bytes, stable across restarts), or null for an unnamed one.</summary>
    public string? Consumer { get; }

    /// <summary>
    /// Takes up to <paramref name="maxRecords"/> records. An empty result is an answer
    /// (nothing was owed), not an error. A record neither settled nor handed back is
    /// redelivered after its claim lapses.
    /// </summary>
    /// <param name="maxRecords">At least one.</param>
    /// <param name="wait">How long the broker may hold the poll open for work; the broker caps it. Zero returns at once.</param>
    /// <param name="visibility">How long the claims stand before the records are owed again; null or <see cref="TimeSpan.Zero"/> is the broker's own timeout (the C ABI's zero), and a broker that cannot honour another value refuses it.</param>
    /// <param name="reclaim">Take back what this <see cref="Consumer"/> holds from older connections; needs a consumer name.</param>
    /// <exception cref="FelixException">With <see cref="FelixStatus.InvalidArgument"/> for a zero count or a reclaim without a consumer.</exception>
    public async Task<GroupRecord[]> PollAsync(
        uint maxRecords,
        TimeSpan wait = default,
        TimeSpan? visibility = null,
        bool reclaim = false,
        CancellationToken cancellationToken = default)
    {
        var waitMs = Millis(wait, nameof(wait));
        var visibilityMs = visibility is { } v ? Millis(v, nameof(visibility)) : 0;
        var records = await StartPoll(maxRecords, waitMs, visibilityMs, reclaim, cancellationToken).ConfigureAwait(false);
        for (var i = 0; i < records.Length; i++)
        {
            records[i] = records[i] with { Shard = Shard };
        }

        return records;
    }

    private unsafe Task<GroupRecord[]> StartPoll(uint maxRecords, ulong waitMs, ulong visibilityMs, bool reclaim, CancellationToken cancellationToken) =>
        client.Call<GroupRecord[]>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_poll(
                handle, Tenant, Namespace, Stream, Shard, Name, maxRecords, waitMs, visibilityMs,
                Consumer, reclaim ? (byte)1 : (byte)0, cancel, &Callbacks.OnGroupPoll, userData));

    /// <summary>Finishes a record: everything below the group's cursor stays finished.</summary>
    /// <remarks>
    /// Every settle is a write. Cancelled after it may have been sent it fails with
    /// <see cref="FelixStatus.OutcomeUnknown"/>; settling a record again is refused or harmless,
    /// so look at the group and retry.
    /// </remarks>
    public Task AckAsync(GroupRecord record, CancellationToken cancellationToken = default) =>
        AckAsync(Own(record).Offset, cancellationToken);

    /// <inheritdoc cref="AckAsync(GroupRecord, CancellationToken)" />
    public unsafe Task AckAsync(long offset, CancellationToken cancellationToken = default) =>
        client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_ack(
                handle, Tenant, Namespace, Stream, Shard, Name, unchecked((ulong)offset), cancel, &Callbacks.OnDone, userData));

    /// <summary>
    /// Hands a record back for another attempt after <paramref name="delay"/> (zero is at once).
    /// It names the delivery by the record's attempt count, so it is refused once the claim has
    /// lapsed or the record was handed out again. A broker without claim control takes only a zero delay.
    /// </summary>
    public unsafe Task NackAsync(GroupRecord record, TimeSpan delay = default, CancellationToken cancellationToken = default)
    {
        Own(record);
        var delayMs = Millis(delay, nameof(delay));
        return client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_nack(
                handle, Tenant, Namespace, Stream, Shard, Name, unchecked((ulong)record.Offset), record.Attempts, delayMs,
                cancel, &Callbacks.OnDone, userData));
    }

    /// <summary>
    /// Lengthens a record's claim by <paramref name="extension"/> (at least a millisecond), for a consumer
    /// that needs longer than the visibility timeout. Refused once the claim has lapsed or the record
    /// was handed out again. Returns how long the claim now stands, by the broker's clock.
    /// </summary>
    public unsafe Task<TimeSpan> ExtendAsync(GroupRecord record, TimeSpan extension, CancellationToken cancellationToken = default)
    {
        Own(record);
        var extendMs = Millis(extension, nameof(extension));
        return client.Call<TimeSpan>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_extend(
                handle, Tenant, Namespace, Stream, Shard, Name, unchecked((ulong)record.Offset), record.Attempts, extendMs,
                cancel, &Callbacks.OnExtend, userData));
    }

    /// <summary>
    /// Gives up on a record: lists it as a dead letter of the group and finishes it.
    /// <see cref="RedriveAsync"/> puts it back. Refused for a record already finished.
    /// </summary>
    public Task DeadLetterAsync(GroupRecord record, CancellationToken cancellationToken = default) =>
        DeadLetterAsync(Own(record).Offset, cancellationToken);

    /// <inheritdoc cref="DeadLetterAsync(GroupRecord, CancellationToken)" />
    public unsafe Task DeadLetterAsync(long offset, CancellationToken cancellationToken = default) =>
        client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_dead_letter(
                handle, Tenant, Namespace, Stream, Shard, Name, unchecked((ulong)offset), cancel, &Callbacks.OnDone, userData));

    /// <summary>Drops a dead letter from the group's list without delivering it again.</summary>
    public unsafe Task DiscardAsync(long offset, CancellationToken cancellationToken = default) =>
        client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_discard(
                handle, Tenant, Namespace, Stream, Shard, Name, unchecked((ulong)offset), cancel, &Callbacks.OnDone, userData));

    /// <summary>Puts a dead letter back, to be delivered again by a later poll.</summary>
    public unsafe Task RedriveAsync(long offset, CancellationToken cancellationToken = default) =>
        client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_redrive(
                handle, Tenant, Namespace, Stream, Shard, Name, unchecked((ulong)offset), cancel, &Callbacks.OnDone, userData));

    /// <summary>The offsets of the group's dead letters on this shard, oldest first as the broker lists them.</summary>
    public unsafe Task<long[]> DeadLettersAsync(CancellationToken cancellationToken = default) =>
        client.Call<long[]>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_dead_letters(
                handle, Tenant, Namespace, Stream, Shard, Name, cancel, &Callbacks.OnOffsets, userData));

    /// <summary>
    /// Creates the group on this shard at <paramref name="start"/>, leaving it alone when it is
    /// already there: <see cref="GroupPosition.Moved"/> is then false.
    /// </summary>
    public unsafe Task<GroupPosition> CreateAsync(StartPosition start, CancellationToken cancellationToken = default) =>
        client.Call<GroupPosition>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_create(
                handle, Tenant, Namespace, Stream, Shard, Name, start.Kind, start.Offset, cancel, &Callbacks.OnPosition, userData));

    /// <summary>
    /// Moves the group's cursor on this shard to <paramref name="start"/>. Returns where it resumes.
    /// Safe to repeat.
    /// </summary>
    public unsafe Task<GroupPosition> SeekAsync(StartPosition start, CancellationToken cancellationToken = default) =>
        client.Call<GroupPosition>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_seek(
                handle, Tenant, Namespace, Stream, Shard, Name, start.Kind, start.Offset, cancel, &Callbacks.OnPosition, userData));

    /// <summary>Where the group stands on this shard.</summary>
    public unsafe Task<GroupInfo> DescribeAsync(CancellationToken cancellationToken = default) =>
        client.Call<GroupInfo>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_describe(
                handle, Tenant, Namespace, Stream, Shard, Name, cancel, &Callbacks.OnInfo, userData));

    /// <summary>Deletes the group on this shard, with its cursor and dead letters. Returns whether it existed.</summary>
    public unsafe Task<bool> DeleteAsync(CancellationToken cancellationToken = default) =>
        client.Call<bool>(cancellationToken, (handle, cancel, userData) =>
            NativeMethods.felix_client_group_delete(
                handle, Tenant, Namespace, Stream, Shard, Name, cancel, &Callbacks.OnDeleted, userData));

    private GroupRecord Own(GroupRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        if (record.Shard != Shard)
        {
            throw new ArgumentException($"the record came from shard {record.Shard}, not shard {Shard}", nameof(record));
        }

        return record;
    }

    internal static ulong Millis(TimeSpan span, string name)
    {
        if (span < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(name, "must not be negative");
        }

        return (ulong)Math.Ceiling(span.TotalMilliseconds);
    }
}
