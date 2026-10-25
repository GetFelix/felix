namespace Felix;

/// <summary>What a <see cref="CommitOp"/> does. The values are the C ABI's.</summary>
public enum CommitOpKind
{
    /// <summary>The commit's event, delivered to the stream's subscribers.</summary>
    Publish = 0,
    /// <summary>The commit's event, for the stream's consumer groups; the stream is the queue.</summary>
    Enqueue = 1,
    /// <summary>Sets a state key to a value.</summary>
    Put = 2,
    /// <summary>Removes a state key.</summary>
    Delete = 3,
}

/// <summary>
/// One operation of an atomic commit. A commit holds exactly one event
/// (<see cref="Publish"/> or <see cref="Enqueue"/>) and any number of state changes,
/// all on the same stream; the library refuses anything else before sending.
/// </summary>
public sealed class CommitOp
{
    private CommitOp(CommitOpKind kind, string stream, string? key, ReadOnlyMemory<byte> data)
    {
        Arg.Text(stream);
        Arg.OptionalText(key);
        Kind = kind;
        Stream = stream;
        Key = key;
        Data = data;
    }

    /// <summary>What the operation does.</summary>
    public CommitOpKind Kind { get; }

    /// <summary>The stream (or queue, for <see cref="CommitOpKind.Enqueue"/>) it is on.</summary>
    public string Stream { get; }

    /// <summary>The state key, for <see cref="CommitOpKind.Put"/> and <see cref="CommitOpKind.Delete"/>.</summary>
    public string? Key { get; }

    /// <summary>The event payload, or the state value.</summary>
    public ReadOnlyMemory<byte> Data { get; }

    /// <summary>The commit's event, delivered to <paramref name="stream"/>'s subscribers.</summary>
    public static CommitOp Publish(string stream, ReadOnlyMemory<byte> payload) =>
        new(CommitOpKind.Publish, stream, null, payload);

    /// <summary>The commit's event, for the consumer groups of <paramref name="queue"/>.</summary>
    public static CommitOp Enqueue(string queue, ReadOnlyMemory<byte> payload) =>
        new(CommitOpKind.Enqueue, queue, null, payload);

    /// <summary>Sets <paramref name="key"/> in <paramref name="stream"/>'s state to <paramref name="value"/>.</summary>
    public static CommitOp Put(string stream, string key, ReadOnlyMemory<byte> value)
    {
        Arg.Text(key);
        return new(CommitOpKind.Put, stream, key, value);
    }

    /// <summary>Removes <paramref name="key"/> from <paramref name="stream"/>'s state.</summary>
    public static CommitOp Delete(string stream, string key)
    {
        Arg.Text(key);
        return new(CommitOpKind.Delete, stream, key, default);
    }
}

/// <summary>The answer to a conditional commit or publish. A refusal is an answer, not an error.</summary>
/// <param name="Written">Whether the write happened.</param>
/// <param name="Offset">Where the write landed when written; otherwise the shard's next offset (its tail), and nothing was written.</param>
public readonly record struct ConditionalWrite(bool Written, long Offset);

/// <summary>A key in a stream shard's state, copied out of the native read.</summary>
/// <param name="Value">The value; null when the key was never written or was deleted, which is not the same as an empty value.</param>
/// <param name="Version">The offset of the commit that wrote the value.</param>
/// <param name="AsOf">The offset of the last commit the answer reflects.</param>
public sealed record StateValue(ReadOnlyMemory<byte>? Value, long? Version, long? AsOf);
