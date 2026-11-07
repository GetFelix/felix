using System.Text;

namespace Felix;

/// <summary>One record header: a name and a value.</summary>
/// <param name="Name">
/// UTF-8, not empty, and not starting with <c>felix.</c>, which the broker keeps for itself.
/// A name may repeat within a record.
/// </param>
/// <param name="Value">The bytes of the value; may be empty.</param>
public sealed record Header(string Name, ReadOnlyMemory<byte> Value);

/// <summary>A record to publish with a key and headers.</summary>
/// <param name="Payload">The record's bytes.</param>
/// <param name="Key">
/// The record's own key, stored with it and returned as <see cref="FelixEvent.Key"/>. It does not route:
/// the record goes where an unkeyed publish goes. Null or empty is no key.
/// </param>
/// <param name="Headers">The headers, in the order they are kept. Null is none. A record with neither a key nor headers is published plain.</param>
public sealed record HeaderedRecord(
    ReadOnlyMemory<byte> Payload,
    ReadOnlyMemory<byte>? Key = null,
    IReadOnlyList<Header>? Headers = null);

/// <summary>A lease token, granting what the caller's lease templates name for the key. A bearer secret.</summary>
/// <param name="Value">The token; present it to <see cref="FelixClient.WithIdentityAsync"/>.</param>
/// <param name="ExpiresAtSeconds">When it stops working, in seconds since the Unix epoch.</param>
public sealed record LeaseToken(string Value, long ExpiresAtSeconds)
{
    /// <summary><see cref="ExpiresAtSeconds"/> as a time; null when out of range.</summary>
    public DateTimeOffset? ExpiresAt =>
        ExpiresAtSeconds is >= 0 and <= 253_402_300_799 ? DateTimeOffset.FromUnixTimeSeconds(ExpiresAtSeconds) : null;

    // The default would print the secret into logs.
    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append("Value = <redacted>, ExpiresAtSeconds = ").Append(ExpiresAtSeconds);
        return true;
    }
}

/// <summary>The answer to a conditional write that asked for a lease token.</summary>
/// <param name="Result">What the conditional write did.</param>
/// <param name="Token">Set when the write applied and the broker minted a token.</param>
/// <param name="TokenRefused">Why an applied write came back without a token. The write stands.</param>
public sealed record LeaseWrite(ConditionalResult Result, LeaseToken? Token, string? TokenRefused);

/// <summary>One committed record of a stream shard, from <see cref="FelixClient.ReadRangeAsync"/>.</summary>
/// <param name="Offset">The record's log offset.</param>
/// <param name="Payload">The record's bytes as published.</param>
/// <param name="Publisher">The principal that published it, when the broker recorded one.</param>
/// <param name="TimestampMicros">When the broker appended it, in microseconds since the Unix epoch.</param>
public sealed record StreamRecord(long Offset, ReadOnlyMemory<byte> Payload, string? Publisher, long TimestampMicros)
{
    /// <summary><see cref="TimestampMicros"/> as a time; null when out of range.</summary>
    public DateTimeOffset? Timestamp => Time.FromMicros(TimestampMicros);
}

/// <summary>One page read from a stream shard.</summary>
/// <param name="Records">The committed records, in offset order.</param>
/// <param name="NextOffset">
/// Where the next page starts. Offsets that hold no record are passed over, so it is not always the last offset plus one.
/// </param>
public sealed record StreamPage(IReadOnlyList<StreamRecord> Records, long NextOffset);

/// <summary>The live connections a client holds to one broker.</summary>
/// <param name="Addr">The broker's <c>host:port</c>.</param>
/// <param name="Connections">How many connections are open to it.</param>
public readonly record struct NodeConnections(string Addr, int Connections);
