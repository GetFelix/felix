namespace Felix;

/// <summary>Lays payloads end to end, the shape the library's batch calls take.</summary>
internal static class Payloads
{
    internal static (byte[] Data, nuint[] Lengths) Flatten(IReadOnlyList<ReadOnlyMemory<byte>> payloads)
    {
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

        return (data, lengths);
    }
}
