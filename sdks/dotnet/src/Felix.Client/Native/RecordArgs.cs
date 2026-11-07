using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace Felix.Native;

/// <summary>
/// An array of <c>FelixRecord</c> built from managed records, with every pointer in it pinned or
/// in native memory until this is disposed. The library copies all of it before a call returns.
/// </summary>
internal sealed unsafe class RecordArgs : IDisposable
{
    private readonly List<MemoryHandle> pins = [];
    private readonly List<nint> blocks = [];

    internal RecordArgs(IReadOnlyList<HeaderedRecord> records)
    {
        try
        {
            Count = records.Count;
            Records = (NativeRecord*)Allocate(sizeof(NativeRecord) * Math.Max(1, Count));
            for (var i = 0; i < Count; i++)
            {
                Fill(Records + i, records[i]);
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    internal NativeRecord* Records { get; }

    internal int Count { get; }

    public void Dispose()
    {
        foreach (var pin in pins)
        {
            pin.Dispose();
        }

        foreach (var block in blocks)
        {
            Marshal.FreeHGlobal(block);
        }

        pins.Clear();
        blocks.Clear();
    }

    private void Fill(NativeRecord* native, HeaderedRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        *native = default;
        native->Size = (nuint)sizeof(NativeRecord);
        native->Payload = Pin(record.Payload);
        native->PayloadLen = (nuint)record.Payload.Length;
        if (record.Key is { } key)
        {
            native->Key = Pin(key);
            native->KeyLen = (nuint)key.Length;
        }

        var headers = record.Headers;
        if (headers is null || headers.Count == 0)
        {
            return;
        }

        var list = (NativeHeader*)Allocate(sizeof(NativeHeader) * headers.Count);
        for (var i = 0; i < headers.Count; i++)
        {
            var header = headers[i];
            ArgumentNullException.ThrowIfNull(header);
            Arg.Text(header.Name);
            var name = Encoding.UTF8.GetBytes(header.Name);
            list[i] = new NativeHeader
            {
                Name = Pin(name),
                NameLen = (nuint)name.Length,
                Value = Pin(header.Value),
                ValueLen = (nuint)header.Value.Length,
            };
        }

        native->Headers = list;
        native->HeaderCount = (nuint)headers.Count;
    }

    /// <summary>A pointer to the bytes, null when there are none.</summary>
    private byte* Pin(ReadOnlyMemory<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return null;
        }

        var pin = bytes.Pin();
        pins.Add(pin);
        return (byte*)pin.Pointer;
    }

    private nint Allocate(int size)
    {
        var block = Marshal.AllocHGlobal(size);
        blocks.Add(block);
        return block;
    }
}
