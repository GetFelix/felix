using System.Runtime.InteropServices;
using System.Text;

namespace Felix.Native;

/// <summary>
/// A commit's operations laid out for the library, in unmanaged memory freed on dispose.
/// The library copies everything before the call returns, so the caller keeps this
/// only across the call.
/// </summary>
internal sealed unsafe class NativeCommitOps : IDisposable
{
    private readonly List<nint> blocks = [];

    internal NativeCommitOps(IReadOnlyList<CommitOp> ops)
    {
        Count = (nuint)ops.Count;
        try
        {
            if (ops.Count > 0)
            {
                Pointer = (NativeCommitOp*)Alloc((nuint)(ops.Count * sizeof(NativeCommitOp)));
            }

            for (var i = 0; i < ops.Count; i++)
            {
                var op = ops[i];
                ArgumentNullException.ThrowIfNull(op, nameof(ops));
                Pointer[i] = new NativeCommitOp
                {
                    Kind = (int)op.Kind,
                    Stream = Text(op.Stream),
                    Key = op.Key is null ? null : Text(op.Key),
                    Data = Bytes(op.Data.Span),
                    DataLen = (nuint)op.Data.Length,
                };
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    internal NativeCommitOp* Pointer { get; }

    internal nuint Count { get; }

    public void Dispose()
    {
        foreach (var block in blocks)
        {
            NativeMemory.Free((void*)block);
        }

        blocks.Clear();
    }

    private byte* Alloc(nuint size)
    {
        var block = (byte*)NativeMemory.AllocZeroed(size == 0 ? 1 : size);
        blocks.Add((nint)block);
        return block;
    }

    private byte* Text(string value)
    {
        var length = Encoding.UTF8.GetByteCount(value);
        var block = Alloc((nuint)length + 1);
        Encoding.UTF8.GetBytes(value, new Span<byte>(block, length));
        return block;
    }

    private byte* Bytes(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        {
            return null;
        }

        var block = Alloc((nuint)value.Length);
        value.CopyTo(new Span<byte>(block, value.Length));
        return block;
    }
}
