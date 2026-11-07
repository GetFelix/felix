using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Felix.Native;

// The callbacks for leases, page reads and topology. Each copies what it borrows and never throws.
internal static unsafe partial class Callbacks
{
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnLease(
        nint userData, int status, byte* message, byte applied, byte hasVersion, ulong version, byte* token, ulong expiresAt, byte* refused) =>
        Complete(userData, status, message, () => new LeaseWrite(
            new ConditionalResult(applied != 0, hasVersion != 0 ? unchecked((long)version) : null),
            token is null ? null : new LeaseToken(NativeMethods.Utf8(token) ?? string.Empty, unchecked((long)expiresAt)),
            NativeMethods.Utf8(refused)));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnPage(nint userData, int status, byte* message, NativeStreamRecord* records, nuint count, ulong nextOffset) =>
        Complete(userData, status, message, () => CopyPage(records, count, nextOffset));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnCount(nint userData, int status, byte* message, nuint count) =>
        Complete(userData, status, message, () => checked((int)count));

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    internal static void OnConnections(nint userData, int status, byte* message, byte** addrs, nuint* counts, nuint len) =>
        Complete(userData, status, message, () => CopyConnections(addrs, counts, len));

    private static StreamPage CopyPage(NativeStreamRecord* records, nuint count, ulong nextOffset)
    {
        var rows = new StreamRecord[checked((int)count)];
        for (var i = 0; i < rows.Length; i++)
        {
            var row = records + i;
            var publisher = row->Publisher is null || row->PublisherLen == 0
                ? null
                : Encoding.UTF8.GetString(row->Publisher, checked((int)row->PublisherLen));
            rows[i] = new StreamRecord(
                unchecked((long)row->Offset),
                CopyBytes(row->Payload, row->PayloadLen),
                publisher,
                unchecked((long)row->TimestampMicros));
        }

        return new StreamPage(rows, unchecked((long)nextOffset));
    }

    private static NodeConnections[] CopyConnections(byte** addrs, nuint* counts, nuint len)
    {
        var rows = new NodeConnections[checked((int)len)];
        for (var i = 0; i < rows.Length; i++)
        {
            rows[i] = new NodeConnections(NativeMethods.Utf8(addrs[i]) ?? string.Empty, checked((int)counts[i]));
        }

        return rows;
    }
}
