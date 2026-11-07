using System.Runtime.InteropServices;
using System.Text;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Record headers, leases, page reads and topology, with no broker and no native call.</summary>
public class ParityTests
{
    [Fact]
    public unsafe void NativeStructsMatchTheHeader()
    {
        if (nint.Size != 8)
        {
            return;
        }

        Assert.Equal(32, sizeof(NativeHeader));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeHeader>(nameof(NativeHeader.NameLen)));
        Assert.Equal(16, (int)Marshal.OffsetOf<NativeHeader>(nameof(NativeHeader.Value)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeHeader>(nameof(NativeHeader.ValueLen)));
        Assert.Equal(56, sizeof(NativeRecord));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeRecord>(nameof(NativeRecord.Payload)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeRecord>(nameof(NativeRecord.Key)));
        Assert.Equal(40, (int)Marshal.OffsetOf<NativeRecord>(nameof(NativeRecord.Headers)));
        Assert.Equal(48, (int)Marshal.OffsetOf<NativeRecord>(nameof(NativeRecord.HeaderCount)));
        Assert.Equal(48, sizeof(NativeStreamRecord));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeStreamRecord>(nameof(NativeStreamRecord.Publisher)));
        Assert.Equal(40, (int)Marshal.OffsetOf<NativeStreamRecord>(nameof(NativeStreamRecord.TimestampMicros)));
    }

    [Fact]
    public unsafe void RecordsAreLaidOutForTheLibrary()
    {
        var records = new[]
        {
            new HeaderedRecord(
                "body"u8.ToArray(),
                "k"u8.ToArray(),
                [new Header("trace", "t-1"u8.ToArray()), new Header("trace", ReadOnlyMemory<byte>.Empty)]),
            new HeaderedRecord(ReadOnlyMemory<byte>.Empty),
        };
        using var args = new RecordArgs(records);

        Assert.Equal(2, args.Count);
        var first = args.Records[0];
        Assert.Equal((nuint)sizeof(NativeRecord), first.Size);
        Assert.Equal("body", Encoding.UTF8.GetString(first.Payload, (int)first.PayloadLen));
        Assert.Equal("k", Encoding.UTF8.GetString(first.Key, (int)first.KeyLen));
        Assert.Equal((nuint)2, first.HeaderCount);
        Assert.Equal("trace", Encoding.UTF8.GetString(first.Headers[0].Name, (int)first.Headers[0].NameLen));
        Assert.Equal("t-1", Encoding.UTF8.GetString(first.Headers[0].Value, (int)first.Headers[0].ValueLen));
        Assert.True(first.Headers[1].Value == null && first.Headers[1].ValueLen == 0);

        // A plain record has no pointers at all.
        var second = args.Records[1];
        Assert.Equal((nuint)sizeof(NativeRecord), second.Size);
        Assert.True(second.Payload == null && second.Key == null && second.Headers == null);
        Assert.Equal((nuint)0, second.HeaderCount);
    }

    [Fact]
    public void ARecordWithAnEmbeddedNulHeaderNameIsRefusedAndNothingLeaks()
    {
        var record = new HeaderedRecord("x"u8.ToArray(), null, [new Header("a\0b", ReadOnlyMemory<byte>.Empty)]);
        Assert.Throws<ArgumentException>(() => new RecordArgs([record]));
    }

    [Fact]
    public void TheDefaultTokenPrintDoesNotShowTheSecret()
    {
        var text = new LeaseToken("s3cret", 99).ToString();
        Assert.DoesNotContain("s3cret", text);
        Assert.Contains("99", text);
    }

    [Fact]
    public async Task ALeaseWriteIsCopiedWithItsTokenOrTheReasonForNone()
    {
        var state = new CallState<LeaseWrite>(default);
        var userData = state.Pin();
        var token = Utf8("tok", out var tokenPin);
        try
        {
            Lease(userData, 0, 1, 1, 5, token, 99, 0);
        }
        finally
        {
            tokenPin.Free();
        }

        var granted = await state.Task;
        Assert.True(granted.Result.Applied);
        Assert.Equal(5L, granted.Result.Version);
        Assert.Equal(new LeaseToken("tok", 99), granted.Token);
        Assert.Null(granted.TokenRefused);

        state = new CallState<LeaseWrite>(default);
        userData = state.Pin();
        var why = Utf8("no template", out var whyPin);
        try
        {
            Lease(userData, 0, 1, 0, 0, 0, 0, why);
        }
        finally
        {
            whyPin.Free();
        }

        var refused = await state.Task;
        Assert.True(refused.Result.Applied);
        Assert.Null(refused.Result.Version);
        Assert.Null(refused.Token);
        Assert.Equal("no template", refused.TokenRefused);
    }

    [Fact]
    public async Task APageIsCopiedWithItsPublishersAndNextOffset()
    {
        var state = new CallState<StreamPage>(default);
        var userData = state.Pin();
        var payload = GCHandle.Alloc("hi"u8.ToArray(), GCHandleType.Pinned);
        var who = GCHandle.Alloc("alice"u8.ToArray(), GCHandleType.Pinned);
        try
        {
            unsafe
            {
                var rows = new[]
                {
                    new NativeStreamRecord
                    {
                        Offset = 4,
                        Payload = (byte*)payload.AddrOfPinnedObject(),
                        PayloadLen = 2,
                        Publisher = (byte*)who.AddrOfPinnedObject(),
                        PublisherLen = 5,
                        TimestampMicros = 11,
                    },
                    new NativeStreamRecord { Offset = 6, TimestampMicros = 12 },
                };
                fixed (NativeStreamRecord* pointer = rows)
                {
                    ((delegate* unmanaged[Cdecl]<nint, int, byte*, NativeStreamRecord*, nuint, ulong, void>)&Callbacks.OnPage)(
                        userData, 0, null, pointer, 2, 7);
                }
            }
        }
        finally
        {
            payload.Free();
            who.Free();
        }

        var page = await state.Task;
        Assert.Equal(7L, page.NextOffset);
        Assert.Equal(2, page.Records.Count);
        Assert.Equal(4L, page.Records[0].Offset);
        Assert.Equal("hi", Encoding.UTF8.GetString(page.Records[0].Payload.Span));
        Assert.Equal("alice", page.Records[0].Publisher);
        Assert.Equal(11L, page.Records[0].TimestampMicros);
        Assert.Equal(6L, page.Records[1].Offset);
        Assert.Null(page.Records[1].Publisher);
        Assert.True(page.Records[1].Payload.IsEmpty);
    }

    [Fact]
    public async Task ConnectionsAndCountsAreCopied()
    {
        var count = new CallState<int>(default);
        unsafe
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, nuint, void>)&Callbacks.OnCount)(count.Pin(), 0, null, 3);
        }

        Assert.Equal(3, await count.Task);

        var state = new CallState<NodeConnections[]>(default);
        var userData = state.Pin();
        var a = Utf8("10.0.0.1:4433", out var aPin);
        var b = Utf8("10.0.0.2:4433", out var bPin);
        try
        {
            unsafe
            {
                var addrs = new[] { a, b };
                var counts = new nuint[] { 1, 3 };
                fixed (nint* addrPointer = addrs)
                fixed (nuint* countPointer = counts)
                {
                    ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte**, nuint*, nuint, void>)&Callbacks.OnConnections)(
                        userData, 0, null, (byte**)addrPointer, countPointer, 2);
                }
            }
        }
        finally
        {
            aPin.Free();
            bPin.Free();
        }

        Assert.Equal(
            [new NodeConnections("10.0.0.1:4433", 1), new NodeConnections("10.0.0.2:4433", 3)],
            await state.Task);
    }

    [Fact]
    public unsafe void RecordHeadersAreOnUnlessTurnedOff()
    {
        var options = new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k" };
        Assert.Equal(0, FelixClient.NativeOptionsOf(options).DisableRecordHeaders);
        Assert.Equal(1, FelixClient.NativeOptionsOf(
            new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k", DisableRecordHeaders = true }).DisableRecordHeaders);
    }

    /// <summary>Skips unless the native library is on hand.</summary>
    private sealed class NativeFactAttribute : FactAttribute
    {
        public NativeFactAttribute()
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FELIX_NATIVE_LIB")))
            {
                Skip = "FELIX_NATIVE_LIB is not set; build libfelix with `cargo build -p felix-capi`";
            }
        }
    }

    // A null client is the library's own refusal, so each entry point must resolve, take its
    // arguments and map the status, with no broker.
    [NativeFact]
    public async Task EveryNewEntryPointResolvesAndRefusesANullClient()
    {
        using var client = new FelixClient(new FelixClientHandle(0));
        var cache = client.Cache("t", "n", "c");
        var record = new HeaderedRecord("x"u8.ToArray(), "k"u8.ToArray(), [new Header("a", "b"u8.ToArray())]);
        Task[] calls =
        [
            client.PublishWithHeadersAsync("t", "n", "s", record),
            client.PublishBatchWithHeadersAsync("t", "n", "s", [record, record]),
            client.WithIdentityAsync("t", "token"),
            client.WithIdentityAsync("t", _ => ValueTask.FromResult("token")),
            client.ReadRangeAsync("t", "n", "s", 0, 1, 5, 2),
            client.RefreshTopologyAsync(),
            client.GetConnectionsPerNodeAsync(),
            cache.PutIfAbsentLeaseAsync("k", "v"u8.ToArray()),
            cache.PutIfVersionLeaseAsync("k", "v"u8.ToArray(), 3),
        ];
        foreach (var call in calls)
        {
            var error = await Assert.ThrowsAsync<FelixException>(() => call);
            Assert.Equal(FelixStatus.InvalidArgument, error.Status);
            Assert.Contains("client", error.Message);
        }
    }

    [Fact]
    public void AnIdentityKeepsTheParentsTokenSourceAliveUntilItIsDisposed()
    {
        var source = new TokenSource(_ => ValueTask.FromResult("token"));
        var parent = new FelixClient(new FelixClientHandle(0), source);
        var identity = new FelixClient(new FelixClientHandle(0), parent.ShareTokens());

        parent.Dispose();
        Assert.False(source.IsCancelled);
        Assert.Same(source, identity.Tokens);

        identity.Dispose();
        Assert.True(source.IsCancelled);
        source.Abandon();
    }

    [NativeFact]
    public async Task ARefusedIdentityLeavesTheTokenSourcesAsItFoundThem()
    {
        var live = TokenSource.Live;
        var source = new TokenSource(_ => ValueTask.FromResult("token"));
        using var client = new FelixClient(new FelixClientHandle(0), source);

        var error = await Assert.ThrowsAsync<FelixException>(() => client.WithIdentityAsync("t", "token"));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
        Assert.Equal(1, source.Holders);

        // The refused provider form never keeps its source, so token_release never runs for it.
        await Assert.ThrowsAsync<FelixException>(() =>
            client.WithIdentityAsync("t", _ => ValueTask.FromResult("token")));
        Assert.Equal(live + 1, TokenSource.Live);
        source.Abandon();
    }

    [Fact]
    public async Task ABatchWithNoRecordsIsRefusedBeforeAnyNativeCall()
    {
        await using var client = new FelixClient(new FelixClientHandle(0));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.PublishBatchWithHeadersAsync("t", "n", "s", []));
    }

    private static unsafe void Lease(nint userData, int status, byte applied, byte hasVersion, ulong version, nint token, ulong expiresAt, nint refused) =>
        ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte, byte, ulong, byte*, ulong, byte*, void>)&Callbacks.OnLease)(
            userData, status, null, applied, hasVersion, version, (byte*)token, expiresAt, (byte*)refused);

    private static nint Utf8(string text, out GCHandle pin)
    {
        pin = GCHandle.Alloc(Encoding.UTF8.GetBytes(text + "\0"), GCHandleType.Pinned);
        return pin.AddrOfPinnedObject();
    }
}
