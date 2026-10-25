using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Group types and the results writer, with no broker and no native call.</summary>
public class GroupTests
{
    [Fact]
    public unsafe void NativeStructsMatchTheHeader()
    {
        // Offsets and sizes as a 64-bit C compiler lays out felix.h.
        if (nint.Size != 8)
        {
            // 8-byte fields align differently on 32-bit targets; the size field and two flags
            // pad to two pointers on every one, and the three token fields are pointers.
            Assert.Equal(5 * nint.Size, sizeof(NativeConnectOptions));
            return;
        }

        Assert.Equal(40, sizeof(NativeConnectOptions));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeConnectOptions>(nameof(NativeConnectOptions.Publishers)));
        Assert.Equal(16, (int)Marshal.OffsetOf<NativeConnectOptions>(nameof(NativeConnectOptions.TokenFetch)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeConnectOptions>(nameof(NativeConnectOptions.TokenUserData)));
        Assert.Equal(32, (int)Marshal.OffsetOf<NativeConnectOptions>(nameof(NativeConnectOptions.TokenRelease)));
        Assert.Equal(72, sizeof(NativeGroupRecord));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeGroupRecord>(nameof(NativeGroupRecord.Attempts)));
        Assert.Equal(16, (int)Marshal.OffsetOf<NativeGroupRecord>(nameof(NativeGroupRecord.SkippedBefore)));
        Assert.Equal(24, (int)Marshal.OffsetOf<NativeGroupRecord>(nameof(NativeGroupRecord.Payload)));
        Assert.Equal(40, (int)Marshal.OffsetOf<NativeGroupRecord>(nameof(NativeGroupRecord.Publisher)));
        Assert.Equal(56, (int)Marshal.OffsetOf<NativeGroupRecord>(nameof(NativeGroupRecord.HasTimestamp)));
        Assert.Equal(64, (int)Marshal.OffsetOf<NativeGroupRecord>(nameof(NativeGroupRecord.TimestampMicros)));
        Assert.Equal(80, sizeof(NativeShardedGroupRecord));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeShardedGroupRecord>(nameof(NativeShardedGroupRecord.Record)));
        Assert.Equal(48, sizeof(NativeGroupInfo));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeGroupInfo>(nameof(NativeGroupInfo.Committed)));
        Assert.Equal(16, sizeof(NativeGroupPosition));
    }

    [Fact]
    public unsafe void ARecordIsCopiedOutOfNativeMemory()
    {
        var payload = "job"u8.ToArray();
        var publisher = Encoding.UTF8.GetBytes("tenant/alice");
        fixed (byte* payloadPointer = payload)
        fixed (byte* publisherPointer = publisher)
        {
            var native = new NativeGroupRecord
            {
                Offset = 41,
                Attempts = 2,
                SkippedBefore = 3,
                Payload = payloadPointer,
                PayloadLen = (nuint)payload.Length,
                Publisher = publisherPointer,
                PublisherLen = (nuint)publisher.Length,
                HasTimestamp = 1,
                TimestampMicros = 1_700_000_000_500_000,
            };
            var record = Callbacks.CopyRecord(&native, 3);
            payload[0] = (byte)'X';

            Assert.Equal(3u, record.Shard);
            Assert.Equal(41, record.Offset);
            Assert.Equal(2u, record.Attempts);
            Assert.Equal(3, record.SkippedBefore);
            Assert.Equal("job", Encoding.UTF8.GetString(record.Payload.Span));
            Assert.Equal("tenant/alice", record.Publisher);
            Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_500), record.Timestamp);
        }
    }

    [Fact]
    public unsafe void AnEmptyRecordHasNoPublisherOrTime()
    {
        var native = new NativeGroupRecord { Offset = 1 };
        var record = Callbacks.CopyRecord(&native);
        Assert.True(record.Payload.IsEmpty);
        Assert.Null(record.Publisher);
        Assert.Null(record.TimestampMicros);
        Assert.Null(record.Timestamp);
    }

    [Fact]
    public unsafe void AGroupWithoutACursorReportsNoCommittedOffset()
    {
        var none = new NativeGroupInfo { Tail = 9 };
        Assert.Equal(new GroupInfo(null, 9, 0, 0, 0), Callbacks.ToInfo(&none));
        var some = new NativeGroupInfo { HasCommitted = 1, Committed = 4, Tail = 9, InFlight = 2, Owed = 1, DeadLetters = 5 };
        Assert.Equal(new GroupInfo(4, 9, 2, 1, 5), Callbacks.ToInfo(&some));
    }

    [Fact]
    public void ATimeOutsideTheCalendarIsNotATime()
    {
        Assert.Null(Time.FromMicros(null));
        Assert.Null(Time.FromMicros(-1));
        Assert.Null(Time.FromMicros(long.MaxValue));
        Assert.Equal(DateTimeOffset.UnixEpoch, Time.FromMicros(0));
    }

    [Fact]
    public void ASettleForAnotherShardIsRefusedBeforeAnythingIsSent()
    {
        var group = new FelixGroup(null!, "t", "n", "s", 2, "g", null);
        var other = new GroupRecord(3, 1, 1, 0, default, null, null);
        Assert.Throws<ArgumentException>(() => { _ = group.AckAsync(other); });
        Assert.Throws<ArgumentException>(() => { _ = group.NackAsync(other); });
    }

    [Fact]
    public void AShardedGroupRefusesANullRecord()
    {
        // The constructor needs a live native group; the null check runs before any use of it.
        var group = (FelixShardedGroup)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(FelixShardedGroup));
        Assert.Throws<ArgumentNullException>(() => { _ = group.AckAsync(null!); });
        Assert.Throws<ArgumentNullException>(() => { _ = group.NackAsync(null!); });
        Assert.Throws<ArgumentNullException>(() => { _ = group.ExtendAsync(null!, TimeSpan.FromSeconds(1)); });
        Assert.Throws<ArgumentNullException>(() => { _ = group.DeadLetterAsync(null!); });
    }

    [Fact]
    public async Task NegativeDurationsAreRefused()
    {
        var group = new FelixGroup(null!, "t", "n", "s", 0, "g", null);
        var record = new GroupRecord(0, 1, 1, 0, default, null, null);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => group.PollAsync(1, TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = group.NackAsync(record, TimeSpan.FromSeconds(-1)); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = group.ExtendAsync(record, TimeSpan.FromSeconds(-1)); });
    }
}

public class ConformanceResultsTests
{
    private static JsonElement Parse(ConformanceResults results) =>
        JsonDocument.Parse(results.ToJson("Felix.Client (C#)", "0.7.0")).RootElement;

    [Fact]
    public void TheDocumentHasTheShapeVerifyReads()
    {
        var results = new ConformanceResults();
        results.Record(["pubsub.roundtrip"], "pass", null);
        results.Record(["cache.put_get"], "skip", "not bound yet");
        results.Record(["pubsub.order"], "fail", "expected 3, got 2");

        var root = Parse(results);
        Assert.Equal("Felix.Client (C#)", root.GetProperty("client").GetString());
        Assert.Equal("0.7.0", root.GetProperty("version").GetString());
        var outcomes = root.GetProperty("outcomes").EnumerateArray().ToList();
        Assert.Equal(["cache.put_get", "pubsub.order", "pubsub.roundtrip"], outcomes.Select(o => o.GetProperty("id").GetString()!));
        Assert.Equal(["skip", "fail", "pass"], outcomes.Select(o => o.GetProperty("status").GetString()!));
        Assert.Equal("not bound yet", outcomes[0].GetProperty("detail").GetString());
        Assert.False(outcomes[2].TryGetProperty("detail", out _));
    }

    [Fact]
    public void TheWorstOutcomeOfASharedScenarioWins()
    {
        var results = new ConformanceResults();
        results.Record(["queue.a"], "pass", null);
        results.Record(["queue.a"], "fail", "broke");
        results.Record(["queue.a"], "skip", "later skip");
        results.Record(["queue.b"], "skip", "no fixture");
        results.Record(["queue.b"], "pass", null);

        var outcomes = Parse(results).GetProperty("outcomes").EnumerateArray().ToDictionary(
            o => o.GetProperty("id").GetString()!, o => o.GetProperty("status").GetString()!);
        Assert.Equal("fail", outcomes["queue.a"]);
        Assert.Equal("skip", outcomes["queue.b"]);
    }

    [Fact]
    public void NothingIsWrittenWhenNothingRan()
    {
        var path = Path.Combine(Path.GetTempPath(), $"felix-cs-{Guid.NewGuid():N}", "results.json");
        new ConformanceResults().WriteTo(path, "c", "v");
        Assert.False(File.Exists(path));

        var results = new ConformanceResults();
        results.Record(["x"], "pass", null);
        results.WriteTo(path, "c", "v");
        Assert.True(File.Exists(path));
        Directory.Delete(Path.GetDirectoryName(path)!, true);
    }

    [Fact]
    public void AnUnknownStatusIsRefused()
    {
        Assert.Throws<ArgumentException>(() => new ConformanceResults().Record(["x"], "passed", null));
    }
}
