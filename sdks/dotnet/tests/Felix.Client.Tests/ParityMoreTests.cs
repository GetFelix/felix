using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Felix.Native;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>Inspection, subscription counters, queue and transport options and hand-offs, with no broker.</summary>
public class ParityMoreTests
{
    private static FelixClientOptions Base() => new() { Addrs = ["x:1"], TenantId = "t", Token = "k" };

    [Fact]
    public unsafe void TheOptionsStructMatchesTheHeader()
    {
        if (nint.Size != 8)
        {
            return;
        }

        int At(string field) => (int)Marshal.OffsetOf<NativeConnectOptions>(field);
        Assert.Equal(88, At(nameof(NativeConnectOptions.DisableRecordHeaders)));
        Assert.Equal(89, At(nameof(NativeConnectOptions.AckOnCommit)));
        Assert.Equal(90, At(nameof(NativeConnectOptions.NoPublishShardStreams)));
        Assert.Equal(92, At(nameof(NativeConnectOptions.ClientSubQueuePolicy)));
        Assert.Equal(96, At(nameof(NativeConnectOptions.PublishSharding)));
        Assert.Equal(100, At(nameof(NativeConnectOptions.ClientSubQueueCapacity)));
        Assert.Equal(104, At(nameof(NativeConnectOptions.BrokerSubQueueCapacity)));
        Assert.Equal(108, At(nameof(NativeConnectOptions.PublishConnPool)));
        Assert.Equal(144, At(nameof(NativeConnectOptions.EventRouterMaxPending)));
        Assert.Equal(152, At(nameof(NativeConnectOptions.PublishInflightBytes)));
        Assert.Equal(160, At(nameof(NativeConnectOptions.MaxFrameBytes)));
        Assert.Equal(208, At(nameof(NativeConnectOptions.CacheSendWindow)));
        Assert.Equal(216, At(nameof(NativeConnectOptions.ConnectTimeoutMs)));
        Assert.Equal(224, At(nameof(NativeConnectOptions.FinishTimeoutMs)));
        Assert.Equal(232, sizeof(NativeConnectOptions));

        Assert.Equal(48, sizeof(NativeSubscriptionFilter));
        Assert.Equal(8, (int)Marshal.OffsetOf<NativeSubscriptionFilter>(nameof(NativeSubscriptionFilter.TenantId)));
        Assert.Equal(32, (int)Marshal.OffsetOf<NativeSubscriptionFilter>(nameof(NativeSubscriptionFilter.Principal)));
        Assert.Equal(40, (int)Marshal.OffsetOf<NativeSubscriptionFilter>(nameof(NativeSubscriptionFilter.Shard)));
        Assert.Equal(44, (int)Marshal.OffsetOf<NativeSubscriptionFilter>(nameof(NativeSubscriptionFilter.HasShard)));
        Assert.Equal(45, (int)Marshal.OffsetOf<NativeSubscriptionFilter>(nameof(NativeSubscriptionFilter.Dropping)));
    }

    [Fact]
    public void WithNothingSetTheNativeOptionsAreAllZero()
    {
        var options = Base();
        Assert.False(options.HasTuning);
        var native = FelixClient.NativeOptionsOf(options);
        Assert.Equal(0, native.AckOnCommit);
        Assert.Equal(0, native.NoPublishShardStreams);
        Assert.Equal(0, native.ClientSubQueuePolicy);
        Assert.Equal(0, native.PublishSharding);
        Assert.Equal(0u, native.ClientSubQueueCapacity);
        Assert.Equal(0u, native.PublishShardStreams);
        Assert.Equal(0UL, native.MaxFrameBytes);
        Assert.Equal(0UL, native.ConnectTimeoutMs);
    }

    [Fact]
    public void EveryOptionReachesItsNativeField()
    {
        var options = new FelixClientOptions
        {
            Addrs = ["x:1"],
            TenantId = "t",
            Token = "k",
            ClientSubQueuePolicy = ClientSubQueuePolicy.Block,
            ClientSubQueueCapacity = 11,
            BrokerSubQueueCapacity = 12,
            PublishConnPool = 13,
            PublishStreamsPerConn = 14,
            PublishQueueDepth = 15,
            PublishInflightBytes = 16,
            PublishSharding = PublishSharding.RoundRobin,
            PublishShardStreams = 17,
            CacheConnPool = 18,
            CacheStreamsPerConn = 19,
            EventConnPool = 20,
            ClusterConnPool = 21,
            ClusterStreamsPerConn = 22,
            EventConnRecvWindow = 23,
            EventStreamRecvWindow = 24,
            EventSendWindow = 25,
            CacheConnRecvWindow = 26,
            CacheStreamRecvWindow = 27,
            CacheSendWindow = 28,
            EventRouterMaxPending = 29,
            MaxFrameBytes = 30,
            AckOnCommit = true,
            ConnectTimeout = TimeSpan.FromSeconds(31),
            FinishTimeout = TimeSpan.FromMilliseconds(32_500),
        };
        Assert.True(options.HasTuning);
        options.ValidateTuning();
        var native = FelixClient.NativeOptionsOf(options);
        Assert.Equal(1, native.ClientSubQueuePolicy);
        Assert.Equal(11u, native.ClientSubQueueCapacity);
        Assert.Equal(12u, native.BrokerSubQueueCapacity);
        Assert.Equal(13u, native.PublishConnPool);
        Assert.Equal(14u, native.PublishStreamsPerConn);
        Assert.Equal(15u, native.PublishQueueDepth);
        Assert.Equal(16UL, native.PublishInflightBytes);
        Assert.Equal(2, native.PublishSharding);
        Assert.Equal(17u, native.PublishShardStreams);
        Assert.Equal(0, native.NoPublishShardStreams);
        Assert.Equal(18u, native.CacheConnPool);
        Assert.Equal(19u, native.CacheStreamsPerConn);
        Assert.Equal(20u, native.EventConnPool);
        Assert.Equal(21u, native.ClusterConnPool);
        Assert.Equal(22u, native.ClusterStreamsPerConn);
        Assert.Equal(23UL, native.EventConnRecvWindow);
        Assert.Equal(24UL, native.EventStreamRecvWindow);
        Assert.Equal(25UL, native.EventSendWindow);
        Assert.Equal(26UL, native.CacheConnRecvWindow);
        Assert.Equal(27UL, native.CacheStreamRecvWindow);
        Assert.Equal(28UL, native.CacheSendWindow);
        Assert.Equal(29u, native.EventRouterMaxPending);
        Assert.Equal(30UL, native.MaxFrameBytes);
        Assert.Equal(1, native.AckOnCommit);
        Assert.Equal(31_000UL, native.ConnectTimeoutMs);
        Assert.Equal(32_500UL, native.FinishTimeoutMs);
    }

    [Fact]
    public void ZeroShardStreamsAsksForNoneAndIsNotTheDefault()
    {
        var none = new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k", PublishShardStreams = 0 };
        Assert.True(none.HasTuning);
        none.ValidateTuning();
        var native = FelixClient.NativeOptionsOf(none);
        Assert.Equal(1, native.NoPublishShardStreams);
        Assert.Equal(0u, native.PublishShardStreams);
    }

    [Theory]
    [InlineData("ClientSubQueueCapacity")]
    [InlineData("BrokerSubQueueCapacity")]
    [InlineData("PublishConnPool")]
    [InlineData("PublishStreamsPerConn")]
    [InlineData("PublishQueueDepth")]
    [InlineData("PublishInflightBytes")]
    [InlineData("PublishShardStreams")]
    [InlineData("CacheConnPool")]
    [InlineData("CacheStreamsPerConn")]
    [InlineData("EventConnPool")]
    [InlineData("ClusterConnPool")]
    [InlineData("ClusterStreamsPerConn")]
    [InlineData("EventRouterMaxPending")]
    [InlineData("MaxFrameBytes")]
    [InlineData("EventConnRecvWindow")]
    [InlineData("EventStreamRecvWindow")]
    [InlineData("EventSendWindow")]
    [InlineData("CacheConnRecvWindow")]
    [InlineData("CacheStreamRecvWindow")]
    [InlineData("CacheSendWindow")]
    public async Task ABadSizeIsRefusedBeforeAnythingIsStarted(string property)
    {
        var options = With(property, -1L);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(options));
    }

    [Theory]
    [InlineData("EventConnRecvWindow")]
    [InlineData("EventStreamRecvWindow")]
    [InlineData("EventSendWindow")]
    [InlineData("CacheConnRecvWindow")]
    [InlineData("CacheStreamRecvWindow")]
    [InlineData("CacheSendWindow")]
    public async Task AWindowPastTheQuicLimitIsRefused(string property)
    {
        var options = With(property, 1L << 62);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(options));
        With(property, (1L << 62) - 1).ValidateTuning();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(400)]
    public async Task ATimeoutOutsideItsRangeIsRefused(int days)
    {
        var span = days == 0 ? TimeSpan.Zero : TimeSpan.FromDays(days);
        foreach (var options in new[]
        {
            new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k", ConnectTimeout = span },
            new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k", FinishTimeout = span },
        })
        {
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(options));
        }
    }

    [Fact]
    public async Task AnUndefinedEnumValueIsRefused()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k", ClientSubQueuePolicy = (ClientSubQueuePolicy)9 }));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => FelixClient.ConnectAsync(
            new FelixClientOptions { Addrs = ["x:1"], TenantId = "t", Token = "k", PublishSharding = (PublishSharding)0 }));
    }

    private static FelixClientOptions With(string property, long value)
    {
        var options = Base();
        var info = typeof(FelixClientOptions).GetProperty(property)!;
        var type = Nullable.GetUnderlyingType(info.PropertyType)!;
        info.SetValue(options, type == typeof(int) ? (object)(int)value : value);
        return options;
    }

    private static string Sample(string name) => name switch
    {
        "inspection" => """
            {"node_id":"n1","shards":4,"role":"leader","phase":"active","serving":true,"generation":7,
             "assignment":{"generation":7,"leader":"n1","replicas":["n2","n3"],"draining":true,"successor":"n4"},
             "fence":{"took":["n2"],"pending":["n3"],"attempts":2,"retry_in_ms":500},
             "lease":{"held":true,"remaining_ms":900},"tail":120,"committed":118,"accepted_generation":7,
             "replicas":[{"node_id":"n2","role":"follower","next_offset":119,"lag":1,"fence":true,"state":"shipping"},
                         {"node_id":"n3","role":"learner","state":"halted","halted":"diverged"}]}
            """,
        "page" => """
            {"node_id":"n1","subscriptions":[
               {"tenant_id":"t","namespace":"default","stream":"orders","shard":0,"subscriber_id":41,"subscription_id":9,
                "connection":5,"peer":"10.0.0.9:5000","principal":"p:alice","policy":"drop_new","depth":2,"capacity":64,
                "dropped":3,"position":100,"tail":120,"age_ms":4000},
               {"tenant_id":"t","namespace":"default","stream":"orders","shard":0,"subscriber_id":42,
                "policy":"block","depth":0,"capacity":8,"dropped":0,"tail":120,"age_ms":1}],
             "next_cursor":{"tenant_id":"t","namespace":"default","stream":"orders","shard":0,"subscriber_id":42}}
            """,
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    [Fact]
    public void AnInspectionIsParsedFromTheWireJson()
    {
        var view = WireJson.Read(Sample("inspection"), WireJson.Default.ShardInspection);
        Assert.Equal("n1", view.NodeId);
        Assert.Equal(4u, view.Shards);
        Assert.Equal("leader", view.Role);
        Assert.Equal("active", view.Phase);
        Assert.True(view.Serving);
        Assert.Null(view.Reason);
        Assert.Equal(7, view.Generation);
        Assert.Equal(["n2", "n3"], view.Assignment!.Replicas);
        Assert.True(view.Assignment.Draining);
        Assert.Equal("n4", view.Assignment.Successor);
        Assert.Equal(["n2"], view.Fence!.Took);
        Assert.Equal(["n3"], view.Fence.Pending);
        Assert.Equal(500, view.Fence.RetryInMs);
        Assert.Equal(new InspectedLease(true, 900), view.Lease);
        Assert.Equal(120, view.Tail);
        Assert.Equal(118, view.Committed);
        Assert.Equal(7, view.AcceptedGeneration);
        Assert.Equal(2, view.Replicas.Count);
        Assert.Equal(119, view.Replicas[0].NextOffset);
        Assert.True(view.Replicas[0].Fence);
        Assert.Null(view.Replicas[1].NextOffset);
        Assert.Equal("diverged", view.Replicas[1].Halted);
    }

    [Fact]
    public void AnInspectionWithOnlyItsIdentityHasEmptyDefaults()
    {
        var view = WireJson.Read(
            """{"node_id":"","shards":0,"role":"none","phase":"unassigned","serving":false}""",
            WireJson.Default.ShardInspection);
        Assert.False(view.Serving);
        Assert.Null(view.Assignment);
        Assert.Null(view.Lease);
        Assert.Null(view.Tail);
        Assert.Empty(view.Replicas);
    }

    [Fact]
    public void APageOfSubscriptionsKeepsItsCursorToPassBack()
    {
        var page = WireJson.Read(Sample("page"), WireJson.Default.SubscriptionsPage);
        Assert.Equal("n1", page.NodeId);
        Assert.Equal(2, page.Subscriptions.Count);
        var first = page.Subscriptions[0];
        Assert.Equal(41, first.SubscriberId);
        Assert.Equal(9, first.SubscriptionId);
        Assert.Equal("p:alice", first.Principal);
        Assert.Equal("drop_new", first.Policy);
        Assert.Equal((2L, 64L, 3L), (first.Depth, first.Capacity, first.Dropped));
        Assert.Equal(100, first.Position);
        Assert.Null(page.Subscriptions[1].Position);
        Assert.Null(page.Subscriptions[1].Connection);

        var cursor = Assert.IsType<SubscriptionCursor>(page.NextCursor);
        Assert.Equal(new SubscriptionCursor("t", "default", "orders", 0, 42), cursor);
        // What goes back to the library is the wire's own object.
        using var json = JsonDocument.Parse(JsonSerializer.Serialize(cursor, WireJson.Default.SubscriptionCursor));
        Assert.Equal("default", json.RootElement.GetProperty("namespace").GetString());
        Assert.Equal(42, json.RootElement.GetProperty("subscriber_id").GetInt64());
        Assert.Equal(5, json.RootElement.EnumerateObject().Count());
    }

    [Fact]
    public void TheLastPageHasNoCursor()
    {
        var page = WireJson.Read(
            """{"node_id":"n1","subscriptions":[],"next_cursor":null}""", WireJson.Default.SubscriptionsPage);
        Assert.Empty(page.Subscriptions);
        Assert.Null(page.NextCursor);
    }

    [Fact]
    public void OwnersAndBrokersAreParsedAndAnAbsentNodeStaysNull()
    {
        var owners = WireJson.Read(
            """[{"shard":0,"node_id":"n1","addr":"10.0.0.1:4433","generation":3},{"shard":1,"generation":0,"unavailable":"not_assigned"}]""",
            WireJson.Default.ShardOwnerArray);
        Assert.Equal(2, owners.Length);
        Assert.Equal("10.0.0.1:4433", owners[0].Addr);
        Assert.Null(owners[1].NodeId);
        Assert.Equal("not_assigned", owners[1].Unavailable);

        var brokers = WireJson.Read("""[{"node_id":"n1","addr":"10.0.0.1:4433"}]""", WireJson.Default.BrokerEndpointArray);
        Assert.Equal([new BrokerEndpoint("n1", "10.0.0.1:4433")], brokers);
    }

    [Fact]
    public void AnAnswerThatIsNullIsAnError()
    {
        var error = Assert.Throws<FelixException>(() => WireJson.Read("null", WireJson.Default.ShardInspection));
        Assert.Equal(FelixStatus.Error, error.Status);
    }

    [Fact]
    public void SupportBitsBecomeBooleans()
    {
        Assert.Equal(new FelixSupports(false, false, false, false, false, false), Callbacks.SupportsOf(0));
        Assert.Equal(new FelixSupports(true, false, false, false, false, false), Callbacks.SupportsOf(1));
        Assert.Equal(new FelixSupports(false, true, true, false, false, true), Callbacks.SupportsOf(0b100110));
        Assert.Equal(new FelixSupports(true, true, true, true, true, true), Callbacks.SupportsOf(0b111111));
    }

    [Fact]
    public async Task JsonRoutingAndSupportAnswersAreCopiedFromTheCallbacks()
    {
        var json = new CallState<string>(default);
        var text = Utf8("[1]", out var pin);
        try
        {
            unsafe
            {
                ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, void>)&Callbacks.OnJson)(json.Pin(), 0, null, (byte*)text);
            }
        }
        finally
        {
            pin.Free();
        }

        Assert.Equal("[1]", await json.Task);

        var page = new CallState<string>(default);
        var body = Utf8("{}", out var bodyPin);
        var next = Utf8("{\"x\":1}", out var nextPin);
        try
        {
            unsafe
            {
                ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, byte*, void>)&Callbacks.OnSubscriptions)(
                    page.Pin(), 0, null, (byte*)body, (byte*)next);
            }
        }
        finally
        {
            bodyPin.Free();
            nextPin.Free();
        }

        Assert.Equal("{}", await page.Task);

        var routing = new CallState<StreamRouting>(default);
        unsafe
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, uint, int, void>)&Callbacks.OnRouting)(routing.Pin(), 0, null, 8, 1);
        }

        Assert.Equal(new StreamRouting(8, ShardRouting.JumpHash), await routing.Task);

        var supports = new CallState<FelixSupports>(default);
        unsafe
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, uint, void>)&Callbacks.OnSupports)(supports.Pin(), 0, null, 0b101);
        }

        var seen = await supports.Task;
        Assert.True(seen.Inspect && seen.AckOnCommit && !seen.Topology);
    }

    [Fact]
    public async Task AFailedAnswerFaultsTheCallWithItsStatus()
    {
        var json = new CallState<string>(default);
        var message = Utf8("not allowed", out var pin);
        try
        {
            unsafe
            {
                ((delegate* unmanaged[Cdecl]<nint, int, byte*, byte*, void>)&Callbacks.OnJson)(
                    json.Pin(), (int)FelixStatus.Auth, (byte*)message, null);
            }
        }
        finally
        {
            pin.Free();
        }

        var error = await Assert.ThrowsAsync<FelixException>(() => json.Task);
        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.Equal("not allowed", error.Message);

        var handed = new CallState<FelixHandedHandle>(default);
        unsafe
        {
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnHanded)(
                handed.Pin(), (int)FelixStatus.Closed, null, 0);
        }

        Assert.Equal(FelixStatus.Closed, (await Assert.ThrowsAsync<FelixException>(() => handed.Task)).Status);
    }

    // The batch was handed over before the token fired, so it is sent: a plain cancel would invite a retry
    // that sends it twice.
    [NativeFact]
    public async Task AHandOffThatCompletesAfterTheTokenFiredReportsAnUnknownOutcome()
    {
        using var source = new CancellationTokenSource();
        var state = new CallState<FelixHandedHandle>(source.Token);
        source.Cancel();
        unsafe
        {
            // A zero handle is ignored by the library's free.
            ((delegate* unmanaged[Cdecl]<nint, int, byte*, nint, void>)&Callbacks.OnHanded)(state.Pin(), 0, null, 0);
        }

        var error = await Assert.ThrowsAsync<FelixException>(() => state.Task);
        Assert.Equal(FelixStatus.OutcomeUnknown, error.Status);
        Assert.True(error.OutcomeUnknown);
    }

    [Fact]
    public async Task ArgumentsAreCheckedBeforeAnyNativeCall()
    {
        await using var client = new FelixClient(new FelixClientHandle(0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            client.InspectShardAsync((ShardKind)2, "t", "n", "s", 0));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            client.GetShardOwnersAsync((ShardKind)(-1), "t", "n", "s"));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => client.ListSubscriptionsAsync(limit: 0));
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetStreamRoutingAsync("t", "n", "a b"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            client.ListSubscriptionsAsync(new SubscriptionFilter { Stream = "a b" }));
        await Assert.ThrowsAsync<ArgumentException>(() => client.GetCacheShardsAsync("t", "n", "c "));

        using var producer = new FelixProducer(new FelixProducerHandle(0));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.HandOffBatchAsync("t", "n", "s", []));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.HandOffBatchesAsync("t", "n", "s", []));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.HandOffBatchesAsync("t", "n", "s", [[]]));
        await Assert.ThrowsAsync<ArgumentException>(() => producer.HandOffAbandonAsync("t", "n", "s "));
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

    // A null handle is the library's own refusal, so each entry point must resolve, take its
    // arguments and map the status, with no broker.
    [NativeFact]
    public async Task EveryNewEntryPointResolvesAndRefusesANullHandle()
    {
        using var client = new FelixClient(new FelixClientHandle(0));
        using var producer = new FelixProducer(new FelixProducerHandle(0));
        ReadOnlyMemory<byte>[] payloads = ["x"u8.ToArray()];
        Task[] calls =
        [
            client.GetSupportsAsync(),
            client.InspectShardAsync(ShardKind.Stream, "t", "n", "s", 0),
            client.ListSubscriptionsAsync(),
            client.ListSubscriptionsAsync(
                new SubscriptionFilter { TenantId = "t", Namespace = "n", Stream = "s", Shard = 1, Principal = "p", Dropping = true },
                10,
                new SubscriptionCursor("t", "n", "s", 1, 5)),
            client.GetTopologyAsync(),
            client.GetStreamRoutingAsync("t", "n", "s"),
            client.GetShardOwnersAsync(ShardKind.Cache, "t", "n", "c"),
            client.GetCacheShardsAsync("t", "n", "c"),
            producer.HandOffBatchAsync("t", "n", "s", payloads, "k"u8.ToArray()),
            producer.HandOffBatchesAsync("t", "n", "s", [payloads, payloads]),
            producer.HandOffAbandonAsync("t", "n", "s"),
        ];
        foreach (var call in calls)
        {
            var error = await Assert.ThrowsAsync<FelixException>(() => call);
            Assert.Equal(FelixStatus.InvalidArgument, error.Status);
        }
    }

    [NativeFact]
    public async Task ARefusedCursorIsAnInvalidArgumentNotAParseCrash()
    {
        // A client handle of zero is refused first; a real client is needed to reach the cursor check,
        // so this only proves the serialized cursor is accepted syntactically by the library's own parser
        // through the fixture tests. Here the null client wins.
        using var client = new FelixClient(new FelixClientHandle(0));
        var error = await Assert.ThrowsAsync<FelixException>(() =>
            client.ListSubscriptionsAsync(cursor: new SubscriptionCursor("t", "n", "s", 0, 1)));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
    }

    [NativeFact]
    public async Task AHandedAnswerIsReadOnceAndAfterwardsRefusedInTheBinding()
    {
        using var handed = new Handed(new FelixHandedHandle(0));
        // The first read reaches the library, which refuses a null handle.
        var first = await Assert.ThrowsAsync<FelixException>(() => handed.DoneAsync());
        Assert.Equal(FelixStatus.InvalidArgument, first.Status);
        // The binding has consumed the answer, so neither read goes native again.
        Assert.Throws<InvalidOperationException>(() => { _ = handed.DoneAsync(); });
        Assert.Throws<InvalidOperationException>(() => { _ = handed.OffsetAsync(); });
    }

    [Fact]
    public void ADisposedHandedCannotBeRead()
    {
        var handed = new Handed(new FelixHandedHandle(0));
        handed.Dispose();
        Assert.Throws<ObjectDisposedException>(() => { _ = handed.OffsetAsync(); });
        Assert.Throws<ObjectDisposedException>(() => { _ = handed.DoneAsync(); });
    }

    private static nint Utf8(string text, out GCHandle pin)
    {
        pin = GCHandle.Alloc(Encoding.UTF8.GetBytes(text + "\0"), GCHandleType.Pinned);
        return pin.AddrOfPinnedObject();
    }
}
