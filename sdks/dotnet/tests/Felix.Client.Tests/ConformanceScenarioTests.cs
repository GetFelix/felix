using System.Diagnostics;
using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Felix.Client.Tests;

/// <summary>As <see cref="FixtureFactAttribute"/>, and skips when the fixture has a single broker.</summary>
public sealed class MultiBrokerFixtureFactAttribute : FixtureFactAttribute
{
    public MultiBrokerFixtureFactAttribute()
    {
        if (Skip is null && Fixture.Load().Addrs.Length < 2)
        {
            Skip = "the fixture has one broker; a redirect and discovery need a second";
        }
    }
}

/// <summary>
/// Publish, subscribe, acknowledgement and routing scenarios against the fixture's durable stream,
/// mirroring <c>pubsub.c</c> in felix-capi. They share the stream with the other fixture tests, so
/// every payload carries a run-unique prefix and the tests rely on running one at a time within a
/// host (<c>DisableTestParallelization</c> in LeakTests.cs).
/// </summary>
public class ConformanceScenarioTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private readonly ITestOutputHelper output;

    public ConformanceScenarioTests(ITestOutputHelper output) => this.output = output;

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(FelixEvent ev) => Encoding.UTF8.GetString(ev.Payload.Span);

    private static async Task<FelixEvent> Next(FelixSubscription sub)
    {
        var ev = await sub.ReadAsync(Wait);
        Assert.NotNull(ev);
        return ev!;
    }

    /// <summary>Reads until an event carrying <paramref name="payload"/>; returns it and how many events were read.</summary>
    private static async Task<(FelixEvent Event, int Seen)> ReadUntil(FelixSubscription sub, string payload)
    {
        var seen = 0;
        var deadline = DateTime.UtcNow + Wait;
        while (DateTime.UtcNow < deadline)
        {
            var ev = await sub.ReadAsync(TimeSpan.FromSeconds(2));
            if (ev is null)
            {
                continue;
            }

            seen++;
            if (Text(ev) == payload)
            {
                return (ev, seen);
            }
        }

        throw new Xunit.Sdk.XunitException($"'{payload}' did not arrive within {Wait.TotalSeconds}s ({seen} other events read)");
    }

    private static Task<FelixClient> ConnectTo(Fixture f, string addr, string? token = null) =>
        FelixClient.ConnectAsync(
            new FelixClientOptions
            {
                Addrs = [addr],
                TenantId = f.Tenant,
                Token = token ?? f.Token,
                CaFile = f.CaFile,
            });

    [FixtureFact]
    [Scenario("pubsub.offsets_are_contiguous")]
    public async Task OffsetsOfConsecutiveEventsDifferByOne()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        const int count = 10;
        var prefix = $"cs-{Run()}-seq-";
        for (var i = 0; i < count; i++)
        {
            await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(prefix + i));
        }

        // Every event on a durable stream carries its offset, ours or not: a record between two of
        // ours still takes an offset, so the whole run must step by one.
        long? previous = null;
        var ours = 0;
        while (ours < count)
        {
            var ev = await Next(sub);
            Assert.NotNull(ev.Offset);
            if (previous is not null)
            {
                Assert.Equal(previous + 1, ev.Offset);
            }

            previous = ev.Offset;
            if (Text(ev).StartsWith(prefix, StringComparison.Ordinal))
            {
                Assert.Equal(prefix + ours, Text(ev));
                ours++;
            }
        }
    }

    [FixtureFact]
    [Scenario("pubsub.resume_from_offset", "reconnect.subscription_resumes_at_the_next_offset")]
    public async Task ASubscriptionResumesAtTheRequestedOffsetWithNoGapOrDuplicate()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var recs = Enumerable.Range(0, 3).Select(i => $"cs-{run}-rec{i}").ToArray();
        var offsets = new long[recs.Length];
        long last;

        var first = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        try
        {
            for (var i = 0; i < recs.Length; i++)
            {
                offsets[i] = (await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(recs[i])))!.Value;
            }

            // Consume up to a known offset, then lose the subscription.
            long? seenOffset = null;
            for (var i = 0; i < recs.Length; i++)
            {
                var ev = (await ReadUntil(first, recs[i])).Event;
                Assert.Equal(offsets[i], ev.Offset);
                seenOffset = ev.Offset;
            }

            last = seenOffset!.Value;
        }
        finally
        {
            await first.DisposeAsync();
        }

        // Published while nothing is subscribed; resuming at last + 1 must deliver it first.
        var resumed = $"cs-{run}-resumed";
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(resumed));
        await using (var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.FromOffset((ulong)(last + 1))))
        {
            var ev = await Next(sub);
            Assert.Equal(last + 1, ev.Offset);
            Assert.Equal(resumed, Text(ev));
        }

        // Resuming at a middle record starts there: the record before it does not come again, and
        // the one after it follows with nothing missing between.
        await using (var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.FromOffset((ulong)offsets[1])))
        {
            var ev = await Next(sub);
            Assert.Equal(recs[1], Text(ev));
            Assert.Equal(offsets[1], ev.Offset);
            var after = await Next(sub);
            Assert.Equal(offsets[1] + 1, after.Offset);
        }
    }

    [FixtureFact]
    [Scenario("pubsub.start_earliest")]
    public async Task EarliestStartsBeforeRecordsPublishedJustBeforeTheSubscription()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var before = $"cs-{run}-before";
        var marker = $"cs-{run}-earliest";
        var beforeOffset = await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(before));
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(marker));
        Assert.NotNull(beforeOffset);

        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Earliest);

        // Earliest is as far back as the log goes, which cannot be later than what was just written.
        Assert.NotNull(sub.StartOffset);
        Assert.True(sub.StartOffset <= beforeOffset, $"start {sub.StartOffset} is after {beforeOffset}");
        var (ev, seen) = await ReadUntil(sub, marker);
        Assert.True(seen > 1, "the earliest subscription began at the record published just before it");
        Assert.NotNull(ev.Offset);
    }

    [FixtureFact]
    [Scenario("client.subscription_ends_cleanly")]
    public async Task AReadPendingWhenTheSubscriptionIsFreedEndsRatherThanHangs()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        var read = sub.ReadAsync(Timeout.InfiniteTimeSpan);
        await Task.Delay(200);
        Assert.False(read.IsCompleted, "the read finished with nothing published");
        sub.Dispose();

        var finished = await Task.WhenAny(read, Task.Delay(Wait));
        Assert.Same(read, finished);
        var error = await Record.ExceptionAsync(() => read);
        Assert.True(
            error is OperationCanceledException or FelixException { Status: FelixStatus.Closed or FelixStatus.Cancelled },
            $"the pending read ended with {error?.GetType().Name ?? "a value"}: {error?.Message}");
    }

    [FixtureFact]
    [Scenario("client.concurrent_calls")]
    public async Task ConcurrentPublishesThroughOneClientAllLand()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        const int workers = 8;
        const int each = 10;
        var run = Run();
        var prefix = $"cs-{run}-conc-";
        var jobs = Enumerable.Range(0, workers).Select(w => Task.Run(async () =>
        {
            var offsets = new List<long?>();
            for (var i = 0; i < each; i++)
            {
                offsets.Add(await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"{prefix}{w}-{i}")));
            }

            return offsets;
        })).ToArray();
        var results = await Task.WhenAll(jobs);

        // Each acknowledgement names its own offset: two publishes never share one.
        var all = results.SelectMany(r => r).ToList();
        Assert.All(all, o => Assert.NotNull(o));
        Assert.Equal(workers * each, all.Distinct().Count());

        // Arrival order across tasks is not defined; only that every payload arrives intact, once.
        var expected = new HashSet<string>(StringComparer.Ordinal);
        for (var w = 0; w < workers; w++)
        {
            for (var i = 0; i < each; i++)
            {
                expected.Add($"{prefix}{w}-{i}");
            }
        }

        var remaining = new HashSet<string>(expected, StringComparer.Ordinal);
        var deadline = DateTime.UtcNow + Wait;
        while (remaining.Count > 0 && DateTime.UtcNow < deadline)
        {
            var ev = await sub.ReadAsync(TimeSpan.FromSeconds(2));
            if (ev is null)
            {
                continue;
            }

            var text = Text(ev);
            if (text.StartsWith(prefix, StringComparison.Ordinal))
            {
                Assert.True(expected.Contains(text), $"unexpected payload '{text}'");
                Assert.True(remaining.Remove(text), $"'{text}' arrived twice");
            }
        }

        Assert.True(remaining.Count == 0, $"{remaining.Count} of {expected.Count} concurrent publishes never arrived");
    }

    [FixtureFact]
    [Scenario("ack.per_message_means_the_broker_accepted_it")]
    public async Task AnAcknowledgedPublishReadsBackAtTheOffsetItNamed()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var payload = $"cs-{Run()}-acked";

        var offset = await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(payload), ack: AckMode.PerMessage);
        Assert.NotNull(offset);

        // With no wait in between, the record is already there at that offset.
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.FromOffset((ulong)offset!.Value));
        var ev = await Next(sub);
        Assert.Equal(payload, Text(ev));
        Assert.Equal(offset, ev.Offset);
    }

    [FixtureFact]
    [Scenario("ack.none_does_not_wait_for_the_broker")]
    public async Task UnacknowledgedPublishesCompleteWithoutABrokerRoundTrip()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();

        // Warm the connection so setup is not counted, then time 20 publishes. The bound is loose
        // enough for a loaded machine and tight enough to catch a client that waits anyway.
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"cs-{run}-warm"));
        var clock = Stopwatch.StartNew();
        for (var i = 0; i < 20; i++)
        {
            var offset = await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"cs-{run}-none-{i}"), ack: AckMode.None);
            Assert.Null(offset);
        }

        clock.Stop();
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"20 unacknowledged publishes took {clock.Elapsed.TotalSeconds:F2}s");
    }

    [FixtureFact]
    [Scenario("retry.ambiguous_outcomes_are_not_silently_retried")]
    public async Task APlainPublishIsAtMostOnceAndRetryingIsAnExplicitChoice()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        var run = Run();
        var once = $"cs-{run}-once";
        var sentinel = $"cs-{run}-sentinel";

        // The default is at most once, and it lands exactly once: the sentinel comes after one copy.
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(once), delivery: Delivery.AtMostOnce);
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(sentinel));
        var copies = 0;
        while (true)
        {
            var text = Text(await Next(sub));
            if (text == once)
            {
                copies++;
            }
            else if (text == sentinel)
            {
                break;
            }
        }

        Assert.Equal(1, copies);

        // Re-sending is opt-in and works; a choice the library does not know is refused, not defaulted.
        Assert.NotNull(await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"cs-{run}-at-least"), delivery: Delivery.AtLeastOnce));
        var refused = await Assert.ThrowsAsync<FelixException>(
            () => client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"cs-{run}-bad"), delivery: (Delivery)7));
        Assert.Equal(FelixStatus.InvalidArgument, refused.Status);
    }

    [FixtureFact]
    [Scenario("retry.terminal_failures_are_not_retried")]
    public async Task AnUnauthorizedPublishIsRefusedPromptlyAndStaysAuth()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync(f.UnauthorizedToken);

        // Promptly is half of it: retrying a refusal no retry can fix would spend the whole backoff
        // budget reaching the same answer.
        var clock = Stopwatch.StartNew();
        var error = await Assert.ThrowsAsync<FelixException>(
            () => client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"cs-{Run()}-not-allowed")));
        clock.Stop();

        Assert.Equal(FelixStatus.Auth, error.Status);
        Assert.False(error.IsRetryable);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), $"the refusal took {clock.Elapsed.TotalSeconds:F2}s, so it was retried");
    }

    // A lag cannot be forced here (it takes a flood past the broker's queue), so this checks what a
    // healthy watch delivers and that a lag item, if the broker did end the watch, names where to
    // resume. That a lag reaches the application as an item is covered offline by CacheTests.
    [FixtureFact]
    [Scenario("watch.lag_is_reported_rather_than_silent")]
    public async Task AWatchDeliversItsChangeAndALagItemNamesWhereToResume()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var cache = client.Cache(f.Tenant, f.Namespace, f.SingleShardCache);
        var key = $"cs-{Run()}-lag";
        try
        {
            await using var watch = await cache.WatchPrefixAsync(key);
            await cache.PutAsync(key, Bytes("v"));
            var item = await watch.ReadAsync(Wait);
            Assert.NotNull(item);
            Assert.True(item!.Kind is WatchItemKind.Change or WatchItemKind.Lagged or WatchItemKind.ShardMoved, $"kind {item.Kind}");
            if (item.Kind == WatchItemKind.Lagged)
            {
                Assert.NotNull(item.ResumeFrom);
            }
        }
        finally
        {
            try
            {
                await cache.DeleteAsync(key);
            }
            catch (Exception e)
            {
                output.WriteLine($"cleanup of {key} failed: {e.Message}");
            }
        }
    }

    [MultiBrokerFixtureFact]
    [Scenario("reconnect.discovers_brokers_from_one_seed")]
    public async Task OneSeedIsEnoughToLearnTheCluster()
    {
        var f = Fixture.Load();
        await using var client = await ConnectTo(f, f.Addrs[0]);

        var endpoints = await client.GetEndpointsAsync();
        Assert.True(endpoints.Length > 1, $"the client knows {endpoints.Length} endpoint(s) after one seed; it never learned the cluster");
        Assert.All(endpoints, e => Assert.True(e.LastIndexOf(':') > 0 && e.LastIndexOf(':') < e.Length - 1, $"'{e}' has no port"));
    }

    [MultiBrokerFixtureFact]
    [Scenario("redirect.follows_to_the_shard_owner", "redirect.is_not_reported_as_a_failure")]
    public async Task EveryBrokerAskedAloneServesTheShardWhoeverOwnsIt()
    {
        var f = Fixture.Load();
        var run = Run();

        // A shard has one owner, so with two or more brokers each asked alone, at least one is not
        // the owner and the request has to be routed. A redirect that surfaced would throw here.
        for (var i = 0; i < f.Addrs.Length; i++)
        {
            await using var client = await ConnectTo(f, f.Addrs[i]);
            await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
            var payload = $"cs-{run}-redirect-{i}";
            Assert.NotNull(await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(payload)));
            Assert.Equal(payload, Text((await ReadUntil(sub, payload)).Event));
        }
    }

    [MultiBrokerFixtureFact]
    [Scenario("redirect.carries_the_start_offset_through_every_hop")]
    public async Task ARedirectedSubscriptionBeginsAtTheOffsetAskedFor()
    {
        var f = Fixture.Load();
        var run = Run();
        const int history = 6;
        var payloads = Enumerable.Range(0, history).Select(h => $"cs-{run}-history-{h}").ToArray();
        var offsets = new long[history];

        // Records exist before the subscription does, so a redirect that dropped the start offset
        // would begin at the tail and never see the first.
        await using (var writer = await ConnectTo(f, f.Addrs[0]))
        await using (var watch = await writer.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest))
        {
            foreach (var payload in payloads)
            {
                Assert.NotNull(await writer.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(payload)));
            }

            for (var h = 0; h < history; h++)
            {
                var ev = (await ReadUntil(watch, payloads[h])).Event;
                Assert.NotNull(ev.Offset);
                offsets[h] = ev.Offset!.Value;
            }
        }

        for (var i = 0; i < f.Addrs.Length; i++)
        {
            await using var client = await ConnectTo(f, f.Addrs[i]);
            await using var resumed = await client.SubscribeAsync(
                f.Tenant, f.Namespace, f.Stream, 0, StartPosition.FromOffset((ulong)offsets[0]));
            Assert.Equal(offsets[0], resumed.StartOffset);
            for (var h = 0; h < history; h++)
            {
                var ev = await Next(resumed);
                Assert.True(ev.Offset == offsets[h], $"through {f.Addrs[i]} record {h} read at {ev.Offset}, asked for {offsets[h]}");
                Assert.Equal(payloads[h], Text(ev));
            }
        }
    }
}
