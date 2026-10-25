using System.Diagnostics;
using System.Text;
using Xunit;
using Xunit.Sdk;

namespace Felix.Client.Tests;

/// <summary>
/// Connection faults produced by the fixture's link interposer, mirroring the C harness. Every test
/// runs its client through <c>link_addr</c>, always heals afterwards, and keys its records under a
/// run-unique prefix.
/// </summary>
[Collection(FaultCollection.Name)]
public class LinkFaultTests
{
    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(250);

    [LinkFixtureFact]
    [Scenario("fault.publish_through_a_dropped_link")]
    public Task PublishThroughADroppedLink() => Through("fault.publish_through_a_dropped_link");

    [LinkFixtureFact]
    [Scenario("fault.publish_through_a_reset_link")]
    public Task PublishThroughAResetLink() => Through("fault.publish_through_a_reset_link");

    [LinkFixtureFact]
    [Scenario("fault.publish_through_a_stalled_link")]
    public Task PublishThroughAStalledLink() => Through("fault.publish_through_a_stalled_link");

    [LinkFixtureFact]
    [Scenario("fault.subscription_through_a_dropped_link")]
    public Task SubscriptionThroughADroppedLink() => Through("fault.subscription_through_a_dropped_link");

    [LinkFixtureFact]
    [Scenario("fault.subscription_through_a_reset_link")]
    public Task SubscriptionThroughAResetLink() => Through("fault.subscription_through_a_reset_link");

    [LinkFixtureFact]
    [Scenario("fault.subscription_through_a_stalled_link")]
    public Task SubscriptionThroughAStalledLink() => Through("fault.subscription_through_a_stalled_link");

    [LinkFixtureFact]
    [Scenario("fault.cache_through_a_reset_link")]
    public Task CacheThroughAResetLink() => Through("fault.cache_through_a_reset_link");

    private static async Task Through(string id)
    {
        var f = FaultFixture.Load();
        Assert.True(f.Steps.TryGetValue(id, out var step), $"the fixture file has no usable step for {id}");
        Assert.True(step!.Records > 0 && step.AfterRecords >= 0 && step.AfterRecords < step.Records, $"{id} has an unusable step");
        var stamp = $"cs-{FaultControl.Run()}-{id["fault.".Length..]}";

        (int Code, string Body) healed;
        try
        {
            switch (step.During)
            {
                case "publish":
                    await PublishThroughFault(f, step, stamp);
                    break;
                case "cache":
                    await CacheThroughFault(f, step, stamp);
                    break;
                default:
                    await SubscribeThroughFault(f, step, stamp);
                    break;
            }
        }
        finally
        {
            // A client that resumed elsewhere can finish inside a drop's hold, and the next test
            // would connect into it.
            healed = await FaultControl.HealAsync(f);
        }

        Assert.True(healed.Code == 200, $"heal answered {healed.Code}: {healed.Body}");
    }

    private static async Task PublishThroughFault(FaultFixture f, FaultStep step, string stamp)
    {
        var acked = new List<string>();
        var stall = step.Fault == "stall";
        var posted = false;
        var recovered = false;

        await using (var linked = await f.ConnectAsync(f.LinkAddr!))
        {
            var began = Stopwatch.StartNew();
            double healedAt = 0;
            for (var i = 0; i < step.Records; i++)
            {
                if (i == step.AfterRecords)
                {
                    await FaultControl.InjectAsync(f, step);
                    posted = true;
                    healedAt = began.Elapsed.TotalSeconds + (step.HoldMs / 1000.0);
                }

                var payload = $"{stamp}-{i}";
                try
                {
                    await FaultControl.PublishAsync(linked, f, f.LinkStream!, payload);
                    acked.Add(payload);
                }
                catch (FelixException e)
                {
                    Assert.False(stall, $"publish {i} failed during a stall: {e.Status} ({e.Message})");
                }
            }

            // Once the fault is over, the client has to publish again.
            var wait = healedAt - began.Elapsed.TotalSeconds;
            if (healedAt > 0 && wait > 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(wait));
            }

            var after = $"{stamp}-after";
            var settling = Stopwatch.StartNew();
            while (!recovered && settling.Elapsed < FaultControl.Settle)
            {
                try
                {
                    await FaultControl.PublishAsync(linked, f, f.LinkStream!, after);
                    acked.Add(after);
                    recovered = true;
                }
                catch (FelixException)
                {
                    await Task.Delay(Poll);
                }
            }
        }

        Assert.True(posted, "the fault was never injected");
        Assert.True(recovered, "the client never published again after the fault");

        var missing = await FaultControl.UnreadAsync(f, f.LinkStream!, acked);
        Assert.True(missing == 0, $"{missing} of {acked.Count} acknowledged records were not in the stream");
    }

    private static async Task SubscribeThroughFault(FaultFixture f, FaultStep step, string stamp)
    {
        await using var linked = await f.ConnectAsync(f.LinkAddr!);
        await using var direct = await f.ConnectAsync();
        await using var sub = await linked.SubscribeAsync(f.Tenant, f.Namespace, f.LinkStream!, 0, StartPosition.Latest);

        var total = step.Records;
        var payloads = Enumerable.Range(0, total).Select(i => $"{stamp}-{i}").ToArray();
        for (var i = 0; i < step.AfterRecords; i++)
        {
            await FaultControl.PublishAsync(direct, f, f.LinkStream!, payloads[i]);
        }

        var stall = step.Fault == "stall";
        var injected = false;
        long? previous = null;
        var received = 0;
        var began = Stopwatch.StartNew();
        var deadline = FaultControl.Settle.TotalSeconds;
        while (received < total)
        {
            if (!injected && received == step.AfterRecords)
            {
                await FaultControl.InjectAsync(f, step);
                injected = true;
                deadline = began.Elapsed.TotalSeconds + (step.HoldMs / 1000.0) + FaultControl.Settle.TotalSeconds;
                for (var i = step.AfterRecords; i < total; i++)
                {
                    await FaultControl.PublishAsync(direct, f, f.LinkStream!, payloads[i]);
                }
            }

            Assert.True(
                began.Elapsed.TotalSeconds < deadline,
                $"neither resumed nor reported the loss: {received} of {total} records, then nothing");

            FelixEvent? ev;
            try
            {
                ev = await sub.ReadAsync(Poll);
            }
            catch (FelixException e) when (e.Status == FelixStatus.Closed)
            {
                throw new XunitException(
                    $"the subscription ended cleanly after {received} of {total} records; a dead connection must be resumed or reported, not read as the end of the stream");
            }
            catch (FelixException e)
            {
                // An error is the other right answer to a dead connection, but only once one was
                // caused: and never during a stall.
                Assert.True(injected && !stall, $"the subscription failed with no loss to report: {e.Status} ({e.Message})");
                return;
            }

            if (ev is null)
            {
                continue;
            }

            var got = Encoding.UTF8.GetString(ev.Payload.Span);
            if (!got.StartsWith(stamp, StringComparison.Ordinal))
            {
                // Not ours: another run's record on the shared stream.
                previous = null;
                continue;
            }

            Assert.True(got == payloads[received], $"record {received} was {got}: a gap, a duplicate or a reordering");
            Assert.True(ev.Offset is not null, "a durable stream delivered an event without an offset");
            Assert.True(previous is null || ev.Offset == previous + 1, $"offsets jumped: {previous} then {ev.Offset}");
            previous = ev.Offset;
            received++;
        }

        Assert.True(injected, "the fault was never injected");
    }

    private static async Task CacheThroughFault(FaultFixture f, FaultStep step, string stamp)
    {
        await using var linked = await f.ConnectAsync(f.LinkAddr!);
        var cache = linked.Cache(f.Tenant, f.Namespace, f.Cache);
        var acked = new List<string>();
        var counter = $"{stamp}-count";
        var added = 0;
        var posted = false;
        try
        {
            for (var i = 0; i < step.Records; i++)
            {
                if (i == step.AfterRecords)
                {
                    await FaultControl.InjectAsync(f, step);
                    posted = true;
                }

                var key = $"{stamp}-{i}";
                if (await Bounded(ct => cache.PutAsync(key, FaultControl.Bytes(key), null, ct)))
                {
                    acked.Add(key);
                }

                if (await Bounded(ct => cache.CounterAddAsync(counter, 1, ct)))
                {
                    added++;
                }
            }

            var after = $"{stamp}-after";
            var settling = Stopwatch.StartNew();
            var recovered = false;
            while (!recovered && settling.Elapsed < FaultControl.Settle)
            {
                if (await Bounded(ct => cache.PutAsync(after, FaultControl.Bytes(after), null, ct)))
                {
                    acked.Add(after);
                    recovered = true;
                }
                else
                {
                    await Task.Delay(Poll);
                }
            }

            Assert.True(posted, "the fault was never injected");
            Assert.True(recovered, "the client never wrote to the cache again after the fault");

            foreach (var key in acked)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
                var value = await cache.GetAsync(key, cts.Token);
                Assert.True(
                    value is not null && Encoding.UTF8.GetString(value.Value.Span) == key,
                    $"the acknowledged put of {key} did not read back");
            }

            using var read = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var count = await cache.CounterGetAsync(counter, read.Token) ?? 0;
            Assert.True(
                count >= added && count <= step.Records,
                $"the counter is {count} after {added} acknowledged adds of {step.Records}");
        }
        finally
        {
            foreach (var key in acked.Append(counter))
            {
                try
                {
                    await cache.DeleteAsync(key);
                }
                catch (Exception e)
                {
                    Console.Error.WriteLine($"fault cleanup: deleting {key} failed: {e.Message}");
                }
            }
        }
    }

    /// <summary>
    /// Runs a cache call and says whether it succeeded. A failure the broker or the link caused is
    /// an answer; a call still pending after a minute is a hang, which fails the test.
    /// </summary>
    private static async Task<bool> Bounded(Func<CancellationToken, Task> call)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        try
        {
            await call(cts.Token);
            return true;
        }
        catch (FelixException)
        {
            return false;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            throw new XunitException("a cache call was still pending after 60s");
        }
    }
}
