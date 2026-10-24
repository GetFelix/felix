using System.Text;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Consumer groups against the fixture. Each test makes groups under its own names. The queue
/// test counts records on shard 0, which relies on tests running one at a time within a host
/// (<c>DisableTestParallelization</c> in LeakTests.cs) and on CI running one framework at a time,
/// so nothing else publishes there meanwhile.
/// </summary>
public class GroupFixtureTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static async Task<GroupRecord[]> Collect(FelixGroup group, int count)
    {
        var got = new List<GroupRecord>();
        var deadline = DateTime.UtcNow + Wait;
        while (got.Count < count && DateTime.UtcNow < deadline)
        {
            got.AddRange(await group.PollAsync((uint)(count - got.Count), TimeSpan.FromSeconds(2)));
        }

        Assert.Equal(count, got.Count);
        return [.. got];
    }

    private static string Text(GroupRecord record) => Encoding.UTF8.GetString(record.Payload.Span);

    [GroupFixtureFact]
    [Scenario(
        "queue.poll_returns_claimed_records",
        "queue.unacknowledged_records_are_redelivered",
        "queue.acknowledged_records_are_not_redelivered",
        "queue.dead_letters_are_listable_and_redrivable")]
    public async Task AQueueClaimsSettlesAndDeadLetters()
    {
        var f = Fixture.Load();
        var operatorToken = f.GroupOperatorToken!;

        await using var client = await f.ConnectAsync();
        await using var admin = await f.ConnectAsync(operatorToken);
        var run = Run();
        var name = $"cs-queue-{run}";
        var consumer = $"consumer-{run}";
        var asAdmin = admin.JoinGroup(f.Tenant, f.Namespace, f.Stream, 0, name);
        var group = client.JoinGroup(f.Tenant, f.Namespace, f.Stream, 0, name, consumer);

        // Creating reports the move; creating again leaves the group alone.
        Assert.True((await asAdmin.CreateAsync(StartPosition.Earliest)).Moved);
        Assert.False((await asAdmin.CreateAsync(StartPosition.Earliest)).Moved);
        await asAdmin.SeekAsync(StartPosition.Latest);

        var payloads = Enumerable.Range(0, 4).Select(i => $"cs-{run}-rec{i}").ToArray();
        await client.PublishBatchAsync(
            f.Tenant, f.Namespace, f.Stream, payloads.Take(3).Select(p => (ReadOnlyMemory<byte>)Encoding.UTF8.GetBytes(p)).ToArray());

        var got = await Collect(group, 3);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(payloads[i], Text(got[i]));
            Assert.Equal(1u, got[i].Attempts);
            Assert.Equal(0u, got[i].Shard);
        }

        // A nack with no delay hands the first record out again, one attempt on.
        await group.NackAsync(got[0]);
        var again = (await Collect(group, 1))[0];
        Assert.Equal(got[0].Offset, again.Offset);
        Assert.Equal(2u, again.Attempts);
        Assert.True(await group.ExtendAsync(again, TimeSpan.FromSeconds(30)) > TimeSpan.Zero);
        await group.AckAsync(again);

        await group.AckAsync(got[1]);
        var after = await group.PollAsync(4, TimeSpan.FromSeconds(1));
        Assert.DoesNotContain(after, r => r.Offset == got[0].Offset || r.Offset == got[1].Offset);

        // A dead letter is listed, can be driven back, and is delivered again.
        await group.DeadLetterAsync(got[2]);
        Assert.Equal([got[2].Offset], await group.DeadLettersAsync());
        Assert.Equal(1, (await group.DescribeAsync()).DeadLetters);
        await asAdmin.RedriveAsync(got[2].Offset);
        var redriven = (await Collect(group, 1))[0];
        Assert.Equal(got[2].Offset, redriven.Offset);
        await group.AckAsync(redriven);

        // A dead letter can instead be discarded.
        await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes(payloads[3]));
        var last = (await Collect(group, 1))[0];
        await group.DeadLetterAsync(last);
        await asAdmin.DiscardAsync(last.Offset);
        Assert.Empty(await group.DeadLettersAsync());

        // Nothing is owed any more, and an empty poll is an answer, not an error.
        Assert.Empty(await group.PollAsync(1, TimeSpan.FromMilliseconds(500)));
        var info = await group.DescribeAsync();
        Assert.NotNull(info.Committed);
        Assert.True(info.Committed <= info.Tail);
        Assert.Equal(0, info.DeadLetters);

        // Deleting says whether the group was there.
        Assert.True(await asAdmin.DeleteAsync());
        Assert.False(await asAdmin.DeleteAsync());
    }

    [FixtureFact]
    [Scenario("sharded.reports_the_shard_count")]
    public async Task TheShardCountIsAskedOfTheBroker()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        Assert.Equal(4u, await client.GetShardCountAsync(f.Tenant, f.Namespace, f.Stream));
        // Zero, not one: an unknown stream must not read as a real single-shard one.
        Assert.Equal(0u, await client.GetShardCountAsync(f.Tenant, f.Namespace, f.MissingStream));
    }

    [GroupFixtureFact]
    public async Task ACancelledPollEndsAsCancelled()
    {
        var f = Fixture.Load();
        var operatorToken = f.GroupOperatorToken!;

        await using var client = await f.ConnectAsync();
        await using var admin = await f.ConnectAsync(operatorToken);
        var name = $"cs-idle-{Run()}";
        await admin.JoinGroup(f.Tenant, f.Namespace, f.Stream, 1, name).CreateAsync(StartPosition.Latest);
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => client.JoinGroup(f.Tenant, f.Namespace, f.Stream, 1, name).PollAsync(1, TimeSpan.FromSeconds(10), cancellationToken: cts.Token));
        }
        finally
        {
            try
            {
                await admin.JoinGroup(f.Tenant, f.Namespace, f.Stream, 1, name).DeleteAsync();
            }
            catch (Exception)
            {
                // Best effort; it must not mask a failure above.
            }
        }
    }

    [FixtureFact]
    public async Task PollArgumentsTheCallRefusesAreTyped()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var group = client.JoinGroup(f.Tenant, f.Namespace, f.Stream, 1, $"cs-args-{Run()}");
        var noReclaim = await Assert.ThrowsAsync<FelixException>(() => group.PollAsync(1, reclaim: true));
        Assert.Equal(FelixStatus.InvalidArgument, noReclaim.Status);
        var none = await Assert.ThrowsAsync<FelixException>(() => group.PollAsync(0));
        Assert.Equal(FelixStatus.InvalidArgument, none.Status);
    }

    [GroupFixtureFact]
    public async Task AShardedGroupCoversEveryShard()
    {
        var f = Fixture.Load();
        var operatorToken = f.GroupOperatorToken!;

        await using var client = await f.ConnectAsync(null, publishers: false, timestamps: true);
        await using var admin = await f.ConnectAsync(operatorToken);
        var run = Run();
        var name = $"cs-sharded-{run}";

        // An offset means something different on each shard, so it is refused.
        await Assert.ThrowsAsync<FelixException>(
            () => admin.CreateStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name, StartPosition.FromOffset(5)));

        var created = await admin.CreateStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name, StartPosition.Latest);
        Assert.Equal(4, created.Length);
        Assert.All(created, p => Assert.True(p.Moved));
        var again = await admin.CreateStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name, StartPosition.Latest);
        Assert.All(again, p => Assert.False(p.Moved));
        Assert.Equal(4, (await admin.SeekStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name, StartPosition.Latest)).Length);

        try
        {
            await using var group = await client.OpenShardedGroupAsync(f.Tenant, f.Namespace, f.Stream, name);
            Assert.Equal(4u, group.ShardCount);

            // Keys spread the records over the shards; the poll finds them by visiting each.
            var expected = new HashSet<string>();
            for (var i = 0; i < 64; i++)
            {
                var payload = $"cs-{run}-k{i}";
                expected.Add(payload);
                await client.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes(payload), key: BitConverter.GetBytes(i));
            }

            var seen = new HashSet<string>();
            var shards = new HashSet<uint>();
            var deadline = DateTime.UtcNow + Wait;
            while (seen.Count < expected.Count && DateTime.UtcNow < deadline)
            {
                foreach (var record in await group.PollAsync(16))
                {
                    var text = Text(record);
                    Assert.Contains(text, expected);
                    Assert.NotNull(record.Timestamp);
                    shards.Add(record.Shard);
                    seen.Add(text);
                    // The settle goes to the shard the record came from.
                    await group.AckAsync(record);
                }
            }

            Assert.Equal(expected.Count, seen.Count);
            Assert.Equal(4, shards.Count);

            var infos = await admin.DescribeStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name);
            Assert.Equal(4, infos.Length);
            Assert.All(infos, i => Assert.True(i.Committed <= i.Tail));
        }
        catch
        {
            // Best effort: the failure being reported is the test's, not the cleanup's.
            try
            {
                await admin.DeleteStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name);
            }
            catch (Exception)
            {
            }

            throw;
        }

        Assert.True(await admin.DeleteStreamGroupAsync(f.Tenant, f.Namespace, f.Stream, name));
    }

    [FixtureFact]
    public async Task EventsCarryAPublisherAndATimeOnlyWhenAsked()
    {
        var f = Fixture.Load();
        await using var plain = await f.ConnectAsync();
        await using var asking = await f.ConnectAsync(null, publishers: true, timestamps: true);
        await using var plainSub = await plain.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 1, StartPosition.Latest);
        await using var askingSub = await asking.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 1, StartPosition.Latest);

        var before = DateTimeOffset.UtcNow;
        // Shard 1 holds records of key "a"? Not guaranteed, so publish keyed records until one lands there.
        var payload = $"cs-meta-{Run()}";
        FelixEvent? plainEvent = null;
        FelixEvent? askingEvent = null;
        for (var key = 0; key < 64 && askingEvent is null; key++)
        {
            await plain.PublishAsync(f.Tenant, f.Namespace, f.Stream, Encoding.UTF8.GetBytes(payload), key: BitConverter.GetBytes(key));
            askingEvent = await askingSub.ReadAsync(TimeSpan.FromMilliseconds(500));
            plainEvent = await plainSub.ReadAsync(TimeSpan.FromMilliseconds(500));
        }

        Assert.NotNull(askingEvent);
        Assert.NotNull(plainEvent);
        Assert.Null(plainEvent!.Publisher);
        Assert.Null(plainEvent.TimestampMicros);
        Assert.NotNull(askingEvent!.Timestamp);
        Assert.InRange(askingEvent.Timestamp!.Value, before - TimeSpan.FromSeconds(5), DateTimeOffset.UtcNow + TimeSpan.FromSeconds(5));
        if (f.RecordsPublishers)
        {
            Assert.False(string.IsNullOrEmpty(askingEvent.Publisher));
        }
    }
}
