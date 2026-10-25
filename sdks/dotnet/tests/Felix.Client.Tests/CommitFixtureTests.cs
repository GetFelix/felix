using System.Text;
using Xunit;
using Xunit.Abstractions;

namespace Felix.Client.Tests;

/// <summary>
/// Atomic commits, conditional writes, state reads and idempotent producers against the fixture.
/// Like the C scenarios, they authenticate with the fixture's token. Each test keys under a run-unique
/// name so another test's entities never meet its own. Commit state cannot be deleted by a name
/// alone, so the cleanup deletes through a commit of its own.
/// </summary>
public class CommitFixtureTests
{
    private readonly ITestOutputHelper output;

    public CommitFixtureTests(ITestOutputHelper output) => this.output = output;

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    private static string Run() => Guid.NewGuid().ToString("N")[..12];

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte>? value) => Encoding.UTF8.GetString(value!.Value.Span);

    /// <summary>Best-effort cleanup that never masks the failure being reported.</summary>
    private async Task DeleteState(FelixClient client, Fixture f, string entity, uint? shard, params string[] keys)
    {
        foreach (var key in keys)
        {
            try
            {
                await client.CommitAsync(
                    f.Tenant,
                    f.Namespace,
                    Bytes(entity),
                    [CommitOp.Publish(f.Stream, Bytes("cleanup")), CommitOp.Delete(f.Stream, key)],
                    shard);
            }
            catch (Exception e)
            {
                output.WriteLine($"cleanup of state key '{key}' failed: {e.Message}");
            }
        }
    }

    /// <summary>
    /// The last shard of the stream. Named-shard tests use it, so they need no more than one shard
    /// and never name one the stream lacks.
    /// </summary>
    private static async Task<uint> LastShard(FelixClient client, Fixture f)
    {
        var count = await client.GetShardCountAsync(f.Tenant, f.Namespace, f.Stream);
        Assert.True(count >= 1, $"the stream reports {count} shards");
        return count - 1;
    }

    private static Task<long> Commit(FelixClient client, Fixture f, string entity, string eventText, string key, string? value, uint? shard = null) =>
        client.CommitAsync(
            f.Tenant,
            f.Namespace,
            Bytes(entity),
            [CommitOp.Publish(f.Stream, Bytes(eventText)), value is null ? CommitOp.Delete(f.Stream, key) : CommitOp.Put(f.Stream, key, Bytes(value))],
            shard);

    /// <summary>
    /// A write at a stale offset is refused with the tail; one at the tail lands there; the same again is
    /// refused. Another writer can move the tail between the probe and the write, so that step is retried.
    /// </summary>
    private static async Task CheckConditional(string what, Func<long, Task<ConditionalWrite>> write)
    {
        ConditionalWrite landed = default;
        long tail = 0;
        for (var attempt = 0; attempt < 5; attempt++)
        {
            // No shard's tail is this large, so the probe is always stale.
            var stale = await write(long.MaxValue);
            Assert.False(stale.Written, $"{what} at a stale offset was written");

            tail = stale.Offset;
            landed = await write(tail);
            if (landed.Written)
            {
                break;
            }
        }

        Assert.True(landed.Written, $"{what} at the tail was refused every time; last tail {tail}");
        Assert.Equal(tail, landed.Offset);

        // The tail has moved past the write, whoever else is writing.
        var again = await write(tail);
        Assert.False(again.Written, $"{what} again at {tail} was written");
        Assert.True(again.Offset > landed.Offset, $"{what} again reported tail {again.Offset}, not past {landed.Offset}");
    }

    [FixtureFact]
    [Scenario("commit.state_get_returns_the_version")]
    public async Task AStateChangeReadsBackAtTheCommitsOffsetAndDeleteLeavesItAbsent()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var key = $"cm-{Run()}-a";
        try
        {
            var offset = await Commit(client, f, key, "placed", key, "placed");
            var state = await client.GetStateAsync(f.Tenant, f.Namespace, f.Stream, key, Bytes(key));
            Assert.Equal("placed", Text(state.Value));
            Assert.Equal(offset, state.Version);
            Assert.NotNull(state.AsOf);
            Assert.True(state.AsOf >= offset);

            await Commit(client, f, key, "cancelled", key, null);
            var gone = await client.GetStateAsync(f.Tenant, f.Namespace, f.Stream, key, Bytes(key));
            Assert.Null(gone.Value);

            // A key never written is absent too, not empty.
            var never = await client.GetStateAsync(f.Tenant, f.Namespace, f.Stream, key + "-never", Bytes(key));
            Assert.Null(never.Value);
        }
        finally
        {
            await DeleteState(client, f, key, null, key);
        }
    }

    [FixtureFact]
    [Scenario("commit.expected_offset_refuses_a_stale_write")]
    public async Task AConditionalCommitIsRefusedOnceTheShardHasMovedOn()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var byEntity = $"cm-{run}-if";
        var byShard = $"cm-{run}-ifs";
        var shard = await LastShard(client, f);
        try
        {
            await CheckConditional("commit_if", expected => client.CommitIfAsync(
                f.Tenant,
                f.Namespace,
                Bytes(byEntity),
                [CommitOp.Publish(f.Stream, Bytes(byEntity)), CommitOp.Put(f.Stream, byEntity, Bytes("cond"))],
                expected));
            await CheckConditional("commit_if_on_shard", expected => client.CommitIfAsync(
                f.Tenant,
                f.Namespace,
                Bytes(byShard),
                [CommitOp.Publish(f.Stream, Bytes(byShard)), CommitOp.Put(f.Stream, byShard, Bytes("cond"))],
                expected,
                shard: shard));
        }
        finally
        {
            await DeleteState(client, f, byEntity, null, byEntity);
            await DeleteState(client, f, byShard, shard, byShard);
        }
    }

    [FixtureFact]
    [Scenario("explicit_shard.commit_lands_on_the_named_shard")]
    public async Task ACommitOnANamedShardIsDeliveredThereAndReadBackFromIt()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var entity = $"cm-{run}-shard";
        var text = $"cm-{run}-event";
        var shard = await LastShard(client, f);
        try
        {
            var offset = await Commit(client, f, entity, text, entity, "shard-value", shard: shard);

            // The commit's event is on the named shard, at the offset the commit answered with.
            await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, shard, StartPosition.FromOffset((ulong)offset));
            var ev = await sub.ReadAsync(Wait);
            Assert.NotNull(ev);
            Assert.Equal(text, Encoding.UTF8.GetString(ev!.Payload.Span));
            Assert.Equal(offset, ev.Offset);

            var state = await client.GetStateAsync(f.Tenant, f.Namespace, f.Stream, entity, shard: shard);
            Assert.Equal("shard-value", Text(state.Value));
            Assert.Equal(offset, state.Version);
        }
        finally
        {
            await DeleteState(client, f, entity, shard, entity);
        }
    }

    [FixtureFact]
    [Scenario("commit.returns_the_offset", "commit.negotiated")]
    public async Task AnEntityKeyedCommitsEventIsReadBackAtTheReturnedOffset()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var entity = $"cm-{run}-ent";
        var text = $"cm-{run}-ent-event";
        var count = await client.GetShardCountAsync(f.Tenant, f.Namespace, f.Stream);
        Assert.True(count >= 1);

        // The entity key picks the shard, so watch every shard from its tail before committing.
        var subs = new List<FelixSubscription>();
        try
        {
            for (uint shard = 0; shard < count; shard++)
            {
                subs.Add(await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, shard, StartPosition.Latest));
            }

            var offset = await Commit(client, f, entity, text, entity, "entity-value");

            FelixEvent? found = null;
            var deadline = DateTime.UtcNow + Wait;
            while (found is null && DateTime.UtcNow < deadline)
            {
                foreach (var sub in subs)
                {
                    var ev = await sub.ReadAsync(TimeSpan.FromMilliseconds(100));
                    if (ev is not null && Encoding.UTF8.GetString(ev.Payload.Span) == text)
                    {
                        found = ev;
                        break;
                    }
                }
            }

            Assert.NotNull(found);
            Assert.Equal(offset, found!.Offset);
        }
        finally
        {
            foreach (var sub in subs)
            {
                await sub.DisposeAsync();
            }

            await DeleteState(client, f, entity, null, entity);
        }
    }

    [FixtureFact]
    [Scenario("publish_if.lands_at_the_expected_offset", "publish_if.refused_with_the_tail")]
    public async Task AConditionalPublishLandsAtTheTailAndTheSecondAtThatOffsetIsRefused()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var key = $"cm-{Run()}-pub";
        ReadOnlyMemory<byte>[] payloads = [Bytes("pi-1"), Bytes("pi-2")];

        await CheckConditional("publish_if", expected => client.PublishIfAsync(f.Tenant, f.Namespace, f.Stream, payloads, expected, Bytes(key)));
        var shard = await LastShard(client, f);
        await CheckConditional("publish_if_on_shard", expected => client.PublishIfAsync(f.Tenant, f.Namespace, f.Stream, payloads, expected, shard: shard));
    }

    [FixtureFact]
    [Scenario("commit.exactly_one_event")]
    public async Task ACommitWithNoEventOrTwoIsRefusedBeforeItIsSent()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var key = Bytes($"cm-{Run()}-bad");

        var two = await Assert.ThrowsAsync<FelixException>(() => client.CommitAsync(
            f.Tenant, f.Namespace, key, [CommitOp.Publish(f.Stream, default), CommitOp.Enqueue(f.Stream, default)]));
        Assert.Equal(FelixStatus.InvalidArgument, two.Status);

        var none = await Assert.ThrowsAsync<FelixException>(() => client.CommitAsync(
            f.Tenant, f.Namespace, key, [CommitOp.Put(f.Stream, "k", default)]));
        Assert.Equal(FelixStatus.InvalidArgument, none.Status);

        var empty = await Assert.ThrowsAsync<FelixException>(() => client.CommitAsync(f.Tenant, f.Namespace, key, []));
        Assert.Equal(FelixStatus.InvalidArgument, empty.Status);
    }

    [FixtureFact]
    [Scenario("commit.another_stream_is_refused")]
    public async Task AnOperationOnAnotherStreamIsRefusedBeforeItIsSent()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var key = Bytes($"cm-{Run()}-split");

        var error = await Assert.ThrowsAsync<FelixException>(() => client.CommitAsync(
            f.Tenant, f.Namespace, key, [CommitOp.Publish(f.Stream, default), CommitOp.Put(f.MissingStream, "k", default)]));
        Assert.Equal(FelixStatus.InvalidArgument, error.Status);
        Assert.Contains(f.MissingStream, error.Message);
    }

    [FixtureFact]
    public async Task AProducerPublishesIncreasingOffsetsSettlesAndClosesGracefully()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var run = Run();
        var producer = await client.CreateProducerAsync();
        try
        {
            var id = producer.GetId();
            Assert.NotEqual(0UL, id);
            Assert.Equal(id, producer.GetIdFor(f.Tenant, f.Namespace, f.Stream));

            // Single-record publishes get increasing offsets, when the broker reports them.
            long? previous = null;
            for (var i = 0; i < 3; i++)
            {
                var offset = await producer.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"pr-{run}-one{i}"));
                if (offset is { } o)
                {
                    Assert.True(previous is null || o > previous, $"offset {o} did not follow {previous}");
                    previous = o;
                }
            }

            await producer.PublishBatchAsync(f.Tenant, f.Namespace, f.Stream, [Bytes($"pr-{run}-k0"), Bytes($"pr-{run}-k1")], Bytes($"pk-{run}"));

            ReadOnlyMemory<byte>[] two = [Bytes($"pr-{run}-m0"), Bytes($"pr-{run}-m1")];
            ReadOnlyMemory<byte>[] one = [Bytes($"pr-{run}-m2")];
            await producer.PublishBatchesAsync(f.Tenant, f.Namespace, f.Stream, [two, one]);

            // Nothing is in doubt, so giving up is a no-op that succeeds.
            await producer.AbandonInDoubtAsync(f.Tenant, f.Namespace, f.Stream);

            // Closing waits for what was sent; the handle then reports CLOSED.
            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            var late = await Assert.ThrowsAsync<FelixException>(() => producer.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes($"pr-{run}-late")));
            Assert.Equal(FelixStatus.Closed, late.Status);
            Assert.Equal(FelixStatus.Closed, Assert.Throws<FelixException>(() => producer.GetId()).Status);
            var second = await Assert.ThrowsAsync<FelixException>(() => producer.CloseAsync());
            Assert.Equal(FelixStatus.Closed, second.Status);
        }
        finally
        {
            producer.Dispose();
        }
    }

    [FixtureFact]
    public async Task DisposingAProducerClosesItGracefullyAndASecondDisposeIsSafe()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();
        var text = $"pr-{Run()}-dispose";
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);
        var producer = await client.CreateProducerAsync();
        try
        {
            await producer.PublishAsync(f.Tenant, f.Namespace, f.Stream, Bytes(text));
        }
        finally
        {
            await producer.DisposeAsync();
        }

        // Closed and freed, in either order, as many times as cleanup runs.
        await producer.DisposeAsync();
        producer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => producer.GetId());

        // What it published before the dispose was delivered.
        var ev = await sub.ReadAsync(Wait);
        Assert.NotNull(ev);
        Assert.Equal(text, Encoding.UTF8.GetString(ev!.Payload.Span));
    }

    [FixtureFact]
    [Scenario("client.close_is_idempotent")]
    public async Task ClosingAClientOrASubscriptionTwiceIsSafe()
    {
        var f = Fixture.Load();
        var client = await f.ConnectAsync();
        var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, f.Stream, 0, StartPosition.Latest);

        await sub.DisposeAsync();
        await sub.DisposeAsync();
        sub.Dispose();

        await client.DisposeAsync();
        await client.DisposeAsync();
        client.Dispose();
    }

    [FixtureFact]
    public async Task AProducerFreedWithoutACloseAndACancelledCreateLeaveNothingBehind()
    {
        var f = Fixture.Load();
        await using var client = await f.ConnectAsync();

        var producer = await client.CreateProducerAsync();
        producer.Dispose();

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.CreateProducerAsync(cts.Token));
    }
}
