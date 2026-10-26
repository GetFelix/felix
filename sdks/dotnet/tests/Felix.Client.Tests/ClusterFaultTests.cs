using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Faults the fixture's control endpoint produces on the shared cluster: a fenced shard and a leader
/// cut off from its followers. Both heal afterwards and wait for the stream to take a publish again.
/// </summary>
[Collection(FaultCollection.Name)]
public class ClusterFaultTests
{
    [ShardFixtureFact]
    [Scenario("error.shard_unavailable_is_retryable", "retry.deadline_bounds_an_operation")]
    public async Task AFencedShardRefusesAndSaysItIsSafeToRetry()
    {
        var f = FaultFixture.Load();
        var run = FaultControl.Run();
        try
        {
            // Published straight to the fenced owner: through another broker the answer would be
            // about the forward.
            var (code, body) = await FaultControl.PostAsync(f, "/fence");
            FaultControl.RequireFault("/fence", code, body);
            var addr = FaultControl.AddrIn(body);
            Assert.True(addr is not null, $"fence answered no broker address: {body}");

            await using (var client = await f.ConnectAsync(addr!))
            {
                var e = await Assert.ThrowsAsync<FelixException>(() =>
                    client.PublishAsync(f.Tenant, f.Namespace, f.MovableStream!, FaultControl.Bytes("mid-move")));
                Assert.True(e.Status == FelixStatus.ShardUnavailable, $"a publish during a move gave {e.Status} ({e.Message})");
                Assert.True(e.IsRetryable, "a refusal during a move was not marked retryable");
                Assert.False(e.OutcomeUnknown, "a clean refusal was marked outcome unknown");
            }

            // A refusal is retryable at once, so only the deadline stops an at-least-once publish with a
            // large attempt budget.
            await using (var bounded = await f.ConnectAsync(40, TimeSpan.FromSeconds(6), addr!))
            {
                var began = DateTime.UtcNow;
                var e = await Assert.ThrowsAsync<FelixException>(() =>
                    bounded.PublishAsync(f.Tenant, f.Namespace, f.MovableStream!, FaultControl.Bytes("bounded"), delivery: Delivery.AtLeastOnce));
                var took = DateTime.UtcNow - began;
                Assert.True(e.Status == FelixStatus.ShardUnavailable, $"a bounded publish during a move gave {e.Status} ({e.Message})");
                Assert.True(took < TimeSpan.FromSeconds(14), $"the publish took {took} against a 6s deadline: it spent its attempts instead");

                // The same deadline bounds a group call to the fenced owner. The owner refuses a settle
                // before it looks the group up, so the group need not exist.
                began = DateTime.UtcNow;
                var grouped = await Assert.ThrowsAsync<FelixException>(() =>
                    bounded.JoinGroup(f.Tenant, f.Namespace, f.MovableStream!, 0, "bounded-group").AckAsync(0L));
                took = DateTime.UtcNow - began;
                Assert.True(grouped.Status == FelixStatus.ShardUnavailable, $"a bounded group call during a move gave {grouped.Status} ({grouped.Message})");
                // The largest backoff step is 2s, so a client still retrying stops no earlier than this.
                Assert.True(took >= TimeSpan.FromSeconds(6 - 2 - 0.25), $"the group call took {took}: it gave up before the 6s deadline was in reach");
                Assert.True(took < TimeSpan.FromSeconds(8), $"the group call took {took} against a 6s deadline: it spent its attempts instead");
            }

            // Retryable means the same publish lands once the move finishes.
            await FaultControl.HealAndRecoverAsync(f, addr!, f.MovableStream!, run);
        }
        finally
        {
            await FaultControl.HealAsync(f);
        }
    }

    [ShardFixtureFact]
    [Scenario("error.quorum_timeout_is_outcome_unknown", "retry.ambiguous_outcomes_are_not_silently_retried")]
    public async Task AWriteNoMajorityConfirmedIsOutcomeUnknownAndNotResent()
    {
        var f = FaultFixture.Load();
        var run = FaultControl.Run();
        var ambiguous = $"cs-{run}-no-majority";
        try
        {
            // The leader wrote it and cannot say whether it will survive.
            var (code, body) = await FaultControl.PostAsync(f, "/partition");
            FaultControl.RequireFault("/partition", code, body);
            var addr = FaultControl.AddrIn(body);
            Assert.True(addr is not null, $"partition answered no broker address: {body}");

            await using (var client = await f.ConnectAsync(addr!))
            {
                // The default delivery is at most once: the client must report, not re-send.
                var e = await Assert.ThrowsAsync<FelixException>(() =>
                    client.PublishAsync(f.Tenant, f.Namespace, f.QuorumStream!, FaultControl.Bytes(ambiguous)));
                Assert.True(e.Status == FelixStatus.OutcomeUnknown, $"a write no majority confirmed gave {e.Status} ({e.Message})");
                Assert.True(e.OutcomeUnknown, "the error did not say the outcome is unknown");
                Assert.False(e.IsRetryable, "an outcome-unknown write was marked retryable");
            }

            await FaultControl.HealAndRecoverAsync(f, addr!, f.QuorumStream!, run);

            // The publish was at most once and its outcome unknown: a client that re-sent it would
            // leave two copies. One or none is right.
            var copies = await FaultControl.CopiesOfAsync(f, f.QuorumStream!, ambiguous);
            Assert.True(copies is >= 0 and <= 1, $"{copies} copies of a publish that ended outcome unknown");
        }
        finally
        {
            var healed = await FaultControl.HealAsync(f);
            if (healed.Code != 200)
            {
                Console.Error.WriteLine($"fault cleanup: heal answered {healed.Code}: {healed.Body}");
            }
        }
    }
}
