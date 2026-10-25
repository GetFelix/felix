using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Faults the fixture's control endpoint produces on the shared cluster: a fenced shard and a leader
/// cut off from its followers. Both heal afterwards and wait for the stream to take a publish again.
/// </summary>
public class ClusterFaultTests
{
    [ShardFixtureFact]
    [Scenario("error.shard_unavailable_is_retryable")]
    public async Task AFencedShardRefusesAndSaysItIsSafeToRetry()
    {
        var f = FaultFixture.Load();
        var run = FaultControl.Run();
        try
        {
            // Published straight to the fenced owner: through another broker the answer would be
            // about the forward.
            var (code, body) = await FaultControl.PostAsync(f, "/fence");
            Assert.True(code == 200, $"POST /fence answered {code}: {body}");
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
            Assert.True(code == 200, $"POST /partition answered {code}: {body}");
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
