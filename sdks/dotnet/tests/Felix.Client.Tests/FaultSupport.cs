using System.Text;
using System.Text.Json;
using Xunit;
using Xunit.Sdk;

namespace Felix.Client.Tests;

/// <summary>One catalogue fault step, as <c>felix-cluster client-fixture</c> writes it into the fixture file.</summary>
internal sealed record FaultStep(string During, string Fault, int AfterRecords, int Records, int HoldMs);

/// <summary>
/// The fixture fields the fault scenarios need, read straight from the fixture file so these tests
/// do not depend on <see cref="Fixture"/>'s shape.
/// </summary>
internal sealed class FaultFixture
{
    private FaultFixture()
    {
    }

    internal string[] Addrs { get; private init; } = [];

    internal string Tenant { get; private init; } = "";

    internal string Namespace { get; private init; } = "";

    internal string Token { get; private init; } = "";

    internal string CaFile { get; private init; } = "";

    internal string Stream { get; private init; } = "";

    internal string? ControlUrl { get; private init; }

    /// <summary>The stub broker that advertises no features; empty when the fixture has none.</summary>
    internal string[] NoCommitAddrs { get; private init; } = [];

    internal string? LinkAddr { get; private init; }

    internal string? LinkStream { get; private init; }

    internal string? MovableStream { get; private init; }

    internal string? QuorumStream { get; private init; }

    internal string Cache { get; private init; } = "";

    internal Dictionary<string, FaultStep> Steps { get; private init; } = new(StringComparer.Ordinal);

    internal static FaultFixture Load() => LoadFrom(Environment.GetEnvironmentVariable("FELIX_FIXTURE")!);

    internal static FaultFixture LoadFrom(string path)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var root = doc.RootElement;
        string Text(string name) => root.GetProperty(name).GetString()!;
        string? Optional(string name) =>
            root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String && v.GetString() is { Length: > 0 } s ? s : null;

        var steps = new Dictionary<string, FaultStep>(StringComparer.Ordinal);
        if (root.TryGetProperty("faults", out var faults) && faults.ValueKind == JsonValueKind.Array)
        {
            foreach (var fault in faults.EnumerateArray())
            {
                steps[fault.GetProperty("id").GetString()!] = new FaultStep(
                    fault.GetProperty("during").GetString()!,
                    fault.GetProperty("fault").GetString()!,
                    fault.GetProperty("after_records").GetInt32(),
                    fault.GetProperty("records").GetInt32(),
                    fault.GetProperty("hold_ms").GetInt32());
            }
        }

        return new FaultFixture
        {
            Addrs = root.GetProperty("addrs").EnumerateArray().Select(a => a.GetString()!).ToArray(),
            Tenant = Text("tenant_id"),
            Namespace = Text("namespace"),
            Token = Text("token"),
            CaFile = Text("ca_file"),
            Stream = Text("durable_stream"),
            ControlUrl = Optional("control_url"),
            NoCommitAddrs = root.TryGetProperty("no_commit_addrs", out var bare) && bare.ValueKind == JsonValueKind.Array
                ? bare.EnumerateArray().Select(a => a.GetString()!).ToArray()
                : [],
            LinkAddr = Optional("link_addr"),
            LinkStream = Optional("link_stream"),
            MovableStream = Optional("movable_stream"),
            QuorumStream = Optional("quorum_stream"),
            Cache = Text("cache"),
            Steps = steps,
        };
    }

    /// <summary>Connects to <paramref name="addrs"/>, or to the fixture's own brokers when none are given.</summary>
    internal Task<FelixClient> ConnectAsync(params string[] addrs) =>
        FelixClient.ConnectAsync(
            new FelixClientOptions
            {
                Addrs = addrs.Length > 0 ? addrs : Addrs,
                TenantId = Tenant,
                Token = Token,
                CaFile = CaFile,
            });
}

/// <summary>
/// The tests that drive the fixture's control endpoint. A fence drains a broker and a partition
/// isolates a leader for every shard it serves, and /heal undoes every fault, so none of them can run
/// beside a test using the shared streams or beside each other. xUnit runs this collection alone,
/// after the parallel ones finish.
/// </summary>
[CollectionDefinition(FaultCollection.Name, DisableParallelization = true)]
public sealed class FaultCollection
{
    public const string Name = "fixture faults";
}

/// <summary>Skips unless the fixture file carries the link interposer and control endpoint.</summary>
public sealed class LinkFixtureFactAttribute : FixtureFactAttribute
{
    public LinkFixtureFactAttribute()
    {
        if (Skip is null)
        {
            var f = FaultFixture.Load();
            if (f.ControlUrl is null || f.LinkAddr is null || f.LinkStream is null)
            {
                Skip = "the fixture file has no link interposer";
            }
        }
    }
}

/// <summary>Skips unless the fixture file carries the no-commit stub broker and the control endpoint that counts its requests.</summary>
public sealed class NoCommitFixtureFactAttribute : FixtureFactAttribute
{
    public NoCommitFixtureFactAttribute()
    {
        if (Skip is null)
        {
            var f = FaultFixture.Load();
            if (f.NoCommitAddrs.Length == 0 || f.ControlUrl is null)
            {
                Skip = "the fixture file has no no_commit_addrs or control_url";
            }
        }
    }
}

/// <summary>Skips unless the fixture file carries the control endpoint and the streams /fence and /partition act on.</summary>
[XunitTestCaseDiscoverer("Felix.Client.Tests.RuntimeSkipDiscoverer", "Felix.Client.Tests")]
public sealed class ShardFixtureFactAttribute : FixtureFactAttribute
{
    public ShardFixtureFactAttribute()
    {
        if (Skip is null)
        {
            var f = FaultFixture.Load();
            var absent = f.ControlUrl is null ? "control_url" : f.MovableStream is null ? "movable_stream" : f.QuorumStream is null ? "quorum_stream" : null;
            if (absent is not null)
            {
                Skip = $"the fixture file has no {absent}";
            }
        }
    }
}

/// <summary>The fixture's control endpoint, and the waits the fault scenarios share.</summary>
internal static class FaultControl
{
    /// <summary>Room for the idle timeout to fire and a reconnect to land, on top of a fault's own hold.</summary>
    internal static readonly TimeSpan Settle = TimeSpan.FromSeconds(45);

    private static readonly TimeSpan Poll = TimeSpan.FromMilliseconds(250);

    /// <summary>A call that has not ended by then is hung; a hold plus the idle timeout is the longest a healthy one takes.</summary>
    private static readonly TimeSpan Hung = TimeSpan.FromSeconds(60);

    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(150) };

    internal static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    internal static string Run() => Guid.NewGuid().ToString("N")[..12];

    /// <summary>POSTs to the control endpoint; the HTTP status and body.</summary>
    internal static async Task<(int Code, string Body)> PostAsync(FaultFixture f, string path, string? json = null)
    {
        using var content = json is null ? null : new StringContent(json, Encoding.UTF8, "application/json");
        using var response = await Http.PostAsync(f.ControlUrl + path, content);
        return ((int)response.StatusCode, await response.Content.ReadAsStringAsync());
    }

    /// <summary>The broker address in a control answer, <c>{"node_id":..,"addr":".."}</c>.</summary>
    internal static string? AddrIn(string body)
    {
        try
        {
            using var doc = JsonDocument.Parse(body);
            return doc.RootElement.TryGetProperty("addr", out var a) && a.GetString() is { Length: > 0 } s ? s : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Skips the test when the endpoint refuses a fault with 409, as it does when the cluster has no
    /// shard to move or cut off; any other non-200 answer is a failure.
    /// </summary>
    internal static void RequireFault(string path, int code, string body)
    {
        if (code == 409)
        {
            throw RuntimeSkipException.Because($"POST {path} answered {code}: {body}");
        }

        Assert.True(code == 200, $"POST {path} answered {code}: {body}");
    }

    internal static async Task InjectAsync(FaultFixture f, FaultStep step)
    {
        var (code, body) = await PostAsync(f, "/link", $"{{\"fault\":\"{step.Fault}\",\"hold_ms\":{step.HoldMs}}}");
        Assert.True(code == 200, $"injecting {step.Fault} answered {code}: {body}");
    }

    /// <summary>Heals every fault. Never throws: it runs in cleanup, and the caller asserts on the status.</summary>
    internal static async Task<(int Code, string Body)> HealAsync(FaultFixture f)
    {
        try
        {
            return await PostAsync(f, "/heal");
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"fault cleanup: /heal failed: {e.Message}");
            return (0, e.Message);
        }
    }

    /// <summary>Publishes one record, failing the test rather than hanging if the call never ends.</summary>
    internal static async Task PublishAsync(FelixClient client, FaultFixture f, string stream, string payload)
    {
        using var cts = new CancellationTokenSource(Hung);
        try
        {
            await client.PublishAsync(f.Tenant, f.Namespace, stream, Bytes(payload), cancellationToken: cts.Token);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            throw new XunitException($"the publish of {payload} was still pending after {Hung.TotalSeconds:0}s");
        }
    }

    /// <summary>Heals, and waits until <paramref name="stream"/> takes a publish through <paramref name="addr"/> again.</summary>
    internal static async Task HealAndRecoverAsync(FaultFixture f, string addr, string stream, string run)
    {
        var (code, body) = await PostAsync(f, "/heal");
        Assert.True(code == 200, $"heal answered {code}: {body}");
        var began = DateTime.UtcNow;
        FelixClient? client = null;
        try
        {
            while (DateTime.UtcNow - began < TimeSpan.FromSeconds(90))
            {
                try
                {
                    client ??= await f.ConnectAsync(addr);
                    await PublishAsync(client, f, stream, $"cs-{run}-healed");
                    return;
                }
                catch (FelixException)
                {
                    await Task.Delay(Poll);
                }
            }
        }
        finally
        {
            if (client is not null)
            {
                await client.DisposeAsync();
            }
        }

        throw new XunitException($"{stream} never took a publish again after the heal");
    }

    /// <summary>How many of <paramref name="payloads"/> <paramref name="stream"/> does not hold, read from the start on a fresh connection.</summary>
    internal static async Task<int> UnreadAsync(FaultFixture f, string stream, IReadOnlyCollection<string> payloads)
    {
        var missing = new HashSet<string>(payloads, StringComparer.Ordinal);
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, stream, 0, StartPosition.Earliest);
        var began = DateTime.UtcNow;
        while (missing.Count > 0 && DateTime.UtcNow - began < Settle)
        {
            var ev = await sub.ReadAsync(TimeSpan.FromSeconds(1));
            if (ev is not null)
            {
                missing.Remove(Encoding.UTF8.GetString(ev.Payload.Span));
            }
        }

        return missing.Count;
    }

    /// <summary>How many copies of <paramref name="payload"/> <paramref name="stream"/> holds, read until the feed has been idle for a few seconds.</summary>
    internal static async Task<int> CopiesOfAsync(FaultFixture f, string stream, string payload)
    {
        await using var client = await f.ConnectAsync();
        await using var sub = await client.SubscribeAsync(f.Tenant, f.Namespace, stream, 0, StartPosition.Earliest);
        var copies = 0;
        while (await sub.ReadAsync(TimeSpan.FromSeconds(3)) is { } ev)
        {
            if (Encoding.UTF8.GetString(ev.Payload.Span) == payload)
            {
                copies++;
            }
        }

        return copies;
    }
}
