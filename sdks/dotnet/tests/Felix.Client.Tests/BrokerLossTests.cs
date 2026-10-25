using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using Xunit.Sdk;

namespace Felix.Client.Tests;

/// <summary>
/// Skips unless the test can start its own cluster (<c>FELIX_CLUSTER_BIN</c>) and tell which process
/// listens on a broker's port (<c>lsof</c> or <c>ss</c>).
/// </summary>
[XunitTestCaseDiscoverer("Felix.Client.Tests.RuntimeSkipDiscoverer", "Felix.Client.Tests")]
public sealed class ClusterProcessFactAttribute : FactAttribute
{
    public ClusterProcessFactAttribute()
    {
        var bin = Environment.GetEnvironmentVariable("FELIX_CLUSTER_BIN");
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("FELIX_NATIVE_LIB")))
        {
            Skip = "FELIX_NATIVE_LIB is not set; build libfelix with `cargo build --release -p felix-capi`";
        }
        else if (string.IsNullOrEmpty(bin) || !File.Exists(bin))
        {
            Skip = "FELIX_CLUSTER_BIN does not name a felix-cluster binary; build it with `cargo build -p felix-cluster`";
        }
        else if (OperatingSystem.IsWindows())
        {
            Skip = "finding the broker process by its port needs lsof or ss";
        }
        else if (BrokerProcess.Tool() is null)
        {
            Skip = "neither lsof nor ss is installed, so the broker process cannot be identified";
        }
    }
}

/// <summary>Finds the broker listening on a UDP port and kills it, and only it.</summary>
internal static class BrokerProcess
{
    private static readonly Regex SsPid = new("\"felix-broker\",pid=(\\d+)", RegexOptions.Compiled);

    internal static string? Tool() => OnPath("lsof") is not null ? "lsof" : OnPath("ss") is not null ? "ss" : null;

    private static string? OnPath(string name) =>
        (Environment.GetEnvironmentVariable("PATH") ?? "")
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(dir => Path.Combine(dir, name))
            .FirstOrDefault(File.Exists);

    /// <summary>
    /// The pid of the felix-broker listening on the UDP port of <paramref name="addr"/>. The port picks
    /// the process and the name keeps a client socket that happens to use the port from being chosen.
    /// </summary>
    internal static async Task<int?> FindAsync(string addr)
    {
        var port = addr[(addr.LastIndexOf(':') + 1)..];
        if (port.Length == 0 || !port.All(char.IsAsciiDigit))
        {
            throw new ArgumentException($"no port in '{addr}'");
        }

        if (OnPath("lsof") is not null)
        {
            var output = await RunAsync("lsof", "-nP", "-a", "-c", "felix-broker", $"-iUDP:{port}", "-t");
            return int.TryParse(output.Split('\n', StringSplitOptions.RemoveEmptyEntries).FirstOrDefault()?.Trim(), out var pid) ? pid : null;
        }

        var ss = await RunAsync("ss", "-H", "-lunp", $"sport = :{port}");
        var match = SsPid.Match(ss);
        return match.Success ? int.Parse(match.Groups[1].Value) : null;
    }

    /// <summary>Kills the broker on <paramref name="addr"/> with SIGKILL; false when none was found.</summary>
    internal static async Task<bool> KillAsync(string addr)
    {
        if (await FindAsync(addr) is not { } pid)
        {
            return false;
        }

        using var process = Process.GetProcessById(pid);
        process.Kill();
        return true;
    }

    private static async Task<string> RunAsync(string file, params string[] args)
    {
        var info = new ProcessStartInfo(file) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var arg in args)
        {
            info.ArgumentList.Add(arg);
        }

        using var process = Process.Start(info)!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        _ = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();
        return await stdout;
    }
}

/// <summary>A <c>felix-cluster client-fixture</c> process of the test's own, so killing a broker damages nothing else.</summary>
internal sealed class DisposableCluster : IAsyncDisposable
{
    private readonly Process process;
    private readonly string dir;
    private readonly StringBuilder log = new();

    private DisposableCluster(Process process, string dir)
    {
        this.process = process;
        this.dir = dir;
    }

    internal FaultFixture Fixture { get; private set; } = null!;

    internal static async Task<DisposableCluster> StartAsync(string bin)
    {
        var dir = Directory.CreateTempSubdirectory("felix-cs-cluster-").FullName;
        var info = new ProcessStartInfo(bin)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        info.ArgumentList.Add("client-fixture");
        info.ArgumentList.Add("--out");
        info.ArgumentList.Add(Path.Combine(dir, "fixture.json"));
        info.ArgumentList.Add("--ca-file");
        info.ArgumentList.Add(Path.Combine(dir, "ca.pem"));
        info.Environment["RUST_LOG"] = "warn";

        var process = new Process { StartInfo = info };
        var cluster = new DisposableCluster(process, dir);
        void Append(object _, DataReceivedEventArgs e)
        {
            if (e.Data is not null)
            {
                lock (cluster.log)
                {
                    cluster.log.AppendLine(e.Data);
                }
            }
        }

        process.OutputDataReceived += Append;
        process.ErrorDataReceived += Append;
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        try
        {
            await cluster.WaitReadyAsync();
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }

        return cluster;
    }

    /// <summary>Waits until the fixture document and the CA are written and the document parses, as run.sh does.</summary>
    private async Task WaitReadyAsync()
    {
        var fixture = Path.Combine(dir, "fixture.json");
        var ca = Path.Combine(dir, "ca.pem");
        for (var i = 0; i < 600; i++)
        {
            if (new FileInfo(fixture) is { Exists: true, Length: > 0 } && new FileInfo(ca) is { Exists: true, Length: > 0 })
            {
                try
                {
                    Fixture = FaultFixture.LoadFrom(fixture);
                    return;
                }
                catch (Exception e) when (e is JsonException or IOException or KeyNotFoundException)
                {
                    // Still being written.
                }
            }

            if (process.HasExited)
            {
                throw new XunitException($"the fixture exited during startup:\n{Log()}");
            }

            await Task.Delay(200);
        }

        throw new XunitException($"the fixture was not ready in time:\n{Log()}");
    }

    private string Log()
    {
        lock (log)
        {
            return log.ToString();
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!process.HasExited)
            {
                // SIGTERM first, so the fixture stops the brokers it started.
                using (var term = Process.Start(new ProcessStartInfo("kill", $"-TERM {process.Id}") { RedirectStandardError = true }))
                {
                    if (term is not null)
                    {
                        await term.WaitForExitAsync();
                    }
                }

                using var grace = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                try
                {
                    await process.WaitForExitAsync(grace.Token);
                }
                catch (OperationCanceledException)
                {
                    process.Kill(entireProcessTree: true);
                }
            }
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"cluster cleanup: stopping the fixture failed: {e.Message}");
        }

        try
        {
            process.Dispose();
            Directory.Delete(dir, recursive: true);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"cluster cleanup: removing {dir} failed: {e.Message}");
        }
    }
}

public class BrokerLossTests
{
    [ClusterProcessFact]
    [Scenario("reconnect.survives_broker_loss")]
    public async Task AClientOutlivesTheBrokerItWasUsing()
    {
        await using var cluster = await DisposableCluster.StartAsync(Environment.GetEnvironmentVariable("FELIX_CLUSTER_BIN")!);
        var f = cluster.Fixture;
        if (f.Addrs.Length < 2)
        {
            throw RuntimeSkipException.Because("the disposable fixture has one broker");
        }

        await using var client = await f.ConnectAsync();
        var run = FaultControl.Run();
        await FaultControl.PublishAsync(client, f, f.Stream, $"cs-{run}-before-loss");

        var seed = f.Addrs[0];
        if (!await BrokerProcess.KillAsync(seed))
        {
            throw RuntimeSkipException.Because($"could not identify the broker process listening on {seed}");
        }

        // Generous: the client has to notice the broker is gone, and the cluster has to give its
        // shards a new leader. Only the first is the client's.
        var payload = $"cs-{run}-after-loss";
        string? last = null;
        for (var attempt = 0; attempt < 60; attempt++)
        {
            try
            {
                await FaultControl.PublishAsync(client, f, f.Stream, payload);
                return;
            }
            catch (Exception e) when (e is FelixException or XunitException)
            {
                last = e.Message;
                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        throw new XunitException($"the client never recovered from losing {seed}: {last}");
    }
}
