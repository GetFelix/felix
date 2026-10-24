using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

[assembly: Xunit.TestFramework("Felix.Client.Tests.ConformanceFramework", "Felix.Client.Tests")]

namespace Felix.Client.Tests;

/// <summary>
/// Names the conformance scenario(s) from <c>felix-conformance</c>'s catalogue that a test
/// covers. A scenario's outcome is the worst outcome of the tests that carry its id.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
public sealed class ScenarioAttribute : Attribute
{
    public ScenarioAttribute(params string[] ids) => Ids = ids;

    public string[] Ids { get; }
}

/// <summary>
/// The results document <c>felix-conformance verify</c> reads, in the shape the Python and C
/// harnesses write: <c>{client, version, outcomes: [{id, status, detail?}]}</c>.
/// </summary>
internal sealed class ConformanceResults
{
    private static readonly string[] Ranks = ["pass", "skip", "fail"];
    private readonly Dictionary<string, (string Status, string? Detail)> outcomes = new(StringComparer.Ordinal);
    private readonly object gate = new();

    internal bool IsEmpty
    {
        get
        {
            lock (gate)
            {
                return outcomes.Count == 0;
            }
        }
    }

    /// <summary>Records <paramref name="status"/> (pass, skip or fail) for each id; the worst outcome wins.</summary>
    internal void Record(IEnumerable<string> ids, string status, string? detail)
    {
        var rank = Array.IndexOf(Ranks, status);
        if (rank < 0)
        {
            throw new ArgumentException($"unknown status '{status}'", nameof(status));
        }

        lock (gate)
        {
            foreach (var id in ids)
            {
                if (!outcomes.TryGetValue(id, out var held) || rank > Array.IndexOf(Ranks, held.Status))
                {
                    outcomes[id] = (status, status == "pass" ? null : detail);
                }
            }
        }
    }

    internal string ToJson(string client, string version)
    {
        lock (gate)
        {
            var document = new Document(
                client,
                version,
                outcomes.OrderBy(o => o.Key, StringComparer.Ordinal)
                    .Select(o => new Entry(o.Key, o.Value.Status, string.IsNullOrEmpty(o.Value.Detail) ? null : o.Value.Detail))
                    .ToList());
            return JsonSerializer.Serialize(document, new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            }) + "\n";
        }
    }

    /// <summary>Writes the document to <paramref name="path"/>; nothing when no scenario ran, so verify reports it missing.</summary>
    internal void WriteTo(string path, string client, string version)
    {
        if (IsEmpty)
        {
            return;
        }

        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (directory is not null)
        {
            Directory.CreateDirectory(directory);
        }

        File.WriteAllText(path, ToJson(client, version));
    }

    internal static string SdkVersion()
    {
        var info = typeof(FelixClient).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        return string.IsNullOrEmpty(info) ? "unknown" : info.Split('+')[0];
    }

    private sealed record Entry(string Id, string Status, string? Detail);

    private sealed record Document(string Client, string Version, List<Entry> Outcomes);
}

/// <summary>Runs xunit as usual and records each tagged test's outcome into the results document.</summary>
public sealed class ConformanceFramework : XunitTestFramework
{
    public ConformanceFramework(IMessageSink messageSink)
        : base(messageSink)
    {
    }

    protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName) =>
        new Executor(assemblyName, SourceInformationProvider, DiagnosticMessageSink);

    private sealed class Executor : XunitTestFrameworkExecutor
    {
        public Executor(AssemblyName assemblyName, ISourceInformationProvider sourceInformationProvider, IMessageSink diagnosticMessageSink)
            : base(assemblyName, sourceInformationProvider, diagnosticMessageSink)
        {
        }

        protected override async void RunTestCases(IEnumerable<IXunitTestCase> testCases, IMessageSink executionMessageSink, ITestFrameworkExecutionOptions executionOptions)
        {
            var recorder = new Recorder(executionMessageSink);
            using var runner = new XunitTestAssemblyRunner(TestAssembly, testCases, DiagnosticMessageSink, recorder, executionOptions);
            await runner.RunAsync();
        }
    }

    private sealed class Recorder : LongLivedMarshalByRefObject, IMessageSink
    {
        private readonly IMessageSink inner;
        private readonly ConformanceResults results = new();

        internal Recorder(IMessageSink inner) => this.inner = inner;

        public bool OnMessage(IMessageSinkMessage message)
        {
            try
            {
                switch (message)
                {
                    case ITestPassed passed:
                        results.Record(Ids(passed.Test), "pass", null);
                        break;
                    case ITestSkipped skipped:
                        results.Record(Ids(skipped.Test), "skip", skipped.Reason);
                        break;
                    case ITestFailed failed:
                        results.Record(Ids(failed.Test), "fail", Flatten(failed.Messages.FirstOrDefault() ?? failed.Test.DisplayName));
                        break;
                    case ITestAssemblyFinished:
                        Write();
                        break;
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"conformance results: {e.Message}");
            }

            return inner.OnMessage(message);
        }

        private void Write()
        {
            var path = Environment.GetEnvironmentVariable("FELIX_CONFORMANCE_RESULTS");
            if (string.IsNullOrEmpty(path))
            {
                return;
            }

            results.WriteTo(path, "Felix.Client (C#)", ConformanceResults.SdkVersion());
            if (!results.IsEmpty)
            {
                Console.WriteLine($"conformance results written to {path}");
            }
        }

        private static IEnumerable<string> Ids(ITest test)
        {
            var method = test.TestCase.TestMethod?.Method;
            if (method is null)
            {
                yield break;
            }

            foreach (var attribute in method.GetCustomAttributes(typeof(ScenarioAttribute).AssemblyQualifiedName!))
            {
                foreach (var argument in attribute.GetConstructorArguments())
                {
                    if (argument is string single)
                    {
                        yield return single;
                    }
                    else if (argument is IEnumerable<string> many)
                    {
                        foreach (var id in many)
                        {
                            yield return id;
                        }
                    }
                }
            }
        }

        private static string Flatten(string text)
        {
            var line = text.Replace('\t', ' ').Replace('\r', ' ').Replace('\n', ' ');
            return line.Length <= 300 ? line : line[..300];
        }
    }
}
