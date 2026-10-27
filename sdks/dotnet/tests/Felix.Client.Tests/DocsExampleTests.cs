using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Compiles every csharp block on the C# client page and the resilience guide against this build of
/// Felix.Client, so the pages cannot drift from the API. Each tutorial page is compiled as one program, because its
/// blocks are steps of it. Nothing is run: most blocks need a broker.
/// </summary>
public class DocsExampleTests
{
    private const string Page = "docs-site/src/content/docs/clients/csharp.md";

    private static readonly string[] Tutorials =
    [
        "first-stream",
        "work-queue",
        "cache-leases-watches",
        "build-an-app",
    ];

    // What the page's blocks use without defining: implicit usings, and the names an earlier block
    // or the surrounding prose supplies. A block that declares one of these shadows it.
    private const string Prelude = """
        global using System;
        global using System.Collections.Generic;
        global using System.IO;
        global using System.Linq;
        global using System.Net.Http;
        global using System.Threading;
        global using System.Threading.Tasks;
        global using Felix;
        global using static DocsPrelude;

        internal static class DocsPrelude
        {
            public static string token => throw new NotImplementedException();
            public static string oidcToken => throw new NotImplementedException();
            public static FelixClient client => throw new NotImplementedException();
            public static FelixClient adminClient => throw new NotImplementedException();
            public static FelixShardedSubscription all => throw new NotImplementedException();
            public static FelixCache cache => throw new NotImplementedException();
            public static byte[] payloadA => throw new NotImplementedException();
            public static byte[] payloadB => throw new NotImplementedException();
            public static byte[] payloadC => throw new NotImplementedException();
            public static byte[] orderKey => throw new NotImplementedException();
            public static void Handle(ReadOnlySpan<byte> payload) => throw new NotImplementedException();
            public static void Store(string key, ReadOnlyMemory<byte> value, DateTimeOffset? expiresAt) => throw new NotImplementedException();
            public static void Remove(string key) => throw new NotImplementedException();
            public static byte[] payload => throw new NotImplementedException();
            public static Task<long?> LoadCheckpointAsync() => throw new NotImplementedException();
            public static Task SaveCheckpointAsync(long offset) => throw new NotImplementedException();
            public static Task<IReadOnlyList<ShardPosition>> LoadPositionsAsync() => throw new NotImplementedException();
            public static Task SavePositionsAsync(IReadOnlyList<ShardPosition> positions) => throw new NotImplementedException();
        }
        """;

    [Fact]
    public void EveryCSharpBlockOnThePageCompiles()
    {
        var blocks = Extract(File.ReadAllText(FindPage(Page)));
        Assert.NotEmpty(blocks);
        AssertCompiles("csharp.md", blocks);
    }

    [Fact]
    public void EveryCSharpBlockOnTheResilienceGuideCompiles()
    {
        var blocks = Extract(File.ReadAllText(FindPage("docs-site/src/content/docs/clients/resilient-clients.mdx")));
        Assert.NotEmpty(blocks);
        AssertCompiles("resilient-clients.mdx", blocks);
    }

    [Fact]
    public void EveryTutorialCompilesAsOneProgram()
    {
        foreach (var slug in Tutorials)
        {
            var path = $"docs-site/src/content/docs/tutorials/{slug}.mdx";
            var blocks = Extract(File.ReadAllText(FindPage(path)));
            Assert.NotEmpty(blocks);
            AssertCompiles($"{slug}.mdx", [Join(blocks)]);
        }
    }

    /// <summary>
    /// The blocks as one file: the <c>using</c> directives first, once each, then the statements
    /// and local functions of every block in order.
    /// </summary>
    internal static (int Line, string Code) Join(List<(int Line, string Code)> blocks)
    {
        var usings = new SortedSet<string>(StringComparer.Ordinal);
        var body = new List<string>();
        foreach (var (_, code) in blocks)
        {
            foreach (var line in code.Split('\n'))
            {
                if (Regex.IsMatch(line, @"^using [\w.]+;\s*$"))
                {
                    usings.Add(line.Trim());
                }
                else
                {
                    body.Add(line);
                }
            }

            body.Add("");
        }

        return (blocks[0].Line, string.Join('\n', usings.Concat(body)));
    }

    private static void AssertCompiles(string label, List<(int Line, string Code)> blocks)
    {
        var references = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!)
            .Split(Path.PathSeparator)
            .Append(typeof(FelixClient).Assembly.Location)
            .Select(path => MetadataReference.CreateFromFile(path))
            .ToList();
        var parse = new CSharpParseOptions(LanguageVersion.Latest);
        var prelude = CSharpSyntaxTree.ParseText(Prelude, parse);
        var options = new CSharpCompilationOptions(OutputKind.ConsoleApplication)
            .WithNullableContextOptions(NullableContextOptions.Enable);

        var failures = new List<string>();
        foreach (var (line, code) in blocks)
        {
            var compilation = CSharpCompilation.Create(
                $"{label.Replace('.', '_').Replace('-', '_')}_L{line}",
                [CSharpSyntaxTree.ParseText(code, parse), prelude],
                references,
                options);
            failures.AddRange(compilation.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error || d.Severity == DiagnosticSeverity.Warning)
                .Where(d => d.Location.SourceTree != prelude)
                .Select(d => $"{label}:{line + d.Location.GetLineSpan().StartLinePosition.Line}: {d.Id}: {d.GetMessage()}"));
        }

        Assert.True(failures.Count == 0, string.Join('\n', failures));
    }

    /// <summary>Each csharp block with the page line its code starts on.</summary>
    internal static List<(int Line, string Code)> Extract(string markdown)
    {
        var lines = markdown.Split('\n');
        var blocks = new List<(int, string)>();
        for (var i = 0; i < lines.Length; i++)
        {
            if (!Regex.IsMatch(lines[i], @"^\s*```(csharp|cs|c#)\s*$"))
            {
                continue;
            }

            var start = i + 1;
            var end = Array.FindIndex(lines, start, l => Regex.IsMatch(l, @"^\s*```\s*$"));
            Assert.True(end >= 0, $"csharp.md:{start}: unclosed code block");
            blocks.Add((start + 1, string.Join('\n', lines[start..end])));
            i = end;
        }

        return blocks;
    }

    private static string FindPage(string page)
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, page);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException($"{page} not found above {AppContext.BaseDirectory}");
    }
}
