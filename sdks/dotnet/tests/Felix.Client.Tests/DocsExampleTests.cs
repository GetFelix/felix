using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace Felix.Client.Tests;

/// <summary>
/// Compiles every csharp block on the C# client page against this build of Felix.Client, so the
/// page cannot drift from the API. Nothing is run: most blocks need a broker.
/// </summary>
public class DocsExampleTests
{
    private const string Page = "docs-site/src/content/docs/clients/csharp.md";

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
        }
        """;

    [Fact]
    public void EveryCSharpBlockOnThePageCompiles()
    {
        var blocks = Extract(File.ReadAllText(FindPage()));
        Assert.NotEmpty(blocks);

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
                $"csharp_md_L{line}",
                [CSharpSyntaxTree.ParseText(code, parse), prelude],
                references,
                options);
            failures.AddRange(compilation.GetDiagnostics()
                .Where(d => d.Severity == DiagnosticSeverity.Error || d.Severity == DiagnosticSeverity.Warning)
                .Where(d => d.Location.SourceTree != prelude)
                .Select(d => $"csharp.md:{line + d.Location.GetLineSpan().StartLinePosition.Line}: {d.Id}: {d.GetMessage()}"));
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

    private static string FindPage()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, Page);
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException($"{Page} not found above {AppContext.BaseDirectory}");
    }
}
