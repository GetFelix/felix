using System.Reflection;
using System.Runtime.InteropServices;

namespace Felix.Native;

/// <summary>Finds <c>libfelix</c> next to the assembly, under <c>runtimes/&lt;rid&gt;/native</c>, or at <c>FELIX_NATIVE_LIB</c>.</summary>
internal static class NativeLoader
{
    private static readonly object Gate = new();
    private static bool installed;

    internal static void EnsureInstalled()
    {
        lock (Gate)
        {
            if (installed)
            {
                return;
            }

            NativeLibrary.SetDllImportResolver(typeof(NativeLoader).Assembly, Resolve);
            installed = true;
        }
    }

    internal static string FileName =>
        OperatingSystem.IsWindows() ? "felix.dll" : OperatingSystem.IsMacOS() ? "libfelix.dylib" : "libfelix.so";

    internal static string? RuntimeId()
    {
        var arch = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "x64",
            Architecture.Arm64 => "arm64",
            _ => null,
        };
        if (arch is null)
        {
            return null;
        }

        var os = OperatingSystem.IsWindows() ? "win" : OperatingSystem.IsMacOS() ? "osx" : OperatingSystem.IsLinux() ? "linux" : null;
        return os is null ? null : $"{os}-{arch}";
    }

    /// <summary>
    /// Why the packaged libraries cannot serve this runtime, or null. The package carries
    /// glibc builds only, so a musl runtime (Alpine) would otherwise fail with a bare
    /// <see cref="DllNotFoundException"/>.
    /// </summary>
    internal static string? Unsupported(string runtimeIdentifier) =>
        runtimeIdentifier.Contains("musl", StringComparison.OrdinalIgnoreCase)
            ? $"Felix.Client's native libraries are built for glibc, and this runtime is {runtimeIdentifier}. " +
              "The package supports linux-x64, linux-arm64, osx-x64, osx-arm64, win-x64 and win-arm64. " +
              "Build libfelix with `cargo build --release -p felix-capi` for musl and set FELIX_NATIVE_LIB to its path."
            : null;

    /// <summary>The paths to try, in order. The first that exists wins.</summary>
    internal static IEnumerable<string> Candidates(string? envPath, string baseDirectory)
    {
        if (!string.IsNullOrEmpty(envPath))
        {
            yield return envPath;
            yield break;
        }

        yield return Path.Combine(baseDirectory, FileName);
        var rid = RuntimeId();
        if (rid is not null)
        {
            yield return Path.Combine(baseDirectory, "runtimes", rid, "native", FileName);
        }
    }

    private static IntPtr Resolve(string name, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (name != "felix")
        {
            return IntPtr.Zero;
        }

        var env = Environment.GetEnvironmentVariable("FELIX_NATIVE_LIB");
        if (string.IsNullOrEmpty(env) && Unsupported(RuntimeInformation.RuntimeIdentifier) is { } reason)
        {
            throw new PlatformNotSupportedException(reason);
        }

        var directory = Path.GetDirectoryName(assembly.Location);
        foreach (var candidate in Candidates(env, string.IsNullOrEmpty(directory) ? AppContext.BaseDirectory : directory))
        {
            if (File.Exists(candidate) && NativeLibrary.TryLoad(candidate, out var handle))
            {
                return handle;
            }
        }

        // Not found: fall back to the runtime's own probing (PATH, LD_LIBRARY_PATH).
        return IntPtr.Zero;
    }
}
