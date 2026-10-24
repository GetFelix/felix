using System.Runtime.CompilerServices;

namespace Felix;

/// <summary>Argument checks shared by every call.</summary>
internal static class Arg
{
    /// <summary>
    /// A string passed to the library as a C string. A NUL inside it would end the string there,
    /// so the library would act on a shorter name or key than the caller gave; refuse it instead.
    /// </summary>
    internal static void Text(string? value, [CallerArgumentExpression(nameof(value))] string? name = null)
    {
        ArgumentNullException.ThrowIfNull(value, name);
        NoNul(value, name);
    }

    /// <summary>As <see cref="Text"/>, for a string that may be null.</summary>
    internal static void OptionalText(string? value, [CallerArgumentExpression(nameof(value))] string? name = null)
    {
        if (value is not null)
        {
            NoNul(value, name);
        }
    }

    private static void NoNul(string value, string? name)
    {
        if (value.Contains('\0'))
        {
            throw new ArgumentException("must not contain a NUL character", name);
        }
    }
}
