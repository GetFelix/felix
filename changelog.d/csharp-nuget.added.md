- C# SDK (`sdks/dotnet`): `Felix.Client` is a NuGet package that carries `libfelix` for Linux, macOS and Windows on
  x64 and arm64, so `dotnet add package Felix.Client` needs no separate native build; `FELIX_NATIVE_LIB` still
  overrides it. The release workflow builds the six libraries, packs them (failing if one is missing) and pushes
  the package when `PUBLISH_NUGET` is `true`, through nuget.org trusted publishing. CI installs the packed package
  into a throwaway app on each platform with `FELIX_NATIVE_LIB` unset.
