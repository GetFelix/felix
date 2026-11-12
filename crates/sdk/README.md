# SDK

The clients an application links to talk to Felix. All are Apache-2.0. Each client's README is its page on its registry.

| Client | Package | Install | Docs |
| --- | --- | --- | --- |
| [Rust](felix-client) | `felix-client` on crates.io | `cargo add felix-client` | [guide](https://docs.getfelix.dev/clients/rust/) |
| [Python](felix-python) | `felix-client` on PyPI | `pip install felix-client` | [guide](https://docs.getfelix.dev/clients/python/) |
| [Node.js and TypeScript](felix-typescript) | `felix-client` on npm | `npm install felix-client` | [guide](https://docs.getfelix.dev/clients/typescript/) |
| [C#](../../sdks/dotnet) | `Felix.Client` on NuGet | `dotnet add package Felix.Client` | [guide](https://docs.getfelix.dev/clients/csharp/) |
| [C](felix-capi) | `libfelix` archive on GitHub releases | see its README | [guide](https://docs.getfelix.dev/clients/c/) |

The Python, Node.js and C bindings wrap the Rust client rather than reimplementing the protocol, and the C# client binds the C library. `felix-python` and `felix-typescript` are outside the Cargo workspace because they build with their own toolchains (maturin, napi-rs); `task python:check` and `task ts:check` build them.

The bindings are held to the same behaviour as the Rust client by the conformance kit in [`../testing/felix-conformance`](../testing/felix-conformance). [Choosing a client](https://docs.getfelix.dev/clients/overview/) explains how they relate.
