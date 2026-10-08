# SDK

What an application links. All four are Apache-2.0.

- [`felix-client`](felix-client) is the Rust client. Start at `Client`.
- [`felix-capi`](felix-capi) is a C ABI over the Rust client: a native
  library and a generated header, which the Go and C# SDKs will bind to.
- [`felix-python`](felix-python) and [`felix-typescript`](felix-typescript)
  wrap the Rust client for Python (pyo3) and Node.js (napi) rather than
  reimplementing the protocol. They are outside the Cargo workspace because
  they build with their own toolchains (maturin, napi-rs); `task python:check`
  and `task ts:check` build them.

The bindings are held to the same behaviour as the Rust client by the
conformance kit in [`../testing/felix-conformance`](../testing/felix-conformance).
