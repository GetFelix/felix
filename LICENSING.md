# Licensing

Felix uses a split license: the wire protocol and client SDK are permissively
licensed to keep the ecosystem open, while the broker and control-plane server
components are strong copyleft, so anyone who runs a modified Felix as a
service has to publish what they changed.

Both halves are OSI-approved open source. Felix is not source-available and
reserves no commercial rights to anyone, its author included; the asymmetry is
about reciprocity, not about who is allowed to make money.

| Path | License | Why |
|---|---|---|
| `crates/protocol/felix-wire/` | Apache-2.0 | The wire protocol. Anyone should be able to implement a Felix client or server in any language without friction. |
| `crates/sdk/felix-client/` | Apache-2.0 | The Rust client SDK. Embeddable in your own products without restriction. |
| `crates/sdk/felix-python/`, `crates/sdk/felix-typescript/` | Apache-2.0 | The Python and Node.js bindings over the Rust client. |
| `crates/protocol/felix-transport/` | Apache-2.0 | Generic QUIC transport plumbing, not Felix-specific server logic. |
| `crates/server/felix-common/` | Apache-2.0 | Membership shapes, the env-var registry, and process lifecycle shared by the services. |
| `crates/server/felix-broker/`, `felix-storage`, `felix-authz`, `felix-router`, `felix-kafka`, `felix-replication` | AGPL-3.0-only | Server-side core logic. |
| `services/felix-broker-service/`, `services/felix-controlplane-service/` | AGPL-3.0-only | The runnable server binaries. |
| `crates/testing/felix-conformance/` | AGPL-3.0-only | The client conformance kit: the scenario catalogue, the verifier, and a suite that runs against the reference broker. It links `felix-broker-service`, `felix-broker`, `felix-storage` and `felix-authz`, so a build of it is AGPL whatever its manifest says. Running it against your client's results puts no obligation on your client. Not published. |
| `crates/testing/felix-cluster/` | AGPL-3.0-only | Local multi-node cluster harness for integration and failure tests. It embeds the control plane and drives the broker, so it is internal tooling rather than something a third-party implementer runs. Not published. |
| `crates/testing/felix-loadgen/` | AGPL-3.0-only | Load generator for the real-network performance suite. It drives a remote cluster through `felix-client` and is internal instrumentation, not something a third-party implementer runs. Not published. |

The root [`LICENSE`](LICENSE) file is AGPL-3.0 (the license for
the project as a whole / the deployable server). [`LICENSE-APACHE`](LICENSE-APACHE)
holds the Apache-2.0 text.

**Every crate directory carries its own `LICENSE` file**, holding the text that
applies to that subtree, and it takes precedence for that subtree. This is not
just tidiness: `cargo package` only bundles files from inside the crate
directory, so a crate without its own `LICENSE` would publish to a registry with
no license text at all.

## How the split is kept honest

The table above is the authoritative statement, and it is enforced rather than
trusted. `Cargo.toml`'s `[workspace.package]` sets `license = "AGPL-3.0-only"` as a
**fail-closed default**: a crate added without thinking inherits the copyleft
license, and the permissive crates opt in by setting
`license = "Apache-2.0"` explicitly. The previous default was Apache-2.0, which
meant a new server crate that forgot to override became silently permissive.

`task publish:check` (run in CI) asserts that every workspace member's resolved
license matches this table, that a crate is not left unclassified, that each has
its own `LICENSE` file, and that the `publish` flags are what we intend. Adding a
crate fails CI until it is deliberately classified here.

A label is only half of it: an Apache-2.0 crate that links an AGPL crate is AGPL
once built. `scripts/check_license_graph.py`, also run by `task publish:check`,
walks the first-party dependency graph (normal and build dependencies, optional
ones included) and fails if any permissive crate reaches an AGPL one.

## What AGPL-3.0 actually requires

You can self-host, modify, and build on Felix's server components freely,
including commercially. The obligation is reciprocity: if you run a modified
Felix and let other people reach it over a network, those users are entitled to
the source of your modified version. That is the network clause AGPL adds over
GPL, and it is the whole reason it is the right license here — a service is how
this software would be used, so distribution alone is the wrong trigger.

Nothing here reserves anything for the project's author. Anyone may run Felix
as a commercial service, on the same terms as anyone else: publish your
changes. The intent is to stop Felix being taken closed, not to stop it being
used.

Two practical consequences worth stating plainly:

- **Linking a proprietary application against the AGPL crates is a problem.**
  That is what the Apache-2.0 half is for. `felix-client` and the two crates it
  is built on, `felix-wire` and `felix-transport`, are everything an application
  needs to *talk to* Felix, and they carry no copyleft obligation at all. They
  are also the only crates published to crates.io. Build whatever you like on
  top.
- **Many organisations ban AGPL dependencies outright.** That is a real cost
  and it is accepted knowingly: the crates such an organisation actually needs
  to depend on are the Apache-2.0 ones.

See the full text in [`LICENSE`](LICENSE) or
https://www.gnu.org/licenses/agpl-3.0.html.

## Contributions

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the CLA process. Because of the
split above, which license a given contribution ultimately sits under
depends on which path it touches.
