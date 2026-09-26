# Security Policy

Felix is pre-1.0 and in early active development. It has **not** been through an
external security review. This document says how to report a vulnerability, what
we consider one, and what we already know is missing so you don't spend time
reporting it.

## Reporting a vulnerability

**Report privately through
[GitHub Security Advisories](https://github.com/gabloe/felix/security/advisories/new).**

Do not open a public issue, pull request, or discussion for a suspected
vulnerability, and please don't push a public fix branch before the advisory is
resolved — a fix commit is a disclosure.

If GitHub Security Advisories is unavailable to you, contact the maintainer
([@gabloe](https://github.com/gabloe)) privately and ask for a reporting channel.
Don't include vulnerability details in that first message.

### What to include

- Affected component and version — a released tag (`v0.3.1`), a commit SHA, or
  a container image digest.
- Which piece: `felix-wire`, `felix-transport`, `felix-client`, `felix-broker`,
  `felix-storage`, `felix-authz`, `felix-router`, `services/felix-broker-service`, or
  `services/felix-controlplane-service`.
- Impact: what an attacker gains, and what position they need to start from
  (unauthenticated network peer, holder of a valid token for another tenant,
  tenant admin, node operator, local disk access).
- Reproduction — a minimal client, a raw frame, a `curl` against the control
  plane, or a failing test is ideal.
- Configuration that matters: bootstrap mode on/off, fsync mode, replication
  factor, whether the control plane is fronted by a load balancer.

### Response targets

These are targets for a small project, not a contractual SLA.

| Stage | Target |
|---|---|
| Acknowledgement of your report | 3 business days |
| Initial assessment (valid / not / need more) | 10 business days |
| Fix or documented mitigation for a confirmed high-severity issue | 90 days from acknowledgement |

We'll keep you updated if a fix runs long, and we'll agree on disclosure timing
with you rather than dropping the advisory unannounced. Reporters are credited
in the advisory and the CHANGELOG unless you'd rather stay anonymous.

## Supported versions

Felix is pre-1.0: the wire protocol and broker semantics may change between
minor versions, and there are no long-term support branches.

| Version | Supported |
|---|---|
| Latest released minor line (currently `0.4.x`) | Security fixes |
| `main` | Security fixes |
| Older minor lines (`0.3.x`, `0.2.x`, `0.1.x`) | Not supported — upgrade |
| Pre-release / preview builds (`*-preview`) | Fixed on `main`, no separate patch release |

Fixes land on `main` and ship in the next patch release of the current minor
line. We do not backport to earlier minor lines.

## Scope

### In scope

Anything that breaks a property Felix claims to enforce:

- **Authentication.** Bypassing OIDC token exchange, forging or replaying a
  Felix token, accepting a token with an unverified or wrong-algorithm
  signature, JWKS handling flaws, or signature verification that can be skipped.
- **Tenant isolation.** Any path where a credential scoped to one tenant reads,
  writes, or observes another tenant's streams, caches, queues, or metadata —
  including through shard placement, replication, or projections.
- **Authorization / RBAC.** Privilege escalation, scope-widening policy or
  assignment writes, wildcard grammar that resolves broader than intended, a
  token exchange that widens rather than narrows granted permissions, or a
  tenant-scoped principal reaching `cluster:*` / `node:{id}` objects.
- **Bootstrap mode.** Token comparison weaknesses, replay, races that produce a
  partially initialized tenant, or any way to reach the bootstrap endpoint
  without the configured token and (when configured) client certificate.
- **Metadata Raft peer listener.** Any way to reach a Raft route (`propose`,
  `append-entries`, `vote`, snapshot install) without the cluster id and peer
  token, or without a certificate from the peer CA when peer mTLS is on, or
  to make empty members form a new group without
  `FELIX_RAFT_INITIAL_CLUSTER_STATE=new`.
- **Wire protocol.** Remotely triggered panics, unbounded allocation, integer
  overflow, or out-of-bounds reads in frame decoding; a malformed or hostile
  frame that takes down a broker, escapes its connection, or corrupts another
  subscriber's stream.
- **Durability and correctness as a security property.** Silent loss of
  acknowledged records, accepting interior segment corruption as valid, or a
  replication/lease flaw that lets two leaders acknowledge conflicting writes.
- **Resource exhaustion by a single authenticated connection** that degrades
  other tenants — subscriber queue, connection, or stream accounting that one
  peer can drive without bound.
- **Secret handling.** Tokens, signing keys, bootstrap tokens, or database
  credentials leaking into logs, metrics, error responses, or crash output.
- **Supply chain.** Compromise of the release, container, or CI publishing path
  in this repository.

### Out of scope

- **Anything on the "not built" list** in
  [docs-site — Security](https://gabloe.github.io/felix/features/security/).
  These are documented gaps, not vulnerabilities: no encryption at rest (log
  segments are plaintext on disk), no end-to-end payload encryption, no
  broker-to-broker mTLS (peers encrypt but do not authenticate each other), no
  operator-supplied client-facing broker certificate (it is generated at
  startup), and no audit logging, quotas, or rate limits. A *new* concrete
  attack these enable in a deployment that follows the deployment guidance is
  still worth reporting — a restatement of the gap is not.
- **Everything under `demos/`.** The demo crates exist to illustrate a failure
  mode or a feature; they are outside the workspace, deliberately
  under-hardened, and not for production.
- **Test fixtures, benchmark harnesses, the cluster harness
  (`crates/testing/felix-cluster`), and the load generator (`crates/testing/felix-loadgen`)** —
  local development tooling that assumes a trusted operator.
- **Defaults that are documented as development-only** — self-signed
  certificates, permissive local configs, the compose/Kubernetes examples'
  sample secrets. Report it if the docs actually recommend the insecure setting
  for production.
- **Known advisory exceptions** recorded with rationale in
  [`deny.toml`](deny.toml). They're re-reviewed when the upstream chain clears;
  a report that an exception is no longer necessary is welcome as a normal
  issue.
- Missing hardening headers, TLS configuration, or authentication on endpoints
  the deployer is expected to place behind their own perimeter — unless you can
  show an actual crossing of a boundary Felix claims to enforce.
- Reports generated purely by a scanner, with no analysis of exploitability
  against Felix.

## Testing guidance (safe harbor)

Test against **your own** deployment — a local cluster or infrastructure you
own or have written permission to test. Do not test against infrastructure
operated by the maintainers or by third parties, do not access or exfiltrate
data that isn't yours, and do not run denial-of-service or resource-exhaustion
testing against anyone else's systems.

Research conducted in good faith under this policy, reported privately through
the channel above, will not be pursued by the project. This is not a paid
program: Felix has no bug bounty, and there is no monetary reward.

## How fixes are shipped

A confirmed vulnerability gets:

1. A private fix developed on the advisory's draft or a private branch.
2. A published GitHub Security Advisory with a CVE where one applies, listing
   affected versions, the fixed version, and any mitigation for operators who
   cannot upgrade immediately.
3. A patch release tag and a CHANGELOG entry that names the advisory.
4. An update to the security documentation if the issue came from a claim the
   code could not back — stale security claims are treated as defects here.

## Hardening a deployment

Until the gaps above close, the deployment-side controls that matter most:

- **Keep bootstrap mode off** except during initial tenant setup. When it is on,
  bind it to loopback or a management network, use a high-entropy
  `FELIX_BOOTSTRAP_TOKEN`, enable mTLS on the listener, and disable it again
  immediately afterwards. It is admin-equivalent power by design. See
  [`docs/security/bootstrap.md`](docs/security/bootstrap.md).
- **Encrypt at the filesystem or block layer** where log segments live.
- **Treat the broker-to-broker network as trusted** — because Felix currently
  does. Keep it on a private network segment or overlay that provides the peer
  authentication Felix does not yet.
- **Scope tokens narrowly** at exchange time. The exchange request can only
  narrow what RBAC grants; ask for the minimum.
- **Keep token TTLs short** (`FELIX_EXCHANGE_TOKEN_TTL_SECONDS`) and rotate
  tenant signing keys through JWKS.
- **Protect the control plane's database** — it holds tenant signing keys and
  RBAC policy. Under the Raft backend the members' volumes are that database,
  and `FELIX_RAFT_PEER_TOKEN` can replace it wholesale: keep the token in a
  Secret, and the Raft peer port (`FELIX_RAFT_BIND_ADDR`) reachable from the
  other members only.

## Non-security bugs

Correctness bugs, crashes that need no hostile input, and feature requests go to
the [issue tracker](https://github.com/gabloe/felix/issues) as normal. If you're
unsure which a finding is, report it privately — we'd rather triage it down than
have it filed in public.
