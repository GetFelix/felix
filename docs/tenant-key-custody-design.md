# Tenant signing-key custody: design

Status: proposed, waiting on the maintainer's decisions at the end. Nothing
here is built. Tracks [#2801](https://github.com/GetFelix/felix/issues/2801),
threat T24 in the NineLives threat model, and the "Every control-plane member
holds every tenant's signing keys" gap on the
[threat model page](../docs-site/src/content/docs/security/threat-model.md).

## The threat

Every Felix token is an Ed25519 JWT signed with its tenant's key. Brokers
verify it against the tenant's public keys (the JWKS), so whoever holds a
tenant's private key can mint a token for that tenant with any permission,
cluster scope included, and every broker will accept it.

The private key is a 32-byte seed (`SigningKey.private_key` in
`services/felix-controlplane-service/src/auth/felix_token.rs`). The Raft
state machine must be deterministic, so the seed is generated at the API
layer and carried inside the command that stores it (`StageSigningKey`,
`SetTenantSigningKeys`, `EnsureSigningKeys` and `BootstrapTenantAuth` in
`store/raft/command.rs`). As a result the plaintext seed of every tenant's
current, staged and previous keys sits in four places on every
control-plane member:

1. the Raft log entries that carried it, until they are purged;
2. every Raft snapshot (`docs/metadata-raft-design.md` already says "a
   snapshot is a credential");
3. the in-memory state of the running member;
4. any whole-store export (`store/export.rs`) and any backup of the data
   directory.

So T24 has two halves, and they need different fixes:

- **Key theft.** Someone reads one member's disk, a snapshot, an export or a
  backup, or dumps the process memory once. They leave with every tenant's
  seed and can forge tokens long after they lose access, from anywhere. The
  only recovery is to rotate every tenant's key.
- **Live misuse.** Someone runs code as a control-plane member. They can mint
  through the member's own code path while they are there.

Encryption at rest of the volume only covers the first half, and only for a
disk read while the machine is off.

One limit applies to every design below. A member that speaks for the Raft
group can rewrite RBAC: grant some principal `cluster:*`, add an identity
provider it controls, and then get a perfectly legitimate token minted by
whatever does the signing. Key custody cannot fix that, because the metadata
is the root of authority. What key custody can do is make the damage end when
the attacker is evicted, make it visible, and stop the store's copies on
disk from being a credential. A compromised member taking over metadata is
its own threat, covered by the member certificate work on boundary 4 of the
threat model page.

## What needs a seed today

Only minting needs the private key: token exchange (`auth/exchange.rs`),
refresh (`auth/refresh.rs`) and delegation (`auth/delegate.rs`), plus key
generation when a tenant is created (`api/tenants.rs`), staged or
bootstrapped. Everything else uses the public key: the
JWKS route (`auth/jwks.rs`), the control plane checking its own API tokens
(`auth/bearer.rs`), and every broker. Refresh tokens are opaque and not
signed. That is what makes the designs below possible: the tokens, the JWKS
and the brokers can stay byte-for-byte as they are.

## Design A: wrapped seeds, opened only on signer members

The store keeps each seed encrypted ("wrapped") to a key-encryption key (KEK)
that the store never holds. Members that are configured with the KEK's
private half are *signer members*. The rest replicate ciphertext they cannot
open.

How it works:

- An operator generates an X25519 key pair as the KEK, gives the private half
  to two or more members (a file, like the other secret files), and the
  public half to all of them.
- To stage a key, a signer member generates the seed, wraps it with HPKE
  (RFC 9180: X25519, HKDF-SHA256, ChaCha20-Poly1305) and proposes a new
  command carrying `kid`, the public key, the KEK's id and the ciphertext.
  The HPKE associated data is the cluster id, tenant id and `kid`, so a
  wrapped seed cannot be moved to another tenant or cluster. A non-signer
  member forwards the stage request to a signer, the same way followers
  forward writes to the leader today.
- To mint, a signer member unwraps the seed on first use and keeps it in
  memory. A non-signer member forwards the whole exchange, refresh or
  delegation request to a signer member, which runs the checks itself. It
  does not forward "please sign these claims", because then any member could
  still get anything signed.
- KEK rotation rewraps every seed under the new KEK with one command per
  tenant. The seeds themselves do not change, so no tenant rotation is
  needed.

A variant keeps the KEK in an external KMS. The signer calls the KMS to
unwrap a seed and caches the result. The KMS adds an audit trail of unwraps
and a way to cut a member off by revoking its KMS credential, at the price of
an external dependency for every member restart.

What it protects: the Raft log, snapshots, exports and backups on every
member stop being a credential, and a non-signer member holds nothing to mint
with. Operators can run signer members on machines with no broker or
workload, which is the minimum #2801 asks for.

What it does not protect: a compromised signer member has the KEK and every
seed. A compromised non-signer member can still forward exchange requests
and rewrite RBAC through Raft. Seeds written before the change stay in
plaintext in old log entries, snapshots and backups (see Migration).

## Design B: an external signer, no seeds in Felix

Members never see a seed. A signer outside the control-plane process holds
the private keys and signs on request. The store keeps `kid`, the public key
and a reference to the key in the signer. This is the shape of the
Kubernetes external JWT signer (KEP-740) mentioned on the issue.

How it works:

- A `TokenSigner` seam in the control plane has three calls:
  `create_key(tenant) -> (kid, public_key, key_ref)`,
  `sign(key_ref, signing_input) -> signature`, and `destroy_key(key_ref)`.
  Stage, activate and retire keep their current meaning. Stage calls
  `create_key`; retire calls `destroy_key` after the Raft command commits.
- Adapters: a local signer process over a Unix socket, the OpenBao transit
  engine (which supports Ed25519 keys and signing), and PKCS#11 for HSMs
  that implement `CKM_EDDSA`.
- The signer authenticates the member (socket peer credentials or mTLS) and
  can apply its own policy before signing: a maximum lifetime, the allowed
  audiences, and refusing `cluster:*` permissions unless the caller is a
  member allowed to ask for them. It logs every signature, which gives an
  audit trail the control plane cannot rewrite.

What it protects: everything Design A does, on every member including the
ones that mint. Once a compromised member is evicted, it holds no key, so
no tenant has to rotate. With a remote signer, the keys are on a machine with
a much smaller attack surface than a control-plane member.

What it does not protect: while compromised, a member can still ask the
signer to sign anything the signer's policy allows, and can still rewrite
RBAC. A local signer on the same host as the member protects against a store
read and a memory dump of the member process, and little else.

## Design C: threshold signing

Each tenant key is split into shares with FROST (RFC 9591, Ed25519 suite).
Each member holds one share, and *t* of *n* members must cooperate to produce
a signature. The result is an ordinary Ed25519 signature, so brokers and the
JWKS do not change. No single member, and no single member's disk, can mint.

For that to mean anything, each co-signer has to decide on its own that the
token should exist: check the upstream identity-provider token and evaluate
RBAC against its own copy of the store. Otherwise the first member just asks
the others to sign whatever it wants.

The costs are high:

- Two message rounds among *t* members for every token minted, plus each
  member's own checks.
- A distributed key generation for every tenant key, and resharing every time
  the group's membership changes.
- Signing nonces that must never be reused. A reused nonce after a crash
  leaks the share, so nonce state needs its own durable, careful handling.
- Library maturity: the Rust FROST crates exist and have had an audit, but
  wiring them to openraft membership changes is new work with no prior art in
  this codebase.

This is the only design where one compromised member cannot mint at all.
It is also the one most likely to ship with a subtle key-leaking bug.

## Costs side by side

| | A: wrapped seeds | B: external signer | C: threshold |
| --- | --- | --- | --- |
| Mint latency | Unchanged on a signer; one forwarding hop otherwise | A local socket call, or one round trip to a remote signer (milliseconds) | Two rounds among *t* members, plus their checks |
| CPU per mint | One cached unwrap, then unchanged | Moves to the signer | About *t* times today's, plus the protocol |
| Tokens, JWKS, brokers | Unchanged | Unchanged | Unchanged |
| Wire flags | None | None | None |
| Metadata level | One level for the wrapped-key commands | One level for the key-reference commands | One level, plus share and reshare commands |
| Key rotation | Unchanged for tenants; KEK rotation rewraps | Unchanged; the signer creates and destroys | Unchanged API, but every rotation is a new key generation |
| New availability risk | Exchange needs a live signer member | Exchange and refresh need the signer | Exchange needs *t* healthy members |
| Operator burden | Make a KEK, pick signer members, protect the key file | Run and make the signer highly available | Little to deploy, a lot to debug |

A signer outage never affects tokens already issued: brokers verify locally.
Clients refresh every few minutes (the access token lives 900 s by default),
so an outage longer than that turns into clients failing to refresh.

Token exchange is per session and per refresh, not per message, so a few
milliseconds per mint is acceptable. The case to watch is a reconnect storm,
where every client refreshes at once and a remote signer or KMS may rate-limit.

## Migration

None of the designs change a token, the JWKS or a broker, so clients and
brokers are not touched and there is no wire negotiation.

The control plane needs new commands at a new metadata level. A group with a
member below that level keeps writing plaintext keys, as today. Once the
level is reached, new stages write wrapped keys (A) or key references (B),
and bootstrap's candidate keys go through the same path.

The plaintext seeds already written are still in old log entries, every
snapshot taken before the change, and every backup and export. Turning the
feature on does not make them safe. The migration therefore has to rotate
every tenant's key (stage, activate, retire, with the usual waits from
`docs/auth.md`), and then purge the log and take a fresh snapshot. A
`felixctl` command that does this for every tenant would make it one step.
Old backups stay a credential until the retired keys' tokens have all
expired, after which the seeds in them verify nothing.

Exports would carry wrapped keys or references. An export can only be
imported into a cluster that has the same KEK or signer, which the import
should check and refuse with a clear error.

The in-memory store, used for development and tests, keeps plaintext seeds.

## Not in scope

The broker-side lease key (`FELIX_LEASE_TOKEN_KEY_FILE`, see
`docs/auth.md`, "The lease key") has the same shape of problem on the
brokers: one key file that signs for every tenant. The `TokenSigner` seam
from Design B would fit it too, but it is a separate decision.

## Recommendation

Build Design A behind the `TokenSigner` seam from Design B.

Design A closes the key-theft half of T24 for the store, its snapshots,
exports and backups on every member, and lets an operator keep all signing on
machines that run nothing else. It needs no new process, keeps Raft as the
way keys replicate between signer members, and changes no token or broker.
The main cost is operator burden: a KEK to make, place and protect, and at
least two signer members to keep exchange available. Its built-in signer is
"unwrap with the KEK and sign locally".

Putting it behind the seam means an OpenBao transit or PKCS#11 adapter can
follow later without another store change. Those are worth doing for
deployments that already run one, and can wait until someone asks.

Design C is not practical for an alpha. It is the strongest on paper, it
costs every mint two rounds among members, and nonce and resharing mistakes
leak keys silently.

## Questions for the maintainer

1. Is "signer members" (a configured subset of members with the KEK, others
   forwarding exchange, refresh and delegation to them) acceptable as the
   minimum for NineLives M2?
2. Should the upgrade force the full tenant key rotation and log purge, or
   ship it as a documented `felixctl` step the operator runs?
3. Should the built-in signer apply its own policy (a lifetime cap, no
   `cluster:*` unless the caller is allowed), or leave all policy to RBAC?
4. Which external adapter comes first when one is wanted: OpenBao transit or
   PKCS#11?
5. Does the lease key get its own issue under the same seam, or stay as it
   is for 0.8?
