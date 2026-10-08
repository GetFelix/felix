---
title: "felixctl"
description: "The felixctl command: publish, subscribe, read caches, work consumer groups and counters, see where shards live, manage the control plane, and run benchmarks."
---

`felixctl` is Felix's command-line tool. It publishes to and reads from
streams, reads, writes and watches cache keys, claims and settles records for
consumer groups, reads and adds to counters, shows which broker owns each
shard, creates, changes and deletes tenants, namespaces, streams and caches,
manages RBAC policies and role assignments, moves shards, drains brokers,
and runs load tests. Every command prints readable text by default and JSON
with `--json`.

Its data-plane commands use only the public API of the Rust client
(`felix-client`); the control-plane commands use the control plane's REST API.

## Install

Starting with 0.6.0-preview, each release publishes felixctl in three forms.

From crates.io, which puts `felixctl` in `~/.cargo/bin`:

```bash
cargo install felixctl --version 0.6.0-preview.2
```

A preview has to be named with `--version`, because cargo skips pre-releases
otherwise.

As a prebuilt binary from the
[GitHub release](https://github.com/GetFelix/felix/releases), for Linux
(x86_64, aarch64), macOS (Apple Silicon, Intel) and Windows (x86_64). Each
archive is `felixctl-<tag>-<target>.tar.gz` (`.zip` on Windows) with a
`.sha256` beside it, and holds shell completions in `completions/` and man
pages in `man/`:

```bash
tag=v0.6.0-preview.2 target=aarch64-apple-darwin
base=https://github.com/GetFelix/felix/releases/download/$tag
curl -fsSLO "$base/felixctl-$tag-$target.tar.gz"
curl -fsSLO "$base/felixctl-$tag-$target.tar.gz.sha256"
shasum -a 256 -c "felixctl-$tag-$target.tar.gz.sha256"
tar -xzf "felixctl-$tag-$target.tar.gz"
```

As a container image for linux/amd64 and linux/arm64. The config file is
written mode `0600`, so run as your own user to read a mounted one:

```bash
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$HOME/.config/felixctl:/home/felix/.config/felixctl" \
  ghcr.io/getfelix/felixctl:0.6.0-preview.2 stream ls
```

Releases before 0.6.0-preview.2 are under `ghcr.io/gabloe`, the project's previous owner.

A preview tag is never `latest`.

From a checkout of the repository:

```bash
cargo install --path crates/tools/felixctl
```

To run it without installing, use `cargo run --release -p felixctl -- <args>`.

## Try it against a local cluster

`felix-cluster up` starts a control plane and brokers, creates a stream named
`orders` and a cache named `users`, and writes what a client needs into a
session file in your temp directory. Start one and leave it running:

```bash
cargo run --release -p felix-cluster -- up --nodes 3
```

In a second terminal, turn the session file into a context. The brokers
generate their own certificates, and the session names each one so the CLI can
check them:

```bash
S=${TMPDIR:-/tmp}/felix-cluster.json
cat $(jq -r '.nodes[].cert_file' "$S") > brokers.pem
jq -r .client_token "$S" > token.jwt

felixctl context add local \
  --brokers "$(jq -r '[.nodes[].client_addr] | join(",")' "$S")" \
  --tenant "$(jq -r .tenant_id "$S")" \
  --namespace "$(jq -r .namespace "$S")" \
  --token-file token.jwt \
  --ca-file brokers.pem \
  --controlplane-url "$(jq -r .control_plane "$S")" \
  --controlplane-token "$(jq -r .admin_token "$S")"
```

The tokens in the session file last an hour, and `up` rewrites the file with
fresh ones every half hour. If a context stops authenticating, rerun the commands
above to pick up the current ones.

The first context you add becomes the current one. Now subscribe in one
terminal:

```bash
felixctl sub orders
```

and publish in another:

```bash
felixctl pub orders 'hello'
```

The publisher reports the offset the record was written at, when the broker's
acknowledgement carries one (see below), and the subscriber prints `hello`. Read a cache key, see where the stream lives,
and run a short benchmark:

```bash
felixctl cache put users alice 'online'
felixctl cache get users alice
felixctl topology orders
felixctl bench latency orders
```

The benchmark prints the publish rate with the acknowledgement p50 and p99,
then the delivered rate with the publish-to-delivery p50 and p99. Numbers from
a laptop loopback run say little about a real deployment; see
[Performance](/features/performance/) for measured results.

## Contexts

A context is a named set of connection settings: broker addresses, the
control-plane URL, tenant, namespace, tokens and TLS files. They live in
`felixctl/config.toml` under `$XDG_CONFIG_HOME`, or by default
`~/.config` on Linux, `~/Library/Application Support` on macOS and
`%APPDATA%` on Windows. `--config` or `FELIX_CLI_CONFIG` points elsewhere.

```bash
felixctl context add prod --brokers a.example:5000,b.example:5000 \
    --tenant acme --token-file ~/.felix/acme.jwt --use
felixctl context ls
felixctl context use local
felixctl context rm prod
```

`context add` saves the connection flags given on its command line and nothing
from the environment. Relative file paths are saved as absolute ones. The file
is written readable only by you, because a context can hold a token, and a key
it does not recognise is an error rather than ignored.

Each setting comes from the first of: a flag, its environment variable, the
context. `--context` or `FELIX_CONTEXT` picks a context other than the current
one.

| Flag | Variable | Meaning |
| --- | --- | --- |
| `--brokers` | `FELIX_BROKERS` | Broker addresses, `host:port`, comma-separated |
| `--tenant` | `FELIX_AUTH_TENANT` | Tenant to act as |
| `-n`, `--namespace` | `FELIX_NAMESPACE` | Namespace, `default` if unset |
| `--token`, `--token-file` | `FELIX_AUTH_TOKEN`, `FELIX_AUTH_TOKEN_FILE` | Token the brokers accept |
| `--controlplane-url` | `FELIX_CONTROLPLANE_URL` | Control-plane base URL |
| `--controlplane-token`, `--controlplane-token-file` | `FELIX_CONTROLPLANE_TOKEN`, `FELIX_CONTROLPLANE_TOKEN_FILE` | Token the control plane accepts |
| `--ca-file` | `FELIX_CA_FILE` | PEM bundle broker certificates are checked against |
| `--client-cert-file`, `--client-key-file` | `FELIX_CLIENT_CERT_FILE`, `FELIX_CLIENT_KEY_FILE` | Client certificate to present |
| `--controlplane-ca-file` | `FELIX_CONTROLPLANE_CA` | PEM bundle an `https://` control plane is checked against |
| `--server-name` | `FELIX_SERVER_NAME` | TLS server name, by default the first broker's host name or `localhost` |
| `--alpn` | | Offer the `felix/1` ALPN, which a broker with `FELIX_TLS_REQUIRE_ALPN` needs |

Brokers and the control plane accept different tokens: a broker refuses a
token carrying control-plane actions. That is why there are two.

Certificates are always checked. Without `--ca-file` the platform trust store
is used, which suits a broker serving a certificate from a public CA. A broker
that generates its own certificate can export it with `FELIX_TLS_CERT_EXPORT`;
pass that file as `--ca-file`. A client certificate, when given, is presented
to every broker, which is what a broker with `FELIX_TLS_CLIENT_CA` and
`FELIX_TLS_CLIENT_CERT_BIND_SUBJECT` expects.

The client's tuning variables (`FELIX_PUB_CONN_POOL`, `FELIX_CLIENT_CONFIG` and
the rest in the [environment reference](/reference/environment-variables/))
apply to `felixctl` as to any client.

## Publishing

```bash
felixctl pub orders 'hello'                       # one message
felixctl pub orders --key customer-42 '{"n":1}'   # keyed: same key, same shard
tail -f app.log | felixctl pub logs               # one message per line of stdin
felixctl pub images --file cat.png                # a file as one message
felixctl pub orders --whole < report.json         # all of stdin as one message
felixctl pub orders 'tick' --count 100            # the same message 100 times
```

Each publish waits for the broker's acknowledgement and reports the offset it
carries. Not every acknowledgement carries one. A broker that owns the shard
and runs with `FELIX_ACK_ON_COMMIT` off (the default) answers a `Leader`
stream's publish as soon as it is queued, before the record has an offset, so
`--json` shows `null` for it. A publish that reached another broker first is
forwarded to the owner and answered after the write, so it does carry one.
Which broker the client reached decides which you get. `--idempotent` and
`Quorum` streams are always answered after the write.
`--ack none` sends without waiting, and still flushes before exiting.
`--idempotent` publishes through an idempotent producer, so a re-send after a
reconnect cannot duplicate a record. With `--key`, the producer keeps a
sequence for the key's shard.

## Subscribing

```bash
felixctl sub orders                                 # from now on
felixctl sub orders --from earliest --count 10      # the oldest ten retained
felixctl sub orders --from 1500 --format offsets    # resume at offset 1500
felixctl sub orders --shard 2                       # one shard only
felixctl sub orders --json | jq -r .payload
```

Without `--shard`, every shard of the stream is read and merged; the order
between shards is not defined. `--format raw` prints each payload on a line,
`offsets` prefixes the shard and offset, and `json` (or `--json`) prints one
object per message with `payload`, or `payload_base64` for bytes that are not
UTF-8. On a terminal, `raw` and `offsets` escape bytes that are not printable
text (`\x00`); piped, they write the payload byte for byte. The subscription
follows a shard that moves to another broker, with or without `--shard`.

## Caches

```bash
felixctl cache put users alice 'online' --ttl-ms 60000
felixctl cache get users alice
felixctl cache del users alice
felixctl cache watch users                            # every key, every shard
felixctl cache watch users --key alice                # one key
felixctl cache watch users --prefix al --retained     # current values, then changes
felixctl cache ls
felixctl cache info users
```

`cache get` prints the value as stored and exits with status 5 when the key is
not set. `cache put` reads the value from stdin when none is given. A watch
prints `key<TAB>value`, or `(deleted)`, per change.

## Consumer groups

```bash
felixctl group create orders billing --from earliest
felixctl group poll orders billing --max 5
felixctl group ack orders billing 0:15:1
felixctl group nack orders billing 0:16:1 --delay-ms 30000
felixctl group extend orders billing 0:17:1 --for-ms 60000
felixctl group describe orders billing
felixctl group seek orders billing latest
felixctl group rm orders billing --yes
```

A group keeps a cursor on each shard of its stream, held by that shard's
leader. `create`, `describe`, `seek` and `rm` act on every shard unless
`--shard` names one; `poll` asks each shard in turn until it has `--max`
records, waiting up to `--wait-ms` on each.

`poll` prints each record as `CLAIM<TAB>payload`. The claim is
`SHARD:OFFSET:ATTEMPTS`, and it is what the settling commands take: `ack`
finishes a record, `nack` hands it back (after `--delay-ms`, if given), and
`extend` keeps the claim standing longer. `extend` needs the attempt count,
because the broker refuses to extend a record that has been handed out again
since. The others accept `SHARD:OFFSET` as well.

```
$ felixctl group poll orders billing --max 2
0:15:1	{"id": 1}
0:16:1	{"id": 2}
$ felixctl group describe orders billing
SHARD  COMMITTED  TAIL  LAG  IN_FLIGHT  OWED  DEAD_LETTERS
0      15         40    25   2          0     0
1      -          12    12   0          0     0
```

`COMMITTED` is `-` on a shard where the group has no cursor yet; a poll there
starts at the beginning of the log. `IN_FLIGHT` and `OWED` are the leader's
memory and start from zero after a leader change.

Dead letters are records the group gave up on. The records stay in the log;
the group lists their offsets.

```bash
felixctl group dead-letters ls orders billing
felixctl group dead-letters add orders billing 0:17:2       # give up on a claimed record
felixctl group dead-letters redrive orders billing 0:17     # deliver it again
felixctl group dead-letters discard orders billing 0:17 --yes
```

`create`, `seek`, `rm`, `redrive` and `discard` need a broker token allowed
`group.manage` (or `stream.manage`) on the stream; the rest need
`group.consume` (or `stream.subscribe`).

Three commands ask before acting, because they cannot be taken back: `rm`,
`dead-letters discard`, and a `seek` that moves any shard's cursor back over
records the group has finished. At a terminal they ask `[y/N]`; anywhere else,
they need `--yes` and exit with status 2 without it. `seek` reads where the
group stands to tell; when it cannot, as with a token allowed only
`group.manage`, it asks for any seek other than `latest`. A seek to an offset needs
`--shard` on a stream with more than one shard, since offsets are per shard.

## Counters

```bash
felixctl counter add stats page-views 1
felixctl counter add stats stock -3
felixctl counter get stats page-views
```

A counter lives in a cache, beside its keys. `add` prints the sum including
the add and is sent once: a failure is not retried, because the broker may
already have applied it. `get` exits with status 5 for a counter that has
never been written.

## Topology

```bash
felixctl topology orders
felixctl topology users --cache
```

```
stream t1/ns/orders: 1 shard(s), modulo routing

SHARD  LEADER    ADDR             REPLICAS  GENERATION  STATE
0      broker-2  127.0.0.1:50410  broker-2  1           active

BROKER    ADDR
broker-0  127.0.0.1:53348
broker-1  127.0.0.1:65027
broker-2  127.0.0.1:50410
```

The shard count, each shard's owner and the brokers come from a broker.
Replicas and assignment state come from the control plane, so those columns are
filled only when a control-plane URL is set. A broker older than this release
cannot name owners; against one, owners come from the control plane too.

## The control plane

### Listing and inspecting

```bash
felixctl tenant ls
felixctl tenant info t1
felixctl namespace ls
felixctl stream ls
felixctl stream info orders
felixctl node ls
felixctl node info broker-1
felixctl shard ls --leader broker-2
felixctl shard ls --name orders --json
```

Listings follow the control plane's `next_cursor` until every page is read.
`tenant ls` needs a token allowed `tenant.manage` on the whole cluster; the
rest need the matching manage or view permission for the tenant, or
`node.view` for nodes and shards.

### Creating, changing and deleting

```bash
felixctl tenant create acme --display-name 'Acme Corp'
felixctl namespace create payments --tenant acme
felixctl stream create orders --shards 4 --replication 3 --consistency quorum
felixctl stream set orders --retention-secs 86400
felixctl cache create sessions --shards 2
felixctl cache set sessions --display-name 'Login sessions'
felixctl stream rm orders
felixctl namespace rm payments --yes
```

Streams and caches go in the current tenant and namespace, namespaces in the
current tenant. `stream create` defaults to one shard, a replication factor
of 1, `leader` consistency, `at-least-once` delivery, a durable log and the
broker's own retention bounds:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--shards` | 1 | Shard count, fixed at creation |
| `--replication` | 1 | Brokers holding a copy of each shard, the leader included |
| `--kind` | `stream` | The kind recorded for the stream, `stream` or `queue` |
| `--consistency` | `leader` | `quorum` acknowledges once a majority of copies hold a record |
| `--delivery` | `at-least-once` | or `at-most-once` |
| `--durable` | `true` | `false` keeps records in memory only |
| `--retention-secs`, `--retention-bytes` | broker's bound | Age and per-shard size bounds |
| `--region` | anywhere | Region the stream's data stays in, fixed at creation |
| `--routing` | `modulo` | `jump-hash` once the `jump_hash_routing` fleet feature is finalized |

`cache create` takes `--shards`, `--replication`, `--consistency` and
`--display-name`. Creating a stream or cache that already exists with the
same settings succeeds and says so; with different settings it is refused
with status 4. A tenant or namespace that already exists is refused too.

`stream set` changes consistency, delivery, durability or retention and prints
each field that changed. A retention bound not given keeps its value, and
`default` hands a bound back to the broker. Shards, replication, region and
routing cannot change. `cache set` changes only the display name.

`rm` deletes, and deleting a namespace or tenant deletes everything in it. A
tenant takes its signing keys and RBAC rules with it.

### Brokers, shards and placement

```bash
felixctl node drain broker-2
felixctl node deregister broker-2 --yes
felixctl shard move orders 2 --to broker-3 --dry-run
felixctl shard move orders 2 --to broker-3
felixctl shard move sessions 0 --to broker-1 --cache
felixctl shard move cancel orders 2
felixctl placement pause
felixctl placement resume
felixctl placement abandon orders 2 --yes
```

`node drain` stops placement putting anything new on a broker and moves its
shards away while it keeps serving them. `node deregister` marks it as having
left on purpose; its shards fail over once its lease has run out, so drain it
first to move them without a failover.

`shard move` moves a shard's leadership: the destination copies the log, and
the old leader serves until it has caught up and cuts over. It prints the step
the move is at and the assignment it wrote; `--dry-run` prints what it would
write without starting it. `shard move cancel` stops a move that has not cut
over. Placement may start the same move again, so `placement pause` first to
keep a shard where it is. Pausing stops only the moves placement starts by
itself: new shards are still placed, failed leaders still replaced, and moves
under way finish.

`placement abandon` gives up the log of a durable shard whose only copies are
out of reach and places it afresh. Records only the old leader held are lost,
acknowledged ones included. The control plane refuses it while the leader is
serving or a replica can take over without loss.

These need `node.manage` on the cluster.

### Confirmation

`tenant rm`, `namespace rm`, `stream rm`, `cache rm`, `node drain` and
`node deregister` ask before acting when stdin is a terminal, and go ahead
only on `y` or `yes`. Anywhere else, a script or a pipe, they need `--yes`
(`-y`) and stop with status 2 without it. `placement abandon` never asks and
always needs `--yes`.

## RBAC

```bash
felixctl rbac policy ls
felixctl rbac policy ls --subject role:reader --json
felixctl rbac policy add role:reader stream:t1/payments/* stream.subscribe
felixctl rbac policy add role:alice-session cache:t1/default/sessions/user:alice cache.read
felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.write
felixctl rbac policy rm role:reader stream:t1/payments/* stream.subscribe
felixctl rbac grouping ls --role role:reader
felixctl rbac grouping add p:alice role:reader
felixctl rbac grouping add 'group:https://idp.example#ops' role:reader
felixctl rbac grouping rm p:alice role:reader --yes
```

A policy lets a subject, usually a role, take an action on an object. A
grouping assigns a user, or an IdP group, to a role. The commands act on the
current tenant and need a control-plane token with `rbac.view`,
`rbac.policy.manage` or `rbac.assignment.manage` over the objects involved.
A change reaches brokers in the next token the principal is issued.

`policy add` checks the object against the
[object grammar](/features/security/#rbac-object-grammar-and-delegation)
before sending it, and stops with status 2 and the reason when it does not
fit: an object for another tenant, a `*` over a named stream, a cache key with
a `*` anywhere but the end, or a key object with an action other than
`cache.read` or `cache.write`. An object of a kind felixctl does not know is
sent as is. Action names, and whether the token may grant the rule, are left
to the control plane, which answers a refusal with status 4 and its message.

`rm` removes one rule, named exactly as `ls` prints it, and exits with status
5 when there is none. It asks first on a terminal, and needs `--yes`
anywhere else.

## Benchmarks

`felixctl bench` runs the scenarios of `felix-loadgen`, the instrument behind
the [real-network performance runs](/features/performance/), in process
against the current context, and prints the rate and the p50 and p99 latency.

```bash
felixctl bench latency orders                 # 1 publisher, 1 subscriber
felixctl bench fanout orders --fanout 20      # 1 publisher, 20 subscribers
felixctl bench ingest orders --concurrency 8  # several publishers, no subscribers
felixctl bench cache users                    # cache put then get
felixctl bench latency orders --json          # summary plus the full result
```

The defaults finish in seconds: 500 warmup and 5000 measured operations, 256
byte payloads. The stream or cache must exist. `--json` includes the full
`LOADGEN_JSON` object the perf suite records, so a `felixctl bench` result and
a `felix-loadgen` one can be compared field by field. Run benchmarks only
against a cluster you are allowed to load.

## Output and exit status

Text is for reading; `--json` is for scripts. With `--json`, single results
are one JSON object, `sub`, `cache watch` and `group poll` print one object per line, and an
error goes to stderr as `{"error": "...", "exit": N}`.

| Status | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Any other failure |
| 2 | Bad arguments, or a setting missing or unreadable |
| 3 | No broker or control plane could be reached |
| 4 | A broker or the control plane refused the request |
| 5 | The key, resource or context does not exist |

## Help, completions and man pages

`felixctl help <command>` and `felixctl <command> --help` print the same help,
with examples. `felixctl` alone prints a short overview.

```bash
felixctl completions bash > ~/.local/share/bash-completion/completions/felixctl
felixctl completions zsh > "${fpath[1]}/_felixctl"
felixctl completions fish > ~/.config/fish/completions/felixctl.fish
felixctl man --out-dir ~/.local/share/man/man1
```

## Not yet

State reads are planned, and so is a Homebrew formula. See
[issue #1005](https://github.com/GetFelix/felix/issues/1005).
