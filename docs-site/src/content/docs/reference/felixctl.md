---
title: "felixctl Reference"
description: "Every felixctl command, subcommand and flag, with defaults and examples."
---

Every felixctl command and flag. This page is generated from the command definitions, and `felixctl help <command>` prints the same text. For installing felixctl, setting up a context and a first session, see [felixctl](/getting-started/felixctl/).

Publish and subscribe to streams, read and watch caches, see where shards live, list what the control plane knows, and run benchmarks.

Connection settings come from a named context (see `felixctl context`), overridden by FELIX_\* environment variables, overridden by flags.

Examples:

```bash
felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 --token-file token.jwt
felixctl pub orders 'hello'
felixctl sub orders --from earliest --count 10
felixctl stream ls --json
```

## Commands

| Command | Summary |
| --- | --- |
| [`felixctl context`](#felixctl-context) | Manage named connection profiles |
| [`felixctl pub`](#felixctl-pub) | Publish messages to a stream |
| [`felixctl sub`](#felixctl-sub) | Read messages from a stream |
| [`felixctl cache`](#felixctl-cache) | Read, write and watch cache keys, or manage caches |
| [`felixctl topology`](#felixctl-topology) | Show a stream's or cache's shards, owners and brokers |
| [`felixctl group`](#felixctl-group) | Create, inspect and move consumer groups; claim and settle records |
| [`felixctl counter`](#felixctl-counter) | Read and add to counters |
| [`felixctl tenant`](#felixctl-tenant) | Create, list, inspect and delete tenants (control plane) |
| [`felixctl namespace`](#felixctl-namespace) | Create, list, inspect and delete namespaces (control plane) |
| [`felixctl stream`](#felixctl-stream) | Create, change, list, inspect and delete streams (control plane) |
| [`felixctl node`](#felixctl-node) | List, inspect, drain and deregister brokers (control plane) |
| [`felixctl shard`](#felixctl-shard) | List shard assignments and move shards (control plane) |
| [`felixctl placement`](#felixctl-placement) | Pause or resume placement's own moves (control plane) |
| [`felixctl rbac`](#felixctl-rbac) | List, grant and revoke RBAC policies and role assignments (control plane) |
| [`felixctl inspect`](#felixctl-inspect) | Look at a cluster's live state, read-only (operators) |
| [`felixctl bench`](#felixctl-bench) | Run a load test against the cluster |
| [`felixctl completions`](#felixctl-completions) | Print a shell completion script |

## Global options

These flags apply to every command. Each connection flag overrides the environment variable named in its help, which overrides the current context.

### Connection

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--context` | `<NAME>` | - | Context to use instead of the current one [env: FELIX_CONTEXT] |
| `--config` | `<PATH>` | - | Config file holding the contexts [env: FELIX_CLI_CONFIG] [default: the platform config directory, e.g. ~/.config/felixctl/config.toml] |
| `--brokers` | `<ADDRS>` | - | Broker addresses, host:port, comma-separated [env: FELIX_BROKERS] |
| `--controlplane-url` | `<URL>` | - | Control-plane base URL [env: FELIX_CONTROLPLANE_URL] |
| `--tenant` | `<ID>` | - | Tenant to act as [env: FELIX_AUTH_TENANT] |
| `-n, --namespace` | `<NS>` | - | Namespace [env: FELIX_NAMESPACE] [default: default] |
| `--token` | `<JWT>` | - | Broker token [env: FELIX_AUTH_TOKEN] |
| `--token-file` | `<PATH>` | - | File holding the broker token [env: FELIX_AUTH_TOKEN_FILE] |
| `--controlplane-token` | `<JWT>` | - | Control-plane token [env: FELIX_CONTROLPLANE_TOKEN] |
| `--controlplane-token-file` | `<PATH>` | - | File holding the control-plane token [env: FELIX_CONTROLPLANE_TOKEN_FILE] |
| `--ca-file` | `<PATH>` | - | PEM bundle the broker certificates are checked against [env: FELIX_CA_FILE] [default: the platform trust store] |
| `--client-cert-file` | `<PATH>` | - | PEM client certificate chain to present to brokers [env: FELIX_CLIENT_CERT_FILE] |
| `--client-key-file` | `<PATH>` | - | PEM private key for --client-cert-file [env: FELIX_CLIENT_KEY_FILE] |
| `--controlplane-ca-file` | `<PATH>` | - | PEM bundle an https control plane is checked against [env: FELIX_CONTROLPLANE_CA] [default: the platform trust store] |
| `--server-name` | `<NAME>` | - | TLS server name sent to brokers [env: FELIX_SERVER_NAME] [default: the first broker's host name, or localhost for an IP address] |
| `--alpn` | - | - | Offer the felix/1 ALPN, which a broker with FELIX_TLS_REQUIRE_ALPN needs. Brokers from before ALPN support refuse it |

### Output

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--json` | - | - | Print JSON instead of human-readable output. Errors go to stderr as JSON too |

## Exit status

Scripts can branch on the exit status. Each value means one thing.

| Status | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | Anything not covered below. |
| 2 | Bad arguments, or settings missing or unreadable. |
| 3 | No broker or control plane could be reached. |
| 4 | A broker or the control plane refused the request. |
| 5 | The key, resource or context does not exist. |

With `--json`, errors go to stderr as a JSON object holding `error` and `exit`.

## `felixctl context`

Manage named connection profiles.

A context holds the broker addresses, control-plane URL, tenant, namespace, tokens and TLS files for one cluster, so commands do not need them as flags. Contexts live in a TOML file in the platform config directory; --config or FELIX_CLI_CONFIG points elsewhere.

```text
felixctl context [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 --token-file token.jwt
felixctl context ls
felixctl context use prod
felixctl context rm local
```

### `felixctl context add`

Save the connection flags given on this command line as a new context. Only flags are saved; environment variables are not. The first context added becomes the current one.

```text
felixctl context add [OPTIONS] <NAME>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Name of the context |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--use` | - | - | Make it the current context |
| `--replace` | - | - | Replace a context that already has this name |

Examples:

```bash
felixctl context add local --brokers 127.0.0.1:5000 --tenant t1 \
    --token-file token.jwt --ca-file broker-cert.pem
felixctl context add prod --brokers a.example:5000,b.example:5000 \
    --controlplane-url https://cp.example --tenant acme --use
```

### `felixctl context use`

Make NAME the context every command uses unless --context or FELIX_CONTEXT says otherwise.

```text
felixctl context use [OPTIONS] <NAME>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Name of the context |

Examples:

```bash
felixctl context use prod
```

### `felixctl context ls`

List the saved contexts. The current one is marked with \*.

```text
felixctl context ls [OPTIONS]
```

Examples:

```bash
felixctl context ls
felixctl context ls --json
```

### `felixctl context rm`

Delete a saved context. Deleting the current one leaves no context current.

```text
felixctl context rm [OPTIONS] <NAME>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Name of the context |

Examples:

```bash
felixctl context rm local
```

## `felixctl pub`

Publish messages to a stream.

The message comes from the DATA argument, from --file, or from stdin. Stdin is read one message per line unless --whole is given. By default each publish waits for the broker's acknowledgement and reports the record's offset when the ack carries one. A broker that owns the shard and acks on enqueue (FELIX_ACK_ON_COMMIT=0, or an in-memory stream) answers before the record has an offset. An --idempotent publish is always answered after the write.

```text
felixctl pub [OPTIONS] <STREAM> [DATA]
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream to publish to |
| `DATA` | no | The message. Without it, --file or stdin is read |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--file` | `<PATH>` | - | Publish this file's contents as one message |
| `--whole` | - | - | Read all of stdin as one message instead of one per line |
| `--key` | `<KEY>` | - | Routing key; records with the same key go to the same shard |
| `--count` | `<N>` | `1` | Publish each message this many times |
| `--ack` | `<MODE>` | `message` | What to wait for: `message` waits for each acknowledgement, `none` sends without one. One of `message`, `none`. |
| `--idempotent` | - | - | Publish through an idempotent producer, so a re-send after a reconnect cannot duplicate a record. Always acknowledged |

Examples:

```bash
felixctl pub orders 'hello'
felixctl pub orders --key customer-42 '{"total": 10}'
tail -f app.log | felixctl pub logs
felixctl pub images --file cat.png --idempotent
```

## `felixctl sub`

Read messages from a stream and print them.

Starts at the live tail unless --from says otherwise. Without --shard, every shard of the stream is read and merged. Runs until --count messages have arrived or it is interrupted.

```text
felixctl sub [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream to read |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--from` | `<POSITION>` | `latest` | Where to start: `latest`, `earliest`, or a log offset |
| `--shard` | `<N>` | - | Read only this shard |
| `--count` | `<N>` | - | Stop after this many messages |
| `--format` | `<FORMAT>` | `raw` | How to print each message. --json is the same as --format json. One of `raw`, `offsets`, `json`. |

Examples:

```bash
felixctl sub orders
felixctl sub orders --from earliest --count 10
felixctl sub orders --shard 2 --from 1500 --format offsets
felixctl sub orders --json | jq .payload
```

## `felixctl cache`

Read, write and watch cache keys, or create, change, list, inspect and delete caches.

get, put, del and watch go to the brokers. ls, info, create, set and rm go to the control plane.

```text
felixctl cache [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl cache put sessions user-1 'logged-in' --ttl-ms 60000
felixctl cache get sessions user-1
felixctl cache watch sessions --prefix user-
felixctl cache create sessions --shards 4
felixctl cache ls
```

### `felixctl cache get`

Print a key's value as stored. Exits with status 5 when the key is not set.

```text
felixctl cache get [OPTIONS] <CACHE> <KEY>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |
| `KEY` | yes | Key to read |

Examples:

```bash
felixctl cache get sessions user-1
felixctl cache get sessions user-1 --json
```

### `felixctl cache put`

Set a key to VALUE, to the contents of --file, or to all of stdin.

```text
felixctl cache put [OPTIONS] <CACHE> <KEY> [VALUE]
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |
| `KEY` | yes | Key to set |
| `VALUE` | no | The value. Without it, --file or stdin is read |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--file` | `<PATH>` | - | Use this file's contents as the value |
| `--ttl-ms` | `<MS>` | - | Expire the key after this many milliseconds [default: never] |

Examples:

```bash
felixctl cache put sessions user-1 'logged-in'
felixctl cache put sessions user-1 'logged-in' --ttl-ms 60000
felixctl cache put config app --file app.json
```

### `felixctl cache del`

Delete a key. Deleting a key that is not set succeeds.

```text
felixctl cache del [OPTIONS] <CACHE> <KEY>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |
| `KEY` | yes | Key to delete |

Examples:

```bash
felixctl cache del sessions user-1
```

### `felixctl cache watch`

Print changes to one key (--key) or to every key under a prefix (--prefix, default every key) as they happen. A prefix watch covers every shard of the cache. With --retained, the current values are printed first.

```text
felixctl cache watch [OPTIONS] <CACHE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--key` | `<KEY>` | - | Watch only this key |
| `--prefix` | `<PREFIX>` | - | Watch keys starting with this prefix [default: every key] |
| `--retained` | - | - | Print each matching key's current value before live changes |
| `--from` | `<OFFSET>` | - | Resume a key watch at this cache-log offset |
| `--count` | `<N>` | - | Stop after this many changes |

Examples:

```bash
felixctl cache watch sessions
felixctl cache watch sessions --key user-1
felixctl cache watch sessions --prefix user- --retained --count 100
```

### `felixctl cache ls`

List the caches of the current tenant and namespace.

```text
felixctl cache ls [OPTIONS]
```

Examples:

```bash
felixctl cache ls
felixctl cache ls -n payments
```

### `felixctl cache info`

Show a cache's shard count, replication factor and consistency.

```text
felixctl cache info [OPTIONS] <CACHE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |

Examples:

```bash
felixctl cache info sessions
```

### `felixctl cache create`

Create a cache in the current tenant and namespace. Creating one that already exists with the same settings succeeds; with different settings it is refused.

```text
felixctl cache create [OPTIONS] <CACHE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--display-name` | `<TEXT>` | - | Display name [default: the cache name] |
| `--shards` | `<N>` | `1` | Shards to split the keyspace across |
| `--replication` | `<N>` | `1` | Brokers holding a copy of each shard, the leader included |
| `--consistency` | `<LEVEL>` | `leader` | `quorum` acknowledges a write once a majority of the shard's copies hold it. One of `leader`, `quorum`. |

Examples:

```bash
felixctl cache create sessions
felixctl cache create sessions --shards 4 --replication 3 --consistency quorum
```

### `felixctl cache set`

Change a cache's display name, the only setting that can change after creation, and print what changed.

```text
felixctl cache set [OPTIONS] --display-name <TEXT> <CACHE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--display-name` | `<TEXT>` | - | New display name |

Examples:

```bash
felixctl cache set sessions --display-name 'Login sessions'
```

### `felixctl cache rm`

Delete a cache and every key in it. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl cache rm [OPTIONS] <CACHE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl cache rm sessions
felixctl cache rm sessions --yes
```

## `felixctl topology`

Show where a stream or cache lives: its shard count, the broker that leads each shard, and the brokers clients can reach.

The shard count and brokers come from a broker. Owners come from the control plane, so they are shown only when a control-plane URL is configured.

```text
felixctl topology [OPTIONS] <NAME>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Stream (or, with --cache, cache) name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--cache` | - | - | NAME is a cache, not a stream |

Examples:

```bash
felixctl topology orders
felixctl topology sessions --cache
felixctl topology orders --json
```

## `felixctl group`

Work with consumer groups: create, describe, seek and delete them, claim records with poll, and settle them with ack, nack, extend and dead-letters.

A group keeps a cursor on each shard of its stream, with that shard's leader. A record is named by its claim, SHARD:OFFSET, which poll prints with the delivery attempt added.

```text
felixctl group [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl group create orders billing --from earliest
felixctl group poll orders billing --max 5
felixctl group ack orders billing 0:15:1
felixctl group describe orders billing
```

### `felixctl group create`

Create GROUP on every shard of STREAM, or on --shard, so that its first poll begins at --from instead of the beginning of the log. A shard where the group already exists is left where it is. Needs the group.manage permission.

```text
felixctl group create [OPTIONS] <STREAM> <GROUP>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard [default: every shard] |
| `--from` | `<POSITION>` | `latest` | Where the group starts: `latest`, `earliest`, or an offset (with --shard, or on a single-shard stream) |

Examples:

```bash
felixctl group create orders billing
felixctl group create orders billing --from earliest
felixctl group create orders billing --shard 2 --from 1500
```

### `felixctl group describe`

Show, for each shard, the offset GROUP has finished up to, the shard's tail, the lag between them, records handed out and not settled, records owed again, and dead letters. In-flight and owed counts are the leader's memory and start from zero after a leader change.

```text
felixctl group describe [OPTIONS] <STREAM> <GROUP>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard [default: every shard] |

Examples:

```bash
felixctl group describe orders billing
felixctl group describe orders billing --shard 0 --json
```

### `felixctl group seek`

Move GROUP's cursor on every shard, or on --shard, to POSITION. Records the group had handed out are void and are delivered again from the new position. Dead letters are kept. Moving the cursor backwards replays finished records, so it asks first on a terminal and needs --yes elsewhere. Needs the group.manage permission.

```text
felixctl group seek [OPTIONS] <STREAM> <GROUP> <POSITION>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `POSITION` | yes | Where to move to: `latest`, `earliest`, or an offset (with --shard, or on a single-shard stream) |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard [default: every shard] |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl group seek orders billing latest
felixctl group seek orders billing earliest --yes
felixctl group seek orders billing 1500 --shard 2
```

### `felixctl group rm`

Delete GROUP on every shard, or on --shard: its cursor, its dead letters and the records it has handed out. A consumer that polls it again starts afresh. Asks first on a terminal; elsewhere it needs --yes. Needs the group.manage permission.

```text
felixctl group rm [OPTIONS] <STREAM> <GROUP>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard [default: every shard] |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl group rm orders billing
felixctl group rm orders billing --yes
```

### `felixctl group poll`

Claim up to --max records for GROUP and print each with its claim, SHARD:OFFSET:ATTEMPTS, which ack, nack, extend and dead-letters add take. Without --shard each shard is asked in turn until --max records are claimed. A claim not settled is handed out again once it lapses.

```text
felixctl group poll [OPTIONS] <STREAM> <GROUP>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard [default: every shard] |
| `--max` | `<N>` | `10` | Most records to claim |
| `--wait-ms` | `<MS>` | `0` | How long the broker may wait for records on each shard it is asked |
| `--visibility-ms` | `<MS>` | - | How long the claims stand [default: the broker's visibility timeout] |

Examples:

```bash
felixctl group poll orders billing
felixctl group poll orders billing --max 100 --wait-ms 5000
felixctl group poll orders billing --shard 1 --visibility-ms 60000 --json
```

### `felixctl group ack`

Finish each claimed record. Claims are acted on in order; one that fails stops the rest.

```text
felixctl group ack [OPTIONS] <STREAM> <GROUP> <CLAIM>...
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `CLAIM` | yes | Records, as SHARD:OFFSET or the SHARD:OFFSET:ATTEMPTS poll prints |

Examples:

```bash
felixctl group ack orders billing 0:15:1
felixctl group ack orders billing 0:15 0:16 1:4
```

### `felixctl group nack`

Hand each claimed record back to the group, to be delivered again at once, or after --delay-ms. A claim given as SHARD:OFFSET:ATTEMPTS is refused once that delivery's claim has lapsed or the record has gone out again.

```text
felixctl group nack [OPTIONS] <STREAM> <GROUP> <CLAIM>...
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `CLAIM` | yes | Records, as SHARD:OFFSET or the SHARD:OFFSET:ATTEMPTS poll prints |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--delay-ms` | `<MS>` | - | Deliver the records again only after this long |

Examples:

```bash
felixctl group nack orders billing 0:15:1
felixctl group nack orders billing 0:15:1 --delay-ms 30000
```

### `felixctl group extend`

Keep the claim on a record standing for --for-ms from now, for a consumer still working on it. The claim needs its attempt count, as poll prints it: the broker refuses an extension for a record that has been handed out again since. The broker may cap the time; the answer says how long the claim now stands.

```text
felixctl group extend [OPTIONS] --for-ms <MS> <STREAM> <GROUP> <CLAIM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `CLAIM` | yes | The claim, SHARD:OFFSET:ATTEMPTS |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--for-ms` | `<MS>` | - | How long the claim should stand from now |

Examples:

```bash
felixctl group extend orders billing 0:15:1 --for-ms 60000
```

### `felixctl group dead-letters`

Dead letters are records a group gave up on. The records stay in the log; the group keeps a list of their offsets.

```text
felixctl group dead-letters [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl group dead-letters ls orders billing
felixctl group dead-letters redrive orders billing 0:15
```

#### `felixctl group dead-letters ls`

List the dead letters of GROUP on every shard, or on --shard, as claims that redrive and discard take.

```text
felixctl group dead-letters ls [OPTIONS] <STREAM> <GROUP>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard [default: every shard] |

Examples:

```bash
felixctl group dead-letters ls orders billing
felixctl group dead-letters ls orders billing --shard 0 --json
```

#### `felixctl group dead-letters add`

Give up on each claimed record: finish it and list it as a dead letter, as the group does once a record runs out of attempts.

```text
felixctl group dead-letters add [OPTIONS] <STREAM> <GROUP> <CLAIM>...
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `CLAIM` | yes | Records, as SHARD:OFFSET or the SHARD:OFFSET:ATTEMPTS poll prints |

Examples:

```bash
felixctl group dead-letters add orders billing 0:15
```

#### `felixctl group dead-letters redrive`

Deliver each dead letter again, its attempt count reset. The group's cursor does not move.

```text
felixctl group dead-letters redrive [OPTIONS] <STREAM> <GROUP> <CLAIM>...
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `CLAIM` | yes | Records, as SHARD:OFFSET or the SHARD:OFFSET:ATTEMPTS poll prints |

Examples:

```bash
felixctl group dead-letters redrive orders billing 0:15 1:4
```

#### `felixctl group dead-letters discard`

Drop each dead letter from the group's list. The record stays in the log, but nothing records that it failed any more. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl group dead-letters discard [OPTIONS] <STREAM> <GROUP> <CLAIM>...
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream the group reads |
| `GROUP` | yes | Group name |
| `CLAIM` | yes | Records, as SHARD:OFFSET or the SHARD:OFFSET:ATTEMPTS poll prints |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl group dead-letters discard orders billing 0:15 --yes
```

## `felixctl counter`

Read a counter's sum, or add a signed delta to it. Counters live in a cache, beside its keys.

```text
felixctl counter [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl counter add stats page-views 1
felixctl counter get stats page-views
```

### `felixctl counter get`

Print a counter's sum. Exits with status 5 when it has never been written.

```text
felixctl counter get [OPTIONS] <CACHE> <KEY>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache the counter lives in |
| `KEY` | yes | Counter key |

Examples:

```bash
felixctl counter get stats page-views
felixctl counter get stats page-views --json
```

### `felixctl counter add`

Add DELTA, which may be negative, to a counter and print its sum including the add. A counter that has never been written starts at zero. The add is sent once: a failure is not retried, since the broker may already have applied it.

```text
felixctl counter add [OPTIONS] <CACHE> <KEY> <DELTA>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache the counter lives in |
| `KEY` | yes | Counter key |
| `DELTA` | yes | Signed amount to add |

Examples:

```bash
felixctl counter add stats page-views 1
felixctl counter add stats stock -3
```

## `felixctl tenant`

Create, list, inspect and delete tenants. Needs a control-plane token allowed tenant.manage on the cluster.

```text
felixctl tenant [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl tenant ls
felixctl tenant info t1
felixctl tenant create acme --display-name 'Acme Corp'
felixctl tenant rm acme --yes
```

### `felixctl tenant ls`

List every tenant.

```text
felixctl tenant ls [OPTIONS]
```

Examples:

```bash
felixctl tenant ls
felixctl tenant ls --json
```

### `felixctl tenant info`

Show one tenant. Exits with status 5 when it does not exist.

```text
felixctl tenant info [OPTIONS] <TENANT>
```

| Argument | Required | Description |
| --- | --- | --- |
| `TENANT` | yes | Tenant id |

Examples:

```bash
felixctl tenant info t1
```

### `felixctl tenant create`

Create a tenant. One that already exists is refused.

```text
felixctl tenant create [OPTIONS] <TENANT>
```

| Argument | Required | Description |
| --- | --- | --- |
| `TENANT` | yes | Tenant id |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--display-name` | `<TEXT>` | - | Display name [default: the tenant id] |

Examples:

```bash
felixctl tenant create acme
felixctl tenant create acme --display-name 'Acme Corp'
```

### `felixctl tenant rm`

Delete a tenant with everything in it: namespaces, streams, caches, signing keys and RBAC rules. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl tenant rm [OPTIONS] <TENANT>
```

| Argument | Required | Description |
| --- | --- | --- |
| `TENANT` | yes | Tenant id |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl tenant rm acme
felixctl tenant rm acme --yes
```

## `felixctl namespace`

Create, list, inspect and delete the namespaces of the current tenant.

```text
felixctl namespace [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl namespace ls
felixctl namespace info default --tenant t1
felixctl namespace create payments
felixctl namespace rm payments --yes
```

### `felixctl namespace ls`

List the namespaces of the current tenant.

```text
felixctl namespace ls [OPTIONS]
```

Examples:

```bash
felixctl namespace ls
felixctl namespace ls --tenant acme
```

### `felixctl namespace info`

Show one namespace of the current tenant.

```text
felixctl namespace info [OPTIONS] <NAMESPACE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAMESPACE` | yes | Namespace name |

Examples:

```bash
felixctl namespace info default
```

### `felixctl namespace create`

Create a namespace in the current tenant. One that already exists is refused.

```text
felixctl namespace create [OPTIONS] <NAMESPACE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAMESPACE` | yes | Namespace name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--display-name` | `<TEXT>` | - | Display name [default: the namespace name] |

Examples:

```bash
felixctl namespace create payments
felixctl namespace create payments --display-name Payments
```

### `felixctl namespace rm`

Delete a namespace of the current tenant, its streams and caches included. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl namespace rm [OPTIONS] <NAMESPACE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAMESPACE` | yes | Namespace name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl namespace rm payments
felixctl namespace rm payments --yes
```

## `felixctl stream`

Create, change, list, inspect and delete the streams of the current tenant and namespace.

```text
felixctl stream [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl stream ls
felixctl stream info orders
felixctl stream create orders --shards 4 --replication 3 --consistency quorum
felixctl stream set orders --retention-secs 86400
felixctl stream rm orders --yes
```

### `felixctl stream ls`

List the streams of the current tenant and namespace.

```text
felixctl stream ls [OPTIONS]
```

Examples:

```bash
felixctl stream ls
felixctl stream ls -n payments --json
```

### `felixctl stream info`

Show a stream's shards, replication, retention, consistency and delivery settings.

```text
felixctl stream info [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream name |

Examples:

```bash
felixctl stream info orders
felixctl stream info orders --json
```

### `felixctl stream create`

Create a stream in the current tenant and namespace. Creating one that already exists with the same settings succeeds; with different settings it is refused. Shards, replication, region and routing are fixed once it exists.

```text
felixctl stream create [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shards` | `<N>` | `1` | Shards; fixed at creation |
| `--replication` | `<N>` | `1` | Brokers holding a copy of each shard, the leader included. `quorum` needs at least 3 to mean anything |
| `--kind` | `<KIND>` | `stream` | The kind the control plane records for the stream. One of `stream`, `queue`. |
| `--consistency` | `<LEVEL>` | `leader` | `quorum` acknowledges a publish once a majority of the shard's copies hold it. One of `leader`, `quorum`. |
| `--delivery` | `<GUARANTEE>` | `at-least-once` | The delivery guarantee readers get. One of `at-most-once`, `at-least-once`. |
| `--durable` | `<BOOL>` | `true` | Keep records in an on-disk log. `false` keeps them in memory only. One of `true`, `false`. |
| `--retention-secs` | `<SECS>` | - | Keep records this many seconds [default: the broker's own bound] |
| `--retention-bytes` | `<BYTES>` | - | Keep at most this many bytes per shard [default: the broker's own bound] |
| `--region` | `<REGION>` | - | Region the stream's data must stay in; fixed at creation [default: anywhere] |
| `--routing` | `<ROUTING>` | `modulo` | How routing keys map to shards; fixed at creation. `jump-hash` needs the jump_hash_routing fleet feature finalized. One of `modulo`, `jump-hash`. |

Examples:

```bash
felixctl stream create orders
felixctl stream create orders --shards 4 --replication 3 --consistency quorum
felixctl stream create audit --retention-secs 604800 --region eu-west
```

### `felixctl stream set`

Change a stream's consistency, delivery, durability or retention, and print what changed. A retention bound not given keeps its current value.

```text
felixctl stream set [OPTIONS] <--consistency <LEVEL>|--delivery <GUARANTEE>|--durable <BOOL>|--retention-secs <SECS>|--retention-bytes <BYTES>> <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--consistency` | `<LEVEL>` | - | New consistency level. One of `leader`, `quorum`. |
| `--delivery` | `<GUARANTEE>` | - | New delivery guarantee. One of `at-most-once`, `at-least-once`. |
| `--durable` | `<BOOL>` | - | Keep records in an on-disk log, or not. One of `true`, `false`. |
| `--retention-secs` | `<SECS>` | - | Keep records this many seconds; `default` for the broker's own bound |
| `--retention-bytes` | `<BYTES>` | - | Keep at most this many bytes per shard; `default` for the broker's own bound |

Examples:

```bash
felixctl stream set orders --retention-secs 86400
felixctl stream set orders --retention-bytes default
felixctl stream set orders --consistency quorum
```

### `felixctl stream rm`

Delete a stream and its records. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl stream rm [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream name |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl stream rm orders
felixctl stream rm orders --yes
```

## `felixctl node`

List and inspect the brokers registered with the control plane, with their lifecycle and whether placement may use them, and drain or deregister one.

```text
felixctl node [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl node ls
felixctl node info broker-1 --json
felixctl node drain broker-2
felixctl node deregister broker-2 --yes
```

### `felixctl node ls`

List the brokers registered with the control plane.

```text
felixctl node ls [OPTIONS]
```

Examples:

```bash
felixctl node ls
felixctl node ls --json
```

### `felixctl node info`

Show one broker: its addresses, lifecycle, heartbeat and placement eligibility.

```text
felixctl node info [OPTIONS] <NODE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NODE` | yes | Node id |

Examples:

```bash
felixctl node info broker-1
```

### `felixctl node drain`

Mark a broker draining: placement puts nothing new on it and moves the shards it holds to other brokers, while it keeps serving each one until the shard is handed off. Draining a node already draining succeeds. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl node drain [OPTIONS] <NODE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NODE` | yes | Node id |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl node drain broker-2
felixctl node drain broker-2 --yes
```

### `felixctl node deregister`

Mark a broker as having left the cluster on purpose. Its shards stay put until its lease has run out, then fail over. Drain it first to move them without a failover. Asks first on a terminal; elsewhere it needs --yes.

```text
felixctl node deregister [OPTIONS] <NODE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NODE` | yes | Node id |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl node deregister broker-2
felixctl node deregister broker-2 --yes
```

## `felixctl shard`

List which broker leads each shard of every stream and cache, as the control plane assigned them, and move a shard's leadership or cancel a move.

```text
felixctl shard [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl shard ls
felixctl shard ls --leader broker-2
felixctl shard move orders 2 --to broker-3
felixctl shard move cancel orders 2
```

### `felixctl shard ls`

List shard assignments: for each shard of each stream and cache, its leader, replicas, generation and state.

```text
felixctl shard ls [OPTIONS]
```

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--leader` | `<NODE>` | - | Only shards this node leads |
| `--name` | `<NAME>` | - | Only shards of this stream or cache |

Examples:

```bash
felixctl shard ls
felixctl shard ls --leader broker-2
felixctl shard ls --name orders --json
```

### `felixctl shard move`

Move a shard's leadership to the broker --to names. The destination copies the log first and the leader keeps serving until it has caught up. Prints the step the move is at and the assignment it wrote. --dry-run prints what it would write without starting it.

`shard move cancel` stops a move that has not cut over yet.

```text
felixctl shard move [OPTIONS] --to <NODE> <NAME> <SHARD>
       felixctl shard move <COMMAND>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Stream (or, with --cache, cache) name |
| `SHARD` | yes | Shard number |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--to` | `<NODE>` | - | Broker to move the leadership to |
| `--cache` | - | - | NAME is a cache, not a stream |
| `--dry-run` | - | - | Print what the move would write, without starting it |

Examples:

```bash
felixctl shard move orders 2 --to broker-3
felixctl shard move orders 2 --to broker-3 --dry-run
felixctl shard move sessions 0 --to broker-1 --cache
felixctl shard move cancel orders 2
```

#### `felixctl shard move cancel`

Cancel a shard's move or follower replacement, whoever started it. Before the fence the destination is dropped; after it the old leader serves again. A move that has cut over cannot be cancelled; move the shard back instead. Placement may choose the same move again, so pause it first to keep the shard where it is.

```text
felixctl shard move cancel [OPTIONS] <NAME> <SHARD>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Stream (or, with --cache, cache) name |
| `SHARD` | yes | Shard number |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--cache` | - | - | NAME is a cache, not a stream |

Examples:

```bash
felixctl shard move cancel orders 2
felixctl shard move cancel sessions 0 --cache
```

## `felixctl placement`

Pause or resume the moves placement starts by itself, and give up a stranded shard's log. Needs a control-plane token allowed node.manage on the cluster.

```text
felixctl placement [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl placement pause
felixctl placement resume
felixctl placement abandon orders 2 --yes
```

### `felixctl placement pause`

Stop placement starting moves of its own, on every control-plane instance. Moves already under way finish; cancel one to stop it. New shards are still placed and a failed leader is still replaced, and an operator can still move shards.

```text
felixctl placement pause [OPTIONS]
```

Examples:

```bash
felixctl placement pause
```

### `felixctl placement resume`

Let placement start moves of its own again.

```text
felixctl placement resume [OPTIONS]
```

Examples:

```bash
felixctl placement resume
```

### `felixctl placement abandon`

Give up the log of a durable shard that placement is holding unplaced because the only copies of it are out of reach, and place the shard afresh. Records only the old leader held are lost, acknowledged ones included. The control plane refuses it while the leader serves or a replica can take over without loss. Never asks: it always needs --yes.

```text
felixctl placement abandon [OPTIONS] <NAME> <SHARD>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Stream (or, with --cache, cache) name |
| `SHARD` | yes | Shard number |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--cache` | - | - | NAME is a cache, not a stream |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl placement abandon orders 2 --yes
felixctl placement abandon sessions 0 --cache --yes
```

## `felixctl rbac`

List, add and remove the current tenant's RBAC rules.

A policy lets a subject (usually a role) take an action on an object. A grouping assigns a user, or an IdP group, to a role. Changes reach brokers with the next token the principal is issued.

```text
felixctl rbac [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl rbac policy ls
felixctl rbac policy add role:reader stream:t1/payments/orders stream.subscribe
felixctl rbac grouping add p:alice role:reader
felixctl rbac policy rm role:reader stream:t1/payments/orders stream.subscribe --yes
```

### `felixctl rbac policy`

List, add and remove policies. Each one is a subject, an object and an action.

Objects: cluster:\*, node:{node}, tenant:{tenant}, namespace:{tenant}/{ns}, stream:{tenant}/{ns}/{stream}, cache:{tenant}/{ns}/{cache}, cache:{tenant}/{ns}/{cache}/{key}, cache:{tenant}/{ns}/{cache}/{prefix}\*, and group:{tenant}/{ns}/{stream}/{group}. A `*` may stand for a stream, cache or group, and for the namespace above it when it does.

```text
felixctl rbac policy [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl rbac policy ls --subject role:reader
felixctl rbac policy add role:reader stream:t1/payments/* stream.subscribe
felixctl rbac policy add role:session-1 cache:t1/default/sessions/user-1 cache.read
felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.write
felixctl rbac policy rm role:reader stream:t1/payments/* stream.subscribe
```

#### `felixctl rbac policy ls`

List the tenant's policies that the control-plane token may see.

```text
felixctl rbac policy ls [OPTIONS]
```

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--subject` | `<SUBJECT>` | - | Only policies for this subject |

Examples:

```bash
felixctl rbac policy ls
felixctl rbac policy ls --subject role:reader --json
```

#### `felixctl rbac policy add`

Let SUBJECT take ACTION on OBJECT. The object is checked here before it is sent; the control plane checks it again, along with the action and whether the token may grant it. A cache key or key prefix object takes only cache.read or cache.write.

```text
felixctl rbac policy add [OPTIONS] <SUBJECT> <OBJECT> <ACTION>
```

| Argument | Required | Description |
| --- | --- | --- |
| `SUBJECT` | yes | Who the policy is for, usually a role such as role:reader |
| `OBJECT` | yes | What it covers, such as stream:t1/payments/orders |
| `ACTION` | yes | What it allows, such as stream.publish |

Examples:

```bash
felixctl rbac policy add role:writer stream:t1/payments/orders stream.publish
felixctl rbac policy add role:users cache:t1/default/sessions/user:* cache.read
```

#### `felixctl rbac policy rm`

Remove one policy, named exactly as `policy ls` prints it. Asks first on a terminal; elsewhere it needs --yes. Exits with status 5 when there is no such policy.

```text
felixctl rbac policy rm [OPTIONS] <SUBJECT> <OBJECT> <ACTION>
```

| Argument | Required | Description |
| --- | --- | --- |
| `SUBJECT` | yes | Who the policy is for, usually a role such as role:reader |
| `OBJECT` | yes | What it covers, such as stream:t1/payments/orders |
| `ACTION` | yes | What it allows, such as stream.publish |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl rbac policy rm role:writer stream:t1/payments/orders stream.publish
felixctl rbac policy rm role:writer stream:t1/payments/orders stream.publish --yes
```

### `felixctl rbac grouping`

List, add and remove groupings. Each one assigns a user, or a group named by an IdP claim, to a role.

```text
felixctl rbac grouping [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl rbac grouping ls --role role:reader
felixctl rbac grouping add p:alice role:reader
felixctl rbac grouping rm p:alice role:reader
```

#### `felixctl rbac grouping ls`

List the tenant's groupings for roles the control-plane token may see.

```text
felixctl rbac grouping ls [OPTIONS]
```

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--user` | `<USER>` | - | Only groupings for this user or group |
| `--role` | `<ROLE>` | - | Only groupings to this role |

Examples:

```bash
felixctl rbac grouping ls
felixctl rbac grouping ls --user p:alice --json
```

#### `felixctl rbac grouping add`

Assign ROLE to USER. The token must be able to grant every policy the role carries.

```text
felixctl rbac grouping add [OPTIONS] <USER> <ROLE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `USER` | yes | The user or group, such as p:alice |
| `ROLE` | yes | The role, such as role:reader |

Examples:

```bash
felixctl rbac grouping add p:alice role:reader
```

#### `felixctl rbac grouping rm`

Take ROLE away from USER. Asks first on a terminal; elsewhere it needs --yes. Exits with status 5 when there is no such assignment.

```text
felixctl rbac grouping rm [OPTIONS] <USER> <ROLE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `USER` | yes | The user or group, such as p:alice |
| `ROLE` | yes | The role, such as role:reader |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `-y, --yes` | - | - | Do not ask for confirmation. Needed when stdin is not a terminal |

Examples:

```bash
felixctl rbac grouping rm p:alice role:reader --yes
```

## `felixctl inspect`

Look at what the brokers themselves hold, read-only. Each answer is one broker's own view; felixctl asks every broker that has a part in what is shown.

Needs a broker token allowed node.view on cluster:\*. A broker too old to answer is reported, not guessed at.

```text
felixctl inspect [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl inspect shard orders --shard 3
felixctl inspect shard acme/default/orders
felixctl inspect shard sessions --cache --json
felixctl inspect subs orders --dropping
felixctl inspect subs --node broker-a --limit 500
```

### `felixctl inspect shard`

Show one shard as its brokers see it: the generation and leader, whether the leader serves and why not, the fence a promoted leader waits on, its lease, the tail and commit mark, and each replica's position and state.

The assignment comes from the control plane when a control-plane URL is configured, else from the broker connected to. Every broker in it is then asked directly; one that cannot be reached is listed under unreachable rather than described. Without --shard, every shard of the stream is shown.

```text
felixctl inspect shard [OPTIONS] <NAME>
```

| Argument | Required | Description |
| --- | --- | --- |
| `NAME` | yes | Stream (or, with --cache, cache) name, or TENANT/NAMESPACE/NAME for another tenant's |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard |
| `--cache` | - | - | NAME is a cache, not a stream |

Examples:

```bash
felixctl inspect shard orders --shard 3
felixctl inspect shard acme/default/orders
felixctl inspect shard sessions --cache --shard 0 --json
```

### `felixctl inspect subs`

List the subscriptions each broker serves: the stream and shard, the connection and principal it delivers to, the queue's overflow policy, depth and capacity, the records it has dropped, and its position against the shard's tail.

Every broker the cluster advertises is asked, or only --node. A page holds --limit subscriptions per broker (at most 1000); when a broker has more, felixctl prints the --cursor that continues from there. Depth and capacity count batches; dropped counts records.

```text
felixctl inspect subs [OPTIONS] [STREAM]
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | no | Only this stream: NAME, or TENANT/NAMESPACE/NAME for another tenant's |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--shard` | `<N>` | - | Only this shard of the stream |
| `--node` | `<NODE>` | - | Only this broker, by node id |
| `--principal` | `<P>` | - | Only subscriptions made under this principal |
| `--dropping` | - | - | Only subscriptions that have dropped records |
| `--limit` | `<N>` | `100` | At most this many per broker (1-1000) |
| `--cursor` | `<C>` | - | Continue after the page a previous run printed |

Examples:

```bash
felixctl inspect subs
felixctl inspect subs orders --shard 0
felixctl inspect subs --dropping --json
felixctl inspect subs --principal p:billing
felixctl inspect subs --node broker-a --cursor eyJ0ZW5hbnRfaWQiOi4uLn0
```

## `felixctl bench`

Run one of felix-loadgen's scenarios against the current context and print a short summary: the rate, and p50 and p99 latency.

The defaults are small enough to finish in seconds. The stream or cache must already exist. --json prints the summary together with the full felix-loadgen result.

```text
felixctl bench [OPTIONS] <COMMAND>
```

Examples:

```bash
felixctl bench latency orders
felixctl bench fanout orders --fanout 20
felixctl bench ingest orders --concurrency 8 --duration-secs 30
felixctl bench cache sessions --json
```

### `felixctl bench ingest`

Measure aggregate publish throughput: --concurrency publishers, each on its own connection, send batches with --in-flight acknowledged batches outstanding. No subscribers. Reports msg/s, MB/s and batch acknowledgement latency.

```text
felixctl bench ingest [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream to publish to |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--total` | `<N>` | `100000` | Records to publish in total |
| `--payload-bytes` | `<BYTES>` | `256` | Payload size in bytes |
| `--concurrency` | `<N>` | `4` | Publishers, each on its own connection |
| `--batch` | `<N>` | `64` | Records per batch |
| `--in-flight` | `<N>` | `4` | Acknowledged batches each publisher keeps outstanding; 0 sends without acknowledgements |
| `--keys` | `<N>` | `0` | Spread batches over this many routing keys; 0 publishes unkeyed, which puts everything on shard 0 |
| `--duration-secs` | `<SECS>` | - | Publish for this many seconds instead of --total records |

Examples:

```bash
felixctl bench ingest orders
felixctl bench ingest orders --concurrency 8 --batch 256 --duration-secs 30
felixctl bench ingest orders --keys 64 --in-flight 0
```

### `felixctl bench latency`

Measure latency with one publisher and one subscriber: the publish acknowledgement round trip and the publish-to-delivery time.

```text
felixctl bench latency [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream to publish to and subscribe to |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--total` | `<N>` | `5000` | Operations measured |
| `--warmup` | `<N>` | `500` | Operations run first and not measured |
| `--payload-bytes` | `<BYTES>` | `256` | Payload size in bytes |

Examples:

```bash
felixctl bench latency orders
felixctl bench latency orders --total 20000 --payload-bytes 1024
```

### `felixctl bench fanout`

Measure fanout: one publisher and --fanout subscribers. Reports the publish rate, the delivered rate across all subscribers, and delivery latency.

```text
felixctl bench fanout [OPTIONS] <STREAM>
```

| Argument | Required | Description |
| --- | --- | --- |
| `STREAM` | yes | Stream to publish to and subscribe to |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--fanout` | `<N>` | `10` | Subscribers |
| `--total` | `<N>` | `5000` | Operations measured |
| `--warmup` | `<N>` | `500` | Operations run first and not measured |
| `--payload-bytes` | `<BYTES>` | `256` | Payload size in bytes |

Examples:

```bash
felixctl bench fanout orders
felixctl bench fanout orders --fanout 50
```

### `felixctl bench cache`

Measure cache round trips: puts, then gets of the same keys, from --concurrency workers spread across the brokers.

```text
felixctl bench cache [OPTIONS] <CACHE>
```

| Argument | Required | Description |
| --- | --- | --- |
| `CACHE` | yes | Cache to write and read |

| Option | Value | Default | Description |
| --- | --- | --- | --- |
| `--concurrency` | `<N>` | `8` | Concurrent workers |
| `--total` | `<N>` | `5000` | Operations measured |
| `--warmup` | `<N>` | `500` | Operations run first and not measured |
| `--payload-bytes` | `<BYTES>` | `256` | Payload size in bytes |

Examples:

```bash
felixctl bench cache sessions
felixctl bench cache sessions --concurrency 32 --payload-bytes 64
```

## `felixctl completions`

Print a completion script for SHELL to stdout. Source it from your shell's startup file.

```text
felixctl completions [OPTIONS] <SHELL>
```

| Argument | Required | Description |
| --- | --- | --- |
| `SHELL` | yes | Shell to generate the script for. One of `bash`, `elvish`, `fish`, `powershell`, `zsh`. |

Examples:

```bash
felixctl completions bash > ~/.local/share/bash-completion/completions/felixctl
felixctl completions zsh > "${fpath[1]}/_felixctl"
felixctl completions fish > ~/.config/fish/completions/felixctl.fish
```

