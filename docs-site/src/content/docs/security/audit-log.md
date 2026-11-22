---
title: "Audit Log"
description: "The control plane's record of who changed what: what it records, where it is kept, how to read and ship it, and what it can and cannot prove."
---

The control plane keeps an audit log: a numbered record of every change made
through its API and every request it refused. Each record says who asked,
which credential they used, where the request came from, what they asked for,
and what the control plane answered.

Read it with `felixctl`:

```bash
felixctl audit ls
```

```text
SEQ  TIME (UTC)           MEMBER  METHOD  PATH                             STATUS  SUBJECT  CREDENTIAL   REFUSAL
41   2026-11-22 17:52:27  1       POST    /v1/tenants/acme/rbac/policies   204     alice    felix_token  -
42   2026-11-22 17:53:02  2       POST    /v1/tenants/{tenant_id}/streams  401     -        none         invalid_token
```

Reading needs a control-plane token allowed `audit.view` on the cluster
(`audit.view:cluster:*`). Broker credentials (`node.view`, `node.manage`) do
not include it.

## What is recorded

Every request that changes something, on the public API and on the bootstrap
listener: tenants, namespaces, streams and caches, RBAC policies and
groupings, identity provider issuers, signing keys, refresh-token revocation,
token exchange, refresh and delegation, node registration and lifecycle
changes, fleet feature finalizes, shard moves and placement, Raft re-form
plans, bootstrap initialization, state imports and Raft floor waivers.

Every request refused for its credential (a 401 or 403), on any route, reads
included.

Not recorded: successful reads, and the reports brokers send every few
seconds (heartbeats, replica status, Raft copy ids and floors, seal reports,
re-form reports). Those would push real changes out of the log within
minutes. A refused report is still recorded.

A record holds:

| Field | Meaning |
| --- | --- |
| `seq` | The record's number. Numbers have no gaps. |
| `at_ms` | When the serving member handled the request, by its clock, in milliseconds since the epoch. |
| `member` | The control-plane member that served the request (its Raft node id, or `local` on the memory backend). |
| `listener` | `api` or `bootstrap`. |
| `peer` | The socket address the request came from. Forwarding headers such as `X-Forwarded-For` are ignored, so behind a load balancer this is the balancer. |
| `method`, `route`, `path` | What was asked. `route` is the template, such as `/v1/tenants/{tenant_id}/streams`. |
| `tenant` | The tenant of the verified credential. It is never read from the path: a change made with the bootstrap token or an operator token has no `tenant`, and `path` shows which tenant it named. |
| `subject` | The subject of the verified credential. For a delegated token, `actor` is the gateway that acted for it. |
| `cert_sha256` | The SHA-256 of the client's TLS certificate, when the API asks for one. |
| `credential` | What verified: `felix_token`, `idp_token`, `refresh_token`, `bootstrap_token`, `operator_token`, or `none`. |
| `status` | The HTTP status answered. `0` means the client went away before the answer, and the change may have been made anyway. |
| `refusal` | Why a credential was refused, with the same reasons as `felix_controlplane_auth_rejected_total`. |
| `suppressed` | Rate-limited requests this member skipped since its last recorded one (see below). |

Tokens, request bodies, query strings, keys and unverified claims are never
recorded. A refused request, and any request without a verified credential,
records only its route template as its path, since the caller chose the path
and could use it to fill the log. A request that names a subject but fails to
verify records no subject: an unverified name proves nothing. Every text
field is cut to 256 bytes, so a record stays a few KiB.

## Where it is kept

On the Raft metadata backend, records are committed through the control
plane's Raft log, like any other metadata change, and every member keeps the
newest 10,000. That number is a constant, so every member trims at the same
record. A committed record is held by a majority of members, so changing or
removing one means changing a majority of them.

Every member also writes each committed record to its own log, at target
`felix_audit`. Ship those logs to your log store and you have a copy outside
the control plane, one per member.

The memory backend keeps the newest 10,000 in memory, lost at restart.

An import (`/internal/bootstrap/import-state`) replaces the store but keeps
the audit log, the import's own record included.

## Reading and shipping

`GET /v1/audit?after=<seq>&limit=<n>` answers up to `n` records after `seq`
(100 by default, at most 1000), oldest first, with the oldest number still
kept and the newest number given out:

```json
{"records": [{"seq": 41, "at_ms": 1795369947596, "member": "1", "...": "..."}],
 "oldest_seq": 1, "last_seq": 42}
```

A reader that remembers the last `seq` it saw and asks for what comes after
sees every record, as long as it keeps up. If `after + 1` is below
`oldest_seq`, records were trimmed before it read them, and `felixctl` warns.

To ship records continuously:

```bash
felixctl audit ls --follow --json >> audit.jsonl
```

`--json` prints one record per line. `--follow` keeps polling every two
seconds. Start again from the last `seq` in the file with `--after`.

## When a record is not kept

A record is written after the request is answered and never holds it up. If
the record cannot be committed, the change still stands. Each record that is
not committed is logged at error level, at target `felix_audit`, and counted
in `felix_audit_records_dropped_total` by `reason`:

| Reason | What happened |
| --- | --- |
| `rate_limited` | A refusal, or a request with no verified credential whatever its status (a malformed body's 400, a 405), over this member's limit of 20 a second (bursts of 100). These need no credential, so without a limit a flood of bad requests would be a flood of Raft writes. The next one recorded carries the count in `suppressed`. |
| `busy` | More than 4,096 records were waiting to be committed on this member. |
| `store` | The Raft write failed, for example with no quorum. |
| `level_pending` | Some member runs a release older than metadata level 49, which cannot apply records. The local log line is the only copy until every member is upgraded. |
| `closed` | The member was shutting down. A member drains its queued records before its Raft node stops. |

Alert on any increase of `felix_audit_records_dropped_total` other than
`rate_limited`.

The control plane fails open here on purpose. A change and its record need
the same quorum, so refusing the change when the record fails would add
nothing but a second way for every write to fail.

## What it proves

The audit log is evidence against a mistaken or careless operator, and
against an outsider using a stolen credential. It is weaker against a
compromised control-plane member, and the limits matter:

- A compromised member can leave out records for requests it serves, and can
  add records that never happened. It can also make changes without asking
  anyone, so its own records prove nothing about it.
- It cannot change or remove a record that is already committed: that needs
  a majority of members.
- Under Raft member certificates
  ([Raft member certificates](/security/hardening/#raft-member-certificates)),
  the leader refuses records a member sends under another member's name, so
  `member` is the member that sent the record. Without them, `member` is what
  the sending member says.
- `at_ms` is the serving member's clock.
- Retention is a count. A flood of records, from a compromised member or from
  a busy tenant, pushes older ones out, which is why records should be shipped
  as they are written.
- The bootstrap token is shared by every member and names no person, and
  `operator_token` says that one of the configured operator tokens was used,
  not which.

Brokers keep no records here. A broker's own administrative actions, such as
a trim, show in that broker's log only.

## Upgrading

Records need metadata level 49. Until every member runs a release at that
level, nothing is committed and each member keeps only its local
`felix_audit` log lines. Once records are committed, rolling a member back
below level 49 is not supported: the older member refuses the group's
snapshot instead of dropping the records.
