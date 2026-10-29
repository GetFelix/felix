---
title: "felixd"
description: "A node agent that runs deterministic Wasm workloads across a cluster with Felix as its only backend: a workload survives the loss of its node, resumes from an offset and can be replayed to any point. What runs today and what does not."
---

[felixd](https://github.com/GetFelix/felixd) is a node agent. It runs on every
machine in a cluster and runs WebAssembly programs, called
[workloads](/built-on-felix/felixd/terminology/#workload), whose only inputs and
outputs are Felix streams. Felix is the only backend: there is no etcd, no
workflow database and no job manager beside it. Kill a node and each of its
workloads continues on another node from the last record it handled, with no
gaps and no duplicates in its output. Any workload can be rebuilt as it was at a
past input offset or time, and inspected.

This section documents felixd as it is on its main branch today. Where the
design describes something that is not built, the page says so and links the
felixd issue.

| Page | What it covers |
|---|---|
| [How it works](/built-on-felix/felixd/how-it-works/) | The life of a workload: leases, epochs, fenced output, snapshots, failover, resume, replay and checking |
| [Submitting work](/built-on-felix/felixd/submitting-work/) | Writing a workload, `felixd run` step by step, feeding it input, reading its output, and changing, stopping or fixing it |
| [Example workloads](/built-on-felix/felixd/examples/) | Six complete workloads with their specs, code, and the commands to submit them and read their outputs |
| [The workload contract](/built-on-felix/felixd/contract/) | What a workload exports, what it receives, what it may return and what happens when it breaks the rules |
| [Record formats](/built-on-felix/felixd/records/) | The output and snapshot records, the cache values, and which versions are still read |
| [Running felixd](/built-on-felix/felixd/running/) | The subcommands and flags, what felixd needs from Felix, and a quickstart |
| [Terminology](/built-on-felix/felixd/terminology/) | Every term used in these pages, including fuel, epoch, fence and fold |

## What runs today

- **Agents.** `felixd agent` watches the `workloads` cache, claims the lease of
  each workload nobody holds and runs it while the lease holds. Several agents
  spread the workloads between them. When a node stops renewing, another node
  claims its workloads within one lease TTL (10 seconds by default).
- **Exactly-once output.** A new owner writes a claim record to the workload's
  output stream before it does anything else. A node that was frozen past its
  lease and wakes up finds its writes refused, even if the new owner has not
  written an output yet. Lost answers, partial batches and crashes between an
  append and its answer do not duplicate or drop an output.
- **Snapshots and resume.** The workload's state is saved in chunks, only the
  chunks that changed are written again, and a new owner restores the latest
  snapshot and runs the inputs after it.
- **Replay.** `felixd replay` rebuilds a workload as of an input offset or a
  time. With `--diff` it compares a replay with the recorded outputs, and with
  `--module` it runs a different module over the recorded inputs, which checks
  an upgrade before it goes live.
- **Checking.** `felixd check` replays each workload over its input and
  reports gaps, duplicates, records out of order, epoch regressions and outputs
  that differ.
- **Workload contract 0.2.** Named and keyed outputs, rejection of a single
  input without failing the workload, chunked snapshots, and separate fuel for
  snapshots and restores.
- **A formal model and tests that kill nodes.** A TLA+ model of the lease,
  fence, snapshot and failure paths is checked in CI. A three-node failover test
  that keeps killing the node holding the most leases runs on every pull
  request and ends with `felixd check`.

## What is not built yet

| Not built | Where it stands |
|---|---|
| `felixd ps` and `felixd drain` | [#17](https://github.com/GetFelix/felixd/issues/17). The binary has `agent`, `run`, `check` and `replay`. An agent does release its leases on a clean shutdown (Ctrl-C), which is the only drain there is |
| Metrics | [#18](https://github.com/GetFelix/felixd/issues/18) |
| Trimming old snapshots and inputs | [#19](https://github.com/GetFelix/felixd/issues/19), [#63](https://github.com/GetFelix/felixd/issues/63). Everything is kept |
| A tested three-stage pipeline | [#16](https://github.com/GetFelix/felixd/issues/16). A workload can already read another workload's output stream, but no failover test covers a chain yet |
| Several inputs per workload | [#58](https://github.com/GetFelix/felixd/issues/58) |
| Effects: reaching the outside world | [#71](https://github.com/GetFelix/felixd/issues/71). The contract reserves the shape. A workload that returns an effect request fails today |
| Module upgrades at a recorded offset | [#64](https://github.com/GetFelix/felixd/issues/64) and [#72](https://github.com/GetFelix/felixd/issues/72). Today a new module needs a new workload id |
| Typed system records and error classes | [#73](https://github.com/GetFelix/felixd/issues/73). Specs, leases and failure reasons are JSON and text |
| A recorded way out of a poison input, and retrying a host-dependent trap elsewhere | [#98](https://github.com/GetFelix/felixd/issues/98) |
| Counting restarts across the cluster and recording `degraded` | [#99](https://github.com/GetFelix/felixd/issues/99) |
| A determinism ABI recorded in the spec | [#62](https://github.com/GetFelix/felixd/issues/62). Every node has to run the same felixd build, and nothing checks that |
| Placement beyond "first claim wins" with a per-node cap | [#60](https://github.com/GetFelix/felixd/issues/60), [#100](https://github.com/GetFelix/felixd/issues/100) |
| Per-lease credentials | [#74](https://github.com/GetFelix/felixd/issues/74). Any node may write any workload's output, and fencing keeps that correct |
| Release binaries, a compose file for a three-node cluster and operator docs | [#22](https://github.com/GetFelix/felixd/issues/22), [#23](https://github.com/GetFelix/felixd/issues/23), [#24](https://github.com/GetFelix/felixd/issues/24). Build from source |
| Native programs | [#75](https://github.com/GetFelix/felixd/issues/75) |

## How it uses Felix

felixd keeps nothing on a node's disk that matters. Everything is a Felix cache
or stream in one tenant and namespace. A node can lose its disk and rejoin.

| Need | Felix feature |
|---|---|
| What should run | A [cache](/features/cache/) of workload specs, watched with retained values |
| Which node runs a workload | A cache entry with a TTL, taken and renewed with [`cache_put_if`](/features/cache/#9-conditional-writes) |
| Which nodes are alive | A cache with a TTL, one key per node |
| A workload's input | A durable stream, read with `subscribe_from` an offset |
| Output a stale owner cannot write | [`publish_if`](/features/atomic-commits/#a-commit-at-an-expected-offset) against the tail of the workload's single-shard output stream |
| Snapshots | A durable single-shard stream per workload |
| Surviving a broker's loss | `Quorum` streams and caches |
| Replay to a time | `offset_for_time` and range reads |
| What each node may write | [RBAC](/features/security/) grants on the node's token |

[How it works](/built-on-felix/felixd/how-it-works/) follows a workload through
these.

## Why WebAssembly first

felixd's guarantees rest on one property: a workload given the same input
records produces the same outputs and the same state, every time, on any node.
Failover is a second node running the same inputs from the last offset handled.
Exactly-once output holds because a stale owner that writes after a takeover
writes exactly what the new owner would have, so the new owner skips past it.
Replay rebuilds a past state by running the recorded inputs again. A program
that could behave differently the second time breaks all three.

WebAssembly is how felixd enforces that rather than asks for it. A workload may
import nothing, so it has no clock, randomness, files or network, and anything
it needs from outside, the current time included, arrives as an input record.
Wasm floating point is deterministic apart from NaN bit patterns, which the
runtime canonicalizes. Each call runs under a fuel limit and a memory limit, and
the same module runs on any CPU in the cluster.

Wasm is where felixd starts, not the only kind of program the design runs.
Programs that need the outside world come later, with every response recorded
as an input so they replay exactly. Existing native binaries come after that,
with checkpoint and restart instead of replay. Neither exists yet.

## Where it is going

felixd is the first step toward a distributed operating system whose state is a
log. Every change to the system would be a durable, ordered record: what should
run, where it runs, what each program received and what it produced. Everything
else would be derived from that log and could be rebuilt from it. That is what
would make failover, migration, time travel and an audit trail one mechanism
rather than four features.

The design lists the steps: deterministic workloads (what exists now), then
placement decided through records on the log, then services that answer
requests, then controlled access to the outside world with every response
recorded, then native processes. Each later step is a separate design. Felix is
the system bus and journal, not the kernel: a round trip through a broker costs
far more than local IPC, so Felix carries coordination and program input and
output, never memory traffic. The design says plainly that this is for
services, data processing, fleets and simulations, not for desktops or tight
loops.

The operating system has a proposed name, NineLives, under
[discussion](https://github.com/GetFelix/felixd/discussions/42). It is not
decided, and felixd would stay the name of the agent on each node.

## What it is for

felixd is aimed at work that keeps state, reacts to a stream of events, and must
not lose or repeat results when a machine fails.

- Stateful stream processing without a separate cluster: running totals,
  sessions, fraud scores, deduplication.
- State machines where history matters, such as a ledger or inventory, where you
  need to say what the system believed at a given time and why.
- Fleets of unreliable machines, where a workload moves to a machine that is up
  and continues without an operator.
- Authoritative game and simulation servers that can be replayed afterwards.
- Reproducing a production bug by replaying the workload to the offset before
  the bad output.

It is not a fit for low-latency request and reply, programs that need the
network or files directly, or heavy compute where replaying from a snapshot
takes too long.

## Follow along

The plan is open for change. Start with
[what to build first](https://github.com/GetFelix/felixd/discussions/31) and the
[open design questions](https://github.com/GetFelix/felixd/discussions/33), or
browse all [discussions](https://github.com/GetFelix/felixd/discussions) and
[issues](https://github.com/GetFelix/felixd/issues). The
[README](https://github.com/GetFelix/felixd#readme) has the goal, prior work and
use cases, and
[docs/design.md](https://github.com/GetFelix/felixd/blob/main/docs/design.md) the
long-term picture.
