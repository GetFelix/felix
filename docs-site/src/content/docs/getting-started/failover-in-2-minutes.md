---
title: "See failover in 2 minutes"
description: "Start three brokers, publish under quorum, kill the leader, and watch the stream stay readable and writable."
---

This runs a real three-broker cluster on your machine and kills the broker
that is leading a stream. You need Git and Rust (see
[Installation](/getting-started/installation/)); the first build takes a few
minutes, and after that the demo itself takes under a minute.

```bash
git clone https://github.com/GetFelix/felix.git
cd felix
cargo build --release -p felix-broker-service --bin felix-broker
cargo run --release -p felix-cluster -- failover
```

The harness runs the prebuilt `felix-broker`, so build it first and again
after you change broker code. Pass `--pace 0.5` to slow the narration down.

## What it does

1. Starts a control plane and three brokers, with a shard replicated three ways.
2. Publishes records under `Quorum`, so each acknowledgement means a majority
   of the replicas has the record.
3. Kills the broker that acknowledged them.
4. Reads the whole stream back from the broker that took over.
5. Publishes through the failover, from a client given exactly one broker
   address. It asks that broker who else is in the cluster and reconnects to
   one of them on its own.

It fails loudly instead of narrating past a problem: a record acknowledged
before the kill that is not readable afterwards ends the demo with an error.

Two things in the output look like defects and are not. There are several
`harness-probe` records, one per broker, which the harness publishes at
startup to prove the cluster can serve. And sometimes one record appears
twice: the publisher resends after a failure it cannot prove was not applied,
so delivery here is at least once.

## Where next

- [Cross-broker publishing](/demos/cross-broker-cluster/): the same cluster, watched through three panes.
- [Cluster consistency](/demos/cluster-consistency/): what `Quorum` buys and what `Leader` costs under a fault (`cargo run --release -p felix-cluster -- consistency`).
- [Failover without the control plane](/deployment/failover-without-control-plane/): what happens when the control plane is gone too.
- [Quickstart](/getting-started/quickstart/): the other `felix-cluster` commands, and the Rust client.
