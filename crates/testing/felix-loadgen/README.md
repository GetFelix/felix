# felix-loadgen

The load generator for Felix's real-network performance suite. It dials the
broker addresses it is given and measures what a client of that cluster sees:
publish acknowledgement and delivery latency, fanout, aggregate ingest, cache
and counter round trips, cache watches, consumer groups and retained joins.

Scenarios: `pubsub`, `cache`, `counter`, `watch`, `queue`, `retained`,
`ingest` and `subscribe`. `subscribe` only subscribes, so it pairs with
`ingest` on another generator for read-heavy runs:

```text
# generator A: 200 subscribers over 8 connections, counting for 60 s
felix-loadgen ... --scenario subscribe --stream perf --fanout 200 \
    --concurrency 8 --duration-secs 60 --start-at 1760000000 --stamp-send-time

# generator B: the publishers, starting at the same moment
felix-loadgen ... --scenario ingest --stream perf --concurrency 4 \
    --duration-secs 60 --start-at 1760000000 --stamp-send-time
```

A `subscribe` run reports what its subscribers were delivered
(`delivered_throughput_msg_s`, `received`), offset gaps, which are records the
broker dropped for a subscriber that fell behind, and, with
`--stamp-send-time` on both sides, delivery latency. That latency subtracts one
machine's clock from another's, so it is only as good as their sync. The run
publishes nothing and reports no publish `throughput`.

```text
felix-loadgen --brokers 10.0.0.4:5000,10.0.0.5:5000 \
    --tenant t1 --token-file /run/felix/token \
    --scenario pubsub --fanout 10 --payload-bytes 256 --total 20000
```

Run `felix-loadgen --help` for every flag. Each run prints human-readable
result lines and one `LOADGEN_JSON {...}` line, which is what the perf scripts
parse.

The scenarios are also a library (`felix_loadgen::run`), which is how
`felixctl bench` runs them against a context.

The binary accepts any broker certificate, because brokers that generate their
own certificate publish nothing to verify against. Run it only against a
cluster and network you own. `felixctl bench` verifies certificates as its
context says.

Licensed AGPL-3.0-only; see `LICENSING.md` in the repository.
