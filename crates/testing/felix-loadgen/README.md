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

### Ingest against a cluster

By default each `ingest` publisher holds a plain connection to one broker, so
on a cluster most batches reach a broker that does not lead the shard and are
forwarded. `--cluster` makes the generator share one `ClusterClient`, which
sends each shard's records straight to its leader (`publish_keyed` at
`--batch 1`, `publish_batch_keyed` above it), fire-and-forget or acked:

```text
felix-loadgen ... --scenario ingest --stream perf --keys 48 --cluster \
    --concurrency 8 --batch 100 --in-flight 4 --duration-secs 60
```

A publisher whose connection breaks reconnects (to the next broker, or via the
cluster client's own reconnect) and keeps publishing, so a broker dying mid-run
does not fail it. A coded refusal from a live broker is counted, not treated as
a broken connection. The JSON counts `reconnects` and `failed_sends` (records in
sends that errored, which may or may not have landed), and `per_shard_acked`
gives acked records by shard so a loss check can compare them with what was
delivered. A run only exits non-zero when nothing succeeds for 30 s in total.

`forwarded_sends` and `forwarded_share` are exact: the client counts an acked
send as forwarded when its ack names another broker as the shard's owner
(`felix_client::publishes_forwarded`), and the share is out of `acked_sends`.
They are `null` when fire-and-forget (`--in-flight 0`), where no ack comes back;
read the brokers' forward counters for that. `--keys` should be set (and spread
across the shards), since unkeyed records all go to shard 0.

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
