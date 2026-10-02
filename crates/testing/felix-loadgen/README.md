# felix-loadgen

The load generator for Felix's real-network performance suite. It dials the
broker addresses it is given and measures what a client of that cluster sees:
publish acknowledgement and delivery latency, fanout, aggregate ingest, cache
and counter round trips, cache watches, consumer groups and retained joins.

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
