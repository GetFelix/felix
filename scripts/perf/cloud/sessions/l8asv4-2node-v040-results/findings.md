# Two-broker session, v0.4.0, L8as_v4 + local NVMe

## Generator sweep (stream perf-durable, OnCommit, 16 publishers per generator)

| generators | publishers | aggregate MB/s | msg/s |
|---|---|---|---|
| 1 | 16 | 1051.0 | 256,602 |
| 2 | 32 | **2018.9** | 492,903 |
| 3 | 48 | 2020.5 | 493,278 |

1 -> 2 generators is 1.92x. 2 -> 3 adds 0.08%. The wall is at ~2020 MB/s.

**The single-generator numbers in every prior session were instrument-limited.**
One D4 generator tops out near ~1050 MB/s, which is the same order as the
per-log ceiling, so the two could not be told apart with one generator. That
includes the ~977 MB/s in nvme-findings §3 and the ~1000 MB/s single-broker
ramp in the sibling session.

## Where the traffic actually went

Shard ownership was balanced (24/25, and 6/6 on the stream under test), but the
clients all entered through broker-0:

| | broker-0 | broker-1 |
|---|---|---|
| forwards (ok) | 55,822 | — |
| append records | *(never appended)* | 14,279,468 |
| append bytes | — | 58.9 GB |

So the 2020 MB/s wall is one broker's log absorbing writes, with the other
broker spending its cores relaying. Filed as #536.

## The ceiling, explained

| | broker-0 (1 shard) | broker-1 (12 shards) |
|---|---|---|
| group-commit fan-in | **1.004** | **1.007** |
| mean fsync | 282 µs | 311 µs |

```
1 flush / ~300 µs                = ~3,333 appends/s per log
1 append = 64 records x 4096 B   = 256 KB
                                 = ~853 MB/s
```

which is the observed wall. Shard count makes no difference -- a single-shard
stream with 48 publishers still fans in at 1.004. Filed as #535.

## Placement

49/0 on first placement, because the streams were created while only broker-0
had re-registered after a control-plane restart. Re-creating the streams with
both brokers live gave 24/25. #388 works; the absence of rebalancing (#130) is
what turns a seconds-long race into a permanent imbalance.

## Caveat

The per-broker CPU figures printed by `run-nvme-multi.sh` are not usable -- it
samples with `top -bn1`, whose first iteration has nothing to difference
against. It reported the *only* writing broker as 100% idle. See #537.
