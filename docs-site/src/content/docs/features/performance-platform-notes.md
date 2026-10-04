---
title: "Performance & Platform Notes"
---

Felix targets Linux-first deployments. Most published measurements are macOS,
because that is where the throughput ceiling that drove the tuning work lived;
Linux is faster and has a different set of constraints, noted throughout.

## Platform sensitivity

- Felix is optimized for Linux; microsecond-scale localhost QUIC measurements are OS-sensitive.
- At `batch=1`, tail latency is often dominated by scheduler wakeups and UDP/QUIC receive-path behavior, not broker business logic.
- On macOS and other desktop OSes, expect higher jitter in `p99`/`p999` for the same benchmark profile.

## Transport scheduling defaults

The transport defaults that matter for performance are on out of the box. You
don't need any environment tuning to get sustained throughput:

- QUIC driver tasks run on dedicated single-threaded I/O runtimes
  (`FELIX_IO_RUNTIME_THREADS`), with transport-facing pump tasks colocated on
  the same threads, **on macOS only**. It defaults to `0` (off) everywhere
  else. On Linux the pool measures 47–88% worse p50 latency and up to 25%
  less throughput, because Linux's scheduler does not have the re-poll
  pathology the pool exists to work around.
- The ACK-frequency extension is negotiated between quinn peers (2 ms max
  ACK delay, ACK every 20th packet; `FELIX_ACK_ELICITING_THRESHOLD` /
  `FELIX_ACK_FREQ_DISABLE`).
- Path-MTU discovery probes up to 16 KiB on macOS and 4,096 elsewhere, and UDP
  socket buffers request 8 MiB. Loopback connections additionally *guarantee*
  their MTU, at 16,336 on macOS and 4,096 elsewhere. Linux UDP GSO caps a
  `sendmsg` batch at one 65,535-byte IP datagram, so a larger guarantee stalls
  delivery outright (`FELIX_INITIAL_MTU`). For the same reason, on Linux
  `FELIX_INITIAL_MTU` and `FELIX_MTU_UPPER_BOUND` are clamped to 6,553, with
  a warning.

Together these raised sustained macOS loopback throughput ~7.5×: driver
isolation was the dominant term, since per-datagram scheduler wakeup latency
had been the byte-rate ceiling. That ceiling is macOS-specific. Linux's
*pre-fix* baseline already exceeded what macOS reaches with every fix applied,
which is why the pool is off there.

Runs that land far below the numbers in
[Benchmarks](/features/benchmarks/) used to be expected occasionally, and
are not any more: the ~30% "degraded mode" was a path-MTU black-hole collapse
and is fixed. A slow run now means something is wrong, so investigate it
rather than rerunning.

## Recommended perf environment

- Prefer a Linux host (or a Linux VM pinned to dedicated CPU resources).
- Use release builds.
- Pin CPU governor to performance mode where possible.
- Isolate benchmark cores and avoid noisy neighbors/background load.

## Throughput semantics in `latency-demo`

- Throughput output primarily reflects pipeline throughput through publish/fanout/delivery stages.
- If your scenario needs strict end-to-end acknowledgement barriers, run with ack-enabled publish modes and compare against ack counters.
- `publish_write_shape` output helps compare write fragmentation (`writes_per_batch`, `avg_bytes_per_write`) across settings like `publish_fastpath` true/false.

## Reproducing perf runs

```bash
# Baseline Linux-friendly settings
export FELIX_SUB_QUEUE_CAPACITY=4096
export FELIX_SUB_QUEUE_POLICY=drop_new
export FELIX_SUB_SINGLE_WRITER_PER_CONN=true
export FELIX_CLIENT_SUB_QUEUE_CAPACITY=4096
export FELIX_CLIENT_SUB_QUEUE_POLICY=drop_new

# Optional flow-control tuning
export FELIX_EVENT_CONN_RECV_WINDOW=268435456
export FELIX_EVENT_STREAM_RECV_WINDOW=67108864
export FELIX_EVENT_SEND_WINDOW=268435456

cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
  --all false \
  --warmup 2000 \
  --total 20000 \
  --payload 4096 \
  --fanout 10 \
  --batch 64 \
  --binary
```

For latency-focused runs (`batch=1`), keep fanout low first, then scale fanout to identify queueing and flow-control effects.
