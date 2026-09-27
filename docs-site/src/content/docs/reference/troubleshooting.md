---
title: "Troubleshooting Guide"
---

The failures people actually hit, what each one means, and the command that confirms it.

## Build and Compilation Issues

### Rust Version Too Old

**Symptom**: Compilation errors mentioning unstable features or syntax errors.

```
error[E0658]: use of unstable library feature 'try_blocks'
```

**Solution**: Update Rust to 1.97.1 or later.

```bash
# Check current version
rustc --version

# Update Rust
rustup update

# Set specific toolchain (if needed)
rustup override set 1.97.1
```

### Missing Dependencies

**Symptom**: Linker errors or missing system libraries.

```
error: linking with `cc` failed
```

**Solution**: Install required system dependencies.

**Linux (Debian/Ubuntu)**:
```bash
sudo apt-get update
sudo apt-get install build-essential pkg-config libssl-dev
```

**Linux (Fedora/RHEL)**:
```bash
sudo dnf install gcc pkg-config openssl-devel
```

**macOS**:
```bash
xcode-select --install
brew install openssl pkg-config
```

### Build Hangs or Takes Forever

**Symptom**: `cargo build` appears stuck or takes excessive time.

**Solution**:

1. Check cargo build jobs:
```bash
# Limit parallel jobs
cargo build --jobs 2
```

2. Clean build cache:
```bash
cargo clean
cargo build --release
```

3. Check disk space:
```bash
df -h
# Clean target directory if low
rm -rf target
```

### Compilation Errors After Git Pull

**Symptom**: Build fails after pulling latest changes.

**Solution**: Clean and rebuild.

```bash
cargo clean
cargo update
cargo build --workspace --release
```

## Network and Connection Issues

### Port Already in Use

**Symptom**: Broker fails to start with error.

```
Error: Address already in use (os error 48)
```

**Solution**: Change ports or kill conflicting process.

**Option 1 - Change ports**:
```bash
export FELIX_QUIC_BIND="0.0.0.0:5001"
export FELIX_BROKER_METRICS_BIND="0.0.0.0:8081"
cargo run --release -p felix-broker-service
```

**Option 2 - Find and kill process**:
```bash
# Linux/Mac - find process on port 5000
lsof -i :5000
sudo kill -9 <PID>

# Or use ss
ss -tulpn | grep 5000
```

### Connection Refused

**Symptom**: Client cannot connect to broker.

```
Error: Connection refused (os error 111)
```

**Solutions**:

1. **Verify broker is running**:
```bash
ps aux | grep broker
lsof -i UDP:5000
```

2. **Check bind address**:
```bash
# Broker logs should show:
# "QUIC listening on 0.0.0.0:5000"

# If binding to 127.0.0.1, external connections won't work
export FELIX_QUIC_BIND="0.0.0.0:5000"
```

3. **Check firewall** (Linux):
```bash
# Allow UDP 5000
sudo ufw allow 5000/udp

# Or iptables
sudo iptables -A INPUT -p udp --dport 5000 -j ACCEPT
```

4. **Test connectivity**:
```bash
# From client machine
nc -zvu <broker-ip> 5000
```

### QUIC Handshake Timeout

**Symptom**: Connection hangs during QUIC handshake.

```
Error: HandshakeTimeout
```

**Solutions**:

1. **Check network path**: Ensure UDP traffic is not blocked.

2. **Verify MTU**: QUIC sensitive to MTU issues.
```bash
# Test with ping
ping -M do -s 1472 <broker-ip>

# Reduce MTU if needed (client-side)
ip link set dev eth0 mtu 1400
```

3. **Check NAT/Load Balancer**: Ensure UDP pass-through.

### Connection Reset by Peer

**Symptom**: Established connections drop unexpectedly.

```
Error: Connection reset by peer (os error 104)
```

**Solutions**:

1. **Check broker logs** for crashes or panics.

2. **Increase flow-control windows**:
```bash
export FELIX_CACHE_CONN_RECV_WINDOW="536870912"
export FELIX_EVENT_CONN_RECV_WINDOW="536870912"
```

3. **Check resource limits**:
```bash
# Increase open file limit
ulimit -n 65536
```

## Performance Issues

### High Latency

**Symptom**: p99/p999 latency much higher than expected.

**Solutions**:

1. **Use release builds**:
```bash
# Debug builds are 10-100x slower
cargo build --release
cargo run --release -p felix-broker-service
```

2. **Disable timing collection**:
```bash
export FELIX_DISABLE_TIMINGS="1"
```

3. **Reduce batch delay**:
```bash
export FELIX_EVENT_BATCH_MAX_DELAY_US="50"
```

4. **Check system load**:
```bash
top
htop
# Look for CPU saturation, memory pressure
```

5. **Profile with perf** (Linux):
```bash
sudo perf record -g cargo run --release -p felix-broker-service
sudo perf report
```

### Low Throughput

**Symptom**: Messages/second much lower than expected.

**Solutions**:

1. **Increase batch sizes**:
```bash
export FELIX_EVENT_BATCH_MAX_EVENTS="256"
export FELIX_EVENT_BATCH_MAX_BYTES="1048576"
export FELIX_EVENT_BATCH_MAX_DELAY_US="1000"
```

2. **Increase connection pools**:
```bash
export FELIX_EVENT_CONN_POOL="16"
export FELIX_CACHE_CONN_POOL="16"
```

3. **Check network bandwidth**:
```bash
iperf3 -c <broker-ip> -u -b 1G
```

4. **Verify CPU affinity**:
```bash
# Pin broker to specific cores
taskset -c 0-7 cargo run --release -p felix-broker-service
```

### High Memory Usage

**Symptom**: Broker consuming excessive memory.

```
OOM Killed
```

**Solutions**:

1. **Check actual memory usage**:
```bash
ps aux | grep broker
pmap <pid>
```

2. **Reduce flow-control windows**:
```bash
export FELIX_CACHE_CONN_RECV_WINDOW="134217728"   # 128 MiB
export FELIX_CACHE_STREAM_RECV_WINDOW="33554432"  # 32 MiB
export FELIX_EVENT_CONN_RECV_WINDOW="134217728"
```

3. **Reduce queue depths**:
```bash
export FELIX_BROKER_PUB_QUEUE_DEPTH="512"
export FELIX_SUBSCRIBER_QUEUE_CAPACITY="64"
```

4. **Limit connection pools** (client-side):
```bash
export FELIX_EVENT_CONN_POOL="4"
export FELIX_CACHE_CONN_POOL="4"
```

5. **Check for memory leaks**:
```bash
# Use valgrind (debug build)
valgrind --leak-check=full ./target/debug/broker
```

### CPU Saturation

**Symptom**: Broker at 100% CPU, high latency.

**Solutions**:

1. **Scale horizontally**: Deploy multiple broker instances.

2. **Reduce fanout batch size**:
```bash
export FELIX_FANOUT_BATCH="32"
```

3. **Disable telemetry**:
```bash
export FELIX_DISABLE_TIMINGS="1"
# Or rebuild without telemetry feature
cargo build --release --no-default-features
```

4. **Profile hot paths**:
```bash
cargo flamegraph -p felix-broker-service
```

## Runtime Errors

### Panic or Crash

**Symptom**: Broker exits with panic message.

```
thread 'main' panicked at 'called `Result::unwrap()` on an `Err` value'
```

**Solutions**:

1. **Enable backtraces**:
```bash
export RUST_BACKTRACE=1
cargo run --release -p felix-broker-service
```

2. **Check logs** for context before panic.

3. **Run with debug symbols**:
```bash
cargo build --profile release-with-debug
./target/release-with-debug/broker
```

4. **Report issue** with full backtrace and reproduction steps.

### Out of File Descriptors

**Symptom**: Error opening connections or files.

```
Error: Too many open files (os error 24)
```

**Solution**: Increase file descriptor limit.

```bash
# Check current limit
ulimit -n

# Increase temporarily
ulimit -n 65536

# Increase permanently (Linux)
echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf

# Verify
ulimit -n
```

### Queue Full / Backpressure

**Symptom**: Publish operations timing out.

```
Error: publish queue full; retry in 10 ms
```

The error is `overloaded` with `detail.reason = "publish_queue_full"`: nothing
was queued, and it is safe to retry. `felix_tenant_publish_queue_full_total`
shows which tenants are being refused.

**Solutions**:

1. **Increase queue depth**:
```bash
export FELIX_BROKER_PUB_QUEUE_DEPTH="2048"
```

2. **Increase timeout**:
```bash
export FELIX_PUBLISH_QUEUE_WAIT_MS="5000"
```

3. **Slow down publisher** or scale broker capacity.

4. **Check subscriber health**: Slow subscribers cause backpressure.

## Docker and Container Issues

### Container Won't Start

**Symptom**: Docker container exits immediately.

```bash
docker logs felix-broker
# Check for errors
```

**Solutions**:

1. **Check image build**:
```bash
docker compose build --no-cache
```

2. **Run interactively**:
```bash
docker run -it --rm felix/broker:latest /bin/sh
```

3. **Verify entrypoint**:
```bash
docker inspect felix/broker:latest | grep -A 5 Entrypoint
```

### Container Health Check Failing

**Symptom**: Container marked unhealthy.

```bash
docker compose ps
# STATUS: unhealthy
```

**Solutions**:

1. **Test health endpoint**:
```bash
docker exec felix-broker wget -qO- http://localhost:8080/healthz
```

2. **Check metrics bind**:
```yaml
environment:
  - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
```

3. **Increase start period**:
```yaml
healthcheck:
  start_period: 30s
```

### Volume Permission Issues

**Symptom**: Cannot write to mounted volume.

```
Permission denied
```

**Solution**: Fix volume permissions.

```bash
# Check user in container
docker exec felix-broker id

# Fix ownership
docker exec -u root felix-broker chown -R 10001:nogroup /data

# Or in Dockerfile
RUN chown -R 10001:nogroup /data
```

## Kubernetes Issues

### Pod Stuck in Pending

**Symptom**: Pod never schedules.

```bash
kubectl describe pod felix-broker-0 -n felix
# Events: FailedScheduling
```

**Solutions**:

1. **Check node resources**:
```bash
kubectl describe nodes
kubectl top nodes
```

2. **Check PVC binding**:
```bash
kubectl get pvc -n felix
# Look for Pending PVCs
```

3. **Check pod affinity**:
```bash
kubectl get pods -n felix -o wide
# Verify anti-affinity not blocking
```

### CrashLoopBackOff

**Symptom**: Pod repeatedly crashes.

```bash
kubectl get pods -n felix
# STATUS: CrashLoopBackOff
```

**Solutions**:

1. **Check logs**:
```bash
kubectl logs felix-broker-0 -n felix
kubectl logs felix-broker-0 -n felix --previous
```

2. **Check events**:
```bash
kubectl describe pod felix-broker-0 -n felix
```

3. **Increase resources**:
```yaml
resources:
  limits:
    memory: "8Gi"
```

### Service Not Reachable

**Symptom**: Cannot connect to service.

**Solutions**:

1. **Test from within cluster**:
```bash
kubectl run -it --rm debug --image=busybox -n felix -- sh
nc -zvu felix-broker-headless 5000
```

2. **Check service endpoints**:
```bash
kubectl get endpoints -n felix
```

3. **Verify service selector**:
```bash
kubectl get svc felix-broker -n felix -o yaml
kubectl get pods -n felix --show-labels
```

### StatefulSet Not Scaling

**Symptom**: Replicas not increasing.

**Solutions**:

1. **Check PVC provisioning**:
```bash
kubectl get pvc -n felix
```

2. **Check storage class**:
```bash
kubectl get storageclass
kubectl describe storageclass fast-ssd
```

3. **Check events**:
```bash
kubectl get events -n felix --sort-by='.lastTimestamp'
```

## Client SDK Issues

### Connection Pool Exhaustion

**Symptom**: Client operations hang or timeout.

**Solution**: Increase pool sizes.

```bash
export FELIX_EVENT_CONN_POOL="16"
export FELIX_CACHE_CONN_POOL="16"
export FELIX_CACHE_STREAMS_PER_CONN="8"
```

### Stream Errors

**Symptom**: Stream operations fail unexpectedly.

**Solutions**:

1. **Check broker logs** for corresponding errors.

2. **Verify stream exists**:
```bash
# Use broker API or control plane
curl http://broker:8080/streams
```

3. **Increase timeouts** (implementation-dependent).

## Debugging Tools

### Enable Verbose Logging

```bash
# Debug all Felix crates
export RUST_LOG="felix=debug"

# Trace specific crate
export RUST_LOG="felix_broker=trace"

# Multiple filters
export RUST_LOG="felix_broker=debug,felix_wire=trace,felix_transport=debug"
```

### Capture Network Traffic

```bash
# Capture QUIC traffic
sudo tcpdump -i any -w felix.pcap udp port 5000

# Analyze with Wireshark
wireshark felix.pcap
```

### Profile Performance

**Linux perf**:
```bash
sudo perf record -g --call-graph dwarf cargo run --release -p felix-broker-service
sudo perf report
```

**flamegraph**:
```bash
cargo install flamegraph
cargo flamegraph -p felix-broker-service -- --custom-args
```

**Memory profiling**:
```bash
cargo install --locked cargo-profdata
cargo profdata run -p felix-broker-service
```

### Test Latency Locally

```bash
# Run broker
cargo run --release -p felix-broker-service

# In another terminal, run latency demo
cargo run --release -p felix-broker-service --features demo --bin latency-demo -- \
  --binary \
  --fanout 1 \
  --batch 1 \
  --payload 1024 \
  --total 5000
```

### Check System Limits

```bash
# File descriptors
ulimit -n

# Max user processes
ulimit -u

# Memory locked
ulimit -l

# See all limits
ulimit -a
```

## Common Configuration Mistakes

### Mismatched Window Sizes

**Issue**: Client/broker window mismatch causes stalls.

**Solution**: Ensure consistent configuration.

```bash
# Broker
export FELIX_CACHE_CONN_RECV_WINDOW="268435456"

# Client (matching)
export FELIX_CACHE_SEND_WINDOW="268435456"
```

### Wrong Frame Format Assumptions

**Issue**: Client assumes JSON event frames.

**Solution**: Ensure clients decode binary `EventBatch` on subscription event streams.

### Insufficient Resources

**Issue**: Resource limits too low for workload.

**Solution**: Profile and adjust limits.

```bash
# Monitor resources
docker stats
kubectl top pods -n felix

# Adjust based on observations
```

## Getting Help

If you're still stuck:

1. **Check GitHub Issues**: [https://github.com/gabloe/felix/issues](https://github.com/gabloe/felix/issues)

2. **Search documentation**: Use site search or `grep` docs directory

3. **Collect diagnostic info**:
```bash
# Broker version
cargo run --release -p felix-broker-service -- --version

# System info
uname -a
rustc --version
docker version
kubectl version

# Configuration
env | grep FELIX_

# Logs (last 100 lines)
journalctl -u felix-broker -n 100
```

4. **Create minimal reproduction**: Simplify to smallest failing case

5. **Open an issue** with:
   - Felix version
   - Operating system
   - Rust version
   - Configuration
   - Full error message
   - Steps to reproduce

## Next Steps

- **Configuration reference**: [Configuration Reference](/felix/reference/configuration/)
- **Environment variables**: [Environment Variables](/felix/reference/environment-variables/)
- **FAQ**: [Frequently Asked Questions](/felix/reference/faq/)
