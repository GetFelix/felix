#!/bin/sh
# felix-agent: the VM-side half of the session drivers. lib.sh installs it at
# /usr/local/sbin/felix-agent in the same run-command that calls it, so the VM
# always runs the operator's copy.
#
# It runs under dash as root. Every command prints compact key=value lines:
# run-command returns only the last ~4 KB of output, so anything bulky (the
# metrics page, per-second samples, fio JSON) is reduced here and the raw file
# stays on the VM under /var/tmp/felix-*.
set -eu

SAMPLES=/var/tmp/felix-samples
PERFDIR=/var/tmp/felix-perf
OVERRIDES=/etc/felix/overrides.env
# Covers any listener count the sweep uses; the internal port is counted too
# so replication traffic can be told apart from client traffic.
PORT_FIRST=5000
PORT_COUNT=8
PORT_INTERNAL=7000

metrics_url() {
  bind=$(sed -n 's/^FELIX_BROKER_METRICS_BIND=//p' /etc/felix/broker.env 2>/dev/null | tail -1)
  if [ -n "$bind" ]; then echo "http://$bind/metrics"; fi
}

felix_pid() {
  pidof -s felix-broker 2>/dev/null || pidof -s felix-controlplane 2>/dev/null || true
}

# install-ref <base-url> <label>: fetch a served build, check it against the
# SHA256SUMS served beside it, and unpack it into /opt/felix/<label>/. The hash
# check is the point: a curl that "succeeds" with a stale file is how an old
# binary once kept running through a whole session.
cmd_install_ref() {
  base="$1"; label="$2"
  root="felix-$label-linux-x86_64"
  tmp=$(mktemp -d /var/tmp/felix-ref.XXXXXX)
  curl -fsS "$base/$label/$root.tar.gz" -o "$tmp/$root.tar.gz"
  curl -fsS "$base/$label/SHA256SUMS" -o "$tmp/SHA256SUMS"
  (cd "$tmp" && sha256sum -c --quiet SHA256SUMS)
  rm -rf "/opt/felix/$label"; mkdir -p "/opt/felix/$label"
  tar -xzf "$tmp/$root.tar.gz" -C "/opt/felix/$label" --strip-components=1
  rm -rf "$tmp"
  echo "installed.$label=$(cat "/opt/felix/$label/REF")"
}

# activate <broker|controlplane> <label>: point the service at a build. The
# caller restarts.
cmd_activate() {
  kind="$1"; label="$2"
  bin="/opt/felix/$label/felix-$kind"
  [ -x "$bin" ] || { echo "!! $bin is not installed" >&2; exit 1; }
  ln -sfn "$bin" "/usr/local/bin/felix-$kind"
  echo "active.$kind=$label $(sha256sum "$bin" | cut -c1-16)"
}

# env-replace: stdin becomes the whole overrides file.
cmd_env_replace() {
  mkdir -p /etc/felix
  cat > "$OVERRIDES.tmp"
  mv "$OVERRIDES.tmp" "$OVERRIDES"
  sed 's/^/overrides./' "$OVERRIDES"
}

# env-set KEY=VALUE...: change or add single knobs.
cmd_env_set() {
  mkdir -p /etc/felix; touch "$OVERRIDES"
  for kv in "$@"; do
    k="${kv%%=*}"
    grep -v "^$k=" "$OVERRIDES" > "$OVERRIDES.tmp" || true
    echo "$kv" >> "$OVERRIDES.tmp"
    mv "$OVERRIDES.tmp" "$OVERRIDES"
  done
  sed 's/^/overrides./' "$OVERRIDES"
}

cmd_env_unset() {
  touch "$OVERRIDES"
  for k in "$@"; do
    grep -v "^$k=" "$OVERRIDES" > "$OVERRIDES.tmp" || true
    mv "$OVERRIDES.tmp" "$OVERRIDES"
  done
  sed 's/^/overrides./' "$OVERRIDES"
}

cmd_restart() {
  systemctl reset-failed felix-broker 2>/dev/null || true
  systemctl restart felix-broker
  sleep 2
  echo "state=$(systemctl is-active felix-broker || true)"
}

# wipe: stop the broker, empty the durable log and drop the page cache, so a
# cell neither inherits the previous cell's segments nor reads its cache.
cmd_wipe() {
  systemctl stop felix-broker
  rm -rf /data/felix
  mkdir -p /data/felix
  sync
  echo 3 > /proc/sys/vm/drop_caches
  echo "wiped=$(df -h /data | awk 'NR==2 { print $3 " used" }')"
}

# counters-install: one counting rule per client port and the internal port,
# in a chain of their own. Counting only; nothing is dropped or redirected.
cmd_counters_install() {
  command -v iptables >/dev/null || { echo "counters=unavailable"; return 0; }
  iptables -w -N FELIX_PORTS 2>/dev/null || iptables -w -F FELIX_PORTS
  iptables -w -C INPUT -p udp -j FELIX_PORTS 2>/dev/null || iptables -w -I INPUT -p udp -j FELIX_PORTS
  p=$PORT_FIRST
  while [ "$p" -lt $((PORT_FIRST + PORT_COUNT)) ]; do
    iptables -w -A FELIX_PORTS -p udp --dport "$p" -j RETURN
    p=$((p + 1))
  done
  iptables -w -A FELIX_PORTS -p udp --dport "$PORT_INTERNAL" -j RETURN
  echo "counters=installed"
}

# snapshot [--env]: the counters a cell is diffed on. --env adds the running
# binary and its full FELIX_* environment, which is what makes a row
# attributable to a build and a configuration.
cmd_snapshot() {
  echo "__SNAP_BEGIN__"
  echo "t=$(date +%s.%N)"
  pid=$(felix_pid)
  if [ -n "$pid" ]; then
    awk '{ print "proc.ticks=" $14 + $15 }' "/proc/$pid/stat"
    awk '/^Threads/ { print "proc.threads=" $2 } /^VmRSS/ { print "proc.rss_kb=" $2 }' "/proc/$pid/status"
  fi
  url=$(metrics_url || true)
  if [ -n "$url" ]; then
    curl -fsS -m 5 "$url" 2>/dev/null | awk '
      /^#/ { next }
      {
        n = $1; sub(/\{.*/, "", n); v = $NF + 0
        if (n == "felix_storage_append_bytes_total") { ab += v; if (v > 0) abn++ }
        else if (n == "felix_storage_append_records_total") ar += v
        else if (n == "felix_storage_sync_total") st += v
        else if (n == "felix_storage_sync_duration_seconds_sum") sds += v
        else if (n == "felix_storage_sync_duration_seconds_count") sdc += v
        else if (n == "felix_storage_sync_batch_appends_sum") sbs += v
        else if (n == "felix_storage_sync_batch_appends_count") sbc += v
        else if (n == "felix_storage_sync_failures_total") sf += v
        else if (n == "felix_broker_publish_quorum_failed_total") qf += v
        else if (n == "felix_publish_bytes_total") pb += v
        # A relayed publish is counted as forwarded on top of its outcome.
        else if (n == "felix_publish_requests_total") { if ($1 ~ /result="forwarded"/) fw += v; else pr += v }
      }
      END {
        printf "m.append_bytes=%.0f\nm.append_series_active=%d\nm.append_records=%.0f\n", ab, abn, ar
        printf "m.sync_total=%.0f\nm.sync_dur_sum=%.6f\nm.sync_dur_count=%.0f\n", st, sds, sdc
        printf "m.sync_batch_sum=%.0f\nm.sync_batch_count=%.0f\nm.sync_failures=%.0f\n", sbs, sbc, sf
        printf "m.quorum_failed=%.0f\nm.publish_bytes=%.0f\nm.publish_requests=%.0f\nm.forwarded=%.0f\n", qf, pb, pr, fw
      }'
  fi
  awk '/^Udp:/ { if (!h) { for (i = 2; i <= NF; i++) k[i] = $i; h = 1 } else { for (i = 2; i <= NF; i++) print "udp." k[i] "=" $i } }' /proc/net/snmp
  if command -v iptables >/dev/null; then
    iptables -w -nvxL FELIX_PORTS 2>/dev/null | awk '/dpt:/ {
      for (i = 1; i <= NF; i++) if ($i ~ /^dpt:/) p = substr($i, 5)
      print "port." p ".pkts=" $1; print "port." p ".bytes=" $2 }'
  fi
  if [ "${1:-}" = "--env" ]; then
    ports=$(ss -Huanp 2>/dev/null | awk '/felix-broker/ { n = split($4, a, ":"); p = a[n]; if (p >= 5000 && p < 5064) printf "%s%s", s, p; s = "," }')
    echo "listen.ports=$ports"
    for kind in broker controlplane; do
      if [ -e "/usr/local/bin/felix-$kind" ]; then
        bin=$(readlink -f "/usr/local/bin/felix-$kind")
        echo "bin.$kind.path=$bin"
        echo "bin.$kind.sha256=$(sha256sum "$bin" | cut -d' ' -f1)"
        echo "bin.$kind.ref=$(cat "$(dirname "$bin")/REF" 2>/dev/null || echo unknown)"
      fi
    done
    if [ -n "$pid" ]; then
      # Nothing secret is expected here (the node token is a file), but redact
      # by name anyway: results are committed.
      tr '\0' '\n' < "/proc/$pid/environ" | awk -F= '/^FELIX_/ {
        k = $1
        if (k ~ /(TOKEN|SECRET|PASSWORD)/ && k !~ /_FILE$/) print "env." k "=<redacted>"
        else print "env." $0 }'
    fi
  fi
  echo "__SNAP_END__"
}

# The ports the series records bytes for, in column order.
series_ports() {
  p=$PORT_FIRST
  while [ "$p" -lt $((PORT_FIRST + PORT_COUNT)) ]; do printf '%s ' "$p"; p=$((p + 1)); done
  echo "$PORT_INTERNAL"
}

# sampler-start <tag>: once a second until sampler-stop, one line of
# /proc/stat, the felix process's CPU ticks, the broker's append and publish
# byte counters, UDP InDatagrams/RcvbufErrors and the per-port byte counters.
# Armed before the load: run-command dispatch takes seconds, so a sample taken
# "during" the load from the operator often lands after it.
cmd_sampler_start() {
  tag="$1"
  mkdir -p "$SAMPLES"
  rm -f "$SAMPLES/$tag.txt" "$SAMPLES/$tag.stop" "$SAMPLES/$tag.series.tsv" "$SAMPLES/$tag.series.gz.b64"
  url=$(metrics_url || true)
  nohup "$0" _sample "$tag" "$url" >/dev/null 2>&1 &
  echo "sampler=$tag"
}

# Columns: t, cpu user+nice sys idle iowait irq softirq steal, proc ticks,
# append bytes, publish bytes, udp InDatagrams, udp RcvbufErrors, then bytes
# per port in series_ports order. Each line costs one scrape, one iptables
# list and a few small /proc reads.
cmd__sample() {
  tag="$1"; url="${2:-}"; f="$SAMPLES/$tag.txt"; i=0
  ports=$(series_ports)
  ipt=0
  if command -v iptables >/dev/null && iptables -w -nL FELIX_PORTS >/dev/null 2>&1; then ipt=1; fi
  while [ "$i" -lt 3600 ] && [ ! -e "$SAMPLES/$tag.stop" ]; do
    t=$(date +%s.%N)
    pid=$(pidof -s felix-broker 2>/dev/null || pidof -s felix-loadgen 2>/dev/null || true)
    pt=0
    if [ -n "$pid" ]; then pt=$(awk '{ print $14 + $15 }' "/proc/$pid/stat" 2>/dev/null || echo 0); fi
    m="0 0"
    if [ -n "$url" ]; then
      m=$(curl -s -m 1 "$url" | awk '
        /^#/ { next }
        { n = $1; sub(/\{.*/, "", n)
          if (n == "felix_storage_append_bytes_total") a += $NF
          else if (n == "felix_publish_bytes_total") p += $NF }
        END { printf "%.0f %.0f", a, p }')
    fi
    pb=""
    if [ "$ipt" = 1 ]; then
      pb=$(iptables -w -nvxL FELIX_PORTS 2>/dev/null | awk '/dpt:/ {
        for (i = 1; i <= NF; i++) if ($i ~ /^dpt:/) printf "%s:%s ", substr($i, 5), $2 }')
    fi
    awk -v t="$t" -v pt="$pt" -v m="${m:-0 0}" -v pb="$pb" -v ports="$ports" '
      FILENAME == "/proc/stat" && /^cpu / { c = ($2 + $3) " " $4 " " $5 " " $6 " " $7 " " $8 " " $9 }
      FILENAME == "/proc/net/snmp" && /^Udp:/ {
        if (!h) { for (i = 2; i <= NF; i++) k[$i] = i; h = 1 }
        else { ind = $(k["InDatagrams"]); rb = $(k["RcvbufErrors"]) }
      }
      END {
        n = split(pb, kv, " "); for (i = 1; i <= n; i++) { split(kv[i], x, ":"); b[x[1]] = x[2] }
        line = t " " c " " pt " " m " " (ind + 0) " " (rb + 0)
        n = split(ports, pp, " "); for (i = 1; i <= n; i++) line = line " " (b[pp[i]] + 0)
        print line
      }' /proc/stat /proc/net/snmp >> "$f" || true
    i=$((i + 1))
    sleep 1
  done
}

# sampler-stop <tag>: stop and reduce. CPU averages only samples above 15%
# busy, so the idle head and tail do not dilute the loaded middle. Append
# throughput is taken over the window between the first and last second the
# counter moved, so it does not depend on when the operator's calls landed.
# The raw counters also go out as <tag>.series.gz.b64 for the operator to
# fetch; summarize.py cuts the steady-state window from them.
cmd_sampler_stop() {
  tag="$1"
  touch "$SAMPLES/$tag.stop"
  sleep 1
  f="$SAMPLES/$tag.txt"
  [ -s "$f" ] || { echo "s.samples=0"; return 0; }
  echo "__SAMPLE_BEGIN__"
  echo "s.nproc=$(nproc)"
  awk -v hz="$(getconf CLK_TCK)" '
    { t[NR] = $1; u[NR] = $2; s[NR] = $3; id[NR] = $4; w[NR] = $5; iq[NR] = $6; si[NR] = $7; sl[NR] = $8; pt[NR] = $9; ab[NR] = $10 }
    END {
      for (i = 2; i <= NR; i++) {
        du = u[i] - u[i-1]; ds = s[i] - s[i-1]; di = id[i] - id[i-1]; dw = w[i] - w[i-1]
        dq = iq[i] - iq[i-1]; dsi = si[i] - si[i-1]; dsl = sl[i] - sl[i-1]
        tot = du + ds + di + dw + dq + dsi + dsl
        dt = t[i] - t[i-1]
        if (tot > 0) {
          b = 100 * (tot - di - dw) / tot
          if (b > 15) {
            n++; B += b; U += 100 * du / tot; S += 100 * ds / tot; SI += 100 * (dq + dsi) / tot
            W += 100 * dw / tot; ST += 100 * dsl / tot
            dp = pt[i] - pt[i-1]
            if (dp > 0 && dt > 0) { c = dp / hz / dt; P += c; if (c > PM) PM = c }
          }
        }
        if (ab[i] > ab[i-1]) {
          if (!a) a = i; z = i
          if (dt > 0) { r = (ab[i] - ab[i-1]) / dt; if (r > R) R = r }
        }
      }
      printf "s.samples=%d\ns.busy_samples=%d\n", NR, n
      if (n > 0) printf "s.cpu_busy=%.1f\ns.cpu_us=%.1f\ns.cpu_sy=%.1f\ns.cpu_si=%.1f\ns.cpu_wa=%.1f\ns.cpu_st=%.1f\ns.proc_cores=%.2f\ns.proc_cores_max=%.2f\n", B/n, U/n, S/n, SI/n, W/n, ST/n, P/n, PM
      if (a) {
        bytes = ab[z] - ab[a-1]; secs = t[z] - t[a-1]
        printf "s.append_bytes=%.0f\ns.append_secs=%.2f\ns.append_mb_s=%.1f\ns.append_peak_mb_s=%.1f\n", bytes, secs, bytes / secs / 1e6, R / 1e6
      }
    }' "$f"
  series="$SAMPLES/$tag.series.tsv"
  {
    echo "# hz=$(getconf CLK_TCK) nproc=$(nproc) host=$(hostname)"
    printf 't\tproc_ticks\tappend_bytes\tpublish_bytes\tudp_in\tudp_rcvbuf_errors'
    for p in $(series_ports); do printf '\tport.%s.bytes' "$p"; done
    echo
    # Milliseconds are plenty, and the shorter stamp keeps the fetch small.
    awk 'NF >= 14 { $1 = sprintf("%.3f", $1); $2 = $3 = $4 = $5 = $6 = $7 = $8 = ""; print }' "$f" \
      | tr -s ' ' | tr ' ' '\t'
  } > "$series"
  gzip -9 -c "$series" | base64 -w0 > "$SAMPLES/$tag.series.gz.b64"
  echo "s.series_b64_bytes=$(wc -c < "$SAMPLES/$tag.series.gz.b64")"
  echo "__SAMPLE_END__"
}

cmd_ready_wait() {
  timeout="$1"; shift
  deadline=$(( $(date +%s) + timeout ))
  while :; do
    ok=0; n=0
    for ip in "$@"; do
      n=$((n + 1))
      curl -fs -m 2 "http://$ip:9100/ready" >/dev/null 2>&1 && ok=$((ok + 1))
    done
    [ "$ok" -eq "$n" ] && break
    [ "$(date +%s)" -ge "$deadline" ] && break
    sleep 2
  done
  echo "ready=$ok/$n"
  [ "$ok" -eq "$n" ]
}

cmd_sysinfo() {
  imds() { curl -s -m 3 -H Metadata:true "http://169.254.169.254/metadata/instance/compute/$1?api-version=2021-02-01&format=text"; }
  echo "__SYS_BEGIN__"
  echo "sys.host=$(hostname)"
  echo "sys.kernel=$(uname -r)"
  echo "sys.nproc=$(nproc)"
  echo "sys.mem_kb=$(awk '/^MemTotal/ { print $2 }' /proc/meminfo)"
  echo "sys.vm_size=$(imds vmSize)"
  echo "sys.zone=$(imds zone)"
  echo "sys.location=$(imds location)"
  curl -s -m 3 -H Metadata:true "http://169.254.169.254/metadata/instance/compute/storageProfile?api-version=2021-02-01" \
    | python3 -c 'import json,sys
d = json.load(sys.stdin)
for x in d.get("dataDisks", []):
    md = x.get("managedDisk") or {}
    print("sys.data_disk.%s=%sGiB %s caching=%s" % (x.get("lun"), x.get("diskSizeGB"), md.get("storageAccountType"), x.get("caching")))' 2>/dev/null || true
  for k in net.core.rmem_max net.core.wmem_max net.core.rmem_default net.core.wmem_default \
      net.core.netdev_max_backlog net.core.netdev_budget kernel.io_uring_disabled; do
    echo "sysctl.$k=$(sysctl -n "$k" 2>/dev/null || echo n/a)"
  done
  if command -v ethtool >/dev/null; then
    echo "nic.channels=$(ethtool -l eth0 2>/dev/null | awk '/^Current/ { c = 1 } c && /Combined/ { print $2; exit }')"
    echo "nic.offloads=$(ethtool -k eth0 2>/dev/null | grep -E 'gro|gso|rx-udp' | tr -d ' ' | tr '\n' ',')"
  fi
  if findmnt /data >/dev/null 2>&1; then
    echo "data.mount=$(findmnt -no SOURCE,FSTYPE,OPTIONS /data)"
    src=$(findmnt -no SOURCE /data)
    echo "data.dev=$(lsblk -dno NAME,SIZE,ROTA,MODEL "$src" 2>/dev/null | tr -s ' ')"
    if [ -e /proc/mdstat ]; then echo "data.md=$(grep -E '^md' /proc/mdstat | tr -s ' ')"; fi
  fi
  echo "tools.fio=$(fio --version 2>/dev/null || echo none)"
  echo "tools.perf=$(perf --version 2>/dev/null || echo none)"
  for r in /opt/felix/*/REF; do
    if [ -e "$r" ]; then echo "felix.installed=$(cat "$r")"; fi
  done
  if [ -e /etc/felix/loadgen.ref ]; then echo "felix.loadgen=$(cat /etc/felix/loadgen.ref)"; fi
  echo "__SYS_END__"
}

# fio <label> <bs> <jobs> <runtime-s>: sequential buffered writes with an
# fdatasync after each, in /data — the broker's OnCommit write pattern.
cmd_fio() {
  label="$1"; bs="$2"; jobs="$3"; runtime="$4"
  command -v fio >/dev/null || { echo "fio.$label=unavailable"; return 0; }
  dir=/data/fio-baseline
  rm -rf "$dir"; mkdir -p "$dir"
  fio --name="$label" --directory="$dir" --rw=write --bs="$bs" --size=1G --numjobs="$jobs" \
    --fdatasync=1 --ioengine=psync --time_based --runtime="$runtime" --group_reporting \
    --output-format=json > "/var/tmp/fio-$label.json" 2>/dev/null
  rm -rf "$dir"
  python3 - "$label" "/var/tmp/fio-$label.json" <<'PY'
import json, sys
label, path = sys.argv[1], sys.argv[2]
j = json.load(open(path))["jobs"][0]
w = j["write"]
sync = (j.get("sync") or {}).get("lat_ns") or {}
pct = sync.get("percentile") or {}
print("fio.%s.mb_s=%.1f" % (label, w["bw_bytes"] / 1e6))
print("fio.%s.iops=%.0f" % (label, w["iops"]))
print("fio.%s.write_lat_p50_us=%.1f" % (label, (w.get("clat_ns", {}).get("percentile", {}).get("50.000000", 0)) / 1e3))
print("fio.%s.sync_lat_mean_us=%.1f" % (label, sync.get("mean", 0) / 1e3))
print("fio.%s.sync_lat_p50_us=%.1f" % (label, pct.get("50.000000", 0) / 1e3))
print("fio.%s.sync_lat_p99_us=%.1f" % (label, pct.get("99.000000", 0) / 1e3))
PY
}

# profile-start <tag> <seconds>: `perf record -g` on the broker plus
# per-thread CPU from pidstat, both in the background, both stopping on their
# own. Useful stacks need the frame-pointer build (<ref>-fp); on a plain
# release build most of the tree collapses into [unknown].
cmd_profile_start() {
  tag="$1"; secs="$2"
  mkdir -p "$PERFDIR"
  pid=$(pidof -s felix-broker)
  if command -v perf >/dev/null; then
    nohup perf record -F 199 -g -p "$pid" -o "$PERFDIR/$tag.data" -- sleep "$secs" >/dev/null 2>&1 &
    echo "perf=$tag"
  else
    echo "perf=unavailable"
  fi
  if command -v pidstat >/dev/null; then
    LC_ALL=C nohup pidstat -t -p "$pid" 1 "$secs" > "$PERFDIR/$tag.pidstat" 2>/dev/null &
    echo "pidstat=$tag"
  else
    echo "pidstat=unavailable"
  fi
}

# profile-report <tag>: per-thread-name CPU (summed over threads of the same
# name) and the heads of the perf report by thread and by symbol, clipped to
# fit one run-command message.
cmd_profile_report() {
  tag="$1"
  f="$PERFDIR/$tag.data"
  # perf record exits once its `sleep` ends; wait for the file to be complete.
  i=0
  while pgrep -f "perf record .*$tag.data" >/dev/null 2>&1 && [ "$i" -lt 120 ]; do
    sleep 1; i=$((i + 1))
  done
  if [ -s "$PERFDIR/$tag.pidstat" ]; then
    echo "-- threads (%CPU averaged over the run, summed per name) --"
    awk '$1 == "Average:" && $3 == "-" {
        name = $NF; sub(/^\|__/, "", name); cpu[name] += $9; n[name]++ }
      END { for (k in cpu) printf "%7.1f%%  x%-3d %s\n", cpu[k], n[k], k }' "$PERFDIR/$tag.pidstat" \
      | sort -rn | head -15
  fi
  [ -s "$f" ] || { echo "perf.$tag=missing"; return 0; }
  echo "-- perf by thread --"
  perf report -i "$f" --stdio --no-children --sort comm --percent-limit 1 2>/dev/null \
    | grep -E '^ +[0-9.]+%' | head -12 | cut -c1-90
  echo "-- perf by symbol --"
  perf report -i "$f" --stdio --no-children --sort sym --percent-limit 0.5 2>/dev/null \
    | grep -E '^ +[0-9.]+%' | head -25 | cut -c1-110
}

# profile-collapse <tag>: fold the perf samples into `comm;root;...;leaf count`
# lines (flamegraph.pl input), gzip and base64 them, and print the size so the
# operator can pull the file home in chunks.
cmd_profile_collapse() {
  tag="$1"
  f="$PERFDIR/$tag.data"
  [ -s "$f" ] || { echo "folded.bytes=0"; return 0; }
  perf script -i "$f" 2>/dev/null | awk '
    function flush(   s, i) {
      if (have) { s = comm; for (i = n; i >= 1; i--) s = s ";" fr[i]; cnt[s]++ }
      have = 0; n = 0
    }
    /^[^ \t]/ { flush(); comm = $1; have = 1; next }
    /^[ \t]+[0-9a-f]+ / {
      line = $0
      sub(/^[ \t]+[0-9a-f]+ /, "", line); sub(/ \([^)]*\)$/, "", line)
      sub(/\+0x[0-9a-f]+$/, "", line); gsub(/;/, ":", line)
      fr[++n] = line; next
    }
    /^$/ { flush() }
    END { flush(); for (s in cnt) print s, cnt[s] }' | gzip -9 | base64 -w0 > "$PERFDIR/$tag.folded.gz.b64"
  echo "folded.bytes=$(wc -c < "$PERFDIR/$tag.folded.gz.b64")"
}

# chunk <path> <offset> <length>: a slice of a file, for pulling a small
# artifact home through run-command's size-capped output.
cmd_chunk() {
  printf '__CHUNK_BEGIN__'
  tail -c +"$(($2 + 1))" "$1" | head -c "$3"
  printf '__CHUNK_END__\n'
}

sub="${1:?usage: felix-agent <command> [args]}"
shift
case "$sub" in
  install-ref) cmd_install_ref "$@" ;;
  activate) cmd_activate "$@" ;;
  env-replace) cmd_env_replace ;;
  env-set) cmd_env_set "$@" ;;
  env-unset) cmd_env_unset "$@" ;;
  restart) cmd_restart ;;
  wipe) cmd_wipe ;;
  counters-install) cmd_counters_install ;;
  snapshot) cmd_snapshot "$@" ;;
  sampler-start) cmd_sampler_start "$@" ;;
  sampler-stop) cmd_sampler_stop "$@" ;;
  _sample) cmd__sample "$@" ;;
  ready-wait) cmd_ready_wait "$@" ;;
  sysinfo) cmd_sysinfo ;;
  fio) cmd_fio "$@" ;;
  profile-start) cmd_profile_start "$@" ;;
  profile-report) cmd_profile_report "$@" ;;
  profile-collapse) cmd_profile_collapse "$@" ;;
  chunk) cmd_chunk "$@" ;;
  *) echo "!! unknown command $sub" >&2; exit 2 ;;
esac
