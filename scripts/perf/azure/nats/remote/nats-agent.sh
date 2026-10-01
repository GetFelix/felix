#!/bin/sh
# nats-agent: the VM-side half of the NATS comparison (scripts/perf/azure/nats/).
# nats-lib.sh installs it at /usr/local/sbin/nats-agent in the same run-command
# that calls it, next to felix-agent, so the VM always runs the operator's copy.
#
# Runs under dash as root. Output is compact key=value lines, because
# run-command returns only the last ~4 KB; bulky files stay under
# /var/tmp/nats-cells/.
set -eu

CONF_DIR=/etc/nats
TLS_DIR=/etc/nats/tls
STORE=/data/nats
MON=http://127.0.0.1:8222
CELLS=/var/tmp/nats-cells
SYSCTL_FILE=/etc/sysctl.d/99-nats-bench.conf

nats_url() { echo "tls://$(cat "$CONF_DIR/listen-ip"):4222"; }
nats_cli() { nats -s "$(nats_url)" --tlsca "$TLS_DIR/ca.pem" "$@"; }

# install-server <version> <sha256>: the release tarball, checked against the
# published SHA256SUMS value the operator passes in.
cmd_install_server() {
  ver="$1"; sum="$2"
  dir="/opt/nats/server-v$ver"
  if [ ! -x "$dir/nats-server" ]; then
    tmp=$(mktemp -d /var/tmp/nats-dl.XXXXXX)
    f="nats-server-v$ver-linux-amd64.tar.gz"
    curl -fsSL -o "$tmp/$f" "https://github.com/nats-io/nats-server/releases/download/v$ver/$f"
    echo "$sum  $tmp/$f" | sha256sum -c --quiet
    mkdir -p "$dir"
    tar -xzf "$tmp/$f" -C "$dir" --strip-components=1
    rm -rf "$tmp"
  fi
  ln -sfn "$dir/nats-server" /usr/local/bin/nats-server
  echo "nats_server=$(/usr/local/bin/nats-server --version)"
}

# install-cli <version> <sha256>: the natscli zip. python3 unpacks it so the
# VM needs no unzip package.
cmd_install_cli() {
  ver="$1"; sum="$2"
  dir="/opt/nats/cli-v$ver"
  if [ ! -x "$dir/nats" ]; then
    tmp=$(mktemp -d /var/tmp/nats-dl.XXXXXX)
    f="nats-$ver-linux-amd64.zip"
    curl -fsSL -o "$tmp/$f" "https://github.com/nats-io/natscli/releases/download/v$ver/$f"
    echo "$sum  $tmp/$f" | sha256sum -c --quiet
    python3 -m zipfile -e "$tmp/$f" "$tmp/x"
    mkdir -p "$dir"
    install -m0755 "$(find "$tmp/x" -type f -name nats | head -1)" "$dir/nats"
    rm -rf "$tmp"
  fi
  ln -sfn "$dir/nats" /usr/local/bin/nats
  echo "nats_cli=$(/usr/local/bin/nats --version 2>&1)"
}

# tls-init <ip>: a throwaway CA and a server certificate for the broker's
# private IP. P-256, like the certificates Felix serves QUIC with, so the
# handshake cost is comparable. Kept across cells; only the store is wiped.
cmd_tls_init() {
  ip="$1"
  mkdir -p "$TLS_DIR"
  echo "$ip" > "$CONF_DIR/listen-ip"
  if [ ! -s "$TLS_DIR/server.pem" ]; then
    cd "$TLS_DIR"
    openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem
    openssl req -x509 -new -key ca-key.pem -sha256 -days 30 -subj "/CN=felixperf-nats-ca" -out ca.pem
    openssl ecparam -name prime256v1 -genkey -noout -out server-key.pem
    openssl req -new -key server-key.pem -subj "/CN=felixperf-nats" -out server.csr
    printf 'subjectAltName=IP:%s,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' "$ip" > ext.cnf
    openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -days 30 \
      -sha256 -extfile ext.cnf -out server.pem 2>/dev/null
    chmod 600 ca-key.pem server-key.pem
    rm -f server.csr
  fi
  echo "tls=$(openssl x509 -in "$TLS_DIR/server.pem" -noout -ext subjectAltName 2>/dev/null | tail -1 | tr -d ' ')"
}

# ca-get / ca-put <base64>: carry the CA (public) from the broker to the
# generators.
cmd_ca_get() { printf '__CA_BEGIN__%s__CA_END__\n' "$(base64 -w0 < "$TLS_DIR/ca.pem")"; }
cmd_ca_put() {
  ip="$2"
  mkdir -p "$TLS_DIR"
  printf '%s' "$1" | base64 -d > "$TLS_DIR/ca.pem"
  echo "$ip" > "$CONF_DIR/listen-ip"
  echo "ca=$(openssl x509 -in "$TLS_DIR/ca.pem" -noout -subject)"
}

# tune: TCP buffers raised to the 25 MiB the Felix VMs already allow UDP
# sockets (cloudinit's 99-felix-quic.conf), so TCP autotuning is not capped at
# the stock 6 MiB while QUIC gets 24 MiB. The originals are kept so untune
# restores them.
cmd_tune() {
  mkdir -p "$CONF_DIR"
  if [ ! -e "$CONF_DIR/sysctl.orig" ]; then
    for k in net.ipv4.tcp_rmem net.ipv4.tcp_wmem net.core.somaxconn net.ipv4.tcp_slow_start_after_idle \
        net.ipv4.tcp_notsent_lowat; do
      echo "$k = $(sysctl -n "$k" | tr '\t' ' ')"
    done > "$CONF_DIR/sysctl.orig"
  fi
  cat > "$SYSCTL_FILE" <<'EOF'
net.ipv4.tcp_rmem = 4096 131072 26214400
net.ipv4.tcp_wmem = 4096 65536 26214400
net.core.somaxconn = 4096
net.ipv4.tcp_slow_start_after_idle = 0
EOF
  sysctl -q -p "$SYSCTL_FILE"
  echo "tuned=$(sysctl -n net.ipv4.tcp_rmem | tr '\t' ' ')"
}

cmd_untune() {
  rm -f "$SYSCTL_FILE"
  if [ -e "$CONF_DIR/sysctl.orig" ]; then sysctl -q -p "$CONF_DIR/sysctl.orig"; fi
  echo "untuned=$(sysctl -n net.ipv4.tcp_rmem | tr '\t' ' ')"
}

write_unit() {
  cat > /etc/systemd/system/nats.service <<'UNIT'
[Unit]
Description=NATS server (Felix comparison)
After=network-online.target
[Service]
EnvironmentFile=-/etc/nats/nats.env
ExecStart=/usr/local/bin/nats-server -c /etc/nats/nats.conf
LimitNOFILE=1048576
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
  systemctl daemon-reload
}

# reset <default|always|memory>: stop the server (and the Felix broker, which shares
# /data and the cores), empty the store, drop the page cache, write the config
# for the durability mode and start again. Every cell starts here, as every
# Felix durable cell starts from a wiped /data/felix.
#
# Server-level extras come from /etc/nats/extra.conf and process environment
# (GOGC, GOMAXPROCS, GOMEMLIMIT) from /etc/nats/nats.env; the operator writes
# both before calling this, empty for the defaults.
cmd_reset() {
  mode="$1"
  ip=$(cat "$CONF_DIR/listen-ip")
  systemctl stop felix-broker 2>/dev/null || true
  systemctl stop nats 2>/dev/null || true
  rm -rf "$STORE"; mkdir -p "$STORE"
  sync
  echo 3 > /proc/sys/vm/drop_caches
  touch "$CONF_DIR/extra.conf" "$CONF_DIR/nats.env"
  # Store limit: 90% of what /data has free, so no cell hits a limit and
  # starts discarding (which would flatten the stored-bytes counter).
  avail=$(df -B1 --output=avail "$STORE" | tail -1 | tr -d ' ')
  max_file=$((avail * 9 / 10))
  sync_line=""
  case "$mode" in
    default|memory) ;;
    always) sync_line="  sync_interval: always" ;;
    *) echo "!! mode is default, always or memory, not $mode" >&2; exit 2 ;;
  esac
  cat > "$CONF_DIR/nats.conf" <<CONF
server_name: felixperf-nats
listen: $ip:4222
# Monitoring for the sampler, loopback only.
http: 127.0.0.1:8222
tls {
  cert_file: "$TLS_DIR/server.pem"
  key_file: "$TLS_DIR/server-key.pem"
  timeout: 5
}
jetstream {
  store_dir: "$STORE"
  max_file_store: $max_file
  max_memory_store: $(mem_store_bytes)
$sync_line
}
include "extra.conf"
CONF
  echo "$mode" > "$CONF_DIR/mode"
  write_unit
  systemctl reset-failed nats 2>/dev/null || true
  systemctl start nats
  i=0
  until curl -fs -m 2 "$MON/healthz?js-enabled-only=true" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -lt 60 ] || { echo "!! nats not healthy"; journalctl -u nats -n 20 --no-pager; exit 1; }
    sleep 1
  done
  echo "reset=$mode state=$(systemctl is-active nats)"
}

# 75% of RAM, nats-server's own default, written out so it is recorded.
mem_store_bytes() { awk '/^MemTotal/ { printf "%.0f", $2 * 1024 * 0.75 }' /proc/meminfo; }

# width <n>: the zero-padding `nats bench --multisubjectmax n` uses for its
# subject suffix (len of n in decimal), so stream subjects match it.
width() { printf '%s' "$1" | wc -c | tr -d ' '; }

# streams <n>: n R1 file streams, bench<i> on subject bench.<i>. Fast batch
# publish must be allowed on the stream for `nats bench js pub fast`.
#
# streams <n> memory: memory streams instead, each limited to its share of 90%
# of max_memory_store and discarding the oldest records at the limit; a 90 s
# cell at line rate would not fit in RAM otherwise.
cmd_streams() {
  n="$1"; storage="${2:-file}"; w=$(width "$n"); i=0; limit=""
  if [ "$storage" = memory ]; then
    limit="--max-bytes=$(( $(mem_store_bytes) * 9 / 10 / n ))"
  fi
  while [ "$i" -lt "$n" ]; do
    s=$(printf "%0${w}d" "$i")
    # shellcheck disable=SC2086 # limit is empty or one flag
    nats_cli stream add "bench$s" --subjects "bench.$s" --storage "$storage" --replicas 1 \
      --retention limits --discard old --allow-fast $limit --defaults >/dev/null
    i=$((i + 1))
  done
  echo "streams=$(nats_cli stream ls -n 2>/dev/null | tr '\n' ' ')"
}

# hostfacts: the conditions a row is quoted under.
cmd_hostfacts() {
  echo "host.nproc=$(nproc)"
  echo "host.kernel=$(uname -r)"
  dev=$(ip -o route get 1.1.1.1 2>/dev/null | sed -n 's/.* dev \([^ ]*\).*/\1/p')
  echo "host.nic=${dev:-eth0}"
  echo "host.mtu=$(cat "/sys/class/net/${dev:-eth0}/mtu" 2>/dev/null || echo unknown)"
  for k in net.core.rmem_max net.core.wmem_max net.core.rmem_default net.core.wmem_default \
      net.core.netdev_max_backlog net.core.netdev_budget net.ipv4.tcp_rmem net.ipv4.tcp_wmem \
      net.core.somaxconn net.ipv4.tcp_slow_start_after_idle net.ipv4.tcp_congestion_control; do
    echo "sysctl.$k=$(sysctl -n "$k" 2>/dev/null | tr '\t' ' ' || echo n/a)"
  done
}

json_int() { sed -n "s/^  \"$1\": *\([0-9]*\).*/\1/p" | head -1; }
json_str() { sed -n "s/^  \"$1\": *\"\([^\"]*\)\".*/\1/p" | head -1; }

# snapshot [--env]: what a cell is diffed on, in the key names summarize.py
# already reads for Felix: m.append_bytes is JetStream's stored bytes,
# m.publish_bytes the server's received bytes. --env adds the binary, the
# full config, the process environment and the host facts.
cmd_snapshot() {
  echo "__SNAP_BEGIN__"
  echo "t=$(date +%s.%N)"
  pid=$(pidof -s nats-server 2>/dev/null || true)
  if [ -n "$pid" ]; then
    awk '{ print "proc.ticks=" $14 + $15 }' "/proc/$pid/stat"
    awk '/^Threads/ { print "proc.threads=" $2 } /^VmRSS/ { print "proc.rss_kb=" $2 }' "/proc/$pid/status"
  fi
  jsz=$(curl -fs -m 5 "$MON/jsz?streams=true" 2>/dev/null || true)
  varz=$(curl -fs -m 5 "$MON/varz" 2>/dev/null || true)
  # Appended, not stored: a memory stream discards at its limit. Records are
  # the streams' last sequences; bytes scale them by the mean stored size, as
  # the sampler does.
  printf '%s\n' "$jsz" | awk '
    /"last_seq":/ { v = $2; gsub(/[^0-9]/, "", v); s += v }
    /^  "bytes":/ { b = $2; gsub(/[^0-9]/, "", b) }
    /^  "messages":/ { n = $2; gsub(/[^0-9]/, "", n) }
    END {
      printf "m.append_records=%.0f\nm.append_bytes=%.0f\n", s, (n > 0) ? s * b / n : 0
      printf "m.stored_bytes=%.0f\nm.stored_records=%.0f\n", b, n
    }'
  echo "m.publish_bytes=$(printf '%s\n' "$varz" | json_int in_bytes)"
  echo "m.publish_requests=$(printf '%s\n' "$varz" | json_int in_msgs)"
  echo "m.slow_consumers=$(printf '%s\n' "$varz" | json_int slow_consumers)"
  awk '/^Tcp:/ { if (!h) { for (i = 2; i <= NF; i++) k[i] = $i; h = 1 } else { for (i = 2; i <= NF; i++) print "tcp." k[i] "=" $i } }' /proc/net/snmp
  if [ "${1:-}" = "--env" ]; then
    bin=$(readlink -f /usr/local/bin/nats-server)
    echo "bin.broker.path=$bin"
    echo "bin.broker.sha256=$(sha256sum "$bin" | cut -d' ' -f1)"
    echo "bin.broker.ref=$(printf '%s\n' "$varz" | json_str version) $(sha256sum "$bin" | cut -c1-12)"
    echo "nats.version=$(printf '%s\n' "$varz" | json_str version)"
    echo "nats.go=$(printf '%s\n' "$varz" | json_str go)"
    echo "nats.gomaxprocs=$(printf '%s\n' "$varz" | json_int gomaxprocs)"
    echo "nats.cores=$(printf '%s\n' "$varz" | json_int cores)"
    echo "nats.max_payload=$(printf '%s\n' "$varz" | json_int max_payload)"
    echo "nats.max_pending=$(printf '%s\n' "$varz" | json_int max_pending)"
    echo "nats.write_deadline_ns=$(printf '%s\n' "$varz" | json_int write_deadline)"
    echo "nats.mode=$(cat "$CONF_DIR/mode" 2>/dev/null || echo none)"
    # The config names key files but holds no secret material.
    awk '{ printf "nats.conf.%02d=%s\n", NR, $0 }' "$CONF_DIR/nats.conf" 2>/dev/null || true
    sed 's/^/nats.extra=/' "$CONF_DIR/extra.conf" 2>/dev/null || true
    if [ -n "$pid" ]; then
      tr '\0' '\n' < "/proc/$pid/environ" | awk -F= '/^GO/ { print "env." $0 }'
    fi
    echo "data.mount=$(findmnt -no SOURCE,FSTYPE,OPTIONS /data 2>/dev/null)"
    cmd_hostfacts
  fi
  echo "__SNAP_END__"
}

# bench <name> <start_at> <secs> <kind> <size> <clients> <window> <flow> <streams> <gen> [<gens-total>]
#
# Runs `nats bench` from <start_at> (epoch seconds, shared by every generator)
# until <secs> have passed. nats bench only stops on a message count (its
# --duration needs --throughput, a rate cap), so the run is a series of chunks,
# each sized from the previous chunk's rate to last CHUNK_SECS (default 10) or
# what remains. The gaps between chunks (connect, stream lookup) are short
# against the chunk and show up as dips the steady-state median ignores.
#
# kinds:
#   js-async  `js pub async --batch <window>`: send <window> messages, wait for
#             all their acks, repeat. Subjects rotate over the streams.
#   js-sync   `js pub sync`: one message in flight. Subjects rotate.
#   js-fast   `js pub fast --batch <flow> --max-outstanding-acks <window>`: one
#             ack per <flow> messages, up to <window> acks outstanding. A fast
#             batch targets one stream, so each publisher is pinned to stream
#             (global publisher index mod <streams>).
#   core      `pub`: core NATS, no JetStream, no subscriber.
cmd_bench() {
  name="$1"; start_at="$2"; secs="$3"; kind="$4"; size="$5"; clients="$6"; window="$7"; flow="$8"
  nstreams="$9"; gen="${10}"
  dir="$CELLS/$name"
  rm -rf "$dir"; mkdir -p "$dir"
  chunk_secs="${CHUNK_SECS:-10}"
  w=$(width "$nstreams")
  base="nats -s $(nats_url) --tlsca $TLS_DIR/ca.pem bench"
  multi="--multisubject --multisubjectmax $nstreams"
  common="--size $size --no-progress"
  # A first chunk of 8 MiB per client: a couple of seconds at any plausible rate.
  per_client=$((8388608 / size))
  end=$((start_at + secs))
  now=$(date +%s)
  [ "$now" -ge "$start_at" ] || sleep $((start_at - now))
  s=$(date +%s.%N)
  i=0
  while [ "$(date +%s)" -lt "$end" ]; do
    i=$((i + 1))
    c0=$(date +%s.%N)
    case "$kind" in
      js-fast)
        # Clients of this generator per stream.
        p=$((gen * clients)); last=$((p + clients)); pids=""
        st=0
        while [ "$st" -lt "$nstreams" ]; do
          k=0; q=$p
          while [ "$q" -lt "$last" ]; do [ $((q % nstreams)) -eq "$st" ] && k=$((k + 1)); q=$((q + 1)); done
          if [ "$k" -gt 0 ]; then
            sn=$(printf "%0${w}d" "$st")
            cmd="$base js pub fast bench.$sn --stream bench$sn --clients $k --msgs $((per_client * k)) --batch $flow --max-outstanding-acks $window $common --csv $dir/c$i-s$sn.csv"
            [ "$i" -gt 1 ] || echo "$cmd" >> "$dir/cmds"
            $cmd > "$dir/c$i-s$sn.log" 2>&1 &
            pids="$pids $!"
          fi
          st=$((st + 1))
        done
        rc=0
        for pd in $pids; do wait "$pd" || rc=1; done
        ;;
      js-async|js-sync|core)
        case "$kind" in
          js-async) sub="js pub async bench --stream bench$(printf "%0${w}d" 0) --batch $window $multi" ;;
          js-sync) sub="js pub sync bench --stream bench$(printf "%0${w}d" 0) $multi" ;;
          core) sub="pub core $multi" ;;
        esac
        cmd="$base $sub --clients $clients --msgs $((per_client * clients)) $common --csv $dir/c$i.csv"
        [ "$i" -gt 1 ] || echo "$cmd" >> "$dir/cmds"
        rc=0
        $cmd > "$dir/c$i.log" 2>&1 || rc=1
        ;;
      *) echo "!! unknown kind $kind" >&2; exit 2 ;;
    esac
    if [ "$rc" != 0 ]; then
      echo "!! chunk $i failed"; tail -8 "$dir"/c"$i"*.log; exit 1
    fi
    c1=$(date +%s.%N)
    # Next chunk: this chunk's per-client rate times min(CHUNK_SECS, what is left).
    per_client=$(awk -v n="$per_client" -v a="$c0" -v b="$c1" -v e="$end" -v cs="$chunk_secs" -v fl="${window:-1}" 'BEGIN {
      r = n / ((b - a) > 0.05 ? (b - a) : 0.05); left = e - b; if (left > cs) left = cs
      m = int(r * left); if (m < 2 * fl) m = 2 * fl; if (m < 1000) m = 1000; print m }')
  done
  t=$(date +%s.%N)
  tail -c 400 "$(ls -t "$dir"/*.log | head -1)"
  # The first chunk's command line; js-fast runs one per stream, alike but
  # for the stream and client count.
  echo "nats_cmd=$(head -1 "$dir/cmds")"
  echo "nats_cmd_count=$(wc -l < "$dir/cmds")"
  python3 - "$dir" "$kind" "$size" "$clients" "$window" "$flow" "$nstreams" "$s" "$t" "$i" <<'PY'
import csv, glob, json, statistics, sys
d, kind, size, clients, window, flow, nstreams, s, t, chunks = sys.argv[1:]
size = int(size); elapsed = float(t) - float(s)
total, p50, p99 = 0, [], []
for f in glob.glob(d + "/*.csv"):
    for row in csv.reader(open(f)):
        if not row or row[0].startswith("#"):
            continue
        total += int(row[3])
        if len(row) >= 13:
            p50.append(float(row[10])); p99.append(float(row[12]))
doc = {
    "system": "nats", "scenario": "nats-" + kind, "payload_bytes": size, "clients": int(clients),
    "window": int(window), "flow": int(flow), "streams": int(nstreams), "chunks": int(chunks),
    "records": total // size, "bytes": total, "elapsed_s": round(elapsed, 3),
    "throughput_mb_s": total / elapsed / 1e6 if elapsed > 0 else None,
    "throughput_msg_s": total / size / elapsed if elapsed > 0 else None,
}
if p50:
    # nats bench's latency is per operation: one async window, one sync
    # publish, one fast-batch flow ack. Medians over clients and chunks.
    doc["ack_latency_us"] = {"p50": statistics.median(p50), "p99": statistics.median(p99)}
print("NATS_BENCH_JSON " + json.dumps(doc, separators=(",", ":")))
PY
  echo "gen.start=$s"
  echo "gen.end=$t"
}

# uninstall: stop and remove NATS, restore TCP sysctls, start Felix again.
cmd_uninstall() {
  systemctl disable --now nats 2>/dev/null || true
  rm -f /etc/systemd/system/nats.service
  systemctl daemon-reload
  rm -rf "$STORE" "$CELLS"
  cmd_untune
  if systemctl cat felix-broker >/dev/null 2>&1; then
    systemctl reset-failed felix-broker 2>/dev/null || true
    systemctl start felix-broker
    echo "felix_broker=$(systemctl is-active felix-broker || true)"
  fi
}

sub="${1:?usage: nats-agent <command> [args]}"
shift
case "$sub" in
  install-server) cmd_install_server "$@" ;;
  install-cli) cmd_install_cli "$@" ;;
  tls-init) cmd_tls_init "$@" ;;
  ca-get) cmd_ca_get ;;
  ca-put) cmd_ca_put "$@" ;;
  tune) cmd_tune ;;
  untune) cmd_untune ;;
  reset) cmd_reset "$@" ;;
  streams) cmd_streams "$@" ;;
  hostfacts) cmd_hostfacts ;;
  snapshot) cmd_snapshot "$@" ;;
  bench) cmd_bench "$@" ;;
  uninstall) cmd_uninstall ;;
  *) echo "!! unknown command $sub" >&2; exit 2 ;;
esac
