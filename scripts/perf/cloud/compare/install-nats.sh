#!/usr/bin/env bash
# Stand up a 3-node NATS cluster with JetStream (file storage on the Premium data
# disk) on the broker VMs, and the `nats` CLI on the client VMs. rf=1 is a
# per-stream property (replicas=1), set at bench time. Each node routes to the
# other two; JetStream persists to /data/nats.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
: "${NATS_VER:=2.10.22}"
: "${NATSCLI_VER:=0.1.5}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP

IFS=',' read -ra BIPS <<<"${BROKER_IPS}"
IFS=',' read -ra CIPS <<<"${CLIENT_IPS}"
ROUTES=""
for ip in "${BIPS[@]}"; do ROUTES="${ROUTES}    nats-route://${ip}:6222\n"; done

for i in "${!BIPS[@]}"; do
  self="${BIPS[$i]}"
  echo ">> installing NATS on broker-${i} (${self})"
  run_on_str "$(broker_vm "${i}")" "$(cat <<REMOTE
#!/bin/sh
set -e
cd /opt
if [ ! -x /usr/local/bin/nats-server ]; then
  curl -1sLf -o nats.tgz https://github.com/nats-io/nats-server/releases/download/v${NATS_VER}/nats-server-v${NATS_VER}-linux-amd64.tar.gz
  tar xzf nats.tgz
  install -m0755 nats-server-v${NATS_VER}-linux-amd64/nats-server /usr/local/bin/nats-server
  rm -rf nats.tgz nats-server-v${NATS_VER}-linux-amd64
fi
systemctl stop nats 2>/dev/null || true
rm -rf /data/nats
mkdir -p /data/nats /etc/nats
cat > /etc/nats/nats.conf <<CONF
server_name: n${i}
listen: 0.0.0.0:4222
jetstream {
  store_dir: /data/nats
  max_memory_store: 2GB
  max_file_store: 100GB
}
cluster {
  name: cmpcluster
  listen: 0.0.0.0:6222
  routes: [
$(printf "${ROUTES}")
  ]
}
CONF
cat > /etc/systemd/system/nats.service <<UNIT
[Unit]
Description=NATS
After=network.target
[Service]
ExecStart=/usr/local/bin/nats-server -c /etc/nats/nats.conf
LimitNOFILE=1048576
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now nats
sleep 3
systemctl is-active nats
echo __RUNOK__
REMOTE
)" | tail -3
done

for i in "${!CIPS[@]}"; do
  echo ">> installing nats CLI on client-${i}"
  run_on_str "$(client_vm "${i}")" "$(cat <<REMOTE
#!/bin/sh
set -e
cd /opt
if [ ! -x /usr/local/bin/nats ]; then
  curl -1sLf -o natscli.zip https://github.com/nats-io/natscli/releases/download/v${NATSCLI_VER}/nats-${NATSCLI_VER}-linux-amd64.zip
  apt-get install -y unzip >/dev/null 2>&1 || true
  unzip -o natscli.zip >/dev/null
  install -m0755 nats-${NATSCLI_VER}-linux-amd64/nats /usr/local/bin/nats
  rm -rf natscli.zip nats-${NATSCLI_VER}-linux-amd64
fi
/usr/local/bin/nats --version
echo __RUNOK__
REMOTE
)" | tail -2
done

echo ">> waiting for cluster to form"
sleep 8
run_on_str "$(client_vm 0)" "$(cat <<REMOTE
#!/bin/sh
nats --server ${BIPS[0]}:4222 server list 2>&1 | head -20 || true
echo __RUNOK__
REMOTE
)"
echo ">> NATS up. Bench with: SESSION=${SESSION} ${here}/bench-nats.sh"
