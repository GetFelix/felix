#!/usr/bin/env bash
# Stand up a 3-node Apache Kafka cluster in KRaft mode (no ZooKeeper) on the
# broker VMs, data on the Premium data disk, TLS on the client listener. Every
# node is a combined broker+controller; the controller and inter-broker
# listeners stay plaintext on the private VNet, and clients reach 9092 over SSL —
# the fairness requirement, so Kafka pays TLS the way Felix's QUIC does.
#
# Durability match: Kafka's default is OS-flush (acks=1 acks from the page cache,
# fsync happens in the background) — the same in-memory-acked behaviour as Felix's
# headline and Redpanda write_caching. The durable comparison sets
# log.flush.interval.messages=1 (fsync every message) to match Felix on_commit.
set -euo pipefail
: "${SESSION:?SESSION=<name>}"
: "${CERTDIR:?CERTDIR=<dir with ca.crt node.crt node.key>}"
: "${KAFKA_VER:=3.9.1}"
: "${KEYSTORE_PASS:=changeit}"
here="$(cd "$(dirname "$0")" && pwd)"
source "${here}/compare-lib.sh"
source "${here}/sessions/${SESSION}.env"
export GROUP

IFS=',' read -ra BIPS <<<"${BROKER_IPS}"
IFS=',' read -ra CIPS <<<"${CLIENT_IPS}"

# Build the broker keystore (PKCS12 with the node cert+key) once, locally.
if [ ! -f "${CERTDIR}/node.p12" ]; then
  openssl pkcs12 -export -in "${CERTDIR}/node.crt" -inkey "${CERTDIR}/node.key" \
    -out "${CERTDIR}/node.p12" -name kafka -passout "pass:${KEYSTORE_PASS}"
fi
P12_B64="$(base64 < "${CERTDIR}/node.p12" | tr -d '\n')"
CA_B64="$(base64 < "${CERTDIR}/ca.crt" | tr -d '\n')"

# A single cluster id, shared by every node's storage format.
CLUSTER_ID="$(openssl rand -base64 16 | tr '+/' '-_' | tr -d '=')"

# Controller quorum voters: id@ip:9093 for all three.
VOTERS=""
for i in "${!BIPS[@]}"; do VOTERS="${VOTERS}${i}@${BIPS[$i]}:9093,"; done
VOTERS="${VOTERS%,}"

# --- phase 1: install + configure + format every node (do NOT start) ---------
for i in "${!BIPS[@]}"; do
  self="${BIPS[$i]}"
  echo ">> configuring Kafka on broker-${i} (${self})"
  run_on_str "$(broker_vm "${i}")" "$(cat <<REMOTE
#!/bin/sh
set -e
export DEBIAN_FRONTEND=noninteractive
if [ ! -x /opt/kafka/bin/kafka-server-start.sh ]; then
  cd /opt
  curl -1sLf -o kafka.tgz https://archive.apache.org/dist/kafka/${KAFKA_VER}/kafka_2.13-${KAFKA_VER}.tgz
  tar xzf kafka.tgz && rm -f kafka.tgz
  ln -sfn /opt/kafka_2.13-${KAFKA_VER} /opt/kafka
fi
pkill -f kafka.Kafka 2>/dev/null || true
sleep 2
rm -rf /data/kafka && mkdir -p /data/kafka /etc/kafka/certs
echo '${P12_B64}' | base64 -d > /etc/kafka/certs/node.p12
echo '${CA_B64}'  | base64 -d > /etc/kafka/certs/ca.crt
cat > /etc/kafka/server.properties <<CONF
process.roles=broker,controller
node.id=${i}
controller.quorum.voters=${VOTERS}
listeners=CLIENT://${self}:9092,CONTROLLER://${self}:9093,INTERNAL://${self}:9094
advertised.listeners=CLIENT://${self}:9092,INTERNAL://${self}:9094
listener.security.protocol.map=CONTROLLER:PLAINTEXT,CLIENT:SSL,INTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
controller.listener.names=CONTROLLER
ssl.keystore.location=/etc/kafka/certs/node.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=${KEYSTORE_PASS}
ssl.key.password=${KEYSTORE_PASS}
ssl.client.auth=none
log.dirs=/data/kafka
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
CONF
/opt/kafka/bin/kafka-storage.sh format -t ${CLUSTER_ID} -c /etc/kafka/server.properties --ignore-formatted
cat > /etc/systemd/system/kafka.service <<UNIT
[Unit]
Description=Apache Kafka (KRaft)
After=network.target
[Service]
ExecStart=/opt/kafka/bin/kafka-server-start.sh /etc/kafka/server.properties
LimitNOFILE=1048576
Environment=KAFKA_HEAP_OPTS=-Xmx4g -Xms4g
Restart=on-failure
[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
echo "configured broker-${i}"
echo __RUNOK__
REMOTE
)" | tail -2
done

# --- phase 2: start every node (they form the KRaft quorum together) ---------
for i in "${!BIPS[@]}"; do
  echo ">> starting Kafka on broker-${i}"
  run_on_str "$(broker_vm "${i}")" 'systemctl reset-failed kafka 2>/dev/null || true; systemctl --no-block start kafka; echo started; echo __RUNOK__' | tail -1
done

echo ">> waiting for the cluster to come up"
sleep 20
run_on_str "$(broker_vm 0)" "$(cat <<REMOTE
#!/bin/sh
echo 'security.protocol=SSL' > /tmp/probe.properties
echo 'ssl.truststore.type=PEM' >> /tmp/probe.properties
echo 'ssl.truststore.location=/etc/kafka/certs/ca.crt' >> /tmp/probe.properties
/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${BIPS[0]}:9092 --command-config /tmp/probe.properties 2>&1 | grep -c id: || true
echo __RUNOK__
REMOTE
)" | sed -n '/stdout/,/__RUNOK__/p' | head -4
echo ">> Kafka up. Bench with: SESSION=${SESSION} SYSTEM=kafka TLS=1 bash ${here}/bench-kafka-api.sh"
