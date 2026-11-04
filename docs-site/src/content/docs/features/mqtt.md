---
title: "MQTT"
description: "Let MQTT devices connect to a Felix broker with a Felix token, mount a stream at a topic prefix, and publish to it at QoS 0 and 1. Every PUBACK means the message is in the stream. Subscribing is not built yet."
---

A Felix broker can accept MQTT connections from devices, over MQTT 3.1.1 and
5.0. The goal is to let a device publish to a Felix stream with a stock MQTT
client and read from it, with Felix's acknowledgements and durability behind
every `PUBACK`.

**Status: connections, mounts and publishing.** Today a device can connect with
a Felix token, publish at QoS 0 and 1 to a stream mounted at a topic prefix, and
get a `PUBACK` once the message is in the stream. `SUBSCRIBE` is still refused
until the subscribe path lands. The plan for the rest is in the
[design](https://github.com/GetFelix/felix/blob/main/docs/mqtt-design.md).

## Turn it on

Set the address to listen on. The listener is off when it is unset, and a broker
with it off runs exactly the code it ran before.

```bash
FELIX_MQTT_LISTEN=0.0.0.0:8883 felix-broker
```

TLS is on by default, using the same certificate as the QUIC listeners. For a
local test without certificates, `FELIX_MQTT_TLS=false` serves plain TCP. Tokens
then cross the network in clear text, and the broker says so in its log.

## Connect a device

The username is the tenant id and the password is a Felix token, the same
convention as the [Kafka listener](/features/kafka/). With the broker started
as `FELIX_MQTT_LISTEN=127.0.0.1:1883 FELIX_MQTT_TLS=false`:

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -u my-tenant -P "$FELIX_TOKEN" -t sensors/a -m 21.5
```

With a stream mounted at `sensors/` (next section), the connection is accepted
and the message is written to the stream. With no mount the publish is refused;
see [Publish to a mounted stream](#publish-to-a-mounted-stream).

## Mount a stream at a topic prefix

MQTT clients publish to topics nobody created, such as `plant/7/line/3/temp`.
To say which stream those topics belong to, *mount* a durable stream at a topic
prefix. Every topic that starts with the prefix is then a message in that stream.
Mount it when you create the stream, or later with a `PATCH`, through the
control plane (`$CP` is its address, `http://localhost:8443` for a local one, and
`$TOKEN` a Felix token with `stream.manage` on the stream):

```bash
curl -sS -X POST "$CP/v1/tenants/my-tenant/namespaces/plant/streams" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
    "stream": "telemetry", "kind": "Stream", "shards": 4, "durable": true,
    "retention": { "max_age_seconds": null, "max_size_bytes": null },
    "consistency": "Leader", "delivery": "AtLeastOnce", "mount": "plant/" }'

# Move the mount to another prefix, then clear it
curl -sS -X PATCH "$CP/v1/tenants/my-tenant/namespaces/plant/streams/telemetry" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{ "mount": "factory/" }'
curl -sS -X PATCH "$CP/v1/tenants/my-tenant/namespaces/plant/streams/telemetry" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{ "mount": null }'
```

The prefix is a plain byte prefix: `plant/` catches `plant/7/temp` and not `plants/7`.
Only a durable stream can be mounted, and none of a tenant's mounts can overlap,
whichever namespace the streams are in. The control plane refuses a mount that
does with `409` and names the other stream. Changing or clearing a mount moves no
records, and deleting the stream frees its prefix. A topic under a mount must
still fit the listener's topic limits (`FELIX_MQTT_MAX_TOPIC_BYTES`, 1024 bytes
by default, and 16 levels), so a very long or deep mount leaves little room or
none. The
[control plane API](/api/control-plane-api/) has the exact rules.

A connection with no credentials is refused unless the broker was started with
`FELIX_MQTT_ANONYMOUS_TENANT=<tenant>`, which is for development only: such a
connection may publish to every stream mounted for that tenant.

A connection ends when its token expires or is revoked. A device is also closed
if it is silent for one and a half keepalive intervals, with the interval capped
at `FELIX_MQTT_KEEPALIVE_MAX_SECS`.

## Publish to a mounted stream

Publish with any MQTT client. This writes one message into the stream mounted at
`plant/`:

```bash
mosquitto_pub -h 127.0.0.1 -p 1883 -u my-tenant -P "$FELIX_TOKEN" \
  -t plant/7/line/3/temp -q 1 -m 21.5
```

At QoS 1 the broker answers with a `PUBACK` only once the message is written the
way the stream asks: on the leader under its fsync policy, and on a `Quorum`
stream on a majority of replicas as well. It is the same wait a Kafka producer
with `acks=all` makes, so a `PUBACK` is never early. `PUBACK`s leave in the order
the publishes arrived. QoS 0 is written with nothing sent back. QoS 2 is not
supported: the broker tells a 5.0 client `Maximum QoS` 1.

The message becomes a record: the topic name is its key, the payload is stored
as sent, and the MQTT fields (QoS, content type, response topic, correlation
data, expiry) and any 5.0 user properties are record headers. Every message on
one topic goes to one shard, so a topic keeps its order. The header names are in
[`docs/mqtt-compatibility.md`](https://github.com/GetFelix/felix/blob/main/docs/mqtt-compatibility.md#what-is-stored).

Storing headers needs the broker to accept record headers, which a single broker
does by default and a clustered one does once its fleet has finalized
`record_headers`. The first MQTT publish to a mounted stream is what moves its
logs to storage format v8, so mounting a stream is the step that opts in. See
[upgrades](/deployment/upgrades/).

A refused publish is always answered or counted. A publish to a topic under no
mount, or by a token without `stream.publish` on the mounted stream, is
refused: a 5.0 client at QoS 1
gets a `PUBACK` with reason 0x90 or 0x87 and keeps its connection, a 3.1.1
client's connection closes (3.1.1 cannot refuse one message). Each refusal is
counted in `felix_mqtt_refused_total{reason}`.

Three limits to know:

- **Any broker in the cluster.** A device can connect to any broker. A message
  for a shard another broker owns is forwarded there, and the `PUBACK` waits for
  the owner's answer. If the owner dies before answering, the connected broker
  sends the message again to whichever broker owns the shard next, and the new
  owner recognises it and does not write it twice. A device that connects
  without credentials (`FELIX_MQTT_ANONYMOUS_TENANT`) has no token for the owner
  to check, so it can publish only to shards its own broker owns. Anything else
  is refused with `not_owner`: a 5.0 client at QoS 1 gets `PUBACK` 0x83, a
  3.1.1 client's connection closes, and a QoS 0 message is dropped and counted.
- **Unknown outcome.** If a majority does not answer in time, or the owner
  cannot be reached again before the forward budget runs out, the broker cannot
  say whether the message was written. It
  sends neither a `PUBACK` nor an error reason, and closes the connection. Most
  clients drop an unacknowledged message when a clean session ends, so publish
  it again from the application after reconnecting; the stream may then hold it
  twice.
- **No retained messages or topic aliases.** A retained publish is refused with
  reason 0x9A (3.1.1: the connection closes), and no topic alias is accepted.

`FELIX_MQTT_MAX_INFLIGHT` (default 64) caps the QoS 1 publishes a device may have
waiting for a `PUBACK`. A 5.0 client is told it as `Receive Maximum` and is
disconnected with reason 0x93 if it goes past it; a 3.1.1 client is slowed by TCP
instead. Publishes also count against the tenant's publish quota: over quota,
the broker stops reading that connection for the delay, at most five seconds.

## Browsers and WebSockets

A browser cannot open a raw TCP socket, so web apps and dashboards connect over
WebSockets instead. Set a second address next to the first:

```bash
FELIX_MQTT_LISTEN=0.0.0.0:8883 \
FELIX_MQTT_WS_LISTEN=0.0.0.0:8884 \
FELIX_MQTT_WS_ALLOWED_ORIGINS=https://app.example.com \
felix-broker
```

The address serves `wss` when TLS is on and `ws` when `FELIX_MQTT_TLS=false`.
A client asks for the `mqtt` subprotocol, as MQTT.js does, on any path:

```js
import mqtt from "mqtt";

const client = mqtt.connect("wss://broker.example.com:8884/mqtt", {
  username: "my-tenant",
  password: token,
});
client.on("connect", () => console.log("connected"));
client.on("error", (err) => console.error(err.message));
```

The connection behaves exactly like one on the TCP listener: the same token
check, packet limits, keepalive, and caps. Connections on both sockets count
toward one `FELIX_MQTT_MAX_CONNECTIONS`, one per-address limit and one connect
rate.

A browser always sends an `Origin` header, and the broker refuses any request
that has one unless `FELIX_MQTT_WS_ALLOWED_ORIGINS` lists it (`*` allows every
origin). Without the list, a browser app gets `403` and the refusal is counted
as `ws_origin`. Most command-line tools and device clients send none and connect either
way, but some libraries do send one (the Python `paho-mqtt` client does): list
the origin it sends or use `*`. Write each origin the way a browser does,
`scheme://host[:port]` with no path, trailing slash or default port; the broker
refuses to start on an entry that could never match. The `Origin` check keeps other websites from using a visitor's browser to
reach a broker on a private network; it is not authentication, the token is.

The upgrade request is read with a size limit (`FELIX_MQTT_WS_MAX_HEADER_BYTES`)
and a time limit (`FELIX_MQTT_WS_UPGRADE_TIMEOUT_MS`), and a request that is
not a valid WebSocket upgrade for `mqtt` is answered with an error and closed.
An MQTT message may be split across WebSocket messages, or several may share
one; the broker reads the bytes as one stream, so the packet limits above apply
the same way. Text messages are refused, as are compressed ones (the broker
does not negotiate `permessage-deflate`).

## Protect the broker from the fleet

The listener runs on its own threads (`FELIX_MQTT_WORKER_THREADS`), so a storm
of devices reconnecting does not slow QUIC clients. Connections are capped for
the whole broker and for each source address, new connections are paced, and a
connection that does not send a valid `CONNECT` within ten seconds is closed.
With TLS the ten seconds start once the handshake is done, and the handshake has ten of its own.
The limits and their metrics are in the
[environment variable reference](/reference/environment-variables/) and
[observability](/features/observability/).

## Reference

[`docs/mqtt-compatibility.md`](https://github.com/GetFelix/felix/blob/main/docs/mqtt-compatibility.md)
lists every packet and what the broker answers.
