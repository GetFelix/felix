---
title: "MQTT"
description: "Let MQTT devices connect to a Felix broker with a Felix token, and mount a stream at a topic prefix. Today the listener authenticates and keeps connections alive, and resolves topics to streams; publishing and subscribing are not built yet."
---

A Felix broker can accept MQTT connections from devices, over MQTT 3.1.1 and
5.0. The goal is to let a device publish to a Felix stream with a stock MQTT
client and read from it, with Felix's acknowledgements and durability behind
every `PUBACK`.

**Status: connections and mounts.** Today a device can connect with a Felix
token, ping and disconnect, and you can mount a stream at a topic prefix. Every
`PUBLISH` and `SUBSCRIBE` is still refused until the publish and subscribe paths
land. The plan for the rest is in the
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

The connection is accepted and the publish is refused. At QoS 0 an MQTT client
cannot be told no for one message, so the broker closes the connection; a 5.0
client gets a `DISCONNECT` with reason "not authorized" first. At QoS 1 and 2 a
5.0 client gets that reason on the acknowledgement.

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
records, and deleting the stream frees its prefix. The
[control plane API](/api/control-plane-api/) has the exact rules.

A publish to a topic under no mount is never dropped silently. A 3.1.1 client's
connection closes. A 5.0 client at QoS 1 gets a `PUBACK` with reason 0x90 (topic
name invalid) and keeps its connection; at QoS 0 it gets a `DISCONNECT` with the
same reason. Each is counted in `felix_mqtt_refused_total{reason="unmounted"}`.
A publish to a mounted topic is refused as before, because writing to the stream
is the next piece of work.

A connection with no credentials is refused unless the broker was started with
`FELIX_MQTT_ANONYMOUS_TENANT=<tenant>`, which is for development only.

A connection ends when its token expires or is revoked. A device is also closed
if it is silent for one and a half keepalive intervals, with the interval capped
at `FELIX_MQTT_KEEPALIVE_MAX_SECS`.

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
