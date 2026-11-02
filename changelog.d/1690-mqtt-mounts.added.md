- A durable stream can be mounted at an MQTT topic prefix (#1690): create it
  with `"mount": "plant/"`, or set, change or clear the mount with a `PATCH`
  (`null` clears it). A tenant's mounts may not overlap, in any namespace; the
  control plane refuses an overlap with `409` inside the write, in every store,
  so racing requests cannot both succeed, and refuses a mount on a stream that
  is not durable with `400`. Postgres migration 0040 adds `streams.mount`, and
  under Raft a mount needs metadata level 24 on every member. Brokers receive
  the mount with the stream and resolve each MQTT `PUBLISH` topic to its stream
  and shard by longest prefix. A publish to a topic under no mount closes a
  3.1.1 connection and gets reason 0x90 under 5.0, counted in
  `felix_mqtt_refused_total{reason="unmounted"}`; a publish to a mounted topic
  is still refused until the publish path lands. Streams with no mount are
  unchanged. See `docs/mqtt-compatibility.md`.
