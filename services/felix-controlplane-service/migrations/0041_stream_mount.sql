-- The MQTT topic prefix a stream is mounted at (#1690). A PUBLISH to a topic
-- that starts with it lands in the stream.
--
-- Nullable with no default: a stream with none is not reachable from MQTT, as
-- every stream was before.
--
-- Writers hold the streams feed lock and check for an overlapping prefix in
-- the same transaction, which is what keeps two mounts of a tenant from
-- overlapping. The index backs that up for the one case a constraint can
-- state, two streams with the same prefix.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS mount TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS streams_mount_unique
    ON streams (tenant_id, mount)
    WHERE mount IS NOT NULL;
