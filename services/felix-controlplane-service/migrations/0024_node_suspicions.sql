-- The leaders each broker last said it cannot reach, from its heartbeat.
--
-- Soft state: placement reads a row only while it is recent, and nothing
-- here outlives the broker in any way that matters, so there is no foreign
-- key holding a node's deletion up.
CREATE TABLE IF NOT EXISTS node_suspicions (
    node_id TEXT PRIMARY KEY,
    incarnation BIGINT NOT NULL,
    suspects JSONB NOT NULL,
    reported_at_millis BIGINT NOT NULL
);
