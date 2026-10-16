-- Whether a replica report's leader gave the shard up after a crash that may
-- have cost it records it acknowledged, and measured every follower since.
-- Placement then replaces it, or names it again when no follower holds more.
--
-- given_up_at is the marker, as by_round_at is for by_round (0029): the flag
-- is read only while it matches reported_at_millis, so what a member that
-- predates it leaves behind is never read as given up.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS given_up BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS given_up_at BIGINT;
