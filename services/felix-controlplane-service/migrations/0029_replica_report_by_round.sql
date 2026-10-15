-- Whether the leader that made the report serves nothing on its lease, and
-- the node incarnation it said so from. Placement replaces a leader on its
-- followers' word only when it does. False is what every report held before
-- this said, and the safe reading of it.
--
-- by_round_at is the reported_at_millis of the report that set by_round. A
-- member that predates these columns updates the report without them, so
-- the flag it leaves behind belongs to an earlier report: it is read as true
-- only while by_round_at matches the row's reported_at_millis.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS by_round BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS leader_incarnation BIGINT,
    ADD COLUMN IF NOT EXISTS by_round_at BIGINT;
