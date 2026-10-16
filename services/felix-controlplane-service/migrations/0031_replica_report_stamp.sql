-- Which node made each replica report, and the incarnation each node it names
-- (the leader and its followers) was registered at when the report was taken
-- (a JSON object of node id to incarnation). Placement counts a follower only
-- from the report of the assignment's current leader, and one without a
-- storage identity only from a report taken at its current incarnation. NULL
-- is read as a report from before this, which placement reads as it did.
--
-- stamped_at is the marker, as follower_storage_at is in 0030: both columns
-- are read only while it matches reported_at_millis, so what a member that
-- predates them leaves behind is never read as its own.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS leader TEXT,
    ADD COLUMN IF NOT EXISTS node_incarnations JSONB,
    ADD COLUMN IF NOT EXISTS stamped_at BIGINT;
