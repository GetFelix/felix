-- Each node a report names, with its node incarnation when the report was
-- accepted (a JSON object of node id to incarnation). Placement counts a
-- node from the report only while that is still its incarnation, and only a
-- stamped report outlives a dead leader. NULL is what every report held
-- before this said, and is read as unstamped.
--
-- follower_incarnations_at is the reported_at_millis of the report that set
-- the stamp, as by_round_at is for by_round (0029): a member that predates
-- these columns updates the report without them, and the stamp it leaves
-- behind is read only while the two match.
ALTER TABLE replica_reports
    ADD COLUMN IF NOT EXISTS follower_incarnations JSONB,
    ADD COLUMN IF NOT EXISTS follower_incarnations_at BIGINT;
