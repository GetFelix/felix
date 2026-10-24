-- A change to a stream's consistency or durability advances the placement
-- token (#1253). Placement decides from those how a shard fails over: on a
-- durable Quorum stream it may replace a leader its followers cannot reach,
-- on any other it waits out the leader's lease. A pass that read the stream
-- before the change and writes after it would act on the old answer, so the
-- token fences it. A trigger, so an instance that predates this migration is
-- held to it too.
CREATE OR REPLACE FUNCTION felix_fence_placement_on_stream_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE placement_lease SET token = token + 1 WHERE id = 1;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS streams_fence_placement ON streams;
CREATE TRIGGER streams_fence_placement
    AFTER UPDATE OF consistency, durable ON streams
    FOR EACH ROW
    WHEN (OLD.consistency IS DISTINCT FROM NEW.consistency
          OR OLD.durable IS DISTINCT FROM NEW.durable)
    EXECUTE FUNCTION felix_fence_placement_on_stream_change();
