-- When a refresh token was rotated and which token replaced it, so a retry of
-- a refresh whose answer was lost can be told apart from a replay. Null on
-- tokens spent before this, and on tokens spent by a wrong secret.
ALTER TABLE refresh_tokens
    ADD COLUMN IF NOT EXISTS rotated_at_secs BIGINT,
    ADD COLUMN IF NOT EXISTS successor_id TEXT;
