-- Robust live shopping-list sync.
-- Safe to run multiple times.

ALTER TABLE shopping_lists
  ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS last_event_id BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ NULL,
  ADD COLUMN IF NOT EXISTS last_run_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_shopping_lists_closed_at
  ON shopping_lists (closed_at);

CREATE INDEX IF NOT EXISTS idx_shopping_lists_last_run_at
  ON shopping_lists (last_run_at DESC);
