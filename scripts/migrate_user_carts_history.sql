-- Keep shopping-list cart history per user without expiring shared-list saves.
-- Safe to run multiple times.

ALTER TABLE user_carts
  ADD COLUMN IF NOT EXISTS list_code VARCHAR(20);

ALTER TABLE user_carts
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE user_carts
  ALTER COLUMN expires_at DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_user_carts_saved_at
  ON user_carts (saved_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_carts_user_list_code
  ON user_carts (user_id, list_code)
  WHERE list_code IS NOT NULL;
