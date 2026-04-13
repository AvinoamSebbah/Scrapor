-- Migration: יצירת טבלת רשימות קניות משותפות
-- Run this script against the production database to apply the schema change.

CREATE TABLE IF NOT EXISTS shopping_lists (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  code       CHAR(5)     NOT NULL,
  name       TEXT        NOT NULL DEFAULT 'רשימת קניות',
  items      JSONB       NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS shopping_lists_code_idx ON shopping_lists (code);

-- Auto-update updated_at on row change
CREATE OR REPLACE FUNCTION update_shopping_list_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS shopping_lists_updated_at ON shopping_lists;
CREATE TRIGGER shopping_lists_updated_at
  BEFORE UPDATE ON shopping_lists
  FOR EACH ROW EXECUTE FUNCTION update_shopping_list_timestamp();
