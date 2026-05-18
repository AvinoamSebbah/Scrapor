-- One-shot migration for the product image source-of-truth column.
-- Safe to run once before the first sync_product_images.py backfill.

ALTER TABLE products
  ADD COLUMN IF NOT EXISTS has_image BOOLEAN DEFAULT NULL;

-- Existing rows are already NULL automatically when a new nullable column
-- with DEFAULT NULL is added. No table-wide UPDATE is needed here.
