# PostgreSQL Backend Handoff (Scale + Fast Reads)

## Goals
- Keep ingestion stable for millions of rows.
- Keep frontend reads fast for product offers by city/store.
- Avoid expensive full scans and avoid duplicating the full prices table.

## Current Runtime Tables
- `stores`: one row per `(chain_id, store_id)`.
- `products`: one row per `item_code`.
- `product_prices`: one row per `(product_id, store_id)` with `price` and optional `promo_price`.
- `promotions`: promotion documents as source data.
- `processed_files`: ingestion idempotency and observability.
- `maintenance_state`: throttle periodic maintenance cadence.
- `promotion_store_items`: normalized promo rows per `(product_id, store_id)` for fast joins.

## Identity and Uniqueness
- Store business key is `(chain_id, store_id)`.
- `store_id` alone is not globally unique.
- Product business key is `item_code`.

## Read-Oriented SQL Helpers

### 1) Offers for one product code
Use `get_offers_for_item_code(...)`.

Input:
- `p_item_code`: required.
- `p_city`: optional city prefix filter.
- `p_chain_id`: optional chain filter.
- `p_limit` / `p_offset`: pagination.

Output columns include:
- item metadata,
- chain/store identity,
- `price`, `promo_price`,
- `effective_price = LEAST(price, COALESCE(promo_price, price))`.

Ordering:
- Best offer first (`effective_price ASC`), then newest update.

Example:
```sql
SELECT *
FROM get_offers_for_item_code('7290000000000', NULL, NULL, 100, 0);
```

### 2) Product search
Use `search_products_fts(...)`.

Input:
- `p_query`: full-text query string.
- `p_limit` / `p_offset`: pagination.

Example:
```sql
SELECT *
FROM search_products_fts('milk 3%', 30, 0);
```

### 3) One-call frontend response (search + city offers)
Use `get_city_offers_for_search(...)`.

Input:
- `p_query`: user text query.
- `p_city`: city prefix filter.
- `p_chain_id`: optional chain filter.
- `p_limit_products`: usually `10`.

Returns flattened rows grouped by `product_rank`:
- top N matched products,
- for each product: all matching stores in city,
- with computed `effective_price`.

Example:
```sql
SELECT *
FROM get_city_offers_for_search('חלב', 'תל אביב', NULL, 10, 0);
```

## Index Strategy

### Product prices
- PK `(product_id, store_id)` for idempotent upsert and joins.
- `idx_pp_product` for product -> all offers.
- `idx_pp_store` and `idx_pp_store_product` for store drill-down and joins.
- `idx_pp_product_updated_at` for recency sorting.
- `idx_pp_product_effective_price` for ranking where direct promo exists in price feed.

### Stores
- Unique `(chain_id, store_id)` for business identity.
- `idx_stores_city` and `idx_stores_city_chain_id` for city/chain filtering.

### Products
- Unique `item_code` for direct product lookup.
- `idx_products_name_mfr_fts` for scalable text search on product and manufacturer names.

### Promotion store items
- `idx_psi_product_store` and `idx_psi_store_product` for fast offer joins.
- `idx_psi_chain_end_date` for active promo filtering.

## Recommended API Read Pattern
1. Use `get_city_offers_for_search` for the primary screen (10 products + city offers).
2. Use `get_offers_for_item_code` for product-detail drilldown.
3. Keep city/chain filters in SQL (not in application loops).
4. Keep pagination server-side (`limit`, `offset`).

## Performance Notes
- Keep transactions short during ingestion.
- Avoid creating many overlapping indexes; each extra index slows writes.
- Use periodic `ANALYZE` after heavy imports (already throttled by `maintenance_state`).
- For endpoints returning many products, prefer keyset pagination over deep `OFFSET`.
- Refresh `promotion_store_items` per chain after promo ingestion (`refresh_promotion_store_items(chain_id)`).

## Minimal Backend Contract
- Frontend gets all offers per product for selected city/chain.
- Frontend highlights best offer using `effective_price`.
- Backend remains source of truth for filtering and ordering.
