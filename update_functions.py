"""Push only the RPC function definitions to PostgreSQL.

Safe to run at any time - uses CREATE OR REPLACE, never drops data tables.
Requires POSTGRESQL_URL or DATABASE_URL (SUPABASE_DATABASE_URL is accepted for backward compatibility).
"""
import os
import psycopg2

db_url = os.getenv('POSTGRESQL_URL') or os.getenv('DATABASE_URL') or os.getenv('SUPABASE_DATABASE_URL')

def update_functions():
  conn = None
  try:
    conn = psycopg2.connect(db_url, connect_timeout=15)
    conn.autocommit = True
    with conn.cursor() as cur:

        cur.execute("""
        CREATE OR REPLACE FUNCTION get_table_stats()
        RETURNS TABLE(table_name TEXT, approx_count BIGINT)
        LANGUAGE sql SECURITY DEFINER AS $$
          SELECT relname::TEXT, n_live_tup::BIGINT
          FROM pg_stat_user_tables
          WHERE relname IN ('stores','products','product_prices','product_search_stats','promotions','promotion_store_items','top_promotions_cache','processed_files')
          ORDER BY relname;
        $$;
        """)
        print("✅ get_table_stats")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS product_search_stats (
          product_id INT PRIMARY KEY,
          chain_count INT NOT NULL DEFAULT 0,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("✅ ensure product_search_stats table")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS top_promotions_cache (
          window_hours INT NOT NULL DEFAULT 24,
          scope_type VARCHAR NOT NULL,
          city VARCHAR NOT NULL,
          chain_id VARCHAR NOT NULL DEFAULT '',
          chain_name VARCHAR,
          store_id VARCHAR NOT NULL DEFAULT '',
          store_name VARCHAR,
          rank_position INT NOT NULL,
          item_code VARCHAR NOT NULL,
          item_name VARCHAR,
          manufacturer_name VARCHAR,
          unit_of_measure VARCHAR,
          unit_qty VARCHAR,
          b_is_weighted BOOLEAN DEFAULT false,
          price NUMERIC NOT NULL,
          promo_price NUMERIC NOT NULL,
          effective_price NUMERIC NOT NULL,
          discount_amount NUMERIC NOT NULL,
          discount_percent NUMERIC NOT NULL,
          smart_score NUMERIC NOT NULL,
          promotion_end_date DATE,
          updated_at TIMESTAMP,
          refreshed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (window_hours, scope_type, city, chain_id, store_id, rank_position)
        );
        """)
        print("✅ ensure top_promotions_cache table")

        cur.execute("""
        ALTER TABLE product_prices
          ADD COLUMN IF NOT EXISTS unit_of_measure VARCHAR,
          ADD COLUMN IF NOT EXISTS unit_qty VARCHAR,
          ADD COLUMN IF NOT EXISTS b_is_weighted BOOLEAN DEFAULT false;
        """)
        print("✅ ensure product_prices unit columns")

        cur.execute("""
        ALTER TABLE top_promotions_cache
          ADD COLUMN IF NOT EXISTS unit_of_measure VARCHAR,
          ADD COLUMN IF NOT EXISTS unit_qty VARCHAR,
          ADD COLUMN IF NOT EXISTS b_is_weighted BOOLEAN DEFAULT false;
        """)
        print("✅ ensure top_promotions_cache unit columns")

        cur.execute("""
        CREATE OR REPLACE FUNCTION upsert_product_prices(p_records JSONB)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
          INSERT INTO product_prices (
            product_id, store_id, price, promo_price, unit_of_measure, unit_qty, b_is_weighted, updated_at
          )
          SELECT
            (r->>'product_id')::INT,
            (r->>'store_id')::INT,
            CASE
              WHEN COALESCE(r->>'price', '') ~ '^-?\\d+(\\.\\d+)?$' THEN (r->>'price')::NUMERIC
              ELSE NULL
            END,
            CASE
              WHEN COALESCE(r->>'promo_price', '') ~ '^-?\\d+(\\.\\d+)?$' THEN (r->>'promo_price')::NUMERIC
              ELSE NULL
            END,
            NULLIF(BTRIM(r->>'unit_of_measure'), ''),
            NULLIF(BTRIM(r->>'unit_qty'), ''),
            CASE
              WHEN LOWER(COALESCE(NULLIF(BTRIM(r->>'b_is_weighted'), ''), 'false')) IN ('1', 'true', 't', 'yes', 'y')
              THEN TRUE
              ELSE FALSE
            END,
            COALESCE((r->>'updated_at')::TIMESTAMP, NOW())
          FROM jsonb_array_elements(p_records) AS r
          ON CONFLICT (product_id, store_id) DO UPDATE SET
            price = EXCLUDED.price,
            promo_price = EXCLUDED.promo_price,
            unit_of_measure = EXCLUDED.unit_of_measure,
            unit_qty = EXCLUDED.unit_qty,
            b_is_weighted = EXCLUDED.b_is_weighted,
            updated_at = NOW();
        END;
        $$;
        """)
        print("✅ upsert_product_prices")

        cur.execute("""
        CREATE OR REPLACE FUNCTION refresh_promotion_store_items(p_chain_id TEXT DEFAULT NULL)
        RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE
          affected_rows INTEGER := 0;
        BEGIN
          IF p_chain_id IS NULL OR p_chain_id = '' THEN
            DELETE FROM promotion_store_items;
          ELSE
            DELETE FROM promotion_store_items WHERE chain_id = p_chain_id;
          END IF;

          WITH promo_pairs AS (
            SELECT
              p.chain_id,
              p.promotion_id,
              p.promotion_end_date,
              sid_txt.sid::INT AS store_id,
              item.item_code,
              item.promo_price_num
            FROM promotions p
            JOIN LATERAL unnest(COALESCE(p.available_in_store_ids, '{}')) AS sid_txt(sid) ON TRUE
            JOIN LATERAL (
              SELECT
                COALESCE(q.obj->>'itemcode', q.obj->>'ItemCode') AS item_code,
                CASE
                  WHEN metrics.is_weighted THEN
                    COALESCE(
                      CASE
                        WHEN metrics.item_discounted_price_per_mida_num IS NOT NULL
                          AND metrics.item_discounted_price_per_mida_num > 0
                        THEN CASE
                          WHEN metrics.item_min_qty_num IS NOT NULL
                            AND metrics.item_min_qty_num > 0
                            AND metrics.item_min_qty_num < 1
                          THEN metrics.item_discounted_price_per_mida_num / metrics.item_min_qty_num
                          ELSE metrics.item_discounted_price_per_mida_num
                        END
                        ELSE NULL
                      END,
                      CASE
                        WHEN metrics.item_discounted_price_num IS NULL THEN NULL
                        WHEN metrics.item_discounted_price_num <= 0 THEN NULL
                        WHEN metrics.item_discounted_price_num < 1
                          AND metrics.promotion_discounted_price_num IS NOT NULL
                          AND metrics.promotion_discounted_price_num >= 1
                        THEN NULL
                        WHEN metrics.item_discounted_price_num < 1 THEN NULL
                        ELSE metrics.item_discounted_price_num
                      END,
                      CASE
                        WHEN metrics.promotion_discounted_price_num IS NULL THEN NULL
                        WHEN metrics.promotion_discounted_price_num <= 0 THEN NULL
                        WHEN metrics.promotion_discounted_price_num < 1 THEN NULL
                        ELSE metrics.promotion_discounted_price_num
                      END
                    )
                  ELSE
                    COALESCE(
                      CASE
                        WHEN metrics.item_discounted_price_num IS NULL THEN NULL
                        WHEN metrics.item_discounted_price_num <= 0 THEN NULL
                        WHEN metrics.item_discounted_price_num < 1
                          AND metrics.promotion_discounted_price_num IS NOT NULL
                          AND metrics.promotion_discounted_price_num >= 1
                        THEN NULL
                        WHEN metrics.item_min_qty_num IS NOT NULL
                          AND metrics.item_min_qty_num >= 2
                          AND metrics.item_min_qty_num = TRUNC(metrics.item_min_qty_num)
                        THEN metrics.item_discounted_price_num / metrics.item_min_qty_num
                        ELSE metrics.item_discounted_price_num
                      END,
                      CASE
                        WHEN metrics.promotion_discounted_price_num IS NULL THEN NULL
                        WHEN metrics.promotion_discounted_price_num <= 0 THEN NULL
                        WHEN metrics.item_min_qty_num IS NOT NULL
                          AND metrics.item_min_qty_num >= 2
                          AND metrics.item_min_qty_num = TRUNC(metrics.item_min_qty_num)
                        THEN metrics.promotion_discounted_price_num / metrics.item_min_qty_num
                        ELSE metrics.promotion_discounted_price_num
                      END
                    )
                END AS promo_price_num
              FROM (
                SELECT jsonb_path_query(COALESCE(p.items, '[]'::jsonb), '$.** ? (@.itemcode != null)') AS obj
                UNION ALL
                SELECT jsonb_path_query(COALESCE(p.items, '[]'::jsonb), '$.** ? (@.ItemCode != null)') AS obj
              ) AS q
              CROSS JOIN LATERAL (
                SELECT
                  parsed.item_discounted_price_num,
                  parsed.item_discounted_price_per_mida_num,
                  parsed.item_min_qty_num,
                  parsed.promotion_discounted_price_num,
                  (
                    LOWER(
                      COALESCE(
                        NULLIF(BTRIM(q.obj->>'bisweighted'), ''),
                        NULLIF(BTRIM(q.obj->>'bIsWeighted'), ''),
                        CASE WHEN p.is_weighted_promo THEN '1' ELSE '' END,
                        '0'
                      )
                    ) IN ('1', 'true', 't', 'yes', 'y')
                    OR (
                      parsed.item_min_qty_num IS NOT NULL
                      AND parsed.item_min_qty_num > 0
                      AND parsed.item_min_qty_num < 1
                    )
                  ) AS is_weighted
                FROM (
                  SELECT
                    CASE
                      WHEN COALESCE(
                        NULLIF(REPLACE(q.obj->>'discountedprice', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'DiscountedPrice', ',', '.'), '')
                      ) ~ '^\s*[-]?[0-9]+(\.[0-9]+)?\s*$'
                      THEN COALESCE(
                        NULLIF(REPLACE(q.obj->>'discountedprice', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'DiscountedPrice', ',', '.'), '')
                      )::NUMERIC
                      ELSE NULL
                    END AS item_discounted_price_num,
                    CASE
                      WHEN COALESCE(
                        NULLIF(REPLACE(q.obj->>'discountedpricepermida', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'DiscountedPricePerMida', ',', '.'), ''),
                        NULLIF(REPLACE(p.discounted_price_per_mida, ',', '.'), '')
                      ) ~ '^\s*[-]?[0-9]+(\.[0-9]+)?\s*$'
                      THEN COALESCE(
                        NULLIF(REPLACE(q.obj->>'discountedpricepermida', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'DiscountedPricePerMida', ',', '.'), ''),
                        NULLIF(REPLACE(p.discounted_price_per_mida, ',', '.'), '')
                      )::NUMERIC
                      ELSE NULL
                    END AS item_discounted_price_per_mida_num,
                    CASE
                      WHEN COALESCE(
                        NULLIF(REPLACE(q.obj->>'minqty', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'MinQty', ',', '.'), ''),
                        NULLIF(REPLACE(p.min_qty, ',', '.'), '')
                      ) ~ '^\s*[-]?[0-9]+(\.[0-9]+)?\s*$'
                      THEN COALESCE(
                        NULLIF(REPLACE(q.obj->>'minqty', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'MinQty', ',', '.'), ''),
                        NULLIF(REPLACE(p.min_qty, ',', '.'), '')
                      )::NUMERIC
                      ELSE NULL
                    END AS item_min_qty_num,
                    CASE
                      WHEN COALESCE(
                        NULLIF(REPLACE(p.discounted_price, ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'discountedprice', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'DiscountedPrice', ',', '.'), '')
                      ) ~ '^\s*[-]?[0-9]+(\.[0-9]+)?\s*$'
                      THEN COALESCE(
                        NULLIF(REPLACE(p.discounted_price, ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'discountedprice', ',', '.'), ''),
                        NULLIF(REPLACE(q.obj->>'DiscountedPrice', ',', '.'), '')
                      )::NUMERIC
                      ELSE NULL
                    END AS promotion_discounted_price_num
                ) parsed
              ) metrics
            ) AS item ON TRUE
            WHERE (p_chain_id IS NULL OR p_chain_id = '' OR p.chain_id = p_chain_id)
              AND item.item_code IS NOT NULL
              AND item.promo_price_num IS NOT NULL
              AND item.promo_price_num > 0
          )
          INSERT INTO promotion_store_items (
            chain_id, promotion_id, product_id, store_id, promo_price, promotion_end_date, updated_at
          )
          SELECT
            pp.chain_id,
            pp.promotion_id,
            pr.id AS product_id,
            pp.store_id,
            MIN(pp.promo_price_num) AS promo_price,
            pp.promotion_end_date,
            NOW()
          FROM promo_pairs pp
          JOIN products pr ON pr.item_code = pp.item_code
          GROUP BY pp.chain_id, pp.promotion_id, pr.id, pp.store_id, pp.promotion_end_date
          ON CONFLICT (chain_id, promotion_id, product_id, store_id) DO UPDATE SET
            promo_price = EXCLUDED.promo_price,
            promotion_end_date = EXCLUDED.promotion_end_date,
            updated_at = NOW();

          GET DIAGNOSTICS affected_rows = ROW_COUNT;
          RETURN affected_rows;
        END;
        $$;
        """)
        print("✅ refresh_promotion_store_items")

        cur.execute("""
        CREATE OR REPLACE FUNCTION refresh_product_search_stats(p_product_ids INT[] DEFAULT NULL)
        RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE
          affected_rows INTEGER := 0;
        BEGIN
          IF p_product_ids IS NULL OR array_length(p_product_ids, 1) IS NULL THEN
            TRUNCATE TABLE product_search_stats;

            INSERT INTO product_search_stats (product_id, chain_count, updated_at)
            SELECT
              pp.product_id,
              COUNT(DISTINCT s.chain_id)::INT AS chain_count,
              NOW()
            FROM product_prices pp
            JOIN stores s ON s.id = pp.store_id
            WHERE pp.price IS NOT NULL
            GROUP BY pp.product_id;
          ELSE
            DELETE FROM product_search_stats WHERE product_id = ANY(p_product_ids);

            INSERT INTO product_search_stats (product_id, chain_count, updated_at)
            SELECT
              pp.product_id,
              COUNT(DISTINCT s.chain_id)::INT AS chain_count,
              NOW()
            FROM product_prices pp
            JOIN stores s ON s.id = pp.store_id
            WHERE pp.price IS NOT NULL
              AND pp.product_id = ANY(p_product_ids)
            GROUP BY pp.product_id;
          END IF;

          GET DIAGNOSTICS affected_rows = ROW_COUNT;
          RETURN affected_rows;
        END;
        $$;
        """)
        print("✅ refresh_product_search_stats")

        cur.execute("""
        DROP FUNCTION IF EXISTS get_offers_for_item_code(TEXT, TEXT, TEXT, INT, INT);
        """)
        print("✅ drop get_offers_for_item_code (for return signature update)")

        cur.execute("""
        CREATE OR REPLACE FUNCTION get_offers_for_item_code(
          p_item_code TEXT,
          p_city TEXT DEFAULT NULL,
          p_chain_id TEXT DEFAULT NULL,
          p_limit INT DEFAULT 300,
          p_offset INT DEFAULT 0
        )
        RETURNS TABLE(
          item_code TEXT,
          item_name TEXT,
          manufacturer_name TEXT,
          chain_id TEXT,
          store_id TEXT,
          store_name TEXT,
          city TEXT,
          price NUMERIC,
          promo_price NUMERIC,
          effective_price NUMERIC,
          unit_of_measure TEXT,
          unit_qty TEXT,
          b_is_weighted BOOLEAN,
          updated_at TIMESTAMP
        )
        LANGUAGE sql STABLE SECURITY DEFINER AS $$
          SELECT
            p.item_code::TEXT,
            p.item_name::TEXT,
            p.manufacturer_name::TEXT,
            s.chain_id::TEXT,
            s.store_id::TEXT,
            s.store_name::TEXT,
            s.city::TEXT,
            pp.price,
            CASE
              WHEN COALESCE(pb.promo_price, pp.promo_price) IS NOT NULL
                AND COALESCE(pb.promo_price, pp.promo_price) < pp.price
                AND COALESCE(pb.promo_price, pp.promo_price) >= (pp.price * 0.05)
              THEN COALESCE(pb.promo_price, pp.promo_price)
              ELSE NULL
            END AS promo_price,
            LEAST(
              pp.price,
              COALESCE(pb.promo_price, pp.promo_price, pp.price)
            ) AS effective_price,
            pp.unit_of_measure::TEXT,
            pp.unit_qty::TEXT,
            COALESCE(pp.b_is_weighted, FALSE) AS b_is_weighted,
            pp.updated_at
          FROM products p
          JOIN product_prices pp ON pp.product_id = p.id
          JOIN stores s ON s.id = pp.store_id
          LEFT JOIN LATERAL (
            SELECT MIN(psi.promo_price) AS promo_price
            FROM promotion_store_items psi
            WHERE psi.product_id = pp.product_id
              AND psi.store_id = pp.store_id
              AND psi.promo_price IS NOT NULL
              AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
          ) pb ON TRUE
          WHERE p.item_code = p_item_code
            AND pp.price IS NOT NULL
            AND (p_city IS NULL OR p_city = '' OR s.city ILIKE p_city || '%')
            AND (p_chain_id IS NULL OR p_chain_id = '' OR s.chain_id = p_chain_id)
          ORDER BY LEAST(pp.price, COALESCE(pb.promo_price, pp.promo_price, pp.price)) ASC NULLS LAST, pp.updated_at DESC
          LIMIT GREATEST(COALESCE(p_limit, 300), 1)
          OFFSET GREATEST(COALESCE(p_offset, 0), 0);
        $$;
        """)
        print("✅ get_offers_for_item_code")

        cur.execute("""
        DROP FUNCTION IF EXISTS get_city_offers_for_search(TEXT, TEXT, TEXT, INT, INT);
        """)
        print("✅ drop get_city_offers_for_search (for return signature update)")

        cur.execute("""
        CREATE OR REPLACE FUNCTION get_city_offers_for_search(
          p_query TEXT,
          p_city TEXT,
          p_chain_id TEXT DEFAULT NULL,
          p_limit_products INT DEFAULT 10,
          p_offset_products INT DEFAULT 0
        )
        RETURNS TABLE(
          product_rank BIGINT,
          item_code TEXT,
          item_name TEXT,
          manufacturer_name TEXT,
          chain_id TEXT,
          store_id TEXT,
          store_name TEXT,
          city TEXT,
          price NUMERIC,
          promo_price NUMERIC,
          effective_price NUMERIC,
          unit_of_measure TEXT,
          unit_qty TEXT,
          b_is_weighted BOOLEAN,
          updated_at TIMESTAMP
        )
        LANGUAGE sql STABLE SECURITY DEFINER AS $$
          WITH ranked_products AS (
            SELECT
              p.id,
              p.item_code,
              p.item_name,
              p.manufacturer_name,
              ts_rank_cd(
                to_tsvector('simple', COALESCE(p.item_name,'') || ' ' || COALESCE(p.manufacturer_name,'')),
                plainto_tsquery('simple', p_query)
              ) AS rank_score,
              ROW_NUMBER() OVER (
                ORDER BY
                  ts_rank_cd(
                    to_tsvector('simple', COALESCE(p.item_name,'') || ' ' || COALESCE(p.manufacturer_name,'')),
                    plainto_tsquery('simple', p_query)
                  ) DESC,
                  p.item_code ASC
              ) AS product_rank
            FROM products p
            WHERE COALESCE(p_query, '') <> ''
              AND (
                to_tsvector('simple', COALESCE(p.item_name,'') || ' ' || COALESCE(p.manufacturer_name,''))
                  @@ plainto_tsquery('simple', p_query)
                OR p.item_name ILIKE '%' || p_query || '%'
                OR p.manufacturer_name ILIKE '%' || p_query || '%'
              )
            ORDER BY rank_score DESC, p.item_code ASC
            LIMIT GREATEST(COALESCE(p_limit_products, 10), 1)
            OFFSET GREATEST(COALESCE(p_offset_products, 0), 0)
          ),
          filtered_stores AS (
            SELECT s.id, s.chain_id, s.store_id, s.store_name, s.city
            FROM stores s
            WHERE (p_city IS NULL OR p_city = '' OR s.city ILIKE p_city || '%')
              AND (p_chain_id IS NULL OR p_chain_id = '' OR s.chain_id = p_chain_id)
          )
          SELECT
            rp.product_rank,
            rp.item_code::TEXT,
            rp.item_name::TEXT,
            rp.manufacturer_name::TEXT,
            fs.chain_id::TEXT,
            fs.store_id::TEXT,
            fs.store_name::TEXT,
            fs.city::TEXT,
            pp.price,
            CASE
              WHEN COALESCE(pb.promo_price, pp.promo_price) IS NOT NULL
                AND COALESCE(pb.promo_price, pp.promo_price) < pp.price
                AND COALESCE(pb.promo_price, pp.promo_price) >= (pp.price * 0.05)
              THEN COALESCE(pb.promo_price, pp.promo_price)
              ELSE NULL
            END AS promo_price,
            LEAST(pp.price, COALESCE(pb.promo_price, pp.promo_price, pp.price)) AS effective_price,
            pp.unit_of_measure::TEXT,
            pp.unit_qty::TEXT,
            COALESCE(pp.b_is_weighted, FALSE) AS b_is_weighted,
            pp.updated_at
          FROM ranked_products rp
          JOIN product_prices pp ON pp.product_id = rp.id
          JOIN filtered_stores fs ON fs.id = pp.store_id
          LEFT JOIN LATERAL (
            SELECT MIN(psi.promo_price) AS promo_price
            FROM promotion_store_items psi
            WHERE psi.product_id = pp.product_id
              AND psi.store_id = pp.store_id
              AND psi.promo_price IS NOT NULL
              AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
          ) pb ON TRUE
          WHERE pp.price IS NOT NULL
          ORDER BY rp.product_rank ASC, effective_price ASC NULLS LAST, pp.updated_at DESC;
        $$;
        """)
        print("✅ get_city_offers_for_search")

        cur.execute("""
        CREATE OR REPLACE FUNCTION refresh_top_promotions_cache(
          p_window_hours INT DEFAULT 24,
          p_top_n INT DEFAULT 200
        )
        RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE
          v_window_hours INT := GREATEST(COALESCE(p_window_hours, 24), 1);
          v_top_n INT := LEAST(GREATEST(COALESCE(p_top_n, 200), 1), 200);
          affected_rows INTEGER := 0;
        BEGIN
          DELETE FROM top_promotions_cache WHERE window_hours = v_window_hours;

          scoped_store_promos AS (
            SELECT
              s.id AS store_db_id,
              s.city::TEXT AS city,
              s.chain_id::TEXT AS chain_id,
              s.chain_name::TEXT AS chain_name,
              s.store_id::TEXT AS store_id,
              s.store_name::TEXT AS store_name,
              p.id AS product_id,
              p.item_code::TEXT AS item_code,
              p.item_name::TEXT AS item_name,
              p.manufacturer_name::TEXT AS manufacturer_name,
              pp.unit_of_measure::TEXT AS unit_of_measure,
              pp.unit_qty::TEXT AS unit_qty,
              COALESCE(pp.b_is_weighted, FALSE) AS b_is_weighted,
              pp.price,
              psi.promo_price,
              LEAST(pp.price, psi.promo_price) AS effective_price,
              GREATEST(pp.price - LEAST(pp.price, psi.promo_price), 0) AS discount_amount,
              CASE
                WHEN pp.price > 0 THEN ROUND(
                  (GREATEST(pp.price - LEAST(pp.price, psi.promo_price), 0) / pp.price) * 100.0,
                  2
                )
                ELSE 0::NUMERIC
              END AS discount_percent,
              ROUND(
                (
                  CASE
                    WHEN pp.price > 0 THEN (GREATEST(pp.price - LEAST(pp.price, psi.promo_price), 0) / pp.price) * 100.0
                    ELSE 0
                  END
                ) * 0.40
                + (LEAST(GREATEST(pp.price - LEAST(pp.price, psi.promo_price), 0), 80) * 0.60),
                2
              ) AS smart_score,
              psi.promotion_end_date,
              psi.updated_at,
              ROW_NUMBER() OVER (
                PARTITION BY s.id
                ORDER BY 
                  ROUND(
                    (CASE WHEN pp.price > 0 THEN (GREATEST(pp.price - LEAST(pp.price, psi.promo_price), 0) / pp.price) * 100.0 ELSE 0 END) * 0.40
                    + (LEAST(GREATEST(pp.price - LEAST(pp.price, psi.promo_price), 0), 80) * 0.60), 2
                  ) DESC NULLS LAST,
                  psi.promo_price ASC NULLS LAST,
                  p.item_code ASC
              ) AS store_rank
            FROM promotion_store_items psi
            JOIN stores s ON s.id = psi.store_id
            JOIN product_prices pp ON pp.product_id = psi.product_id AND pp.store_id = psi.store_id
            JOIN products p ON p.id = psi.product_id
            WHERE COALESCE(s.city, '') <> ''
              AND psi.promo_price IS NOT NULL
              AND psi.promo_price > 0
              AND pp.price IS NOT NULL
              AND pp.price > 0
              AND psi.promo_price < pp.price
              AND psi.promo_price >= (pp.price * 0.05)
              AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
              AND (v_window_hours <= 0 OR psi.updated_at >= NOW() - make_interval(hours => v_window_hours))
              AND p.item_code IS NOT NULL
              AND p.item_code ~ '^[0-9]{8,14}$'
              AND COALESCE(BTRIM(p.item_name), '') <> ''
              AND p.item_name NOT ILIKE '%משלוח%'
          ),
          scored_raw AS (
            SELECT *
            FROM scoped_store_promos
            WHERE store_rank <= 250
          ),
          deduped AS (

            SELECT
              sr.*,
              ROW_NUMBER() OVER (
                PARTITION BY sr.city, sr.chain_id, sr.store_id, sr.item_code
                ORDER BY sr.promo_price ASC NULLS LAST, sr.updated_at DESC NULLS LAST
              ) AS dedupe_rank
            FROM scored_raw sr
          ),
          city_ranked AS (
            SELECT
              'city'::TEXT AS scope_type,
              d.city,
              d.chain_id,
              d.chain_name,
              d.store_id,
              d.store_name,
              ROW_NUMBER() OVER (
                PARTITION BY d.city
                ORDER BY d.smart_score DESC NULLS LAST, d.discount_percent DESC NULLS LAST, d.discount_amount DESC NULLS LAST, d.updated_at DESC NULLS LAST, d.item_code ASC
              ) AS rank_position,
              d.item_code,
              d.item_name,
              d.manufacturer_name,
              d.unit_of_measure,
              d.unit_qty,
              d.b_is_weighted,
              d.price,
              d.promo_price,
              d.effective_price,
              d.discount_amount,
              d.discount_percent,
              d.smart_score,
              d.promotion_end_date,
              d.updated_at
            FROM deduped d
            WHERE d.dedupe_rank = 1
          ),
          chain_ranked AS (
            SELECT
              'chain'::TEXT AS scope_type,
              d.city,
              d.chain_id,
              d.chain_name,
              ''::TEXT AS store_id,
              NULL::TEXT AS store_name,
              ROW_NUMBER() OVER (
                PARTITION BY d.city, d.chain_id
                ORDER BY d.smart_score DESC NULLS LAST, d.discount_percent DESC NULLS LAST, d.discount_amount DESC NULLS LAST, d.updated_at DESC NULLS LAST, d.item_code ASC
              ) AS rank_position,
              d.item_code,
              d.item_name,
              d.manufacturer_name,
              d.unit_of_measure,
              d.unit_qty,
              d.b_is_weighted,
              d.price,
              d.promo_price,
              d.effective_price,
              d.discount_amount,
              d.discount_percent,
              d.smart_score,
              d.promotion_end_date,
              d.updated_at
            FROM deduped d
            WHERE d.dedupe_rank = 1
          ),
          store_ranked AS (
            SELECT
              'store'::TEXT AS scope_type,
              d.city,
              d.chain_id,
              d.chain_name,
              d.store_id,
              d.store_name,
              ROW_NUMBER() OVER (
                PARTITION BY d.city, d.chain_id, d.store_id
                ORDER BY d.smart_score DESC NULLS LAST, d.discount_percent DESC NULLS LAST, d.discount_amount DESC NULLS LAST, d.updated_at DESC NULLS LAST, d.item_code ASC
              ) AS rank_position,
              d.item_code,
              d.item_name,
              d.manufacturer_name,
              d.unit_of_measure,
              d.unit_qty,
              d.b_is_weighted,
              d.price,
              d.promo_price,
              d.effective_price,
              d.discount_amount,
              d.discount_percent,
              d.smart_score,
              d.promotion_end_date,
              d.updated_at
            FROM deduped d
            WHERE d.dedupe_rank = 1
          )
          INSERT INTO top_promotions_cache (
            window_hours,
            scope_type,
            city,
            chain_id,
            chain_name,
            store_id,
            store_name,
            rank_position,
            item_code,
            item_name,
            manufacturer_name,
            unit_of_measure,
            unit_qty,
            b_is_weighted,
            price,
            promo_price,
            effective_price,
            discount_amount,
            discount_percent,
            smart_score,
            promotion_end_date,
            updated_at,
            refreshed_at
          )
          SELECT
            v_window_hours,
            q.scope_type,
            q.city,
            q.chain_id,
            q.chain_name,
            q.store_id,
            q.store_name,
            q.rank_position,
            q.item_code,
            q.item_name,
            q.manufacturer_name,
            q.unit_of_measure,
            q.unit_qty,
            q.b_is_weighted,
            q.price,
            q.promo_price,
            q.effective_price,
            q.discount_amount,
            q.discount_percent,
            q.smart_score,
            q.promotion_end_date,
            q.updated_at,
            NOW()
          FROM (
            SELECT * FROM city_ranked WHERE rank_position <= v_top_n
            UNION ALL
            SELECT * FROM chain_ranked WHERE rank_position <= v_top_n
            UNION ALL
            SELECT * FROM store_ranked WHERE rank_position <= v_top_n
          ) q;

          GET DIAGNOSTICS affected_rows = ROW_COUNT;
          RETURN affected_rows;
        END;
        $$;
        """)
        print("✅ refresh_top_promotions_cache")

        cur.execute("""
        DROP FUNCTION IF EXISTS get_top_city_promotions(TEXT, TEXT, TEXT, INT, INT, INT);
        DROP FUNCTION IF EXISTS get_top_city_promotions(TEXT, TEXT, TEXT, INT, INT, INT, TEXT);
        """)
        print("✅ drop get_top_city_promotions (for return signature update)")

        cur.execute("""
        CREATE OR REPLACE FUNCTION get_top_city_promotions(
          p_city TEXT,
          p_chain_id TEXT DEFAULT NULL,
          p_store_id TEXT DEFAULT NULL,
          p_window_hours INT DEFAULT 24,
          p_limit INT DEFAULT 50,
          p_offset INT DEFAULT 0,
          p_order_by TEXT DEFAULT 'score'
        )
        RETURNS TABLE(
          item_code TEXT,
          item_name TEXT,
          manufacturer_name TEXT,
          chain_id TEXT,
          chain_name TEXT,
          store_id TEXT,
          store_name TEXT,
          city TEXT,
          unit_of_measure TEXT,
          unit_qty TEXT,
          b_is_weighted BOOLEAN,
          price NUMERIC,
          promo_price NUMERIC,
          effective_price NUMERIC,
          discount_amount NUMERIC,
          discount_percent NUMERIC,
          smart_score NUMERIC,
          promotion_end_date DATE,
          updated_at TIMESTAMP
        )
        LANGUAGE plpgsql STABLE SECURITY DEFINER AS $$
        DECLARE
          v_scope TEXT;
          v_chain_id TEXT := COALESCE(p_chain_id, '');
          v_store_id TEXT := COALESCE(p_store_id, '');
          v_window_hours INT := GREATEST(COALESCE(p_window_hours, 24), 1);
          v_limit INT := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 50);
          v_offset INT := GREATEST(COALESCE(p_offset, 0), 0);
        BEGIN
          v_scope := CASE
            WHEN v_store_id <> '' THEN 'store'
            WHEN v_chain_id <> '' THEN 'chain'
            ELSE 'city'
          END;

          RETURN QUERY
          SELECT
            c.item_code::TEXT,
            c.item_name::TEXT,
            c.manufacturer_name::TEXT,
            c.chain_id::TEXT,
            c.chain_name::TEXT,
            c.store_id::TEXT,
            c.store_name::TEXT,
            c.city::TEXT,
            c.unit_of_measure::TEXT,
            c.unit_qty::TEXT,
            COALESCE(c.b_is_weighted, FALSE) AS b_is_weighted,
            c.price,
            c.promo_price,
            c.effective_price,
            c.discount_amount,
            c.discount_percent,
            c.smart_score,
            c.promotion_end_date,
            c.updated_at
          FROM top_promotions_cache c
          WHERE c.window_hours = v_window_hours
            AND c.scope_type = v_scope
            AND (p_city IS NULL OR p_city = '' OR c.city ILIKE p_city || '%')
            AND (v_scope <> 'chain' OR c.chain_id = v_chain_id)
            AND (v_scope <> 'store' OR c.store_id = v_store_id)
            AND (v_scope <> 'store' OR v_chain_id = '' OR c.chain_id = v_chain_id)
          ORDER BY
            CASE WHEN p_order_by = 'percent' THEN c.discount_percent END DESC NULLS LAST,
            CASE WHEN p_order_by = 'savings' THEN c.discount_amount END DESC NULLS LAST,
            c.rank_position ASC
          LIMIT v_limit
          OFFSET v_offset;
        END;
        $$;

        """)
        print("✅ get_top_city_promotions")

        cur.execute("""
        DROP FUNCTION IF EXISTS search_products_fts(TEXT, INT, INT);
        """)

        cur.execute("""
        CREATE OR REPLACE FUNCTION search_products_fts(
          p_query TEXT,
          p_limit INT DEFAULT 50,
          p_offset INT DEFAULT 0
        )
        RETURNS TABLE(
          item_code TEXT,
          item_name TEXT,
          manufacturer_name TEXT,
          rank REAL,
          chain_count INTEGER
        )
        LANGUAGE sql STABLE SECURITY DEFINER AS $$
          WITH q AS (
            SELECT plainto_tsquery('simple', p_query) AS tsq
          ),
          matched AS (
            SELECT
              p.id,
              p.item_code,
              p.item_name,
              p.manufacturer_name,
              ts_rank_cd(
                to_tsvector('simple', COALESCE(p.item_name,'') || ' ' || COALESCE(p.manufacturer_name,'')),
                q.tsq
              ) AS text_rank
            FROM products p
            CROSS JOIN q
            WHERE COALESCE(p_query, '') <> ''
              AND (
                to_tsvector('simple', COALESCE(p.item_name,'') || ' ' || COALESCE(p.manufacturer_name,'')) @@ q.tsq
                OR p.item_name ILIKE '%' || p_query || '%'
                OR p.manufacturer_name ILIKE '%' || p_query || '%'
              )
          )
          SELECT
            m.item_code::TEXT,
            m.item_name::TEXT,
            m.manufacturer_name::TEXT,
            (
              m.text_rank
              + (LN(1 + COALESCE(pss.chain_count, 0)) * 0.20)
              + (LEAST(COALESCE(pss.chain_count, 0), 20) * 0.03)
            )::REAL AS rank,
            COALESCE(pss.chain_count, 0) AS chain_count
          FROM matched m
          LEFT JOIN product_search_stats pss ON pss.product_id = m.id
          ORDER BY COALESCE(pss.chain_count, 0) DESC, rank DESC, m.item_code ASC
          LIMIT GREATEST(COALESCE(p_limit, 50), 1)
          OFFSET GREATEST(COALESCE(p_offset, 0), 0);
        $$;
        """)
        print("✅ search_products_fts")

        cur.execute("""
        DROP FUNCTION IF EXISTS refresh_product_best_prices(INT[]);
        """)
        print("✅ drop legacy refresh_product_best_prices")

        cur.execute("""
        DROP FUNCTION IF EXISTS merge_prices(JSONB);
        """)
        print("✅ drop legacy merge_prices")

        cur.execute("""
        CREATE OR REPLACE FUNCTION merge_promotions(p_records JSONB)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
          INSERT INTO promotions (
            chain_id, promotion_id, sub_chain_id, bikoret_no, promotion_description,
            promotion_update_date, promotion_start_date, promotion_start_hour,
            promotion_end_date, promotion_end_hour, promotion_days, redemption_limit,
            reward_type, allow_multiple_discounts, is_weighted_promo, is_gift_item,
            min_no_of_item_offered, additional_is_coupon, additional_gift_count,
            additional_is_total, additional_is_active, additional_restrictions,
            remarks, min_qty, discounted_price, discounted_price_per_mida,
            weight_unit, club_id, items, store_promotions, available_in_store_ids,
            created_at, updated_at
          )
          SELECT
            r->>'chain_id', r->>'promotion_id', r->>'sub_chain_id', r->>'bikoret_no',
            r->>'promotion_description',
            CASE
              WHEN COALESCE(r->>'promotion_update_date', '') = '' THEN NULL
              WHEN (r->>'promotion_update_date') ~ '^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}(:\d{2})?)?$'
                THEN (r->>'promotion_update_date')::TIMESTAMP
              WHEN (r->>'promotion_update_date') ~ '^\d{2}/\d{2}/\d{4}$'
                THEN to_timestamp(r->>'promotion_update_date', 'DD/MM/YYYY')
              WHEN (r->>'promotion_update_date') ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
                THEN to_timestamp(r->>'promotion_update_date', 'DD/MM/YYYY HH24:MI')
              WHEN (r->>'promotion_update_date') ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
                THEN to_timestamp(r->>'promotion_update_date', 'DD/MM/YYYY HH24:MI:SS')
              ELSE NULL
            END,
            CASE
              WHEN COALESCE(r->>'promotion_start_date', '') = '' THEN NULL
              WHEN (r->>'promotion_start_date') ~ '^\d{4}-\d{2}-\d{2}$'
                THEN (r->>'promotion_start_date')::DATE
              WHEN (r->>'promotion_start_date') ~ '^\d{2}/\d{2}/\d{4}$'
                THEN to_date(r->>'promotion_start_date', 'DD/MM/YYYY')
              WHEN (r->>'promotion_start_date') ~ '^\d{2}-\d{2}-\d{4}$'
                THEN to_date(r->>'promotion_start_date', 'DD-MM-YYYY')
              ELSE NULL
            END,
            r->>'promotion_start_hour',
            CASE
              WHEN COALESCE(r->>'promotion_end_date', '') = '' THEN NULL
              WHEN (r->>'promotion_end_date') ~ '^\d{4}-\d{2}-\d{2}$'
                THEN (r->>'promotion_end_date')::DATE
              WHEN (r->>'promotion_end_date') ~ '^\d{2}/\d{2}/\d{4}$'
                THEN to_date(r->>'promotion_end_date', 'DD/MM/YYYY')
              WHEN (r->>'promotion_end_date') ~ '^\d{2}-\d{2}-\d{4}$'
                THEN to_date(r->>'promotion_end_date', 'DD-MM-YYYY')
              ELSE NULL
            END,
            r->>'promotion_end_hour', r->>'promotion_days', r->>'redemption_limit',
            r->>'reward_type', r->>'allow_multiple_discounts',
            COALESCE((r->>'is_weighted_promo')::boolean, false),
            r->>'is_gift_item', r->>'min_no_of_item_offered', r->>'additional_is_coupon',
            r->>'additional_gift_count', r->>'additional_is_total', r->>'additional_is_active',
            r->>'additional_restrictions', r->>'remarks', r->>'min_qty',
            r->>'discounted_price', r->>'discounted_price_per_mida', r->>'weight_unit',
            r->>'club_id',
            COALESCE(r->'items', '[]'::jsonb),
            COALESCE(r->'store_promotions', '{}'::jsonb),
            COALESCE(ARRAY(SELECT jsonb_array_elements_text(r->'available_in_store_ids')), '{}'),
            NOW(), NOW()
          FROM jsonb_array_elements(p_records) AS r
          ON CONFLICT (chain_id, promotion_id) DO UPDATE SET
            store_promotions       = promotions.store_promotions || EXCLUDED.store_promotions,
            available_in_store_ids = ARRAY(SELECT DISTINCT unnest(
              COALESCE(promotions.available_in_store_ids, '{}') || COALESCE(EXCLUDED.available_in_store_ids, '{}')
            )),
            sub_chain_id              = EXCLUDED.sub_chain_id,
            bikoret_no                = EXCLUDED.bikoret_no,
            promotion_description     = EXCLUDED.promotion_description,
            promotion_update_date     = EXCLUDED.promotion_update_date,
            promotion_start_date      = EXCLUDED.promotion_start_date,
            promotion_start_hour      = EXCLUDED.promotion_start_hour,
            promotion_end_date        = EXCLUDED.promotion_end_date,
            promotion_end_hour        = EXCLUDED.promotion_end_hour,
            promotion_days            = EXCLUDED.promotion_days,
            redemption_limit          = EXCLUDED.redemption_limit,
            reward_type               = EXCLUDED.reward_type,
            allow_multiple_discounts  = EXCLUDED.allow_multiple_discounts,
            is_weighted_promo         = EXCLUDED.is_weighted_promo,
            is_gift_item              = EXCLUDED.is_gift_item,
            min_no_of_item_offered    = EXCLUDED.min_no_of_item_offered,
            additional_is_coupon      = EXCLUDED.additional_is_coupon,
            additional_gift_count     = EXCLUDED.additional_gift_count,
            additional_is_total       = EXCLUDED.additional_is_total,
            additional_is_active      = EXCLUDED.additional_is_active,
            additional_restrictions   = EXCLUDED.additional_restrictions,
            remarks                   = EXCLUDED.remarks,
            min_qty                   = EXCLUDED.min_qty,
            discounted_price          = EXCLUDED.discounted_price,
            discounted_price_per_mida = EXCLUDED.discounted_price_per_mida,
            weight_unit               = EXCLUDED.weight_unit,
            club_id                   = EXCLUDED.club_id,
            items                     = EXCLUDED.items,
            updated_at                = NOW();
        END;
        $$;
        """)
        print("✅ merge_promotions")

        cur.execute("""
        CREATE OR REPLACE FUNCTION cleanup_expired_promotions(p_dry_run BOOLEAN DEFAULT FALSE)
        RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE
          deleted_count INTEGER;
        BEGIN
          IF p_dry_run THEN
            SELECT COUNT(*) INTO deleted_count
            FROM promotions
            WHERE promotion_end_date IS NOT NULL
              AND promotion_end_date < CURRENT_DATE;
          ELSE
            DELETE FROM promotions
            WHERE promotion_end_date IS NOT NULL
              AND promotion_end_date < CURRENT_DATE;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
          END IF;
          RETURN deleted_count;
        END;
        $$;
        """)
        print("✅ cleanup_expired_promotions")

        cur.execute("""
        CREATE OR REPLACE FUNCTION cleanup_stale_prices(
          p_cutoff TIMESTAMP WITH TIME ZONE,
          p_dry_run BOOLEAN DEFAULT FALSE
        )
        RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE
          deleted_count INTEGER;
        BEGIN
          IF p_dry_run THEN
            SELECT COUNT(*) INTO deleted_count FROM product_prices WHERE updated_at < p_cutoff;
          ELSE
            DELETE FROM product_prices WHERE updated_at < p_cutoff;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
          END IF;
          RETURN deleted_count;
        END;
        $$;
        """)
        print("✅ cleanup_stale_prices")

        cur.execute("""
        CREATE OR REPLACE FUNCTION cleanup_old_processed_files(
          p_cutoff TIMESTAMP WITH TIME ZONE,
          p_dry_run BOOLEAN DEFAULT FALSE
        )
        RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
        DECLARE
          deleted_count INTEGER;
        BEGIN
          IF p_dry_run THEN
            SELECT COUNT(*) INTO deleted_count FROM processed_files WHERE processed_at < p_cutoff;
          ELSE
            DELETE FROM processed_files WHERE processed_at < p_cutoff;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
          END IF;
          RETURN deleted_count;
        END;
        $$;
        """)
        print("✅ cleanup_old_processed_files")

    print("\nAll functions updated successfully.")
  finally:
    if conn is not None:
      conn.close()

if __name__ == '__main__':
    update_functions()
