import os
import psycopg2

db_url = os.getenv('POSTGRESQL_URL') or os.getenv('DATABASE_URL') or os.getenv('SUPABASE_DATABASE_URL')

def update_schema():
    if not db_url:
        raise ValueError("POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) must be set")

    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Base tables needed by the uploader (must exist on a brand-new DB).
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR NOT NULL,
            chain_name VARCHAR,
            last_update_date VARCHAR,
            last_update_time VARCHAR,
            store_id VARCHAR NOT NULL,
            bikoret_no VARCHAR,
            store_type VARCHAR,
            store_name VARCHAR,
            address VARCHAR,
            city VARCHAR,
            zip_code VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, store_id)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            item_code VARCHAR NOT NULL UNIQUE,
            item_name VARCHAR,
            manufacturer_name VARCHAR,
            manufacture_country VARCHAR,
            manufacturer_item_description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            file_name VARCHAR NOT NULL UNIQUE,
            file_path VARCHAR,
            file_hash VARCHAR,
            file_size BIGINT DEFAULT 0,
            file_type VARCHAR,
            record_count INTEGER DEFAULT 0,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            chain_id VARCHAR,
            store_id VARCHAR,
            store_name VARCHAR,
            chain_name VARCHAR
        );
        """)

        cur.execute("DROP TABLE IF EXISTS prices CASCADE;")
        cur.execute("DROP TABLE IF EXISTS promotions CASCADE;")
        
        cur.execute("""
        CREATE TABLE prices (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR NOT NULL,
            item_code VARCHAR NOT NULL,
            base_price VARCHAR,
            store_prices JSONB DEFAULT '{}'::jsonb,
            available_in_store_ids TEXT[] DEFAULT '{}',
            item_type VARCHAR,
            unit_qty VARCHAR,
            quantity VARCHAR,
            unit_of_measure VARCHAR,
            b_is_weighted BOOLEAN DEFAULT false,
            qty_in_package VARCHAR,
            price_update_date TIMESTAMP,
            allow_discount VARCHAR,
            item_status VARCHAR,
            item_id VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, item_code)
        );
        """)
        
        cur.execute("""
        CREATE INDEX idx_prices_chain_id ON prices(chain_id);
        CREATE INDEX idx_prices_item_code ON prices(item_code);
        """)
        
        cur.execute("""
        CREATE TABLE promotions (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR NOT NULL,
            promotion_id VARCHAR NOT NULL,
            sub_chain_id VARCHAR,
            bikoret_no VARCHAR,
            promotion_description VARCHAR,
            promotion_update_date TIMESTAMP,
            promotion_start_date DATE,
            promotion_start_hour VARCHAR,
            promotion_end_date DATE,
            promotion_end_hour VARCHAR,
            promotion_days VARCHAR,
            redemption_limit VARCHAR,
            reward_type VARCHAR,
            allow_multiple_discounts VARCHAR,
            is_weighted_promo BOOLEAN DEFAULT false,
            is_gift_item VARCHAR,
            min_no_of_item_offered VARCHAR,
            additional_is_coupon VARCHAR,
            additional_gift_count VARCHAR,
            additional_is_total VARCHAR,
            additional_is_active VARCHAR,
            additional_restrictions VARCHAR,
            remarks VARCHAR,
            min_qty VARCHAR,
            discounted_price VARCHAR,
            discounted_price_per_mida VARCHAR,
            weight_unit VARCHAR,
            club_id VARCHAR,
            items JSONB,
            store_promotions JSONB DEFAULT '{}'::jsonb,
            available_in_store_ids TEXT[] DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, promotion_id)
        );
        """)
        
        cur.execute("""
        CREATE INDEX idx_promotions_chain_id ON promotions(chain_id);
        CREATE INDEX idx_promotions_promotion_id ON promotions(promotion_id);
        """)
        
        cur.execute("CREATE INDEX IF NOT EXISTS idx_stores_chain_store ON stores(chain_id, store_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_products_item_code ON products(item_code);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_files_file_name ON processed_files(file_name);")

        # -------------------------------------------------------------------
        # RPC functions for atomic JSONB merge (called via supabase-py REST)
        # -------------------------------------------------------------------
        cur.execute("""
        CREATE OR REPLACE FUNCTION merge_prices(p_records JSONB)
        RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
        BEGIN
          INSERT INTO prices (
            chain_id, item_code, base_price, store_prices, available_in_store_ids,
            item_type, unit_qty, quantity, unit_of_measure, b_is_weighted,
            qty_in_package, price_update_date, allow_discount, item_status,
            item_id, created_at, updated_at
          )
          SELECT
            r->>'chain_id',
            r->>'item_code',
            r->>'base_price',
            COALESCE(r->'store_prices', '{}'::jsonb),
            COALESCE(ARRAY(SELECT jsonb_array_elements_text(r->'available_in_store_ids')), '{}'),
            r->>'item_type',
            r->>'unit_qty',
            r->>'quantity',
            r->>'unit_of_measure',
            COALESCE((r->>'b_is_weighted')::boolean, false),
            r->>'qty_in_package',
            CASE WHEN (r->>'price_update_date') IS NOT NULL AND (r->>'price_update_date') != ''
                 THEN (r->>'price_update_date')::TIMESTAMP ELSE NULL END,
            r->>'allow_discount',
            r->>'item_status',
            r->>'item_id',
            NOW(), NOW()
          FROM jsonb_array_elements(p_records) AS r
          ON CONFLICT (chain_id, item_code) DO UPDATE SET
            store_prices = prices.store_prices || EXCLUDED.store_prices,
            available_in_store_ids = ARRAY(SELECT DISTINCT unnest(
              COALESCE(prices.available_in_store_ids, '{}') || COALESCE(EXCLUDED.available_in_store_ids, '{}')
            )),
            base_price        = EXCLUDED.base_price,
            item_type         = EXCLUDED.item_type,
            unit_qty          = EXCLUDED.unit_qty,
            quantity          = EXCLUDED.quantity,
            unit_of_measure   = EXCLUDED.unit_of_measure,
            b_is_weighted     = EXCLUDED.b_is_weighted,
            qty_in_package    = EXCLUDED.qty_in_package,
            price_update_date = COALESCE(EXCLUDED.price_update_date, prices.price_update_date),
            allow_discount    = EXCLUDED.allow_discount,
            item_status       = EXCLUDED.item_status,
            item_id           = EXCLUDED.item_id,
            updated_at        = NOW();
        END;
        $$;
        """)

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
            r->>'chain_id',
            r->>'promotion_id',
            r->>'sub_chain_id',
            r->>'bikoret_no',
            r->>'promotion_description',
            CASE WHEN (r->>'promotion_update_date') IS NOT NULL AND (r->>'promotion_update_date') != ''
                 THEN (r->>'promotion_update_date')::TIMESTAMP ELSE NULL END,
            CASE WHEN (r->>'promotion_start_date') IS NOT NULL AND (r->>'promotion_start_date') != ''
                 THEN (r->>'promotion_start_date')::DATE ELSE NULL END,
            r->>'promotion_start_hour',
            CASE WHEN (r->>'promotion_end_date') IS NOT NULL AND (r->>'promotion_end_date') != ''
                 THEN (r->>'promotion_end_date')::DATE ELSE NULL END,
            r->>'promotion_end_hour',
            r->>'promotion_days',
            r->>'redemption_limit',
            r->>'reward_type',
            r->>'allow_multiple_discounts',
            COALESCE((r->>'is_weighted_promo')::boolean, false),
            r->>'is_gift_item',
            r->>'min_no_of_item_offered',
            r->>'additional_is_coupon',
            r->>'additional_gift_count',
            r->>'additional_is_total',
            r->>'additional_is_active',
            r->>'additional_restrictions',
            r->>'remarks',
            r->>'min_qty',
            r->>'discounted_price',
            r->>'discounted_price_per_mida',
            r->>'weight_unit',
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

        # -------------------------------------------------------------------
        # RPC functions for safe periodic cleanup (called by cleanup_db.py)
        # None of these tables have FK references pointing TO them, so there
        # is no cascade risk from these deletes.
        # p_dry_run=TRUE only counts — no rows are deleted.
        # -------------------------------------------------------------------
        cur.execute("""
        CREATE OR REPLACE FUNCTION get_table_stats()
        RETURNS TABLE(table_name TEXT, approx_count BIGINT)
        LANGUAGE sql SECURITY DEFINER AS $$
          SELECT relname::TEXT, n_live_tup::BIGINT
          FROM pg_stat_user_tables
          WHERE relname IN ('stores','products','prices','promotions','processed_files')
          ORDER BY relname;
        $$;
        """)

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
            SELECT COUNT(*) INTO deleted_count FROM prices WHERE updated_at < p_cutoff;
          ELSE
            DELETE FROM prices WHERE updated_at < p_cutoff;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
          END IF;
          RETURN deleted_count;
        END;
        $$;
        """)

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

        print("Schema updated: tables recreated + merge_prices / merge_promotions + 3 cleanup RPC functions created.")

if __name__ == '__main__':
    update_schema()
