"""Push only the RPC function definitions to Supabase.

Safe to run at any time — uses CREATE OR REPLACE, never drops data tables.
Requires SUPABASE_DATABASE_URL in the environment.
"""
import os
import psycopg2

db_url = os.getenv('SUPABASE_DATABASE_URL')

def update_functions():
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:

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
        print("✅ get_table_stats")

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
        print("✅ merge_prices")

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
            SELECT COUNT(*) INTO deleted_count FROM prices WHERE updated_at < p_cutoff;
          ELSE
            DELETE FROM prices WHERE updated_at < p_cutoff;
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

    conn.close()
    print("\nAll functions updated successfully.")

if __name__ == '__main__':
    update_functions()
