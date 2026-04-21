-- Update get_offers_for_item_code to include p_chain_name
CREATE OR REPLACE FUNCTION public.get_offers_for_item_code(p_item_code text, p_city text DEFAULT NULL::text, p_chain_id text DEFAULT NULL::text, p_limit integer DEFAULT 300, p_offset integer DEFAULT 0, p_chain_name text DEFAULT NULL::text)
 RETURNS TABLE(item_code text, item_name text, manufacturer_name text, chain_id text, store_id text, store_name text, city text, price numeric, promo_price numeric, effective_price numeric, unit_of_measure text, unit_qty text, b_is_weighted boolean, updated_at timestamp without time zone)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
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
            AND (p_chain_name IS NULL OR p_chain_name = '' OR s.chain_name = p_chain_name)
          ORDER BY LEAST(pp.price, COALESCE(pb.promo_price, pp.promo_price, pp.price)) ASC NULLS LAST, pp.updated_at DESC
          LIMIT GREATEST(COALESCE(p_limit, 300), 1)
          OFFSET GREATEST(COALESCE(p_offset, 0), 0);
        $function$;

-- Update get_city_offers_for_search to include p_chain_name
CREATE OR REPLACE FUNCTION public.get_city_offers_for_search(p_query text, p_city text, p_chain_id text DEFAULT NULL::text, p_limit_products integer DEFAULT 10, p_offset_products integer DEFAULT 0, p_chain_name text DEFAULT NULL::text)
 RETURNS TABLE(product_rank bigint, item_code text, item_name text, manufacturer_name text, chain_id text, store_id text, store_name text, city text, price numeric, promo_price numeric, effective_price numeric, unit_of_measure text, unit_qty text, b_is_weighted boolean, updated_at timestamp without time zone)
 LANGUAGE sql
 STABLE SECURITY DEFINER
AS $function$
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
              AND (p_chain_name IS NULL OR p_chain_name = '' OR s.chain_name = p_chain_name)
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
        $function$;

-- Update get_top_city_promotions to include p_chain_name
CREATE OR REPLACE FUNCTION public.get_top_city_promotions(p_city text, p_chain_id text DEFAULT NULL::text, p_store_id text DEFAULT NULL::text, p_window_hours integer DEFAULT 24, p_limit integer DEFAULT 50, p_offset integer DEFAULT 0, p_order_by text DEFAULT 'score'::text, p_chain_name text DEFAULT NULL::text)
 RETURNS TABLE(item_code text, item_name text, manufacturer_name text, chain_id text, chain_name text, store_id text, store_name text, city text, unit_of_measure text, unit_qty text, b_is_weighted boolean, price numeric, promo_price numeric, effective_price numeric, discount_amount numeric, discount_percent numeric, smart_score numeric, promotion_end_date date, updated_at timestamp without time zone, promotion_id text, promotion_description text, promo_kind text, promo_label text, is_conditional_promo boolean, has_image boolean)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
AS $function$
        DECLARE
          v_scope TEXT;
          v_chain_id TEXT := COALESCE(p_chain_id, '');
          v_store_id TEXT := COALESCE(p_store_id, '');
          v_window_hours INT := GREATEST(COALESCE(p_window_hours, 24), 1);
          v_limit INT := LEAST(GREATEST(COALESCE(p_limit, 50), 1), 200);
          v_offset INT := GREATEST(COALESCE(p_offset, 0), 0);
        BEGIN
          v_scope := CASE
            WHEN v_store_id <> '' THEN 'store'
            WHEN v_chain_id <> '' THEN 'chain'
            ELSE 'city'
          END;

          -- Snap window_hours to the nearest available cached window (avoids empty results for e.g. 720h)
          SELECT MAX(c2.window_hours) INTO v_window_hours
          FROM top_promotions_cache c2
          WHERE c2.window_hours <= v_window_hours
            AND c2.scope_type = v_scope;
          -- Fallback: if nothing <= requested, take the smallest available
          IF v_window_hours IS NULL THEN
            SELECT MIN(c2.window_hours) INTO v_window_hours
            FROM top_promotions_cache c2
            WHERE c2.scope_type = v_scope;
          END IF;
          -- Last resort default
          IF v_window_hours IS NULL THEN
            v_window_hours := 24;
          END IF;

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
            c.updated_at,
            COALESCE(c.promotion_id, '')::TEXT AS promotion_id,
            COALESCE(c.promotion_description, '')::TEXT AS promotion_description,
            COALESCE(c.promo_kind, 'regular')::TEXT AS promo_kind,
            COALESCE(c.promo_label, 'מבצע')::TEXT AS promo_label,
            COALESCE(c.is_conditional_promo, FALSE) AS is_conditional_promo,
            c.has_image
          FROM top_promotions_cache c
          WHERE c.window_hours = v_window_hours
            AND c.scope_type = v_scope
            AND c.has_image IS TRUE
            AND (p_city IS NULL OR p_city = '' OR c.city ILIKE p_city || '%')
            AND (v_scope <> 'chain' OR c.chain_id = v_chain_id)
            AND (v_scope <> 'store' OR c.store_id = v_store_id)
            AND (v_scope <> 'store' OR v_chain_id = '' OR c.chain_id = v_chain_id)
            AND (p_chain_name IS NULL OR p_chain_name = '' OR c.chain_name = p_chain_name)
          ORDER BY
            CASE WHEN p_order_by = 'percent' THEN c.discount_percent END DESC NULLS LAST,
            CASE WHEN p_order_by = 'savings' THEN c.discount_amount END DESC NULLS LAST,
            c.rank_position ASC
          LIMIT v_limit
          OFFSET v_offset;
        END;
        $function$;
