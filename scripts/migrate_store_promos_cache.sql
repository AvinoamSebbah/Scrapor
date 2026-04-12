-- =============================================================================
-- migrate_store_promos_cache.sql
-- =============================================================================
-- Crée la table store_promotions_cache + index + fonctions SQL
-- qui remplacent l'ancienne top_promotions_cache pour la page Promotions.
--
-- Axes de segmentation :
--   time_window  : '24h' | '7days' | '30days'
--   promo_type   : 'regular' | 'conditional'  (coupon/club/carte = conditional)
--   sort_metric  : 'percent' | 'savings'
--   rank_position: 1..25  (25 par combinaison → 50 lignes / (store × window × type))
-- =============================================================================

-- ─── Nettoyage éventuel ──────────────────────────────────────────────────────

DROP TABLE IF EXISTS public.store_promotions_cache CASCADE;
DROP FUNCTION IF EXISTS public.refresh_store_promotions_window(integer, varchar);
DROP FUNCTION IF EXISTS public.get_store_promotions(varchar, varchar, varchar, varchar, varchar);

-- ─── Table principale ────────────────────────────────────────────────────────

CREATE TABLE public.store_promotions_cache (
  -- Identité du magasin
  store_db_id         integer       NOT NULL,   -- FK logique → stores.id
  chain_id            varchar       NOT NULL,
  chain_name          varchar,
  store_id            varchar       NOT NULL,
  store_name          varchar,
  city                varchar       NOT NULL,

  -- Axes de segmentation
  time_window         varchar(6)    NOT NULL,   -- '24h' | '7days' | '30days'
  promo_type          varchar(12)   NOT NULL,   -- 'regular' | 'conditional'
  sort_metric         varchar(10)   NOT NULL,   -- 'percent' | 'savings'
  rank_position       smallint      NOT NULL,   -- 1..25

  -- Données produit
  item_code           varchar       NOT NULL,
  item_name           varchar,
  manufacturer_name   varchar,
  unit_of_measure     varchar,
  unit_qty            varchar,
  b_is_weighted       boolean       DEFAULT false,

  -- Prix & remises
  price               numeric       NOT NULL,
  promo_price         numeric       NOT NULL,
  effective_price     numeric       NOT NULL,
  discount_amount     numeric       NOT NULL,   -- ₪ économisés
  discount_percent    numeric       NOT NULL,   -- % de remise
  smart_score         numeric       NOT NULL,   -- score pondéré (40% % + 60% ₪ cap 80)

  -- Méta promo (déjà décodée, pas besoin de lookup supplémentaire)
  promo_kind          varchar(12),              -- 'regular'|'coupon'|'club'|'card'|'insurance'
  promo_label         varchar,                  -- libellé hébreu affiché au front
  promotion_id        varchar,
  promotion_description varchar,
  promotion_end_date  date,

  -- Horodatage
  updated_at          timestamp without time zone,
  refreshed_at        timestamp without time zone DEFAULT NOW(),

  PRIMARY KEY (store_db_id, time_window, promo_type, sort_metric, rank_position)
);

COMMENT ON TABLE public.store_promotions_cache IS
  'Cache nightly des 50 meilleures promos par magasin (25 par % + 25 par ₪), '
  '3 fenêtres temporelles × 2 types (regular / conditional). '
  'Rechargé chaque nuit à 2h par nightly_promos_refresh.py.';

-- ─── Index de recherche rapide ───────────────────────────────────────────────

-- Accès par ville + fenêtre temporelle (le plus fréquent)
CREATE INDEX idx_spc_city_window
  ON public.store_promotions_cache (city, time_window);

-- Accès par chaîne + ville + fenêtre
CREATE INDEX idx_spc_chain_city_window
  ON public.store_promotions_cache (chain_id, city, time_window);

-- Accès par store_db_id + fenêtre (lookup direct)
CREATE INDEX idx_spc_store_window
  ON public.store_promotions_cache (store_db_id, time_window);


-- ─── Fonction de refresh pour UNE fenêtre ────────────────────────────────────

CREATE OR REPLACE FUNCTION public.refresh_store_promotions_window(
  p_window_hours integer,          -- 24 | 168 | 720
  p_time_window  varchar           -- '24h' | '7days' | '30days'
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_affected integer := 0;
BEGIN

  -- Supprime et recrée uniquement cette fenêtre temporelle
  DELETE FROM public.store_promotions_cache
  WHERE time_window = p_time_window;

  WITH
  -- ── 1. Données brutes : une ligne par (store, product, promotion) ──────────
  base AS (
    SELECT
      s.id                        AS store_db_id,
      s.chain_id::varchar,
      s.chain_name::varchar,
      s.store_id::varchar,
      s.store_name::varchar,
      s.city::varchar,
      p_time_window::varchar      AS time_window,

      -- Type de promo : 'conditional' si coupon / club / carte / assurance
      CASE
        WHEN COALESCE(prom.additional_is_coupon,'') IN ('1','true','t','yes','y') THEN 'conditional'
        WHEN prom.club_id IS NOT NULL
          AND LOWER(TRIM(COALESCE(prom.club_id,''))) NOT IN ('','0','0.0','no_body','none','null','nan')
          THEN 'conditional'
        WHEN LOWER(COALESCE(prom.promotion_description,'')) SIMILAR TO
             '%(קופון|coupon|מועדון|club|אשראי|ויזה|מאסטר|אמקס|visa|mastercard|ביטוח|insurance)%'
          THEN 'conditional'
        ELSE 'regular'
      END::varchar                AS promo_type,

      -- Kind détaillé pour le label hébreu
      CASE
        WHEN COALESCE(prom.additional_is_coupon,'') IN ('1','true','t','yes','y')
          THEN 'coupon'
        WHEN LOWER(COALESCE(prom.promotion_description,'')) LIKE '%קופון%'
          OR LOWER(COALESCE(prom.promotion_description,'')) LIKE '%coupon%'
          THEN 'coupon'
        WHEN prom.club_id IS NOT NULL
          AND LOWER(TRIM(COALESCE(prom.club_id,''))) NOT IN ('','0','0.0','no_body','none','null','nan')
          THEN 'club'
        WHEN LOWER(COALESCE(prom.promotion_description,'')) LIKE '%מועדון%'
          OR LOWER(COALESCE(prom.promotion_description,'')) LIKE '%club%'
          THEN 'club'
        WHEN LOWER(COALESCE(prom.promotion_description,'')) SIMILAR TO '%(אשראי|ויזה|מאסטר|אמקס|visa|mastercard|card)%'
          THEN 'card'
        WHEN LOWER(COALESCE(prom.promotion_description,'')) SIMILAR TO '%(ביטוח|insurance)%'
          THEN 'insurance'
        ELSE 'regular'
      END::varchar                AS promo_kind,

      -- Informations produit
      pr.item_code::varchar,
      pr.item_name::varchar,
      pr.manufacturer_name::varchar,
      pp.unit_of_measure::varchar,
      pp.unit_qty::varchar,
      COALESCE(pp.b_is_weighted, FALSE)  AS b_is_weighted,

      -- Prix
      pp.price,
      psi.promo_price,
      LEAST(pp.price, psi.promo_price)   AS effective_price,
      GREATEST(pp.price - psi.promo_price, 0)  AS discount_amount,
      CASE
        WHEN pp.price > 0
        THEN ROUND((pp.price - psi.promo_price) / pp.price * 100.0, 2)
        ELSE 0::numeric
      END                                AS discount_percent,

      -- Smart score pondéré : 40 % remise en % + 60 % remise en ₪ (plafonnée à 80)
      ROUND(
        (CASE WHEN pp.price > 0
          THEN (pp.price - psi.promo_price) / pp.price * 100.0
          ELSE 0 END) * 0.40
        + LEAST(GREATEST(pp.price - psi.promo_price, 0), 80) * 0.60
      , 2)                               AS smart_score,

      -- Méta promotion
      psi.promotion_id::varchar,
      prom.promotion_description::varchar,
      psi.promotion_end_date,
      psi.updated_at

    FROM public.promotion_store_items psi
    JOIN public.stores s
      ON s.id = psi.store_id
    JOIN public.products pr
      ON pr.id = psi.product_id
    JOIN public.product_prices pp
      ON pp.product_id = psi.product_id
     AND pp.store_id   = psi.store_id
    LEFT JOIN public.promotions prom
      ON prom.chain_id     = psi.chain_id
     AND prom.promotion_id = psi.promotion_id

    WHERE
      -- Prix valides
      psi.promo_price IS NOT NULL AND psi.promo_price > 0
      AND pp.price IS NOT NULL    AND pp.price > 0
      AND psi.promo_price < pp.price
      AND psi.promo_price >= pp.price * 0.05   -- min 5% de remise réelle

      -- Promo non expirée
      AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)

      -- Fenêtre temporelle
      AND (
        p_window_hours <= 0
        OR psi.updated_at >= NOW() - make_interval(hours => p_window_hours)
      )

      -- Filtres qualité produit
      AND pr.item_code ~ '^[0-9]{8,14}$'
      AND COALESCE(BTRIM(pr.item_name), '') <> ''
      AND pr.item_name NOT ILIKE '%משלוח%'
      AND COALESCE(s.city, '') <> ''
  ),

  -- ── 2. Déduplication : un seul enregistrement par (store, type, item_code) ─
  deduped AS (
    SELECT DISTINCT ON (store_db_id, promo_type, item_code)
      *
    FROM base
    ORDER BY store_db_id, promo_type, item_code, promo_price ASC, updated_at DESC NULLS LAST
  ),

  -- ── 3. Classement par % (pondéré) ─────────────────────────────────────────
  ranked_pct AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY store_db_id, promo_type
        ORDER BY discount_percent DESC NULLS LAST, discount_amount DESC NULLS LAST,
                 updated_at DESC NULLS LAST, item_code ASC
      ) AS rn
    FROM deduped
  ),

  -- ── 4. Classement par économie ₪ ──────────────────────────────────────────
  ranked_savings AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY store_db_id, promo_type
        ORDER BY discount_amount DESC NULLS LAST, discount_percent DESC NULLS LAST,
                 updated_at DESC NULLS LAST, item_code ASC
      ) AS rn
    FROM deduped
  )

  -- ── 5. Insertion ──────────────────────────────────────────────────────────
  INSERT INTO public.store_promotions_cache (
    store_db_id, chain_id, chain_name, store_id, store_name, city,
    time_window, promo_type, sort_metric, rank_position,
    item_code, item_name, manufacturer_name, unit_of_measure, unit_qty, b_is_weighted,
    price, promo_price, effective_price, discount_amount, discount_percent, smart_score,
    promo_kind, promo_label,
    promotion_id, promotion_description, promotion_end_date,
    updated_at, refreshed_at
  )

  -- Top 25 par % de remise
  SELECT
    store_db_id, chain_id, chain_name, store_id, store_name, city,
    time_window, promo_type, 'percent'::varchar, rn::smallint,
    item_code, item_name, manufacturer_name, unit_of_measure, unit_qty, b_is_weighted,
    price, promo_price, effective_price, discount_amount, discount_percent, smart_score,
    promo_kind,
    CASE promo_kind
      WHEN 'coupon'    THEN 'קופון'
      WHEN 'club'      THEN 'הטבת מועדון'
      WHEN 'card'      THEN 'הטבת אשראי'
      WHEN 'insurance' THEN 'הטבת ביטוח'
      ELSE                  'מבצע'
    END::varchar,
    promotion_id, promotion_description, promotion_end_date,
    updated_at, NOW()
  FROM ranked_pct
  WHERE rn <= 25

  UNION ALL

  -- Top 25 par économie ₪
  SELECT
    store_db_id, chain_id, chain_name, store_id, store_name, city,
    time_window, promo_type, 'savings'::varchar, rn::smallint,
    item_code, item_name, manufacturer_name, unit_of_measure, unit_qty, b_is_weighted,
    price, promo_price, effective_price, discount_amount, discount_percent, smart_score,
    promo_kind,
    CASE promo_kind
      WHEN 'coupon'    THEN 'קופון'
      WHEN 'club'      THEN 'הטבת מועדון'
      WHEN 'card'      THEN 'הטבת אשראי'
      WHEN 'insurance' THEN 'הטבת ביטוח'
      ELSE                  'מבצע'
    END::varchar,
    promotion_id, promotion_description, promotion_end_date,
    updated_at, NOW()
  FROM ranked_savings
  WHERE rn <= 25;

  GET DIAGNOSTICS v_affected = ROW_COUNT;
  RETURN v_affected;
END;
$$;

COMMENT ON FUNCTION public.refresh_store_promotions_window IS
  'Refresh le cache pour UNE fenêtre temporelle. '
  'Appeler 3 fois : (24, ''24h''), (168, ''7days''), (720, ''30days'').';


-- ─── Fonction de lecture (remplace get_top_city_promotions) ──────────────────

CREATE OR REPLACE FUNCTION public.get_store_promotions(
  p_city        varchar,
  p_chain_id    varchar  DEFAULT NULL,
  p_store_id    varchar  DEFAULT NULL,
  p_time_window varchar  DEFAULT '24h',
  p_promo_type  varchar  DEFAULT 'regular',
  p_sort_metric varchar  DEFAULT 'percent'
)
RETURNS TABLE (
  store_db_id         integer,
  chain_id            varchar,
  chain_name          varchar,
  store_id            varchar,
  store_name          varchar,
  city                varchar,
  time_window         varchar,
  promo_type          varchar,
  sort_metric         varchar,
  rank_position       smallint,
  item_code           varchar,
  item_name           varchar,
  manufacturer_name   varchar,
  unit_of_measure     varchar,
  unit_qty            varchar,
  b_is_weighted       boolean,
  price               numeric,
  promo_price         numeric,
  effective_price     numeric,
  discount_amount     numeric,
  discount_percent    numeric,
  smart_score         numeric,
  promo_kind          varchar,
  promo_label         varchar,
  promotion_id        varchar,
  promotion_description varchar,
  promotion_end_date  date,
  updated_at          timestamp without time zone
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
  SELECT
    spc.store_db_id,
    spc.chain_id,
    spc.chain_name,
    spc.store_id,
    spc.store_name,
    spc.city,
    spc.time_window,
    spc.promo_type,
    spc.sort_metric,
    spc.rank_position,
    spc.item_code,
    spc.item_name,
    spc.manufacturer_name,
    spc.unit_of_measure,
    spc.unit_qty,
    spc.b_is_weighted,
    spc.price,
    spc.promo_price,
    spc.effective_price,
    spc.discount_amount,
    spc.discount_percent,
    spc.smart_score,
    spc.promo_kind,
    spc.promo_label,
    spc.promotion_id,
    spc.promotion_description,
    spc.promotion_end_date,
    spc.updated_at
  FROM public.store_promotions_cache spc
  WHERE
    -- filtre ville (toujours requis)
    spc.city ILIKE p_city || '%'
    -- filtre chaîne (optionnel)
    AND (p_chain_id IS NULL OR p_chain_id = '' OR spc.chain_id = p_chain_id)
    -- filtre magasin (optionnel)
    AND (p_store_id IS NULL OR p_store_id = '' OR spc.store_id = p_store_id)
    -- fenêtre temporelle
    AND spc.time_window   = COALESCE(p_time_window, '24h')
    -- type de promo
    AND spc.promo_type    = COALESCE(p_promo_type, 'regular')
    -- métrique de tri
    AND spc.sort_metric   = COALESCE(p_sort_metric, 'percent')
  -- Tri : par store_db_id puis rank, pour que le front puisse regrouper
  -- Si toute la ville : tri global par discount_percent ou discount_amount DESC
  ORDER BY
    CASE WHEN COALESCE(p_sort_metric, 'percent') = 'percent'
      THEN spc.discount_percent
      ELSE spc.discount_amount
    END DESC NULLS LAST,
    spc.store_db_id ASC,
    spc.rank_position ASC;
$$;

COMMENT ON FUNCTION public.get_store_promotions IS
  'Lecture du cache store_promotions_cache. 1 fetch = 1 carrousel. '
  'Paramètres : city (requis), chainId, storeId, timeWindow, promoType, sortMetric.';
