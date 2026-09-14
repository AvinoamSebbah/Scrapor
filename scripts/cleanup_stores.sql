-- =============================================================================
-- PARTIE 1 : NETTOYAGE ET SEGMENTATION DES ENSEIGNES (CHANNELS)
-- =============================================================================

-- 1. Suppression des lignes sans nom de chaîne
DELETE FROM stores 
WHERE chain_name IS NULL;

-- 2. Segmentation de Shufersal (Yesh Chesed et BE)
UPDATE stores
SET chain_name = CASE 
    WHEN store_name ILIKE '%יש חסד%' THEN 'יש חסד'
    WHEN store_name ILIKE '%BE%' THEN 'BE'
    ELSE chain_name
END
WHERE chain_name = 'שופרסל';

-- City is curated separately. This recurring cleanup must never modify stores.city.
-- New stores still receive their initial city in the insert performed by the uploader.

DO $$
BEGIN
  IF to_regclass('public.top_promotions_cache') IS NOT NULL THEN
    WITH manual_city_overrides(chain_id, store_id, city) AS (
        VALUES
          ('7290055700007', '3740', 'אשקלון'),
          ('7290055700007', '2740', 'רחובות'),
          ('7290055700007', '2190', 'רחובות'),
          ('7290055700007', '620',  'נתניה')
    )
    UPDATE top_promotions_cache tpc
    SET city = o.city
    FROM manual_city_overrides o
    WHERE tpc.chain_id = o.chain_id
      AND tpc.store_id = o.store_id
      AND COALESCE(tpc.city, '') <> o.city;
  END IF;

  IF to_regclass('public.store_promotions_cache') IS NOT NULL THEN
    WITH manual_city_overrides(chain_id, store_id, city) AS (
        VALUES
          ('7290055700007', '3740', 'אשקלון'),
          ('7290055700007', '2740', 'רחובות'),
          ('7290055700007', '2190', 'רחובות'),
          ('7290055700007', '620',  'נתניה')
    )
    UPDATE store_promotions_cache spc
    SET city = o.city
    FROM manual_city_overrides o
    WHERE spc.chain_id = o.chain_id
      AND spc.store_id = o.store_id
      AND COALESCE(spc.city, '') <> o.city;
  END IF;
END $$;


UPDATE stores
SET chain_name = CASE 
    WHEN store_name ILIKE '%יש חסד%' THEN 'יש חסד'
    WHEN store_name ILIKE '%BE%' THEN 'BE'
    WHEN store_name ILIKE '%דיל%' THEN 'שופרסל דיל'
    WHEN store_name ILIKE '%אקספרס%' THEN 'שופרסל אקספרס'
    WHEN store_name ILIKE '%שלי%' THEN 'שופרסל שלי'
    WHEN store_name ILIKE '%יש%' THEN 'יש'
    WHEN store_name ILIKE '%יוניברס%' THEN 'יוניברס'
    WHEN store_name ILIKE '%GOOD MARKET%' THEN 'גוד מרקט'
    WHEN store_name ILIKE '%גוד מרקט%' THEN 'גוד מרקט'
    WHEN store_name ILIKE '%שופרסל ONLINE%' THEN 'שופרסל ONLINE'
    ELSE chain_name
END
WHERE chain_name = 'שופרסל';
