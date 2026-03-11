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

-- =============================================================================
-- PARTIE 2 : STANDARDISATION ET EXTRACTION DES VILLES (CITY)
-- =============================================================================

UPDATE stores
SET city = CASE 
    -- --- MAPPING DES CODES STATISTIQUES (Priorité 1) ---
    WHEN city IN ('3000.0', '3000') THEN 'ירושלים'
    WHEN city IN ('4000.0', '4000') THEN 'חיפה'
    WHEN city IN ('5000.0', '5000') THEN 'תל אביב'
    WHEN city IN ('9000.0', '9000') THEN 'באר שבע'
    WHEN city IN ('8800.0', '8800') THEN 'שפרעם'
    WHEN city IN ('8600.0', '8600') THEN 'רמת גן'
    WHEN city IN ('8300.0', '8300') THEN 'ראשון לציון'

    -- --- STANDARDIZATION ET EXTRACTION (Vérifie city, address ET store_name) ---
    -- Tel Aviv
    WHEN city IN ('תל אביב-יפו', 'תל אביב - יפו', 'ת"א') 
         OR address ILIKE '%תל אביב%' OR address ILIKE '%ת"א%' 
         OR store_name ILIKE '%תל אביב%' OR store_name ILIKE '%ת"א%' THEN 'תל אביב'
         
    -- Ramat Gan
    WHEN city = 'רמת-גן' 
         OR address ILIKE '%רמת גן%' OR address ILIKE '%רמת-גן%' 
         OR store_name ILIKE '%רמת גן%' OR store_name ILIKE '%רמת-גן%' THEN 'רמת גן'
         
    -- Ramat HaSharon
    WHEN city IN ('רמת-השרון', 'רמתהשרון') 
         OR address ILIKE '%רמת השרון%' OR address ILIKE '%רמת-השרון%' 
         OR store_name ILIKE '%רמת השרון%' OR store_name ILIKE '%רמת-השרון%' THEN 'רמת השרון'
         
    -- Rishon LeZion
    WHEN city = 'ראשל"צ' 
         OR address ILIKE '%ראשון לציון%' OR address ILIKE '%ראשל"צ%' 
         OR store_name ILIKE '%ראשון לציון%' OR store_name ILIKE '%ראשל"צ%' THEN 'ראשון לציון'
         
    -- Petah Tikva
    WHEN city IN ('פתח-תקווה', 'פתח-תקוה') 
         OR address ILIKE '%פתח תקווה%' OR address ILIKE '%פ"ת%' 
         OR store_name ILIKE '%פתח תקווה%' OR store_name ILIKE '%פ"ת%' THEN 'פתח תקווה'

    -- --- CAS GÉNÉRAUX (Si city est unknown, nombre 0.0, 139.0, ou virgule) ---
    WHEN (city IS NULL OR TRIM(city) = 'unknown' OR city ~ '^[0-9\.]+$' OR city LIKE '%,%') THEN
        CASE 
            -- VILLES RÉCENTES (Vérification double address + store_name)
            WHEN (address ILIKE '%טלזסטון%' OR store_name ILIKE '%טלזסטון%') THEN 'טלזסטון'
            WHEN (address ILIKE '%בית אל%' OR store_name ILIKE '%בית אל%') THEN 'בית אל'
            WHEN (address ILIKE '%קריית ספר%' OR store_name ILIKE '%קריית ספר%') THEN 'קריית ספר'
            WHEN (address ILIKE '%הר יונה%' OR store_name ILIKE '%הר יונה%') THEN 'הר יונה'
            WHEN (address ILIKE '%כפר עזר%' OR store_name ILIKE '%כפר עזר%') THEN 'כפר עזר'
            WHEN (address ILIKE '%בני דרום%' OR store_name ILIKE '%בני דרום%') THEN 'בני דרום'
            WHEN (address ILIKE '%שורש%' OR store_name ILIKE '%שורש%') THEN 'שורש'
            WHEN (address ILIKE '%שפרעם%' OR store_name ILIKE '%שפרעם%') THEN 'שפרעם'
            WHEN (address ILIKE '%אלישמע%' OR store_name ILIKE '%אלישמע%') THEN 'אלישמע'
            WHEN (address ILIKE '%יגור%' OR store_name ILIKE '%יגור%') THEN 'יגור'
            WHEN (address ILIKE '%נוווה מונסון%' OR store_name ILIKE '%נווה מונסון%' OR address ILIKE '%נווה מונסון%') THEN 'נווה מונסון'

            -- AUTRES VILLES (Vérification double)
            WHEN (address ILIKE '%ראש פינה%' OR store_name ILIKE '%ראש פינה%') THEN 'ראש פינה'
            WHEN (address ILIKE '%באר שבע%' OR address ILIKE '%ב"ש%' OR store_name ILIKE '%באר שבע%' OR store_name ILIKE '%ב"ש%') THEN 'באר שבע'
            WHEN (address ILIKE '%רמת רחל%' OR store_name ILIKE '%רמת רחל%') THEN 'רמת רחל'
            WHEN (address ILIKE '%יפעת%' OR store_name ILIKE '%יפעת%') THEN 'יפעת'
            WHEN (address ILIKE '%לכיש%' OR store_name ILIKE '%לכיש%') THEN 'לכיש'
            WHEN (address ILIKE '%יוקנעם%' OR store_name ILIKE '%יוקנעם%') THEN 'יוקנעם'
            WHEN (address ILIKE '%קריית אונו%' OR address ILIKE '%קרית אונו%' OR store_name ILIKE '%קריית אונו%' OR store_name ILIKE '%קרית אונו%') THEN 'קרית אונו'
            WHEN (address ILIKE '%מעוז חיים%' OR store_name ILIKE '%מעוז חיים%') THEN 'מעוז חיים'
            WHEN (address ILIKE '%תל יצחק%' OR store_name ILIKE '%תל יצחק%') THEN 'תל יצחק'
            WHEN (address ILIKE '%מודיעין%' OR store_name ILIKE '%מודיעין%') THEN 'מודיעין'
            WHEN (address ILIKE '%קיסריה%' OR store_name ILIKE '%קיסריה%') THEN 'קיסריה'
            WHEN (address ILIKE '%מזרע%' OR store_name ILIKE '%מזרע%') THEN 'מזרע'
            WHEN (address ILIKE '%מישור אדומים%' OR store_name ILIKE '%מישור אדומים%') THEN 'מישור אדומים'
            WHEN (address ILIKE '%גוש עציון%' OR store_name ILIKE '%גוש עציון%') THEN 'גוש עציון'
            WHEN (address ILIKE '%לטרון%' OR store_name ILIKE '%לטרון%') THEN 'לטרון'
            WHEN (address ILIKE '%ירושלים%' OR store_name ILIKE '%ירושלים%') THEN 'ירושלים'
            WHEN (address ILIKE '%חיפה%' OR store_name ILIKE '%חיפה%') THEN 'חיפה'
            WHEN (address ILIKE '%נתניה%' OR store_name ILIKE '%נתניה%') THEN 'נתניה'
            WHEN (address ILIKE '%אשדוד%' OR store_name ILIKE '%אשדוד%') THEN 'אשדוד'
            WHEN (address ILIKE '%אשקלון%' OR store_name ILIKE '%אשקלון%') THEN 'אשקלון'
            WHEN (address ILIKE '%הרצליה%' OR store_name ILIKE '%הרצליה%') THEN 'הרצליה'
            WHEN (address ILIKE '%חולון%' OR store_name ILIKE '%חולון%') THEN 'חולון'
            WHEN (address ILIKE '%בת ים%' OR store_name ILIKE '%בת ים%') THEN 'בת ים'
            WHEN (address ILIKE '%כפר סבא%' OR store_name ILIKE '%כפר סבא%') THEN 'כפר סבא'
            WHEN (address ILIKE '%רעננה%' OR store_name ILIKE '%רעננה%') THEN 'רעננה'
            WHEN (address ILIKE '%חדרה%' OR store_name ILIKE '%חדרה%') THEN 'חדרה'
            WHEN (address ILIKE '%יהוד%' OR store_name ILIKE '%יהוד%') THEN 'יהוד מונוסון'
            WHEN (address ILIKE '%באר יעקב%' OR store_name ILIKE '%באר יעקב%') THEN 'באר יעקב'
            WHEN (address ILIKE '%קרית שמונה%' OR store_name ILIKE '%קרית שמונה%') THEN 'קרית שמונה'
            WHEN (address ILIKE '%ביאליק%' OR store_name ILIKE '%ביאליק%') THEN 'קרית ביאליק'
            WHEN (address ILIKE '%נהריה%' OR store_name ILIKE '%נהריה%') THEN 'נהריה'
            WHEN (address ILIKE '%עפולה%' OR store_name ILIKE '%עפולה%') THEN 'עפולה'
            WHEN (address ILIKE '%כרמיאל%' OR store_name ILIKE '%כרמיאל%') THEN 'כרמיאל'
            
            ELSE city 
        END

    ELSE city 
END;

-- =============================================================================
-- PARTIE 3 : HARMONISATION FINALE DU FORMAT
-- =============================================================================

-- Suppression systématique des tirets par des espaces
UPDATE stores
SET city = REPLACE(city, '-', ' ')
WHERE city LIKE '%-%';


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
