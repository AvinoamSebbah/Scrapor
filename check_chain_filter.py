#!/usr/bin/env python3
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# Simulate the exact chain filter query from the backend offers.ts (line 644-651)
# SELECT DISTINCT s.chain_id, COALESCE(NULLIF(s.chain_name, ''), s.chain_id) AS chain_name
# FROM stores s WHERE s.city ILIKE 'ירושלים%' ORDER BY chain_name ASC LIMIT 100
city = 'ירושלים'
cur.execute("""
    SELECT DISTINCT
        s.chain_id,
        COALESCE(NULLIF(s.chain_name, ''), s.chain_id)::text AS chain_name
    FROM stores s
    WHERE s.city ILIKE %s || '%%'
    ORDER BY chain_name ASC
    LIMIT 100;
""", (city,))
cols = [d[0] for d in cur.description]
result["chain_filter_query_result"] = [dict(zip(cols, r)) for r in cur.fetchall()]

# Now what does the store filter look like?
cur.execute("""
    SELECT
        s.chain_id,
        COALESCE(NULLIF(s.chain_name, ''), s.chain_id)::text AS chain_name,
        s.store_id,
        COALESCE(NULLIF(s.store_name, ''), s.store_id)::text AS store_name,
        s.city::text AS city
    FROM stores s
    WHERE s.city ILIKE %s || '%%'
    ORDER BY chain_name ASC, store_name ASC
    LIMIT 500;
""", (city,))
cols = [d[0] for d in cur.description]
all_stores = [dict(zip(cols, r)) for r in cur.fetchall()]
# Filter only יש חסד
result["yesh_hesed_in_store_filter"] = [s for s in all_stores if 'יש חסד' in (s.get('chain_name') or '') or 'יש חסד' in (s.get('store_name') or '')]

# Key question: what chain_id does יש חסד have?
cur.execute("""
    SELECT DISTINCT chain_id, chain_name
    FROM stores
    WHERE chain_name = 'יש חסד'
    ORDER BY chain_id;
""")
result["yesh_hesed_chain_ids"] = [{"chain_id": r[0], "chain_name": r[1]} for r in cur.fetchall()]

# Also check the get_offers_for_item_code function to see how chain_id is used
# Let's look at the function definition
cur.execute("""
    SELECT pg_get_functiondef(oid)
    FROM pg_proc
    WHERE proname = 'get_offers_for_item_code';
""")
row = cur.fetchone()
result["get_offers_for_item_code_def"] = row[0] if row else None

# Also check get_top_city_promotions
cur.execute("""
    SELECT pg_get_functiondef(oid)
    FROM pg_proc
    WHERE proname = 'get_top_city_promotions';
""")
row = cur.fetchone()
result["get_top_city_promotions_def"] = row[0] if row else "not found"

# Check the shopping list page - how does the frontend get chains?
# Look at /api/offers/top-promotions for city=ירושלים - what chains appear?
# In particular, can the filter select 'יש חסד'?
# The filter in top-promotions uses chainId = chain_id (the SHUFERSAL id)
# So when user selects "יש חסד" from filter list, what chainId does it pass?

# Let's check: the chainFilters returns chain_id, chain_name
# יש חסד stores have chain_id = '7290027600007'
# But שופרסל דיל also has chain_id = '7290027600007'!
# So the filter would show "יש חסד" as a separate chain_name entry
# but with the SAME chain_id as שופרסל דיל

# When user clicks on "יש חסד" in the filter, chainId = '7290027600007' is sent
# But then ALL shufersal sub-chains are shown, not just יש חסד!
# The real question is: does "יש חסד" appear in the PROMOTIONS at all?

# Check store_promotions_cache for יש חסד Jerusalem
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'store_promotions_cache'
    ORDER BY ordinal_position;
""")
result["store_promotions_cache_schema"] = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]

cur.execute("""
    SELECT DISTINCT chain_name, city, COUNT(*) as promo_count
    FROM store_promotions_cache
    WHERE (chain_name = 'יש חסד' OR chain_name ILIKE '%יש חסד%')
    GROUP BY chain_name, city
    ORDER BY city, chain_name;
""")
result["yesh_hesed_in_promo_cache"] = [{"chain_name": r[0], "city": r[1], "count": r[2]} for r in cur.fetchall()]

cur.execute("""
    SELECT DISTINCT chain_name, city, COUNT(*) as promo_count
    FROM store_promotions_cache
    WHERE city ILIKE '%ירושלים%'
    GROUP BY chain_name, city
    ORDER BY chain_name;
""")
result["jerusalem_chains_in_promo_cache"] = [{"chain_name": r[0], "city": r[1], "count": r[2]} for r in cur.fetchall()]

cur.close()
conn.close()

with open("check_chain_filter_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done -> check_chain_filter_result.json")
