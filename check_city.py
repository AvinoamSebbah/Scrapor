#!/usr/bin/env python3
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# Check exact city values for יש חסד Jerusalem stores
cur.execute("""
    SELECT id, chain_name, store_name, city, LOWER(TRIM(city)) as city_lower
    FROM stores
    WHERE chain_name = 'יש חסד'
    AND city ILIKE '%ירושלים%'
    ORDER BY store_name;
""")
rows = cur.fetchall()
result["yesh_hesed_jlm_city_exact"] = [
    {"id": r[0], "chain_name": r[1], "store_name": r[2], "city": r[3], "city_lower": r[4]}
    for r in rows
]

# Now simulate what the backend does: city.trim().toLowerCase() = 'ירושלים'
# The prisma query does: WHERE city = 'ירושלים' (exact, case-sensitive in hebrew)
cur.execute("""
    SELECT id, store_name, city
    FROM stores
    WHERE city = 'ירושלים'
    AND (chain_name = 'יש חסד' OR store_name ILIKE '%יש חסד%')
    ORDER BY store_name;
""")
rows = cur.fetchall()
result["yesh_hesed_jlm_exact_match"] = [
    {"id": r[0], "store_name": r[1], "city": r[2]}
    for r in rows
]

# How many stores in Jerusalem total with exact match vs ILIKE
cur.execute("SELECT COUNT(*) FROM stores WHERE city = 'ירושלים';")
result["jerusalem_exact_count"] = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM stores WHERE city ILIKE '%ירושלים%';")
result["jerusalem_ilike_count"] = cur.fetchone()[0]

# What's the getCityStoreIdsCached doing? 
# prisma.store.findMany({ where: { city: 'ירושלים' } })  (exact match!)
# city field: is it stored trimmed + lowercase? Let's check
cur.execute("""
    SELECT DISTINCT city
    FROM stores
    WHERE city ILIKE '%ירושל%'
    ORDER BY city;
""")
result["jerusalem_city_variants"] = [r[0] for r in cur.fetchall()]

# Get the getCityStoreIds equivalent
cur.execute("""
    SELECT id FROM stores WHERE city = 'ירושלים';
""")
store_ids = [r[0] for r in cur.fetchall()]
result["city_store_ids_ירושלים"] = store_ids
result["city_store_ids_count"] = len(store_ids)

# Check if יש חסד Jerusalem stores have ids in that list
yesh_hesed_jlm_ids = [95971, 96162, 96205, 96055]
result["yesh_hesed_in_jerusalem_store_ids"] = [sid for sid in yesh_hesed_jlm_ids if sid in store_ids]
result["yesh_hesed_missing_from_jerusalem_store_ids"] = [sid for sid in yesh_hesed_jlm_ids if sid not in store_ids]

# Now check product_prices for these stores
for sid in yesh_hesed_jlm_ids:
    cur.execute("SELECT COUNT(*) FROM product_prices WHERE store_id = %s;", (sid,))
    count = cur.fetchone()[0]
    result.setdefault("product_prices_count_by_store", {})[str(sid)] = count

# Check the prices table schema (different from product_prices)
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'prices'
    ORDER BY ordinal_position;
""")
result["prices_table_columns"] = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]

# Sample from prices table (DIFFERENT from product_prices)
cur.execute("SELECT * FROM prices LIMIT 2;")
cols = [d[0] for d in cur.description]
result["prices_sample_cols"] = cols

# How many prices rows?
cur.execute("SELECT COUNT(*) FROM prices;")
result["prices_total"] = cur.fetchone()[0]

# Does 'prices' table have available_in_store_ids?
if 'available_in_store_ids' in cols:
    cur.execute("""
        SELECT p.item_code, p.available_in_store_ids
        FROM prices p
        WHERE %s = ANY(p.available_in_store_ids::text[])
        LIMIT 5;
    """, (str(95971),))
    result["prices_for_store_95971"] = [str(r) for r in cur.fetchall()]

cur.close()
conn.close()

with open("check_city_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done -> check_city_result.json")
