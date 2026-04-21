#!/usr/bin/env python3
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# All tables
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;")
result["all_tables"] = [r[0] for r in cur.fetchall()]

# Check exact city values for יש חסד Jerusalem stores
cur.execute("""
    SELECT id, chain_name, store_name, city
    FROM stores
    WHERE chain_name = 'יש חסד'
    AND city ILIKE '%ירושלים%'
    ORDER BY store_name;
""")
rows = cur.fetchall()
result["yesh_hesed_jlm_stores"] = [
    {"id": r[0], "chain_name": r[1], "store_name": r[2], "city": r[3]}
    for r in rows
]

# The backend query: city = key (exact match, with key = city.trim().toLowerCase())
# But Hebrew doesn't have uppercase, so toLowerCase() doesn't change anything
# The key question is: does the frontend send 'ירושלים' exactly?

# How many stores match city = 'ירושלים' (exact)?
cur.execute("SELECT COUNT(*) FROM stores WHERE city = 'ירושלים';")
result["jerusalem_exact_match_count"] = cur.fetchone()[0]

# How many match with ILIKE?
cur.execute("SELECT COUNT(*) FROM stores WHERE city ILIKE '%ירושלים%';")
result["jerusalem_ilike_count"] = cur.fetchone()[0]

# All distinct city values containing ירושלים
cur.execute("SELECT DISTINCT city FROM stores WHERE city ILIKE '%ירושל%' ORDER BY city;")
result["jerusalem_variants"] = [r[0] for r in cur.fetchall()]

# Do the יש חסד JLM stores have city = 'ירושלים' exactly?
yesh_hesed_jlm_ids = [95971, 96162, 96205, 96055]

cur.execute("""
    SELECT id, city FROM stores WHERE id = ANY(%s);
""", (yesh_hesed_jlm_ids,))
result["yesh_hesed_jlm_city_values"] = [{"id": r[0], "city": r[1]} for r in cur.fetchall()]

# Get all store IDs for city = 'ירושלים' (exact) - simulating backend
cur.execute("SELECT id FROM stores WHERE city = 'ירושלים';")
exact_ids = [r[0] for r in cur.fetchall()]
result["city_exact_store_ids_count"] = len(exact_ids)

# Are the יש חסד stores in that list?
result["yesh_hesed_in_exact_list"] = [sid for sid in yesh_hesed_jlm_ids if sid in exact_ids]
result["yesh_hesed_MISSING_from_exact_list"] = [sid for sid in yesh_hesed_jlm_ids if sid not in exact_ids]

# product_prices counts for יש חסד JLM stores
for sid in yesh_hesed_jlm_ids:
    cur.execute("SELECT COUNT(*) FROM product_prices WHERE store_id = %s;", (sid,))
    count = cur.fetchone()[0]
    result.setdefault("product_prices_by_store", {})[str(sid)] = count

# Now check the 'prices' table - maybe it doesn't exist but let's check the search function
# The backend search.fast uses: prices pr WHERE pr.available_in_store_ids && cityStoreIds
# Let's check if there's a 'prices' view or function
cur.execute("""
    SELECT routine_name, routine_type
    FROM information_schema.routines
    WHERE routine_schema = 'public'
    ORDER BY routine_name;
""")
result["db_functions"] = [{"name": r[0], "type": r[1]} for r in cur.fetchall()]

# Check if there's a prices view
cur.execute("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")
result["all_tables_with_type"] = [{"name": r[0], "type": r[1]} for r in cur.fetchall()]

cur.close()
conn.close()

with open("check_city2_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done -> check_city2_result.json")
