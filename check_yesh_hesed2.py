#!/usr/bin/env python3
"""
Investigation: Shufersal sub-chain splitting & יש חסד in Jerusalem
Output to JSON to avoid encoding issues
"""
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# 1. Distribution des chains
cur.execute("""
    SELECT chain_name, COUNT(*) as nb
    FROM stores
    GROUP BY chain_name
    ORDER BY nb DESC
    LIMIT 30;
""")
result["1_chain_distribution"] = [{"chain_name": r[0], "count": r[1]} for r in cur.fetchall()]

# 2. Toutes les sous-chaines Shufersal
cur.execute("""
    SELECT chain_name, COUNT(*) as nb
    FROM stores
    WHERE chain_name ILIKE '%שופרסל%'
       OR chain_name IN ('יש חסד', 'BE', 'יש', 'יוניברס', 'גוד מרקט', 'שופרסל דיל', 'שופרסל אקספרס', 'שופרסל שלי', 'שופרסל ONLINE')
    GROUP BY chain_name
    ORDER BY nb DESC;
""")
result["2_shufersal_subchains"] = [{"chain_name": r[0], "count": r[1]} for r in cur.fetchall()]

# 3. Magasins יש חסד
cur.execute("""
    SELECT store_id, store_name, chain_name, city, address
    FROM stores
    WHERE chain_name = 'יש חסד'
       OR store_name ILIKE '%יש חסד%'
    ORDER BY city, store_name
    LIMIT 50;
""")
result["3_yesh_hesed_stores"] = [
    {"store_id": r[0], "store_name": r[1], "chain_name": r[2], "city": r[3], "address": r[4]}
    for r in cur.fetchall()
]

# 4. Stores avec יש dans le nom
cur.execute("""
    SELECT store_id, store_name, chain_name, city, address
    FROM stores
    WHERE store_name ILIKE '%יש%'
    ORDER BY chain_name, city
    LIMIT 60;
""")
result["4_stores_with_yesh_in_name"] = [
    {"store_id": r[0], "store_name": r[1], "chain_name": r[2], "city": r[3], "address": r[4]}
    for r in cur.fetchall()
]

# 5. Jerusalem stores par chain
cur.execute("""
    SELECT chain_name, COUNT(*) as nb
    FROM stores
    WHERE city ILIKE '%ירושלים%'
    GROUP BY chain_name
    ORDER BY nb DESC;
""")
result["5_jerusalem_chains"] = [{"chain_name": r[0], "count": r[1]} for r in cur.fetchall()]

# 6. יש חסד Jerusalem stores detail
cur.execute("""
    SELECT store_id, store_name, chain_name, city, address
    FROM stores
    WHERE (chain_name = 'יש חסד' OR store_name ILIKE '%יש חסד%')
    AND city ILIKE '%ירושלים%'
    ORDER BY store_name;
""")
result["6_yesh_hesed_jerusalem"] = [
    {"store_id": r[0], "store_name": r[1], "chain_name": r[2], "city": r[3], "address": r[4]}
    for r in cur.fetchall()
]

# 7. Check prices table
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name IN ('prices', 'store_prices', 'products', 'items')
    ORDER BY table_name;
""")
tables = [r[0] for r in cur.fetchall()]
result["7_available_price_tables"] = tables

# 8. Count prices for יש חסד
if 'prices' in tables:
    cur.execute("""
        SELECT s.store_id, s.store_name, s.city, COUNT(p.item_code) as price_count
        FROM prices p
        JOIN stores s ON s.store_id = p.store_id
        WHERE (s.chain_name = 'יש חסד' OR s.store_name ILIKE '%יש חסד%')
        GROUP BY s.store_id, s.store_name, s.city
        ORDER BY s.city, s.store_name;
    """)
    result["8_prices_per_yesh_hesed_store"] = [
        {"store_id": r[0], "store_name": r[1], "city": r[2], "price_count": r[3]}
        for r in cur.fetchall()
    ]
else:
    result["8_prices_per_yesh_hesed_store"] = "no prices table"

# 9. Sample raw Shufersal stores (before split - detect if any remain incorrectly)
cur.execute("""
    SELECT store_id, store_name, chain_name, city
    FROM stores
    WHERE chain_name = 'שופרסל'
    ORDER BY city, store_name
    LIMIT 40;
""")
result["9_remaining_raw_shufersal"] = [
    {"store_id": r[0], "store_name": r[1], "chain_name": r[2], "city": r[3]}
    for r in cur.fetchall()
]

# 10. Check if split SQL was ever applied - look for total shufersal family
cur.execute("""
    SELECT chain_name, COUNT(*) 
    FROM stores 
    WHERE chain_name IN ('שופרסל', 'יש חסד', 'BE', 'יש', 'יוניברס', 'גוד מרקט', 'שופרסל דיל', 'שופרסל אקספרס', 'שופרסל שלי', 'שופרסל ONLINE')
    GROUP BY chain_name
    ORDER BY chain_name;
""")
result["10_shufersal_family_full"] = [{"chain_name": r[0], "count": r[1]} for r in cur.fetchall()]

# 11. How many stores have יש חסד in store_name but different chain_name
cur.execute("""
    SELECT store_id, store_name, chain_name, city
    FROM stores
    WHERE store_name ILIKE '%יש חסד%'
    AND chain_name != 'יש חסד'
    LIMIT 20;
""")
result["11_yesh_hesed_wrong_chain"] = [
    {"store_id": r[0], "store_name": r[1], "chain_name": r[2], "city": r[3]}
    for r in cur.fetchall()
]

cur.close()
conn.close()

# Write to JSON
with open("check_yesh_hesed_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("Result written to check_yesh_hesed_result.json")
