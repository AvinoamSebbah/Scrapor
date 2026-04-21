#!/usr/bin/env python3
"""
Check products/prices for יש חסד Jerusalem stores
"""
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# יש חסד Jerusalem store IDs: 364, 608, 610, 496
YH_JLM_IDS = ['364', '608', '610', '496']

# Check what the products table structure looks like
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'products'
    ORDER BY ordinal_position;
""")
result["products_schema"] = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]

# Check all tables
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")
result["all_tables"] = [r[0] for r in cur.fetchall()]

# Count products per store for יש חסד Jerusalem
cur.execute("""
    SELECT p.store_id, s.store_name, s.city, COUNT(*) as product_count
    FROM products p
    JOIN stores s ON s.store_id = p.store_id
    WHERE p.store_id = ANY(%s)
    GROUP BY p.store_id, s.store_name, s.city
    ORDER BY product_count DESC;
""", (YH_JLM_IDS,))
result["yesh_hesed_jlm_product_count"] = [
    {"store_id": r[0], "store_name": r[1], "city": r[2], "count": r[3]}
    for r in cur.fetchall()
]

# Sample products for store 364 (יש חסד בית וגן)
cur.execute("""
    SELECT store_id, item_code, item_name, item_price, price_update_date
    FROM products
    WHERE store_id = '364'
    LIMIT 10;
""")
result["sample_products_364"] = [
    {"store_id": r[0], "item_code": r[1], "item_name": r[2], "price": str(r[3]), "update_date": str(r[4])}
    for r in cur.fetchall()
]

# Check if products have recent data
cur.execute("""
    SELECT store_id, MAX(price_update_date) as latest_update, COUNT(*) as total
    FROM products
    WHERE store_id = ANY(%s)
    GROUP BY store_id
    ORDER BY store_id;
""", (YH_JLM_IDS,))
result["yesh_hesed_jlm_latest_update"] = [
    {"store_id": r[0], "latest_update": str(r[1]), "total_products": r[2]}
    for r in cur.fetchall()
]

# Check what is the API serving for stores in Jerusalem for chain יש חסד
# Look at how the API queries stores - check if there's a table for app/city/store mappings
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name ILIKE '%city%' OR table_name ILIKE '%app%' OR table_name ILIKE '%user%'
    ORDER BY table_name;
""")
result["relevant_tables"] = [r[0] for r in cur.fetchall()]

# Check if there's a store_promotions_cache or similar
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")
result["all_tables_full"] = [r[0] for r in cur.fetchall()]

cur.close()
conn.close()

with open("check_prices_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print("Done -> check_prices_result.json")
