#!/usr/bin/env python3
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# Schema of product_prices
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'product_prices'
    ORDER BY ordinal_position;
""")
result["product_prices_columns"] = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]

# Sample product_prices
cur.execute("SELECT * FROM product_prices LIMIT 3;")
cols = [d[0] for d in cur.description]
result["product_prices_sample_cols"] = cols
result["product_prices_sample"] = [dict(zip(cols, [str(v) for v in r])) for r in cur.fetchall()]

# יש חסד Jerusalem stores (chain_id + store_id combos)
cur.execute("""
    SELECT id, chain_id, store_id, store_name, city
    FROM stores
    WHERE (chain_name = 'יש חסד' OR store_name ILIKE '%יש חסד%')
    AND city ILIKE '%ירושלים%'
    ORDER BY store_name;
""")
jlm_stores = cur.fetchall()
result["jlm_yesh_hesed_stores_raw"] = [
    {"db_id": r[0], "chain_id": r[1], "store_id": r[2], "store_name": r[3], "city": r[4]}
    for r in jlm_stores
]

# Now count product_prices for these stores
for store in jlm_stores:
    db_id, chain_id, store_id, store_name, city = store
    cur.execute("""
        SELECT COUNT(*) FROM product_prices
        WHERE chain_id = %s AND store_id = %s;
    """, (chain_id, store_id))
    count = cur.fetchone()[0]
    result.setdefault("jlm_yesh_hesed_price_counts", []).append({
        "db_id": db_id,
        "chain_id": chain_id,
        "store_id": store_id,
        "store_name": store_name,
        "count": count
    })

# Also check ALL יש חסד stores
cur.execute("""
    SELECT s.id, s.chain_id, s.store_id, s.store_name, s.city,
           COUNT(pp.id) as price_count
    FROM stores s
    LEFT JOIN product_prices pp ON pp.chain_id = s.chain_id AND pp.store_id = s.store_id  
    WHERE s.chain_name = 'יש חסד' OR s.store_name ILIKE '%יש חסד%'
    GROUP BY s.id, s.chain_id, s.store_id, s.store_name, s.city
    ORDER BY s.city, s.store_name;
""")
result["all_yesh_hesed_price_counts"] = [
    {"db_id": r[0], "chain_id": r[1], "store_id": r[2], "store_name": r[3], "city": r[4], "price_count": r[5]}
    for r in cur.fetchall()
]

# Check the total product_prices count and date range
cur.execute("SELECT COUNT(*), MIN(price_update_date), MAX(price_update_date) FROM product_prices;")
r = cur.fetchone()
result["product_prices_total"] = {"count": r[0], "min_date": str(r[1]), "max_date": str(r[2])}

# Check if product_prices join is on chain_id or something else
cur.execute("SELECT * FROM product_prices LIMIT 2;")
cols = [d[0] for d in cur.description]
# just re-confirm column names
result["product_prices_columns_confirmed"] = cols

cur.close()
conn.close()

with open("check_prices2_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done -> check_prices2_result.json")
