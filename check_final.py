#!/usr/bin/env python3
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# Get all יש חסד stores with their db id (PK)
cur.execute("""
    SELECT id, chain_id, store_id, store_name, city
    FROM stores
    WHERE chain_name = 'יש חסד' OR store_name ILIKE '%יש חסד%'
    ORDER BY city, store_name;
""")
stores = cur.fetchall()
result["yesh_hesed_stores"] = [
    {"db_id": r[0], "chain_id": r[1], "store_id": r[2], "store_name": r[3], "city": r[4]}
    for r in stores
]

# For each store, count product_prices using the db id (PK)
price_counts = []
for store in stores:
    db_id, chain_id, store_id, store_name, city = store
    cur.execute("""
        SELECT COUNT(*), MAX(updated_at)
        FROM product_prices
        WHERE store_id = %s;
    """, (db_id,))
    r = cur.fetchone()
    price_counts.append({
        "db_id": db_id,
        "chain_id": chain_id,
        "store_id": store_id,
        "store_name": store_name,
        "city": city,
        "price_count": r[0],
        "last_update": str(r[1])
    })

result["yesh_hesed_price_counts"] = price_counts

# Filter Jerusalem ones
result["jerusalem_yesh_hesed"] = [p for p in price_counts if p["city"] and "ירושלים" in p["city"]]

# Total product_prices rows
cur.execute("SELECT COUNT(*) FROM product_prices;")
result["total_product_prices"] = cur.fetchone()[0]

# Which stores have prices in Jerusalem overall?
cur.execute("""
    SELECT s.chain_name, s.store_name, s.city, COUNT(pp.product_id) as price_count
    FROM stores s
    JOIN product_prices pp ON pp.store_id = s.id
    WHERE s.city ILIKE '%ירושלים%'
    GROUP BY s.chain_name, s.store_name, s.city
    ORDER BY price_count DESC
    LIMIT 20;
""")
result["jerusalem_stores_with_prices"] = [
    {"chain_name": r[0], "store_name": r[1], "city": r[2], "price_count": r[3]}
    for r in cur.fetchall()
]

cur.close()
conn.close()

with open("check_final_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done -> check_final_result.json")
