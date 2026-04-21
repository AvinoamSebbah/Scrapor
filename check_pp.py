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

# Sample product_prices - just get raw cols
cur.execute("SELECT * FROM product_prices LIMIT 2;")
cols = [d[0] for d in cur.description]
result["product_prices_cols"] = cols
rows = cur.fetchall()
result["product_prices_sample"] = [dict(zip(cols, [str(v) for v in r])) for r in rows]

cur.close()
conn.close()

with open("check_pp_schema.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done")
