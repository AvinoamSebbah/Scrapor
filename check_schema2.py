#!/usr/bin/env python3
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

result = {}

# Check all tables
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name;
""")
result["all_tables"] = [r[0] for r in cur.fetchall()]

# Schema of products table
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'products'
    ORDER BY ordinal_position;
""")
result["products_columns"] = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]

# Schema of stores table
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'stores'
    ORDER BY ordinal_position;
""")
result["stores_columns"] = [{"col": r[0], "type": r[1]} for r in cur.fetchall()]

# Sample products row
cur.execute("SELECT * FROM products LIMIT 3;")
cols = [d[0] for d in cur.description]
result["products_sample_cols"] = cols
result["products_sample"] = [dict(zip(cols, [str(v) for v in r])) for r in cur.fetchall()]

cur.close()
conn.close()

with open("check_schema_result.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done")
