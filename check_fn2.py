import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

cur.execute("SELECT prosrc FROM pg_proc WHERE proname = 'refresh_top_promotions_cache'")
row = cur.fetchone()
body = row[0]

# Find DELETE/TRUNCATE section
for keyword in ['DELETE', 'TRUNCATE', 'INSERT INTO top_promotions_cache']:
    idx = body.find(keyword)
    if idx >= 0:
        print(f"Found '{keyword}' at index {idx}:")
        print(body[idx:idx+200])
        print("---")
    else:
        print(f"NOT FOUND: {keyword}")

# Also check the alcohol items - are they in promotion_store_items (source)?
cur.execute("""
SELECT p.item_name, p.price, p.promo_price
FROM promotion_store_items p
WHERE p.item_name ILIKE '%וויסקי%'
  AND p.end_date >= CURRENT_DATE
LIMIT 5
""")
rows = cur.fetchall()
print(f"\nWhisky in source data: {len(rows)}")
for r in rows:
    print(r)

conn.close()
