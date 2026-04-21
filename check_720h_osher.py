import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# 1. What window_hours exist in cache?
cur.execute("SELECT DISTINCT window_hours FROM top_promotions_cache ORDER BY window_hours")
print("window_hours in cache:", [r[0] for r in cur.fetchall()])

# 2. אושר עד stores in Jerusalem
cur.execute("""
SELECT s.id, s.chain_id, s.store_id, s.store_name, s.city
FROM stores s
WHERE s.chain_id = '7290103152017'
  AND s.city ILIKE '%ירושלים%'
""")
rows = cur.fetchall()
print(f"\nאושר עד stores in Jerusalem: {len(rows)}")
for r in rows: print(" ", r)

# 3. אושר עד in 24h cache for Jerusalem
cur.execute("""
SELECT COUNT(*), scope_type
FROM top_promotions_cache
WHERE window_hours = 24
  AND city = 'ירושלים'
  AND chain_id = '7290103152017'
GROUP BY scope_type
""")
rows = cur.fetchall()
print(f"\nאושר עד in 24h cache (Jerusalem):")
for r in rows: print(" ", r)

# 4. אושר עד any city in 24h cache
cur.execute("""
SELECT COUNT(*)
FROM top_promotions_cache
WHERE window_hours = 24
  AND chain_id = '7290103152017'
""")
print(f"אושר עד total in 24h cache: {cur.fetchone()[0]}")

# 5. Active promos for אושר עד in Jerusalem stores
cur.execute("""
SELECT COUNT(*)
FROM promotion_store_items psi
JOIN stores s ON s.id = psi.store_id
WHERE s.chain_id = '7290103152017'
  AND s.city ILIKE '%ירושלים%'
  AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
  AND psi.updated_at >= NOW() - INTERVAL '24 hours'
""")
print(f"Active promos for אושר עד Jerusalem (24h): {cur.fetchone()[0]}")

conn.close()
