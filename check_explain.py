import psycopg2, os, time

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# EXPLAIN ANALYZE the top_promotions_cache query for city scope
t = time.time()
cur.execute("""
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT *
FROM top_promotions_cache
WHERE window_hours = 24
  AND scope_type = 'city'
  AND city = 'ירושלים'
ORDER BY rank_position ASC
LIMIT 200 OFFSET 0
""")
rows = cur.fetchall()
print(f"Cache query: {(time.time()-t)*1000:.0f}ms")
for r in rows:
    print(r[0])

print("\n---\n")

# EXPLAIN ANALYZE the stores DISTINCT query
t = time.time()
cur.execute("""
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT DISTINCT
  s.chain_id,
  COALESCE(NULLIF(s.chain_name, ''), s.chain_id)::text AS chain_name
FROM stores s
WHERE s.city ILIKE 'ירושלים' || '%'
ORDER BY chain_name ASC
LIMIT 100
""")
rows = cur.fetchall()
print(f"Stores DISTINCT query: {(time.time()-t)*1000:.0f}ms")
for r in rows:
    print(r[0])

conn.close()
