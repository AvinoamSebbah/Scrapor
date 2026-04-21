import psycopg2, os

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

# Quelles chain_names sont stockées pour chain_id=7290803800003 (Shufersal) en scope chain?
cur.execute("""
SELECT chain_name, COUNT(*) as cnt
FROM top_promotions_cache
WHERE window_hours=24 AND scope_type='chain'
  AND chain_id='7290803800003'
  AND city ILIKE 'ירושלים%'
GROUP BY chain_name ORDER BY cnt DESC
""")
print("chain scope - chain_names pour Shufersal Jeru:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

# Et en scope store?
cur.execute("""
SELECT chain_name, COUNT(*) as cnt
FROM top_promotions_cache
WHERE window_hours=24 AND scope_type='store'
  AND chain_id='7290803800003'
  AND city ILIKE 'ירושלים%'
GROUP BY chain_name ORDER BY cnt DESC
""")
print("\nstore scope - chain_names pour Shufersal Jeru:")
for row in cur.fetchall():
    print(f"  {row[0]}: {row[1]}")

conn.close()
