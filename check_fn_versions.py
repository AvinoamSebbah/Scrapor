import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Check ALL versions of refresh_top_promotions_cache
cur.execute("""
SELECT proname, oid, pronargs, proargtypes::text, 
       pg_get_function_arguments(oid) AS args
FROM pg_proc 
WHERE proname = 'refresh_top_promotions_cache'
ORDER BY oid
""")
rows = cur.fetchall()
print(f"Versions of refresh_top_promotions_cache: {len(rows)}")
for r in rows:
    print(f"  oid={r[1]}, args=({r[4]})")

# Check each version's blacklist
for row in rows:
    oid = row[1]
    cur.execute("SELECT prosrc FROM pg_proc WHERE oid = %s", (oid,))
    body = cur.fetchone()[0]
    has_shibas = 'שיבאס' in body
    has_blacklist = 'NOT ILIKE ANY' in body
    print(f"  oid={oid}: has_שיבאס={has_shibas}, has_blacklist={has_blacklist}")
    # Show first 200 chars after DELETE
    idx = body.find('DELETE')
    if idx >= 0:
        print(f"    starts with: {body[idx:idx+100]}")

conn.close()
