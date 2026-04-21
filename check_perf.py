import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Check indexes on stores.city
cur.execute("""
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'stores'
ORDER BY indexname
""")
print("Stores indexes:")
for r in cur.fetchall():
    print(" ", r[0], "->", r[1])

# Check get_top_city_promotions function body
cur.execute("SELECT prosrc FROM pg_proc WHERE proname = 'get_top_city_promotions'")
row = cur.fetchone()
if row:
    body = row[0]
    print("\nget_top_city_promotions body (first 1500 chars):")
    print(body[:1500])
else:
    print("Function not found")

conn.close()
