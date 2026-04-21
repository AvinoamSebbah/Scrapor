import psycopg2, os
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
cur.execute("SELECT city, item_name, window_hours FROM top_promotions_cache WHERE item_name ILIKE '%שיבאס%' LIMIT 20")
rows = cur.fetchall()
print('Shibas in cache:', len(rows))
for r in rows:
    print(r)
conn.close()
