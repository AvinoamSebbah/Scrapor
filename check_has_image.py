import psycopg2, os

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("SELECT has_image, COUNT(*) FROM top_promotions_cache WHERE window_hours=24 GROUP BY has_image ORDER BY has_image NULLS LAST")
print("24h distribution:", cur.fetchall())

cur.execute("SELECT item_code, has_image, scope_type FROM top_promotions_cache WHERE item_code=%s AND window_hours=24 LIMIT 5", ("8025789000517",))
print("item 8025789000517:", cur.fetchall())

conn.close()
