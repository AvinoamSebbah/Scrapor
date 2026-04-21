import psycopg2, os
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()
cur.execute("DELETE FROM top_promotions_cache WHERE scope_type IN ('city', 'chain')")
print("Deleted:", cur.rowcount, "rows (city + chain scopes)")
conn.commit()
conn.close()
