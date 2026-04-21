import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Schema
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='top_promotions_cache' ORDER BY ordinal_position")
print("Schema:")
for r in cur.fetchall():
    print(" ", r)

# Sample rows
cur.execute("SELECT * FROM top_promotions_cache LIMIT 2")
print("\nSample:", cur.fetchall())

conn.close()
