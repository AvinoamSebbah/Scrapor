import psycopg2, psycopg2.extras, os
conn = psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=psycopg2.extras.RealDictCursor)
cur = conn.cursor()
cur.execute("UPDATE observations SET min_discount_pct = 20")
conn.commit()
print("Done:", cur.rowcount, "row(s) updated")
conn.close()
