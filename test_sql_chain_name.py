import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("SELECT chain_name, count(*) FROM get_top_city_promotions('ירושלים', '7290027600007', NULL, 720, 50, 0, 'score') GROUP BY chain_name;")
print("Without chainName:", cur.fetchall())

cur.execute("SELECT chain_name, count(*) FROM get_top_city_promotions('ירושלים', '7290027600007', NULL, 720, 50, 0, 'score', 'יש חסד'::text) GROUP BY chain_name;")
print("With chainName יש חסד:", cur.fetchall())

cur.close()
conn.close()
