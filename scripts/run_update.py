import os
import psycopg2

DB_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DB_URL)
conn.autocommit = True

with open("c:/Users/avinoams/Dev/Agali/AGALI/scrapor/scripts/update_chain_name_filter.sql", "r", encoding="utf-8") as f:
    sql = f.read()

with conn.cursor() as cur:
    cur.execute(sql)

print("Applied SQL updates successfully.")
