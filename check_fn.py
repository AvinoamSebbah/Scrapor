import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Get the actual function body to see if blacklist is there
cur.execute("""
SELECT prosrc FROM pg_proc WHERE proname = 'refresh_top_promotions_cache'
""")
row = cur.fetchone()
if row:
    body = row[0]
    # Find the blacklist section
    idx = body.find('NOT ILIKE ANY')
    if idx >= 0:
        print("Found blacklist at index", idx)
        print(body[idx:idx+800])
    else:
        print("NO BLACKLIST FOUND IN FUNCTION!")
        print(body[:500])
else:
    print("Function not found!")

conn.close()
