import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
conn.autocommit = False
cur = conn.cursor()

# Call the function directly for 24h and check result immediately
print("Calling refresh_top_promotions_cache(24, 200)...")
cur.execute("SELECT refresh_top_promotions_cache(24, 200) AS affected")
affected = cur.fetchone()[0]
print(f"Affected: {affected}")

# Check for שיבאס RIGHT AFTER the function call, before commit
cur.execute("SELECT COUNT(*) FROM top_promotions_cache WHERE window_hours=24 AND item_name ILIKE '%שיבאס%'")
count = cur.fetchone()[0]
print(f"שיבאס in 24h cache (before commit): {count}")

# Also check ויסקי and יין
for term in ['%ויסקי%', '%יין%', '%בירה%', '%וודקה%']:
    cur.execute(f"SELECT COUNT(*) FROM top_promotions_cache WHERE window_hours=24 AND item_name ILIKE '{term}'")
    c = cur.fetchone()[0]
    print(f"  {term}: {c}")

conn.commit()

# After commit
cur.execute("SELECT COUNT(*) FROM top_promotions_cache WHERE window_hours=24 AND item_name ILIKE '%שיבאס%'")
count = cur.fetchone()[0]
print(f"\nשיבאס in 24h cache (after commit): {count}")

conn.close()
