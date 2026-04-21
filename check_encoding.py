import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Test ILIKE directly on an actual item name
cur.execute("""
SELECT 
    item_name,
    item_name ILIKE '%שיבאס%' AS matches_ilike,
    item_name ~ 'שיבאס' AS matches_regex
FROM top_promotions_cache
WHERE item_name ILIKE '%שיבאס%'
LIMIT 3
""")
rows = cur.fetchall()
for r in rows:
    name = r[0]
    print(f"item_name bytes: {name.encode('utf-8').hex()}")
    print(f"matches_ilike={r[1]}, matches_regex={r[2]}")
    print(f"name: {name}")
    print()

# Check the pattern itself
pattern = '%שיבאס%'
print(f"Pattern bytes: {pattern.encode('utf-8').hex()}")

# Now test if the function's exact blacklist pattern works
cur.execute("""
SELECT item_name
FROM top_promotions_cache
WHERE item_name ILIKE ANY(ARRAY[
    '%וויסקי%','%ויסקי%','%ווסקי%','%וודקה%',
    '%יין%','%ערק%','%בירה%','%רום%',
    '%ברנדי%','%קוניאק%','%שמפניה%','%ליקר%',
    '%סיגריות%','%טבק%','%סיגר%','%אלכוהול%',
    '%שיבאס%','%גלנליווט%','%גים בים%',
    '%ט.קוארבו%','%אוזו%','%פלומרי%',
    '%whisky%','%whiskey%','%% wine %%','%wine%','%% beer %%','%vodka%',
    '%brandy%','%cognac%','%champagne%','%liqueur%','%liquor%',
    '%tequila%','%rum %%','%% rum%','%bourbon%','%scotch%',
    '%cigarette%','%tobacco%','%cigar%'
])
LIMIT 5
""")
rows = cur.fetchall()
print(f"\nRows matching blacklist patterns directly: {len(rows)}")
for r in rows:
    print(r[0])

# Check what the function actually does - run it in EXPLAIN mode
cur.execute("""
SELECT COUNT(*) FROM promotion_store_items p
JOIN stores s ON s.id = p.store_id
WHERE p.item_name ILIKE '%שיבאס%'
  AND p.end_date >= CURRENT_DATE
LIMIT 1
""")
count = cur.fetchone()[0]
print(f"\nשיבאס in promotion_store_items (raw): {count}")

conn.close()
