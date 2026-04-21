import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Find Jerusalem city_id
cur.execute("SELECT DISTINCT city FROM top_promotions_cache WHERE window_hours=24 AND scope='city' ORDER BY city")
cities = cur.fetchall()
print('Cities in 24h city cache:', [r[0] for r in cities])

# Check alcohol in ANY city, 24h cache
cur.execute("""
SELECT city, item_name, discount_pct, window_hours, scope
FROM top_promotions_cache
WHERE window_hours = 24
  AND scope = 'city'
  AND item_name ILIKE ANY(ARRAY[
    '%ויסקי%','%וודקה%','%יין%','%ערק%','%בירה%','%שיבאס%',
    '%גלנליווט%','%גים בים%','%ט.קוארבו%','%אוזו%','%פלומרי%',
    '%whisky%','%whiskey%','%wine%','%beer%','%vodka%',
    '%tequila%','%rum%','%bourbon%','%scotch%',
    '%cigarette%','%tobacco%','%סיגריות%'
  ])
ORDER BY score DESC LIMIT 20
""")
rows = cur.fetchall()
print(f'\nAlcohol items in 24h city cache: {len(rows)}')
for r in rows:
    print(r)

# Check what's in cache for 24h vs 168h
cur.execute("SELECT window_hours, scope, COUNT(*) FROM top_promotions_cache GROUP BY window_hours, scope ORDER BY window_hours, scope")
counts = cur.fetchall()
print('\nCache summary:')
for r in counts:
    print(f'  window={r[0]}h  scope={r[1]:10s}  {r[2]:,} rows')

conn.close()
