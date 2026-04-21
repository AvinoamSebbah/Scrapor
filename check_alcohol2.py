import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# What cities exist in 24h cache
cur.execute("SELECT DISTINCT city FROM top_promotions_cache WHERE window_hours=24 AND scope_type='city' ORDER BY city")
cities = cur.fetchall()
print('Cities in 24h city cache:', [r[0] for r in cities])

# Check alcohol in 24h city cache
cur.execute("""
SELECT city, item_name, discount_percent, scope_type
FROM top_promotions_cache
WHERE window_hours = 24
  AND scope_type = 'city'
  AND item_name ILIKE ANY(ARRAY[
    '%ויסקי%','%וודקה%','%יין%','%ערק%','%בירה%','%שיבאס%',
    '%גלנליווט%','%גים בים%','%ט.קוארבו%','%אוזו%','%פלומרי%',
    '%whisky%','%whiskey%','%wine%','%beer%','%vodka%',
    '%tequila%','%rum%','%bourbon%','%scotch%',
    '%cigarette%','%tobacco%','%סיגריות%','%ליקר%'
  ])
ORDER BY smart_score DESC LIMIT 20
""")
rows = cur.fetchall()
print(f'\nAlcohol items in 24h city cache: {len(rows)}')
for r in rows:
    print(r)

# Cache summary
cur.execute("SELECT window_hours, scope_type, COUNT(*) FROM top_promotions_cache GROUP BY window_hours, scope_type ORDER BY window_hours, scope_type")
counts = cur.fetchall()
print('\nCache summary:')
for r in counts:
    print(f'  window={r[0]}h  scope={r[1]:10s}  {r[2]:,} rows')

# Check refreshed_at
cur.execute("SELECT window_hours, MAX(refreshed_at) FROM top_promotions_cache GROUP BY window_hours ORDER BY window_hours")
for r in cur.fetchall():
    print(f'  window={r[0]}h  last_refresh={r[1]}')

conn.close()
