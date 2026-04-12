#!/usr/bin/env python3
"""Quick diagnostic: why so few promotions in cache?"""
import os, psycopg2
from psycopg2.extras import RealDictCursor

url = os.getenv("DATABASE_URL") or os.getenv("POSTGRESQL_URL")
conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
conn.autocommit = True
cur = conn.cursor()

print("=== 1. Cache top_promotions_cache (tous windows/scopes) ===")
cur.execute("""
    SELECT window_hours, scope_type, city, COUNT(*) as cnt
    FROM top_promotions_cache
    GROUP BY window_hours, scope_type, city
    ORDER BY window_hours, scope_type, city
    LIMIT 30
""")
rows = cur.fetchall()
if not rows:
    print("  CACHE VIDE")
else:
    for r in rows:
        print(f"  window={r['window_hours']}h  scope={r['scope_type']:<6}  city={str(r['city']):<22}  {r['cnt']} lignes")

print()
print("=== 2. Recency de promotion_store_items ===")
cur.execute("""
    SELECT
      SUM(CASE WHEN updated_at >= NOW()-INTERVAL '24 hours'  THEN 1 ELSE 0 END)::int  as h24,
      SUM(CASE WHEN updated_at >= NOW()-INTERVAL '168 hours' THEN 1 ELSE 0 END)::int  as h168,
      SUM(CASE WHEN updated_at >= NOW()-INTERVAL '720 hours' THEN 1 ELSE 0 END)::int  as h720,
      COUNT(*)::int as total
    FROM promotion_store_items
    WHERE promo_price IS NOT NULL AND promo_price > 0
      AND (promotion_end_date IS NULL OR promotion_end_date >= CURRENT_DATE)
""")
r = cur.fetchone()
print(f"  last 24h={r['h24']}   last 7d={r['h168']}   last 30d={r['h720']}   total={r['total']}")

print()
print("=== 3. Distribution par semaine (promotion_store_items actifs) ===")
cur.execute("""
    SELECT DATE_TRUNC('week', updated_at)::date as week, COUNT(*)::int as cnt
    FROM promotion_store_items
    WHERE promo_price IS NOT NULL AND promo_price > 0
    GROUP BY week ORDER BY week DESC LIMIT 8
""")
for r in cur.fetchall():
    print(f"  week={r['week']}  {r['cnt']:>8} lignes")

print()
print("=== 4. Top villes dans `stores` ===")
cur.execute("""
    SELECT city, COUNT(*)::int as cnt
    FROM stores
    WHERE city IS NOT NULL AND city <> ''
    GROUP BY city ORDER BY cnt DESC LIMIT 15
""")
for r in cur.fetchall():
    print(f"  city={str(r['city']):<25}  {r['cnt']} magasins")

print()
print("=== 5. Promotions sans correspondance product_prices (bloquant pour le cache) ===")
cur.execute("""
    SELECT COUNT(*)::int as no_price
    FROM promotion_store_items psi
    WHERE psi.promo_price IS NOT NULL
      AND psi.promo_price > 0
      AND NOT EXISTS (
        SELECT 1 FROM product_prices pp
        WHERE pp.product_id = psi.product_id
          AND pp.store_id = psi.store_id
          AND pp.price IS NOT NULL
          AND pp.price > 0
      )
""")
r = cur.fetchone()
print(f"  Promos actives SANS product_price matching: {r['no_price']:,}")

conn.close()
print("\nDone.")
