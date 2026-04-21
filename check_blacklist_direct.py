import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Find שיבאס in products table
cur.execute("""
SELECT id, item_code, item_name
FROM products
WHERE item_name ILIKE '%שיבאס%'
LIMIT 5
""")
rows = cur.fetchall()
print(f"שיבאס in products table: {len(rows)}")
for r in rows:
    print(f"  id={r[0]}, code={r[1]}, name={r[2]}")
    print(f"  bytes: {r[2].encode('utf-8').hex()}")

# Now test the EXACT condition used in the function
cur.execute("""
SELECT COUNT(*) FROM promotion_store_items psi
JOIN stores s ON s.id = psi.store_id
JOIN product_prices pp ON pp.product_id = psi.product_id AND pp.store_id = psi.store_id
JOIN products p ON p.id = psi.product_id
WHERE COALESCE(s.city, '') <> ''
  AND psi.promo_price IS NOT NULL
  AND psi.promo_price > 0
  AND pp.price IS NOT NULL
  AND pp.price > 0
  AND psi.promo_price < pp.price
  AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
  AND p.item_name ILIKE '%שיבאס%'
""")
count_before = cur.fetchone()[0]
print(f"\nשיבאס rows BEFORE blacklist filter: {count_before}")

# Same WITH the blacklist filter applied
cur.execute("""
SELECT COUNT(*) FROM promotion_store_items psi
JOIN stores s ON s.id = psi.store_id
JOIN product_prices pp ON pp.product_id = psi.product_id AND pp.store_id = psi.store_id
JOIN products p ON p.id = psi.product_id
WHERE COALESCE(s.city, '') <> ''
  AND psi.promo_price IS NOT NULL
  AND psi.promo_price > 0
  AND pp.price IS NOT NULL
  AND pp.price > 0
  AND psi.promo_price < pp.price
  AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
  AND p.item_name ILIKE '%שיבאס%'
  AND p.item_name NOT ILIKE ANY(ARRAY['%שיבאס%'])
""")
count_after = cur.fetchone()[0]
print(f"שיבאס rows AFTER blacklist filter: {count_after}")

conn.close()
