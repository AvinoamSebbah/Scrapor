import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Check if promotion_store_items has its own item_name
cur.execute("""
SELECT column_name FROM information_schema.columns
WHERE table_name = 'promotion_store_items'
ORDER BY ordinal_position
""")
cols = [r[0] for r in cur.fetchall()]
print("promotion_store_items columns:", cols)

# Find שיבאס in promotion_store_items vs products
cur.execute("""
SELECT 
    psi.item_name AS psi_item_name,
    p.item_name AS products_item_name
FROM promotion_store_items psi
JOIN products p ON p.id = psi.product_id
WHERE psi.item_name ILIKE '%שיבאס%'
   OR p.item_name ILIKE '%שיבאס%'
LIMIT 5
""")
rows = cur.fetchall()
print(f"\nשיבאס source comparison ({len(rows)} rows):")
for r in rows:
    print(f"  psi.item_name: {r[0]}")
    print(f"  p.item_name:   {r[1]}")
    print()

conn.close()
