import psycopg2, os

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Check promo_kind distribution in cache for אושר עד
cur.execute("""
SELECT promo_kind, promo_label, COUNT(*) 
FROM top_promotions_cache 
WHERE chain_id = '7290103152017' AND window_hours = 24
GROUP BY promo_kind, promo_label
ORDER BY COUNT(*) DESC
""")
rows = cur.fetchall()
print("אושר עד promo_kind distribution (24h cache):")
for r in rows:
    print(f"  kind={r[0]}, label={r[1]}, count={r[2]}")

# Check ALL chains - what % of their promos are 'club'
cur.execute("""
SELECT 
    chain_name,
    COUNT(*) FILTER (WHERE promo_kind = 'club') AS club_count,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE promo_kind = 'club') / COUNT(*), 1) AS club_pct
FROM top_promotions_cache 
WHERE window_hours = 24 AND scope_type = 'city'
GROUP BY chain_name
ORDER BY club_pct DESC
LIMIT 20
""")
rows = cur.fetchall()
print("\nChains with highest % of 'club' promos in 24h city cache:")
for r in rows:
    print(f"  {r[0]:<30} club={r[1]}/{r[2]} ({r[3]}%)")

# Inspect the actual promotions metadata for אושר עד
cur.execute("""
SELECT 
    promo.promotion_id,
    promo.promotion_description,
    promo.club_id,
    promo.additional_is_coupon,
    promo.chain_id
FROM promotions promo
WHERE promo.chain_id = '7290103152017'
LIMIT 10
""")
rows = cur.fetchall()
print("\nSample promotions for אושר עד:")
for r in rows:
    print(f"  id={r[0]}, desc={r[1]}, club_id={r[2]}, coupon={r[3]}")

conn.close()
