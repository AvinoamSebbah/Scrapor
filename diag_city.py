import psycopg2, os

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("SELECT COUNT(*) FROM top_promotions_cache WHERE window_hours=24 AND scope_type='city' AND has_image IS TRUE AND city ILIKE 'ירושלים%'")
print("city rows Jerusalem:", cur.fetchone())

cur.execute("SELECT MAX(rank_position) FROM top_promotions_cache WHERE window_hours=24 AND scope_type='city' AND has_image IS TRUE AND city ILIKE 'ירושלים%'")
print("max rank_position:", cur.fetchone())

cur.execute("SELECT COUNT(DISTINCT chain_id), COUNT(DISTINCT store_id) FROM top_promotions_cache WHERE window_hours=24 AND scope_type='city' AND has_image IS TRUE AND city ILIKE 'ירושלים%'")
print("chains/stores:", cur.fetchone())

# Distribution rank_position - max par combien de groupes ?
cur.execute("SELECT chain_id, COUNT(*) as cnt, MAX(rank_position) as max_rank FROM top_promotions_cache WHERE window_hours=24 AND scope_type='city' AND has_image IS TRUE AND city ILIKE 'ירושלים%' GROUP BY chain_id ORDER BY cnt DESC LIMIT 5")
print("par chain:", cur.fetchall())

conn.close()
