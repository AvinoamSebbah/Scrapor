#!/usr/bin/env python3
"""
Investigation: Shufersal sub-chain splitting & יש חסד in Jerusalem
"""
import os
import psycopg2

DB_URL = os.environ["DATABASE_URL"]

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

print("=" * 70)
print("1. DISTRIBUTION DES CHAINS (top 30)")
print("=" * 70)
cur.execute("""
    SELECT chain_name, COUNT(*) as nb
    FROM stores
    GROUP BY chain_name
    ORDER BY nb DESC
    LIMIT 30;
""")
for row in cur.fetchall():
    print(f"  {row[0]!r:40s} -> {row[1]} magasins")

print()
print("=" * 70)
print("2. TOUTES LES SOUS-CHAINES SHUFERSAL (chain_name contient 'שופרסל' ou chaines connues)")
print("=" * 70)
cur.execute("""
    SELECT chain_name, COUNT(*) as nb
    FROM stores
    WHERE chain_name ILIKE '%שופרסל%'
       OR chain_name = 'יש חסד'
       OR chain_name = 'BE'
       OR chain_name = 'יש'
       OR chain_name = 'יוניברס'
       OR chain_name = 'גוד מרקט'
    GROUP BY chain_name
    ORDER BY nb DESC;
""")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"  {row[0]!r:40s} -> {row[1]} magasins")
else:
    print("  AUCUNE")

print()
print("=" * 70)
print("3. MAGASINS 'יש חסד' DANS LA DB")
print("=" * 70)
cur.execute("""
    SELECT store_id, store_name, chain_name, city, address
    FROM stores
    WHERE chain_name = 'יש חסד'
       OR store_name ILIKE '%יש חסד%'
    ORDER BY city, store_name
    LIMIT 50;
""")
rows = cur.fetchall()
if rows:
    print(f"  Trouvé {len(rows)} magasin(s):")
    for row in rows:
        print(f"  ID={row[0]} | chain={row[2]!r} | ville={row[3]!r} | nom={row[1]!r} | addr={row[4]!r}")
else:
    print("  AUCUN MAGASIN יש חסד TROUVÉ!")

print()
print("=" * 70)
print("4. STORES SHUFERSAL AVEC 'יש' DANS LE NOM (pour voir si le split a marché)")
print("=" * 70)
cur.execute("""
    SELECT store_id, store_name, chain_name, city, address
    FROM stores
    WHERE store_name ILIKE '%יש%'
    ORDER BY chain_name, city
    LIMIT 50;
""")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"  chain={row[2]!r:20s} | ville={row[3]!r:20s} | nom={row[1]!r}")
else:
    print("  AUCUN")

print()
print("=" * 70)
print("5. STORES A JERUSALEM (ירושלים) - toutes les chaines")
print("=" * 70)
cur.execute("""
    SELECT chain_name, COUNT(*) as nb
    FROM stores
    WHERE city ILIKE '%ירושלים%'
       OR city = 'Jerusalem'
       OR city ILIKE '%jerusalem%'
    GROUP BY chain_name
    ORDER BY nb DESC;
""")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"  {row[0]!r:40s} -> {row[1]} magasins")
else:
    print("  AUCUN MAGASIN A JERUSALEM!")

print()
print("=" * 70)
print("6. PRODUITS POUR יש חסד JERUSALEM (via prices/store_prices)")
print("=" * 70)
# First check what tables exist
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name IN ('prices', 'store_prices', 'products', 'items')
    ORDER BY table_name;
""")
tables = [r[0] for r in cur.fetchall()]
print(f"  Tables disponibles: {tables}")

# Check if we have store_prices or prices table
if 'prices' in tables:
    cur.execute("""
        SELECT COUNT(DISTINCT p.item_code), COUNT(p.item_code)
        FROM prices p
        JOIN stores s ON s.store_id = p.store_id
        WHERE (s.chain_name = 'יש חסד' OR s.store_name ILIKE '%יש חסד%')
        AND (s.city ILIKE '%ירושלים%');
    """)
    row = cur.fetchone()
    print(f"  Prix dans יש חסד Jerusalem: {row[0]} produits distincts, {row[1]} entrées total")

print()
print("=" * 70)
print("7. STORES SHUFERSAL BRUTS (store_name contient יש חסד mais chain_name ≠ יש חסד)")
print("=" * 70)
cur.execute("""
    SELECT store_id, store_name, chain_name, city
    FROM stores
    WHERE store_name ILIKE '%יש חסד%'
    AND chain_name != 'יש חסד'
    LIMIT 20;
""")
rows = cur.fetchall()
if rows:
    print(f"  PROBLÈME: {len(rows)} store(s) avec 'יש חסד' dans le nom mais chain_name différent!")
    for row in rows:
        print(f"    chain={row[2]!r} | ville={row[3]!r} | nom={row[1]!r}")
else:
    print("  OK: Tous les stores 'יש חסד' ont bien le bon chain_name")

print()
print("=" * 70)
print("8. SAMPLE DE TOUS LES STORES SHUFERSAL (pour voir les noms bruts)")
print("=" * 70)
cur.execute("""
    SELECT DISTINCT chain_name, store_name, city
    FROM stores
    WHERE chain_name ILIKE '%שופרסל%'
       OR chain_name IN ('יש חסד', 'BE', 'יש', 'יוניברס', 'גוד מרקט', 'שופרסל דיל', 'שופרסל אקספרס', 'שופרסל שלי', 'שופרסל ONLINE')
    ORDER BY chain_name, city
    LIMIT 40;
""")
rows = cur.fetchall()
for row in rows:
    print(f"  chain={row[0]!r:25s} | ville={row[1]!r:25s} | nom={row[2]!r}")

cur.close()
conn.close()
print()
print("DONE.")
