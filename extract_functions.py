#!/usr/bin/env python3
"""
Extrait les définitions actuelles des 3 fonctions SQL et les sauvegarde
pour référence, puis crée le script SQL mis à jour avec p_chain_name.
"""
import os
import psycopg2
import json

DB_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

funcs = ['get_offers_for_item_code', 'get_city_offers_for_search', 'get_top_city_promotions']
result = {}
for fn in funcs:
    cur.execute("SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = %s;", (fn,))
    row = cur.fetchone()
    result[fn] = row[0] if row else None

cur.close()
conn.close()

with open("current_functions.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)
print("Done -> current_functions.json")
