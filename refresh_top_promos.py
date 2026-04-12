#!/usr/bin/env python3
"""
refresh_top_promos.py
─────────────────────
Peuple top_promotions_cache via refresh_top_promotions_cache().

Contourne l'erreur "No space left on device" :
  - Exécute tout dans une seule transaction
  - SET LOCAL éteint les parallel workers (qui écrivent dans /tmp)
  - SET LOCAL temp_file_limit = -1 lève la limite de fichiers temporaires

Usage:
    python refresh_top_promos.py
    python refresh_top_promos.py --window-hours 168 --top-n 200
    python refresh_top_promos.py --skip-audit   # refresh rapide sans audit

Requiert POSTGRESQL_URL ou DATABASE_URL dans l'environnement.
"""

import argparse
import os
import sys
import time

import psycopg2
from psycopg2.extras import RealDictCursor


# ─── Connexion ──────────────────────────────────────────────────────────────


def _db_url():
    url = (
        os.getenv("POSTGRESQL_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
    )
    if not url:
        print("❌  Aucune variable POSTGRESQL_URL / DATABASE_URL trouvée.")
        sys.exit(1)
    return url


def _connect(db_url: str):
    """Connexion en mode autocommit=False (transaction explicite)."""
    conn = psycopg2.connect(
        db_url,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        cursor_factory=RealDictCursor,
        application_name="refresh_top_promos_script",
    )
    conn.autocommit = False  # on gère nous-mêmes BEGIN / COMMIT
    return conn


def _banner(msg: str):
    print(f"\n{'─'*55}")
    print(f"  {msg}")
    print(f"{'─'*55}")


# ─── Audit léger (sans JOIN lourd) ──────────────────────────────────────────


def _audit(conn, window_hours: int):
    _banner("Audit DB")
    with conn.cursor() as cur:
        # Nombre brut de promotion_store_items (pas de JOIN)
        cur.execute("SELECT COUNT(*)::bigint AS cnt FROM promotion_store_items;")
        total_psi = cur.fetchone()["cnt"]
        print(f"  promotion_store_items (total)  : {total_psi:,}")

        # Promos actives non expirées avec un prix de promo > 0
        cur.execute("""
            SELECT COUNT(*)::bigint AS cnt
            FROM promotion_store_items
            WHERE promo_price IS NOT NULL
              AND promo_price > 0
              AND (promotion_end_date IS NULL OR promotion_end_date >= CURRENT_DATE);
        """)
        print(f"  Promos actives (non expirées)  : {cur.fetchone()['cnt']:,}")

        # État actuel du cache (table très petite → rapide)
        cur.execute("""
            SELECT window_hours, scope_type, COUNT(*) AS cnt
            FROM top_promotions_cache
            GROUP BY window_hours, scope_type
            ORDER BY window_hours, scope_type;
        """)
        rows = cur.fetchall()
        if rows:
            print(f"\n  Cache actuel (top_promotions_cache) :")
            for r in rows:
                print(f"    window={r['window_hours']}h  scope={r['scope_type']:<6}  → {r['cnt']} lignes")
        else:
            print("\n  Cache actuel : VIDE")

        # Villes disponibles dans le cache
        cur.execute("""
            SELECT DISTINCT city
            FROM top_promotions_cache
            WHERE window_hours = %s
            ORDER BY city LIMIT 20;
        """, (window_hours,))
        cities = [r["city"] for r in cur.fetchall()]
        if cities:
            print(f"\n  Villes en cache pour {window_hours}h : {', '.join(cities)}")


# ─── Refresh (transaction unique, workers séquentiels) ──────────────────────


def _refresh(conn, window_hours: int, top_n: int) -> int:
    _banner(f"Refresh  window_hours={window_hours}  top_n={top_n}")

    t0 = time.time()
    with conn.cursor() as cur:
        # ── Session settings INSIDE the transaction ──────────────────────────
        # Désactive tous les parallel workers → plus d'écriture dans /tmp
        cur.execute("SET LOCAL max_parallel_workers_per_gather = 0;")
        cur.execute("SET LOCAL max_parallel_workers = 0;")
        # Lève la limite de fichiers temporaires (-1 = illimité)
        cur.execute("SET LOCAL temp_file_limit = -1;")
        # Réduit work_mem pour éviter les buffers hash trop larges
        cur.execute("SET LOCAL work_mem = '32MB';")

        print("  Appel de refresh_top_promotions_cache() …  (30-180s)")
        cur.execute(
            "SELECT refresh_top_promotions_cache(%s::integer, %s::integer) AS affected;",
            (window_hours, top_n),
        )
        row = cur.fetchone()
        affected = int(row["affected"]) if row else 0

    conn.commit()
    elapsed = time.time() - t0
    print(f"  ✅  {affected:,} lignes insérées en {elapsed:.1f}s")
    return affected


# ─── Résumé post-refresh ────────────────────────────────────────────────────


def _post_audit(conn, window_hours: int):
    _banner("Résultat")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT scope_type,
                   COUNT(*)::int AS cnt,
                   ROUND(MIN(discount_percent), 1) AS min_pct,
                   ROUND(MAX(discount_percent), 1) AS max_pct,
                   ROUND(MIN(discount_amount),  2) AS min_amt,
                   ROUND(MAX(discount_amount),  2) AS max_amt
            FROM top_promotions_cache
            WHERE window_hours = %s
            GROUP BY scope_type
            ORDER BY scope_type;
        """, (window_hours,))
        rows = cur.fetchall()
        if not rows:
            print("  ⚠️  Cache vide après refresh.")
            print("     → Vérifiez que promotion_store_items a des données actives")
            print("       et que product_prices contient des prix récents.")
        else:
            for r in rows:
                print(
                    f"  scope={r['scope_type']:<6}  {r['cnt']:>5} lignes  "
                    f"remise: {r['min_pct']}%–{r['max_pct']}%  "
                    f"économie: ₪{r['min_amt']}–₪{r['max_amt']}"
                )

        # Top 10 globaux par %
        cur.execute("""
            SELECT city, item_name, chain_name,
                   discount_percent, price, effective_price
            FROM top_promotions_cache
            WHERE window_hours = %s AND scope_type = 'city'
            ORDER BY discount_percent DESC NULLS LAST
            LIMIT 10;
        """, (window_hours,))
        rows = cur.fetchall()
        if rows:
            print(f"\n  Top 10 par % (scope=city) :")
            for r in rows:
                name = (r["item_name"] or "")[:38]
                print(
                    f"    {r['discount_percent']:>5.1f}%  {name:<38}  "
                    f"₪{r['effective_price']} (était ₪{r['price']})  "
                    f"[{r['city']}·{r['chain_name']}]"
                )

    conn.commit()


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Refresh top_promotions_cache (7 jours de données)."
    )
    parser.add_argument("--window-hours", type=int, default=168)
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--skip-audit", action="store_true",
                        help="Sauter l'audit pré-refresh (plus rapide)")
    args = parser.parse_args()

    print(f"🔌  Connexion à la DB…")
    conn = _connect(_db_url())
    print("✅  Connecté (transaction manuelle).")

    try:
        if not args.skip_audit:
            _audit(conn, args.window_hours)
            conn.commit()  # fin de la transaction en lecture

        affected = _refresh(conn, args.window_hours, args.top_n)

        if affected == 0:
            print("\n⚠️  Aucune ligne insérée.")
            print("   Le cache est vide — aucune promo active trouvée dans la fenêtre.")
        else:
            _post_audit(conn, args.window_hours)

    except Exception as e:
        conn.rollback()
        print(f"\n❌  Erreur : {e}")
        sys.exit(1)
    finally:
        conn.close()

    print("\n✅  Terminé.\n")


if __name__ == "__main__":
    main()
