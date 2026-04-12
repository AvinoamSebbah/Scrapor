#!/usr/bin/env python3
"""
nightly_promos_refresh.py
─────────────────────────
Refresh nocturne du cache store_promotions_cache.

Calcule les 50 meilleures promos (25 par % + 25 par ₪ économisés)
pour chaque combinaison (store × time_window × promo_type).

Les 3 fenêtres sont traitées en séquence :
  24h   → promos mises à jour dans les dernières 24 heures
  7days → promos mises à jour dans les 7 derniers jours
  30days→ promos mises à jour dans les 30 derniers jours

Usage :
    python nightly_promos_refresh.py
    python nightly_promos_refresh.py --dry-run   # audit sans écriture
    python nightly_promos_refresh.py --window 24h 7days   # fenêtres sélectives

Requiert POSTGRESQL_URL ou DATABASE_URL dans l'environnement.
"""

import argparse
import os
import sys
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor


# ─── Fenêtres disponibles ─────────────────────────────────────────────────────

WINDOWS = [
    {"name": "24h",    "hours": 24},
    {"name": "7days",  "hours": 168},
    {"name": "30days", "hours": 720},
]


# ─── Connexion ────────────────────────────────────────────────────────────────


def _db_url() -> str:
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
    """Connexion PostgreSQL avec keepalives."""
    conn = psycopg2.connect(
        db_url,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        cursor_factory=RealDictCursor,
        application_name="nightly_promos_refresh",
    )
    conn.autocommit = False
    return conn


# ─── Affichage ────────────────────────────────────────────────────────────────


def _banner(msg: str):
    print(f"\n{'─' * 60}")
    print(f"  {msg}")
    print(f"{'─' * 60}")


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ─── Audit pré-refresh ───────────────────────────────────────────────────────


def _audit_source(conn) -> dict:
    """Compte les données sources disponibles."""
    _banner("Audit des données sources")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint AS cnt
            FROM promotion_store_items
            WHERE promo_price IS NOT NULL
              AND promo_price > 0
              AND (promotion_end_date IS NULL OR promotion_end_date >= CURRENT_DATE);
            """
        )
        active_promos = cur.fetchone()["cnt"]
        print(f"  Promos actives (non expirées) : {active_promos:,}")

        cur.execute("SELECT COUNT(DISTINCT store_id)::bigint AS cnt FROM promotion_store_items;")
        distinct_stores = cur.fetchone()["cnt"]
        print(f"  Magasins avec promo           : {distinct_stores:,}")

        cur.execute(
            """
            SELECT time_window, COUNT(*)::bigint AS cnt
            FROM store_promotions_cache
            GROUP BY time_window ORDER BY time_window;
            """
        )
        rows = cur.fetchall()
        if rows:
            print("\n  Cache actuel (store_promotions_cache) :")
            for r in rows:
                print(f"    window={r['time_window']:<6}  → {r['cnt']:,} lignes")
        else:
            print("\n  Cache actuel : VIDE")

    conn.commit()
    return {"active_promos": active_promos, "distinct_stores": distinct_stores}


# ─── Refresh d'une fenêtre ────────────────────────────────────────────────────


def _refresh_window(conn, hours: int, name: str, dry_run: bool = False) -> int:
    """Appelle refresh_store_promotions_window() pour une fenêtre."""
    _banner(f"Refresh fenêtre : {name}  (dernières {hours}h)")

    if dry_run:
        print("  ⏩  --dry-run activé : aucune écriture.")
        return 0

    t0 = time.time()
    with conn.cursor() as cur:
        # Désactive le parallélisme pour éviter les écritures dans /tmp
        cur.execute("SET LOCAL max_parallel_workers_per_gather = 0;")
        cur.execute("SET LOCAL max_parallel_workers = 0;")
        cur.execute("SET LOCAL temp_file_limit = -1;")
        cur.execute("SET LOCAL work_mem = '48MB';")

        print(f"  [{_now()}]  Appel de refresh_store_promotions_window({hours}, '{name}') …")
        cur.execute(
            "SELECT public.refresh_store_promotions_window(%s::integer, %s::varchar) AS affected;",
            (hours, name),
        )
        row = cur.fetchone()
        affected = int(row["affected"]) if row else 0

    conn.commit()
    elapsed = time.time() - t0
    print(f"  ✅  {affected:,} lignes insérées / mises à jour en {elapsed:.1f}s")
    return affected


# ─── Rapport post-refresh ────────────────────────────────────────────────────


def _post_report(conn, windows: list[str]):
    """Affiche un résumé par fenêtre temporelle."""
    _banner("Rapport final")
    with conn.cursor() as cur:
        for w in windows:
            cur.execute(
                """
                SELECT
                  promo_type,
                  sort_metric,
                  COUNT(DISTINCT store_db_id)::int AS stores,
                  COUNT(*)::int AS total_rows,
                  ROUND(MAX(discount_percent), 1) AS max_pct,
                  ROUND(MAX(discount_amount),  2) AS max_savings
                FROM store_promotions_cache
                WHERE time_window = %s
                GROUP BY promo_type, sort_metric
                ORDER BY promo_type, sort_metric;
                """,
                (w,),
            )
            rows = cur.fetchall()
            if not rows:
                print(f"  ⚠️  Cache vide pour la fenêtre {w}")
                continue
            print(f"\n  Fenêtre {w} :")
            for r in rows:
                print(
                    f"    type={r['promo_type']:<12}  metric={r['sort_metric']:<8}  "
                    f"stores={r['stores']:>4}  rows={r['total_rows']:>6}  "
                    f"max={r['max_pct']}%  max_save=₪{r['max_savings']}"
                )

        # Top 5 toutes fenêtres confondues
        cur.execute(
            """
            SELECT city, store_name, item_name, discount_percent, discount_amount, time_window
            FROM store_promotions_cache
            WHERE sort_metric = 'percent' AND promo_type = 'regular' AND rank_position = 1
            ORDER BY discount_percent DESC NULLS LAST
            LIMIT 10;
            """
        )
        tops = cur.fetchall()
        if tops:
            print("\n  Top 10 promos #1 par % (regular, rank=1) :\n")
            for r in tops:
                name = (r["item_name"] or "")[:40]
                store = (r["store_name"] or "")[:20]
                print(
                    f"    {r['discount_percent']:>5.1f}%  ₪{r['discount_amount']:>6.2f}  "
                    f"{name:<40}  {store:<20}  [{r['city']} · {r['time_window']}]"
                )

    conn.commit()


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Refresh nocturne du cache store_promotions_cache."
    )
    parser.add_argument(
        "--window",
        nargs="+",
        choices=["24h", "7days", "30days"],
        default=["24h", "7days", "30days"],
        help="Fenêtres à rafraîchir (défaut : toutes).",
    )
    parser.add_argument(
        "--skip-audit",
        action="store_true",
        help="Sauter l'audit pré-refresh.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Audit uniquement, sans écrire dans le cache.",
    )
    args = parser.parse_args()

    selected_windows = [w for w in WINDOWS if w["name"] in args.window]

    print(f"\n🕑  Démarrage nightly_promos_refresh — {datetime.now().isoformat()}")
    print(f"   Fenêtres : {[w['name'] for w in selected_windows]}")

    conn = _connect(_db_url())
    print("✅  Connecté à la base de données.")

    t_global = time.time()
    total_affected = 0

    try:
        if not args.skip_audit:
            stats = _audit_source(conn)
            if stats["active_promos"] == 0:
                print("\n⚠️  Aucune promo active trouvée. Le cache ne sera pas mis à jour.")
                return

        for w in selected_windows:
            affected = _refresh_window(conn, w["hours"], w["name"], dry_run=args.dry_run)
            total_affected += affected

        if not args.dry_run and total_affected > 0:
            _post_report(conn, [w["name"] for w in selected_windows])

    except Exception as e:
        conn.rollback()
        print(f"\n❌  Erreur fatale : {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()

    elapsed_global = time.time() - t_global
    print(f"\n✅  Terminé en {elapsed_global:.1f}s — {total_affected:,} lignes au total.\n")


if __name__ == "__main__":
    main()
