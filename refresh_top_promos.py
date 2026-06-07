#!/usr/bin/env python3
"""
refresh_top_promos.py
─────────────────────
Peuple top_promotions_cache via refresh_top_promotions_cache().

Par défaut le script :
  1. Génère le cache all-time via SQL avec top_n comme limite finale
  2. Utilise products.has_image comme source de vérité côté fonction SQL
  3. Re-rank les lignes restantes et recoupe à top_n final

Comportements alternatifs (flags) :
  --skip-audit         : saute l'audit pré-refresh

Voir docs/image_refresh_flags.md pour plus de détails.

Usage:
    python refresh_top_promos.py
    python refresh_top_promos.py --top-n 300
    python refresh_top_promos.py --skip-audit

Requiert POSTGRESQL_URL ou DATABASE_URL dans l'environnement.
"""

import argparse
import os
import sys
import time

import psycopg2
from psycopg2.extras import RealDictCursor

# Fenêtre unique conservée pour le cache promos :
# 0 = "all time" (toutes promos non-expirées, sans limite de date)
WINDOWS_TO_REFRESH = [0]


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

        # Top 10 globaux par score
        cur.execute("""
            SELECT city, item_name, chain_name,
                   discount_percent, price, effective_price
            FROM top_promotions_cache
            WHERE window_hours = %s AND scope_type = 'store'
            ORDER BY smart_score DESC NULLS LAST
            LIMIT 10;
        """, (window_hours,))
        rows = cur.fetchall()
        if rows:
            print(f"\n  Top 10 par score (scope=store) :")
            for r in rows:
                name = (r["item_name"] or "")[:38]
                print(
                    f"    {r['discount_percent']:>5.1f}%  {name:<38}  "
                    f"₪{r['effective_price']} (était ₪{r['price']})  "
                    f"[{r['city']}·{r['chain_name']}]"
                )

    conn.commit()


def _rerank(conn, window_hours: int, top_n: int):
    """
    Re-numérote rank_position au sein de chaque groupe
    (scope_type, city, chain_id, store_id), après avoir recoupé
    le cache à son volume final utile.

    Deux passes pour éviter les collisions de clé primaire :
    1. Passe négative : rank_position = -(new_rank)  (valeurs uniques garanties)
    2. Passe positive : rank_position = ABS(rank_position)
    """
    standard_limit = max(1, round(top_n * 2 / 3))
    coupon_limit = max(0, top_n - standard_limit)

    with conn.cursor() as cur:
        # Coupe finale : pour chaque store, on garde 2/3 de promos "normales"
        # et 1/3 de promos coupon-like. Les scopes legacy non-store restent
        # bornés à top_n pour ne pas gonfler inutilement le cache.
        cur.execute(
            """
            WITH classified AS (
                SELECT
                    ctid,
                    scope_type,
                    city,
                    chain_id,
                    store_id,
                    CASE
                        WHEN scope_type = 'store'
                          AND COALESCE(promo_kind, 'regular') IN ('coupon', 'club', 'card', 'insurance')
                        THEN 'coupon_like'
                        WHEN scope_type = 'store'
                        THEN 'standard'
                        ELSE NULL
                    END AS promo_bucket,
                    ROW_NUMBER() OVER (
                        PARTITION BY window_hours, scope_type, city, chain_id, store_id,
                            CASE
                                WHEN scope_type = 'store'
                                  AND COALESCE(promo_kind, 'regular') IN ('coupon', 'club', 'card', 'insurance')
                                THEN 'coupon_like'
                                WHEN scope_type = 'store'
                                THEN 'standard'
                                ELSE NULL
                            END
                        ORDER BY smart_score DESC NULLS LAST, discount_percent DESC NULLS LAST, discount_amount DESC NULLS LAST, updated_at DESC NULLS LAST, item_code ASC
                    ) AS bucket_rank,
                    ROW_NUMBER() OVER (
                        PARTITION BY window_hours, scope_type, city, chain_id, store_id
                        ORDER BY smart_score DESC NULLS LAST, discount_percent DESC NULLS LAST, discount_amount DESC NULLS LAST, updated_at DESC NULLS LAST, item_code ASC
                    ) AS overall_rank
                FROM top_promotions_cache
                WHERE window_hours = %s
            ),
            keepers AS (
                SELECT ctid
                FROM classified
                WHERE (
                    scope_type = 'store'
                    AND (
                        (promo_bucket = 'standard' AND bucket_rank <= %s)
                        OR (promo_bucket = 'coupon_like' AND bucket_rank <= %s)
                    )
                )
                OR (
                    scope_type <> 'store'
                    AND overall_rank <= %s
                )
            )
            DELETE FROM top_promotions_cache t
            WHERE t.window_hours = %s
              AND NOT EXISTS (
                SELECT 1
                FROM keepers k
                WHERE k.ctid = t.ctid
              )
            """,
            (window_hours, standard_limit, coupon_limit, top_n, window_hours),
        )

        # Passe 1 : appliquer les nouveaux rangs en négatif
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    window_hours, scope_type, city, chain_id, store_id, rank_position,
                    ROW_NUMBER() OVER (
                        PARTITION BY window_hours, scope_type, city, chain_id, store_id
                        ORDER BY (has_image IS TRUE) DESC, smart_score DESC NULLS LAST
                    ) AS new_rank
                FROM top_promotions_cache
                WHERE window_hours = %s
            )
            UPDATE top_promotions_cache t
            SET rank_position = -(r.new_rank)
            FROM ranked r
            WHERE t.window_hours  = r.window_hours
              AND t.scope_type    = r.scope_type
              AND t.city          = r.city
              AND t.chain_id      = r.chain_id
              AND t.store_id      = r.store_id
              AND t.rank_position = r.rank_position
            """,
            (window_hours,),
        )
        # Passe 2 : remettre en positif
        cur.execute(
            """
            UPDATE top_promotions_cache
            SET rank_position = ABS(rank_position)
            WHERE window_hours = %s AND rank_position < 0
            """,
            (window_hours,),
        )
    conn.commit()
    print(f"  Re-rank effectué (quota final: {standard_limit} standard + {coupon_limit} coupon-like)")


def _purge_non_all_time_windows(conn, keep_window: int = 0) -> int:
    """Remove legacy top cache partitions now that promotions are all-time only."""
    _banner("Nettoyage fenêtres legacy")
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM top_promotions_cache WHERE window_hours <> %s",
            (keep_window,),
        )
        deleted = cur.rowcount
    conn.commit()

    if deleted:
        print(f"  ✅  {deleted:,} lignes legacy supprimées (fenêtres ≠ {keep_window})")
    else:
        print("  ✅  Aucune fenêtre legacy à supprimer")
    return deleted


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Refresh top_promotions_cache (all-time uniquement)."
    )
    parser.add_argument("--window-hours", type=int, default=0,
                        help="Compatibilité CLI: toute valeur est forcée à 0 (all-time).")
    parser.add_argument("--top-n", type=int, default=300)
    parser.add_argument("--skip-audit", action="store_true",
                        help="Sauter l'audit pré-refresh (plus rapide)")
    parser.add_argument("--skip-image-check", action="store_true",
                        help="Compatibilité: ignoré, products.has_image est utilisé en SQL")
    parser.add_argument("--include-no-image", action="store_true",
                        help="Compatibilité: ignoré, le cache garde seulement products.has_image=TRUE")
    parser.add_argument("--images-only", action="store_true",
                        help="Compatibilité: ignoré, plus de vérification image dans ce script")
    args = parser.parse_args()

    if args.window_hours != 0:
        print(f"⚠️  --window-hours={args.window_hours} ignoré : top_promotions_cache est all-time uniquement.")
    if args.skip_image_check or args.include_no_image or args.images_only:
        print("⚠️  Flags image ignorés : products.has_image est la source de vérité côté SQL.")

    # Fenêtre à traiter : all-time uniquement.
    windows = WINDOWS_TO_REFRESH

    candidate_top_n = args.top_n

    print(f"🔌  Connexion à la DB…")
    db_url = _db_url()
    conn = _connect(db_url)
    print("✅  Connecté (transaction manuelle).")

    # ── Lock exclusif au niveau session PostgreSQL ───────────────────────────
    # Empêche toute exécution simultanée (double cron, run manuel + cron, etc.).
    # Lock ID 55555 est réservé pour les opérations bulk sur top_promotions_cache.
    # pg_advisory_lock bloque jusqu'à ce que le verrou soit libre.
    print("🔒  Acquisition du lock exclusif (pg_advisory_lock 55555)…")
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(55555)")
    print("🔒  Lock acquis.")

    try:
        print(f"📦  Top final SQL : {candidate_top_n}")
        if not args.skip_audit:
            _audit(conn, windows[0])
            conn.commit()

        for window in windows:
            label = "all-time" if window == 0 else f"{window}h"
            print(f"\n{'═'*55}")
            print(f"  Fenêtre : {label}")
            print(f"{'═'*55}")

            affected = _refresh(conn, window, candidate_top_n)

            if affected == 0:
                print(f"\n⚠️  Aucune ligne insérée pour {label}.")
            else:
                _rerank(conn, window, args.top_n)
                _post_audit(conn, window)

        if 0 in windows:
            _purge_non_all_time_windows(conn, 0)

    except Exception as e:
        conn.rollback()
        print(f"\n❌  Erreur : {e}")
        sys.exit(1)
    finally:
        conn.close()

    print("\n✅  Terminé.\n")


if __name__ == "__main__":
    main()
