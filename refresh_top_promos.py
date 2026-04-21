#!/usr/bin/env python3
"""
refresh_top_promos.py
─────────────────────
Peuple top_promotions_cache via refresh_top_promotions_cache().

Par défaut le script :
  1. Génère CANDIDATE_FACTOR × top_n candidats en SQL
  2. Vérifie les images via l'API backend (Cloudinary → Pricez → OpenFoodFacts)
  3. Supprime les lignes sans image du cache
  4. Re-rank les lignes restantes et coupe à top_n

Comportements alternatifs (flags) :
  --include-no-image   : garde les promos sans image mais les met en dernier (après celles avec image)
  --skip-image-check   : saute totalement la vérification image (dev/debug rapide)
  --skip-audit         : saute l'audit pré-refresh

Voir docs/image_refresh_flags.md pour plus de détails.

Usage:
    python refresh_top_promos.py
    python refresh_top_promos.py --window-hours 168 --top-n 200
    python refresh_top_promos.py --include-no-image
    python refresh_top_promos.py --skip-image-check --skip-audit

Requiert POSTGRESQL_URL ou DATABASE_URL dans l'environnement.
Requiert BACKEND_API_URL (défaut: https://api.agali.live) pour la vérification image.
"""

import argparse
import os
import sys
import time

import psycopg2
import requests
from psycopg2.extras import RealDictCursor

# ─── Configuration image ────────────────────────────────────────────────────

# URL de l'API backend utilisée pour vérifier/uploader les images.
# Override via env var BACKEND_API_URL (ex: http://localhost:3000 en dev).
BACKEND_API_URL = os.getenv("BACKEND_API_URL", "https://api.agali.live")

# Facteur de sur-génération SQL : on demande CANDIDATE_FACTOR × top_n candidats
# pour avoir assez de promos avec images après filtrage.
CANDIDATE_FACTOR = 4

# Fenêtres temporelles à rafraîchir lors d'un run complet :
# 24h, 7 jours, 0 = "all time" (toutes promos non-expirées, sans limite de date)
WINDOWS_TO_REFRESH = [24, 168, 0]

# Taille maximale des lots envoyés à l'API batch images (limite côté backend = 50).
IMAGE_BATCH_SIZE = 50

# Timeout en secondes pour les appels HTTP à l'API backend.
IMAGE_REQUEST_TIMEOUT = 60


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


# ─── Vérification images via API backend ─────────────────────────────────────


def _check_images(conn, window_hours: int, include_no_image: bool) -> tuple:
    """
    1. Récupère tous les item_codes distincts du cache pour window_hours
    2. Envoie des lots de IMAGE_BATCH_SIZE à POST /api/products/images/batch
    3. Met à jour has_image dans la DB
    4. Supprime les lignes sans image (sauf si include_no_image=True)
    5. Re-rank les lignes restantes (celles avec image en premier)
    6. Coupe à top_n par scope/city/chain/store

    Retourne des stats : { total, with_image, without_image, deleted }
    """
    _banner("Vérification images")

    # 1) Récupère les item_codes du cache
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT item_code
            FROM top_promotions_cache
            WHERE window_hours = %s AND has_image IS NULL
            """,
            (window_hours,),
        )
        rows = cur.fetchall()
    item_codes = [r["item_code"] for r in rows]
    total = len(item_codes)
    print(f"  {total} item_codes distincts à vérifier (has_image IS NULL)…")

    if total == 0:
        return conn, {"total": 0, "with_image": 0, "without_image": 0, "deleted": 0}

    # 2) Batch calls à l'API backend
    # autocommit=True évite que la connexion soit en état "idle in transaction"
    # pendant les appels HTTP, ce qui déclenche idle_in_transaction_session_timeout
    # côté PostgreSQL et coupe la connexion.
    conn.commit()  # ferme la transaction ouverte par le SELECT ci-dessus
    conn.autocommit = True
    image_map: dict[str, bool] = {}
    session = requests.Session()
    session.verify = False  # bypass SSL cert issues on Windows (dev)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    endpoint = f"{BACKEND_API_URL}/api/products/images/batch"

    for i in range(0, total, IMAGE_BATCH_SIZE):
        batch = item_codes[i : i + IMAGE_BATCH_SIZE]
        batch_num = i // IMAGE_BATCH_SIZE + 1
        total_batches = (total + IMAGE_BATCH_SIZE - 1) // IMAGE_BATCH_SIZE
        print(f"  Lot {batch_num}/{total_batches} ({len(batch)} items)…", end=" ", flush=True)
        t0 = time.time()
        try:
            resp = session.post(
                endpoint,
                json={"barcodes": batch},
                timeout=IMAGE_REQUEST_TIMEOUT,
                verify=False,
            )
            resp.raise_for_status()
            data = resp.json()
            images = data.get("images", {})
            for code in batch:
                result = images.get(code, {})
                image_map[code] = bool(result.get("imageUrl"))
            print(f"OK ({time.time()-t0:.1f}s)")
        except Exception as exc:
            # Si le batch échoue on marque tout comme ayant une image (conservatif)
            print(f"ERREUR ({exc}) — conservé comme has_image=NULL")
            for code in batch:
                image_map.setdefault(code, True)  # ne pas pénaliser si réseau KO

        # Mise à jour DB — autocommit=True donc chaque execute est son propre commit
        batch_with = [c for c in batch if image_map.get(c) is True]
        batch_without = [c for c in batch if image_map.get(c) is False]
        for _attempt in range(3):
            try:
                with conn.cursor() as cur:
                    if batch_with:
                        cur.execute(
                            "UPDATE top_promotions_cache SET has_image = TRUE WHERE window_hours = %s AND item_code = ANY(%s)",
                            (window_hours, batch_with),
                        )
                    if batch_without:
                        cur.execute(
                            "UPDATE top_promotions_cache SET has_image = FALSE WHERE window_hours = %s AND item_code = ANY(%s)",
                            (window_hours, batch_without),
                        )
                break  # succes
            except psycopg2.errors.DeadlockDetected:
                if _attempt < 2:
                    conn = _connect(_db_url())
                    conn.autocommit = True
                else:
                    raise

    with_image = sum(1 for v in image_map.values() if v)
    without_image = sum(1 for v in image_map.values() if not v)
    print(f"\n  Résultat : {with_image} avec image, {without_image} sans image")

    # Repasser en mode transaction explicite pour la suite (delete + rerank)
    conn.autocommit = False

    deleted = 0
    if not include_no_image:
        # 4) Supprimer les lignes sans image
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM top_promotions_cache WHERE window_hours = %s AND has_image = FALSE",
                (window_hours,),
            )
            deleted = cur.rowcount
        conn.commit()
        print(f"  {deleted} lignes sans image supprimées")
    else:
        print(f"  Mode --include-no-image : promos sans image conservées (en dernier)")

    # 5 & 6) Re-rank et coupe à top_n par groupe
    _rerank(conn, window_hours)

    return conn, {"total": total, "with_image": with_image, "without_image": without_image, "deleted": deleted}


def _rerank(conn, window_hours: int):
    """
    Re-numérotote rank_position au sein de chaque groupe
    (scope_type, city, chain_id, store_id) en mettant
    has_image=TRUE en premier, puis smart_score DESC.

    Deux passes pour éviter les collisions de clé primaire :
    1. Passe négative : rank_position = -(new_rank)  (valeurs uniques garanties)
    2. Passe positive : rank_position = ABS(rank_position)
    """
    with conn.cursor() as cur:
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
    print("  Re-rank effectué")


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Refresh top_promotions_cache (7 jours de données)."
    )
    parser.add_argument("--window-hours", type=int, default=None,
                        help="Fenêtre horaire unique (0=all-time). Si omis, lance toutes les fenêtres (24h, 168h, all-time).")
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--skip-audit", action="store_true",
                        help="Sauter l'audit pré-refresh (plus rapide)")
    parser.add_argument("--skip-image-check", action="store_true",
                        help="Sauter la vérification image (dev/debug rapide)")
    parser.add_argument("--include-no-image", action="store_true",
                        help="Garder les promos sans image (en dernier dans le classement)")
    parser.add_argument("--images-only", action="store_true",
                        help="Faire uniquement le check image sur le cache existant (sans refresh SQL)")
    args = parser.parse_args()

    # Fenêtres à traiter : une seule si --window-hours spécifié, sinon toutes
    windows = [args.window_hours] if args.window_hours is not None else WINDOWS_TO_REFRESH

    # Quand on vérifie les images, générer plus de candidats SQL
    candidate_top_n = args.top_n if args.skip_image_check else args.top_n * CANDIDATE_FACTOR

    print(f"🔌  Connexion à la DB…")
    conn = _connect(_db_url())
    print("✅  Connecté (transaction manuelle).")
    if not args.skip_image_check:
        print(f"📡  Backend images : {BACKEND_API_URL}")

    try:
        if args.images_only:
            # Uniquement le check image + rerank sur les fenêtres demandées
            for window in windows:
                label = "all-time" if window == 0 else f"{window}h"
                print(f"\n  ── Fenêtre {label} ──")
                conn, stats = _check_images(conn, window, args.include_no_image)
                print(
                    f"  📸  Images : {stats['with_image']}/{stats['total']} avec image"
                    + (f", {stats['deleted']} sans image supprimées" if stats['deleted'] else "")
                )
        else:
            if not args.skip_image_check:
                print(f"📦  Candidats SQL : {candidate_top_n} (={args.top_n} × {CANDIDATE_FACTOR})")
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
                    if not args.skip_image_check:
                        conn, stats = _check_images(conn, window, args.include_no_image)
                        print(
                            f"\n  📸  Images : {stats['with_image']}/{stats['total']} avec image"
                            + (f", {stats['deleted']} sans image supprimées" if stats['deleted'] else "")
                        )
                    _post_audit(conn, window)

    except Exception as e:
        conn.rollback()
        print(f"\n❌  Erreur : {e}")
        sys.exit(1)
    finally:
        conn.close()

    print("\n✅  Terminé.\n")


if __name__ == "__main__":
    main()
