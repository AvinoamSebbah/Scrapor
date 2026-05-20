#!/usr/bin/env python3
"""
sync_product_images.py
──────────────────────
Workflow manuel pour peupler products.has_image, qui devient la source de vérité
DB sur l'existence d'une image produit.

Ordre normal :
  1. Préflight obligatoire du pipeline bridge + upload Space
  2. Vérification des images déjà présentes dans DigitalOcean Spaces
  3. Pour les produits encore inconnus :
     - Pricez
     - puis OpenFoodFacts
     - image importée via le bridge Cloudinary
     - puis copiée dans DigitalOcean Spaces sous products/{barcode}.jpg
  4. Mise à jour de products.has_image :
     - TRUE si image déjà trouvée dans Spaces ou importée avec succès
     - FALSE seulement si l'absence est prouvée partout
     - NULL si une étape technique échoue ou reste inconnue

Important :
  - Le script ne stocke pas d'URL signée : le chemin durable est implicite
    (products/{barcode}.jpg) et l'URL de livraison peut être régénérée plus tard.
  - Si --skip-spaces-check est utilisé, les produits sans source distante trouvée
    restent NULL au lieu de passer FALSE, car l'absence dans le Space n'a pas été
    vérifiée.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from urllib.parse import quote, unquote

import boto3
import cloudinary
import cloudinary.uploader
import cloudinary.utils
import psycopg2
import requests
from botocore.exceptions import ClientError
from psycopg2.extras import RealDictCursor


BARCODE_RE = re.compile(r"^\d{8,14}$")
SPACE_PRODUCTS_PREFIX = "products/"
SPACE_IMAGE_SUFFIX = ".jpg"
PREFLIGHT_PREFIX = "preflight/product-image-sync/"
PRICEZ_PREFLIGHT_BARCODE = "7290012901355"
OPENFOODFACTS_PREFLIGHT_BARCODE = "3017620422003"
DEFAULT_BATCH_SIZE = 250
DEFAULT_REQUEST_TIMEOUT = 20
DEFAULT_USER_AGENT = "Agali-image-sync/1.0 (https://agali.live)"
DEFAULT_PROGRESS_EVERY = 100


class TemporaryImageSyncError(RuntimeError):
    """Erreur technique : on doit garder has_image=NULL, pas FALSE."""


class OpenFoodFactsRateLimited(RuntimeError):
    """OFF a rate-limit ce lookup ; pour ce workflow on le traite comme sans image."""


@dataclass
class ProductRow:
    item_code: str
    has_image: bool | None


@dataclass
class SyncStats:
    total_candidates: int = 0
    already_in_spaces: int = 0
    imported_from_pricez: int = 0
    imported_from_openfoodfacts: int = 0
    marked_false: int = 0
    left_unknown: int = 0
    openfoodfacts_incompatible_codes: int = 0
    errors: int = 0


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return _strip_wrapping_quotes(value)


def _strip_wrapping_quotes(value: str) -> str:
    text = str(value)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _db_url() -> str:
    value = (
        os.getenv("POSTGRESQL_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
        or ""
    )
    return _strip_wrapping_quotes(value)


def _connect():
    db_url = _db_url()
    if not db_url:
        raise RuntimeError("POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) must be set")
    conn = psycopg2.connect(
        db_url,
        connect_timeout=30,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
        cursor_factory=RealDictCursor,
        application_name="sync_product_images",
    )
    conn.autocommit = False
    return conn


def _configure_cloudinary(insecure_tls: bool) -> None:
    cloudinary.config(
        cloud_name=_env("CLOUDINARY_CLOUD_NAME"),
        api_key=_env("CLOUDINARY_API_KEY"),
        api_secret=_env("CLOUDINARY_API_SECRET"),
        secure=True,
    )
    if insecure_tls:
        # Usage local uniquement. Le SDK Cloudinary construit son PoolManager à
        # l'import ; il faut le recréer explicitement pour respecter ce mode.
        cloudinary.CERT_KWARGS = {"cert_reqs": "CERT_NONE"}
        cloudinary.uploader._http = cloudinary.utils.get_http_connector(
            cloudinary.config(),
            cloudinary.CERT_KWARGS,
        )


def _build_s3_client(insecure_tls: bool):
    region = _env("DO_SPACES_REGION")
    return boto3.client(
        "s3",
        region_name=region,
        endpoint_url=f"https://{region}.digitaloceanspaces.com",
        aws_access_key_id=_env("DO_SPACES_ACCESS_KEY"),
        aws_secret_access_key=_env("DO_SPACES_SECRET_KEY"),
        verify=not insecure_tls,
    )


def _build_http_session(insecure_tls: bool) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {"User-Agent": _strip_wrapping_quotes(os.getenv("OPENFOODFACTS_USER_AGENT", DEFAULT_USER_AGENT))}
    )
    session.verify = not insecure_tls
    return session


def _banner(message: str) -> None:
    print(f"\n{'─' * 62}")
    print(f"  {message}")
    print(f"{'─' * 62}")


def _reset_all_to_null(conn, dry_run: bool) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*)::bigint AS cnt FROM products WHERE has_image IS NOT NULL;")
        affected = int(cur.fetchone()["cnt"])
        if not dry_run:
            cur.execute("UPDATE products SET has_image = NULL WHERE has_image IS NOT NULL;")
    if not dry_run:
        conn.commit()
    return affected


def _fetch_products_to_process(
    conn,
    recheck_all: bool,
    recheck_non_ean_false: bool,
    spaces_checked: bool,
    limit: int | None,
) -> list[ProductRow]:
    if recheck_all and spaces_checked:
        # Après le scan Space du run courant, les TRUE sont déjà résolus :
        # les retraiter par les sources distantes pourrait les dégrader à tort.
        where_sql = "WHERE has_image IS DISTINCT FROM TRUE"
    elif recheck_all:
        where_sql = ""
    elif recheck_non_ean_false:
        # Réparation ciblée de l'ancienne règle trop stricte qui écrivait FALSE
        # sans tenter Pricez sur les codes hors EAN.
        where_sql = "WHERE has_image IS NULL OR (has_image = FALSE AND item_code !~ '^[0-9]{8,14}$')"
    else:
        where_sql = "WHERE has_image IS NULL"
    limit_sql = "LIMIT %s" if limit is not None else ""
    params = (limit,) if limit is not None else ()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT item_code, has_image
            FROM products
            {where_sql}
            ORDER BY id
            {limit_sql}
            """,
            params,
        )
        return [
            ProductRow(item_code=str(row["item_code"]), has_image=row["has_image"])
            for row in cur.fetchall()
        ]


def _bulk_update_has_image(conn, item_codes: list[str], value: bool, dry_run: bool) -> int:
    if not item_codes:
        return 0
    if dry_run:
        return len(item_codes)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE products
            SET has_image = %s
            WHERE item_code = ANY(%s)
              AND has_image IS DISTINCT FROM %s
            """,
            (value, item_codes, value),
        )
        updated = cur.rowcount
    conn.commit()
    return updated


def _list_space_barcodes(s3_client, bucket: str) -> set[str]:
    found: set[str] = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=SPACE_PRODUCTS_PREFIX):
        for obj in page.get("Contents", []):
            key = str(obj.get("Key") or "")
            if not key.startswith(SPACE_PRODUCTS_PREFIX) or not key.endswith(SPACE_IMAGE_SUFFIX):
                continue
            item_code = key[len(SPACE_PRODUCTS_PREFIX) : -len(SPACE_IMAGE_SUFFIX)]
            if not item_code or "/" in item_code:
                continue
            found.add(unquote(item_code))
    return found


def _pricez_url(barcode: str) -> str:
    return f"https://m.pricez.co.il/ProductPictures/200x/{quote(barcode, safe='')}.jpg"


def _check_pricez_source(session: requests.Session, barcode: str) -> str | None:
    url = _pricez_url(barcode)
    try:
        response = session.get(url, stream=True, timeout=DEFAULT_REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        raise TemporaryImageSyncError(f"Pricez request failed: {exc}") from exc

    with response:
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise TemporaryImageSyncError(f"Pricez unexpected status {response.status_code}")
        content_type = str(response.headers.get("content-type") or "").lower()
        if not content_type.startswith("image/"):
            raise TemporaryImageSyncError(f"Pricez returned non-image content-type {content_type or 'unknown'}")
        return url


def _pick_openfoodfacts_front_image(product: dict) -> str | None:
    selected_images = product.get("selected_images") or {}
    front = selected_images.get("front") or {}
    for size in ("display", "small", "thumb"):
        images = front.get(size) or {}
        if not isinstance(images, dict):
            continue
        for language in ("he", "en", "fr"):
            if images.get(language):
                return str(images[language])
        for url in images.values():
            if url:
                return str(url)
    return None


def _openfoodfacts_front_image_url(session: requests.Session, barcode: str) -> str | None:
    url = f"https://world.openfoodfacts.org/api/v3/product/{quote(barcode, safe='')}"
    params = {"fields": "code,selected_images"}
    try:
        response = session.get(url, params=params, timeout=DEFAULT_REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        raise TemporaryImageSyncError(f"OpenFoodFacts request failed: {exc}") from exc

    if response.status_code == 404:
        return None
    if response.status_code == 429:
        raise OpenFoodFactsRateLimited("OpenFoodFacts unexpected status 429")
    if response.status_code != 200:
        raise TemporaryImageSyncError(f"OpenFoodFacts unexpected status {response.status_code}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise TemporaryImageSyncError("OpenFoodFacts returned invalid JSON") from exc
    product = payload.get("product") or {}
    if not isinstance(product, dict):
        return None
    return _pick_openfoodfacts_front_image(product)


def _bridge_remote_image(remote_url: str, public_id: str) -> str:
    try:
        result = cloudinary.uploader.upload(
            remote_url,
            public_id=public_id,
            folder="spaces-bridge",
            overwrite=True,
            invalidate=False,
            resource_type="image",
            timeout=60,
        )
    except Exception as exc:  # SDK raises provider-specific exceptions
        raise TemporaryImageSyncError(f"Cloudinary bridge failed: {exc}") from exc

    secure_url = result.get("secure_url")
    if not secure_url:
        raise TemporaryImageSyncError("Cloudinary bridge did not return secure_url")
    return str(secure_url)


def _upload_remote_image_to_spaces(
    session: requests.Session,
    s3_client,
    bucket: str,
    key: str,
    remote_url: str,
    source: str,
) -> None:
    try:
        response = session.get(remote_url, timeout=DEFAULT_REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        raise TemporaryImageSyncError(f"Cloudinary download failed: {exc}") from exc

    if response.status_code != 200:
        raise TemporaryImageSyncError(f"Cloudinary download returned status {response.status_code}")
    content_type = str(response.headers.get("content-type") or "image/jpeg").split(";")[0].strip()
    if not content_type.startswith("image/"):
        raise TemporaryImageSyncError(f"Cloudinary returned non-image content-type {content_type}")

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=response.content,
            ContentType=content_type,
            CacheControl="public, max-age=31536000, immutable",
            Metadata={"fetched_from": source},
        )
    except ClientError as exc:
        raise TemporaryImageSyncError(f"Spaces upload failed: {exc}") from exc


def _delete_preflight_object(s3_client, bucket: str, key: str) -> None:
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        # Le préflight a déjà prouvé l'upload ; l'échec du cleanup ne doit pas
        # masquer le résultat principal.
        pass


def _run_preflight(session: requests.Session, s3_client, bucket: str) -> None:
    _banner("Préflight bridge + Spaces")
    run_id = uuid.uuid4().hex[:12]

    pricez_source = _check_pricez_source(session, PRICEZ_PREFLIGHT_BARCODE)
    if not pricez_source:
        raise RuntimeError("Préflight Pricez impossible : l'image de référence est introuvable")
    pricez_bridged = _bridge_remote_image(pricez_source, "preflight-pricez")
    pricez_key = f"{PREFLIGHT_PREFIX}pricez-{run_id}.jpg"
    _upload_remote_image_to_spaces(session, s3_client, bucket, pricez_key, pricez_bridged, "preflight-pricez")
    _delete_preflight_object(s3_client, bucket, pricez_key)
    print("  ✅ Pricez → Cloudinary bridge → Spaces")

    off_source = _openfoodfacts_front_image_url(session, OPENFOODFACTS_PREFLIGHT_BARCODE)
    if not off_source:
        raise RuntimeError("Préflight OpenFoodFacts impossible : l'image de référence est introuvable")
    off_bridged = _bridge_remote_image(off_source, "preflight-openfoodfacts")
    off_key = f"{PREFLIGHT_PREFIX}openfoodfacts-{run_id}.jpg"
    _upload_remote_image_to_spaces(
        session,
        s3_client,
        bucket,
        off_key,
        off_bridged,
        "preflight-openfoodfacts",
    )
    _delete_preflight_object(s3_client, bucket, off_key)
    print("  ✅ OpenFoodFacts → Cloudinary bridge → Spaces")


def _try_persist_from_source(
    session: requests.Session,
    s3_client,
    bucket: str,
    barcode: str,
    source_name: str,
    source_url: str,
) -> bool:
    try:
        bridged_url = _bridge_remote_image(source_url, f"{source_name}-{barcode}")
        _upload_remote_image_to_spaces(
            session,
            s3_client,
            bucket,
            f"{SPACE_PRODUCTS_PREFIX}{barcode}{SPACE_IMAGE_SUFFIX}",
            bridged_url,
            source_name,
        )
        return True
    except TemporaryImageSyncError as exc:
        print(f"    ⚠️  {barcode}: import {source_name} échoué — {exc}")
        return False


def _sync_products(
    conn,
    session: requests.Session,
    s3_client,
    bucket: str,
    products: list[ProductRow],
    spaces_checked: bool,
    dry_run: bool,
    commit_batch_size: int,
    progress_every: int,
) -> SyncStats:
    stats = SyncStats(total_candidates=len(products))
    pending_true: list[str] = []
    pending_false: list[str] = []

    def flush() -> None:
        nonlocal pending_true, pending_false
        if pending_true:
            _bulk_update_has_image(conn, pending_true, True, dry_run)
            pending_true = []
        if pending_false:
            _bulk_update_has_image(conn, pending_false, False, dry_run)
            pending_false = []

    for index, product in enumerate(products, start=1):
        barcode = product.item_code.strip()
        print(f"[{index}/{len(products)}] {barcode}")

        openfoodfacts_compatible = BARCODE_RE.fullmatch(barcode) is not None
        if not openfoodfacts_compatible:
            stats.openfoodfacts_incompatible_codes += 1

        explicit_absences = 0

        try:
            pricez_source = _check_pricez_source(session, barcode)
        except TemporaryImageSyncError as exc:
            print(f"    ⚠️  {barcode}: Pricez inconnu — {exc}")
            pricez_source = None
            stats.errors += 1
        else:
            if pricez_source:
                if _try_persist_from_source(session, s3_client, bucket, barcode, "pricez", pricez_source):
                    pending_true.append(barcode)
                    stats.imported_from_pricez += 1
                    print(f"    ✅ {barcode}: importé depuis pricez → has_image=TRUE")
                    if len(pending_true) + len(pending_false) >= commit_batch_size:
                        flush()
                    if progress_every > 0 and index % progress_every == 0:
                        _print_progress(index, len(products), stats)
                    continue
            else:
                explicit_absences += 1

        if not openfoodfacts_compatible:
            print(f"    ↪️  {barcode}: OpenFoodFacts ignoré (code non-EAN), Pricez a bien été tenté")
            off_source = None
            explicit_absences += 1
        else:
            try:
                off_source = _openfoodfacts_front_image_url(session, barcode)
            except OpenFoodFactsRateLimited as exc:
                # Règle métier voulue pour ce workflow : si Pricez a déjà répondu
                # explicitement "pas d'image", un 429 OFF ne doit pas empêcher de
                # conclure FALSE. On le compte donc comme une absence OFF.
                print(f"    ⚠️  {barcode}: OpenFoodFacts rate-limit — traité comme sans image ({exc})")
                off_source = None
                explicit_absences += 1
            except TemporaryImageSyncError as exc:
                print(f"    ⚠️  {barcode}: OpenFoodFacts inconnu — {exc}")
                off_source = None
                stats.errors += 1
            else:
                if off_source:
                    if _try_persist_from_source(session, s3_client, bucket, barcode, "openfoodfacts", off_source):
                        pending_true.append(barcode)
                        stats.imported_from_openfoodfacts += 1
                        print(f"    ✅ {barcode}: importé depuis openfoodfacts → has_image=TRUE")
                        if len(pending_true) + len(pending_false) >= commit_batch_size:
                            flush()
                        if progress_every > 0 and index % progress_every == 0:
                            _print_progress(index, len(products), stats)
                        continue
                else:
                    explicit_absences += 1

        if spaces_checked and explicit_absences == 2:
            pending_false.append(barcode)
            stats.marked_false += 1
            print(f"    ❌ {barcode}: has_image=FALSE (absent de Pricez et OpenFoodFacts)")
        else:
            stats.left_unknown += 1
            print(f"    ?  {barcode}: has_image=NULL (état encore inconnu)")

        if len(pending_true) + len(pending_false) >= commit_batch_size:
            flush()
        if progress_every > 0 and index % progress_every == 0:
            _print_progress(index, len(products), stats)

    flush()
    return stats


def _print_summary(stats: SyncStats, dry_run: bool) -> None:
    _banner("Résumé")
    mode = "DRY-RUN" if dry_run else "écritures DB actives"
    print(f"  Mode                              : {mode}")
    print(f"  Produits candidats                : {stats.total_candidates}")
    print(f"  Déjà trouvés dans Spaces          : {stats.already_in_spaces}")
    print(f"  Importés depuis Pricez            : {stats.imported_from_pricez}")
    print(f"  Importés depuis OpenFoodFacts     : {stats.imported_from_openfoodfacts}")
    print(f"  Marqués has_image=FALSE           : {stats.marked_false}")
    print(f"  Restés has_image=NULL             : {stats.left_unknown}")
    print(f"  Codes non compatibles OpenFoodFacts: {stats.openfoodfacts_incompatible_codes}")
    print(f"  Erreurs techniques rencontrées    : {stats.errors}")


def _print_progress(index: int, total: int, stats: SyncStats) -> None:
    print(
        "  📊 progression "
        f"{index}/{total} | "
        f"pricez={stats.imported_from_pricez} | "
        f"openfoodfacts={stats.imported_from_openfoodfacts} | "
        f"false={stats.marked_false} | "
        f"null={stats.left_unknown} | "
        f"off_incompatible={stats.openfoodfacts_incompatible_codes} | "
        f"errors={stats.errors}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Synchronise products.has_image et le bucket produit.")
    parser.add_argument(
        "--skip-spaces-check",
        action="store_true",
        help="Ne pas faire le pré-remplissage depuis Spaces. Les absences non prouvées restent NULL.",
    )
    parser.add_argument(
        "--reset-all-to-null",
        action="store_true",
        help="Remet tous les products.has_image à NULL avant la synchronisation.",
    )
    parser.add_argument(
        "--recheck-all",
        action="store_true",
        help="Traiter tous les produits au lieu des seuls produits actuellement à NULL.",
    )
    parser.add_argument(
        "--recheck-non-ean-false",
        action="store_true",
        help="Retraiter les FALSE non-EAN hérités de l'ancienne règle qui ne tentait pas Pricez.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Limiter le nombre de produits traités.")
    parser.add_argument("--dry-run", action="store_true", help="Ne pas écrire dans la DB.")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="Teste uniquement Pricez/OpenFoodFacts → Cloudinary bridge → Spaces puis s'arrête.",
    )
    parser.add_argument(
        "--insecure-tls",
        action="store_true",
        help="Désactive la vérification TLS (uniquement pour dev local).",
    )
    parser.add_argument(
        "--commit-batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Taille des lots d'UPDATE DB (défaut: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help=f"Afficher un résumé compact toutes les N lignes (défaut: {DEFAULT_PROGRESS_EVERY}).",
    )
    args = parser.parse_args()

    _configure_cloudinary(args.insecure_tls)
    bucket = _env("DO_SPACES_BUCKET")
    session = _build_http_session(args.insecure_tls)
    s3_client = _build_s3_client(args.insecure_tls)

    # Préflight AVANT toute mutation DB : si le bridge est cassé, on ne touche rien.
    _run_preflight(session, s3_client, bucket)
    if args.preflight_only:
        print("\n✅  Préflight terminé avec succès. Aucun accès DB effectué.")
        return

    conn = _connect()
    try:
        print("\n🔒  Acquisition du lock exclusif (pg_advisory_lock 55556)…")
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(55556)")
        print("🔒  Lock acquis.")

        if args.reset_all_to_null:
            _banner("Reset has_image")
            affected = _reset_all_to_null(conn, args.dry_run)
            print(f"  {'Aurait remis' if args.dry_run else 'A remis'} {affected} produits à NULL")

        spaces_checked = not args.skip_spaces_check
        stats = SyncStats()

        if spaces_checked:
            _banner("Scan DigitalOcean Spaces")
            space_barcodes = sorted(_list_space_barcodes(s3_client, bucket))
            matched_existing = _bulk_update_has_image(conn, space_barcodes, True, args.dry_run)
            stats.already_in_spaces = matched_existing
            print(f"  {len(space_barcodes)} objets produit détectés dans Spaces")
            print(f"  {matched_existing} produits {'seraient marqués' if args.dry_run else 'marqués'} TRUE")
        else:
            print("\n⏭️  Scan Spaces ignoré (--skip-spaces-check).")

        products = _fetch_products_to_process(
            conn,
            args.recheck_all,
            args.recheck_non_ean_false,
            spaces_checked,
            args.limit,
        )
        _banner("Synchronisation distante")
        print(f"  {len(products)} produits à traiter")
        remote_stats = _sync_products(
            conn,
            session,
            s3_client,
            bucket,
            products,
            spaces_checked=spaces_checked,
            dry_run=args.dry_run,
            commit_batch_size=max(1, args.commit_batch_size),
            progress_every=max(0, args.progress_every),
        )
        remote_stats.already_in_spaces = stats.already_in_spaces
        _print_summary(remote_stats, args.dry_run)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    started = time.time()
    try:
        main()
    except Exception as exc:
        print(f"\n❌  Erreur fatale : {exc}")
        sys.exit(1)
    finally:
        print(f"\n⏱️  Durée totale : {time.time() - started:.1f}s")
