#!/usr/bin/env python3
"""
One-shot safe sync:
mark products.has_image = TRUE for every product image already present in
DigitalOcean Spaces under products/{item_code}.jpg.

No secrets are stored in this file. Provide env vars directly or use --env-file.
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from pathlib import Path
from urllib.parse import unquote

import boto3
import psycopg2
import urllib3
from psycopg2.extras import execute_values


SPACE_PRODUCTS_PREFIX = "products/"
SPACE_IMAGE_SUFFIX = ".jpg"


def _strip_quotes(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_env_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Env file not found: {path}")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        os.environ.setdefault(key, _strip_quotes(value) or "")


def _env(name: str, fallback: str | None = None) -> str:
    value = _strip_quotes(os.getenv(name) or (os.getenv(fallback) if fallback else None))
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


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


def _list_space_item_codes(s3_client, bucket: str) -> set[str]:
    item_codes: set[str] = set()
    skipped_nested = 0
    skipped_other = 0

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=SPACE_PRODUCTS_PREFIX):
        for obj in page.get("Contents", []):
            key = str(obj.get("Key") or "")
            if not key.startswith(SPACE_PRODUCTS_PREFIX) or not key.endswith(SPACE_IMAGE_SUFFIX):
                skipped_other += 1
                continue

            item_code = key[len(SPACE_PRODUCTS_PREFIX) : -len(SPACE_IMAGE_SUFFIX)]
            if not item_code or "/" in item_code:
                skipped_nested += 1
                continue

            item_codes.add(unquote(item_code))

    print(f"  Images produits matchables dans Spaces : {len(item_codes)}")
    if skipped_nested:
        print(f"  Objets ignorés car sous-dossier/nom vide : {skipped_nested}")
    if skipped_other:
        print(f"  Objets ignorés car hors format .jpg      : {skipped_other}")
    return item_codes


def _connect_db():
    return psycopg2.connect(_env("DATABASE_URL", "POSTGRESQL_URL"), application_name="mark_spaces_product_images_true")


def _sync_db(item_codes: set[str], dry_run: bool) -> None:
    if not item_codes:
        print("  Aucun item_code trouvé dans Spaces.")
        return

    conn = _connect_db()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE tmp_space_product_images (item_code text PRIMARY KEY) ON COMMIT DROP;")
            execute_values(
                cur,
                "INSERT INTO tmp_space_product_images (item_code) VALUES %s ON CONFLICT DO NOTHING;",
                [(code,) for code in sorted(item_codes)],
                page_size=5000,
            )

            cur.execute(
                """
                SELECT
                  COUNT(*)::bigint AS matching_products,
                  COUNT(*) FILTER (WHERE p.has_image IS TRUE)::bigint AS already_true,
                  COUNT(*) FILTER (WHERE p.has_image IS FALSE)::bigint AS false_to_true,
                  COUNT(*) FILTER (WHERE p.has_image IS NULL)::bigint AS null_to_true
                FROM tmp_space_product_images s
                JOIN products p ON p.item_code = s.item_code;
                """
            )
            matching_products, already_true, false_to_true, null_to_true = cur.fetchone()

            cur.execute(
                """
                SELECT COUNT(*)::bigint
                FROM tmp_space_product_images s
                LEFT JOIN products p ON p.item_code = s.item_code
                WHERE p.item_code IS NULL;
                """
            )
            missing_products = cur.fetchone()[0]

            print(f"  Images Spaces avec produit en DB         : {matching_products}")
            print(f"  Déjà has_image=TRUE                     : {already_true}")
            print(f"  À corriger FALSE → TRUE                 : {false_to_true}")
            print(f"  À corriger NULL → TRUE                  : {null_to_true}")
            print(f"  Images Spaces sans ligne produit DB     : {missing_products}")

            cur.execute(
                """
                UPDATE products p
                SET has_image = TRUE
                FROM tmp_space_product_images s
                WHERE p.item_code = s.item_code
                  AND p.has_image IS DISTINCT FROM TRUE;
                """
            )
            updated = cur.rowcount

        if dry_run:
            conn.rollback()
            print(f"\nDRY-RUN: aurait marqué TRUE {updated} produit(s).")
        else:
            conn.commit()
            print(f"\n✅  A marqué TRUE {updated} produit(s).")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="Mark has_image=TRUE for products already present in Spaces.")
    parser.add_argument("--env-file", type=Path, default=None, help="Fichier .env local à charger.")
    parser.add_argument("--dry-run", action="store_true", help="Affiche seulement ce qui serait modifié.")
    parser.add_argument("--insecure-tls", action="store_true", help="Désactive la vérification TLS localement si nécessaire.")
    args = parser.parse_args()

    if args.env_file:
        _load_env_file(args.env_file)
    if args.insecure_tls:
        warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

    print("Scan DigitalOcean Spaces products/…")
    s3_client = _build_s3_client(args.insecure_tls)
    item_codes = _list_space_item_codes(s3_client, _env("DO_SPACES_BUCKET"))

    print("\nSynchronisation DB products.has_image…")
    _sync_db(item_codes, args.dry_run)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n❌  Erreur fatale : {exc}", file=sys.stderr)
        sys.exit(1)
