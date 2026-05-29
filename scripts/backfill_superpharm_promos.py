"""Backfill Super-Pharm promotion items from local parsed CSV outputs.

This is intentionally scoped to the Super-Pharm chain and uses the current
Postgres promotion merge path, so it benefits from the fixed row-level
PromotionItems reconstruction without re-scraping remote files.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import json
import time
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SCRAPOR_ROOT = SCRIPT_DIR.parent
if str(SCRAPOR_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRAPOR_ROOT))

from remotes.short_term.postgres_db import PostgresUploader  # noqa: E402


SUPERPHARM_CHAIN_ID = "7290172900007"


def _load_env_file(path: Path) -> None:
    if not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip().strip('"').strip("'")
        if name and name not in os.environ:
            os.environ[name] = value


def _iter_csv_items(csv_path: Path, chunk_size: int):
    ffill: dict[str, str] = {}
    id_like_columns = {
        "chainid",
        "subchainid",
        "storeid",
        "bikoretno",
        "itemcode",
        "promotionid",
    }
    items = []
    rows_seen = 0

    def normalize_value(key: str, value: str) -> str:
        value = value.strip()
        if key.lower() in id_like_columns and value.isdigit():
            return str(int(value))
        return value

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for record in reader:
            rows_seen += 1
            filled = {}
            for key, raw_value in record.items():
                value = "" if raw_value is None else raw_value
                if value == "":
                    value = ffill.get(key, "")
                else:
                    value = normalize_value(key, value)
                    ffill[key] = value
                filled[key] = value

            content = {
                key: value
                for key, value in filled.items()
                if key not in {"found_folder", "file_name"}
            }
            items.append(
                {
                    "row_index": rows_seen - 1,
                    "found_folder": filled.get("found_folder"),
                    "file_name": filled.get("file_name"),
                    "content": content,
                }
            )

            if len(items) >= chunk_size:
                yield rows_seen, items
                items = []

    if items:
        yield rows_seen, items


def _load_store_db_ids(uploader: PostgresUploader) -> dict[tuple[str, str], str]:
    rows = uploader._run_query(
        "SELECT id, chain_id, store_id FROM stores WHERE chain_id = %s",
        (SUPERPHARM_CHAIN_ID,),
        fetch=True,
    )
    return {
        (str(row["chain_id"]), str(row["store_id"])): str(row["id"])
        for row in rows
    }


def _aggregate_csv(csv_path: Path, chunk_size: int, uploader: PostgresUploader) -> tuple[int, dict]:
    store_db_ids = _load_store_db_ids(uploader)
    aggregated: dict = {}
    details_cache: dict[str, dict] = {}
    now = datetime.now()
    now_iso = now.isoformat()
    total_rows = 0
    start = time.monotonic()

    for total_rows, items in _iter_csv_items(csv_path, chunk_size):
        for item in items:
            content = item.get("content", {})
            details_raw = (
                uploader._get_val(content, "promotiondetails")
                or uploader._get_val(content, "PromotionDetails")
                or ""
            )
            details_key = str(details_raw)
            if details_key in details_cache:
                details = details_cache[details_key]
            else:
                details = uploader._parse_structured_value(details_raw) or {}
                details_cache[details_key] = details

            chain_id = uploader._clean_id(
                uploader._get_promo_val(content, details, "ChainId", "chainid")
            )
            if chain_id != SUPERPHARM_CHAIN_ID:
                continue

            store_id = uploader._clean_id(
                uploader._get_promo_val(content, details, "StoreId", "storeid")
            )
            promotion_id = uploader._clean_id(
                uploader._get_promo_val(content, details, "PromotionId", "promotionid")
            )
            if not store_id or not promotion_id:
                continue

            db_store_key = store_db_ids.get((chain_id, store_id), store_id)
            key = (chain_id, promotion_id)
            store_promo_val = (
                uploader._get_promo_val(
                    content,
                    details,
                    "MinNoOfItemOffered",
                    "minnoofitemoffered",
                    "minnoofitemofered",
                    "MinQty",
                    "minqty",
                )
                or "active"
            )

            promotion_items = uploader._parse_structured_value(
                uploader._get_promo_val(content, details, "groups", "PromotionItems", "promotionitems")
            )
            if not promotion_items:
                promo_item = uploader._build_promo_item_from_row(content, details)
                promotion_items = [promo_item] if promo_item else []
            elif isinstance(promotion_items, dict):
                promotion_items = [promotion_items]

            row = {
                "chain_id": chain_id,
                "promotion_id": promotion_id,
                "sub_chain_id": uploader._clean_id(
                    uploader._get_promo_val(content, details, "SubChainId", "subchainid")
                ),
                "bikoret_no": uploader._clean_id(
                    uploader._get_promo_val(content, details, "BikoretNo", "bikoretno")
                ),
                "promotion_description": uploader._get_promo_val(
                    content, details, "PromotionDescription", "promotiondescription"
                ),
                "promotion_update_date": uploader._normalize_date(
                    uploader._get_promo_val(
                        content,
                        details,
                        "promotionupdatetime",
                        "PromotionUpdateDate",
                        "priceupdatedate",
                        "PriceUpdateDate",
                    ),
                    now.strftime("%Y-%m-%d"),
                ),
                "promotion_start_date": uploader._normalize_date(
                    uploader._get_promo_val(
                        content,
                        details,
                        "promotionstartdatetime",
                        "PromotionStartDate",
                        "promotionstartdate",
                    ),
                    now.strftime("%Y-%m-%d"),
                ),
                "promotion_start_hour": uploader._get_promo_val(
                    content, details, "PromotionStartHour", "promotionstarthour", default="00:00"
                ),
                "promotion_end_date": uploader._normalize_date(
                    uploader._get_promo_val(
                        content,
                        details,
                        "promotionenddatetime",
                        "PromotionEndDate",
                        "promotionenddate",
                    ),
                    "2099-12-31",
                ),
                "promotion_end_hour": uploader._get_promo_val(
                    content, details, "PromotionEndHour", "promotionendhour", default="23:59"
                ),
                "promotion_days": uploader._get_promo_val(content, details, "PromotionDays", "promotiondays"),
                "redemption_limit": uploader._get_promo_val(content, details, "RedemptionLimit", "redemptionlimit"),
                "reward_type": uploader._get_promo_val(content, details, "RewardType", "rewardtype"),
                "allow_multiple_discounts": uploader._get_promo_val(
                    content, details, "AllowMultipleDiscounts", "allowmultiplediscounts"
                ),
                "is_weighted_promo": uploader._to_bool(
                    uploader._get_promo_val(content, details, "isWeightedPromo", "bisweighted", "bIsWeighted"),
                    False,
                ),
                "is_gift_item": uploader._get_promo_val(content, details, "IsGiftItem", "isgiftitem"),
                "min_no_of_item_offered": uploader._get_promo_val(
                    content, details, "MinNoOfItemOffered", "minnoofitemoffered", "minnoofitemofered"
                ),
                "additional_is_coupon": uploader._get_promo_val(
                    content, details, "AdditionalIsCoupon", "additionaliscoupon"
                ),
                "additional_gift_count": uploader._get_promo_val(
                    content, details, "AdditionalGiftCount", "additionalgiftcount"
                ),
                "additional_is_total": uploader._get_promo_val(
                    content, details, "AdditionalIsTotal", "additionalistotal"
                ),
                "additional_is_active": uploader._get_promo_val(
                    content, details, "AdditionalIsActive", "additionalisactive"
                ),
                "additional_restrictions": uploader._get_promo_val(
                    content, details, "AdditionalRestrictions", "additionalrestrictions"
                ),
                "remarks": uploader._get_promo_val(content, details, "Remarks", "remarks"),
                "min_qty": uploader._get_promo_val(content, details, "MinQty", "minqty"),
                "discounted_price": uploader._get_promo_val(content, details, "DiscountedPrice", "discountedprice"),
                "discounted_price_per_mida": uploader._get_promo_val(
                    content, details, "DiscountedPricePerMida", "discountedpricepermida"
                ),
                "weight_unit": uploader._get_promo_val(content, details, "WeightUnit", "weightunit"),
                "club_id": uploader._get_promo_val(content, details, "ClubId", "clubid"),
                "items": [],
                "store_promotions": {db_store_key: store_promo_val},
                "available_in_store_ids": [db_store_key],
                "created_at": now_iso,
                "updated_at": now_iso,
            }

            if key not in aggregated:
                row["_item_keys"] = set()
                aggregated[key] = row
            else:
                existing = aggregated[key]
                existing["store_promotions"][db_store_key] = store_promo_val
                if db_store_key not in existing["available_in_store_ids"]:
                    existing["available_in_store_ids"].append(db_store_key)
                for field, value in row.items():
                    if field in {
                        "chain_id",
                        "promotion_id",
                        "items",
                        "store_promotions",
                        "available_in_store_ids",
                    }:
                        continue
                    if field == "is_weighted_promo":
                        existing[field] = bool(existing.get(field) or value)
                        continue
                    if existing.get(field) in (None, "") and value not in (None, ""):
                        existing[field] = value

            existing = aggregated[key]
            for promo_item in promotion_items:
                if not promo_item:
                    continue
                item_key = json.dumps(promo_item, ensure_ascii=False, sort_keys=True, default=str)
                if item_key not in existing["_item_keys"]:
                    existing["items"].append(promo_item)
                    existing["_item_keys"].add(item_key)

        if total_rows % (chunk_size * 5) == 0:
            elapsed = time.monotonic() - start
            print(
                f"scan_rows={total_rows} promos={len(aggregated)} elapsed_sec={elapsed:.1f}",
                flush=True,
            )

    for row in aggregated.values():
        row.pop("_item_keys", None)

    return total_rows, aggregated


def _count_superpharm_promos(uploader: PostgresUploader) -> dict[str, int]:
    rows = uploader._run_query(
        """
        SELECT
          COUNT(*)::INT AS total,
          COUNT(*) FILTER (
            WHERE items IS NOT NULL
              AND items <> '[]'::jsonb
              AND items <> '{}'::jsonb
          )::INT AS with_items
        FROM promotions
        WHERE chain_id = %s
        """,
        (SUPERPHARM_CHAIN_ID,),
        fetch=True,
    )
    return dict(rows[0]) if rows else {"total": 0, "with_items": 0}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", type=Path)
    parser.add_argument("--env-file", type=Path, default=Path("../web-backend/.env"))
    parser.add_argument("--chunk-size", type=int, default=100000)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    csv_path = args.csv.resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)

    _load_env_file(args.env_file.resolve())
    if "POSTGRESQL_URL" not in os.environ and "DATABASE_URL" in os.environ:
        os.environ["POSTGRESQL_URL"] = os.environ["DATABASE_URL"]

    uploader = PostgresUploader()
    start = time.monotonic()
    before = _count_superpharm_promos(uploader)
    print(f"before total={before['total']} with_items={before['with_items']}", flush=True)

    total_rows, aggregated = _aggregate_csv(csv_path, args.chunk_size, uploader)
    print(f"uploading aggregated_promos={len(aggregated)}", flush=True)
    uploader._rpc_batch("merge_promotions", list(aggregated.values()))

    after_merge = _count_superpharm_promos(uploader)
    print(
        f"after_merge total={after_merge['total']} with_items={after_merge['with_items']}",
        flush=True,
    )

    if args.refresh:
        print("refreshing promotion_store_items/top_promotions_cache", flush=True)
        uploader._flush_pending_promotion_refresh()
        after_refresh = uploader._run_query(
            """
            SELECT
              COUNT(*)::INT AS psi_rows,
              COUNT(DISTINCT promotion_id)::INT AS psi_promos
            FROM promotion_store_items
            WHERE chain_id = %s
            """,
            (SUPERPHARM_CHAIN_ID,),
            fetch=True,
        )
        print(f"after_refresh {dict(after_refresh[0]) if after_refresh else {}}", flush=True)

    uploader.close()
    elapsed = time.monotonic() - start
    print(f"done rows={total_rows} elapsed_sec={elapsed:.1f}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
