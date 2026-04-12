#!/usr/bin/env python3
"""Trace a product barcode through promotions/price pipelines.

Usage examples:
  python scripts/trace_promotion_barcode.py 7290001131121
  python scripts/trace_promotion_barcode.py 7290001131121 --city "ירושלים"
  python scripts/trace_promotion_barcode.py 7290001131121 --city "ירושלים" --chain-id 7290103152017 --store-id 3

Reads PostgreSQL URL from one of:
  - POSTGRESQL_URL
  - DATABASE_URL
  - SUPABASE_DATABASE_URL
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


def resolve_db_url() -> str:
    db_url = (
        os.getenv("POSTGRESQL_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
    )
    if not db_url:
        raise ValueError("POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) must be set")
    return db_url


def to_json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def json_default(value: Any) -> Any:
    converted = to_json_value(value)
    if converted is not value:
        return converted
    return str(value)


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", ".")
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def first_match(rows: list[dict[str, Any]], **criteria: Any) -> dict[str, Any] | None:
    for row in rows:
        ok = True
        for key, expected in criteria.items():
            if str(row.get(key)) != str(expected):
                ok = False
                break
        if ok:
            return row
    return None


def filter_rows(rows: list[dict[str, Any]], **criteria: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        ok = True
        for key, expected in criteria.items():
            if str(row.get(key)) != str(expected):
                ok = False
                break
        if ok:
            out.append(row)
    return out


def fetch_all(cur, query: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
    cur.execute(query, params)
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def build_report(
    cur,
    barcode: str,
    city: str | None,
    chain_id: str | None,
    store_id: str | None,
    window_hours: int,
    row_limit: int,
) -> dict[str, Any]:
    product_rows = fetch_all(
        cur,
        """
        SELECT
          id,
          item_code,
          item_name,
          manufacturer_name,
          created_at,
          updated_at
        FROM products
        WHERE item_code = %s
        LIMIT 1
        """,
        (barcode,),
    )

    if not product_rows:
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "barcode": barcode,
            "filters": {
                "city": city,
                "chainId": chain_id,
                "storeId": store_id,
                "windowHours": window_hours,
            },
            "error": "Product not found for barcode",
        }

    product = product_rows[0]
    product_id = product["id"]

    price_rows = fetch_all(
        cur,
        """
        SELECT
          s.id AS store_db_id,
          s.chain_id,
          s.chain_name,
          s.store_id,
          s.store_name,
          s.city,
          pp.price,
          pp.promo_price AS promo_price_in_product_prices,
          pp.updated_at AS price_updated_at
        FROM product_prices pp
        JOIN stores s ON s.id = pp.store_id
        WHERE pp.product_id = %s
        ORDER BY pp.updated_at DESC NULLS LAST, pp.price ASC NULLS LAST
        LIMIT %s
        """,
        (product_id, row_limit),
    )

    psi_rows = fetch_all(
        cur,
        """
        SELECT
          psi.chain_id,
          psi.promotion_id,
          s.id AS store_db_id,
          s.store_id,
          s.store_name,
          s.city,
          psi.promo_price,
          psi.promotion_end_date,
          psi.updated_at AS psi_updated_at
        FROM promotion_store_items psi
        JOIN stores s ON s.id = psi.store_id
        WHERE psi.product_id = %s
        ORDER BY psi.promo_price ASC NULLS LAST, psi.updated_at DESC NULLS LAST
        LIMIT %s
        """,
        (product_id, row_limit),
    )

    promotion_rows = fetch_all(
        cur,
        """
        SELECT
          psi.chain_id,
          psi.promotion_id,
          s.store_id,
          s.store_name,
          s.city,
          psi.promo_price,
          psi.promotion_end_date AS psi_end_date,
          psi.updated_at AS psi_updated_at,
          p.promotion_description,
          p.promotion_start_date,
          p.promotion_end_date AS promotion_end_date,
          p.is_weighted_promo,
          p.min_qty,
          p.discounted_price,
          p.discounted_price_per_mida,
          p.weight_unit,
          p.updated_at AS promotion_updated_at
        FROM promotion_store_items psi
        JOIN stores s
          ON s.id = psi.store_id
        JOIN promotions p
          ON p.chain_id = psi.chain_id
         AND p.promotion_id = psi.promotion_id
        WHERE psi.product_id = %s
        ORDER BY psi.promo_price ASC NULLS LAST, psi.updated_at DESC NULLS LAST
        LIMIT %s
        """,
        (product_id, row_limit),
    )

    promotion_item_rows = fetch_all(
        cur,
        """
        WITH target_promos AS (
          SELECT DISTINCT chain_id, promotion_id
          FROM promotion_store_items
          WHERE product_id = %s
        )
        SELECT
          p.chain_id,
          p.promotion_id,
          COALESCE(obj.obj->>'itemcode', obj.obj->>'ItemCode') AS item_code,
                    obj.obj->>'discountedprice' AS item_discountedprice_lc,
                    obj.obj->>'DiscountedPrice' AS item_discountedprice_uc,
                    obj.obj->>'discountedpricepermida' AS item_discountedpricepermida_lc,
                    obj.obj->>'DiscountedPricePerMida' AS item_discountedpricepermida_uc,
                    obj.obj->>'bisweighted' AS item_bisweighted_lc,
                    obj.obj->>'bIsWeighted' AS item_bisweighted_uc,
                    obj.obj->>'discountrate' AS item_discountrate_lc,
                    obj.obj->>'DiscountRate' AS item_discountrate_uc,
                    obj.obj->>'maxqty' AS item_maxqty_lc,
                    obj.obj->>'MaxQty' AS item_maxqty_uc,
                    obj.obj->>'minqty' AS item_minqty_lc,
                    obj.obj->>'MinQty' AS item_minqty_uc,
          p.updated_at AS promotion_updated_at
        FROM promotions p
        JOIN target_promos tp
          ON tp.chain_id = p.chain_id
         AND tp.promotion_id = p.promotion_id
        JOIN LATERAL (
                    SELECT q.val AS obj
                    FROM jsonb_path_query(COALESCE(p.items, '[]'::jsonb), '$.**') AS q(val)
                    WHERE jsonb_typeof(q.val) = 'object'
                        AND COALESCE(q.val->>'itemcode', q.val->>'ItemCode') IS NOT NULL
        ) obj ON TRUE
        WHERE COALESCE(obj.obj->>'itemcode', obj.obj->>'ItemCode') = %s
        ORDER BY p.updated_at DESC NULLS LAST
        LIMIT %s
        """,
        (product_id, barcode, row_limit),
    )

    offer_rows = fetch_all(
        cur,
        """
        SELECT
          s.chain_id,
          s.chain_name,
          s.store_id,
          s.store_name,
          s.city,
          pp.price,
          pb.promo_price AS promo_from_psi,
          pp.promo_price AS promo_from_product_prices,
          CASE
            WHEN COALESCE(pb.promo_price, pp.promo_price) IS NOT NULL
              AND COALESCE(pb.promo_price, pp.promo_price) < pp.price
            THEN COALESCE(pb.promo_price, pp.promo_price)
            ELSE NULL
          END AS promo_effective,
          LEAST(pp.price, COALESCE(pb.promo_price, pp.promo_price, pp.price)) AS effective_price,
          ROUND(
            CASE
              WHEN pp.price > 0 THEN
                ((pp.price - LEAST(pp.price, COALESCE(pb.promo_price, pp.promo_price, pp.price))) / pp.price) * 100.0
              ELSE 0
            END,
            2
          ) AS discount_percent,
          pp.updated_at
        FROM product_prices pp
        JOIN stores s ON s.id = pp.store_id
        LEFT JOIN LATERAL (
          SELECT MIN(psi.promo_price) AS promo_price
          FROM promotion_store_items psi
          WHERE psi.product_id = pp.product_id
            AND psi.store_id = pp.store_id
            AND psi.promo_price IS NOT NULL
            AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
        ) pb ON TRUE
        WHERE pp.product_id = %s
          AND pp.price IS NOT NULL
          AND (%s IS NULL OR s.city ILIKE %s || '%%')
          AND (%s IS NULL OR s.chain_id = %s)
          AND (%s IS NULL OR s.store_id = %s)
        ORDER BY effective_price ASC NULLS LAST, pp.updated_at DESC NULLS LAST
        LIMIT %s
        """,
        (
            product_id,
            city,
            city,
            chain_id,
            chain_id,
            store_id,
            store_id,
            row_limit,
        ),
    )

    cache_rows = fetch_all(
        cur,
        """
        SELECT
          window_hours,
          scope_type,
          city,
          chain_id,
          chain_name,
          store_id,
          store_name,
          rank_position,
          price,
          promo_price,
          effective_price,
          discount_amount,
          discount_percent,
          smart_score,
          promotion_end_date,
          updated_at,
          refreshed_at
        FROM top_promotions_cache
        WHERE item_code = %s
          AND (%s IS NULL OR city ILIKE %s || '%%')
          AND (%s IS NULL OR chain_id = %s)
          AND (%s IS NULL OR store_id = %s)
        ORDER BY refreshed_at DESC NULLS LAST, window_hours ASC, scope_type ASC, rank_position ASC
        LIMIT %s
        """,
        (
            barcode,
            city,
            city,
            chain_id,
            chain_id,
            store_id,
            store_id,
            row_limit,
        ),
    )

    top_city_rows: list[dict[str, Any]] = []
    if city:
        top_city_rows = fetch_all(
            cur,
            """
            SELECT *
            FROM get_top_city_promotions(%s, %s, %s, %s, 50, 0)
            WHERE item_code = %s
            """,
            (city, chain_id, store_id, window_hours, barcode),
        )

    low_offer = offer_rows[0] if offer_rows else None
    lineage: dict[str, Any] = {}
    if low_offer:
        focus_chain = low_offer.get("chain_id")
        focus_store = low_offer.get("store_id")
        focus_promo_ids = {
            row.get("promotion_id")
            for row in filter_rows(psi_rows, chain_id=focus_chain, store_id=focus_store)
            if row.get("promotion_id")
        }

        lineage = {
            "focus_offer": low_offer,
            "price_row": first_match(price_rows, chain_id=focus_chain, store_id=focus_store),
            "psi_rows": filter_rows(psi_rows, chain_id=focus_chain, store_id=focus_store),
            "promotion_rows": [
                row
                for row in filter_rows(promotion_rows, chain_id=focus_chain, store_id=focus_store)
                if row.get("promotion_id") in focus_promo_ids
            ],
            "promotion_item_rows": [
                row
                for row in filter_rows(promotion_item_rows, chain_id=focus_chain)
                if row.get("promotion_id") in focus_promo_ids
            ],
            "cache_rows": filter_rows(cache_rows, chain_id=focus_chain, store_id=focus_store),
        }

    low_in_promotions = any(
        (as_float(row.get("discounted_price")) is not None and as_float(row.get("discounted_price")) < 1)
        for row in promotion_rows
    )

    def has_weighted_item_signal(row: dict[str, Any]) -> bool:
        weighted_markers = {
            str(row.get("item_bisweighted_lc", "")).strip().lower(),
            str(row.get("item_bisweighted_uc", "")).strip().lower(),
        }
        if "1" in weighted_markers or "true" in weighted_markers or "yes" in weighted_markers:
            return True
        if row.get("item_discountedpricepermida_lc") not in (None, ""):
            return True
        if row.get("item_discountedpricepermida_uc") not in (None, ""):
            return True
        return False

    low_in_promo_items = any(
        (
            (
                as_float(row.get("item_discountedprice_lc")) is not None
                and as_float(row.get("item_discountedprice_lc")) < 1
            )
            or (
                as_float(row.get("item_discountedprice_uc")) is not None
                and as_float(row.get("item_discountedprice_uc")) < 1
            )
        )
        for row in promotion_item_rows
    )
    low_in_psi = any(as_float(row.get("promo_price")) is not None and as_float(row.get("promo_price")) < 1 for row in psi_rows)
    low_in_offer = any(
        as_float(row.get("promo_effective")) is not None and as_float(row.get("promo_effective")) < 1
        for row in offer_rows
    )
    low_in_cache = any(as_float(row.get("promo_price")) is not None and as_float(row.get("promo_price")) < 1 for row in cache_rows)
    weighted_detected = any(has_weighted_item_signal(row) for row in promotion_item_rows) or any(
        bool(row.get("is_weighted_promo"))
        or (row.get("discounted_price_per_mida") is not None and str(row.get("discounted_price_per_mida")).strip() != "")
        for row in promotion_rows
    )

    weighted_low_price_signal = any(
        has_weighted_item_signal(row)
        and (
            (
                as_float(row.get("item_discountedprice_lc")) is not None
                and as_float(row.get("item_discountedprice_lc")) < 1
            )
            or (
                as_float(row.get("item_discountedprice_uc")) is not None
                and as_float(row.get("item_discountedprice_uc")) < 1
            )
        )
        for row in promotion_item_rows
    )

    likely_stage = "not_detected"
    if low_in_promotions or low_in_promo_items:
        likely_stage = "stored_in_promotions_or_source_feed"
    elif low_in_psi:
        likely_stage = "introduced_during_refresh_promotion_store_items"
    elif low_in_offer:
        likely_stage = "introduced_during_offers_read_path"
    elif low_in_cache:
        likely_stage = "introduced_during_top_promotions_cache_refresh"

    analysis_hints: list[str] = []
    if weighted_low_price_signal:
        analysis_hints.append(
            "Weighted promotion marker found with discountedprice < 1; this often means per-unit/per-weight value (e.g. per 0.01kg) is being treated as full item price."
        )
    if low_in_psi and not low_in_promotions and not low_in_promo_items:
        analysis_hints.append(
            "Low price appears in promotion_store_items but not in selected promotions fields; verify JSON extraction paths and merge logic."
        )
    if low_in_cache and low_in_offer and low_in_psi:
        analysis_hints.append(
            "Cache and read-path are consistent with promotion_store_items; root cause is likely upstream of top_promotions_cache."
        )

    min_regular_price = min(
        (as_float(row.get("price")) for row in price_rows if as_float(row.get("price")) is not None),
        default=None,
    )
    min_psi_price = min(
        (as_float(row.get("promo_price")) for row in psi_rows if as_float(row.get("promo_price")) is not None),
        default=None,
    )

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "barcode": barcode,
        "filters": {
            "city": city,
            "chainId": chain_id,
            "storeId": store_id,
            "windowHours": window_hours,
            "rowLimit": row_limit,
        },
        "summary": {
            "productFound": True,
            "productId": product_id,
            "rows": {
                "priceRows": len(price_rows),
                "promotionStoreItemRows": len(psi_rows),
                "promotionJoinRows": len(promotion_rows),
                "promotionItemRows": len(promotion_item_rows),
                "offerRows": len(offer_rows),
                "cacheRows": len(cache_rows),
                "topCityRows": len(top_city_rows),
            },
            "minRegularPrice": min_regular_price,
            "minPromoPriceInPromotionStoreItems": min_psi_price,
            "lowestOffer": low_offer,
            "flags": {
                "lowPromoInPromotions": low_in_promotions,
                "lowPromoInPromotionItems": low_in_promo_items,
                "lowPromoInPromotionStoreItems": low_in_psi,
                "lowPromoInOffers": low_in_offer,
                "lowPromoInTopCache": low_in_cache,
                "weightedPromotionDetected": weighted_detected,
                "weightedLowPriceSignal": weighted_low_price_signal,
            },
            "likely_issue_stage": likely_stage,
            "analysis_hints": analysis_hints,
        },
        "product": product,
        "lineage_focus": lineage,
        "tables": {
            "product_prices_joined": price_rows,
            "promotion_store_items_joined": psi_rows,
            "promotions_joined": promotion_rows,
            "promotion_items_for_barcode": promotion_item_rows,
            "offers_recomputed": offer_rows,
            "top_promotions_cache": cache_rows,
            "top_city_function_rows": top_city_rows,
        },
    }
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace how a barcode flows through prices/promotions/cache to debug anomalies."
    )
    parser.add_argument("barcode", help="Product barcode / item_code to trace")
    parser.add_argument("--city", default=None, help="Optional city filter (prefix match, e.g. ירושלים)")
    parser.add_argument("--chain-id", default=None, help="Optional chain_id filter")
    parser.add_argument("--store-id", default=None, help="Optional store_id filter")
    parser.add_argument("--window-hours", type=int, default=24, help="Window hours for get_top_city_promotions")
    parser.add_argument("--row-limit", type=int, default=120, help="Max rows per diagnostic section")
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output file path for JSON report (if omitted, prints to stdout)",
    )
    return parser.parse_args()


def normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def main() -> int:
    args = parse_args()

    barcode = normalize_optional(args.barcode)
    if not barcode:
        print("Error: barcode is required", file=sys.stderr)
        return 2

    city = normalize_optional(args.city)
    chain_id = normalize_optional(args.chain_id)
    store_id = normalize_optional(args.store_id)
    row_limit = max(int(args.row_limit), 1)
    window_hours = max(int(args.window_hours), 1)

    try:
        db_url = resolve_db_url()
        conn = psycopg2.connect(db_url, connect_timeout=20, cursor_factory=RealDictCursor)
    except Exception as exc:
        print(f"Database connection failed: {exc}", file=sys.stderr)
        return 1

    try:
        with conn.cursor() as cur:
            report = build_report(
                cur=cur,
                barcode=barcode,
                city=city,
                chain_id=chain_id,
                store_id=store_id,
                window_hours=window_hours,
                row_limit=row_limit,
            )
    finally:
        conn.close()

    encoded = json.dumps(report, indent=2, ensure_ascii=False, default=json_default)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(encoded)
        print(f"Wrote report to: {args.output}")
    else:
        print(encoded)

    error = report.get("error")
    if error:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
