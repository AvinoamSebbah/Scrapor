# -*- coding: utf-8 -*-
"""Read-only benchmark for the product search SQL path.

Environment:
  DATABASE_URL or POSTGRESQL_URL
  SEARCH_BENCH_CITY       default: ירושלים
  SEARCH_BENCH_QUERIES    pipe-separated Hebrew queries
  SEARCH_BENCH_LIMIT      default: 10
  SEARCH_BENCH_PAGES      default: 2
  SEARCH_BENCH_REPEAT     default: 3
  SEARCH_BENCH_CHAIN_ID   optional
  SEARCH_BENCH_STORE_ID   optional
"""

from __future__ import annotations

import json
import os
import statistics
import time
from decimal import Decimal
from typing import Any

import psycopg2
import psycopg2.extras


DEFAULT_QUERIES = [
    "מטרנה שלב 2",
    "חלב בקרטון",
    "לחם",
    "ביצים",
    "יוגורט",
    "גבינה",
    "שמן",
    "אורז",
    "קפה",
    "טונה",
]


SEARCH_SQL = """
WITH params AS (
  SELECT
    %(query)s::text AS raw_query,
    websearch_to_tsquery('simple', %(query)s::text) AS tsq
),
fts_candidates AS MATERIALIZED (
  SELECT
    p.id AS product_id,
    p.item_code,
    p.item_name,
    p.manufacturer_name,
    (
      ts_rank(p.search_idx_col, params.tsq)
      + CASE
          WHEN lower(COALESCE(p.item_name, '')) = lower(params.raw_query) THEN 10
          WHEN lower(COALESCE(p.item_name, '')) LIKE lower(params.raw_query) || '%%' THEN 6
          WHEN lower(COALESCE(p.item_name, '')) LIKE '%% ' || lower(params.raw_query) || '%%' THEN 3
          ELSE 0
        END
      + CASE
          WHEN lower(COALESCE(p.manufacturer_name, '')) LIKE lower(params.raw_query) || '%%' THEN 0.4
          ELSE 0
        END
    )::real AS text_rank
  FROM products p
  CROSS JOIN params
  WHERE COALESCE(%(query)s::text, '') <> ''
    AND p.search_idx_col @@ params.tsq
  ORDER BY text_rank DESC NULLS LAST, p.item_code ASC
  LIMIT %(search_text_candidate_limit)s::integer
)
SELECT
  c.product_id,
  c.item_code,
  c.item_name,
  c.manufacturer_name,
  (
    c.text_rank
    + (LN(1 + LEAST(COALESCE(pss.chain_count, 0), 40)) * 0.22)
    + (LEAST(COALESCE(pss.chain_count, 0), 25) * 0.03)
  )::real AS rank,
  COALESCE(pss.chain_count, 0)::integer AS chain_count
FROM fts_candidates c
LEFT JOIN product_search_stats pss ON pss.product_id = c.product_id
ORDER BY chain_count DESC NULLS LAST, rank DESC NULLS LAST, item_code ASC
LIMIT %(candidate_limit)s::integer
OFFSET %(offset)s::integer
"""


FALLBACK_SQL = """
WITH params AS (
  SELECT
    %(query)s::text AS raw_query,
    regexp_replace(%(query)s::text, '\\s+', '', 'g') AS compact_query
),
candidate_hits AS (
  SELECT
    p.id,
    p.item_code,
    p.item_name,
    p.manufacturer_name,
    similarity(p.item_name, params.raw_query) AS base_similarity
  FROM products p
  CROSS JOIN params
  WHERE p.item_name %% params.raw_query
    AND (cardinality(%(existing_ids)s::int[]) = 0 OR p.id <> ALL(%(existing_ids)s::int[]))
  UNION ALL
  SELECT
    p.id,
    p.item_code,
    p.item_name,
    p.manufacturer_name,
    similarity(p.manufacturer_name, params.raw_query) AS base_similarity
  FROM products p
  CROSS JOIN params
  WHERE p.manufacturer_name %% params.raw_query
    AND (cardinality(%(existing_ids)s::int[]) = 0 OR p.id <> ALL(%(existing_ids)s::int[]))
),
ranked_candidates AS (
  SELECT DISTINCT ON (id)
    id,
    item_code,
    item_name,
    manufacturer_name,
    base_similarity
  FROM candidate_hits
  ORDER BY id, base_similarity DESC NULLS LAST
),
candidates AS MATERIALIZED (
  SELECT
    id,
    item_code,
    item_name,
    manufacturer_name
  FROM ranked_candidates
  ORDER BY base_similarity DESC NULLS LAST, item_code ASC
  LIMIT 1000
)
SELECT
  c.id AS product_id,
  c.item_code,
  c.item_name,
  c.manufacturer_name,
  (
    GREATEST(
      similarity(COALESCE(c.item_name, ''), params.raw_query),
      similarity(COALESCE(c.manufacturer_name, ''), params.raw_query),
      similarity(regexp_replace(COALESCE(c.item_name, ''), '\\s+', '', 'g'), params.compact_query),
      word_similarity(params.raw_query, COALESCE(c.item_name, '')),
      word_similarity(params.raw_query, COALESCE(c.manufacturer_name, ''))
    ) * 3.5
    + CASE
        WHEN lower(COALESCE(c.item_name, '')) LIKE lower(params.raw_query) || '%%' THEN 2
        ELSE 0
      END
    + (LN(1 + LEAST(COALESCE(pss.chain_count, 0), 40)) * 0.18)
    + (LEAST(COALESCE(pss.chain_count, 0), 25) * 0.02)
  )::real AS rank,
  COALESCE(pss.chain_count, 0)::integer AS chain_count
FROM candidates c
CROSS JOIN params
LEFT JOIN product_search_stats pss ON pss.product_id = c.id
ORDER BY chain_count DESC NULLS LAST, rank DESC NULLS LAST, c.item_code ASC
LIMIT %(fallback_limit)s::integer
"""


SUMMARY_SQL = """
WITH selected_products AS (
  SELECT unnest(%(product_ids)s::int[]) AS product_id
),
city_stores AS MATERIALIZED (
  SELECT
    id,
    chain_id,
    store_id,
    COALESCE(NULLIF(chain_name, ''), chain_id)::text AS chain_name
  FROM stores
  WHERE (%(city)s::text IS NULL OR city = %(city)s::text OR city ILIKE %(city)s::text || '%%')
    AND (%(chain_id)s::text IS NULL OR chain_id = %(chain_id)s::text)
    AND (%(store_id)s::text IS NULL OR store_id = %(store_id)s::text)
),
priced_rows AS (
  SELECT
    sp.product_id,
    p.item_code,
    cs.chain_name,
    pp.price,
    LEAST(pp.price, COALESCE(pb.promo_price, pp.promo_price, pp.price)) AS effective_price,
    (
      COALESCE(pb.promo_price, pp.promo_price) IS NOT NULL
      AND COALESCE(pb.promo_price, pp.promo_price) < pp.price
      AND COALESCE(pb.promo_price, pp.promo_price) >= (pp.price * 0.05)
    ) AS has_promo
  FROM selected_products sp
  JOIN products p ON p.id = sp.product_id
  JOIN city_stores cs ON TRUE
  JOIN product_prices pp ON pp.product_id = sp.product_id AND pp.store_id = cs.id
  LEFT JOIN LATERAL (
    SELECT MIN(psi.promo_price) AS promo_price
    FROM promotion_store_items psi
    WHERE psi.product_id = pp.product_id
      AND psi.store_id = pp.store_id
      AND psi.promo_price IS NOT NULL
      AND (psi.promotion_end_date IS NULL OR psi.promotion_end_date >= CURRENT_DATE)
  ) pb ON TRUE
  WHERE pp.price IS NOT NULL
),
chain_rows AS (
  SELECT
    product_id,
    item_code,
    chain_name,
    MIN(price) AS min_price,
    MIN(effective_price) AS min_effective_price,
    BOOL_OR(has_promo) AS has_promo
  FROM priced_rows
  GROUP BY product_id, item_code, chain_name
)
SELECT
  product_id,
  item_code,
  MIN(min_price) AS min_price,
  MIN(min_effective_price) AS min_effective_price,
  BOOL_OR(has_promo) AS has_promo,
  ARRAY_AGG(chain_name ORDER BY chain_name) FILTER (WHERE chain_name IS NOT NULL) AS available_chains
FROM chain_rows
GROUP BY product_id, item_code
"""


def positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw else default
    except ValueError:
        return default
    return value if value > 0 else default


def elapsed_ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


def fetch_timed(cur, sql: str, params: dict[str, Any]) -> tuple[list[dict[str, Any]], float]:
    start = time.perf_counter()
    cur.execute(sql, params)
    rows = [dict(row) for row in cur.fetchall()]
    return rows, elapsed_ms(start)


def run_search(cur, query: str, page: int, settings: dict[str, Any]) -> dict[str, Any]:
    limit = settings["limit"]
    offset = (page - 1) * limit
    has_narrow_store_filter = bool(settings["chain_id"] or settings["store_id"])
    candidate_limit = min(
        max(limit * (8 if has_narrow_store_filter else 2), 60 if has_narrow_store_filter else limit + 5),
        160 if has_narrow_store_filter else 30,
    )
    search_text_candidate_limit = min(max(offset + candidate_limit * 40, 1000), 2000)
    fallback_limit = min(max(limit * 3, 30), 80)

    search_rows, search_ms = fetch_timed(
        cur,
        SEARCH_SQL,
        {
            "query": query,
            "candidate_limit": candidate_limit,
            "search_text_candidate_limit": search_text_candidate_limit,
            "offset": offset,
        },
    )

    fallback_ms = 0.0
    fallback_rows: list[dict[str, Any]] = []
    if len(search_rows) < limit and len(query) >= 2:
        fallback_rows, fallback_ms = fetch_timed(
            cur,
            FALLBACK_SQL,
            {
                "query": query,
                "existing_ids": [row["product_id"] for row in search_rows],
                "fallback_limit": fallback_limit,
            },
        )

    rows = search_rows + fallback_rows
    summary_ms = 0.0
    summary_rows: list[dict[str, Any]] = []
    if rows:
        summary_rows, summary_ms = fetch_timed(
            cur,
            SUMMARY_SQL,
            {
                "product_ids": [row["product_id"] for row in rows],
                "city": settings["city"],
                "chain_id": settings["chain_id"],
                "store_id": settings["store_id"],
            },
        )

    available_codes = {row["item_code"] for row in summary_rows}
    filtered_count = len([row for row in rows if row["item_code"] in available_codes][:limit])

    return {
        "query": query,
        "page": page,
        "offset": offset,
        "candidateLimit": candidate_limit,
        "rowCount": len(rows),
        "filteredCount": filtered_count,
        "timingsMs": {
            "searchSql": search_ms,
            "fallbackSql": fallback_ms,
            "summarySql": summary_ms,
            "total": search_ms + fallback_ms + summary_ms,
        },
    }


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, max(0, round((pct / 100) * (len(sorted_values) - 1))))
    return sorted_values[index]


def summarize(runs: list[dict[str, Any]]) -> dict[str, Any]:
    totals = [run["timingsMs"]["total"] for run in runs]
    fallback = [run["timingsMs"]["fallbackSql"] for run in runs]
    summary = [run["timingsMs"]["summarySql"] for run in runs]
    return {
        "count": len(runs),
        "totalAvgMs": statistics.fmean(totals) if totals else 0.0,
        "totalP50Ms": percentile(totals, 50),
        "totalP95Ms": percentile(totals, 95),
        "totalMaxMs": max(totals, default=0.0),
        "fallbackMaxMs": max(fallback, default=0.0),
        "summaryMaxMs": max(summary, default=0.0),
    }


def clean_json(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 1)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [clean_json(item) for item in value]
    if isinstance(value, dict):
        return {key: clean_json(item) for key, item in value.items()}
    return value


def main() -> None:
    db_url = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not db_url:
        raise SystemExit("Set POSTGRESQL_URL or DATABASE_URL")

    queries = [
        query.strip()
        for query in os.getenv("SEARCH_BENCH_QUERIES", "|".join(DEFAULT_QUERIES)).split("|")
        if query.strip()
    ]
    settings = {
        "city": os.getenv("SEARCH_BENCH_CITY", "ירושלים"),
        "chain_id": os.getenv("SEARCH_BENCH_CHAIN_ID") or None,
        "store_id": os.getenv("SEARCH_BENCH_STORE_ID") or None,
        "limit": positive_int("SEARCH_BENCH_LIMIT", 10),
    }
    pages = positive_int("SEARCH_BENCH_PAGES", 2)
    repeat = positive_int("SEARCH_BENCH_REPEAT", 3)

    conn = psycopg2.connect(
        db_url,
        connect_timeout=15,
        application_name="scrapor-search-benchmark",
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    conn.set_session(readonly=True, autocommit=False)

    runs: list[dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30000ms'")
            print(
                "search benchmark "
                f"city={settings['city']} limit={settings['limit']} pages={pages} "
                f"repeat={repeat} queries={len(queries)}"
            )
            for query in queries:
                for page in range(1, pages + 1):
                    for attempt in range(1, repeat + 1):
                        run = run_search(cur, query, page, settings)
                        runs.append(run)
                        timings = run["timingsMs"]
                        print(
                            f"q={query!r} page={page} try={attempt} "
                            f"rows={run['rowCount']} filtered={run['filteredCount']} "
                            f"search={timings['searchSql']:.1f}ms "
                            f"fallback={timings['fallbackSql']:.1f}ms "
                            f"summary={timings['summarySql']:.1f}ms "
                            f"total={timings['total']:.1f}ms"
                        )
            conn.rollback()
    finally:
        conn.close()

    print("SUMMARY " + json.dumps(clean_json(summarize(runs)), ensure_ascii=False))


if __name__ == "__main__":
    main()
