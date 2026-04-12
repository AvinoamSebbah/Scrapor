import argparse
import json
import os
import sys

import psycopg2


def resolve_db_url() -> str:
    db_url = (
        os.getenv("POSTGRESQL_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
    )
    if not db_url:
        raise ValueError("Set POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL)")
    return db_url


def fetch_chains(db_url: str) -> list[dict[str, str]]:
    query = """
    SELECT
      s.chain_id,
      COALESCE(NULLIF(BTRIM(s.chain_name), ''), s.chain_id) AS chain_name
    FROM (
      SELECT DISTINCT ON (chain_id)
        chain_id,
        chain_name,
        updated_at,
        created_at
      FROM stores
      WHERE chain_id IS NOT NULL
        AND BTRIM(chain_id) <> ''
      ORDER BY
        chain_id,
        (chain_name IS NULL OR BTRIM(chain_name) = '') ASC,
        updated_at DESC NULLS LAST,
        created_at DESC NULLS LAST
    ) s
    ORDER BY s.chain_id;
    """

    conn = psycopg2.connect(db_url, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
        return [
            {"chainId": str(chain_id), "chainName": str(chain_name)}
            for chain_id, chain_name in rows
        ]
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export distinct chains as JSON text (chainId, chainName)."
    )
    parser.add_argument(
        "--output",
        help="Optional output file path. If omitted, prints to stdout.",
    )
    args = parser.parse_args()

    try:
        db_url = resolve_db_url()
        payload = fetch_chains(db_url)
        text = json.dumps(payload, ensure_ascii=False, indent=2)

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                f.write(text + "\n")
            print(f"Saved {len(payload)} chains to {args.output}")
        else:
            print(text)

        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
