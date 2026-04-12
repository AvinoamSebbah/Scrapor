import os
import psycopg2


def main() -> None:
    db_url = os.getenv("POSTGRESQL_URL") or os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not db_url:
        raise ValueError("POSTGRESQL_URL (or DATABASE_URL / SUPABASE_DATABASE_URL) must be set")

    conn = psycopg2.connect(db_url, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.product_prices')")
            print("tables=", cur.fetchone())

            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename='product_prices' ORDER BY indexname"
            )
            print("product_prices_indexes=", [r[0] for r in cur.fetchall()])

            cur.execute("SELECT COUNT(*) FROM product_prices")
            print("product_prices_count=", cur.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
