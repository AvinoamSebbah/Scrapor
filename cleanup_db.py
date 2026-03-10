"""Safe database cleanup.

Supports two connection modes (tried in order):
  1. supabase-py REST API  — set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
  2. psycopg2 direct SQL  — set SUPABASE_DATABASE_URL (fallback, works locally)

RULES (never violated):
1. Products and stores are NEVER deleted.
2. Promotions: deleted ONLY when promotion_end_date is strictly in the past (NULL rows skipped).
3. Prices    : deleted ONLY when updated_at < cutoff (default 60 days). No cascade risk.
4. Processed files: deleted when processed_at < cutoff (default 7 days). No cascade risk.
5. Dry-run by default. Pass --execute to apply real deletions.

Environment variables:
  SUPABASE_URL               — Supabase project URL  (REST mode)
  SUPABASE_SERVICE_ROLE_KEY  — Service role key       (REST mode)
  SUPABASE_DATABASE_URL      — PostgreSQL URL          (direct mode fallback)
  PRICES_STALE_DAYS          — Override stale-price threshold in days (default 60)
  PROCESSED_FILES_DAYS       — Override processed-file retention in days (default 7)
"""

import os
import sys
from datetime import datetime, timedelta, timezone

_HTTP_TIMEOUT = 120
_PRICES_STALE_DAYS_DEFAULT = 60
_PROCESSED_FILES_DAYS_DEFAULT = 7


class DatabaseCleaner:
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.stats = {
            "expired_promotions": 0,
            "stale_prices": 0,
            "old_processed_files": 0,
        }

        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        db_url = os.getenv("SUPABASE_DATABASE_URL")

        if url and key:
            from supabase import create_client
            self.client = create_client(url, key)
            try:
                self.client.postgrest.session.timeout = _HTTP_TIMEOUT
            except Exception:
                pass
            self._mode = "rest"
            print("[i] Connection mode: supabase-py REST")
        elif db_url:
            import psycopg2
            self._conn = psycopg2.connect(db_url, connect_timeout=15)
            self._conn.autocommit = True
            # 5-minute per-statement guard (cleanup DELETEs on large tables can be slow)
            with self._conn.cursor() as cur:
                cur.execute("SET statement_timeout = '300s'")
            self._mode = "psycopg2"
            print("[i] Connection mode: psycopg2 direct SQL")
        else:
            raise ValueError(
                "Set SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY  OR  SUPABASE_DATABASE_URL"
            )

    # ------------------------------------------------------------------
    # internal: unified query helpers
    # ------------------------------------------------------------------

    def _rpc_or_sql(self, rpc_name: str, params: dict, sql_count: str, sql_delete: str) -> int:
        """Run a cleanup operation via RPC (REST mode) or direct SQL (psycopg2 mode)."""
        if self._mode == "rest":
            try:
                result = self.client.rpc(rpc_name, params).execute()
                return result.data if isinstance(result.data, int) else 0
            except Exception as e:
                print(f"  [!] RPC error: {e}")
                return 0
        else:
            try:
                with self._conn.cursor() as cur:
                    if self.dry_run:
                        cur.execute(sql_count, params)
                        return cur.fetchone()[0]
                    else:
                        cur.execute(sql_delete, params)
                        return cur.rowcount
            except Exception as e:
                print(f"  [!] SQL error: {e}")
                return 0

    def _stats_summary(self) -> dict:
        if self._mode == "rest":
            try:
                result = self.client.rpc("get_table_stats").execute()
                return {row["table_name"]: row["approx_count"] for row in (result.data or [])}
            except Exception as e:
                print(f"  (summary error: {e})")
                return {}
        else:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("""
                        SELECT relname, n_live_tup
                        FROM pg_stat_user_tables
                        WHERE relname IN ('stores','products','prices','promotions','processed_files')
                    """)
                    return {row[0]: row[1] for row in cur.fetchall()}
            except Exception as e:
                print(f"  (summary error: {e})")
                return {}

    def print_db_summary(self):
        """Print approximate row counts using pg_stat_user_tables — no table scan."""
        print("\n" + "=" * 60)
        print("DATABASE SUMMARY (approximate)")
        print("=" * 60)
        labels = {
            "stores":          "Stores",
            "products":        "Products",
            "prices":          "Prices",
            "promotions":      "Promotions",
            "processed_files": "Processed files",
        }
        counts = self._stats_summary()
        for table, label in labels.items():
            count = counts.get(table, "?")
            if isinstance(count, int):
                print(f"  {label:<25}: {count:>12,}")
            else:
                print(f"  {label:<25}: {count}")
        print("=" * 60)

    def clean_expired_promotions(self) -> int:
        today = datetime.now(timezone.utc).date().isoformat()
        print(f"\n[→] Expired promotions (promotion_end_date < {today})...")
        count = self._rpc_or_sql(
            rpc_name="cleanup_expired_promotions",
            params={"p_dry_run": self.dry_run},
            sql_count="SELECT COUNT(*) FROM promotions WHERE promotion_end_date IS NOT NULL AND promotion_end_date < CURRENT_DATE",
            sql_delete="DELETE FROM promotions WHERE promotion_end_date IS NOT NULL AND promotion_end_date < CURRENT_DATE",
        )
        self.stats["expired_promotions"] = count
        if count > 0:
            print(f"  {'[DRY-RUN]' if self.dry_run else '✅'} {count} expired promotions {'would be ' if self.dry_run else ''}deleted")
        else:
            print("  No expired promotions found.")
        return count

    def clean_stale_prices(self, days: int = _PRICES_STALE_DAYS_DEFAULT) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        print(f"\n[→] Stale prices (updated_at < {days} days ago — cutoff {cutoff[:10]})...")
        count = self._rpc_or_sql(
            rpc_name="cleanup_stale_prices",
            params={"p_cutoff": cutoff, "p_dry_run": self.dry_run},
            sql_count="SELECT COUNT(*) FROM prices WHERE updated_at < %(p_cutoff)s",
            sql_delete="DELETE FROM prices WHERE updated_at < %(p_cutoff)s",
        )
        self.stats["stale_prices"] = count
        if count > 0:
            print(f"  {'[DRY-RUN]' if self.dry_run else '✅'} {count} stale price rows {'would be ' if self.dry_run else ''}deleted")
        else:
            print("  No stale prices found.")
        return count

    def clean_old_processed_files(self, days: int = _PROCESSED_FILES_DAYS_DEFAULT) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        print(f"\n[→] Old processed files (processed_at < {days} days ago — cutoff {cutoff[:10]})...")
        count = self._rpc_or_sql(
            rpc_name="cleanup_old_processed_files",
            params={"p_cutoff": cutoff, "p_dry_run": self.dry_run},
            sql_count="SELECT COUNT(*) FROM processed_files WHERE processed_at < %(p_cutoff)s",
            sql_delete="DELETE FROM processed_files WHERE processed_at < %(p_cutoff)s",
        )
        self.stats["old_processed_files"] = count
        if count > 0:
            print(f"  {'[DRY-RUN]' if self.dry_run else '✅'} {count} processed_files records {'would be ' if self.dry_run else ''}deleted")
        else:
            print("  No old processed files found.")
        return count

    def print_statistics(self):
        print("\n" + "=" * 60)
        print("CLEANUP STATISTICS")
        print("=" * 60)
        print(f"  Expired promotions deleted  : {self.stats['expired_promotions']}")
        print(f"  Stale prices deleted        : {self.stats['stale_prices']}")
        print(f"  Old processed files deleted : {self.stats['old_processed_files']}")
        total = sum(self.stats.values())
        print(f"  TOTAL DELETED               : {total}")
        print(f"  Products deleted            : 0  (NEVER)")
        print(f"  Stores deleted              : 0  (NEVER)")
        print("=" * 60)
        if self.dry_run:
            print("\n⚠️  DRY-RUN mode — no deletions performed")
            print("   Re-run with --execute to apply")

    def run_cleanup(
        self,
        prices_stale_days: int = _PRICES_STALE_DAYS_DEFAULT,
        processed_files_days: int = _PROCESSED_FILES_DAYS_DEFAULT,
    ) -> dict:
        print("\n" + "=" * 60)
        print("SAFE DATABASE CLEANUP")
        print("=" * 60)
        mode = "DRY-RUN (no deletions)" if self.dry_run else "EXECUTE (real deletions)"
        print(f"\nMode : {mode}")
        print("\nRules:")
        print("  ✅ Products and stores       : NEVER deleted")
        print("  ✅ Promotions                : deleted if promotion_end_date < today")
        print(f"  ✅ Prices                    : deleted if updated_at < {prices_stale_days} days ago")
        print(f"  ✅ Processed files           : deleted if processed_at < {processed_files_days} days ago")

        self.print_db_summary()
        self.clean_expired_promotions()
        self.clean_stale_prices(days=prices_stale_days)
        self.clean_old_processed_files(days=processed_files_days)
        self.print_statistics()
        return self.stats


def main() -> int:
    dry_run = "--execute" not in sys.argv
    prices_days = int(os.getenv("PRICES_STALE_DAYS", str(_PRICES_STALE_DAYS_DEFAULT)))
    processed_days = int(os.getenv("PROCESSED_FILES_DAYS", str(_PROCESSED_FILES_DAYS_DEFAULT)))

    if not dry_run:
        print("\n⚠️  EXECUTE mode — real deletions enabled via --execute flag")

    try:
        cleaner = DatabaseCleaner(dry_run=dry_run)
        cleaner.run_cleanup(
            prices_stale_days=prices_days,
            processed_files_days=processed_days,
        )
        print("\n[✓] Cleanup finished successfully!")
        return 0
    except Exception as e:
        print(f"\n[✗] Cleanup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
