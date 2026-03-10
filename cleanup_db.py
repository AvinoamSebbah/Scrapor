"""Safe database cleanup — uses supabase-py REST API (no direct PostgreSQL connection).

RULES (never violated):
1. Products and stores are NEVER deleted.
2. Promotions: deleted ONLY when promotion_end_date is strictly in the past (NULL rows skipped).
3. Prices    : deleted ONLY when updated_at < cutoff (default 60 days). No cascade risk —
               the prices table has no FK references from other tables.
4. Processed files: deleted when processed_at < cutoff (default 7 days). Same — no FK refs.
5. All deletes run server-side via RPC (atomic, no HTTP-timeout risk on large tables).
6. Dry-run by default. Pass --execute to apply real deletions.

Environment variables:
  SUPABASE_URL               — Supabase project URL
  SUPABASE_SERVICE_ROLE_KEY  — Service role key (not the publishable anon key)
  PRICES_STALE_DAYS          — Override stale-price threshold in days (default 60)
  PROCESSED_FILES_DAYS       — Override processed-file retention in days (default 7)
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from supabase import create_client, Client

_HTTP_TIMEOUT = 120
_PRICES_STALE_DAYS_DEFAULT = 60
_PROCESSED_FILES_DAYS_DEFAULT = 7


class DatabaseCleaner:
    def __init__(self, dry_run: bool = True):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set"
            )
        self.client: Client = create_client(url, key)
        try:
            self.client.postgrest.session.timeout = _HTTP_TIMEOUT
        except Exception:
            pass
        self.dry_run = dry_run
        self.stats = {
            "expired_promotions": 0,
            "stale_prices": 0,
            "old_processed_files": 0,
        }

    def _count_rows(self, table: str, col: str, lt_val: str) -> int:
        """Return the number of rows where col < lt_val. Returns -1 on error."""
        try:
            result = (
                self.client.table(table)
                .select("*", count="exact")
                .lt(col, lt_val)
                .limit(0)
                .execute()
            )
            return result.count or 0
        except Exception as e:
            print(f"  [!] Count error on {table}: {e}")
            return -1

    def print_db_summary(self):
        print("\n" + "=" * 60)
        print("DATABASE SUMMARY")
        print("=" * 60)
        tables = [
            ("stores",          "Stores"),
            ("products",        "Products"),
            ("prices",          "Prices"),
            ("promotions",      "Promotions"),
            ("processed_files", "Processed files"),
        ]
        for table, label in tables:
            try:
                result = (
                    self.client.table(table)
                    .select("*", count="exact")
                    .limit(0)
                    .execute()
                )
                count = result.count or 0
                print(f"  {label:<25}: {count:>12,}")
            except Exception as e:
                print(f"  {label:<25}: error ({e})")
        print("=" * 60)

    def clean_expired_promotions(self) -> int:
        """Delete promotions where promotion_end_date is in the past.

        NULL promotion_end_date rows are naturally excluded by the < filter
        (NULL is never less than a date value in SQL/PostgREST).
        The server-side RPC also guards with IS NOT NULL for extra safety.
        """
        today = datetime.now(timezone.utc).date().isoformat()
        print(f"\n[→] Expired promotions (promotion_end_date < {today})...")

        count = self._count_rows("promotions", "promotion_end_date", today)
        if count < 0:
            return 0

        print(f"  Found    : {count}")
        self.stats["expired_promotions"] = count

        if count > 0:
            if not self.dry_run:
                try:
                    result = self.client.rpc("cleanup_expired_promotions").execute()
                    deleted = result.data if isinstance(result.data, int) else count
                    print(f"  ✅ {deleted} expired promotions deleted")
                except Exception as e:
                    print(f"  [✗] Delete failed: {e}")
                    raise
            else:
                print(f"  [DRY-RUN] {count} promotions would be deleted")
        else:
            print("  No expired promotions found.")
        return count

    def clean_stale_prices(self, days: int = _PRICES_STALE_DAYS_DEFAULT) -> int:
        """Delete price rows not updated in the last `days` days.

        Prices are keyed on (chain_id, item_code) — one row per product per chain.
        A row not touched by merge_prices in `days` days means no scraper has seen
        that product recently, suggesting it is discontinued.
        No FK references point TO the prices table, so no cascade risk.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        print(f"\n[→] Stale prices (updated_at < {days} days ago — cutoff {cutoff[:10]})...")

        count = self._count_rows("prices", "updated_at", cutoff)
        if count < 0:
            return 0

        print(f"  Found    : {count}")
        self.stats["stale_prices"] = count

        if count > 0:
            if not self.dry_run:
                try:
                    result = self.client.rpc(
                        "cleanup_stale_prices", {"p_cutoff": cutoff}
                    ).execute()
                    deleted = result.data if isinstance(result.data, int) else count
                    print(f"  ✅ {deleted} stale price rows deleted")
                except Exception as e:
                    print(f"  [✗] Delete failed: {e}")
                    raise
            else:
                print(f"  [DRY-RUN] {count} price rows would be deleted")
        else:
            print("  No stale prices found.")
        return count

    def clean_old_processed_files(self, days: int = _PROCESSED_FILES_DAYS_DEFAULT) -> int:
        """Delete processed_files records older than `days` days.

        These are bookkeeping rows used to skip already-processed files.
        After `days` days, re-processing a file is acceptable, so the record can go.
        No FK references point TO processed_files, so no cascade risk.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        print(f"\n[→] Old processed files (processed_at < {days} days ago — cutoff {cutoff[:10]})...")

        count = self._count_rows("processed_files", "processed_at", cutoff)
        if count < 0:
            return 0

        print(f"  Found    : {count}")
        self.stats["old_processed_files"] = count

        if count > 0:
            if not self.dry_run:
                try:
                    result = self.client.rpc(
                        "cleanup_old_processed_files", {"p_cutoff": cutoff}
                    ).execute()
                    deleted = result.data if isinstance(result.data, int) else count
                    print(f"  ✅ {deleted} processed_files records deleted")
                except Exception as e:
                    print(f"  [✗] Delete failed: {e}")
                    raise
            else:
                print(f"  [DRY-RUN] {count} processed_files records would be deleted")
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
