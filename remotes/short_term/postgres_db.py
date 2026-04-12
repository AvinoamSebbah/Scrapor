"""PostgreSQL implementation of the short-term database uploader."""

import json
import math
import os
import time
from decimal import Decimal, InvalidOperation
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, RealDictCursor, execute_values

from utils import Logger
from .api_base import ShortTermDatabaseUploader

_CLEANUP_STORES_SQL = os.path.join(os.path.dirname(__file__), "..", "..", "scripts", "cleanup_stores.sql")


def _resolve_database_url(explicit_url=None):
    if explicit_url:
        return explicit_url
    return (
        os.getenv("POSTGRESQL_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
    )


def _run_stores_cleanup(db_url=None):
    """Run cleanup_stores.sql if a PostgreSQL URL is configured."""
    db_url = _resolve_database_url(db_url)
    if not db_url:
        return
    sql_path = os.path.normpath(_CLEANUP_STORES_SQL)
    if not os.path.isfile(sql_path):
        Logger.warning("cleanup_stores.sql not found at %s - skipping", sql_path)
        return
    conn = None
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            cleanup_sql = f.read()
        conn = psycopg2.connect(
            db_url,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(cleanup_sql)
        Logger.info("cleanup_stores.sql executed successfully")
    except Exception as e:
        Logger.warning("cleanup_stores.sql failed: %s", e)
    finally:
        if conn is not None:
            conn.close()


# Batch sizes tuned per table - promotions have many fields so smaller chunks
_BATCH_SIZE_DEFAULT = 500
_BATCH_SIZE_PROMOS = int(os.getenv("BATCH_SIZE_PROMOS", "200"))
_BATCH_SIZE_PRICES = 1000
# Products are a hot table under concurrent load - smaller batches reduce lock time
_BATCH_SIZE_PRODUCTS = 50
_ANALYZE_PRODUCTS_LOCK_KEY = 8245112301
_ANALYZE_PRODUCTS_TASK_NAME = "analyze_products"
_PRODUCT_SEARCH_STATS_CHUNK = 20000
_PRODUCT_SEARCH_STATS_FULL_REBUILD_THRESHOLD = int(
    os.getenv("PRODUCT_SEARCH_STATS_FULL_REBUILD_THRESHOLD", "50000")
)
_TOP_PROMOS_CACHE_DEFAULT_WINDOW_HOURS = int(os.getenv("TOP_PROMOS_CACHE_WINDOW_HOURS", "24"))
_TOP_PROMOS_CACHE_DEFAULT_TOP_N = int(os.getenv("TOP_PROMOS_CACHE_TOP_N", "200"))


class PostgresUploader(ShortTermDatabaseUploader):
    """Direct PostgreSQL implementation for storing supermarket data."""

    @staticmethod
    def _get_val(d, key, default=None):
        """Get value from dict with case-insensitive key check and NaN handling."""
        val = None
        if key in d:
            val = d[key]
        else:
            key_lower = key.lower()
            for k, v in d.items():
                if k.lower() == key_lower:
                    val = v
                    break

        if val is not None:
            try:
                if isinstance(val, (float, int)) and math.isnan(val):
                    return default
                if str(val).lower() == "nan":
                    return default
            except Exception:
                pass
            return val

        return default

    @staticmethod
    def _clean_id(val):
        """Clean IDs by removing .0 from float-like strings."""
        if val is None or val == "":
            return ""
        s_val = str(val).strip()
        if s_val.endswith(".0"):
            return s_val[:-2]
        return s_val

    @staticmethod
    def _to_db_value(value):
        if isinstance(value, (dict, list)):
            return Json(value)
        return value

    @staticmethod
    def _to_numeric(value):
        if value is None:
            return None
        s_val = str(value).strip()
        if not s_val or s_val.lower() in {"nan", "none", "null"}:
            return None
        s_val = s_val.replace(",", ".")
        try:
            return Decimal(s_val)
        except (InvalidOperation, ValueError):
            return None

    @staticmethod
    def _to_bool(value, default=False):
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0

        s_val = str(value).strip().lower()
        if not s_val or s_val in {"nan", "none", "null"}:
            return default
        if s_val in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if s_val in {"0", "false", "f", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _normalize_scraped_promo_price(price, promo_price):
        """Normalize suspicious tiny promo prices emitted by some feeds.

        Several feeds encode promo values in scaled units (for example 0.29 instead of 29.0).
        When raw promo is implausibly tiny compared to base price, prefer a x100 normalization.
        """
        if promo_price is None:
            return None
        if promo_price <= 0:
            return None

        if price is None or price <= 0:
            return promo_price

        if promo_price >= price:
            return None

        # Guard against catastrophic under-scaling like 0.09 for a 79.9 item.
        if promo_price < Decimal("1") and (promo_price / price) <= Decimal("0.05"):
            scaled = promo_price * Decimal("100")
            if scaled < price:
                return scaled
            return None

        return promo_price

    @staticmethod
    def _normalize_date(value, default=None):
        """Normalize date-like values to YYYY-MM-DD to keep SQL casts datestyle-agnostic."""
        if value is None:
            return default

        s_val = str(value).strip()
        if not s_val or s_val.lower() in {"nan", "none", "null"}:
            return default

        s_val = s_val.replace("T", " ")
        formats = (
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%d-%m-%Y %H:%M",
            "%d-%m-%Y %H:%M:%S",
        )

        for fmt in formats:
            try:
                return datetime.strptime(s_val, fmt).date().isoformat()
            except ValueError:
                continue

        try:
            return datetime.fromisoformat(s_val).date().isoformat()
        except ValueError:
            return default

    def __init__(self, url=None, key=None, database_url=None):
        """Initialise PostgreSQL client.

        ``url`` and ``key`` are ignored and kept only for backward
        compatibility with previous call sites.
        """
        del key
        db_url = database_url
        if not db_url and isinstance(url, str) and url.startswith("postgresql://"):
            db_url = url
        self.db_url = _resolve_database_url(db_url)
        if not self.db_url:
            raise ValueError(
                "POSTGRESQL_URL (or DATABASE_URL) must be set"
            )

        self.conn = None
        # Connection is established lazily on first write - not at construction time.

        self.seen_stores: set = set()
        self.seen_products: set = set()
        self._store_db_id_cache: dict = {}
        self._products_modified = False
        self._product_prices_modified = False
        self._pending_promo_refresh_chains: set[str] = set()
        self._pending_product_stats_product_ids: set[int] = set()
        self._product_search_stats_full_rebuild = False
        self._stores_cleanup_pending = False
        Logger.info("PostgreSQL uploader initialised")

    # ------------------------------------------------------------------
    # low-level SQL helpers
    # ------------------------------------------------------------------

    def _ensure_connection(self):
        if self.conn is not None and self.conn.closed == 0:
            return
        self.conn = psycopg2.connect(
            self.db_url,
            connect_timeout=30,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
            application_name="scrapor-uploader",
            cursor_factory=RealDictCursor,
        )
        self.conn.autocommit = True
        with self.conn.cursor() as cur:
            cur.execute("SET statement_timeout = '300s'")

    def _close_connection(self):
        if self.conn is not None:
            try:
                self.conn.close()
            finally:
                self.conn = None

    def _run_query(self, query, params=None, fetch=False):
        self._ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
        return []

    def _ensure_maintenance_state_table(self):
        self._run_query(
            """
            CREATE TABLE IF NOT EXISTS maintenance_state (
                task_name TEXT PRIMARY KEY,
                last_run_at TIMESTAMPTZ NOT NULL
            )
            """
        )

    def maybe_analyze_products(self):
        # Price imports also require refreshing product_search_stats even when products table itself did not change.
        if self._product_prices_modified:
            try:
                self._flush_pending_product_search_stats()
            except Exception as e:
                Logger.warning("Deferred product_search_stats refresh failed: %s", e)

        if not self._products_modified:
            Logger.info("Skipping ANALYZE products: no product rows changed in this import")
            return

        interval_minutes = int(os.getenv("ANALYZE_PRODUCTS_MIN_INTERVAL_MINUTES", "15"))
        self._ensure_connection()

        lock_acquired = False
        try:
            rows = self._run_query(
                "SELECT pg_try_advisory_lock(%s) AS locked",
                (_ANALYZE_PRODUCTS_LOCK_KEY,),
                fetch=True,
            )
            lock_acquired = bool(rows and rows[0].get("locked"))
            if not lock_acquired:
                Logger.info("Skipping ANALYZE products: another uploader is handling maintenance")
                return

            self._ensure_maintenance_state_table()
            rows = self._run_query(
                "SELECT last_run_at FROM maintenance_state WHERE task_name = %s",
                (_ANALYZE_PRODUCTS_TASK_NAME,),
                fetch=True,
            )

            now_utc = datetime.now(timezone.utc)
            if rows:
                last_run_at = rows[0].get("last_run_at")
                if last_run_at is not None:
                    if last_run_at.tzinfo is None:
                        last_run_at = last_run_at.replace(tzinfo=timezone.utc)
                    elapsed = now_utc - last_run_at
                    if elapsed < timedelta(minutes=interval_minutes):
                        Logger.info(
                            "Skipping ANALYZE products: last global run was %.1f minutes ago",
                            elapsed.total_seconds() / 60,
                        )
                        return

            Logger.info("Running post-import maintenance: ANALYZE products")
            self._run_query("ANALYZE products")
            if os.getenv("ANALYZE_PROMOTION_STORE_ITEMS", "0").strip().lower() in {"1", "true", "yes", "on"}:
                Logger.info("Running post-import maintenance: ANALYZE promotion_store_items")
                self._run_query("ANALYZE promotion_store_items")
            self._run_query(
                """
                INSERT INTO maintenance_state (task_name, last_run_at)
                VALUES (%s, NOW())
                ON CONFLICT (task_name)
                DO UPDATE SET last_run_at = EXCLUDED.last_run_at
                """,
                (_ANALYZE_PRODUCTS_TASK_NAME,),
            )
        except Exception as e:
            Logger.warning("ANALYZE products maintenance failed: %s", e)
        finally:
            if lock_acquired:
                try:
                    self._run_query("SELECT pg_advisory_unlock(%s)", (_ANALYZE_PRODUCTS_LOCK_KEY,))
                except Exception as e:
                    Logger.warning("Failed to release ANALYZE products lock: %s", e)

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _upsert_batch(
        self,
        table: str,
        records: list,
        on_conflict: str,
        ignore_duplicates: bool = False,
        batch_size: int = _BATCH_SIZE_DEFAULT,
    ) -> None:
        """Upsert ``records`` into ``table`` in safe-sized batches."""
        if not records:
            return

        columns = list(records[0].keys())
        conflict_columns = [c.strip() for c in on_conflict.split(",") if c.strip()]
        update_columns = [c for c in columns if c not in conflict_columns and c != "created_at"]

        insert_sql = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s ").format(
            table=sql.Identifier(table),
            cols=sql.SQL(",").join(sql.Identifier(col) for col in columns),
        )

        if ignore_duplicates or not update_columns:
            conflict_sql = sql.SQL("ON CONFLICT ({conflict}) DO NOTHING").format(
                conflict=sql.SQL(",").join(sql.Identifier(col) for col in conflict_columns)
            )
        else:
            update_set = sql.SQL(",").join(
                sql.SQL("{col}=EXCLUDED.{col}").format(col=sql.Identifier(col))
                for col in update_columns
            )
            conflict_sql = sql.SQL("ON CONFLICT ({conflict}) DO UPDATE SET {updates}").format(
                conflict=sql.SQL(",").join(sql.Identifier(col) for col in conflict_columns),
                updates=update_set,
            )

        final_sql = insert_sql + conflict_sql

        waits = [5, 10, 20, 40]
        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            rows = [tuple(self._to_db_value(rec.get(col)) for col in columns) for rec in chunk]

            for attempt in range(5):
                try:
                    self._ensure_connection()
                    with self.conn.cursor() as cur:
                        execute_values(cur, final_sql.as_string(self.conn), rows)
                    break
                except Exception as e:
                    if attempt == 4:
                        self._close_connection()
                        raise
                    wait = waits[attempt]
                    Logger.warning(
                        "Upsert to %s failed (attempt %d/5), retrying in %ds: %s",
                        table,
                        attempt + 1,
                        wait,
                        e,
                    )
                    time.sleep(wait)

    def _rpc_batch(self, func_name: str, records: list) -> None:
        """Call SQL merge function in batches for atomic server-side JSONB merge."""
        if not records:
            return
        batch_size = _BATCH_SIZE_PROMOS if func_name == "merge_promotions" else _BATCH_SIZE_PRICES
        waits = [5, 10, 20, 40]

        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            payload = json.dumps(chunk, ensure_ascii=False)
            for attempt in range(5):
                try:
                    self._run_query(
                        sql.SQL("SELECT {}(%s::jsonb)").format(sql.Identifier(func_name)),
                        (payload,),
                        fetch=False,
                    )
                    break
                except Exception as e:
                    if attempt == 4:
                        self._close_connection()
                        raise
                    wait = waits[attempt]
                    Logger.warning(
                        "Function %s failed (attempt %d/5), retrying in %ds: %s",
                        func_name,
                        attempt + 1,
                        wait,
                        e,
                    )
                    time.sleep(wait)

    def _fetch_all_pages(self, table: str, columns: str) -> list:
        """Fetch every row from ``table`` using LIMIT/OFFSET pagination."""
        all_rows: list = []
        page = 0
        col_list = [col.strip() for col in columns.split(",") if col.strip()]
        while True:
            query = sql.SQL("SELECT {cols} FROM {table} ORDER BY 1 LIMIT %s OFFSET %s").format(
                cols=sql.SQL(",").join(sql.Identifier(col) for col in col_list),
                table=sql.Identifier(table),
            )
            rows = self._run_query(query, (_BATCH_SIZE_DEFAULT, page * _BATCH_SIZE_DEFAULT), fetch=True)
            all_rows.extend(rows)
            if len(rows) < _BATCH_SIZE_DEFAULT:
                break
            page += 1
        return [dict(row) for row in all_rows]

    def _test_connection(self) -> None:
        try:
            self._run_query("SELECT 1", fetch=True)
            Logger.info("PostgreSQL connection test successful")
        except Exception as e:
            Logger.error("PostgreSQL connection test failed: %s", str(e))
            raise

    def _fetch_existing_jsonb(
        self,
        table: str,
        pk_col: str,
        id_col: str,
        jsonb_col: str,
        arr_col: str,
        by_pk: dict,
    ) -> dict:
        """Fetch existing JSONB/array columns for grouped rows."""
        existing: dict = {}
        for pk_value, id_values in by_pk.items():
            for i in range(0, len(id_values), _BATCH_SIZE_DEFAULT):
                chunk = id_values[i : i + _BATCH_SIZE_DEFAULT]
                try:
                    query = sql.SQL(
                        "SELECT {id_col},{jsonb_col},{arr_col} FROM {table} "
                        "WHERE {pk_col}=%s AND {id_col}=ANY(%s)"
                    ).format(
                        id_col=sql.Identifier(id_col),
                        jsonb_col=sql.Identifier(jsonb_col),
                        arr_col=sql.Identifier(arr_col),
                        table=sql.Identifier(table),
                        pk_col=sql.Identifier(pk_col),
                    )
                    rows = self._run_query(query, (pk_value, chunk), fetch=True)
                    for row in rows:
                        existing[(pk_value, row[id_col])] = dict(row)
                except Exception as e:
                    Logger.warning("Failed to fetch existing %s rows: %s", table, e)
        return existing

    def _refresh_promotion_store_items_for_chains(self, chain_ids: list[str]) -> int:
        refreshed_total = 0
        for chain_id in chain_ids:
            try:
                rows = self._run_query(
                    "SELECT refresh_promotion_store_items(%s) AS affected",
                    (chain_id,),
                    fetch=True,
                )
                refreshed_total += int(rows[0].get("affected", 0)) if rows else 0
            except Exception as e:
                Logger.warning("Failed to refresh promotion_store_items for chain %s: %s", chain_id, e)
        return refreshed_total

    def _refresh_top_promotions_cache(self, window_hours: int, top_n: int) -> int:
        rows = self._run_query(
            "SELECT refresh_top_promotions_cache(%s, %s) AS affected",
            (window_hours, top_n),
            fetch=True,
        )
        return int(rows[0].get("affected", 0)) if rows else 0

    def _refresh_top_promotions_cache_if_enabled(self):
        enabled = os.getenv("TOP_PROMOS_CACHE_REFRESH", "true").strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
        if not enabled:
            return

        try:
            window_hours = int(os.getenv("TOP_PROMOS_CACHE_WINDOW_HOURS", str(_TOP_PROMOS_CACHE_DEFAULT_WINDOW_HOURS)))
        except ValueError:
            window_hours = _TOP_PROMOS_CACHE_DEFAULT_WINDOW_HOURS

        try:
            top_n = int(os.getenv("TOP_PROMOS_CACHE_TOP_N", str(_TOP_PROMOS_CACHE_DEFAULT_TOP_N)))
        except ValueError:
            top_n = _TOP_PROMOS_CACHE_DEFAULT_TOP_N

        window_hours = max(window_hours, 1)
        top_n = max(top_n, 1)

        try:
            affected = self._refresh_top_promotions_cache(window_hours, top_n)
            Logger.info(
                "Refreshed top_promotions_cache rows: %d (window_hours=%d, top_n=%d)",
                affected,
                window_hours,
                top_n,
            )
        except Exception as e:
            Logger.warning("Failed to refresh top_promotions_cache: %s", e)

    def _flush_pending_promotion_refresh(self):
        if not self._pending_promo_refresh_chains:
            return
        chains = sorted(self._pending_promo_refresh_chains)
        Logger.info(
            "Refreshing promotion_store_items for %d chain(s) after promo merge batches",
            len(chains),
        )
        refreshed_total = self._refresh_promotion_store_items_for_chains(chains)
        Logger.info("Refreshed promotion_store_items rows: %d", refreshed_total)
        self._refresh_top_promotions_cache_if_enabled()
        self._pending_promo_refresh_chains.clear()

    def _flush_stores_cleanup(self):
        if not self._stores_cleanup_pending:
            return
        Logger.info("Running deferred stores cleanup")
        _run_stores_cleanup(self.db_url)
        self._stores_cleanup_pending = False

    def _refresh_product_search_stats(self, product_ids: list[int] | None = None) -> int:
        if not product_ids:
            rows = self._run_query(
                "SELECT refresh_product_search_stats(NULL) AS affected",
                fetch=True,
            )
            return int(rows[0].get("affected", 0)) if rows else 0

        total = 0
        for i in range(0, len(product_ids), _PRODUCT_SEARCH_STATS_CHUNK):
            chunk = product_ids[i : i + _PRODUCT_SEARCH_STATS_CHUNK]
            rows = self._run_query(
                "SELECT refresh_product_search_stats(%s::int[]) AS affected",
                (chunk,),
                fetch=True,
            )
            total += int(rows[0].get("affected", 0)) if rows else 0
        return total

    def _flush_pending_product_search_stats(self):
        if not self._product_search_stats_full_rebuild and not self._pending_product_stats_product_ids:
            return

        try:
            if self._product_search_stats_full_rebuild:
                Logger.info("Refreshing product_search_stats with FULL rebuild")
                refreshed = self._refresh_product_search_stats(None)
            else:
                ids = sorted(self._pending_product_stats_product_ids)
                Logger.info("Refreshing product_search_stats for %d touched product(s)", len(ids))
                refreshed = self._refresh_product_search_stats(ids)

            Logger.info("Refreshed product_search_stats rows: %d", refreshed)
        finally:
            self._pending_product_stats_product_ids.clear()
            self._product_search_stats_full_rebuild = False

    # ------------------------------------------------------------------
    # store DB-PK resolution
    # ------------------------------------------------------------------

    def _resolve_store_db_ids(self, pairs: set) -> dict:
        """Return {(chain_id, store_id): stores.id} for all given pairs."""
        result = {}
        missing_by_chain: dict = {}
        for chain_id, store_id in pairs:
            cached = self._store_db_id_cache.get((chain_id, store_id))
            if cached is not None:
                result[(chain_id, store_id)] = cached
            else:
                missing_by_chain.setdefault(chain_id, []).append(store_id)

        for chain_id, store_ids in missing_by_chain.items():
            for i in range(0, len(store_ids), 500):
                chunk = store_ids[i : i + 500]
                try:
                    rows = self._run_query(
                        "SELECT id, store_id FROM stores WHERE chain_id=%s AND store_id=ANY(%s)",
                        (chain_id, chunk),
                        fetch=True,
                    )
                    for row in rows:
                        key = (chain_id, str(row["store_id"]))
                        self._store_db_id_cache[key] = row["id"]
                        result[key] = row["id"]
                except Exception as e:
                    Logger.warning("Failed to resolve store DB ids for chain %s: %s", chain_id, e)
                    for sid in chunk:
                        result[(chain_id, sid)] = sid

        return result

    def _resolve_product_db_ids(self, item_codes: set) -> dict:
        """Return {item_code: products.id} for all given item codes."""
        result = {}
        if not item_codes:
            return result

        all_codes = list(item_codes)
        for i in range(0, len(all_codes), 1000):
            chunk = all_codes[i : i + 1000]
            try:
                rows = self._run_query(
                    "SELECT id, item_code FROM products WHERE item_code=ANY(%s)",
                    (chunk,),
                    fetch=True,
                )
                for row in rows:
                    result[str(row["item_code"])] = row["id"]
            except Exception as e:
                Logger.warning("Failed to resolve product DB ids: %s", e)

        return result

    # ------------------------------------------------------------------
    # routing
    # ------------------------------------------------------------------

    def _insert_to_destinations(self, table_target_name, items):
        """Route items to the correct upsert method based on table name."""
        if not items:
            return

        if any("file_complete" in item for item in items):
            self._handle_processed_files(items)
            return

        name_lower = table_target_name.lower()
        if name_lower.startswith("store"):
            self._upsert_stores(items)
        elif name_lower.startswith("price"):
            self._upsert_prices(items)
        elif name_lower.startswith("promo"):
            self._upsert_promos(items)
        elif "scraperstatus" in name_lower or "parserstatus" in name_lower:
            pass
        else:
            Logger.warning("Unknown table type for PostgreSQL mapping: %s", table_target_name)

    # ------------------------------------------------------------------
    # processed_files
    # ------------------------------------------------------------------

    def _handle_processed_files(self, items):
        """Upsert file-completion markers into processed_files."""
        now = datetime.now().isoformat()
        records = []
        for item in items:
            if item.get("file_complete") != "true":
                continue
            chain_id = self._clean_id(item.get("chain_id"))
            store_id = self._clean_id(item.get("store_id"))
            store_name = item.get("store_name")

            if not store_name and chain_id and store_id:
                try:
                    rows = self._run_query(
                        "SELECT store_name FROM stores WHERE chain_id=%s AND store_id=%s LIMIT 1",
                        (chain_id, store_id),
                        fetch=True,
                    )
                    if rows:
                        store_name = rows[0].get("store_name")
                except Exception as e:
                    Logger.warning("Failed to lookup store name for %s-%s: %s", chain_id, store_id, e)

            records.append(
                {
                    "file_name": item.get("file_name"),
                    "file_path": "",
                    "file_hash": "",
                    "file_size": 0,
                    "file_type": "",
                    "record_count": item.get("total_expected_records", 0),
                    "processed_at": now,
                    "chain_id": chain_id,
                    "store_id": store_id,
                    "store_name": store_name,
                    "chain_name": item.get("chain_name"),
                }
            )

        self._upsert_batch("processed_files", records, on_conflict="file_name")

    # ------------------------------------------------------------------
    # stores
    # ------------------------------------------------------------------

    def _upsert_stores(self, items):
        """Map and upsert stores."""
        now = datetime.now().isoformat()
        records_dict: dict = {}
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            key = (chain_id, store_id)

            if not chain_id or not store_id or key in self.seen_stores or key in records_dict:
                continue

            records_dict[key] = {
                "chain_id": chain_id,
                "chain_name": self._get_val(content, "ChainName") or self._get_val(content, "ChainNm"),
                "last_update_date": self._get_val(content, "LastUpdateDate"),
                "last_update_time": self._get_val(content, "LastUpdateTime"),
                "store_id": store_id,
                "bikoret_no": self._clean_id(self._get_val(content, "BikoretNo")),
                "store_type": self._get_val(content, "StoreType"),
                "store_name": self._get_val(content, "StoreName") or self._get_val(content, "StoreNm"),
                "address": self._get_val(content, "Address") or self._get_val(content, "Addr"),
                "city": self._get_val(content, "City"),
                "zip_code": self._get_val(content, "ZipCode"),
                "created_at": now,
                "updated_at": now,
            }

        if not records_dict:
            return

        Logger.info("Upserting %d stores via PostgreSQL", len(records_dict))
        self._upsert_batch("stores", list(records_dict.values()), on_conflict="chain_id,store_id")
        self.seen_stores.update(records_dict.keys())
        cleanup_mode = os.getenv("STORES_CLEANUP_MODE", "deferred").strip().lower()
        if cleanup_mode == "immediate":
            _run_stores_cleanup(self.db_url)
        else:
            self._stores_cleanup_pending = True

    # ------------------------------------------------------------------
    # prices
    # ------------------------------------------------------------------

    def _upsert_prices(self, items):
        """Map and upsert products and per-store prices in normalized tables."""
        self._ensure_stores_exist(items)
        self._upsert_products(items)

        pairs = set()
        item_codes = set()
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            item_code = self._clean_id(self._get_val(content, "ItemCode"))
            if chain_id and store_id:
                pairs.add((chain_id, store_id))
            if item_code:
                item_codes.add(item_code)

        db_id_map = self._resolve_store_db_ids(pairs)
        product_id_map = self._resolve_product_db_ids(item_codes)

        now = datetime.now().isoformat()
        records: dict = {}
        skipped_missing_mapping = 0

        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            item_code = self._clean_id(self._get_val(content, "ItemCode"))

            if not chain_id or not store_id or not item_code:
                continue

            db_store_id = db_id_map.get((chain_id, store_id))
            product_id = product_id_map.get(item_code)
            if db_store_id is None or product_id is None:
                skipped_missing_mapping += 1
                continue

            key = (product_id, db_store_id)
            price_num = self._to_numeric(self._get_val(content, "ItemPrice"))
            raw_promo_num = self._to_numeric(
                self._get_val(
                    content,
                    "PromoPrice",
                    self._get_val(content, "DiscountedPrice", self._get_val(content, "PromoItemPrice")),
                )
            )
            records[key] = {
                "product_id": product_id,
                "store_id": db_store_id,
                "price": price_num,
                "promo_price": self._normalize_scraped_promo_price(price_num, raw_promo_num),
                "unit_of_measure": self._get_val(content, "UnitOfMeasure"),
                "unit_qty": self._get_val(content, "UnitQty"),
                "b_is_weighted": self._to_bool(
                    self._get_val(content, "bIsWeighted", self._get_val(content, "bisweighted", self._get_val(content, "IsWeighted"))),
                    default=False,
                ),
                "updated_at": now,
            }
        if not records:
            if skipped_missing_mapping:
                Logger.warning("Skipped %d price rows due to missing product/store mapping", skipped_missing_mapping)
            return

        Logger.info("Upserting %d product_prices rows via PostgreSQL", len(records))
        self._upsert_batch(
            "product_prices",
            list(records.values()),
            on_conflict="product_id,store_id",
            batch_size=_BATCH_SIZE_PRICES,
        )
        self._product_prices_modified = True

        touched_product_ids = {pid for (pid, _sid), rec in records.items() if rec.get("price") is not None}
        if touched_product_ids:
            stats_mode = os.getenv("PRODUCT_SEARCH_STATS_MODE", "deferred").strip().lower()
            if stats_mode == "off":
                pass
            elif stats_mode == "immediate":
                try:
                    refreshed = self._refresh_product_search_stats(sorted(touched_product_ids))
                    Logger.info("Refreshed product_search_stats rows (immediate): %d", refreshed)
                except Exception as e:
                    Logger.warning("Failed immediate product_search_stats refresh: %s", e)
            else:
                if len(touched_product_ids) >= _PRODUCT_SEARCH_STATS_FULL_REBUILD_THRESHOLD:
                    self._product_search_stats_full_rebuild = True
                    self._pending_product_stats_product_ids.clear()
                elif not self._product_search_stats_full_rebuild:
                    self._pending_product_stats_product_ids.update(touched_product_ids)

        if skipped_missing_mapping:
            Logger.warning("Skipped %d price rows due to missing product/store mapping", skipped_missing_mapping)

    # ------------------------------------------------------------------
    # products
    # ------------------------------------------------------------------

    def _upsert_products(self, items):
        """Upsert product information."""
        now = datetime.now().isoformat()
        records: dict = {}
        for item in items:
            content = item.get("content", {})
            item_code = self._clean_id(self._get_val(content, "ItemCode"))
            if not item_code or item_code in records or item_code in self.seen_products:
                continue

            records[item_code] = {
                "item_code": item_code,
                "item_name": self._get_val(content, "ItemName") or self._get_val(content, "ItemNm"),
                "manufacturer_name": (
                    self._get_val(content, "manufacturename")
                    or self._get_val(content, "ManufacturerName")
                    or self._get_val(content, "ManufacturerNm")
                ),
                "manufacture_country": (
                    self._get_val(content, "ManufactureCountry")
                    or self._get_val(content, "ManufactureCountryNm")
                ),
                "manufacturer_item_description": (
                    self._get_val(content, "manufactureitemdescription")
                    or self._get_val(content, "ManufacturerItemDescription")
                    or self._get_val(content, "ItemNm")
                ),
                "created_at": now,
                "updated_at": now,
            }

        if not records:
            return

        Logger.info("Upserting %d products via PostgreSQL", len(records))
        self._upsert_batch(
            "products",
            list(records.values()),
            on_conflict="item_code",
            batch_size=_BATCH_SIZE_PRODUCTS,
        )
        self.seen_products.update(records.keys())
        self._products_modified = True

    # ------------------------------------------------------------------
    # promotions
    # ------------------------------------------------------------------

    def _upsert_promos(self, items):
        """Map and upsert promotions."""
        self._ensure_stores_exist(items)

        pairs = set()
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            if chain_id and store_id:
                pairs.add((chain_id, store_id))
        db_id_map = self._resolve_store_db_ids(pairs)

        now = datetime.now()
        now_iso = now.isoformat()
        aggregated: dict = {}

        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            promotion_id = self._clean_id(self._get_val(content, "PromotionId"))

            if not chain_id or not store_id or not promotion_id:
                continue

            db_store_key = str(db_id_map.get((chain_id, store_id), store_id))
            key = (chain_id, promotion_id)
            store_promo_val = (
                self._get_val(content, "MinNoOfItemOffered")
                or self._get_val(content, "minnoofitemoffered")
                or self._get_val(content, "MinQty")
                or "active"
            )

            if key not in aggregated:
                promotion_items = self._get_val(content, "groups") or self._get_val(content, "PromotionItems") or []
                if isinstance(promotion_items, str):
                    try:
                        json_str = promotion_items.replace("'", '"').replace("None", "null")
                        promotion_items = json.loads(json_str)
                    except Exception:
                        promotion_items = []

                aggregated[key] = {
                    "chain_id": chain_id,
                    "promotion_id": promotion_id,
                    "sub_chain_id": self._clean_id(self._get_val(content, "SubChainId")),
                    "bikoret_no": self._clean_id(self._get_val(content, "BikoretNo")),
                    "promotion_description": self._get_val(content, "PromotionDescription"),
                    "promotion_update_date": self._normalize_date(
                        self._get_val(content, "promotionupdatetime")
                        or self._get_val(content, "PromotionUpdateDate"),
                        now.strftime("%Y-%m-%d"),
                    ),
                    "promotion_start_date": self._normalize_date(
                        self._get_val(content, "promotionstartdatetime")
                        or self._get_val(content, "PromotionStartDate"),
                        now.strftime("%Y-%m-%d"),
                    ),
                    "promotion_start_hour": self._get_val(content, "PromotionStartHour") or "00:00",
                    "promotion_end_date": self._normalize_date(
                        self._get_val(content, "promotionenddatetime")
                        or self._get_val(content, "PromotionEndDate"),
                        "2099-12-31",
                    ),
                    "promotion_end_hour": self._get_val(content, "PromotionEndHour") or "23:59",
                    "promotion_days": self._get_val(content, "PromotionDays"),
                    "redemption_limit": self._get_val(content, "RedemptionLimit"),
                    "reward_type": self._get_val(content, "RewardType"),
                    "allow_multiple_discounts": self._get_val(content, "AllowMultipleDiscounts"),
                    "is_weighted_promo": bool(self._get_val(content, "isWeightedPromo", False)),
                    "is_gift_item": self._get_val(content, "IsGiftItem"),
                    "min_no_of_item_offered": (
                        self._get_val(content, "MinNoOfItemOffered")
                        or self._get_val(content, "minnoofitemoffered")
                    ),
                    "additional_is_coupon": self._get_val(content, "AdditionalIsCoupon"),
                    "additional_gift_count": self._get_val(content, "AdditionalGiftCount"),
                    "additional_is_total": self._get_val(content, "AdditionalIsTotal"),
                    "additional_is_active": self._get_val(content, "AdditionalIsActive"),
                    "additional_restrictions": self._get_val(content, "AdditionalRestrictions"),
                    "remarks": self._get_val(content, "Remarks"),
                    "min_qty": self._get_val(content, "MinQty"),
                    "discounted_price": self._get_val(content, "DiscountedPrice"),
                    "discounted_price_per_mida": self._get_val(content, "DiscountedPricePerMida"),
                    "weight_unit": self._get_val(content, "WeightUnit"),
                    "club_id": self._get_val(content, "ClubId"),
                    "items": promotion_items,
                    "store_promotions": {db_store_key: store_promo_val},
                    "available_in_store_ids": [db_store_key],
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            else:
                aggregated[key]["store_promotions"][db_store_key] = store_promo_val
                if db_store_key not in aggregated[key]["available_in_store_ids"]:
                    aggregated[key]["available_in_store_ids"].append(db_store_key)

        if not aggregated:
            return

        Logger.info("Merging %d promotions via PostgreSQL function", len(aggregated))
        self._rpc_batch("merge_promotions", list(aggregated.values()))

        touched_chains = {row["chain_id"] for row in aggregated.values()}
        refresh_mode = os.getenv("PROMO_REFRESH_MODE", "deferred").strip().lower()
        if refresh_mode == "immediate":
            refreshed_total = self._refresh_promotion_store_items_for_chains(sorted(touched_chains))
            Logger.info("Refreshed promotion_store_items rows: %d", refreshed_total)
            self._refresh_top_promotions_cache_if_enabled()
        else:
            self._pending_promo_refresh_chains.update(touched_chains)
            Logger.info(
                "Deferred promotion_store_items refresh for %d chain(s); pending=%d",
                len(touched_chains),
                len(self._pending_promo_refresh_chains),
            )

    # ------------------------------------------------------------------
    # ensure stores exist (FK guard)
    # ------------------------------------------------------------------

    def _ensure_stores_exist(self, items):
        """Insert minimal store rows so FK constraints do not fail on price/promo upserts."""
        now = datetime.now().isoformat()
        records: list = []
        seen: set = set()
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            if not chain_id or not store_id:
                continue
            key = (chain_id, store_id)
            if key not in seen and key not in self.seen_stores:
                records.append({"chain_id": chain_id, "store_id": store_id, "created_at": now, "updated_at": now})
                seen.add(key)

        if records:
            Logger.info("Ensuring %d referenced stores exist via PostgreSQL", len(records))
            self._upsert_batch("stores", records, on_conflict="chain_id,store_id", ignore_duplicates=True)
            self.seen_stores.update(seen)
            self._resolve_store_db_ids(seen)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def restart_database(self, enabled_scrapers: list[str], enabled_file_types: list[str]):
        """PostgreSQL persistence mode: skip wipe, verify connectivity only."""
        del enabled_scrapers, enabled_file_types
        Logger.info("PostgreSQL persistence mode: skipping database wipe on restart.")
        self._test_connection()

    def close(self):
        try:
            self._flush_pending_promotion_refresh()
        except Exception as e:
            Logger.warning("Deferred promotion_store_items refresh failed during close: %s", e)
        try:
            self._flush_pending_product_search_stats()
        except Exception as e:
            Logger.warning("Deferred product_search_stats refresh failed during close: %s", e)
        try:
            self._flush_stores_cleanup()
        except Exception as e:
            Logger.warning("Deferred stores cleanup failed during close: %s", e)
        self._close_connection()

    def __del__(self):
        self._close_connection()

    # ------------------------------------------------------------------
    # cache sync
    # ------------------------------------------------------------------

    def sync_cache(self, local_cache):
        """Sync remote database state to the local cache."""
        cache_file = os.getenv("PROCESSED_FILES_CACHE")
        if cache_file and os.path.exists(cache_file):
            Logger.info("Loading processed_files from pre-fetched cache: %s", cache_file)
            try:
                with open(cache_file, encoding="utf-8") as f:
                    rows = json.load(f)
                for row in rows:
                    if row.get("record_count", 0) > 0:
                        local_cache.update_last_processed_row(row["file_name"], row["record_count"] - 1)
                self.seen_stores = set()
                self.seen_products = set()
                Logger.info("Local cache loaded from file with %d files.", len(rows))
                return
            except Exception as e:
                Logger.warning("Failed to load cache file, falling back to PostgreSQL: %s", e)

        Logger.info("Syncing local cache from PostgreSQL table processed_files...")
        try:
            rows = self._fetch_all_pages("processed_files", "file_name,record_count")
            for row in rows:
                if row.get("record_count", 0) > 0:
                    local_cache.update_last_processed_row(row["file_name"], row["record_count"] - 1)
            self.seen_stores = set()
            self.seen_products = set()
            Logger.info("Local cache synced successfully with %d files.", len(rows))
        except Exception as e:
            Logger.warning("Failed to sync cache from PostgreSQL: %s", str(e))

    def get_processed_files_names(self):
        """Get the set of processed filenames - from cache file if available."""
        cache_file = os.getenv("PROCESSED_FILES_CACHE")
        if cache_file and os.path.exists(cache_file):
            try:
                with open(cache_file, encoding="utf-8") as f:
                    rows = json.load(f)
                return {row["file_name"] for row in rows}
            except Exception as e:
                Logger.warning("Failed to read processed_files cache file: %s", e)
        try:
            rows = self._fetch_all_pages("processed_files", "file_name")
            return {row["file_name"] for row in rows}
        except Exception as e:
            Logger.warning("Failed to fetch processed files from PostgreSQL: %s", str(e))
            return set()

    def get_processed_files_metadata(self):
        """Get metadata (file_name, chain_name) - from cache file if available."""
        cache_file = os.getenv("PROCESSED_FILES_CACHE")
        if cache_file and os.path.exists(cache_file):
            try:
                with open(cache_file, encoding="utf-8") as f:
                    rows = json.load(f)
                return [{"file_name": row["file_name"], "chain_name": row.get("chain_name")} for row in rows]
            except Exception as e:
                Logger.warning("Failed to read processed_files cache file: %s", e)
        try:
            rows = self._fetch_all_pages("processed_files", "file_name,chain_name")
            return [{"file_name": row["file_name"], "chain_name": row.get("chain_name")} for row in rows]
        except Exception as e:
            Logger.warning("Failed to fetch processed files metadata from PostgreSQL: %s", str(e))
            return []

    # ------------------------------------------------------------------
    # admin
    # ------------------------------------------------------------------

    def _clean_all_destinations(self):
        """Delete every row from all managed tables."""
        for table in [
            "processed_files",
            "promotion_store_items",
            "promotions",
            "product_prices",
            "products",
            "stores",
        ]:
            Logger.info("Clearing table %s...", table)
            self._run_query(sql.SQL("DELETE FROM {}") .format(sql.Identifier(table)))
        Logger.info("PostgreSQL tables cleared.")

    def _is_collection_updated(self, collection_name: str, seconds: int = 10800) -> bool:
        """Check if any data was updated recently."""
        del collection_name
        try:
            rows = self._run_query(
                "SELECT processed_at FROM processed_files ORDER BY processed_at DESC LIMIT 1",
                fetch=True,
            )
            if not rows:
                return False
            last_update = rows[0].get("processed_at")
            if not last_update:
                return False
            if isinstance(last_update, str):
                last_update = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
            if last_update.tzinfo is not None:
                last_update = last_update.replace(tzinfo=None)
            return (datetime.now() - last_update).total_seconds() < seconds
        except Exception:
            return False

    def _list_destinations(self):
        return [
            "stores",
            "products",
            "product_prices",
            "promotions",
            "processed_files",
        ]

    def get_destinations_content(self, table_name, filter=None):
        del filter
        try:
            rows = self._run_query(
                sql.SQL("SELECT * FROM {} LIMIT 200").format(sql.Identifier(table_name)),
                fetch=True,
            )
            return [dict(row) for row in rows]
        except Exception as e:
            Logger.warning("Failed to fetch destination content for %s: %s", table_name, e)
            return []
