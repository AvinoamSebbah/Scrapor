"""Supabase REST API implementation of the database uploader.

Uses the official supabase-py client to communicate via PostgREST — no direct
PostgreSQL connection required.  Each upload job for a different store runs
fully in parallel without connection-level contention.
"""

import os
import json
import math
import time
from datetime import datetime
import httpx
from supabase import create_client, Client
from utils import Logger
from .api_base import ShortTermDatabaseUploader

# Batch sizes tuned per table — promotions have ~35 fields so smaller chunks
_BATCH_SIZE_DEFAULT  = 500
_BATCH_SIZE_PROMOS   = 50
_BATCH_SIZE_PRICES   = 50
# Products are a hot table under concurrent load — smaller batches reduce lock hold time
_BATCH_SIZE_PRODUCTS = 50

# Timeout in seconds for each individual PostgREST request
_HTTP_TIMEOUT = 120


class SupabaseUploader(ShortTermDatabaseUploader):
    """Supabase REST API implementation for storing and managing supermarket data."""

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

        # Handle NaN (often comes from pandas reading empty CSV fields)
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

    def __init__(self, url=None, key=None):
        """Initialise the Supabase REST client.

        Reads SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) from
        the environment when not supplied explicitly.
        """
        url = url or os.getenv("SUPABASE_URL")
        key = key or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) must be set"
            )
        self.client: Client = create_client(url, key)
        # Patch the timeout on the underlying httpx session used by postgrest
        try:
            self.client.postgrest.session.timeout = _HTTP_TIMEOUT
        except Exception:
            pass
        self.seen_stores: set = set()
        self.seen_products: set = set()
        # Maps (chain_id, store_id) -> stores.id (DB primary key)
        # Used so store_prices JSONB keys are globally unique across chains.
        self._store_db_id_cache: dict = {}
        Logger.info("Supabase REST client initialised (url=%s, timeout=%ds)", url, _HTTP_TIMEOUT)

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
        """Upsert *records* into *table* in safe-sized batches via PostgREST.

        Used for stores, products, processed_files (simple upserts with no JSONB merge).
        For prices and promotions, use _rpc_batch() instead.
        """
        if not records:
            return
        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            # 5 attempts with increasing waits — statement timeout (57014) is caused by
            # concurrent lock contention; we need longer pauses to let competing
            # transactions finish before retrying.
            _waits = [5, 10, 20, 40]
            for attempt in range(5):
                try:
                    self.client.table(table).upsert(
                        chunk,
                        on_conflict=on_conflict,
                        ignore_duplicates=ignore_duplicates,
                    ).execute()
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = _waits[attempt]
                    Logger.warning(
                        "Upsert to %s failed (attempt %d/5), retrying in %ds: %s",
                        table, attempt + 1, wait, e
                    )
                    time.sleep(wait)

    def _rpc_batch(self, func_name: str, records: list) -> None:
        """Call a Supabase RPC function in batches for atomic server-side JSONB merge.

        merge_prices and merge_promotions run INSERT … ON CONFLICT DO UPDATE with
        store_prices || EXCLUDED.store_prices directly in PostgreSQL — no race condition.
        """
        if not records:
            return
        batch_size = _BATCH_SIZE_PROMOS if func_name == "merge_promotions" else _BATCH_SIZE_PRICES
        _waits = [5, 10, 20, 40]
        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            for attempt in range(5):
                try:
                    self.client.rpc(func_name, {"p_records": chunk}).execute()
                    break
                except Exception as e:
                    if attempt == 4:
                        raise
                    wait = _waits[attempt]
                    Logger.warning(
                        "RPC %s failed (attempt %d/5), retrying in %ds: %s",
                        func_name, attempt + 1, wait, e
                    )
                    time.sleep(wait)

    def _fetch_all_pages(self, table: str, columns: str) -> list:
        """Fetch every row from *table*, paginating through PostgREST results."""
        all_rows: list = []
        page = 0
        while True:
            result = (
                self.client.table(table)
                .select(columns)
                .range(page * _BATCH_SIZE_DEFAULT, (page + 1) * _BATCH_SIZE_DEFAULT - 1)
                .execute()
            )
            all_rows.extend(result.data)
            if len(result.data) < _BATCH_SIZE_DEFAULT:
                break
            page += 1
        return all_rows

    def _test_connection(self) -> None:
        """Verify that the REST client can reach the project.

        Uses LIMIT 1 with no count — avoids a full table scan that would
        trigger a statement timeout on large tables.
        """
        try:
            self.client.table("processed_files").select("file_name").limit(1).execute()
            Logger.info("Supabase REST connection test successful")
        except Exception as e:
            Logger.error("Supabase REST connection test failed: %s", str(e))
            raise

    def _fetch_existing_jsonb(
        self, table: str, pk_col: str, id_col: str, jsonb_col: str, arr_col: str,
        by_pk: dict
    ) -> dict:
        """Fetch existing JSONB and array columns for a set of rows grouped by a primary key.

        Returns a dict keyed by (pk_value, id_value) -> {jsonb_col: {...}, arr_col: [...]}
        """
        existing: dict = {}
        for pk_value, id_values in by_pk.items():
            for i in range(0, len(id_values), _BATCH_SIZE_DEFAULT):
                chunk = id_values[i : i + _BATCH_SIZE_DEFAULT]
                try:
                    result = (
                        self.client.table(table)
                        .select(f"{id_col},{jsonb_col},{arr_col}")
                        .eq(pk_col, pk_value)
                        .in_(id_col, chunk)
                        .execute()
                    )
                    for row in result.data:
                        existing[(pk_value, row[id_col])] = row
                except Exception as e:
                    Logger.warning("Failed to fetch existing %s rows: %s", table, e)
        return existing

    # ------------------------------------------------------------------
    # store DB-PK resolution
    # ------------------------------------------------------------------

    def _resolve_store_db_ids(self, pairs: set) -> dict:
        """Return {(chain_id, store_id): stores.id} for all given pairs.

        Checks the in-memory cache first; fetches missing pairs from Supabase.
        Falls back to using raw store_id as key if the lookup fails.
        """
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
                    rows = (
                        self.client.table("stores")
                        .select("id,store_id")
                        .eq("chain_id", chain_id)
                        .in_("store_id", chunk)
                        .execute()
                    ).data
                    for row in rows:
                        key = (chain_id, str(row["store_id"]))
                        self._store_db_id_cache[key] = row["id"]
                        result[key] = row["id"]
                except Exception as e:
                    Logger.warning("Failed to resolve store DB ids for chain %s: %s", chain_id, e)
                    # Fallback: use raw store_id so data is not lost
                    for sid in chunk:
                        result[(chain_id, sid)] = sid

        return result

    # ------------------------------------------------------------------
    # routing
    # ------------------------------------------------------------------

    def _insert_to_destinations(self, table_target_name, items):
        """Route items to the correct upsert method based on table name."""
        if not items:
            return

        # Check if this is a "file_complete" marker
        if any("file_complete" in item for item in items):
            self._handle_processed_files(items)
            return

        # Check the type of data based on table_target_name
        name_lower = table_target_name.lower()
        if "store" in name_lower:
            self._upsert_stores(items)
        elif "price" in name_lower:
            self._upsert_prices(items)
        elif "promo" in name_lower:
            self._upsert_promos(items)
        elif "scraperstatus" in name_lower or "parserstatus" in name_lower:
            pass
        else:
            Logger.warning("Unknown table type for Supabase mapping: %s", table_target_name)

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

            # If store_name is missing, try to look it up via the REST API
            if not store_name and chain_id and store_id:
                try:
                    result = (
                        self.client.table("stores")
                        .select("store_name")
                        .eq("chain_id", chain_id)
                        .eq("store_id", store_id)
                        .limit(1)
                        .execute()
                    )
                    if result.data:
                        store_name = result.data[0].get("store_name")
                except Exception as e:
                    Logger.warning(
                        "Failed to lookup store name for %s-%s: %s", chain_id, store_id, e
                    )

            records.append({
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
            })

        self._upsert_batch("processed_files", records, on_conflict="file_name")

    # ------------------------------------------------------------------
    # stores
    # ------------------------------------------------------------------

    def _upsert_stores(self, items):
        """Map and upsert stores via REST API."""
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

        Logger.info("Upserting %d stores via Supabase API", len(records_dict))
        self._upsert_batch("stores", list(records_dict.values()), on_conflict="chain_id,store_id")
        self.seen_stores.update(records_dict.keys())

    # ------------------------------------------------------------------
    # prices
    # ------------------------------------------------------------------

    def _upsert_prices(self, items):
        """Map and upsert products and prices via REST API.

        store_prices JSONB keys are stores.id (DB primary key), not raw chain
        store_id — so keys are globally unique across all chains.
        """
        self._ensure_stores_exist(items)
        self._upsert_products(items)

        # Resolve DB primary keys for every (chain_id, store_id) pair touched
        pairs = set()
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            if chain_id and store_id:
                pairs.add((chain_id, store_id))
        db_id_map = self._resolve_store_db_ids(pairs)

        now = datetime.now().isoformat()
        today = datetime.now().strftime("%Y-%m-%d")
        aggregated: dict = {}

        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            item_code = self._clean_id(self._get_val(content, "ItemCode"))

            if not chain_id or not store_id or not item_code:
                continue

            # Use DB PK as the key so it's globally unique across chains
            db_store_key = str(db_id_map.get((chain_id, store_id), store_id))
            key = (chain_id, item_code)
            item_price = self._get_val(content, "ItemPrice")

            if key not in aggregated:
                aggregated[key] = {
                    "chain_id": chain_id,
                    "item_code": item_code,
                    "base_price": item_price,
                    "store_prices": {db_store_key: item_price},
                    "available_in_store_ids": [db_store_key],
                    "item_type": self._get_val(content, "ItemType"),
                    "unit_qty": self._get_val(content, "UnitQty"),
                    "quantity": self._get_val(content, "Quantity"),
                    "unit_of_measure": self._get_val(content, "UnitOfMeasure"),
                    "b_is_weighted": bool(
                        self._get_val(
                            content, "bisweighted",
                            self._get_val(content, "bIsWeighted", False),
                        )
                    ),
                    "qty_in_package": self._get_val(content, "QtyInPackage"),
                    "price_update_date": (
                        self._get_val(content, "priceupdatetime")
                        or self._get_val(content, "PriceUpdateDate")
                        or today
                    ),
                    "allow_discount": self._get_val(content, "AllowDiscount"),
                    "item_status": self._get_val(content, "ItemStatus"),
                    "item_id": self._get_val(content, "ItemId"),
                    "created_at": now,
                    "updated_at": now,
                }
            else:
                aggregated[key]["store_prices"][db_store_key] = item_price
                if db_store_key not in aggregated[key]["available_in_store_ids"]:
                    aggregated[key]["available_in_store_ids"].append(db_store_key)

        if not aggregated:
            return

        Logger.info("Merging %d prices via Supabase RPC (atomic JSONB merge)", len(aggregated))
        self._rpc_batch("merge_prices", list(aggregated.values()))

    # ------------------------------------------------------------------
    # products
    # ------------------------------------------------------------------

    def _upsert_products(self, items):
        """Upsert product information via REST API."""
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

        Logger.info("Upserting %d products via Supabase API", len(records))
        self._upsert_batch(
            "products", list(records.values()), on_conflict="item_code",
            batch_size=_BATCH_SIZE_PRODUCTS,
        )
        self.seen_products.update(records.keys())

    # ------------------------------------------------------------------
    # promotions
    # ------------------------------------------------------------------

    def _upsert_promos(self, items):
        """Map and upsert promotions via REST API.

        store_promotions JSONB keys are stores.id (DB primary key), not raw
        chain store_id — so keys are globally unique across all chains.
        """
        self._ensure_stores_exist(items)

        # Resolve DB primary keys for every (chain_id, store_id) pair touched
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

            # Use DB PK as the key so it's globally unique across chains
            db_store_key = str(db_id_map.get((chain_id, store_id), store_id))
            key = (chain_id, promotion_id)
            store_promo_val = (
                self._get_val(content, "MinNoOfItemOffered")
                or self._get_val(content, "minnoofitemoffered")
                or self._get_val(content, "MinQty")
                or "active"
            )

            if key not in aggregated:
                promotion_items = (
                    self._get_val(content, "groups")
                    or self._get_val(content, "PromotionItems")
                    or []
                )
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
                    "promotion_update_date": (
                        self._get_val(content, "promotionupdatetime")
                        or self._get_val(content, "PromotionUpdateDate")
                        or now.strftime("%Y-%m-%d")
                    ),
                    "promotion_start_date": (
                        self._get_val(content, "promotionstartdatetime")
                        or self._get_val(content, "PromotionStartDate")
                        or now.strftime("%Y-%m-%d")
                    ),
                    "promotion_start_hour": self._get_val(content, "PromotionStartHour") or "00:00",
                    "promotion_end_date": (
                        self._get_val(content, "promotionenddatetime")
                        or self._get_val(content, "PromotionEndDate")
                        or "2099-12-31"
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

        Logger.info("Merging %d promotions via Supabase RPC (atomic JSONB merge)", len(aggregated))
        self._rpc_batch("merge_promotions", list(aggregated.values()))

    # ------------------------------------------------------------------
    # ensure stores exist (FK guard)
    # ------------------------------------------------------------------

    def _ensure_stores_exist(self, items):
        """Insert minimal store rows so FK constraints don't fire on price/promo upserts."""
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
            Logger.info("Ensuring %d referenced stores exist via Supabase API", len(records))
            self._upsert_batch("stores", records, on_conflict="chain_id,store_id", ignore_duplicates=True)
            self.seen_stores.update(seen)
            # Eagerly populate the db-id cache for these stores
            self._resolve_store_db_ids(seen)

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def restart_database(
        self, enabled_scrapers: list[str], enabled_file_types: list[str]
    ):
        """Supabase uses persistent storage — skip the wipe, just verify connectivity."""
        Logger.info("Supabase persistence mode: skipping database wipe on restart.")
        self._test_connection()

    # ------------------------------------------------------------------
    # cache sync
    # ------------------------------------------------------------------

    def sync_cache(self, local_cache):
        """Sync remote database state to the local cache.

        If PROCESSED_FILES_CACHE env var points to a pre-fetched JSON produced
        by fetch_processed_files.py, load from that file instead of hitting
        Supabase again.  Falls back to the REST API if the file is absent.
        """
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
                Logger.warning("Failed to load cache file, falling back to Supabase: %s", e)

        Logger.info("Syncing local cache from Supabase 'processed_files' table...")
        try:
            rows = self._fetch_all_pages("processed_files", "file_name,record_count")
            for row in rows:
                if row.get("record_count", 0) > 0:
                    local_cache.update_last_processed_row(row["file_name"], row["record_count"] - 1)
            self.seen_stores = set()
            self.seen_products = set()
            Logger.info("Local cache synced successfully with %d files.", len(rows))
        except Exception as e:
            Logger.warning("Failed to sync cache from Supabase: %s", str(e))

    def get_processed_files_names(self):
        """Get the set of processed filenames — from cache file if available."""
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
            Logger.warning("Failed to fetch processed files from Supabase: %s", str(e))
            return set()

    def get_processed_files_metadata(self):
        """Get metadata (file_name, chain_name) — from cache file if available."""
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
            Logger.warning("Failed to fetch processed files metadata from Supabase: %s", str(e))
            return []

    # ------------------------------------------------------------------
    # admin
    # ------------------------------------------------------------------

    def _clean_all_destinations(self):
        """Delete every row from all managed tables via REST API."""
        for table, col in [
            ("processed_files", "file_name"),
            ("promotions", "chain_id"),
            ("prices", "chain_id"),
            ("products", "item_code"),
            ("stores", "chain_id"),
        ]:
            Logger.info("Clearing table %s...", table)
            self.client.table(table).delete().not_.is_(col, "null").execute()
        Logger.info("Supabase tables cleared via REST API.")

    def _is_collection_updated(self, collection_name: str, seconds: int = 10800) -> bool:
        """Check if any data was updated recently."""
        try:
            result = (
                self.client.table("processed_files")
                .select("processed_at")
                .order("processed_at", desc=True)
                .limit(1)
                .execute()
            )
            if not result.data:
                return False
            last_update_str = result.data[0]["processed_at"]
            last_update = datetime.fromisoformat(last_update_str.replace("Z", "+00:00"))
            if last_update.tzinfo is not None:
                last_update = last_update.replace(tzinfo=None)
            return (datetime.now() - last_update).total_seconds() < seconds
        except Exception:
            return False

    def _list_destinations(self):
        return ["stores", "products", "prices", "promotions", "processed_files"]

    def get_destinations_content(self, table_name, filter=None):
        # Implementation for retrieval if needed (e.g. for AccessLayer)
        # This would require translating Mongo filters to SQL
        return []
