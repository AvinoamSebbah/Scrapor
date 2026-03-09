"""Supabase (PostgreSQL) implementation of the database uploader.

This module provides functionality for uploading data directly to a Supabase 
PostgreSQL database, mapping the supermarket data to a specific schema.
"""

import os
import json
import logging
import math
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values, Json
from utils import Logger
from .api_base import ShortTermDatabaseUploader

class SupabaseUploader(ShortTermDatabaseUploader):
    """Supabase implementation for storing and managing supermarket data.
    """

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
            except:
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

    def __init__(self, database_url=None):
        """Initialize PostgreSQL connection.

        Args:
            database_url (str, optional): PostgreSQL connection URI.
        """
        self.uri = database_url or os.getenv("SUPABASE_DATABASE_URL")
        self.conn = None
        self.seen_stores = set()
        self.seen_products = set()
        self._connect()

    def _connect(self):
        try:
            self.conn = psycopg2.connect(self.uri)
            self.conn.autocommit = True
            Logger.info("Successfully connected to Supabase PostgreSQL")
        except Exception as e:
            Logger.error("Error connecting to Supabase: %s", str(e))
            raise e

    def _test_connection(self):
        """Test the connection."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            Logger.info("Supabase connection test successful")
        except Exception as e:
            Logger.error("Supabase connection test failed: %s", str(e))
            self._connect()

    def _insert_to_destinations(self, table_target_name, items):
        """Insert items into Supabase tables with mapping."""
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

    def _handle_processed_files(self, items):
        """Update processed_files table with store metadata."""
        with self.conn.cursor() as cur:
            for item in items:
                if item.get("file_complete") == "true":
                    file_name = item.get("file_name")
                    total_expected_records = item.get("total_expected_records", 0)
                    chain_id = self._clean_id(item.get("chain_id"))
                    store_id = self._clean_id(item.get("store_id"))
                    store_name = item.get("store_name")
                    chain_name = item.get("chain_name")

                    # If store_name is missing (common in Price files), try to find it in the DB
                    if not store_name and chain_id and store_id:
                        try:
                            cur.execute("SELECT store_name FROM stores WHERE chain_id = %s AND store_id = %s LIMIT 1", (chain_id, store_id))
                            row = cur.fetchone()
                            if row:
                                store_name = row[0]
                        except Exception as e:
                            Logger.warning(f"Failed to lookup store name for {chain_id}-{store_id}: {e}")
                            self.conn.rollback()

                    cur.execute("""
                        INSERT INTO processed_files (file_name, file_path, file_hash, file_size, file_type, record_count, processed_at, chain_id, store_id, store_name, chain_name)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (file_name) DO UPDATE SET
                            processed_at = EXCLUDED.processed_at,
                            record_count = EXCLUDED.record_count,
                            chain_id = EXCLUDED.chain_id,
                            store_id = EXCLUDED.store_id,
                            store_name = EXCLUDED.store_name,
                            chain_name = EXCLUDED.chain_name
                    """, (
                        file_name,
                        "", 
                        "", 
                        0,  
                        "", 
                        total_expected_records,
                        datetime.now(),
                        chain_id,
                        store_id,
                        store_name,
                        chain_name
                    ))

    def _upsert_stores(self, items):
        """Map and upsert stores."""
        query = """
            INSERT INTO stores (chain_id, chain_name, last_update_date, last_update_time, store_id, bikoret_no, store_type, store_name, address, city, zip_code, created_at, updated_at)
            VALUES %s
            ON CONFLICT (chain_id, store_id) DO UPDATE SET
                chain_name = EXCLUDED.chain_name,
                last_update_date = EXCLUDED.last_update_date,
                last_update_time = EXCLUDED.last_update_time,
                bikoret_no = EXCLUDED.bikoret_no,
                store_type = EXCLUDED.store_type,
                store_name = EXCLUDED.store_name,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                zip_code = EXCLUDED.zip_code,
                updated_at = EXCLUDED.updated_at
        """
        values_dict = {}
        now = datetime.now()
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            key = (chain_id, store_id)
            
            if not chain_id or not store_id:
                continue
            
            if key in self.seen_stores:
                continue

            last_update_date = self._get_val(content, "LastUpdateDate")
            last_update_time = self._get_val(content, "LastUpdateTime")
            
            values_dict[key] = (
                chain_id,
                self._get_val(content, "ChainName") or self._get_val(content, "ChainNm"),
                last_update_date,
                last_update_time,
                store_id,
                self._clean_id(self._get_val(content, "BikoretNo")),
                self._get_val(content, "StoreType"),
                self._get_val(content, "StoreName") or self._get_val(content, "StoreNm"),
                self._get_val(content, "Address") or self._get_val(content, "Addr"),
                self._get_val(content, "City"),
                self._get_val(content, "ZipCode"),
                now,
                now
            )
        
        if not values_dict:
            return

        Logger.info("Upserting %d stores into Supabase", len(values_dict))
        with self.conn.cursor() as cur:
            execute_values(cur, query, list(values_dict.values()))
        
        # Mark as seen
        self.seen_stores.update(values_dict.keys())

    def _upsert_prices(self, items):
        """Map and upsert products and prices using JSONB map for store prices."""
        # Ensure stores and products exist for foreign keys
        self._ensure_stores_exist(items)
        self._upsert_products(items)
        
        query = """
            INSERT INTO prices (chain_id, item_code, base_price, store_prices, available_in_store_ids, item_type, unit_qty, quantity, unit_of_measure, b_is_weighted, qty_in_package, price_update_date, allow_discount, item_status, item_id, created_at, updated_at)
            VALUES %s
            ON CONFLICT (chain_id, item_code) DO UPDATE SET
                store_prices = prices.store_prices || EXCLUDED.store_prices,
                available_in_store_ids = ARRAY(SELECT DISTINCT UNNEST(prices.available_in_store_ids || EXCLUDED.available_in_store_ids)),
                item_type = EXCLUDED.item_type,
                unit_qty = EXCLUDED.unit_qty,
                quantity = EXCLUDED.quantity,
                unit_of_measure = EXCLUDED.unit_of_measure,
                b_is_weighted = EXCLUDED.b_is_weighted,
                qty_in_package = EXCLUDED.qty_in_package,
                price_update_date = EXCLUDED.price_update_date,
                allow_discount = EXCLUDED.allow_discount,
                item_status = EXCLUDED.item_status,
                item_id = EXCLUDED.item_id,
                updated_at = EXCLUDED.updated_at
        """
        
        # We process all items and aggregate by (chain_id, item_code)
        # to minimize JSONB updates per batch and do a single insert
        aggregated_values = {}
        now = datetime.now()
        
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            item_code = self._clean_id(self._get_val(content, "ItemCode"))
            
            if not chain_id or not store_id or not item_code:
                continue
                
            key = (chain_id, item_code)
            item_price = self._get_val(content, "ItemPrice")
            
            if key not in aggregated_values:
                aggregated_values[key] = {
                    "base_price": item_price,
                    "store_prices": {store_id: item_price},
                    "available_in_store_ids": [store_id],
                    "item_type": self._get_val(content, "ItemType"),
                    "unit_qty": self._get_val(content, "UnitQty"),
                    "quantity": self._get_val(content, "Quantity"),
                    "unit_of_measure": self._get_val(content, "UnitOfMeasure"),
                    "b_is_weighted": bool(self._get_val(content, "bisweighted", self._get_val(content, "bIsWeighted", False))),
                    "qty_in_package": self._get_val(content, "QtyInPackage"),
                    "price_update_date": self._get_val(content, "priceupdatetime") or self._get_val(content, "PriceUpdateDate") or now.strftime("%Y-%m-%d"),
                    "allow_discount": self._get_val(content, "AllowDiscount"),
                    "item_status": self._get_val(content, "ItemStatus"),
                    "item_id": self._get_val(content, "ItemId")
                }
            else:
                aggregated_values[key]["store_prices"][store_id] = item_price
                if store_id not in aggregated_values[key]["available_in_store_ids"]:
                    aggregated_values[key]["available_in_store_ids"].append(store_id)

        if not aggregated_values:
            return

        values = []
        for (chain_id, item_code), data in aggregated_values.items():
            values.append((
                chain_id,
                item_code,
                data["base_price"],
                Json(data["store_prices"]),
                data["available_in_store_ids"],
                data["item_type"],
                data["unit_qty"],
                data["quantity"],
                data["unit_of_measure"],
                data["b_is_weighted"],
                data["qty_in_package"],
                data["price_update_date"],
                data["allow_discount"],
                data["item_status"],
                data["item_id"],
                now,
                now
            ))

        Logger.info("Upserting %d prices into Supabase", len(values))
        with self.conn.cursor() as cur:
            execute_values(cur, query, values)

    def _upsert_products(self, items):
        """Upsert product information."""
        query = """
            INSERT INTO products (item_code, item_name, manufacturer_name, manufacture_country, manufacturer_item_description, created_at, updated_at)
            VALUES %s
            ON CONFLICT (item_code) DO UPDATE SET
                item_name = EXCLUDED.item_name,
                manufacturer_name = EXCLUDED.manufacturer_name,
                manufacture_country = EXCLUDED.manufacture_country,
                manufacturer_item_description = EXCLUDED.manufacturer_item_description,
                updated_at = EXCLUDED.updated_at
        """
        values = []
        now = datetime.now()
        seen_items = set()
        for item in items:
            content = item.get("content", {})
            item_code = self._clean_id(self._get_val(content, "ItemCode"))
            if not item_code or item_code in seen_items or item_code in self.seen_products:
                continue
            seen_items.add(item_code)
            
            values.append((
                item_code,
                self._get_val(content, "ItemName") or self._get_val(content, "ItemNm"),
                self._get_val(content, "manufacturename") or self._get_val(content, "ManufacturerName") or self._get_val(content, "ManufacturerNm"),
                self._get_val(content, "ManufactureCountry") or self._get_val(content, "ManufactureCountryNm"),
                self._get_val(content, "manufactureitemdescription") or self._get_val(content, "ManufacturerItemDescription") or self._get_val(content, "ItemNm"),
                now,
                now
            ))
        
        if values:
            Logger.info("Upserting %d products into Supabase", len(values))
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
            
            # Mark as seen
            self.seen_products.update(seen_items)

    def _upsert_promos(self, items):
        """Map and upsert promotions using JSONB store mapping."""
        # Ensure stores exist for foreign key
        self._ensure_stores_exist(items)
        query = """
            INSERT INTO promotions (
                chain_id, promotion_id, sub_chain_id, bikoret_no, promotion_description, 
                promotion_update_date, promotion_start_date, promotion_start_hour, 
                promotion_end_date, promotion_end_hour, promotion_days, redemption_limit,
                reward_type, allow_multiple_discounts, is_weighted_promo, is_gift_item, 
                min_no_of_item_offered, additional_is_coupon, additional_gift_count, 
                additional_is_total, additional_is_active, additional_restrictions,
                remarks, min_qty, discounted_price, discounted_price_per_mida, 
                weight_unit, club_id, items, store_promotions, available_in_store_ids, 
                created_at, updated_at
            )
            VALUES %s
            ON CONFLICT (chain_id, promotion_id) DO UPDATE SET
                store_promotions = promotions.store_promotions || EXCLUDED.store_promotions,
                available_in_store_ids = ARRAY(SELECT DISTINCT UNNEST(promotions.available_in_store_ids || EXCLUDED.available_in_store_ids)),
                sub_chain_id = EXCLUDED.sub_chain_id,
                bikoret_no = EXCLUDED.bikoret_no,
                promotion_description = EXCLUDED.promotion_description,
                promotion_update_date = EXCLUDED.promotion_update_date,
                promotion_start_date = EXCLUDED.promotion_start_date,
                promotion_start_hour = EXCLUDED.promotion_start_hour,
                promotion_end_date = EXCLUDED.promotion_end_date,
                promotion_end_hour = EXCLUDED.promotion_end_hour,
                promotion_days = EXCLUDED.promotion_days,
                redemption_limit = EXCLUDED.redemption_limit,
                reward_type = EXCLUDED.reward_type,
                allow_multiple_discounts = EXCLUDED.allow_multiple_discounts,
                is_weighted_promo = EXCLUDED.is_weighted_promo,
                is_gift_item = EXCLUDED.is_gift_item,
                min_no_of_item_offered = EXCLUDED.min_no_of_item_offered,
                additional_is_coupon = EXCLUDED.additional_is_coupon,
                additional_gift_count = EXCLUDED.additional_gift_count,
                additional_is_total = EXCLUDED.additional_is_total,
                additional_is_active = EXCLUDED.additional_is_active,
                additional_restrictions = EXCLUDED.additional_restrictions,
                remarks = EXCLUDED.remarks,
                min_qty = EXCLUDED.min_qty,
                discounted_price = EXCLUDED.discounted_price,
                discounted_price_per_mida = EXCLUDED.discounted_price_per_mida,
                weight_unit = EXCLUDED.weight_unit,
                club_id = EXCLUDED.club_id,
                items = EXCLUDED.items,
                updated_at = EXCLUDED.updated_at
        """
        aggregated_values = {}
        now = datetime.now()
        
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            promotion_id = self._clean_id(self._get_val(content, "PromotionId"))
            
            if not chain_id or not store_id or not promotion_id:
                continue

            key = (chain_id, promotion_id)
            # Use MinNoOfItemOffered or MinQty as the value for store_promotions to avoid nulls
            store_promo_val = (
                self._get_val(content, "MinNoOfItemOffered") or 
                self._get_val(content, "minnoofitemoffered") or 
                self._get_val(content, "MinQty") or 
                "active"
            )

            if key not in aggregated_values:
                # Extract groups/items and ensure it is a dict/list for Json()
                promotion_items = self._get_val(content, "groups") or self._get_val(content, "PromotionItems") or []
                if isinstance(promotion_items, str):
                    try:
                        # Convert JS-like dict string to valid JSON if needed
                        # (The scraper output sometimes uses single quotes)
                        json_str = promotion_items.replace("'", '"').replace("None", "null")
                        promotion_items = json.loads(json_str)
                    except:
                        promotion_items = []

                aggregated_values[key] = {
                    "sub_chain_id": self._clean_id(self._get_val(content, "SubChainId")),
                    "bikoret_no": self._clean_id(self._get_val(content, "BikoretNo")),
                    "promotion_description": self._get_val(content, "PromotionDescription"),
                    "promotion_update_date": self._get_val(content, "promotionupdatetime") or self._get_val(content, "PromotionUpdateDate") or now.strftime("%Y-%m-%d"),
                    "promotion_start_date": self._get_val(content, "promotionstartdatetime") or self._get_val(content, "PromotionStartDate") or now.strftime("%Y-%m-%d"),
                    "promotion_start_hour": self._get_val(content, "PromotionStartHour") or "00:00",
                    "promotion_end_date": self._get_val(content, "promotionenddatetime") or self._get_val(content, "PromotionEndDate") or "2099-12-31",
                    "promotion_end_hour": self._get_val(content, "PromotionEndHour") or "23:59",
                    "promotion_days": self._get_val(content, "PromotionDays"),
                    "redemption_limit": self._get_val(content, "RedemptionLimit"),
                    "reward_type": self._get_val(content, "RewardType"),
                    "allow_multiple_discounts": self._get_val(content, "AllowMultipleDiscounts"),
                    "is_weighted_promo": bool(self._get_val(content, "isWeightedPromo", False)),
                    "is_gift_item": self._get_val(content, "IsGiftItem"),
                    "min_no_of_item_offered": self._get_val(content, "MinNoOfItemOffered") or self._get_val(content, "minnoofitemoffered"),
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
                    "store_promotions": {store_id: store_promo_val},
                    "available_in_store_ids": [store_id]
                }
            else:
                aggregated_values[key]["store_promotions"][store_id] = store_promo_val
                if store_id not in aggregated_values[key]["available_in_store_ids"]:
                    aggregated_values[key]["available_in_store_ids"].append(store_id)
        
        if not aggregated_values:
            return

        values = []
        for (chain_id, promotion_id), data in aggregated_values.items():
            values.append((
                chain_id,
                promotion_id,
                data["sub_chain_id"],
                data["bikoret_no"],
                data["promotion_description"],
                data["promotion_update_date"],
                data["promotion_start_date"],
                data["promotion_start_hour"],
                data["promotion_end_date"],
                data["promotion_end_hour"],
                data["promotion_days"],
                data["redemption_limit"],
                data["reward_type"],
                data["allow_multiple_discounts"],
                data["is_weighted_promo"],
                data["is_gift_item"],
                data["min_no_of_item_offered"],
                data["additional_is_coupon"],
                data["additional_gift_count"],
                data["additional_is_total"],
                data["additional_is_active"],
                data["additional_restrictions"],
                data["remarks"],
                data["min_qty"],
                data["discounted_price"],
                data["discounted_price_per_mida"],
                data["weight_unit"],
                data["club_id"],
                Json(data["items"]),
                Json(data["store_promotions"]),
                data["available_in_store_ids"],
                now,
                now
            ))

        Logger.info("Upserting %d promotions into Supabase", len(values))
        with self.conn.cursor() as cur:
            execute_values(cur, query, values)



    def _ensure_stores_exist(self, items):
        """Ensure all stores referenced in items exist in the stores table.
        This prevents ForeignKeyViolations if metadata is missing.
        """
        query = """
            INSERT INTO stores (chain_id, store_id, created_at, updated_at)
            VALUES %s
            ON CONFLICT (chain_id, store_id) DO NOTHING
        """
        now = datetime.now()
        values = []
        seen = set()
        for item in items:
            content = item.get("content", {})
            chain_id = self._clean_id(self._get_val(content, "ChainId"))
            store_id = self._clean_id(self._get_val(content, "StoreId"))
            if not chain_id or not store_id:
                continue
            
            key = (chain_id, store_id)
            if key not in seen and key not in self.seen_stores:
                values.append((chain_id, store_id, now, now))
                seen.add(key)
        
        if values:
            Logger.info("Ensuring %d referenced stores exist in Supabase", len(values))
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
            
            # Mark as seen
            self.seen_stores.update(seen)

    def restart_database(
        self, enabled_scrapers: list[str], enabled_file_types: list[str]
    ):
        """Supabase uses persistent storage, so we don't wipe it by default on restart.
        
        Regular short-term databases (Mongo/Kafka) wipe on every new run if no cache exists.
        For Supabase, we prefer to keep historical data and let ON CONFLICT handle updates.
        """
        Logger.info("Supabase persistence mode: skipping database wipe on restart.")
        # We still ensure connection is fine
        self._test_connection()

    def sync_cache(self, local_cache):
        """Sync remote database state to the local cache.
        
        This allows ephemeral runners (like GitHub Actions) to know which files 
        were already processed by querying the 'processed_files' table.
        """
        Logger.info("Syncing local cache from Supabase 'processed_files' table...")
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT file_name, record_count FROM processed_files")
                rows = cur.fetchall()
                for file_name, record_count in rows:
                    if record_count > 0:
                        # Mark as fully processed (using record_count - 1 as the last row index)
                        local_cache.update_last_processed_row(file_name, record_count - 1)
                
                # Also reset local memory cache for a fresh run
                self.seen_stores = set()
                self.seen_products = set()
                
            Logger.info("Local cache synced successfully with %d files.", len(rows))
        except Exception as e:
            Logger.warning("Failed to sync cache from Supabase: %s", str(e))

    def get_processed_files_names(self):
        """Get the set of processed filenames from the database."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT file_name FROM processed_files")
                return {row[0] for row in cur.fetchall()}
        except Exception as e:
            Logger.warning("Failed to fetch processed files from Supabase: %s", str(e))
            return set()

    def get_processed_files_metadata(self):
        """Get metadata (file_name, chain_name) for all processed files."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT file_name, chain_name FROM processed_files")
                return [{"file_name": row[0], "chain_name": row[1]} for row in cur.fetchall()]
        except Exception as e:
            Logger.warning("Failed to fetch processed files metadata from Supabase: %s", str(e))
            return []

    def _clean_all_destinations(self):
        """Cleanup specific tables."""
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stores, products, prices, promotions, processed_files CASCADE")
        Logger.info("Supabase database tables truncated.")

    def _is_collection_updated(self, collection_name: str, seconds: int = 10800) -> bool:
        """Check if any data was updated recently."""
        # For Supabase, we check the latest processed_at in processed_files
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT MAX(processed_at) FROM processed_files")
                last_update = cur.fetchone()[0]
                if not last_update:
                    return False
                return (datetime.now() - last_update).total_seconds() < seconds
        except:
            return False

    def _list_destinations(self):
        return ["stores", "products", "prices", "promotions", "processed_files"]

    def get_destinations_content(self, table_name, filter=None):
        # Implementation for retrieval if needed (e.g. for AccessLayer)
        # This would require translating Mongo filters to SQL
        return []
