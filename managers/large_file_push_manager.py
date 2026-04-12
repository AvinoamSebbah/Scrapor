import os
import re
from utils import Logger
from pandas import DataFrame
import pandas as pd
from remotes import ShortTermDatabaseUploader
from managers.cache_manager import CacheState
from data_models.raw_schema import DataTable, file_name_to_table


class LargeFilePushManager:

    def __init__(
        self,
        outputs_folder: str,
        database_manager: ShortTermDatabaseUploader,
        chunk_size: int = 2000,
        processed_files: set = None,
    ):
        """Initialize the LargeFilePushManager.
        The manager is responsible for pushing large files on a limited RAM machine.
        It does so by reading the file in chunks and uploading to the database.
        Args:
            outputs_folder (str): Path to the folder containing files to process
            database_manager (ShortTermDatabaseManager): Database manager for data insertion
            chunk_size (int): Number of rows to process in each chunk
            processed_files (set): Set of filenames already processed in DB
        """
        env_chunk_size = os.getenv("UPLOAD_CHUNK_SIZE")
        if env_chunk_size:
            try:
                chunk_size = int(env_chunk_size)
            except ValueError:
                Logger.warning("Invalid UPLOAD_CHUNK_SIZE=%s, using default %s", env_chunk_size, chunk_size)

        self.outputs_folder = outputs_folder
        self.database_manager = database_manager
        self.chunk_size = chunk_size
        self.processed_files = processed_files or set()

    def _get_header(self, file: str):
        file_path = os.path.join(self.outputs_folder, file)
        return pd.read_csv(file_path, nrows=0).columns

    def process_file(self, file: str, local_cache: CacheState) -> None:
        """Process a large file in chunks and upload to database.

        Args:
            file (str): Name of the file to process
            local_cache (CacheState): Cache object to track processed rows
        """
        file_path = os.path.join(self.outputs_folder, file)
        target_table_name = file_name_to_table(file)
        Logger.info(f"Pushing {file} to {target_table_name}")

        # Get last processed row from cache
        last_row = local_cache.get_last_processed_row(file, default=-1)
        Logger.info(f"Last row: {last_row}")

        # Read header for column names
        header = self._get_header(file)
        Logger.info(f"Header: {header}")

        last_row_saw = None
        open_file = None
        total_expected_records = 0
        last_chain_id = None
        last_store_id = None
        last_store_name = None

        # Statistics for skipping
        skipped_files = set()

        # Process file in chunks
        for chunk in pd.read_csv(
            file_path,
            skiprows=lambda x: x <= last_row + 1,  # skip header and rows up to last_row
            names=header,
            chunksize=self.chunk_size,
        ):

            if chunk.empty:
                Logger.warning(f"Chunk is empty,exiting... ")
                break

            # Set index releative to the 'last_row'
            stop_index = last_row + 1 + chunk.shape[0]
            chunk.index = range(last_row + 1, stop_index)
            # update for next itreation
            last_row = stop_index - 1

            # Handle overlap with previous chunk if exists
            if last_row_saw is not None:
                chunk = pd.concat([last_row_saw, chunk])

            # Process and upload chunk
            eof_to_send = []
            try:
                chunk = chunk.reset_index(names=["row_index"]).ffill()
                items = []
                for record in chunk.to_dict(orient="records"):
                    curr_file_name = record["file_name"]
                    
                    # DB-driven skip logic
                    if curr_file_name in self.processed_files:
                        if curr_file_name not in skipped_files:
                            Logger.info(f"DB sync: Skipping already processed record-file {curr_file_name}")
                            skipped_files.add(curr_file_name)
                        continue

                    if open_file is None:
                        open_file = curr_file_name
                        total_expected_records = 0
                        last_chain_id = record.get("ChainId") or record.get("ChainID")
                        last_store_id = record.get("StoreId") or record.get("StoreID")
                        last_store_name = record.get("StoreName") or record.get("StoreNm")

                    if open_file == curr_file_name:
                        total_expected_records += 1
                        # Update references in case they were null but appear later
                        if last_chain_id is None: last_chain_id = record.get("ChainId") or record.get("ChainID")
                        if last_store_id is None: last_store_id = record.get("StoreId") or record.get("StoreID")
                        if last_store_name is None: last_store_name = record.get("StoreName") or record.get("StoreNm")
                    else:
                        # Extract chain_name from found_folder (e.g. "dumps\RamiLevy" -> "RAMI_LEVY")
                        folder_name = os.path.basename(os.path.normpath(record.get("found_folder", "")))
                        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', folder_name)
                        chain_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()
                        
                        eof_to_send.append({
                            "file_complete": "true",
                            "file_name": open_file,
                            "total_expected_records": total_expected_records,
                            "chain_id": last_chain_id,
                            "store_id": last_store_id,
                            "store_name": last_store_name,
                            "chain_name": chain_name
                        })
                        open_file = curr_file_name
                        total_expected_records = 1
                        last_chain_id = record.get("ChainId") or record.get("ChainID")
                        last_store_id = record.get("StoreId") or record.get("StoreID")
                        last_store_name = record.get("StoreName") or record.get("StoreNm")

                    items.append(DataTable(
                            row_index=record["row_index"],
                            found_folder=record["found_folder"],
                            file_name=curr_file_name,
                            content={
                                k: v
                                for k, v in record.items()
                                if k not in ["row_index", "found_folder", "file_name"]
                            },
                    ).to_dict())
            except Exception as e:
                Logger.error(f"Error processing chunk: {e}")
                Logger.error(f"Chunk: {chunk}")
                raise e

            # remove the first item since it was used of ffill
            if last_row_saw is not None:
                if items:
                    items = items[1:]

            if items:
                # log the batch
                Logger.info(
                    f"Batch start: {items[0]['row_index']}, end: {items[-1]['row_index']} (Upserting {len(items)} items)"
                )
                self.database_manager._insert_to_destinations(target_table_name, items)
            
            if eof_to_send:
                self.database_manager._insert_to_destinations(target_table_name, eof_to_send)
            
            # Save last row for next iteration
            last_row_saw = chunk.tail(1).set_index("row_index")

        if open_file is not None:
            # Re-read the first line or just pass an empty chain name?
            # Wait, we can't get record easily here unless we saved last_found_folder.
            # But the file only contains records from one found_folder!
            # We can extract it directly from the file_path argument:
            folder_name = os.path.basename(os.path.dirname(os.path.normpath(file_path)))
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', folder_name)
            chain_name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()

            self.database_manager._insert_to_destinations(target_table_name, [{
                                 "file_complete": "true",
                                 "file_name": open_file,
                                 "total_expected_records": total_expected_records,
                                 "chain_id": last_chain_id,
                                 "store_id": last_store_id,
                                 "store_name": last_store_name,
                                 "chain_name": chain_name
                             }])
        # Update cache with last processed row
        local_cache.update_last_processed_row(file, last_row)
        Logger.info(f"Completed pushing {file}")
