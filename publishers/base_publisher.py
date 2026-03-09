"""
Base module for supermarket data publishing.
Provides the core functionality for scraping, converting, and uploading supermarket data.
"""

from utils import Logger
import datetime
import os
import shutil
from il_supermarket_scarper import ScarpingTask, ScraperFactory
from il_supermarket_parsers import ConvertingTask, FileTypesFilters
from managers.long_term_database_manager import LongTermDatasetManager
from managers.short_term_database_manager import ShortTermDBDatasetManager
from managers.cache_manager import CacheManager
from remotes import KaggleUploader, MongoDbUploader
from utils import now


class BaseSupermarketDataPublisher:
    """
    Base class for publishing supermarket data to various destinations.
    Handles scraping, converting, and uploading data to short-term and long-term databases.
    """

    def __init__(
        self,
        long_term_db_target=KaggleUploader,
        short_term_db_target=MongoDbUploader,
        number_of_scraping_processes=3,
        number_of_parseing_processs=None,
        app_folder="app_data",
        data_folder="dumps",
        outputs_folder="outputs",
        status_folder="status",
        enabled_scrapers=None,
        enabled_file_types=None,
        limit=None,
        when_date=None,
    ):
        """
        Initialize the BaseSupermarketDataPublisher.

        Args:
            long_term_db_target: Target for long-term database storage
            short_term_db_target: Target for short-term database storage
            number_of_scraping_processes: Number of concurrent scraping processes
            number_of_parseing_processs: Number of parsing processes
            app_folder: Base folder for application data
            data_folder: Subfolder for storing scraped data
            outputs_folder: Subfolder for storing output data
            status_folder: Subfolder for storing status information
            enabled_scrapers: List of enabled scrapers
            enabled_file_types: List of enabled file types
            limit: Limit on the number of items to scrape
            when_date: Date for which to scrape data
        """
        self.short_term_db_target = short_term_db_target
        self.long_term_db_target = long_term_db_target
        self.today = now()
        self.when_date = when_date
        self.number_of_scraping_processes = number_of_scraping_processes
        self.number_of_parseing_processs = (
            number_of_parseing_processs
            if number_of_parseing_processs
            else number_of_scraping_processes - 2
        )
        self.app_folder = app_folder
        self.data_folder = os.path.join(app_folder, data_folder)
        self.outputs_folder = os.path.join(app_folder, outputs_folder)
        self.status_folder = os.path.join(app_folder, data_folder, status_folder)
        self.enabled_scrapers = (
            enabled_scrapers if enabled_scrapers else ScraperFactory.all_scrapers_name()
        )
        self.enabled_file_types = (
            enabled_file_types if enabled_file_types else FileTypesFilters.all_types()
        )
        self.limit = limit
        self.processed_files_cache = set()

        Logger.info("app_folder=%s", app_folder)

    def _check_tz(self):
        """
        Verify that the system timezone is set to Asia/Jerusalem.

        Raises:
            AssertionError: If the timezone is not correctly set.
        """
        assert (
            datetime.datetime.now().hour == now().hour
        ), "The timezone should be set to Asia/Jerusalem"

    def _execute_scraping(self):
        """
        Execute the scraping task to collect supermarket data.

        Raises:
            Exception: If an error occurs during scraping.
        """
        try:
            os.makedirs(self.data_folder, exist_ok=True)

            Logger.info("Starting the scraping task")
            
            # Incremental Optimization: Sync from Supabase and create dummy markers
            self._sync_remote_state()
            if hasattr(self.short_term_db_target, "get_processed_files_metadata"):
                metadata = self.short_term_db_target.get_processed_files_metadata()

                # Build a precise map: chain_name (e.g. "OSHER_AD") -> real storage path
                # using ScraperFactory so we never rely on brittle name guessing.
                scraper_path_map = {}
                for scraper_name in self.enabled_scrapers:
                    try:
                        scraper_cls = ScraperFactory.get(scraper_name)
                        storage_path = scraper_cls(folder_name=self.data_folder).get_storage_path()
                        scraper_path_map[scraper_name] = storage_path
                    except Exception:
                        pass

                skipped_prep_count = 0
                for item in metadata:
                    f_name = item["file_name"]   # e.g. "Price123.xml" (post-parse name)
                    c_name = item["chain_name"]  # e.g. "OSHER_AD"

                    storage_path = scraper_path_map.get(c_name)
                    if not storage_path or not os.path.exists(storage_path):
                        continue

                    # The FTP file may be compressed (.gz) while the DB stores the
                    # parsed name (.xml).  Create a marker for both variants so the
                    # scraper's disk-based filter recognises the file regardless of
                    # which extension the remote server uses.
                    base_name = os.path.splitext(f_name)[0]
                    for marker_name in (base_name + ".gz", f_name):
                        marker_path = os.path.join(storage_path, marker_name)
                        if not os.path.exists(marker_path):
                            try:
                                with open(marker_path, 'w') as mf:
                                    mf.write("marker")
                                skipped_prep_count += 1
                            except Exception:
                                pass

                if skipped_prep_count > 0:
                    Logger.info(f"Created {skipped_prep_count} dummy markers to skip redundant downloads.")

            ScarpingTask(
                enabled_scrapers=self.enabled_scrapers,
                files_types=self.enabled_file_types,
                dump_folder_name=self.data_folder,
                multiprocessing=self.number_of_scraping_processes,
                lookup_in_db=True,
                when_date=self.when_date if self.when_date else now(backfill_hours=1),
                limit=self.limit,
                suppress_exception=True,
            ).start()
            Logger.info("Scraping task is done")
        except Exception as e:
            Logger.error("An error occurred during scraping: %s", e)
            raise e

    def _sync_remote_state(self):
        """Fetch processed files from the short-term database if supported."""
        if hasattr(self.short_term_db_target, "get_processed_files_names"):
            Logger.info("Syncing remote state from Supabase...")
            self.processed_files_cache = self.short_term_db_target.get_processed_files_names()
            Logger.info("Found %d already processed files in remote DB.", len(self.processed_files_cache))

    def _execute_converting(self):
        """
        Execute the converting task to parse scraped data into structured format.
        """
        Logger.info("Starting the converting task")
        os.makedirs(self.outputs_folder, exist_ok=True)
        
        self._sync_remote_state()
        
        # Pre-filter: remove already processed files (and our dummy markers) to skip conversion
        if self.processed_files_cache:
            skipped_count = 0
            for root, _, files in os.walk(self.data_folder):
                for file in files:
                    # If it's in DB OR it's a 0-byte or 'marker' file we created
                    full_path = os.path.join(root, file)
                    is_marker = False
                    try:
                        if os.path.getsize(full_path) <= 10: # Marker is 'marker' (6 bytes)
                            is_marker = True
                    except: pass

                    if file in self.processed_files_cache or is_marker:
                        os.remove(full_path)
                        skipped_count += 1
            if skipped_count > 0:
                Logger.info("Skipped conversion for %d files already present in Supabase.", skipped_count)

        ConvertingTask(
            enabled_parsers=self.enabled_scrapers,
            files_types=self.enabled_file_types,
            data_folder=self.data_folder,
            multiprocessing=self.number_of_parseing_processs,
            output_folder=self.outputs_folder,
            when_date=datetime.datetime.now(),
        ).start()

        Logger.info("Converting task is done")

    def _download_from_long_term_database(self):
        """
        Download the data from the long-term database.
        """
        Logger.info("Starting the long term database task")
        database = LongTermDatasetManager(
            long_term_db_target=self.long_term_db_target,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
        )
        return database.download()

    def _update_api_database(self, reset_cache=False):
        """
        Update the short-term database with the converted data.

        Args:
            reset_cache: Whether to force a restart of the cache (default: False).
        """
        Logger.info("Starting the short term database task")
        database = ShortTermDBDatasetManager(
            short_term_db_target=self.short_term_db_target,
            app_folder=self.app_folder,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
        )
        database.upload(force_restart=reset_cache)

    def _upload_to_kaggle(self):
        """
        Upload the data to the long-term database (Kaggle by default).
        """
        Logger.info("Starting the long term database task")
        database = LongTermDatasetManager(
            long_term_db_target=self.long_term_db_target,
            enabled_scrapers=self.enabled_scrapers,
            enabled_file_types=self.enabled_file_types,
            outputs_folder=self.outputs_folder,
            status_folder=self.status_folder,
        )
        database.compose()
        database.upload()
        # clean the dataset only if the data was uploaded successfully
        # (upload_to_dataset raise an exception)
        # if not, "compose" will clean it next time
        database.clean()

    def _upload_and_clean(self, compose=True):
        """
        Upload data to Kaggle and clean up afterward, regardless of success.

        Args:
            compose: Whether to compose the dataset before uploading (default: True).
                     This parameter is maintained for compatibility but is not used
                     in the current implementation.

        Raises:
            ValueError: If uploading to Kaggle fails.
        """
        try:
            # compose parameter is maintained for API compatibility
            # but is not used in current implementation
            self._upload_to_kaggle()
        except Exception as e:
            Logger.error("Failed to upload to kaggle")
            raise e
        finally:
            # clean data allways after uploading
            self._clean_all_source_data()

    def _clean_all_dump_files(self):
        """
        Clean all dump files in the data folder, preserving the status folder.
        """
        # Clean the folders in case of an error
        for folder in [self.data_folder]:
            if os.path.exists(folder):
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    if file_path != self.status_folder:
                        shutil.rmtree(file_path)

    def _clean_all_source_data(self):
        """
        Clean all source data, including the data, outputs, and status folders.
        """
        # Clean the folders in case of an error
        for folder in [self.data_folder, self.outputs_folder, self.status_folder]:
            if os.path.exists(folder):
                shutil.rmtree(folder)

        with CacheManager(self.app_folder) as cache:
            cache.clear()
