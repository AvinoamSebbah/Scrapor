"""Local runner shim for reproducing GitHub Actions steps on Windows."""

from __future__ import annotations

import os
import runpy
import sys


sys.path.insert(0, os.getcwd())

_ALLOWED_STORES = {
    "YAYNO_BITAN_AND_CARREFOUR",
    "HAZI_HINAM",
    "HET_COHEN",
    "MAHSANI_ASHUK",
    "SUPER_PHARM",
    "VICTORY",
    "QUIK",
}


def _enforce_requested_store_allowlist() -> None:
    requested = os.getenv("ENABLED_SCRAPERS", "").strip()
    if not requested:
        return

    requested_stores = {store.strip() for store in requested.split(",") if store.strip()}
    blocked = sorted(requested_stores - _ALLOWED_STORES)
    if blocked:
        print(
            "LOCAL_STORE_GUARD: blocked disallowed scraper(s): "
            + ", ".join(blocked)
            + ". No scraping or upload was executed for this run.",
            file=sys.stderr,
        )
        raise SystemExit(86)


def _disable_windows_only_timezone_check() -> None:
    import publishers.dag_publisher as dag_publisher

    dag_publisher.SupermarketDataPublisherInterface._check_tz = lambda self: None


def _disable_ssl_verification_for_scraping_if_requested() -> None:
    if os.getenv("SCRAPOR_INSECURE_SSL", "").strip().lower() not in {"1", "true", "yes"}:
        return

    import requests
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    original_request = requests.sessions.Session.request

    def request_without_verify(self, method, url, **kwargs):
        kwargs.setdefault("verify", False)
        return original_request(self, method, url, **kwargs)

    requests.sessions.Session.request = request_without_verify


def _patch_windows_binary_wget_fallback() -> None:
    """Replace the scraper package wget fallback with a binary requests download."""
    if os.getenv("SCRAPOR_INSECURE_SSL", "").strip().lower() not in {"1", "true", "yes"}:
        return

    import requests
    from il_supermarket_scarper.engines import engine
    from il_supermarket_scarper.utils import connection

    def binary_download(file_link, file_save_path):
        os.makedirs(os.path.dirname(file_save_path), exist_ok=True)
        try:
            with requests.get(file_link, stream=True, timeout=120) as response:
                response.raise_for_status()
                with open(file_save_path, "wb") as out:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            out.write(chunk)
        except Exception:
            if os.path.exists(file_save_path):
                os.remove(file_save_path)
            raise

        if not os.path.exists(file_save_path):
            raise FileNotFoundError(f"File was not downloaded: {file_save_path}")
        return file_save_path

    connection.wget_file = binary_download
    engine.wget_file = binary_download


if __name__ == "__main__":
    _enforce_requested_store_allowlist()
    _disable_windows_only_timezone_check()
    _disable_ssl_verification_for_scraping_if_requested()
    _patch_windows_binary_wget_fallback()
    runpy.run_path("main.py", run_name="__main__")
