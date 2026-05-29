"""Local Python startup hooks for the Windows GitHub Actions simulation."""

from __future__ import annotations

import os


if os.getenv("SCRAPOR_INSECURE_SSL", "").strip().lower() in {"1", "true", "yes"}:
    try:
        import requests
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        _original_request = requests.sessions.Session.request

        def _request_without_verify(self, method, url, **kwargs):
            kwargs["verify"] = False
            return _original_request(self, method, url, **kwargs)

        requests.sessions.Session.request = _request_without_verify
    except Exception:
        pass

    try:
        import requests
        from il_supermarket_scarper.engines import engine
        from il_supermarket_scarper.utils import connection

        def _binary_download(file_link, file_save_path):
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

        connection.wget_file = _binary_download
        engine.wget_file = _binary_download
        engine.Engine._wget_file = lambda self, file_link, file_save_path: _binary_download(
            file_link, file_save_path
        )
    except Exception:
        pass
