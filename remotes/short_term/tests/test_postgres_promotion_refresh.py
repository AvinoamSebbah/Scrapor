import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from remotes.short_term.postgres_db import PostgresUploader


SHUFERSAL_CHAIN_ID = "7290027600007"


class PostgresPromotionRefreshTests(unittest.TestCase):
    def make_uploader(self):
        uploader = object.__new__(PostgresUploader)
        uploader.conn = None
        uploader._set_statement_timeout = MagicMock()
        return uploader

    def test_shufersal_uses_extended_timeout(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SHUFERSAL_PROMO_REFRESH_STATEMENT_TIMEOUT_SECONDS", None)
            self.assertEqual(
                PostgresUploader._promotion_refresh_timeout_seconds(SHUFERSAL_CHAIN_ID),
                7200,
            )

    def test_shufersal_timeout_can_be_overridden(self):
        with patch.dict(
            os.environ,
            {"SHUFERSAL_PROMO_REFRESH_STATEMENT_TIMEOUT_SECONDS": "9000"},
        ):
            self.assertEqual(
                PostgresUploader._promotion_refresh_timeout_seconds(SHUFERSAL_CHAIN_ID),
                9000,
            )

    def test_refresh_audits_yesh_hesed_scope(self):
        uploader = self.make_uploader()
        started_at = datetime(2026, 7, 16, 12, 0, 0)
        uploader._run_query = MagicMock(
            side_effect=[
                [{"started_at": started_at}],
                [{"affected": 2}],
                [
                    {
                        "chain_name": "יש חסד",
                        "row_count": 2,
                        "promotion_count": 1,
                        "oldest_updated_at": started_at + timedelta(seconds=1),
                        "newest_updated_at": started_at + timedelta(seconds=1),
                    }
                ],
            ]
        )

        affected = uploader._refresh_promotion_store_items_for_chains([SHUFERSAL_CHAIN_ID])

        self.assertEqual(affected, 2)
        self.assertEqual(
            [call.args[0] for call in uploader._set_statement_timeout.call_args_list],
            [7200, 300],
        )

    def test_refresh_failure_is_not_swallowed(self):
        uploader = self.make_uploader()
        uploader._run_query = MagicMock(
            side_effect=[
                [{"started_at": datetime(2026, 7, 16, 12, 0, 0)}],
                TimeoutError("statement timeout"),
            ]
        )

        with self.assertRaisesRegex(RuntimeError, SHUFERSAL_CHAIN_ID):
            uploader._refresh_promotion_store_items_for_chains([SHUFERSAL_CHAIN_ID])

        self.assertEqual(
            [call.args[0] for call in uploader._set_statement_timeout.call_args_list],
            [7200, 300],
        )

    def test_refresh_fails_when_rows_are_reported_without_materialized_scopes(self):
        uploader = self.make_uploader()
        started_at = datetime(2026, 7, 16, 12, 0, 0)
        uploader._run_query = MagicMock(
            side_effect=[
                [{"started_at": started_at}],
                [{"affected": 1}],
                [],
            ]
        )

        with self.assertRaisesRegex(RuntimeError, SHUFERSAL_CHAIN_ID):
            uploader._refresh_promotion_store_items_for_chains([SHUFERSAL_CHAIN_ID])

    def test_refresh_fails_when_scope_rows_are_stale(self):
        uploader = self.make_uploader()
        started_at = datetime(2026, 7, 16, 12, 0, 0)
        uploader._run_query = MagicMock(
            side_effect=[
                [{"started_at": started_at}],
                [{"affected": 2}],
                [
                    {
                        "chain_name": "יש חסד",
                        "row_count": 2,
                        "promotion_count": 1,
                        "oldest_updated_at": started_at - timedelta(days=1),
                        "newest_updated_at": started_at - timedelta(days=1),
                    }
                ],
            ]
        )

        with self.assertRaisesRegex(RuntimeError, SHUFERSAL_CHAIN_ID):
            uploader._refresh_promotion_store_items_for_chains([SHUFERSAL_CHAIN_ID])

    def test_close_propagates_promotion_refresh_failure_after_cleanup(self):
        uploader = self.make_uploader()
        uploader._flush_pending_promotion_refresh = MagicMock(
            side_effect=TimeoutError("promotion refresh timeout")
        )
        uploader._flush_pending_product_search_stats = MagicMock()
        uploader._flush_stores_cleanup = MagicMock()
        uploader._close_connection = MagicMock()

        with self.assertRaisesRegex(RuntimeError, "Deferred promotion refresh failed"):
            uploader.close()

        uploader._flush_pending_product_search_stats.assert_called_once_with()
        uploader._flush_stores_cleanup.assert_called_once_with()
        uploader._close_connection.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
