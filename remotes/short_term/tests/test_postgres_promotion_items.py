import json
import unittest
from unittest.mock import MagicMock, patch

from remotes.short_term.postgres_db import PostgresUploader


SHUFERSAL_CHAIN_ID = "7290027600007"


class PostgresPromotionItemsTests(unittest.TestCase):
    @staticmethod
    def make_upsert_uploader():
        uploader = object.__new__(PostgresUploader)
        uploader.conn = None
        uploader._ensure_stores_exist = MagicMock()
        uploader._resolve_store_db_ids = MagicMock(return_value={})
        uploader._rpc_batch = MagicMock()
        uploader._compact_shufersal_promotion_items = MagicMock(return_value=0)
        uploader._pending_promo_refresh_chains = set()
        return uploader

    def test_nested_groups_are_flattened_and_exact_duplicates_removed(self):
        item_a = {"itemcode": "1", "discountedprice": "5.90"}
        item_b = {"ItemCode": "2", "MinQty": "2"}
        nested = [
            {"group": [{"promotionitems": {"promotionitem": [item_a, item_b]}}]},
            {"group": [{"promotionitems": {"promotionitem": [dict(item_a)]}}]},
        ]

        result = PostgresUploader._normalize_nested_promo_items(nested)

        self.assertEqual(result, [{"item": item_a}, {"item": item_b}])

    def test_distinct_variants_of_the_same_item_are_preserved(self):
        nested = {
            "group": [
                {
                    "promotionitems": {
                        "promotionitem": [
                            {"itemcode": "1", "discountedprice": "5.90"},
                            {"itemcode": "1", "discountedprice": "6.90"},
                        ]
                    }
                }
            ]
        }

        result = PostgresUploader._normalize_nested_promo_items(nested)

        self.assertEqual(len(result), 2)

    def test_unknown_non_item_structure_is_not_discarded(self):
        value = {"condition": {"minimum": 3}}

        self.assertEqual(
            PostgresUploader._normalize_nested_promo_items(value),
            [value],
        )

    def test_json_payloads_respect_count_and_byte_limits(self):
        records = [
            {"promotion_id": str(index), "description": "מבצע" * 5}
            for index in range(5)
        ]
        one_record_bytes = len(
            json.dumps(
                records[0],
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        max_bytes = (one_record_bytes * 2) + 4

        payloads = list(
            PostgresUploader._iter_json_payloads(
                records,
                max_records=3,
                max_bytes=max_bytes,
            )
        )

        decoded = [record for payload in payloads for record in json.loads(payload)]
        self.assertEqual(decoded, records)
        self.assertTrue(all(len(json.loads(payload)) <= 3 for payload in payloads))
        self.assertTrue(all(len(payload.encode("utf-8")) <= max_bytes for payload in payloads))

    def test_size_bounded_rpc_batching_is_limited_to_shufersal(self):
        uploader = object.__new__(PostgresUploader)
        uploader.conn = None
        uploader._run_query = MagicMock()
        other_records = [
            {"chain_id": "7290000000000", "promotion_id": str(index), "items": ["x" * 30]}
            for index in range(3)
        ]

        with patch("remotes.short_term.postgres_db._PROMO_JSON_BATCH_MAX_BYTES", 50):
            uploader._rpc_batch("merge_promotions", other_records)

        self.assertEqual(uploader._run_query.call_count, 1)

        uploader._run_query.reset_mock()
        shufersal_records = [
            {"chain_id": SHUFERSAL_CHAIN_ID, "promotion_id": str(index), "items": ["x" * 30]}
            for index in range(3)
        ]
        with patch("remotes.short_term.postgres_db._PROMO_JSON_BATCH_MAX_BYTES", 50):
            uploader._rpc_batch("merge_promotions", shufersal_records)

        self.assertEqual(uploader._run_query.call_count, 3)

    def test_compaction_is_scoped_to_touched_shufersal_promotions(self):
        uploader = object.__new__(PostgresUploader)
        uploader.conn = None
        uploader._set_statement_timeout = MagicMock()
        uploader._run_query = MagicMock(
            side_effect=[
                [{"promotion_id": "10"}],
                [
                    {
                        "items": [
                            {
                                "group": [
                                    {
                                        "promotionitems": {
                                            "promotionitem": [
                                                {"itemcode": "1", "discountedprice": "5.90"},
                                                {"itemcode": "1", "discountedprice": "5.90"},
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ],
                [],
            ]
        )

        compacted = uploader._compact_shufersal_promotion_items(["20", "10", "20"])

        self.assertEqual(compacted, 1)
        candidate_params = uploader._run_query.call_args_list[0].args[1]
        self.assertEqual(candidate_params, (SHUFERSAL_CHAIN_ID, ["10", "20"]))
        update_params = uploader._run_query.call_args_list[2].args[1]
        self.assertEqual(
            json.loads(update_params[0]),
            [{"item": {"itemcode": "1", "discountedprice": "5.90"}}],
        )
        self.assertEqual(update_params[1:], (SHUFERSAL_CHAIN_ID, "10"))
        self.assertEqual(
            [call.args[0] for call in uploader._set_statement_timeout.call_args_list],
            [7200, 300],
        )

    def test_upsert_normalizes_shufersal_before_merge(self):
        uploader = self.make_upsert_uploader()
        nested = [
            {
                "group": [
                    {
                        "promotionitems": {
                            "promotionitem": [
                                {"itemcode": "1", "discountedprice": "5.90"},
                                {"itemcode": "1", "discountedprice": "6.90"},
                            ]
                        }
                    }
                ]
            }
        ]
        rows = [
            {
                "content": {
                    "ChainId": SHUFERSAL_CHAIN_ID,
                    "StoreId": "100",
                    "PromotionId": "promo-1",
                    "groups": nested,
                }
            }
        ]

        uploader._upsert_promos(rows)

        uploader._compact_shufersal_promotion_items.assert_called_once_with(["promo-1"])
        merged = uploader._rpc_batch.call_args.args[1]
        self.assertEqual(
            merged[0]["items"],
            [
                {"item": {"itemcode": "1", "discountedprice": "5.90"}},
                {"item": {"itemcode": "1", "discountedprice": "6.90"}},
            ],
        )
        self.assertEqual(uploader._pending_promo_refresh_chains, {SHUFERSAL_CHAIN_ID})

    def test_upsert_leaves_other_chains_group_structure_unchanged(self):
        uploader = self.make_upsert_uploader()
        other_chain = "7290000000000"
        nested = [{"group": [{"promotionitems": {"promotionitem": {"itemcode": "1"}}}]}]
        rows = [
            {
                "content": {
                    "ChainId": other_chain,
                    "StoreId": "100",
                    "PromotionId": "promo-2",
                    "groups": nested,
                }
            }
        ]

        uploader._upsert_promos(rows)

        uploader._compact_shufersal_promotion_items.assert_not_called()
        merged = uploader._rpc_batch.call_args.args[1]
        self.assertEqual(merged[0]["items"], nested)
        self.assertEqual(uploader._pending_promo_refresh_chains, {other_chain})


if __name__ == "__main__":
    unittest.main()
