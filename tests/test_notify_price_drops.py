import unittest
import sys
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import notify_price_drops as notifier


class PushDeliveryTests(unittest.TestCase):
    def test_push_tokens_are_queried_for_only_the_qualifying_user_ids(self):
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        cursor.fetchone.return_value = {"table_name": "push_devices"}
        cursor.fetchall.return_value = [{"user_id": "user-a", "token": "token-a"}]
        connection = Mock()
        connection.cursor.return_value = cursor

        with patch.object(notifier, "FIREBASE_SERVICE_ACCOUNT_JSON", "configured"):
            result = notifier.load_push_tokens(connection, ["user-a"])

        query, parameters = cursor.execute.call_args_list[1].args
        self.assertIn("user_id IN (%s)", query)
        self.assertEqual(parameters, ["user-a"])
        self.assertEqual(result, {"user-a": ["token-a"]})

    def test_send_push_targets_only_the_supplied_device_token(self):
        credentials = Mock(valid=True, expired=False, token="oauth-token", project_id="agali-test")
        response = Mock(status_code=200, text='{"name":"message-id"}')

        with patch.object(notifier, "_firebase_credentials", return_value=credentials), patch.object(
            notifier.requests, "post", return_value=response
        ) as post:
            sent, invalid = notifier.send_push(
                "device-token-for-one-user",
                "Price drop",
                "One watched item is cheaper",
                {"type": "price_drop"},
            )

        self.assertTrue(sent)
        self.assertFalse(invalid)
        message = post.call_args.kwargs["json"]["message"]
        self.assertEqual(message["token"], "device-token-for-one-user")
        self.assertNotIn("topic", message)
        self.assertNotIn("condition", message)

    def test_unregistered_response_marks_only_that_token_invalid(self):
        credentials = Mock(valid=True, expired=False, token="oauth-token", project_id="agali-test")
        response = Mock(status_code=404, text='{"error":{"status":"UNREGISTERED"}}')

        with patch.object(notifier, "_firebase_credentials", return_value=credentials), patch.object(
            notifier.requests, "post", return_value=response
        ):
            sent, invalid = notifier.send_push("expired-device-token", "Title", "Body", {})

        self.assertFalse(sent)
        self.assertTrue(invalid)


if __name__ == "__main__":
    unittest.main()
