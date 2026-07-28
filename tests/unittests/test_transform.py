import unittest

from tap_twilio.transform import subresources_to_array, transform_json


class TestTransform(unittest.TestCase):
    def test_subresources_to_array_transforms_dict_to_array(self):
        payload = {
            "data": [
                {
                    "sid": "A1",
                    "subresource_uris": {
                        "calls": "/calls",
                        "messages": "/messages",
                    },
                }
            ]
        }

        result = subresources_to_array(payload, "data")

        self.assertIn("_subresource_uris", result["data"][0])
        self.assertEqual(result["data"][0]["_subresource_uris"]["calls"], "/calls")
        self.assertEqual(
            result["data"][0]["subresource_uris"],
            [
                {"subresource": "calls", "uri": "/calls"},
                {"subresource": "messages", "uri": "/messages"},
            ],
        )

    def test_subresources_to_array_keeps_records_without_subresources(self):
        payload = {"data": [{"sid": "A1"}, {"sid": "A2", "subresource_uris": None}]}

        result = subresources_to_array(payload, "data")

        self.assertEqual(result["data"][0], {"sid": "A1"})
        self.assertIsNone(result["data"][1]["subresource_uris"])

    def test_transform_json_returns_transformed_data_key(self):
        payload = {
            "calls": [
                {
                    "sid": "CA1",
                    "subresource_uris": {"recordings": "/recordings"},
                }
            ]
        }

        result = transform_json(payload, "calls")

        self.assertIsInstance(result, list)
        self.assertEqual(result[0]["sid"], "CA1")
        self.assertEqual(result[0]["subresource_uris"][0]["subresource"], "recordings")
