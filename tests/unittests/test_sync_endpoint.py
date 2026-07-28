import unittest
from unittest import mock
from datetime import datetime

from tap_twilio.sync import sync_endpoint


class DummyCatalog:
    def get_stream(self, _name):
        stream = mock.MagicMock()
        stream.schema.to_dict.return_value = {
            "type": "object",
            "properties": {
                "sid": {"type": "string"},
                "date_updated": {"type": "string", "format": "date-time"},
            },
        }
        stream.metadata = [{"breadcrumb": [], "metadata": {"selected": True}}]
        return stream


class TestSyncEndpoint(unittest.TestCase):
    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.process_records")
    @mock.patch("tap_twilio.sync.transform_json")
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_processes_single_page_and_writes_bookmark(
        self,
        mock_get_dates,
        mock_transform_json,
        mock_process_records,
        mock_write_bookmark,
    ):
        start = datetime(2022, 1, 1)
        end = datetime(2022, 1, 2)
        now = datetime(2022, 1, 3)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        transformed_rows = [{"sid": "SM1", "date_updated": "2022-01-02T00:00:00Z"}]
        mock_transform_json.return_value = transformed_rows
        mock_process_records.return_value = ("2022-01-02T00:00:00Z", 1)

        client = mock.MagicMock()
        client.get.return_value = {"messages": transformed_rows, "next_page_uri": None}

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "replication_keys": ["date_updated"],
            "path": "Messages",
        }

        total = sync_endpoint(
            client=client,
            config={"date_window_days": "1", "lookback_window": "15"},
            catalog=DummyCatalog(),
            state={},
            start_date="2022-01-01T00:00:00Z",
            stream_name="messages",
            path="Messages",
            endpoint_config=endpoint_config,
            bookmark_field="date_updated",
            selected_streams=["messages"],
            date_window_days=1,
            required_streams=["messages"],
        )

        self.assertEqual(total, 2)
        mock_process_records.assert_called()
        self.assertEqual(mock_write_bookmark.call_count, 2)

    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_handles_empty_data(self, mock_get_dates, mock_write_bookmark):
        start = datetime(2022, 1, 1)
        end = datetime(2022, 1, 2)
        now = datetime(2022, 1, 3)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        client = mock.MagicMock()
        client.get.return_value = {}

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "replication_keys": ["date_updated"],
            "path": "Messages",
        }

        total = sync_endpoint(
            client=client,
            config={"date_window_days": "1", "lookback_window": "15"},
            catalog=DummyCatalog(),
            state={},
            start_date="2022-01-01T00:00:00Z",
            stream_name="messages",
            path="Messages",
            endpoint_config=endpoint_config,
            bookmark_field="date_updated",
            selected_streams=["messages"],
            date_window_days=1,
            required_streams=["messages"],
        )

        self.assertEqual(total, 0)
        self.assertEqual(mock_write_bookmark.call_count, 2)
