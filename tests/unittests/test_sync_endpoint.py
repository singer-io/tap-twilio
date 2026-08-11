import unittest
from unittest import mock
from datetime import datetime, timezone
import importlib

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


class _ListWithGet(list):
    def get(self, _key, default=None):
        return default


class TestSyncEndpoint(unittest.TestCase):
    @mock.patch("tap_twilio.sync.write_schema")
    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.process_records", return_value=("2022-01-02T00:00:00Z", 1))
    @mock.patch("tap_twilio.sync.transform_json", return_value=[{
        "id": "PARENT_ID",
        "account_sid": "AC999",
        "_subresource_uris": {}
    }])
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_initializes_incremental_child_bookmark_when_no_child_path(
            self,
            mock_get_dates,
            _mock_transform_json,
            _mock_process_records,
            mock_write_bookmark,
            mock_write_schema):
        start = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end = datetime(2022, 1, 2, tzinfo=timezone.utc)
        now = datetime(2022, 1, 2, tzinfo=timezone.utc)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        client = mock.MagicMock()
        client.get.return_value = {
            "messages": [{"id": "PARENT_ID"}],
            "next_page_uri": None,
        }

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "replication_keys": ["date_sent"],
            "path": "Messages",
            "key_properties": ["sid", "id"],
            "children": {
                "message_media": {
                    "sub_resource_key": "media",
                    "replication_method": "INCREMENTAL",
                    "replication_keys": ["date_updated"],
                }
            },
        }

        state = {"bookmarks": {"messages": "2022-01-01T00:00:00Z"}}
        sync_endpoint(
            client=client,
            config={"date_window_days": "1", "lookback_window": "15"},
            catalog=DummyCatalog(),
            state=state,
            start_date="2022-01-01T00:00:00Z",
            stream_name="messages",
            path="Messages",
            endpoint_config=endpoint_config,
            bookmark_field="date_sent",
            selected_streams=["messages", "message_media"],
            date_window_days=1,
            required_streams=["messages", "message_media"],
        )

        self.assertTrue(mock_write_schema.called)
        mock_write_bookmark.assert_any_call(state, "message_media", "2022-01-01T00:00:00Z")

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

    @mock.patch("tap_twilio.sync.write_schema")
    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.process_records", return_value=("2022-01-02T00:00:00Z", 1))
    @mock.patch("tap_twilio.sync.transform_json", return_value=[{
        "id": "PARENT_ID",
        "account_sid": "AC999",
        "_subresource_uris": {
            "usage": "/usage/path",
            "media": "/media/path"
        }
    }])
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_covers_child_path_variants(
            self,
            mock_get_dates,
            _mock_transform_json,
            _mock_process_records,
            mock_write_bookmark,
            mock_write_schema):
        start = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end = datetime(2022, 1, 2, tzinfo=timezone.utc)
        now = datetime(2022, 1, 2, tzinfo=timezone.utc)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        client = mock.MagicMock()
        client.get.side_effect = [
            {
                "messages": [{"id": "PARENT_ID"}],
                "next_page_uri": "/next",
            },
            {
                "messages": [{"id": "PARENT_ID"}],
                "next_page_uri": None,
            },
        ]

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "replication_keys": ["date_updated"],
            "path": "2010-04-01/Messages",
            "bookmark_query_field_from": "DateSent>",
            "bookmark_query_field_to": "DateSent<",
            "params": {"Filter": "<parent_id>"},
            "key_properties": ["sid", "id"],
            "children": {
                "usage_records": {
                    "path": "Accounts/{ParentId}/Usage/Records.json",
                    "replication_keys": ["start_date"],
                },
                "dependent_phone_numbers": {
                    "path": "Accounts/{AccountSid}/Addresses/{ParentId}/DependentPhoneNumbers.json",
                    "replication_keys": ["date_updated"],
                },
                "message_media": {
                    "sub_resource_key": "media",
                    "replication_keys": ["date_updated"],
                },
                "missing_child": {
                    "sub_resource_key": "missing",
                    "replication_keys": ["date_updated"],
                },
            },
        }

        sync_module = importlib.import_module("tap_twilio.sync")
        original_sync_endpoint = sync_module.sync_endpoint
        with mock.patch.object(sync_module, "sync_endpoint", return_value=3) as mock_recursive:
            total = original_sync_endpoint(
                client=client,
                config={"date_window_days": "1", "lookback_window": "15"},
                catalog=DummyCatalog(),
                state={},
                start_date="2022-01-01T00:00:00Z",
                stream_name="messages",
                path="2010-04-01/Messages",
                endpoint_config=endpoint_config,
                bookmark_field="date_updated",
                selected_streams=[],
                date_window_days=1,
                parent_id="PA123",
                required_streams=["usage_records", "dependent_phone_numbers", "message_media", "missing_child"],
            )

        self.assertGreaterEqual(total, 1)
        self.assertGreaterEqual(mock_recursive.call_count, 3)
        mock_write_schema.assert_called()
        self.assertEqual(mock_write_bookmark.call_count, 1)

    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.process_records", return_value=(None, 0))
    @mock.patch("tap_twilio.sync.transform_json")
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_handles_dict_and_raw_dict_data_shapes(
            self,
            mock_get_dates,
            mock_transform_json,
            _mock_process_records,
            _mock_write_bookmark):
        start = datetime(2022, 1, 1)
        end = datetime(2022, 1, 2)
        now = datetime(2022, 1, 2)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        client = mock.MagicMock()
        client.get.return_value = {
            "messages": {"sid": "SM1"},
            "next_page_uri": None,
        }

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "path": "Messages",
            "replication_keys": ["date_updated"],
            "params": {},
        }

        sync_endpoint(
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

        self.assertTrue(mock_transform_json.called)

    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.process_records", return_value=(None, 0))
    @mock.patch("tap_twilio.sync.transform_json", return_value=[])
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_handles_data_key_missing_with_list_payload(
            self,
            mock_get_dates,
            mock_transform_json,
            _mock_process_records,
            _mock_write_bookmark):
        start = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end = datetime(2022, 1, 2, tzinfo=timezone.utc)
        now = datetime(2022, 1, 2, tzinfo=timezone.utc)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        client = mock.MagicMock()
        client.get.return_value = _ListWithGet([{"sid": "SM1"}])

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "path": "Messages",
            "replication_keys": ["date_updated"],
            "params": {},
        }

        sync_endpoint(
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

        self.assertTrue(mock_transform_json.called)

    @mock.patch("tap_twilio.sync.write_bookmark")
    @mock.patch("tap_twilio.sync.process_records", return_value=(None, 0))
    @mock.patch("tap_twilio.sync.transform_json", return_value=[])
    @mock.patch("tap_twilio.sync.get_dates")
    def test_sync_endpoint_handles_data_key_missing_with_dict_payload(
            self,
            mock_get_dates,
            mock_transform_json,
            _mock_process_records,
            _mock_write_bookmark):
        start = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end = datetime(2022, 1, 2, tzinfo=timezone.utc)
        now = datetime(2022, 1, 2, tzinfo=timezone.utc)
        mock_get_dates.return_value = (
            start,
            end,
            1,
            now,
            "2022-01-01T00:00:00Z",
            "2022-01-01T00:00:00Z",
        )

        client = mock.MagicMock()
        client.get.return_value = {"sid": "SM1"}

        endpoint_config = {
            "api_url": "https://api.twilio.com",
            "api_version": "2010-04-01",
            "data_key": "messages",
            "path": "Messages",
            "replication_keys": ["date_updated"],
            "params": {},
        }

        sync_endpoint(
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

        self.assertTrue(mock_transform_json.called)
