import unittest
from unittest import mock
from datetime import datetime, timezone
import importlib
from singer.utils import strptime_to_utc

sync_module = importlib.import_module("tap_twilio.sync")


class DummyCatalog:
    def get_stream(self, _name):
        stream = mock.MagicMock()
        stream.key_properties = ["sid"]
        stream.schema.to_dict.return_value = {"type": "object", "properties": {"sid": {"type": "string"}}}
        return stream


class TestSyncUtils(unittest.TestCase):
    @mock.patch("tap_twilio.sync.singer.write_schema", side_effect=OSError("boom"))
    def test_write_schema_reraises_oserror(self, _mock_write_schema):
        with self.assertRaises(OSError):
            sync_module.write_schema(DummyCatalog(), "accounts")

    @mock.patch("tap_twilio.sync.singer.messages.write_record", side_effect=OSError("boom"))
    def test_write_record_reraises_oserror(self, _mock_write_record):
        with self.assertRaises(OSError):
            sync_module.write_record("accounts", {"sid": "AC1"}, time_extracted=datetime.utcnow())

    @mock.patch("tap_twilio.sync.singer.messages.write_record", side_effect=TypeError("boom"))
    def test_write_record_reraises_typeerror(self, _mock_write_record):
        with self.assertRaises(TypeError):
            sync_module.write_record("accounts", {"sid": "AC1"}, time_extracted=datetime.utcnow())

    @mock.patch("tap_twilio.sync.utils.now")
    def test_get_dates_with_window_fields(self, mock_now):
        now = datetime(2022, 1, 10, tzinfo=timezone.utc)
        mock_now.return_value = now
        state = {"bookmarks": {"calls": "2022-01-01T00:00:00Z"}}

        start_window, end_window, date_window_days, now_dt, last_dt, max_bm = sync_module.get_dates(
            state=state,
            stream_name="calls",
            start_date="2021-01-01T00:00:00Z",
            bookmark_field="date_updated",
            bookmark_query_field_from="DateSent>",
            bookmark_query_field_to="DateSent<",
            date_window_days=2,
            lookback_window=15,
        )

        self.assertEqual(date_window_days, 2)
        self.assertEqual(now_dt, now)
        self.assertEqual(last_dt, "2022-01-01T00:00:00Z")
        self.assertEqual(max_bm, "2022-01-01T00:00:00Z")
        self.assertLessEqual(start_window, end_window)

    @mock.patch("tap_twilio.sync.utils.now")
    def test_get_dates_messages_applies_lookback(self, mock_now):
        mock_now.return_value = datetime(2022, 1, 10, tzinfo=timezone.utc)
        state = {"bookmarks": {"messages": "2022-01-08T00:00:00Z"}}

        _start_window, _end_window, _window_days, _now_dt, last_dt, _max_bm = sync_module.get_dates(
            state=state,
            stream_name="messages",
            start_date="2021-01-01T00:00:00Z",
            bookmark_field="date_sent",
            bookmark_query_field_from="DateSent>",
            bookmark_query_field_to="DateSent<",
            date_window_days=2,
            lookback_window=3,
        )

        self.assertEqual(
            strptime_to_utc(last_dt),
            strptime_to_utc("2022-01-05T00:00:00Z"),
        )

    @mock.patch("tap_twilio.sync.utils.now")
    def test_get_dates_without_window_fields(self, mock_now):
        now = datetime(2022, 1, 3, tzinfo=timezone.utc)
        mock_now.return_value = now

        start_window, end_window, date_window_days, _now_dt, _last_dt, _max_bm = sync_module.get_dates(
            state={"bookmarks": {"accounts": "2022-01-01T00:00:00Z"}},
            stream_name="accounts",
            start_date="2021-01-01T00:00:00Z",
            bookmark_field=None,
            bookmark_query_field_from=None,
            bookmark_query_field_to=None,
            date_window_days=30,
            lookback_window=15,
        )

        self.assertEqual(start_window.day, 1)
        self.assertEqual(end_window, now)
        self.assertGreaterEqual(date_window_days, 0)
