import unittest
from unittest.mock import MagicMock, patch, call
from datetime import datetime, timezone

import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_twilio.sync import (
    process_records,
    update_currently_syncing,
    write_bookmark,
    get_bookmark,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_catalog(stream_name, schema=None, key_properties=None):
    """Return a minimal Catalog with one stream."""
    schema = schema or {"type": "object", "properties": {"sid": {"type": "string"},
                                                          "date_updated": {"type": "string",
                                                                           "format": "date-time"}}}
    entry = CatalogEntry(
        stream=stream_name,
        tap_stream_id=stream_name,
        key_properties=key_properties or ['sid'],
        schema=Schema.from_dict(schema),
        metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
    )
    catalog = Catalog([entry])
    return catalog


def _utc(dt_str):
    """Parse an RFC3339 string to a timezone-aware datetime."""
    return singer.utils.strptime_to_utc(dt_str)


# ---------------------------------------------------------------------------
# process_records – incremental (bookmark filtering)
# ---------------------------------------------------------------------------

class TestProcessRecordsIncremental(unittest.TestCase):

    @patch('tap_twilio.sync.write_record')
    def test_records_after_bookmark_are_written(self, mock_write_record):
        catalog = _make_catalog('calls')
        records = [
            {'sid': 'CA1', 'date_updated': '2022-06-20T00:00:00Z'},
            {'sid': 'CA2', 'date_updated': '2022-06-21T00:00:00Z'},
        ]
        time_extracted = singer.utils.now()

        max_bv, count = process_records(
            catalog=catalog,
            stream_name='calls',
            records=records,
            time_extracted=time_extracted,
            bookmark_field='date_updated',
            max_bookmark_value='2022-06-19T00:00:00Z',
            last_datetime='2022-06-19T00:00:00Z',
        )

        self.assertEqual(count, 2)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch('tap_twilio.sync.write_record')
    def test_records_before_bookmark_are_excluded(self, mock_write_record):
        catalog = _make_catalog('calls')
        records = [
            {'sid': 'CA_old', 'date_updated': '2022-01-01T00:00:00Z'},
        ]
        time_extracted = singer.utils.now()

        _, count = process_records(
            catalog=catalog,
            stream_name='calls',
            records=records,
            time_extracted=time_extracted,
            bookmark_field='date_updated',
            max_bookmark_value='2022-06-01T00:00:00Z',
            last_datetime='2022-06-01T00:00:00Z',
        )

        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch('tap_twilio.sync.write_record')
    def test_max_bookmark_advances_to_latest_record(self, _mock_write_record):
        catalog = _make_catalog('calls')
        records = [
            {'sid': 'CA1', 'date_updated': '2022-06-20T00:00:00Z'},
            {'sid': 'CA2', 'date_updated': '2022-06-25T00:00:00Z'},
        ]
        time_extracted = singer.utils.now()

        max_bv, _ = process_records(
            catalog=catalog,
            stream_name='calls',
            records=records,
            time_extracted=time_extracted,
            bookmark_field='date_updated',
            max_bookmark_value='2022-06-19T00:00:00Z',
            last_datetime='2022-06-19T00:00:00Z',
        )

        # max_bookmark_value should be the latest date_updated seen
        # The transformer converts datetimes, so normalise both sides for comparison
        self.assertIsNotNone(max_bv)
        self.assertGreaterEqual(
            singer.utils.strptime_to_utc(max_bv),
            singer.utils.strptime_to_utc('2022-06-25T00:00:00Z'),
        )


# ---------------------------------------------------------------------------
# process_records – full table (no bookmark field)
# ---------------------------------------------------------------------------

class TestProcessRecordsFullTable(unittest.TestCase):

    @patch('tap_twilio.sync.write_record')
    def test_all_records_written_when_no_bookmark_field(self, mock_write_record):
        catalog = _make_catalog('accounts')
        records = [
            {'sid': 'AC1'},
            {'sid': 'AC2'},
            {'sid': 'AC3'},
        ]
        time_extracted = singer.utils.now()

        _, count = process_records(
            catalog=catalog,
            stream_name='accounts',
            records=records,
            time_extracted=time_extracted,
            bookmark_field=None,
            max_bookmark_value=None,
            last_datetime=None,
        )

        self.assertEqual(count, 3)
        self.assertEqual(mock_write_record.call_count, 3)

    @patch('tap_twilio.sync.write_record')
    def test_empty_records_list_writes_nothing(self, mock_write_record):
        catalog = _make_catalog('accounts')
        time_extracted = singer.utils.now()

        _, count = process_records(
            catalog=catalog,
            stream_name='accounts',
            records=[],
            time_extracted=time_extracted,
            bookmark_field=None,
            max_bookmark_value=None,
            last_datetime=None,
        )

        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()


# ---------------------------------------------------------------------------
# update_currently_syncing
# ---------------------------------------------------------------------------

class TestUpdateCurrentlySyncing(unittest.TestCase):

    @patch('tap_twilio.sync.singer.write_state')
    def test_sets_currently_syncing(self, mock_write_state):
        state = {}
        update_currently_syncing(state, 'calls')
        self.assertEqual(state.get('currently_syncing'), 'calls')
        mock_write_state.assert_called_once_with(state)

    @patch('tap_twilio.sync.singer.write_state')
    def test_clears_currently_syncing_when_none(self, mock_write_state):
        state = {'currently_syncing': 'calls'}
        update_currently_syncing(state, None)
        self.assertNotIn('currently_syncing', state)
        mock_write_state.assert_called_once_with(state)

    @patch('tap_twilio.sync.singer.write_state')
    def test_noop_clear_when_key_absent(self, mock_write_state):
        """When currently_syncing is absent, passing None falls through to the else
        branch which calls singer.set_currently_syncing(state, None). The key is
        therefore present but set to None rather than being absent."""
        state = {}
        update_currently_syncing(state, None)
        # singer.set_currently_syncing sets the key to None when it is not already present
        self.assertIsNone(state.get('currently_syncing'))
        mock_write_state.assert_called_once_with(state)


# ---------------------------------------------------------------------------
# sync() – orchestration
# ---------------------------------------------------------------------------

class TestSync(unittest.TestCase):

    @patch('tap_twilio.sync.sync_endpoint')
    @patch('tap_twilio.sync.write_schema')
    @patch('tap_twilio.sync.update_currently_syncing')
    def test_sync_returns_immediately_when_no_streams_selected(
            self, mock_update, mock_write_schema, mock_sync_endpoint):
        from tap_twilio.sync import sync

        catalog = MagicMock()
        catalog.get_selected_streams.return_value = []

        sync(
            client=MagicMock(),
            config={'start_date': '2022-01-01T00:00:00Z'},
            catalog=catalog,
            state={},
        )

        mock_sync_endpoint.assert_not_called()
        mock_write_schema.assert_not_called()

    @patch('tap_twilio.sync.sync_endpoint', return_value=5)
    @patch('tap_twilio.sync.write_schema')
    @patch('tap_twilio.sync.singer.write_state')
    def test_sync_sets_and_clears_currently_syncing(
            self, mock_write_state, mock_write_schema, mock_sync_endpoint):
        from tap_twilio.sync import sync

        # Build a minimal catalog with 'accounts' selected
        entry = CatalogEntry(
            stream='accounts',
            tap_stream_id='accounts',
            key_properties=['sid'],
            schema=Schema.from_dict({"type": "object",
                                     "properties": {"sid": {"type": "string"}}}),
            metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
        )
        catalog = Catalog([entry])

        state = {}
        sync(
            client=MagicMock(),
            config={'start_date': '2022-01-01T00:00:00Z'},
            catalog=catalog,
            state=state,
        )

        # write_schema should have been called for accounts
        mock_write_schema.assert_any_call(catalog, 'accounts')
        # sync_endpoint was called
        mock_sync_endpoint.assert_called()
        # After sync completes, currently_syncing should be cleared (None or absent)
        currently_syncing = state.get('currently_syncing', None)
        self.assertIsNone(currently_syncing)


if __name__ == '__main__':
    unittest.main()
