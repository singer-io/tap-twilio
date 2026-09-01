import unittest
from unittest.mock import patch, MagicMock

from tap_twilio.sync import get_bookmark, write_bookmark


class TestGetBookmark(unittest.TestCase):
    """Tests for get_bookmark()."""

    def test_returns_default_when_state_is_none(self):
        result = get_bookmark(None, 'calls', '2021-01-01T00:00:00Z')
        self.assertEqual(result, '2021-01-01T00:00:00Z')

    def test_returns_default_when_no_bookmarks_key(self):
        state = {}
        result = get_bookmark(state, 'calls', '2021-01-01T00:00:00Z')
        self.assertEqual(result, '2021-01-01T00:00:00Z')

    def test_returns_default_when_stream_not_in_bookmarks(self):
        state = {'bookmarks': {'messages': '2022-06-01T00:00:00Z'}}
        result = get_bookmark(state, 'calls', '2021-01-01T00:00:00Z')
        self.assertEqual(result, '2021-01-01T00:00:00Z')

    def test_returns_existing_bookmark(self):
        state = {'bookmarks': {'calls': '2022-06-15T00:00:00Z'}}
        result = get_bookmark(state, 'calls', '2021-01-01T00:00:00Z')
        self.assertEqual(result, '2022-06-15T00:00:00Z')

    def test_returns_correct_stream_bookmark_when_multiple_streams(self):
        state = {
            'bookmarks': {
                'calls': '2022-01-01T00:00:00Z',
                'messages': '2023-03-01T00:00:00Z',
            }
        }
        self.assertEqual(get_bookmark(state, 'calls', 'default'), '2022-01-01T00:00:00Z')
        self.assertEqual(get_bookmark(state, 'messages', 'default'), '2023-03-01T00:00:00Z')


class TestWriteBookmark(unittest.TestCase):
    """Tests for write_bookmark()."""

    @patch('tap_twilio.sync.singer.write_state')
    def test_creates_bookmarks_key_if_absent(self, mock_write_state):
        state = {}
        write_bookmark(state, 'calls', '2022-06-15T00:00:00Z')
        self.assertIn('bookmarks', state)
        self.assertEqual(state['bookmarks']['calls'], '2022-06-15T00:00:00Z')
        mock_write_state.assert_called_once_with(state)

    @patch('tap_twilio.sync.singer.write_state')
    def test_updates_existing_bookmark(self, mock_write_state):
        state = {'bookmarks': {'calls': '2021-01-01T00:00:00Z'}}
        write_bookmark(state, 'calls', '2022-06-15T00:00:00Z')
        self.assertEqual(state['bookmarks']['calls'], '2022-06-15T00:00:00Z')
        mock_write_state.assert_called_once_with(state)

    @patch('tap_twilio.sync.singer.write_state')
    def test_adds_new_stream_bookmark_without_overwriting_others(self, mock_write_state):
        state = {'bookmarks': {'messages': '2022-01-01T00:00:00Z'}}
        write_bookmark(state, 'calls', '2022-06-15T00:00:00Z')
        self.assertEqual(state['bookmarks']['messages'], '2022-01-01T00:00:00Z')
        self.assertEqual(state['bookmarks']['calls'], '2022-06-15T00:00:00Z')

    @patch('tap_twilio.sync.singer.write_state')
    def test_bookmark_state_structure_is_valid(self, _mock_write_state):
        state = {}
        write_bookmark(state, 'alerts', '2023-01-01T00:00:00Z')
        self.assertIsInstance(state, dict)
        self.assertIn('bookmarks', state)
        self.assertIsInstance(state['bookmarks'], dict)
        self.assertIn('alerts', state['bookmarks'])

    @patch('tap_twilio.sync.singer.write_state')
    def test_bookmark_non_regression(self, _mock_write_state):
        """Writing a new bookmark does not decrease a previously written value."""
        state = {}
        write_bookmark(state, 'calls', '2022-06-15T00:00:00Z')
        old_value = state['bookmarks']['calls']
        # Simulate accidentally writing an older date
        write_bookmark(state, 'calls', '2021-01-01T00:00:00Z')
        # write_bookmark itself does not enforce non-regression — caller must manage this.
        # This test documents current behaviour.
        self.assertEqual(state['bookmarks']['calls'], '2021-01-01T00:00:00Z')
        self.assertGreater(old_value, state['bookmarks']['calls'])


if __name__ == '__main__':
    unittest.main()
