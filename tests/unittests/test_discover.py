import unittest
from unittest.mock import MagicMock, patch
from singer.catalog import Catalog

from tap_twilio.discover import discover, check_stream_access
from tap_twilio.client import (
    TwilioUnauthorizedError,
    TwilioForbiddenError,
    TwilioNotFoundError,
    TwilioMethodNotAllowedError,
    TwilioBadRequestError,
)


# ---------------------------------------------------------------------------
# check_stream_access
# ---------------------------------------------------------------------------

class TestCheckStreamAccess(unittest.TestCase):

    _stream_config = {
        'api_url': 'https://api.twilio.com',
        'api_version': '2010-04-01',
        'path': 'Accounts.json',
    }

    def _client(self):
        return MagicMock()

    def test_returns_true_when_accessible(self):
        client = self._client()
        result = check_stream_access(client, 'accounts', self._stream_config)
        self.assertTrue(result)
        client.request.assert_called_once()

    def test_returns_false_on_unauthorized(self):
        client = self._client()
        client.request.side_effect = TwilioUnauthorizedError('401')
        result = check_stream_access(client, 'accounts', self._stream_config)
        self.assertFalse(result)

    def test_returns_false_on_forbidden(self):
        client = self._client()
        client.request.side_effect = TwilioForbiddenError('403')
        result = check_stream_access(client, 'accounts', self._stream_config)
        self.assertFalse(result)

    def test_returns_false_on_not_found(self):
        """404 is not caught — it propagates to the caller."""
        client = self._client()
        client.request.side_effect = TwilioNotFoundError('404')
        with self.assertRaises(TwilioNotFoundError):
            check_stream_access(client, 'accounts', self._stream_config)

    def test_returns_false_on_method_not_allowed(self):
        """405 is not caught — it propagates to the caller."""
        client = self._client()
        client.request.side_effect = TwilioMethodNotAllowedError('405')
        with self.assertRaises(TwilioMethodNotAllowedError):
            check_stream_access(client, 'accounts', self._stream_config)

    def test_raises_on_non_auth_twilio_error(self):
        """Non-auth API errors (e.g. 400) are not caught and propagate to the caller."""
        client = self._client()
        client.request.side_effect = TwilioBadRequestError('400')
        with self.assertRaises(TwilioBadRequestError):
            check_stream_access(client, 'accounts', self._stream_config)

    def test_reraises_non_twilio_errors(self):
        client = self._client()
        client.request.side_effect = ConnectionError('network timeout')
        with self.assertRaises(ConnectionError):
            check_stream_access(client, 'accounts', self._stream_config)

    def test_probe_uses_page_size_1(self):
        client = self._client()
        check_stream_access(client, 'accounts', self._stream_config)
        call_kwargs = client.request.call_args
        params = call_kwargs.kwargs.get('params', {})
        self.assertEqual(params.get('PageSize'), 1)

    def test_parent_id_substituted_in_url(self):
        """When parent_id is supplied, {ParentId} in the path is replaced before probing."""
        client = self._client()
        stream_config = {
            'api_url': 'https://api.twilio.com',
            'api_version': '2010-04-01',
            'path': 'Accounts/{ParentId}/AvailablePhoneNumbers.json',
        }
        check_stream_access(client, 'available_phone_number_countries',
                            stream_config, parent_id='ACtest123')
        call_kwargs = client.request.call_args
        url = call_kwargs.kwargs.get('url', '')
        self.assertIn('ACtest123', url)
        self.assertNotIn('{ParentId}', url)


# ---------------------------------------------------------------------------
# discover()
# ---------------------------------------------------------------------------

class TestDiscover(unittest.TestCase):

    @patch('tap_twilio.discover.check_stream_access')
    def test_discover_returns_catalog(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        self.assertIsInstance(catalog, Catalog)

    @patch('tap_twilio.discover.check_stream_access')
    def test_catalog_contains_streams_when_accessible(self, mock_check):
        mock_check.return_value = True
        catalog = discover(MagicMock())
        self.assertGreater(len(catalog.streams), 0)

    @patch('tap_twilio.discover.check_stream_access')
    def test_inaccessible_top_level_excludes_children(self, mock_check):
        """When a top-level stream is inaccessible, its children must also be excluded."""
        # Make 'accounts' inaccessible; 'alerts' accessible
        mock_check.side_effect = lambda client, name, cfg, **kwargs: name != 'accounts'
        catalog = discover(MagicMock())
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        # accounts itself excluded
        self.assertNotIn('accounts', stream_ids)
        # children of accounts excluded
        self.assertNotIn('addresses', stream_ids)
        self.assertNotIn('calls', stream_ids)
        self.assertNotIn('messages', stream_ids)
        # grandchildren excluded too
        self.assertNotIn('dependent_phone_numbers', stream_ids)
        self.assertNotIn('message_media', stream_ids)
        # alerts (independent top-level) still present
        self.assertIn('alerts', stream_ids)

    @patch('tap_twilio.discover.check_stream_access')
    def test_all_inaccessible_raises_exception(self, mock_check):
        mock_check.return_value = False
        with self.assertRaises(TwilioForbiddenError) as ctx:
            discover(MagicMock())
        self.assertIn("HTTP-error-code: 403, Error: The credentials do not have 'read' access to any supported streams.", str(ctx.exception))

    @patch('tap_twilio.discover.check_stream_access')
    def test_inaccessible_direct_child_excluded_from_catalog(self, mock_check):
        """A direct child of accounts that returns 403 must be excluded from the catalog
        even when the parent accounts stream itself is accessible."""
        def _side_effect(client, name, cfg, parent_id=None):
            # accounts and alerts are accessible; available_phone_number_countries is not
            return name != 'available_phone_number_countries'

        mock_client = MagicMock()
        mock_client.account_sid = 'ACtest123'
        mock_check.side_effect = _side_effect

        catalog = discover(mock_client)
        stream_ids = {s.tap_stream_id for s in catalog.streams}

        self.assertNotIn('available_phone_number_countries', stream_ids)
        # grandchildren of the excluded child must also be excluded
        self.assertNotIn('available_phone_numbers_local', stream_ids)
        self.assertNotIn('available_phone_numbers_mobile', stream_ids)
        self.assertNotIn('available_phone_numbers_toll_free', stream_ids)
        # other streams unaffected
        self.assertIn('accounts', stream_ids)


if __name__ == '__main__':
    unittest.main()
