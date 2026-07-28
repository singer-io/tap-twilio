import unittest
from unittest import mock

import tap_twilio


class TestTapMain(unittest.TestCase):
    @mock.patch("tap_twilio.json.dump")
    @mock.patch("tap_twilio.discover")
    def test_do_discover_dumps_catalog(self, mock_discover, mock_json_dump):
        catalog = mock.MagicMock()
        catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = catalog

        tap_twilio.do_discover(mock.MagicMock())

        mock_discover.assert_called_once()
        mock_json_dump.assert_called_once()

    @mock.patch("tap_twilio.sync")
    @mock.patch("tap_twilio.do_discover")
    @mock.patch("tap_twilio.TwilioClient")
    @mock.patch("tap_twilio.singer.utils.parse_args")
    def test_main_discover_path_calls_do_discover(
        self, mock_parse_args, mock_twilio_client, mock_do_discover, _mock_sync
    ):
        parsed = mock.MagicMock()
        parsed.config = {
            "account_sid": "AC123",
            "auth_token": "token",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "ua",
        }
        parsed.state = None
        parsed.discover = True
        parsed.catalog = None
        mock_parse_args.return_value = parsed

        client_ctx = mock.MagicMock()
        mock_twilio_client.return_value.__enter__.return_value = client_ctx

        tap_twilio.main.__wrapped__()

        mock_do_discover.assert_called_once_with(client_ctx)

    @mock.patch("tap_twilio.sync")
    @mock.patch("tap_twilio.do_discover")
    @mock.patch("tap_twilio.TwilioClient")
    @mock.patch("tap_twilio.singer.utils.parse_args")
    def test_main_sync_path_calls_sync_with_state(
        self, mock_parse_args, mock_twilio_client, mock_do_discover, mock_sync
    ):
        parsed = mock.MagicMock()
        parsed.config = {
            "account_sid": "AC123",
            "auth_token": "token",
            "start_date": "2021-01-01T00:00:00Z",
            "user_agent": "ua",
        }
        parsed.state = {"bookmarks": {"calls": "2021-01-02T00:00:00Z"}}
        parsed.discover = False
        parsed.catalog = {"streams": []}
        mock_parse_args.return_value = parsed

        client_ctx = mock.MagicMock()
        mock_twilio_client.return_value.__enter__.return_value = client_ctx

        tap_twilio.main.__wrapped__()

        mock_do_discover.assert_not_called()
        mock_sync.assert_called_once_with(
            client=client_ctx,
            config=parsed.config,
            catalog=parsed.catalog,
            state=parsed.state,
        )
