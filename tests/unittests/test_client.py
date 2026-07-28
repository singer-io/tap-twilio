import unittest
from unittest import mock

import requests

from tap_twilio.client import (
    TwilioClient,
    TwilioBadRequestError,
    TwilioError,
    Server5xxError,
    get_exception_for_status,
    raise_for_error,
)


class TestClientHelpers(unittest.TestCase):
    def test_get_exception_for_status_known_and_unknown(self):
        self.assertIs(get_exception_for_status(400), TwilioBadRequestError)
        self.assertIs(get_exception_for_status(499), TwilioError)

    def test_raise_for_error_returns_none_on_empty_content(self):
        response = mock.MagicMock()
        response.status_code = 400
        response.text = "bad"
        response.reason = "Bad Request"
        response.content = b""
        response.raise_for_status.side_effect = requests.HTTPError("http")

        result = raise_for_error(response)
        self.assertIsNone(result)

    def test_raise_for_error_raises_specific_twilio_error(self):
        response = mock.MagicMock()
        response.status_code = 400
        response.text = "bad"
        response.reason = "Bad Request"
        response.content = b"{\"status\":400}"
        response.raise_for_status.side_effect = requests.HTTPError("http")
        response.json.return_value = {
            "status": 400,
            "message": "No to number is specified",
            "code": 21201,
            "more_info": "http://example",
        }

        with self.assertRaises(TwilioBadRequestError):
            raise_for_error(response)

    def test_raise_for_error_raises_generic_twilio_error_for_non_json(self):
        response = mock.MagicMock()
        response.status_code = 500
        response.text = "bad"
        response.reason = "Server"
        response.content = b"oops"
        response.raise_for_status.side_effect = requests.HTTPError("http")
        response.json.side_effect = ValueError("invalid json")

        with self.assertRaises(TwilioError):
            raise_for_error(response)


class TestTwilioClient(unittest.TestCase):
    def test_check_access_raises_when_credentials_missing(self):
        client = TwilioClient(None, None)
        with self.assertRaises(Exception):
            TwilioClient.check_access.__wrapped__(client)

    def test_check_access_returns_true_on_200(self):
        client = TwilioClient("AC123", "token", user_agent="ua")
        ok_response = mock.MagicMock(status_code=200)
        client._TwilioClient__session.get = mock.MagicMock(return_value=ok_response)

        result = TwilioClient.check_access.__wrapped__(client)

        self.assertTrue(result)
        client._TwilioClient__session.get.assert_called_once()

    def test_request_raises_server5xx(self):
        client = TwilioClient("AC123", "token")
        client._TwilioClient__verified = True

        response = mock.MagicMock()
        response.status_code = 503
        client._TwilioClient__session.request = mock.MagicMock(return_value=response)

        with self.assertRaises(Server5xxError):
            TwilioClient.request.__wrapped__(client, "GET", path="Accounts")

    @mock.patch("tap_twilio.client.raise_for_error")
    def test_request_calls_raise_for_error_on_non_200(self, mock_raise_for_error):
        client = TwilioClient("AC123", "token")
        client._TwilioClient__verified = True

        response = mock.MagicMock()
        response.status_code = 404
        client._TwilioClient__session.request = mock.MagicMock(return_value=response)

        TwilioClient.request.__wrapped__(client, "GET", path="Accounts")
        mock_raise_for_error.assert_called_once_with(response)

    def test_request_returns_json_and_adds_headers(self):
        client = TwilioClient("AC123", "token", user_agent="ua")
        client._TwilioClient__verified = True

        response = mock.MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}
        client._TwilioClient__session.request = mock.MagicMock(return_value=response)

        result = TwilioClient.request.__wrapped__(
            client,
            "POST",
            path="Accounts",
            headers={"X-Test": "1"},
            endpoint="accounts",
        )

        self.assertEqual(result, {"ok": True})
        kwargs = client._TwilioClient__session.request.call_args.kwargs
        self.assertEqual(kwargs["headers"]["Accept"], "application/json")
        self.assertEqual(kwargs["headers"]["User-Agent"], "ua")
        self.assertEqual(kwargs["headers"]["Content-Type"], "application/json")

    def test_request_builds_url_and_verifies_client(self):
        client = TwilioClient("AC123", "token")
        client._TwilioClient__verified = False
        client.check_access = mock.MagicMock(return_value=True)

        response = mock.MagicMock()
        response.status_code = 200
        response.json.return_value = {"ok": True}
        client._TwilioClient__session.request = mock.MagicMock(return_value=response)

        TwilioClient.request.__wrapped__(client, "GET", path="Accounts")

        client.check_access.assert_called_once()
        request_kwargs = client._TwilioClient__session.request.call_args.kwargs
        self.assertTrue(request_kwargs["url"].endswith("/Accounts.json"))
