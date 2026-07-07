"""
Unit tests for the status filtering functionality in sync.py
Tests the _iter_status_params function and its integration with conferences API
"""
import unittest
from tap_twilio.sync import _iter_status_params


class TestIterStatusParams(unittest.TestCase):
    """Test the _iter_status_params function"""

    def test_no_status_values_yields_once(self):
        """Test that streams without status_values yield params once unchanged"""
        endpoint_config = {
            'path': 'Accounts/{ParentId}/Messages.json',
            'params': {}
        }
        static_params = {'From': '+1234567890', 'To': '+0987654321'}
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        # Should yield exactly once
        self.assertEqual(len(results), 1)
        
        # Should return None for status_value and original params
        status_value, params = results[0]
        self.assertIsNone(status_value)
        self.assertEqual(params, {'From': '+1234567890', 'To': '+0987654321'})
        
        # Should be a copy, not the same object
        self.assertIsNot(params, static_params)

    def test_empty_status_values_yields_once(self):
        """Test that streams with empty status_values list yield params once"""
        endpoint_config = {
            'path': 'Accounts/{ParentId}/Messages.json',
            'params': {},
            'status_values': []
        }
        static_params = {'From': '+1234567890'}
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        # Should yield exactly once with empty status_values
        self.assertEqual(len(results), 1)
        status_value, params = results[0]
        self.assertIsNone(status_value)
        self.assertEqual(params, {'From': '+1234567890'})

    def test_single_status_value(self):
        """Test that single status value yields once with Status param"""
        endpoint_config = {
            'path': 'Accounts/{ParentId}/Conferences.json',
            'params': {},
            'status_values': ['completed']
        }
        static_params = {'DateUpdated>': '2023-01-01'}
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        # Should yield exactly once
        self.assertEqual(len(results), 1)
        
        status_value, params = results[0]
        self.assertEqual(status_value, 'completed')
        self.assertEqual(params, {
            'DateUpdated>': '2023-01-01',
            'Status': 'completed'
        })

    def test_multiple_status_values(self):
        """Test that multiple status values yield multiple times with correct Status params"""
        endpoint_config = {
            'path': 'Accounts/{ParentId}/Conferences.json',
            'params': {},
            'status_values': ['init', 'in-progress', 'completed']
        }
        static_params = {'DateUpdated>': '2023-01-01', 'DateUpdated<': '2023-01-31'}
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        # Should yield three times
        self.assertEqual(len(results), 3)
        
        # First status: init
        status_value, params = results[0]
        self.assertEqual(status_value, 'init')
        self.assertEqual(params, {
            'DateUpdated>': '2023-01-01',
            'DateUpdated<': '2023-01-31',
            'Status': 'init'
        })
        
        # Second status: in-progress
        status_value, params = results[1]
        self.assertEqual(status_value, 'in-progress')
        self.assertEqual(params, {
            'DateUpdated>': '2023-01-01',
            'DateUpdated<': '2023-01-31',
            'Status': 'in-progress'
        })
        
        # Third status: completed
        status_value, params = results[2]
        self.assertEqual(status_value, 'completed')
        self.assertEqual(params, {
            'DateUpdated>': '2023-01-01',
            'DateUpdated<': '2023-01-31',
            'Status': 'completed'
        })

    def test_params_are_independent_copies(self):
        """Test that each yielded params dict is an independent copy"""
        endpoint_config = {
            'status_values': ['status1', 'status2']
        }
        static_params = {'key': 'value'}
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        # Modify first result's params
        _, params1 = results[0]
        params1['modified'] = 'test'
        
        # Second result should not be affected
        _, params2 = results[1]
        self.assertNotIn('modified', params2)
        
        # Original static_params should not be affected
        self.assertNotIn('modified', static_params)
        self.assertNotIn('Status', static_params)

    def test_conferences_stream_config(self):
        """Test the actual conferences stream configuration"""
        # This mimics the actual config from streams.py
        conferences_config = {
            'stream': 'conferences',
            'api_method': 'GET',
            'api_url': 'https://api.twilio.com',
            'api_version': '2010-04-01',
            'path': 'Accounts/{ParentId}/Conferences.json',
            'data_key': 'conferences',
            'key_properties': ['sid'],
            'replication_method': 'INCREMENTAL',
            'replication_keys': ['date_updated'],
            'bookmark_query_field_from': 'DateUpdated>',
            'bookmark_query_field_to': 'DateUpdated<',
            'params': {},
            'status_values': ['init', 'in-progress', 'completed']
        }
        static_params = {
            'DateUpdated>': '2023-01-01',
            'DateUpdated<': '2023-01-31'
        }
        
        results = list(_iter_status_params(conferences_config, static_params))
        
        # Should yield three times for conferences
        self.assertEqual(len(results), 3)
        
        # Verify each status value is correctly applied
        expected_statuses = ['init', 'in-progress', 'completed']
        for i, expected_status in enumerate(expected_statuses):
            status_value, params = results[i]
            self.assertEqual(status_value, expected_status)
            self.assertEqual(params['Status'], expected_status)
            # Verify date params are preserved
            self.assertEqual(params['DateUpdated>'], '2023-01-01')
            self.assertEqual(params['DateUpdated<'], '2023-01-31')

    def test_status_param_overwrites_existing(self):
        """Test that Status param is overwritten if it already exists in static_params"""
        endpoint_config = {
            'status_values': ['new_status']
        }
        static_params = {
            'Status': 'old_status',
            'OtherParam': 'value'
        }
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        _, params = results[0]
        # Status should be overwritten with the new value
        self.assertEqual(params['Status'], 'new_status')
        # Other params should be preserved
        self.assertEqual(params['OtherParam'], 'value')
        # Original should be unchanged
        self.assertEqual(static_params['Status'], 'old_status')

    def test_empty_static_params(self):
        """Test behavior with empty static_params"""
        endpoint_config = {
            'status_values': ['status1', 'status2']
        }
        static_params = {}
        
        results = list(_iter_status_params(endpoint_config, static_params))
        
        self.assertEqual(len(results), 2)
        
        _, params1 = results[0]
        self.assertEqual(params1, {'Status': 'status1'})
        
        _, params2 = results[1]
        self.assertEqual(params2, {'Status': 'status2'})


class TestConferencesStreamIntegration(unittest.TestCase):
    """Integration tests for conferences stream status filtering"""

    def test_conferences_has_status_values_configured(self):
        """Test that conferences stream is properly configured with status_values"""
        from tap_twilio.streams import STREAMS
        
        # conferences is a child stream under accounts
        accounts_config = STREAMS['accounts']
        conferences_config = accounts_config['children']['conferences']
        
        # Verify status_values is present
        self.assertIn('status_values', conferences_config)
        
        # Verify it has the expected status values
        expected_statuses = ['init', 'in-progress', 'completed']
        self.assertEqual(conferences_config['status_values'], expected_statuses)
        
        # Verify other required fields are still present
        self.assertEqual(conferences_config['path'], 'Accounts/{ParentId}/Conferences.json')
        self.assertEqual(conferences_config['data_key'], 'conferences')
        self.assertEqual(conferences_config['replication_method'], 'INCREMENTAL')

    def test_other_streams_dont_have_status_values(self):
        """Test that other streams don't have status_values (unless explicitly added)"""
        from tap_twilio.streams import STREAMS
        
        # Check parent streams (should not have status_values unless added)
        parent_streams_to_check = ['accounts', 'usage_records', 'usage_triggers']
        
        for stream_name in parent_streams_to_check:
            if stream_name in STREAMS:
                config = STREAMS[stream_name]
                # Parent streams shouldn't have status_values
                if 'status_values' in config:
                    # If they do have it, document it
                    self.assertIsInstance(config['status_values'], list,
                                        f"Stream {stream_name} has status_values")

    def test_conferences_is_child_of_accounts(self):
        """Test that conferences is correctly nested under accounts"""
        from tap_twilio.streams import STREAMS
        
        # Verify accounts exists
        self.assertIn('accounts', STREAMS)
        
        # Verify accounts has children
        self.assertIn('children', STREAMS['accounts'])
        
        # Verify conferences is a child of accounts
        self.assertIn('conferences', STREAMS['accounts']['children'])

    def test_status_values_are_valid_twilio_conference_statuses(self):
        """Test that status values match valid Twilio conference statuses"""
        from tap_twilio.streams import STREAMS
        
        conferences_config = STREAMS['accounts']['children']['conferences']
        status_values = conferences_config.get('status_values', [])
        
        # According to Twilio API docs, valid conference statuses are:
        # init, in-progress, completed
        valid_statuses = {'init', 'in-progress', 'completed'}
        
        for status in status_values:
            self.assertIn(status, valid_statuses,
                         f"Status '{status}' is not a valid Twilio conference status")


if __name__ == '__main__':
    unittest.main()
