from base import TwilioBaseTest
from tap_tester import connections, runner, LOGGER


class StartDateTest(TwilioBaseTest):

    # Creating variables to store two different start dates

    start_date_1 = ""
    start_date_2 = ""

    def name(self):
        return "tap_twilio_start_date_test"

    def test_run(self):
        """
        - Test that the start_date configuration is respected
        - Verify that a sync with a later start date has at least one record synced
        and less records than the 1st sync with a previous start date
        - Verify that each stream has less records than the earlier start date sync
        - Verify all data from later start data has bookmark values >= start_date
        - Verify that the minimum bookmark sent to the target for the later start_date sync
        is greater than or equal to the start date
        - Verify by primary key values, that all records in the 1st sync are included in the 2nd sync.
        """

        self.start_date_1 = self.get_properties()
        self.start_date_2 = '2023-03-08T00:00:00Z'
        self.START_DATE = self.start_date_1

        ##########################################################################
        ### First Sync
        ##########################################################################

        # Instantiate connection
        conn_id_1 = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - self.NO_DATA_STREAMS

        # Run check mode
        found_catalogs_1 = self.run_and_verify_check_mode(conn_id_1)

        # Table and field selection
        test_catalogs_1_all_fields = [catalog for catalog in found_catalogs_1
                                      if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(
            conn_id_1, test_catalogs_1_all_fields, select_all_fields=True)

        # Run initial sync
        record_count_by_stream_1 = self.run_and_verify_sync(conn_id_1)
        synced_records_1 = runner.get_records_from_target_output()

        ##########################################################################
        ### Update START DATE Between Syncs
        ##########################################################################

        LOGGER.info(f"REPLICATION START DATE CHANGE: {self.START_DATE} ===>>> {self.start_date_2}")
        self.START_DATE = self.start_date_2

        ##########################################################################
        ### Second Sync
        ##########################################################################

        # Create a new connection with the new start_date
        conn_id_2 = connections.ensure_connection(self, original_properties=False)

        # Run check mode
        found_catalogs_2 = self.run_and_verify_check_mode(conn_id_2)

        # Table and field selection
        test_catalogs_2_all_fields = [catalog for catalog in found_catalogs_2
                                      if catalog.get('tap_stream_id') in streams_to_test]
        self.perform_and_verify_table_and_field_selection(
            conn_id_2, test_catalogs_2_all_fields, select_all_fields=True)

        # Run sync
        record_count_by_stream_2 = self.run_and_verify_sync(conn_id_2)
        synced_records_2 = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_start_date_1 = self.start_date_1['start_date']
                expected_start_date_2 = self.start_date_2

                # All the streams obey the start date
                # Collect information for assertions from sync 1 and sync 2 based on expected values
                record_count_sync_1 = record_count_by_stream_1.get(stream, 0)
                record_count_sync_2 = record_count_by_stream_2.get(stream, 0)

                if self.expected_replication_method()[stream] == self.INCREMENTAL:

                    expected_replication_keys = self.expected_replication_keys()[stream]
                    primary_keys_list_1 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                        for message in synced_records_1.get(stream).get('messages')
                                        if message.get('action') == 'upsert']
                    primary_keys_list_2 = [tuple(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                        for message in synced_records_2.get(stream, {'messages': []}).get('messages')
                                        if message.get('action') == 'upsert']

                    primary_keys_sync_1 = set(primary_keys_list_1)
                    primary_keys_sync_2 = set(primary_keys_list_2)

                    replication_key_sync_1 = [message.get('data').get(expected_rk) for expected_rk in expected_replication_keys
                                            for message in synced_records_1.get(stream).get('messages')
                                            if message.get('action') == 'upsert']
                    replication_key_sync_2 = [message.get('data').get(expected_rk) for expected_rk in expected_replication_keys
                                            for message in synced_records_2.get(stream, {'messages': []}).get('messages')
                                            if message.get('action') == 'upsert']

                    replication_key_sync_1 = list(replication_key_sync_1)
                    replication_key_sync_2 = list(replication_key_sync_2)

                    # Verify the number of records replicated in sync 1 is greater than the number
                    # of records replicated in sync 2
                    self.assertGreaterEqual(record_count_sync_1, record_count_sync_2)

                    # Verify the records replicated in sync 2 were also replicated in sync 1
                    self.assertTrue(primary_keys_sync_2.issubset(primary_keys_sync_1))

                    # Verify that the replication keys in sync 1 are greater than or equal to start_date_1
                    for replication_key_value in replication_key_sync_1:
                        self.assertGreaterEqual(replication_key_value, expected_start_date_1)

                    # Verify that the replication keys in sync 2 are greater than or equal to start_date_2
                    for replication_key_value in replication_key_sync_2:
                        self.assertGreaterEqual(replication_key_value, expected_start_date_2)

                else:
                    self.assertEquals(record_count_sync_1, record_count_sync_1,
                                      msg="FULL_TABLE streams should extract less records for later start date.")
