from base import TwilioBaseTest
from tap_tester import LOGGER, connections, runner


class PaginationTest(TwilioBaseTest):
    def name(self):
        return "tap_twilio_pagination_test"

    def test_name(self):
        LOGGER.info("Pagination Test for tap-twilio")

    def test_run(self):

        # instantiate connection
        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - self.DUPLICATE_RECORD_STREAMS

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [
            catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in streams_to_test
        ]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        # Run sync mode
        sync_record_count = self.run_and_verify_sync(conn_id)
        sync_records = runner.get_records_from_target_output()

        # Test by stream
        for stream in streams_to_test:
            with self.subTest(stream=stream):

                record_count = sync_record_count.get(stream, 0)

                sync_messages = sync_records.get(stream, {"messages": []}).get("messages")

                primary_keys = self.expected_primary_keys().get(stream)

                if stream not in self.NO_DATA_STREAMS.union(self.NON_PAGINATION_STREAMS):
                    stream_page_size = self.expected_page_limits()[stream]
                    self.assertLessEqual(stream_page_size, record_count)

                # Verify there are no duplicates across pages
                records_pks_set = {
                    tuple(message.get("data").get(primary_key) for primary_key in primary_keys)
                    for message in sync_messages
                }
                records_pks_list = [
                    tuple(message.get("data").get(primary_key) for primary_key in primary_keys)
                    for message in sync_messages
                ]

                self.assertCountEqual(records_pks_set, records_pks_list, msg=f"We have duplicate records for {stream}")