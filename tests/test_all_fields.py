from base import TwilioBaseTest
from tap_tester import LOGGER, connections, menagerie, runner


class AllFieldsTest(TwilioBaseTest):

    # Skipping following fields since we are not able to generate test data
    MISSING_FIELDS = {
            "incoming_phone_numbers": {"voice_receive_mode"}
        }

    def name(self):
        return "tap_twilio_all_fields_test"

    def test_name(self):
        LOGGER.info("All Fields Test for tap-twilio")

    def test_run(self):
        """
        - Verify no unexpected streams were replicated.
        - Verify that more than just the automatic fields are replicated
        for each stream.
        - Verify all fields for each stream are
        replicated
        """

        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - self.NO_DATA_STREAMS

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

        # Verify no unexpected streams were replicated
        self.assertSetEqual(streams_to_test, set(sync_records.keys()))

        # get all fields metadata after performing table and field selection
        catalog_all_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog["stream_id"], catalog["stream_name"]
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [
                md_entry["breadcrumb"][1] for md_entry in catalog_entry["metadata"] if md_entry["breadcrumb"] != []
            ]
            catalog_all_fields[stream_name] = set(fields_from_field_level_md)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected automatic fields
                expected_automatic_fields = self.expected_automatic_fields().get(stream)

                # expected all fields
                expected_all_fields = catalog_all_fields[stream]

                # verify that we get some records for each stream
                self.assertGreater(sync_record_count.get(stream), 0)

                # collect actual fields
                messages = sync_records.get(stream)
                actual_all_fields = set()  # aggregate across all records
                all_record_fields = [
                    set(message["data"].keys()) for message in messages["messages"] if message["action"] == "upsert"
                ]
                for fields in all_record_fields:
                    actual_all_fields.update(fields)

                # Verify that more than just the automatic fields are replicated for each stream
                self.assertTrue(expected_automatic_fields.issubset(actual_all_fields),
                                msg=f'{expected_automatic_fields-actual_all_fields} is not in "expected_all_keys"')

                # verify all fields for each stream were replicated
                self.assertSetEqual(expected_all_fields - self.MISSING_FIELDS.get(stream, set()), actual_all_fields)
