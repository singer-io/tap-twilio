from base import TwilioBaseTest
from tap_tester import LOGGER, connections, menagerie, runner


class BookmarksTest(TwilioBaseTest):
    def name(self):
        return "tap_twilio_bookmarks_test"

    def test_name(self):
        LOGGER.info("Bookmarks Test for tap-twilio")

    def test_run(self):
        """
        - Verify for each incremental stream you can do a sync which records bookmarks
          and that the format matches expectations.
        - Verify that a bookmark doesn't exist for full table streams.
        - Verify the bookmark is the max value sent to the target for the a given replication key.
        - Verify 2nd sync respects the bookmark. All data of the 2nd sync is >= the bookmark
          from the first sync. The number of records in the 2nd sync is less then the first
        """
        # Instantiate connection
        conn_id = connections.ensure_connection(self)

        streams_to_test = self.expected_streams() - self.NO_DATA_STREAMS - self.DUPLICATE_RECORD_STREAMS
        expected_replication_keys = self.expected_replication_keys()
        expected_replication_methods = self.expected_replication_method()

        # Run check mode
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [
            catalog for catalog in found_catalogs if catalog.get("tap_stream_id") in streams_to_test
        ]
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_all_fields, select_all_fields=True)

        ########################
        # Run first sync
        ########################

        first_sync_record_count = self.run_and_verify_sync(conn_id)
        first_sync_records = runner.get_records_from_target_output()
        first_sync_bookmarks = menagerie.get_state(conn_id)

        #######################
        # Update State between Syncs
        #######################

        new_state = {"bookmarks": dict()}
        simulated_states = self.calculated_states_by_stream(first_sync_bookmarks)

        for stream, updated_state in simulated_states.items():
            new_state["bookmarks"][stream] = updated_state
        menagerie.set_state(conn_id, new_state)

        #######################
        # Run Second sync
        #######################

        second_sync_record_count = self.run_and_verify_sync(conn_id)
        second_sync_records = runner.get_records_from_target_output()
        second_sync_bookmarks = menagerie.get_state(conn_id)

        ########################
        # Test by Stream
        ########################

        # Verify currently syncing is set to None after successful sync
        self.assertNotIn("currently_syncing", first_sync_bookmarks)
        self.assertNotIn("currently_syncing", second_sync_bookmarks)

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Expected values
                expected_replication_method = expected_replication_methods[stream]

                # Information required for assetions from sync 1 & 2 based on expected values
                first_sync_count = first_sync_record_count.get(stream, 0)
                second_sync_count = second_sync_record_count.get(stream, 0)

                # Verify at least 1 record was replicated in the second sync
                self.assertGreater(second_sync_count, 0, msg=f"We are not fully testing bookmarking for {stream}")

                first_sync_messages = [
                    record.get("data")
                    for record in first_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                second_sync_messages = [
                    record.get("data")
                    for record in second_sync_records.get(stream, {}).get("messages", [])
                    if record.get("action") == "upsert"
                ]
                first_bookmark_value = first_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)
                second_bookmark_value = second_sync_bookmarks.get("bookmarks", {stream: None}).get(stream)

                if expected_replication_method == self.INCREMENTAL:

                    # Collect information specific to incremental streams from sync 1 & 2
                    replication_key = next(iter(expected_replication_keys[stream]))
                    simulated_bookmark = new_state["bookmarks"][stream]

                    # Verify the bookmark incremental stremas is not None
                    self.assertIsNotNone(first_bookmark_value)
                    self.assertIsNotNone(second_bookmark_value)

                    # Verify the bookmark value is of type str
                    self.assertIsInstance(first_bookmark_value, str)
                    self.assertIsInstance(second_bookmark_value, str)

                    # Verify the bookmark has expected DATE_FORMAT
                    self.assertIsDateFormat(first_bookmark_value, self.BOOKMARK_DATE_FORMAT)
                    self.assertIsDateFormat(second_bookmark_value, self.BOOKMARK_DATE_FORMAT)

                    # Verify the 2nd bookmark is equal to 1st sync bookmark
                    self.assertEqual(first_bookmark_value, second_bookmark_value)

                    for record in first_sync_messages:
                        replication_key_value = record.get(replication_key)
                        # Verify 1st sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            first_bookmark_value,
                            msg="First sync bookmark was set incorrectly, a record with a greater replication key value was synced",
                        )

                    for record in second_sync_messages:
                        replication_key_value = record.get(replication_key)
                        # Verify the 2nd sync replication key value is greater or equal to the 1st sync bookmarks
                        self.assertGreaterEqual(
                            replication_key_value,
                            simulated_bookmark,
                            msg="Second sync records do not respect the previous bookmark",
                        )
                        # Verify the 2nd sync bookmark value is the max replication key value for a given stream
                        self.assertLessEqual(
                            replication_key_value,
                            second_bookmark_value,
                            msg="Second sync bookmark was set incorrectly, a record with a greater replication key value was synced",
                        )

                    # Verify that we get less data in the 2nd sync
                    self.assertLessEqual(
                        second_sync_count,
                        first_sync_count,
                        msg="Second sync does not have less records, bookmark usage not verified",
                    )

                elif expected_replication_method == self.FULL_TABLE:

                    # Verify the syncs do not set a bookmark for full table streams
                    self.assertIsNone(first_bookmark_value)
                    self.assertIsNone(second_bookmark_value)

                    # Verify the number of records in the second sync is the same as the first
                    self.assertEqual(second_sync_count, first_sync_count)

                    # Verify both syncs have same records
                    for record in first_sync_messages:
                        self.assertIn(record, second_sync_messages)

                else:
                    raise NotImplementedError(
                        "INVALID EXPECTATIONS\t\tSTREAM: {} REPLICATION_METHOD: {}".format(
                            stream, expected_replication_method
                        )
                    )
