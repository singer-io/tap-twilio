import unittest
import os
from datetime import datetime as dt
from datetime import timedelta
import dateutil.parser

from tap_tester import LOGGER, connections, menagerie, runner
from tap_tester.jira_client import JiraClient as jira_client
from tap_tester.jira_client import CONFIGURATION_ENVIRONMENT as jira_config

JIRA_CLIENT = jira_client({ **jira_config })


class TwilioBaseTest(unittest.TestCase):
    """Setup expectations for test sub classes. Metadata describing streams.

    A bunch of shared methods that are used in tap-tester tests. Shared
    tap-specific methods (as needed).
    """

    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    EXPECTED_PAGE_SIZE = "expected-page-size"
    OBEYS_START_DATE = "obey-start-date"
    EXPECTED_PARENT_STREAM = "expected-parent-stream"
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    BOOKMARK_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

    start_date = "2023-02-01T00:00:00Z"

    # Skipping below streams beacuse they require a paid account to generate data
    NO_DATA_STREAMS = {"applications", "conference_participants", "dependent_phone_numbers", \
                   "transcriptions", "message_media", "messages", "alerts", \
                   "calls", "incoming_phone_numbers", "conferences"}

    # Fail the test when the JIRA card is done to allow streams to be re-added and tested
    jira_status = JIRA_CLIENT.get_status_category('TDL-26951')
    if jira_status == 'done':
        raise AssertionError('JIRA ticket has moved to done, re-add the NO_DATA_STREAMS defined to the testable streams')

    # Below stream don't support pagination as we have less data
    NON_PAGINATION_STREAMS = {"accounts", "keys", "incoming_phone_numbers", "outgoing_caller_ids", "usage_triggers"}

    # For below streams Twilio's API returns duplicate records and moreover data for below streams gets generated automatically
    DUPLICATE_RECORD_STREAMS = {"available_phone_numbers_toll_free", "available_phone_numbers_mobile", "available_phone_numbers_local"}

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-twilio"

    @staticmethod
    def get_type():
        """the expected url route ending."""
        return "platform.twilio"

    def get_properties(self, original=True):
        """Configuration properties required for the tap."""
        return_value = {"start_date": os.getenv("TWILIO_START_DATE", "2023-01-01T00:00:00Z")}

        if not original:
            return_value["start_date"] = self.start_date

        return return_value

    def get_credentials(self):
        """Authentication information for the test account."""
        return {
            "account_sid": os.getenv("TWILIO_ACCOUNT_SID"),
            "auth_token": os.getenv("TWILIO_AUTH_TOKEN"),
        }

    def required_environment_variables(self):
        return {"TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN"}

    def expected_metadata(self):
        """The expected streams and metadata about the streams."""
        return {
            "accounts": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 1,
                self.OBEYS_START_DATE: False
            },
            "account_balance": {
                self.PRIMARY_KEYS: {"account_sid"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 1,
                self.OBEYS_START_DATE: False
            },
            "addresses": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "alerts": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "applications": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "available_phone_number_countries": {
                self.PRIMARY_KEYS: {"country_code"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "available_phone_numbers_local": {
                self.PRIMARY_KEYS: {"iso_country", "phone_number"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "available_phone_numbers_mobile": {
                self.PRIMARY_KEYS: {"iso_country", "phone_number"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "available_phone_numbers_toll_free": {
                self.PRIMARY_KEYS: {"iso_country", "phone_number"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "calls": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "conference_participants": {
                self.PRIMARY_KEYS: {"uri"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "conferences": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "dependent_phone_numbers": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "incoming_phone_numbers": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "keys": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "message_media": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: False
            },
            "messages": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_sent"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "outgoing_caller_ids": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "queues": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "recordings": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_created"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "transcriptions": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "usage_records": {
                self.PRIMARY_KEYS: {"account_sid", "category", "start_date"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"start_date"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            },
            "usage_triggers": {
                self.PRIMARY_KEYS: {"sid"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {"date_updated"},
                self.EXPECTED_PAGE_SIZE: 50,
                self.OBEYS_START_DATE: True
            }
        }

    def expected_streams(self):
        """A set of expected stream names."""
        return set(self.expected_metadata().keys())

    def expected_primary_keys(self):
        """return a dictionary with key of table name and value as a set of
        primary key fields."""
        return {
            table: properties.get(self.PRIMARY_KEYS, set()) for table, properties in self.expected_metadata().items()
        }

    def expected_replication_keys(self):
        """return a dictionary with key of table name and value as a set of
        replication key fields."""
        return {
            table: properties.get(self.REPLICATION_KEYS, set())
            for table, properties in self.expected_metadata().items()
        }

    def expected_automatic_fields(self):
        """return a dictionary with key of table name and value as a set of
        automatic fields"""
        auto_fields = {}
        for k, v in self.expected_metadata().items():

            auto_fields[k] = (
                v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set())
            )
        return auto_fields

    def expected_replication_method(self):
        """return a dictionary with key of table name and value of replication
        method."""
        return {
            table: properties.get(self.REPLICATION_METHOD, None)
            for table, properties in self.expected_metadata().items()
        }

    def expected_page_limits(self):
        """return a dictionary with key of table name and value of expected page size"""
        return {
            table: properties.get(self.EXPECTED_PAGE_SIZE, set())
            for table, properties in self.expected_metadata().items()
        }

    def setUp(self):
        missing_envs = [x for x in self.required_environment_variables() if os.getenv(x) is None]
        if missing_envs:
            raise Exception(f"Missing environment variables, please set {missing_envs}.")

    def run_and_verify_check_mode(self, conn_id):
        """Run the tap in check mode and verify it succeeds. This should be ran
        prior to field selection and initial sync.

        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0, msg=f"unable to locate schemas for connection " f"{conn_id}")

        found_catalog_names = {found_catalog["stream_name"] for found_catalog in found_catalogs}
        self.assertSetEqual(self.expected_streams(), found_catalog_names, msg="discovered schemas do not match")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """Run a sync job and make sure it exited properly.

        Return a dictionary with keys of streams synced and values of
        records synced for each stream
        """
        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_primary_keys()
        )
        self.assertGreater(
            sum(sync_record_count.values()), 0, msg=f"failed to replicate any data:" f" {sync_record_count}"
        )
        LOGGER.info(f"total replicated row count: {sum(sync_record_count.values())}")

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """Perform table and field selection based off of the streams to select
        set and field selection parameters.

        Verify this results in the expected streams selected and all or
        no fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(conn_id=conn_id, catalogs=test_catalogs, select_all_fields=select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get("stream_name") for tc in test_catalogs]
        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(conn_id, cat["stream_id"])

            # Verify all testable streams are selected
            selected = catalog_entry.get("annotated-schema").get("selected")
            LOGGER.info(f"Validating selection on {cat['stream_name']}: {selected}")
            if cat["stream_name"] not in expected_selected:
                self.assertFalse(selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get("annotated-schema").get("properties").items():
                    field_selected = field_props.get("selected")
                    LOGGER.info(f"\tValidating selection on {cat['stream_name']}.{field}:" f" {field_selected}")
                    self.assertTrue(field_selected, msg="Field not selected.")

            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(cat["stream_name"])
                selected_fields = self.get_selected_fields_from_metadata(catalog_entry["metadata"])
                self.assertEqual(expected_automatic_fields, selected_fields)

    @staticmethod
    def get_selected_fields_from_metadata(metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field["breadcrumb"]) > 1
            inclusion_automatic_or_selected = (
                field["metadata"]["selected"] is True or field["metadata"]["inclusion"] == "automatic"
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field["breadcrumb"][1])
        return selected_fields

    @staticmethod
    def select_all_streams_and_fields(conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams."""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(conn_id, catalog["stream_id"])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get("annotated-schema", {}).get("properties", {}).keys()

            connections.select_catalog_and_fields_via_metadata(conn_id, catalog, schema, [], non_selected_properties)

    def calculated_states_by_stream(self, current_state):
        # {stream_name: [days, hours, minutes], ...}
        timedelta_by_stream = {stream: [5, 0, 0] for stream in self.expected_streams()}

        stream_to_calculated_state = {stream: "" for stream in current_state["bookmarks"].keys()}
        for stream, state in current_state["bookmarks"].items():

            state_as_datetime = dateutil.parser.parse(state)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = "%Y-%m-%dT00:00:00Z"
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = calculated_state_formatted

        return stream_to_calculated_state

    @staticmethod
    def assertIsDateFormat(value, str_format):
        """
        Assertion Method that verifies a string value is a formatted datetime with
        the specified format.
        """
        try:
            _ = dt.strptime(value, str_format)
        except ValueError as err:
            raise AssertionError(
                f"Value does not conform to expected format: {str_format}"
            ) from err

