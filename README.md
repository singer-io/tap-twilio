# tap-twilio

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [twilio Advertiser API]([xxx](https://support.twilio.com/s/advertiser-api-documentation))
- Extracts the following resources:
- [accounts](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
  - [account_balance](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
  - [addresses](https://www.twilio.com/docs/usage/api/address#read-multiple-address-resources)
    - [dependent_phone_numbers](https://www.twilio.com/docs/usage/api/address?code-sample=code-list-dependent-pns-subresources&code-language=curl&code-sdk-version=json#instance-subresources)
  - [applications](https://www.twilio.com/docs/usage/api/applications#read-multiple-application-resources)
  - [available_phone_number_countries](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-resource#read-a-list-of-countries)
    - [available_phone_numbers_local](https://www.twilio.com/docs/phone-numbers/api/availablephonenumberlocal-resource#read-multiple-availablephonenumberlocal-resources)
    - [available_phone_numbers_mobile](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-mobile-resource#read-multiple-availablephonenumbermobile-resources)
    - [available_phone_numbers_toll_free](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-tollfree-resource#read-multiple-availablephonenumbertollfree-resources)
  - [incoming_phone_numbers](https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources)
  - [keys](https://www.twilio.com/docs/usage/api/keys#read-a-key-resource)
  - [calls](https://www.twilio.com/docs/voice/api/call-resource#read-multiple-call-resources)
  - [conferences](https://www.twilio.com/docs/voice/api/conference-resource#read-multiple-conference-resources)
    - [conference_participants](https://www.twilio.com/docs/voice/api/conference-participant-resource#read-multiple-participant-resources)
  - [outgoing_caller_ids](https://www.twilio.com/docs/voice/api/outgoing-caller-ids#outgoingcallerids-list-resource)
  - [recordings](https://www.twilio.com/docs/voice/api/recording#read-multiple-recording-resources)
  - [transcriptions](https://www.twilio.com/docs/voice/api/recording-transcription?code-sample=code-read-list-all-transcriptions&code-language=curl&code-sdk-version=json#read-multiple-transcription-resources)
  - [queues](https://www.twilio.com/docs/voice/api/queue-resource#read-multiple-queue-resources)
  - [messages](https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources)
    - [message_media](https://www.twilio.com/docs/sms/api/media-resource#read-multiple-media-resources)
  - [usage_records](https://www.twilio.com/docs/usage/api/usage-record#read-multiple-usagerecord-resources)
  - [usage_triggers](https://www.twilio.com/docs/usage/api/usage-trigger#read-multiple-usagetrigger-resources)
- [alerts](https://www.twilio.com/docs/usage/monitor-alert#read-multiple-alert-resources) 

- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams

### Standard Endpoints:

[accounts](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts.json
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[account_balance](https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Balance.json
- Parent: accounts
- Primary key fields: account_sid
- Replication strategy: FULL_TABLE
- Transformations: None


[addresses](https://www.twilio.com/docs/usage/api/address#read-multiple-address-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Addresses.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[dependent_phone_numbers](https://www.twilio.com/docs/usage/api/address?code-sample=code-list-dependent-pns-subresources&code-language=curl&code-sdk-version=json#instance-subresources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Addresses/{ParentId}/DependentPhoneNumbers.json
- Parent: addresses
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[applications](https://www.twilio.com/docs/usage/api/applications#read-multiple-application-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Applications.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[available_phone_number_countries](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-resource#read-a-list-of-countries)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers.json
- Parent: accounts
- Primary key fields: country_code
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[available_phone_numbers_local](https://www.twilio.com/docs/phone-numbers/api/availablephonenumberlocal-resource#read-multiple-availablephonenumberlocal-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/Local.json
- Parent: available_phone_number_countries
- Primary key fields: iso_country, phone_number
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[available_phone_numbers_mobile](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-mobile-resource#read-multiple-availablephonenumbermobile-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/Mobile.json
- Parent: available_phone_number_countries
- Primary key fields: iso_country, phone_number
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[available_phone_numbers_toll_free](https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-tollfree-resource#read-multiple-availablephonenumbertollfree-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/TollFree.json
- Parent: available_phone_number_countries
- Primary key fields: iso_country, phone_number
- Replication strategy: FULL_TABLE
- Transformations: none


[incoming_phone_numbers](https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/IncomingPhoneNumbers.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[keys](https://www.twilio.com/docs/usage/api/keys#read-a-key-resource)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Keys.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[calls](https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Calls.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[conferences](https://www.twilio.com/docs/voice/api/conference-resource#read-multiple-conference-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Conferences.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[conference_participants](https://www.twilio.com/docs/voice/api/conference-participant-resource#read-multiple-participant-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Conferences/{ParentId}/Participants.json
- Parent: conferences
- Primary key fields: uri
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[outgoing_caller_ids](https://www.twilio.com/docs/voice/api/outgoing-caller-ids#outgoingcallerids-list-resource)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/OutgoingCallerIds.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[recordings](https://www.twilio.com/docs/voice/api/recording#read-multiple-recording-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Recordings.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[transcriptions](https://www.twilio.com/docs/voice/api/recording-transcription?code-sample=code-read-list-all-transcriptions&code-language=curl&code-sdk-version=json#read-multiple-transcription-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Transcriptions.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[queues](https://www.twilio.com/docs/voice/api/queue-resource#read-multiple-queue-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Queues.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[messages](https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Messages.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[message_media](https://www.twilio.com/docs/sms/api/media-resource#read-multiple-media-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Messages/{ParentId}/Media.json
- Parent: messages
- Primary key fields: sid
- Replication strategy: FULL_TABLE
- Transformations: subresources_to_array


[usage_records](https://www.twilio.com/docs/usage/api/usage-record#read-multiple-usagerecord-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Usage/Records.json
- Parent: accounts
- Primary key fields: account_sid, category, start_date
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[usage_triggers](https://www.twilio.com/docs/usage/api/usage-trigger#read-multiple-usagetrigger-resources)
- Endpoint: https://api.twilio.com/2010-04-01/Accounts/{AccountSid}/Usage/Triggers.json
- Parent: accounts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: subresources_to_array


[alerts](https://www.twilio.com/docs/usage/monitor-alert#read-multiple-alert-resources)
- Endpoint: https://monitor.twilio.com/v1/Alerts
- Primary key fields: sid
- Replication strategy: INCREMENTAL
- Transformations: none


## Authentication
This tap authenticates to the Twilio API using Basic Auth.

To set up authentication simply include your Twilio `account_sid` and `auth_token` in the tap config.


## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-twilio
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. The `api_key` is available in the twilio Console UI (see **Authentication** above). The `date_window_days` is the integer number of days (between the from and to dates) for date-windowing through the date-filtered endpoints (default = 30). The `start_date` is the absolute beginning date from which incremental loading on the initial load will start.

    ```json
        {
            "account_sid": "YOUR_ACCOUNT_SID",
            "auth_token": "YOUR_AUTH_TOKEN",
            "start_date": "2019-01-01T00:00:00Z",
            "user_agent": "tap-twilio <api_user_email@your_company.com>",
        }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "currently_syncing": "registers",
        "bookmarks": {
            "acounts": "2020-03-23T10:31:14.000000Z",
            "...": "2020-03-23T00:00:00.000000Z"
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-twilio --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-twilio --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-twilio --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-twilio --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

6. Test the Tap
    
    While developing the twilio tap, the following utilities were run in accordance with Singer.io best practices:
    Pylint to improve [code quality](https://github.com/singer-io/getting-started/blob/master/docs/BEST_PRACTICES.md#code-quality):
    ```bash
    > pylint tap_twilio -d missing-docstring -d logging-format-interpolation -d too-many-locals -d too-many-arguments
    ```
    Pylint test resulted in the following score:
    ```bash
    Your code has been rated at 10.00/10

    ```

    To [check the tap](https://github.com/singer-io/singer-tools#singer-check-tap) and verify working:
    ```bash
    > tap-twilio --config tap_config.json --catalog catalog.json | singer-check-tap > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    Check tap resulted in the following:
    ```bash
        Checking stdin for valid Singer-formatted data
        The output is valid.
        It contained 4684 messages for 17 streams.
        
             23 schema messages
           3908 record messages
            753 state messages
        
        Details by stream:
        +----------------------------------+---------+---------+
        | stream                           | records | schemas |
        +----------------------------------+---------+---------+
        | usage_records                    | 0       | 1       |
        | accounts                         | 1       | 1       |
        | recordings                       | 0       | 1       |
        | applications                     | 0       | 1       |
        | conferences                      | 0       | 1       |
        | available_phone_number_countries | 0       | 1       |
        | usage_triggers                   | 0       | 1       |
        | addresses                        | 0       | 1       |
        | queues                           | 0       | 1       |
        | calls                            | 3838    | 1       |
        | alerts                           | 60      | 1       |
        | outgoing_caller_ids              | 0       | 1       |
        | incoming_phone_numbers           | 0       | 1       |
        | transcriptions                   | 0       | 1       |
        | messages                         | 9       | 1       |
        | message_media                    | 0       | 7       |
        | keys                             | 0       | 1       |
        +----------------------------------+---------+---------+
    ```
---

Copyright &copy; 2020 Stitch
