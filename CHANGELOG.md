# Changelog

## 0.2.1
  * Bump dependency versions for twistlock compliance [#12](https://github.com/singer-io/tap-twilio/pull/12)

## 0.2.0
  * Implement lookback window for messages [#10](https://github.com/singer-io/tap-twilio/pull/10)

## 0.1.1
  * Fix Dependabot issue [#9](https://github.com/singer-io/tap-twilio/pull/9)

## 0.1.0
  * Fixes following issues [#7](https://github.com/singer-io/tap-twilio/pull/7)
      * Adds `emergency_address_status` field to `incoming_phone_numbers` stream 
      * Adds `label` field to `conference_participants` stream
      * Adds `reason_conference_ended` and `call_sid_ending_conference` fields to `conferences` stream
      * Adds `price_unit` field to `messages` stream
      * Adds `account_sid` field to `keys` stream during process_records
      * Adds `account_balance` stream implementation
      * Updates/reverts query_params for `alerts` and `usage_records` streams
      * Fixes pagination issue for `alerts` issue

## 0.0.3
  * Fixes following issues [#4](https://github.com/singer-io/tap-twilio/pull/4)
      * Fixes pagination issue
      * Fixes GitHub community issue [#1](https://github.com/singer-io/tap-twilio/issues/1)
      * Modifies query_params for `alerts` and `usage_records` streams
      * Updates data_type for `current_value` field of `usage_triggers` stream to `number`
      * Updates replication_key for `calls` and `messages` stream as `date_updated`

## 0.0.2
  * Automates field selection for replication keys [#5](https://github.com/singer-io/tap-twilio/pull/5)

## 0.0.1
  * Initial commit
