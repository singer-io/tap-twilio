# Changelog

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
