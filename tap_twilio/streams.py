# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
#   path: API endpoint relative path, when added to the base URL, creates the full path
#   key_properties: Primary key field(s) for the object endpoint
#   replication_method: FULL_TABLE or INCREMENTAL
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#        and setting the state
#   params: Query, sort, and other endpoint specific parameters
#   data_key: JSON element containing the records for the endpoint
#   bookmark_query_field: Typically a date-time field used for filtering the query
#   bookmark_type: Data type for bookmark, integer or datetime
#   children: A collection of child endpoints (where the endpoint path includes the parent id)
#   parent: On each of the children, the singular stream name for parent element

STREAMS = {
    # Reference: https://www.twilio.com/docs/usage/api/account#read-multiple-account-resources
    'accounts': {
        'api_url': 'https://api.twilio.com',
        'api_version': '2010-04-01',
        'path': 'Accounts.json',
        'data_key': 'accounts',
        'key_properties': ['sid'],
        'replication_method': 'FULL_TABLE',  # Fetch ALL, filter results
        'params': {},
        'pagingation': 'root',
        'children': {
            'account_balance': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Balance.json',
                'data_key': 'account_balance',
                'sub_resource_key': 'balance',
                'key_properties': ['account_sid'],
                'replication_method': 'FULL_TABLE',
                'params': {}
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/usage/api/address#read-multiple-address-resources
            'addresses': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Addresses.json',
                'data_key': 'addresses',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'root',
                'children': {
                    # pylint: disable=line-too-long
                    # Reference: https://www.twilio.com/docs/usage/api/address?code-sample=code-list-dependent-pns-subresources&code-language=curl&code-sdk-version=json#instance-subresources
                    'dependent_phone_numbers': {
                        'api_url': 'https://api.twilio.com',
                        'api_version': '2010-04-01',
                        'path': 'Accounts/{AccountSid}/Addresses/{ParentId}/DependentPhoneNumbers.json',
                        'data_key': 'dependent_phone_numbers',
                        'key_properties': ['sid'],
                        'replication_method': 'FULL_TABLE',  # ALL for parent Address
                        'params': {},
                        'pagingation': 'root',
                        'parent': 'address'
                    }
                }
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/usage/api/applications#read-multiple-application-resources
            'applications': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Applications.json',
                'data_key': 'applications',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-resource#read-a-list-of-countries
            'available_phone_number_countries': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/AvailablePhoneNumbers.json',
                'data_key': 'countries',
                'sub_resource_key': 'available_phone_numbers',
                'key_properties': ['country_code'],
                'replication_method': 'FULL_TABLE',
                'params': {},
                'pagingation': 'none',
                'children': {
                    # pylint: disable=line-too-long
                    # Reference: https://www.twilio.com/docs/phone-numbers/api/availablephonenumberlocal-resource#read-multiple-availablephonenumberlocal-resources
                    'available_phone_numbers_local': {
                        'api_url': 'https://api.twilio.com',
                        'api_version': '2010-04-01',
                        'path': 'Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/Local.json',
                        'data_key': 'available_phone_numbers',
                        'sub_resource_key': 'local',
                        'key_properties': ['iso_country', 'phone_number'],
                        'replication_method': 'FULL_TABLE',  # ALL for parent Address
                        'params': {},
                        'pagingation': 'root',
                        'activate_version': True
                    },
                    # pylint: disable=line-too-long
                    # Reference: https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-mobile-resource#read-multiple-availablephonenumbermobile-resources
                    'available_phone_numbers_mobile': {
                        'api_url': 'https://api.twilio.com',
                        'api_version': '2010-04-01',
                        'path': 'Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/Mobile.json',
                        'data_key': 'available_phone_numbers',
                        'sub_resource_key': 'mobile',
                        'key_properties': ['iso_country', 'phone_number'],
                        'replication_method': 'FULL_TABLE',  # ALL for parent Address
                        'params': {},
                        'pagingation': 'root',
                        'activate_version': True
                    },
                    # pylint: disable=line-too-long
                    # Reference: https://www.twilio.com/docs/phone-numbers/api/availablephonenumber-tollfree-resource#read-multiple-availablephonenumbertollfree-resources
                    'available_phone_numbers_toll_free': {
                        'api_url': 'https://api.twilio.com',
                        'api_version': '2010-04-01',
                        'path': 'Accounts/{AccountSid}/AvailablePhoneNumbers/{ParentId}/TollFree.json',
                        'data_key': 'available_phone_numbers',
                        'sub_resource_key': 'toll_free',
                        'key_properties': ['iso_country', 'phone_number'],
                        'replication_method': 'FULL_TABLE',  # ALL for parent Address
                        'params': {},
                        'pagingation': 'root',
                        'activate_version': True
                    }
                }
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/phone-numbers/api/incomingphonenumber-resource#read-multiple-incomingphonenumber-resources
            'incoming_phone_numbers': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/IncomingPhoneNumbers.json',
                'data_key': 'incoming_phone_numbers',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'none'
            },
            # Reference: https://www.twilio.com/docs/usage/api/keys#read-a-key-resource
            'keys': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Keys.json',
                'data_key': 'keys',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'parent': 'accounts',
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources
            'calls': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Calls.json',
                'data_key': 'calls',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Filter query
                'replication_keys': ['date_updated'],
                'bookmark_query_field_from': 'EndTime>',  # Daily
                'bookmark_query_field_to': 'EndTime<',
                'params': {},
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/voice/api/conference-resource#read-multiple-conference-resources
            'conferences': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Conferences.json',
                'data_key': 'conferences',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Filter query
                'replication_keys': ['date_updated'],
                'bookmark_query_field_from': 'DateUpdated>',  # Daily
                'bookmark_query_field_to': 'DateUpdated<',
                'params': {},
                'pagingation': 'root',
                'children': {
                    # pylint: disable=line-too-long
                    # Reference: https://www.twilio.com/docs/voice/api/conference-participant-resource#read-multiple-participant-resources
                    'conference_participants': {
                        'api_url': 'https://api.twilio.com',
                        'api_version': '2010-04-01',
                        'path': 'Accounts/{AccountSid}/Conferences/{ParentId}/Participants.json',
                        'data_key': 'participants',
                        'sub_resource_key': 'participants',
                        'key_properties': ['uri'],
                        'replication_method': 'FULL_TABLE',  # ALL for parent Conference
                        'params': {},
                        'pagingation': 'root'
                    }
                }
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/voice/api/outgoing-caller-ids#outgoingcallerids-list-resource
            'outgoing_caller_ids': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/OutgoingCallerIds.json',
                'data_key': 'outgoing_caller_ids',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'none'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/voice/api/recording#read-multiple-recording-resources
            'recordings': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Recordings.json',
                'data_key': 'recordings',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Filter query
                'replication_keys': ['date_created'],
                'bookmark_query_field_from': 'DateCreated>',  # Daily
                'bookmark_query_field_to': 'DateCreated<',
                'params': {},
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/voice/api/recording-transcription?code-sample=code-read-list-all-transcriptions&code-language=curl&code-sdk-version=json#read-multiple-transcription-resources
            'transcriptions': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Transcriptions.json',
                'data_key': 'transcriptions',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/voice/api/queue-resource#read-multiple-queue-resources
            'queues': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Queues.json',
                'data_key': 'queues',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/sms/api/message-resource#read-multiple-message-resources
            'messages': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Messages.json',
                'data_key': 'messages',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Filter query
                'replication_keys': ['date_updated'],
                'bookmark_query_field_from': 'DateSent>',  # Daily
                'bookmark_query_field_to': 'DateSent<',
                'params': {},
                'pagingation': 'root',
                'children': {
                    # pylint: disable=line-too-long
                    # Reference: https://www.twilio.com/docs/sms/api/media-resource#read-multiple-media-resources
                    'message_media': {
                        'api_url': 'https://api.twilio.com',
                        'api_version': '2010-04-01',
                        'path': 'Accounts/{AccountSid}/Messages/{ParentId}/Media.json',
                        'data_key': 'media_list',
                        'sub_resource_key': 'media',
                        'key_properties': ['sid'],
                        'replication_method': 'FULL_TABLE',  # ALL for parent Address
                        'params': {},
                        'pagingation': 'root'
                    }
                }
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/usage/api/usage-record#read-multiple-usagerecord-resources
            'usage_records': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Usage/Records.json',
                'data_key': 'usage_records',
                'key_properties': ['account_sid', 'category', 'start_date'],
                'replication_method': 'INCREMENTAL',  # Filter query
                'replication_keys': ['start_date'],
                'bookmark_query_field_from': 'StartDate',  # Daily
                'params': {},
                'pagingation': 'root'
            },
            # pylint: disable=line-too-long
            # Reference: https://www.twilio.com/docs/usage/api/usage-trigger#read-multiple-usagetrigger-resources
            'usage_triggers': {
                'api_url': 'https://api.twilio.com',
                'api_version': '2010-04-01',
                'path': 'Accounts/{ParentId}/Usage/Triggers.json',
                'data_key': 'usage_triggers',
                'key_properties': ['sid'],
                'replication_method': 'INCREMENTAL',  # Fetch ALL, filter results
                'replication_keys': ['date_updated'],
                'params': {},
                'pagingation': 'root'
            },

        }
    },
    # pylint: disable=line-too-long
    # Reference: https://www.twilio.com/docs/usage/monitor-alert#read-multiple-alert-resources
    'alerts': {
        'api_url': 'https://monitor.twilio.com',
        'api_version': 'v1',
        'path': 'Alerts',
        'data_key': 'alerts',
        'key_properties': ['sid'],
        'replication_method': 'INCREMENTAL',  # Filter query
        'replication_keys': ['date_updated'],
        'bookmark_query_field_from': 'StartDate',  # Bookmark
        'bookmark_query_field_to': 'EndDate',
        'max_days_ago': 30,
        'params': {},
        'pagination_key': 'next_page_url',
        'pagingation': 'meta'
    }
}


# De-nest children nodes for Discovery mode
def flatten_streams():
    flat_streams = {}
    # Loop through parents
    for stream_name, endpoint_config in STREAMS.items():
        flat_streams[stream_name] = {
            'key_properties': endpoint_config.get('key_properties'),
            'replication_method': endpoint_config.get('replication_method'),
            'replication_keys': endpoint_config.get('replication_keys')
        }
        # Loop through children
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_endpoint_config in children.items():
                flat_streams[child_stream_name] = {
                    'key_properties': child_endpoint_config.get('key_properties'),
                    'replication_method': child_endpoint_config.get('replication_method'),
                    'replication_keys': child_endpoint_config.get('replication_keys'),
                    'parent_stream': stream_name
                }
                # Loop through grand-children
                grandchildren = child_endpoint_config.get('children')
                if grandchildren:
                    for grandchild_stream_name, grandchild_endpoint_config in \
                            grandchildren.items():
                        flat_streams[grandchild_stream_name] = {
                            'key_properties': grandchild_endpoint_config.get('key_properties'),
                            'replication_method': grandchild_endpoint_config.get(
                                'replication_method'),
                            'replication_keys': grandchild_endpoint_config.get('replication_keys'),
                            'parent_stream': child_stream_name,
                            'grandparent_stream': stream_name
                        }
    return flat_streams
