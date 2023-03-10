def config():
    return {
        "test_name": "test_sync",
        "tap_name": "tap-twilio",
        "type": "platform.twilio",
        "properties": {
            "account_sid": "TWILIO_ACCOUNT_SID",
        },
        "credentials": {
            "auth_token": "TWILIO_AUTH_TOKEN"
        },
        "bookmark": {
            "bookmark_key": "accounts",
            "bookmark_timestamp": "2020-03-15T14:30:31+0000"
        },
        "streams": {
            "accounts": {"sid"},
            "addresses": {"sid"},
            "dependent_phone_numbers": {"sid"},
            "applications": {"sid"},
            "available_phone_number_countries": {"country_code"},
            "available_phone_numbers_local": {"iso_country", "phone_number"},
            "available_phone_numbers_mobile": {"iso_country", "phone_number"},
            "available_phone_numbers_toll_free": {"iso_country", "phone_number"},
            "incoming_phone_numbers": {"sid"},
            "keys": {"sid"},
            "calls": {"sid"},
            "conferences": {"sid"},
            "conference_participants": {"uri"},
            "outgoing_caller_ids": {"sid"},
            "recordings": {"sid"},
            "transcriptions": {"sid"},
            "queues": {"sid"},
            "messages": {"sid"},
            "message_media": {"sid"},
            "usage_records": {"account_sid", "category", "start_date"},
            "usage_triggers": {"sid"},
            "alerts": {"sid"}
        },
        "exclude_streams": []
    }
