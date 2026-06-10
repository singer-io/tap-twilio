import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_twilio.schema import get_schemas
from tap_twilio.streams import flatten_streams, STREAMS
from tap_twilio.client import (
    TwilioError,
    TwilioUnauthorizedError,
    TwilioForbiddenError,
    TwilioNotFoundError,
    TwilioMethodNotAllowedError,
)

LOGGER = singer.get_logger()


def check_stream_access(client, stream_name, stream_config) -> bool:
    """Probe a top-level stream endpoint (PageSize=1) and return whether it is accessible.
    Returns False on 401/403/404/405; True on success or any other API error.
    """
    api_url = stream_config.get('api_url', 'https://api.twilio.com')
    api_version = stream_config.get('api_version', '2010-04-01')
    path = stream_config['path']
    url = '{}/{}/{}'.format(api_url, api_version, path)
    LOGGER.info("Checking access for stream '%s' via GET %s", stream_name, url)
    try:
        client.request('GET', url=url, params={'PageSize': 1}, endpoint=stream_name)
        return True
    except (TwilioUnauthorizedError, TwilioForbiddenError,
            TwilioNotFoundError, TwilioMethodNotAllowedError):
        return False
    except TwilioError:
        LOGGER.warning(
            "Stream '%s' probe returned a non-auth API error; assuming accessible.",
            stream_name,
        )
        return True


def discover(client) -> Catalog:
    """Build the Singer catalog, probing each top-level stream to verify access.
    Streams returning 401/403/404/405 are excluded. Child streams are excluded
    when their top-level parent is inaccessible. Raises if no streams are accessible.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    flat_streams = flatten_streams()

    # Determine which top-level streams are accessible
    accessible_top_level = set()
    for stream_name, stream_config in STREAMS.items():
        if check_stream_access(client, stream_name, stream_config):
            accessible_top_level.add(stream_name)
        else:
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog due to insufficient permissions.",
                stream_name,
            )

    for stream_name, schema_dict in schemas.items():
        flat = flat_streams.get(stream_name, {})

        # Exclude child/grandchild streams whose top-level parent is inaccessible
        grandparent = flat.get('grandparent_stream')
        parent = flat.get('parent_stream')
        top_level_parent = grandparent or parent  # grandparent takes precedence for grandchildren
        if top_level_parent and top_level_parent not in accessible_top_level:
            LOGGER.warning(
                "Stream '%s' will be excluded from the catalog because its "
                "top-level parent stream '%s' is not accessible.",
                stream_name,
                top_level_parent,
            )
            continue

        # Exclude top-level streams that failed the probe
        if stream_name in STREAMS and stream_name not in accessible_top_level:
            continue

        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=flat.get('key_properties'),
            schema=schema,
            metadata=mdata
        ))

    if not catalog.streams:
        raise Exception(
            "The credentials do not have read access to any of the supported streams. "
            "Verify that the API credentials have the required permissions."
        )

    return catalog

