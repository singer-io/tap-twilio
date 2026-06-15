import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_twilio.schema import get_schemas
from tap_twilio.streams import flatten_streams, STREAMS
from tap_twilio.client import (
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


def _prune_inaccessible_children(schemas: dict, field_metadata: dict, flat_streams: dict) -> None:
    """Remove child streams from the catalog whose parent stream was excluded."""
    for stream_name, stream_config in list(flat_streams.items()):
        if stream_name not in schemas:
            continue
        grandparent = stream_config.get('grandparent_stream')
        parent = stream_config.get('parent_stream')
        top_level_parent = grandparent or parent
        if top_level_parent and top_level_parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                top_level_parent,
            )
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)


def _apply_access_checks(client, schemas: dict, field_metadata: dict, flat_streams: dict) -> None:
    """Remove inaccessible top-level streams and dependent child streams in place."""
    inaccessible_streams = [
        stream_name
        for stream_name, stream_config in STREAMS.items()
        if stream_name in schemas and not check_stream_access(client, stream_name, stream_config)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata, flat_streams)

    if inaccessible_streams:
        if len(inaccessible_streams) == len(STREAMS):
            raise TwilioForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client) -> Catalog:
    """Build the Singer catalog after excluding inaccessible streams."""
    schemas, field_metadata = get_schemas()
    flat_streams = flatten_streams()
    _apply_access_checks(client, schemas, field_metadata, flat_streams)

    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        flat = flat_streams.get(stream_name, {})

        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=flat.get('key_properties'),
            schema=schema,
            metadata=mdata
        ))

    return catalog

