import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_twilio.schema import get_schemas
from tap_twilio.streams import flatten_streams, STREAMS
from tap_twilio.client import (
    TwilioUnauthorizedError,
    TwilioForbiddenError,
)

LOGGER = singer.get_logger()


def check_stream_access(client, stream_name, stream_config, parent_id=None) -> bool:
    """Probe a stream endpoint (PageSize=1) and return whether it is accessible.
    Returns False on 401/403; True on success or any other API error.
    If parent_id is supplied, {ParentId} in the path is replaced before probing.
    """
    api_url = stream_config.get('api_url', 'https://api.twilio.com')
    api_version = stream_config.get('api_version', '2010-04-01')
    path = stream_config['path']
    if parent_id:
        path = path.replace('{ParentId}', parent_id)
    url = '{}/{}/{}'.format(api_url, api_version, path)
    try:
        client.request('GET', url=url, params={'PageSize': 1}, endpoint=stream_name)
        return True
    except (TwilioUnauthorizedError, TwilioForbiddenError) as err:
        LOGGER.warning(
            "Excluding unauthorized stream '%s' from catalog. HTTP-Error-Message: '%s'",
            stream_name,
            str(err),
        )
        return False


def _prune_inaccessible_children(
        schemas: dict, field_metadata: dict, flat_streams: dict) -> list:
    """Remove child streams from the catalog whose parent stream was excluded.
    Returns the list of child stream names that were pruned.
    """
    pruned = []
    for stream_name, stream_config in list(flat_streams.items()):
        if stream_name not in schemas:
            continue
        grandparent = stream_config.get('grandparent_stream')
        parent = stream_config.get('parent_stream')
        # Prune if the immediate parent is gone, or if the grandparent is gone
        missing_ancestor = (
            (parent and parent not in schemas) or
            (grandparent and grandparent not in schemas)
        )
        if missing_ancestor:
            missing = parent if (parent and parent not in schemas) else grandparent
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                stream_name,
                missing,
            )
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)
            pruned.append(stream_name)
    return pruned


def _apply_access_checks(client, schemas: dict, field_metadata: dict, flat_streams: dict) -> None:
    """Remove inaccessible top-level streams and dependent child streams in place.
    Also probes direct children of 'accounts' whose path can be constructed using
    the client's account SID (i.e. streams where parent_stream == 'accounts').
    """
    # --- top-level streams ---
    inaccessible_streams = [
        stream_name
        for stream_name, stream_config in STREAMS.items()
        if stream_name in schemas and not check_stream_access(client, stream_name, stream_config)
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    # --- direct children of 'accounts' (ParentId == account SID) ---
    account_sid = client.account_sid
    inaccessible_children = []
    for stream_name, flat_config in flat_streams.items():
        if flat_config.get('parent_stream') != 'accounts':
            continue
        if stream_name not in schemas:
            continue
        # Retrieve the full endpoint config from the accounts children dict
        stream_config = STREAMS.get('accounts', {}).get('children', {}).get(stream_name)
        if not stream_config:
            continue
        if not check_stream_access(client, stream_name, stream_config, parent_id=account_sid):
            inaccessible_children.append(stream_name)
            schemas.pop(stream_name, None)
            field_metadata.pop(stream_name, None)

    pruned_children = _prune_inaccessible_children(schemas, field_metadata, flat_streams)

    accessible_streams = [s for s in STREAMS if s in schemas]

    if not accessible_streams:
        raise TwilioForbiddenError(
            "HTTP-error-code: 403, Error: The credentials do not have "
            "'read' access to any supported streams."
        )
    all_excluded = inaccessible_streams + inaccessible_children + pruned_children
    if all_excluded:
        LOGGER.warning(
            "No 'read' access to stream(s): %s. Excluded from catalog.",
            ", ".join(all_excluded),
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

        replication_keys = flat.get('replication_keys')
        replication_key = replication_keys[0] if replication_keys else None

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=flat.get('key_properties'),
            schema=schema,
            metadata=mdata,
            replication_key=replication_key,
            replication_method=flat.get('replication_method'),
        ))

    return catalog

