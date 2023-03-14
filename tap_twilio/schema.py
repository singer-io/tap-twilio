import os
import json
from singer.metadata import get_standard_metadata, to_list, to_map, write
from tap_twilio.streams import flatten_streams


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    schemas = {}
    field_metadata = {}

    flat_streams = flatten_streams()
    for stream_name, stream_metadata in flat_streams.items():
        schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
        with open(schema_path, encoding="utf-8") as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = get_standard_metadata(
            **{
                "schema": schema,
                "key_properties": stream_metadata.get('key_properties', None),
                "valid_replication_keys": stream_metadata.get('replication_keys', None),
                "replication_method": stream_metadata.get('replication_method', None),
            }
        )
        mdata = to_map(mdata)
        if stream_metadata.get('replication_keys') is not None:
            for key in stream_metadata.get('replication_keys'):
                mdata = write(mdata, ("properties", key), "inclusion", "automatic")
        mdata = to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
