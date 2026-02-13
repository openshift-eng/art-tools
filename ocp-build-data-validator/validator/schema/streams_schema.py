from schema import And, Optional, Schema, SchemaError

STREAMS_SCHEMA = {
    And(str, len): {
        "image": And(str, len),
        # won't need if we do not mirror or transform
        Optional("upstream_image_base"): And(str, len),
        Optional("upstream_image"): And(str, len),
        Optional("mirror"): bool,
        Optional("mirror_manifest_list"): bool,
        Optional("transform"): And(str, len),
        Optional("upstream_image_mirror"): list,
        Optional("aliases"): list,
    },
}


def streams_schema():
    return Schema(STREAMS_SCHEMA)


def validate(_, data):
    try:
        streams_schema().validate(data)
    except SchemaError as err:
        return "{}".format(err)
