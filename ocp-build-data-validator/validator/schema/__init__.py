from .. import support
from . import image_schema, releases_schema, rpm_schema, shipment_schema, streams_schema


def ignore_validate(*args, **kwargs):
    # No-op validator; just pass through
    return ''


def validate(file, data):
    return {
        'streams': streams_schema.validate,
        'image': image_schema.validate,
        'rpm': rpm_schema.validate,
        'ignore': ignore_validate,
        'releases': releases_schema.validate,
        'shipment': shipment_schema.validate,
    }.get(support.get_artifact_type(file), err)(file, data)


def err(*_):
    return 'Could not determine a schema\nSupported schemas: image, rpm\nMake sure the file is placed in either dir "images" or "rpms"'
