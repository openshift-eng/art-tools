from .. import support
from . import group_schema, image_schema, releases_schema, rpm_schema, shipment_schema, streams_schema


def ignore_validate(*args, **kwargs):
    # No-op validator; just pass through
    return ''


def validate(file, data, images_dir=None):
    return {
        'streams': streams_schema.validate,
        'image': lambda f, d: image_schema.validate(f, d, images_dir=images_dir),
        'rpm': rpm_schema.validate,
        'ignore': ignore_validate,
        'releases': releases_schema.validate,
        'shipment': shipment_schema.validate,
        'group': group_schema.validate,
    }.get(support.get_artifact_type(file), err)(file, data)


def err(*_):
    return (
        'Could not determine a schema\n'
        'Supported schemas: image, rpm\n'
        'Make sure the file is placed in either dir "images" or "rpms"'
    )
