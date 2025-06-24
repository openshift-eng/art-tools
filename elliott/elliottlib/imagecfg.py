from artcommonlib import logutil

from .metadata import Metadata

logger = logutil.get_logger(__name__)


class ImageMetadata(Metadata):
    def __init__(self, runtime, data_obj):
        super(ImageMetadata, self).__init__('image', runtime, data_obj)

    @property
    def image_name(self):
        return self.config.name

    @property
    def image_name_short(self):
        return self.image_name.split('/')[-1]

    @property
    def base_only(self):
        """
        Some images are marked base-only.  Return the flag from the config file
        if present.
        """
        return self.config.base_only

    @property
    def is_release(self):
        return self.config.get('for_release', True)

    @property
    def is_payload(self):
        return self.config.get('for_payload', False)

    @property
    def is_olm_operator(self):
        return self.config.get('update-csv', False)
