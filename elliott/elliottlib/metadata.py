from artcommonlib.metadata import MetadataBase


class Metadata(MetadataBase):
    def __init__(self, meta_type, runtime, data_obj):
        """
        :param: meta_type - a string. Index to the sub-class <'rpm'|'image'>.
        :param: runtime - a Runtime object.
        :param data_obj - a dictionary for the metadata configuration
        """
        super().__init__(meta_type, runtime, data_obj)
