from artcommonlib.metadata import MetadataBase


class Metadata(MetadataBase):
    def __init__(self, meta_type, runtime, data_obj):
        """
        :param: meta_type - a string. Index to the sub-class <'rpm'|'image'>.
        :param: runtime - a Runtime object.
        :param data_obj - a dictionary for the metadata configuration
        """
        super().__init__(meta_type, runtime, data_obj)
        # Fields expected by doozerlib.SourceResolver when used from Elliott (e.g. OLM manifest validation).
        self.prevent_cloning = False
        self.commitish = None
        self.public_upstream_url = None
        self.public_upstream_branch = None
