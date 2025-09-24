from unittest import TestCase
from unittest.mock import MagicMock

from doozerlib.cli.images_health import ImagesHealthPipeline


class TestImagesHealth(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_runtime = MagicMock()
        self.mock_runtime.group_config = MagicMock()
        self.pipeline = ImagesHealthPipeline(
            runtime=self.mock_runtime,
            limit=100,
            url_markup='slack',
        )
