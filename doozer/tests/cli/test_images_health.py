from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from doozerlib.cli.images_health import ImagesHealthPipeline
from doozerlib.constants import ART_BUILD_HISTORY_URL


class TestImagesHealth(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mock_runtime = MagicMock()
        self.mock_runtime.group_config = MagicMock()
        self.pipeline = ImagesHealthPipeline(
            runtime=self.mock_runtime,
            limit=100,
            url_markup='slack'
        )

    def test_generate_art_dash_history_link(self):
        self.mock_runtime.group_config.name = 'openshift-4.18'
        self.pipeline.start_search = datetime(year=2024, month=11, day=18)
        name = 'ironic'
        self.assertEqual(
            self.pipeline.generate_art_dash_history_link(name),
            f'{ART_BUILD_HISTORY_URL}/search?group=openshift-4.18&name=%5Eironic%24&engine=brew&assembly=stream&'
            f'outcome=both&art-job-url=&after=2024-11-18'
        )

        self.mock_runtime.group_config.name = 'openshift-4.16'
        self.pipeline.start_search = datetime(year=2024, month=6, day=18)
        name = 'ose-installer'
        self.assertEqual(
            self.pipeline.generate_art_dash_history_link(name),
            f'{ART_BUILD_HISTORY_URL}/search?group=openshift-4.16&name=%5Eose-installer%24&engine=brew&'
            f'assembly=stream&outcome=both&art-job-url=&after=2024-6-18'
        )

    def test_url_text(self):
        self.pipeline.url_markup = 'slack'
        self.assertEqual(
            self.pipeline.url_text(url=ART_BUILD_HISTORY_URL, text='url'),
            f'<{ART_BUILD_HISTORY_URL}|url>'
        )

        self.pipeline.url_markup = 'github'
        self.assertEqual(
            self.pipeline.url_text(url=ART_BUILD_HISTORY_URL, text='url'),
            f'[url]({ART_BUILD_HISTORY_URL})'
        )

        self.pipeline.url_markup = 'invalid'
        with self.assertRaises(IOError):
            self.pipeline.url_text(url=ART_BUILD_HISTORY_URL, text='url'),
