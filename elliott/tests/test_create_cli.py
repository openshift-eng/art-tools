import unittest
from unittest import mock
from unittest.mock import MagicMock

import yaml
from elliottlib.util import get_advisory_boilerplate

COMMON_ADVISORY_TEMPLATE = """
boilerplates:
  image:
    rhsa:
      synopsis: Common advisory rhsa synopsis
      topic: Common advisory rhsa topic
      description: Common advisory rhsa description
      solution: Common advisory rhsa solution
    rhba:
      synopsis: Common advisory rhba synopsis
      topic: Common advisory rhba topic
      description: Common advisory rhba description
      solution: Common advisory rhba solution
"""

GROUP_ADVISORY_TEMPLATE = """
boilerplates:
  image:
    rhsa:
      synopsis: Group advisory rhsa synopsis
      topic: Group advisory rhsa topic
      description: Group advisory rhsa description
      solution: Group advisory rhsa solution
    rhba:
      synopsis: Group advisory rhba synopsis
      topic: Group advisory rhba topic
      description: Group advisory rhba description
      solution: Group advisory rhba solution
"""

GROUP_ADVISORY_TEMPLATE_LEGACY = """
boilerplates:
  image:
    synopsis: Group advisory rhba synopsis
    topic: Group advisory rhba topic
    description: Group advisory rhba description
    solution: Group advisory rhba solution
"""


class TestGetAdvisoryBoilerplate(unittest.TestCase):
    @mock.patch("elliottlib.util.get_common_advisory_template")
    def test_get_common_advisory_rhsa(self, mock_get_common_advisory_template):
        # Arrange
        et_data = {}
        mock_get_common_advisory_template.return_value = yaml.safe_load(COMMON_ADVISORY_TEMPLATE)
        art_advisory_key = "image"
        errata_type = "rhsa"
        runtime = MagicMock()
        # Act
        result = get_advisory_boilerplate(runtime, et_data, art_advisory_key, errata_type)

        # Assert
        self.assertEqual(result["synopsis"], "Common advisory rhsa synopsis")

    @mock.patch("elliottlib.util.get_common_advisory_template")
    def test_get_common_advisory_rhba(self, mock_get_common_advisory_template):
        # Arrange
        et_data = {}
        mock_get_common_advisory_template.return_value = yaml.safe_load(COMMON_ADVISORY_TEMPLATE)
        art_advisory_key = "image"
        errata_type = "rhba"
        runtime = MagicMock()

        # Act
        result = get_advisory_boilerplate(runtime, et_data, art_advisory_key, errata_type)

        # Assert
        self.assertEqual(result["synopsis"], "Common advisory rhba synopsis")

    @mock.patch("elliottlib.util.get_common_advisory_template")
    def test_get_group_advisory(self, mock_get_common_advisory_template):
        # Arrange
        et_data = yaml.safe_load(GROUP_ADVISORY_TEMPLATE)
        mock_get_common_advisory_template.return_value = yaml.safe_load(COMMON_ADVISORY_TEMPLATE)
        art_advisory_key = "image"
        errata_type = "rhsa"
        runtime = MagicMock()

        # Act
        result = get_advisory_boilerplate(runtime, et_data, art_advisory_key, errata_type)

        # Assert
        self.assertEqual(result["synopsis"], "Group advisory rhsa synopsis")
