import unittest

from flexmock import flexmock
from validator import releases

group_config = group_cfg = {
    "vars": {"MAJOR": 4, "MINOR": 18},
}


class TestReleases(unittest.TestCase):
    def setUp(self):
        flexmock(releases.support).should_receive("load_group_config_for").and_return(group_config)

    def test_self_referential_upgrades(self):
        invalid_releases = {
            "releases": {
                "4.18.4": {"assembly": {"group": {"upgrades": "4.18.3,4.18.4"}}},
                "ec.1": {"assembly": {"group": {"upgrades": "4.18.0-ec.1, 4.20.0-ec.0"}}},
            }
        }
        err = releases.validate(invalid_releases)
        self.assertEqual(
            err, "The following releases contain references to themselves in their respective 'upgrades': 4.18.4, ec.1"
        )

    def test_no_self_referential_upgrades(self):
        valid_releases = {
            "releases": {
                "rc.0": {"assembly": {"group": {"upgrades": "4.18.0-ec.1"}}},
                "ec.1": {"assembly": {"group": {"upgrades": "4.18.0-ec.0"}}},
            }
        }
        err = releases.validate(valid_releases)
        self.assertIsNone(err)
