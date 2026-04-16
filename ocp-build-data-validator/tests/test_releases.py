import unittest

from flexmock import flexmock
from validator import releases
from validator.schema import releases_schema

group_config = group_cfg = {
    'vars': {'MAJOR': 4, 'MINOR': 18},
}


class TestReleases(unittest.TestCase):
    def setUp(self):
        flexmock(releases.support).should_receive('load_group_config_for').and_return(group_config)

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

    def test_member_exclude_image(self):
        valid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "images": [
                                {"distgit_key": "image-a", "exclude": True, "why": "not needed for this release"},
                                {"distgit_key": "image-c", "why": "pinned build", "metadata": {}},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, valid_releases)
        self.assertIsNone(err)

    def test_member_exclude_rpm(self):
        valid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "rpms": [
                                {"distgit_key": "rpm-a", "exclude": True, "why": "not needed for this release"},
                                {
                                    "distgit_key": "rpm-c",
                                    "why": "pinned build",
                                    "metadata": {"is": {"el9": "rpm-c-1.0-1.el9"}},
                                },
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, valid_releases)
        self.assertIsNone(err)

    def test_member_exclude_false_image_invalid(self):
        invalid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "images": [
                                {"distgit_key": "image-a", "exclude": False, "why": "reason"},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, invalid_releases)
        self.assertIsNotNone(err)

    def test_member_exclude_false_rpm_invalid(self):
        invalid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "rpms": [
                                {"distgit_key": "rpm-a", "exclude": False, "why": "reason"},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, invalid_releases)
        self.assertIsNotNone(err)

    def test_member_image_why_required_with_exclude(self):
        invalid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "images": [
                                {"distgit_key": "image-a", "exclude": True},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, invalid_releases)
        self.assertIsNotNone(err)

    def test_member_rpm_why_required_with_exclude(self):
        invalid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "rpms": [
                                {"distgit_key": "rpm-a", "exclude": True},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, invalid_releases)
        self.assertIsNotNone(err)

    def test_member_image_why_required(self):
        invalid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "images": [
                                {"distgit_key": "image-a"},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, invalid_releases)
        self.assertIsNotNone(err)

    def test_member_rpm_why_required(self):
        invalid_releases = {
            "releases": {
                "4.18.1": {
                    "assembly": {
                        "members": {
                            "rpms": [
                                {"distgit_key": "rpm-a"},
                            ]
                        }
                    }
                }
            }
        }
        err = releases_schema.validate(None, invalid_releases)
        self.assertIsNotNone(err)
