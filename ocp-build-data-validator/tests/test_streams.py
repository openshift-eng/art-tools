import unittest

from validator import streams


class TestStreams(unittest.TestCase):
    def test_no_aliases_no_collision(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
            },
            "rhel": {
                "image": "openshift/ose-base:ubi8",
            },
        }
        err = streams.validate(data)
        self.assertIsNone(err)

    def test_aliases_no_collision(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
                "aliases": ["go-toolset"],
            },
            "rhel": {
                "image": "openshift/ose-base:ubi8",
                "aliases": ["base-rhel"],
            },
        }
        err = streams.validate(data)
        self.assertIsNone(err)

    def test_alias_collides_with_stream_name(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
                "aliases": ["rhel"],
            },
            "rhel": {
                "image": "openshift/ose-base:ubi8",
            },
        }
        err = streams.validate(data)
        self.assertIsNotNone(err)
        self.assertIn("Alias 'rhel' in stream 'golang' collides with a top-level stream name", err)

    def test_alias_collides_with_own_stream_name(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
                "aliases": ["golang"],
            },
        }
        err = streams.validate(data)
        self.assertIsNotNone(err)
        self.assertIn("Alias 'golang' in stream 'golang' collides with a top-level stream name", err)

    def test_duplicate_alias_across_streams(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
                "aliases": ["go-toolset"],
            },
            "golang-alt": {
                "image": "openshift/golang-builder:v2.0",
                "aliases": ["go-toolset"],
            },
        }
        err = streams.validate(data)
        self.assertIsNotNone(err)
        self.assertIn("Alias 'go-toolset' is defined in both stream 'golang' and stream 'golang-alt'", err)

    def test_multiple_collisions_reported(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
                "aliases": ["rhel", "shared-alias"],
            },
            "rhel": {
                "image": "openshift/ose-base:ubi8",
                "aliases": ["shared-alias"],
            },
        }
        err = streams.validate(data)
        self.assertIsNotNone(err)
        self.assertIn("Alias 'rhel' in stream 'golang' collides with a top-level stream name", err)
        self.assertIn("Alias 'shared-alias' is defined in both stream 'golang' and stream 'rhel'", err)

    def test_empty_aliases_list_no_collision(self):
        data = {
            "golang": {
                "image": "openshift/golang-builder:v1.0",
                "aliases": [],
            },
            "rhel": {
                "image": "openshift/ose-base:ubi8",
            },
        }
        err = streams.validate(data)
        self.assertIsNone(err)
