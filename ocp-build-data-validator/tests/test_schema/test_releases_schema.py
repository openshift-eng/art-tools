import unittest

from validator.schema import releases_schema


class TestReleasesSchema(unittest.TestCase):
    def test_art_yaml_overrides_switch_is_group_yml_only(self):
        for key in ('art-yaml-overrides-enabled', 'art-yaml-overrides-enabled!'):
            with self.subTest(key=key):
                data = {'releases': {'stream': {'assembly': {'group': {key: True}}}}}
                self.assertIn('art-yaml-overrides-enabled', releases_schema.validate('releases.yml', data))

    def test_validate_assembly_visibility(self):
        assembly = {"type": "custom", "visibility": "private"}
        data = {"releases": {"art1": {"assembly": assembly}}}
        self.assertIsNone(releases_schema.validate("releases.yml", data))
        assembly["visibility"] = "public"
        self.assertIsNotNone(releases_schema.validate("releases.yml", data))

    def test_private_visibility_rejects_non_custom_assembly_types(self):
        for assembly_type in ("stream", "standard", "candidate", "preview"):
            with self.subTest(assembly_type=assembly_type):
                assembly = {"type": assembly_type}
                data = {"releases": {"art1": {"assembly": assembly}}}
                self.assertIsNone(releases_schema.validate("releases.yml", data))

                assembly["visibility"] = "private"
                error = releases_schema.validate("releases.yml", data)
                self.assertIsNotNone(error)
                self.assertIn("$.releases.art1.assembly.type", error)

    def test_private_visibility_allows_inherited_custom_assembly_type(self):
        data = {
            "releases": {
                "art1": {"assembly": {"type": "custom"}},
                "art2": {"assembly": {"basis": {"assembly": "art1"}, "visibility": "private"}},
            }
        }
        self.assertIsNone(releases_schema.validate("releases.yml", data))

    def test_validate_with_valid_use_source_fallback_branch_in_assembly_group_override(self):
        for value in ("yes", "always", "never"):
            with self.subTest(value=value):
                valid_data = {
                    "releases": {
                        "art2026103002": {
                            "assembly": {
                                "group": {
                                    "use_source_fallback_branch": value,
                                },
                            },
                        },
                    },
                }
                self.assertIsNone(releases_schema.validate("releases.yml", valid_data))

    def test_validate_with_invalid_use_source_fallback_branch_in_assembly_group_override(self):
        invalid_data = {
            "releases": {
                "art2026103002": {
                    "assembly": {
                        "group": {
                            "use_source_fallback_branch": "sometimes",
                        },
                    },
                },
            },
        }
        error = releases_schema.validate("releases.yml", invalid_data)
        self.assertIsNotNone(error)
        self.assertIn("'sometimes' is not one of", error)

    def test_validate_with_valid_url_pull_in_assembly_member_image(self):
        """
        url_pull must remain accepted here: releases_schema.validate() (used for releases.yml's
        assembly-scoped overrides) does not go through image_schema.validate()'s Python-level
        rejection of url_pull, which is deliberately scoped to images/*.yml only. See
        test_image_schema.py's test_validate_rejects_content_source_git_url_pull* tests for the
        restriction this proves is not regressed here.
        """
        valid_data = {
            "releases": {
                "art2026103002": {
                    "assembly": {
                        "members": {
                            "images": [
                                {
                                    "distgit_key": "my-image",
                                    "why": "catalog repair",
                                    "metadata": {
                                        "content": {
                                            "source": {
                                                "git": {
                                                    "url_pull": "https://github.com/some-fork/my-image",
                                                },
                                            },
                                        },
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        }
        self.assertIsNone(releases_schema.validate("releases.yml", valid_data))

    def test_validate_with_invalid_url_pull_type_in_assembly_member_image(self):
        invalid_data = {
            "releases": {
                "art2026103002": {
                    "assembly": {
                        "members": {
                            "images": [
                                {
                                    "distgit_key": "my-image",
                                    "why": "catalog repair",
                                    "metadata": {
                                        "content": {
                                            "source": {
                                                "git": {
                                                    "url_pull": 123,
                                                },
                                            },
                                        },
                                    },
                                },
                            ],
                        },
                    },
                },
            },
        }
        error = releases_schema.validate("releases.yml", invalid_data)
        self.assertIsNotNone(error)
        self.assertIn("123 is not of type 'string'", error)
