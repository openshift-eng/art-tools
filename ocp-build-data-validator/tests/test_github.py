import unittest

from flexmock import flexmock
from validator import github


class TestGitHub(unittest.TestCase):
    def setUp(self):
        (flexmock(github.support).should_receive("resource_exists").and_return(True))

    def test_no_declared_repository(self):
        (url, err) = github.validate({}, {})
        self.assertIsNone(url)
        self.assertIsNone(err)

    def test_repository_doesnt_exist(self):
        (
            flexmock(github.support)
            .should_receive("resource_exists")
            .with_args("https://github.com/openshift-priv/myrepo")
            .and_return(False)
        )

        data = {
            "content": {
                "source": {
                    "git": {
                        "url": "git@github.com:openshift-priv/myrepo",
                    },
                },
            },
        }

        (url, err) = github.validate(data, {})
        self.assertEqual(err, ("GitHub repository https://github.com/openshift-priv/myrepo doesn't exist"))
        self.assertEqual(url, "https://github.com/openshift-priv/myrepo")

    def test_no_declared_branches(self):
        data = {
            "content": {
                "source": {
                    "git": {
                        "url": "git@github.com:openshift-priv/myrepo",
                    },
                },
            },
        }

        (url, err) = github.validate(data, {})
        self.assertEqual(url, "https://github.com/openshift-priv/myrepo")
        self.assertEqual(err, ("No branches specified under content > source > git"))

    def test_target_branch_doesnt_exist(self):
        (
            flexmock(github)
            .should_receive("branch_exists")
            .with_args("release-4.2", "https://github.com/openshift-priv/myrepo")
            .and_return(False)
        )

        (
            flexmock(github)
            .should_receive("branch_exists")
            .with_args("fallback-branch", "https://github.com/openshift-priv/myrepo")
            .and_return(True)
        )

        data = {
            "content": {
                "source": {
                    "git": {
                        "branch": {
                            "target": "release-{MAJOR}.{MINOR}",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:openshift-priv/myrepo",
                    },
                },
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertEqual(url, "https://github.com/openshift-priv/myrepo")
        self.assertEqual(err, None)

    def test_target_nor_fallback_branches_exist(self):
        (
            flexmock(github)
            .should_receive("branch_exists")
            .with_args("release-4.2", "https://github.com/openshift-priv/myrepo")
            .and_return(False)
        )

        (
            flexmock(github)
            .should_receive("branch_exists")
            .with_args("fallback-branch", "https://github.com/openshift-priv/myrepo")
            .and_return(False)
        )

        data = {
            "content": {
                "source": {
                    "git": {
                        "branch": {
                            "target": "release-{MAJOR}.{MINOR}",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:openshift-priv/myrepo",
                    },
                },
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertEqual(url, "https://github.com/openshift-priv/myrepo")
        self.assertEqual(err, ("At least one of the following branches should exist: release-4.2 or fallback-branch"))

    def test_declared_dockerfile_doesnt_exist(self):
        (
            flexmock(github.support)
            .should_receive("resource_exists")
            .with_args("https://github.com/openshift-priv/repo/blob/xyz/Dockerfile.rhel7")
            .and_return(False)
        )

        data = {
            "content": {
                "source": {
                    "dockerfile": "Dockerfile.rhel7",
                    "git": {
                        "branch": {
                            "target": "xyz",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:openshift-priv/repo",
                    },
                },
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertEqual(url, "https://github.com/openshift-priv/repo")
        self.assertEqual(err, ("dockerfile Dockerfile.rhel7 not found on branch xyz"))

    def test_declared_dockerfile_on_custom_path(self):
        bad_file_url = "https://github.com/org/repo/blob/xyz/Dockerfile.rhel7"
        (flexmock(github.support).should_receive("resource_exists").with_args(bad_file_url).and_return(False))

        good_file_url = "https://github.com/org/repo/blob/xyz/my/custom/path/Dockerfile.rhel7"
        (flexmock(github.support).should_receive("resource_exists").with_args(good_file_url).and_return(True))

        data = {
            "content": {
                "source": {
                    "dockerfile": "Dockerfile.rhel7",
                    "git": {
                        "branch": {
                            "target": "xyz",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:org/repo",
                    },
                    "path": "my/custom/path",
                },
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertEqual(url, "https://github.com/org/repo")
        self.assertIsNone(err)

    def test_declared_manifest_doesnt_exist(self):
        (
            flexmock(github.support)
            .should_receive("resource_exists")
            .with_args("https://github.com/org/repo/blob/xyz/my-manifests")
            .and_return(False)
        )

        data = {
            "content": {
                "source": {
                    "git": {
                        "branch": {
                            "target": "xyz",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:org/repo",
                    },
                },
            },
            "update-csv": {
                "manifests-dir": "my-manifests",
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertEqual(url, "https://github.com/org/repo")
        self.assertEqual(err, "manifests my-manifests not found on branch xyz")

    def test_declared_manifest_on_custom_path(self):
        bad_file_url = "https://github.com/org/repo/blob/xyz/my-manifests"
        (flexmock(github.support).should_receive("resource_exists").with_args(bad_file_url).and_return(False))

        good_file_url = "https://github.com/org/repo/blob/xyz/my/custom/path/my-manifests"
        (flexmock(github.support).should_receive("resource_exists").with_args(good_file_url).and_return(True))

        data = {
            "content": {
                "source": {
                    "git": {
                        "branch": {
                            "target": "xyz",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:org/repo.git",
                    },
                    "path": "my/custom/path",
                },
            },
            "update-csv": {
                "manifests-dir": "my-manifests",
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertIsNone(err)
        self.assertEqual(url, "https://github.com/org/repo")

    def test_translate_private_upstreams_to_public(self):
        data = {
            "content": {
                "source": {
                    "dockerfile": "Dockerfile.rhel7",
                    "git": {
                        "branch": {
                            "target": "xyz",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:openshift-priv/repo",
                    },
                },
            },
        }
        group_cfg = {
            "vars": {"MAJOR": 4, "MINOR": 2},
            "public_upstreams": [
                {
                    "private": "git@github.com:openshift-priv",
                    "public": "git@github.com:openshift",
                },
                {
                    "private": "git@github.com:openshift/ose",
                    "public": "git@github.com:openshift/origin",
                },
            ],
        }
        (url, err) = github.validate(data, group_cfg)
        self.assertIsNone(err)
        self.assertEqual(url, "https://github.com/openshift-priv/repo")

    def test_translate_private_upstreams_to_public_no_match(self):
        data = {
            "content": {
                "source": {
                    "dockerfile": "dockerfile.rhel7",
                    "git": {
                        "branch": {
                            "target": "xyz",
                            "fallback": "fallback-branch",
                        },
                        "url": "git@github.com:org/repo",
                    },
                },
            },
            "update-csv": {
                "manifests-dir": "my-manifests",
            },
        }

        (url, err) = github.validate(data, {"vars": {"MAJOR": 4, "MINOR": 2}})
        self.assertEqual(url, "https://github.com/org/repo")
        self.assertIsNone(err)

    def test_uses_ssh(self):
        def data(url):
            return {
                "content": {
                    "source": {
                        "dockerfile": "dockerfile",
                        "git": {
                            "branch": {
                                "target": "xyz",
                                "fallback": "fallback-branch",
                            },
                            "url": url,
                        },
                    },
                },
            }

        self.assertFalse(github.uses_ssh(data("https://host/org/repo")))
        self.assertTrue(github.uses_ssh(data("git@host:org/repo.git")))

    def test_has_permitted_repo(self):
        def data(repo):
            return {
                "content": {
                    "source": {
                        "dockerfile": "dockerfile",
                        "git": {
                            "branch": {
                                "target": "xyz",
                                "fallback": "fallback-branch",
                            },
                            "url": f"git@github.com:{repo}.git",
                        },
                    },
                },
            }

        self.assertTrue(github.has_permitted_repo(data("openshift-priv/my-repo")))
        self.assertTrue(github.has_permitted_repo(data("openshift-eng/ocp-build-data")))
        self.assertFalse(github.has_permitted_repo(data("cpuguy85/h4ckzor")))


if __name__ == "__main__":
    unittest.main()
