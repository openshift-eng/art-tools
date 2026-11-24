import unittest
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import MagicMock

import yaml
from doozerlib.exceptions import DoozerFatalError
from doozerlib.rpmcfg import RPMMetadata
from doozerlib.source_resolver import SourceResolution


class TestRPMMetadata(unittest.TestCase):
    FOO_RPM_CONFIG = """
content:
  source:
    git:
      branch:
        target: release-4.7
      url: git@github.com:openshift/foo.git
    specfile: foo.spec
name: foo
owners:
- aos-master@redhat.com
distgit:
  branch: rhaos-4.7-rhel-7
targets:
- rhaos-4.7-rhel-7-candidate
- rhaos-4.7-rhel-8-candidate
    """

    def test_clone_source_detects_private_fix_from_fresh_resolution(self):
        runtime = mock.MagicMock(build_system="konflux", assembly_basis_event=None)
        data_obj = mock.MagicMock(
            key="foo",
            filename="foo.yml",
            path="/path/to/ocp-build-data/rpms/foo.yml",
            data=yaml.safe_load(self.FOO_RPM_CONFIG),
        )
        metadata = RPMMetadata(runtime, data_obj, clone_source=False)
        self.assertIsNone(metadata.public_upstream_branch)

        source_commit = "a" * 40
        source_resolution = SourceResolution(
            source_path="/path/to/sources/foo",
            url="git@github.com:openshift-priv/foo.git",
            branch="release-4.7",
            https_url="https://github.com/openshift-priv/foo",
            commit_hash=source_commit,
            committer_date=datetime.fromtimestamp(0, timezone.utc),
            latest_tag="v4.7.0",
            has_public_upstream=True,
            public_upstream_url="https://github.com/openshift/foo",
            public_upstream_branch="release-4.7",
        )
        runtime.source_resolver.resolve_source.return_value = source_resolution

        with (
            mock.patch("doozerlib.rpmcfg.Dir"),
            mock.patch("doozerlib.rpmcfg.exectools.cmd_assert", return_value=(source_commit, "")),
            mock.patch("doozerlib.rpmcfg.os.path.isfile", return_value=True),
            mock.patch.object(metadata, "extract_kube_env_vars", return_value={}),
            mock.patch("doozerlib.rpmcfg.util.is_commit_in_public_upstream", return_value=False) as is_public,
        ):
            metadata.clone_source()

        self.assertTrue(metadata.private_fix)
        self.assertEqual(metadata.public_upstream_url, source_resolution.public_upstream_url)
        self.assertEqual(metadata.public_upstream_branch, source_resolution.public_upstream_branch)
        is_public.assert_called_once_with(source_commit, "release-4.7", source_resolution.source_path)

    def test_clone_source_rejects_unchecked_fork_and_detached_source(self):
        for branch, pull_url, error in (
            ("release-4.7", "https://github.com/test/fork", "built from a fork"),
            ("HEAD", None, "without a configured public_branch"),
        ):
            with self.subTest(branch=branch, pull_url=pull_url):
                runtime = mock.MagicMock(build_system="konflux", assembly_basis_event=None)
                runtime.group_config.public_upstreams = [
                    {"private": "https://github.com/openshift-priv", "public": "https://github.com/openshift"}
                ]
                data_obj = mock.MagicMock(
                    key="foo",
                    filename="foo.yml",
                    path="/path/to/ocp-build-data/rpms/foo.yml",
                    data=yaml.safe_load(self.FOO_RPM_CONFIG),
                )
                metadata = RPMMetadata(runtime, data_obj, clone_source=False)
                runtime.source_resolver.resolve_source.return_value = SourceResolution(
                    source_path="/path/to/sources/foo",
                    url="git@github.com:openshift-priv/foo.git",
                    branch=branch,
                    https_url="https://github.com/openshift-priv/foo",
                    commit_hash="a" * 40,
                    committer_date=datetime.fromtimestamp(0, timezone.utc),
                    latest_tag="v4.7.0",
                    has_public_upstream=branch != "HEAD",
                    public_upstream_url="https://github.com/openshift/foo",
                    public_upstream_branch=branch,
                    pull_url=pull_url,
                )

                with (
                    mock.patch("doozerlib.rpmcfg.Dir"),
                    mock.patch("doozerlib.rpmcfg.exectools.cmd_assert", return_value=("a" * 40, "")),
                    mock.patch("doozerlib.rpmcfg.util.is_commit_in_public_upstream") as is_public,
                    self.assertRaisesRegex(DoozerFatalError, error),
                ):
                    metadata.clone_source()
                is_public.assert_not_called()

    def test_clone_source_checks_configured_public_branch_for_detached_source(self):
        runtime = mock.MagicMock(build_system="konflux", assembly_basis_event=None)
        runtime.group_config.public_upstreams = [
            {
                "private": "https://github.com/openshift-priv",
                "public": "https://github.com/openshift",
                "public_branch": "release-4.7",
            }
        ]
        data_obj = mock.MagicMock(
            key="foo",
            filename="foo.yml",
            path="/path/to/ocp-build-data/rpms/foo.yml",
            data=yaml.safe_load(self.FOO_RPM_CONFIG),
        )
        metadata = RPMMetadata(runtime, data_obj, clone_source=False)
        source_commit = "a" * 40
        runtime.source_resolver.resolve_source.return_value = SourceResolution(
            source_path="/path/to/sources/foo",
            url="git@github.com:openshift-priv/foo.git",
            branch="HEAD",
            https_url="https://github.com/openshift-priv/foo",
            commit_hash=source_commit,
            committer_date=datetime.fromtimestamp(0, timezone.utc),
            latest_tag="v4.7.0",
            has_public_upstream=False,
            public_upstream_url="https://github.com/openshift-priv/foo",
            public_upstream_branch="HEAD",
        )

        with (
            mock.patch("doozerlib.rpmcfg.Dir"),
            mock.patch("doozerlib.rpmcfg.exectools.cmd_assert", return_value=(source_commit, "")),
            mock.patch("doozerlib.rpmcfg.os.path.isfile", return_value=True),
            mock.patch.object(metadata, "extract_kube_env_vars", return_value={}),
            mock.patch("doozerlib.rpmcfg.util.is_commit_in_public_upstream", return_value=False) as is_public,
        ):
            metadata.clone_source()

        self.assertTrue(metadata.private_fix)
        self.assertEqual(metadata.public_upstream_url, "https://github.com/openshift/foo")
        self.assertEqual(metadata.public_upstream_branch, "release-4.7")
        is_public.assert_called_once_with(source_commit, "release-4.7", "/path/to/sources/foo")

    @mock.patch("artcommonlib.logutil.EntityLoggingAdapter")
    @mock.patch("doozerlib.rpmcfg.Dir")
    def test_assert_golang_versions(self, *_):
        runtime = mock.MagicMock(brew_logs_dir="/path/to/brew/logs", build_system="brew")
        koji_session = runtime.build_retrying_koji_client.return_value
        data_obj = mock.MagicMock(
            key="foo",
            filename="foo.yml",
            path="/path/to/ocp-build-data/rpms/foo.yml",
            data=yaml.safe_load(TestRPMMetadata.FOO_RPM_CONFIG),
        )
        koji_session.multicall.return_value.__enter__.return_value.getBuildTarget.side_effect = lambda target: (
            MagicMock(result={"build_tag_name": target.replace("-candidate", "-build")})
        )
        metadata = RPMMetadata(runtime, data_obj, clone_source=False)
        metadata.targets = ["rhaos-4.7-rhel-7-candidate", "rhaos-4.7-rhel-8-candidate"]

        runtime.group_config.check_golang_versions = "exact"
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:

            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [
                        {"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [
                        {"name": "golang-scl-shim", "version": "1.4.0", "release": "2.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [
                        {"name": "golang", "version": "1.4.5", "release": "3.el8", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]

            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [
                {"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}
            ]
            metadata.assert_golang_versions()
            koji_session.getLatestBuilds.assert_called_with(
                'rhaos-4.7-rhel-7-build', package="go-toolset-1.4", type="rpm"
            )

        runtime.group_config.check_golang_versions = "exact"
        RPMMetadata.target_golangs = {}
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:

            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [
                        {"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [
                        {"name": "golang-scl-shim", "version": "1.4.6", "release": "2.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [
                        {"name": "golang", "version": "1.4.6", "release": "3.el8", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]

            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [
                {"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}
            ]
            with self.assertRaises(DoozerFatalError):
                metadata.assert_golang_versions()

        runtime.group_config.check_golang_versions = "x.y"
        RPMMetadata.target_golangs = {}
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:

            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [
                        {"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [
                        {"name": "golang-scl-shim", "version": "1.4.5", "release": "2.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [
                        {"name": "golang", "version": "1.4.6", "release": "3.el8", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]

            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [
                {"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}
            ]
            metadata.assert_golang_versions()

        runtime.group_config.check_golang_versions = "x.y"
        RPMMetadata.target_golangs = {}
        with mock.patch("doozerlib.rpmcfg.brew.get_latest_builds") as get_latest_builds:

            def fake_get_latest_builds(tag_component_tuples, build_type, event, session):
                results = {
                    ('rhaos-4.7-rhel-7-build', 'golang'): [
                        {"name": "golang", "version": "1.2.3", "release": "1.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-7-build', 'golang-scl-shim'): [
                        {"name": "golang-scl-shim", "version": "1.4.5", "release": "2.el7", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang'): [
                        {"name": "golang", "version": "1.5.6", "release": "3.el8", "epoch": None}
                    ],
                    ('rhaos-4.7-rhel-8-build', 'golang-scl-shim'): [],
                }
                return [results[tag_component] for tag_component in tag_component_tuples]

            get_latest_builds.side_effect = fake_get_latest_builds
            koji_session.getLatestBuilds.return_value = [
                {"name": "go-toolset-1.4", "version": "1.4.5", "release": "4.el7", "epoch": None}
            ]
            with self.assertRaises(DoozerFatalError):
                metadata.assert_golang_versions()
