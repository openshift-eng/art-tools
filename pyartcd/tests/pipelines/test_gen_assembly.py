import asyncio
from collections import OrderedDict
from pathlib import Path
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import click
from pyartcd.pipelines.gen_assembly import GenAssemblyPipeline, validate_release_date


class TestGenAssemblyPipeline(IsolatedAsyncioTestCase):
    @patch("artcommonlib.exectools.cmd_gather_async", autospec=True, return_value=(0, "a b c", ""))
    def test_get_nightlies(self, cmd_gather_async: AsyncMock):
        runtime = MagicMock()
        pipeline = GenAssemblyPipeline(
            runtime,
            "openshift-4.12",
            "4.12.99",
            "brew",
            "https://example.com/ocp-build-data.git",
            nightlies=(),
            allow_pending=False,
            allow_rejected=False,
            allow_inconsistency=False,
            custom=False,
            arches=(),
            in_flight="4.11.88",
            previous_list=(),
            auto_previous=True,
            auto_trigger_build_sync=False,
            skip_get_nightlies=False,
        )
        actual = asyncio.run(pipeline._get_nightlies())
        self.assertEqual(actual, ["a", "b", "c"])
        cmd_gather_async.assert_awaited_once_with(
            ['doozer', '--group', 'openshift-4.12', '--assembly', 'stream', '--build-system', 'brew', 'get-nightlies'],
            stderr=None,
            env=ANY,
        )

        cmd_gather_async.reset_mock()
        pipeline.allow_pending = True
        pipeline.allow_inconsistency = True
        pipeline.allow_rejected = True
        actual = asyncio.run(pipeline._get_nightlies())
        self.assertEqual(actual, ["a", "b", "c"])
        cmd_gather_async.assert_awaited_once_with(
            [
                'doozer',
                '--group',
                'openshift-4.12',
                '--assembly',
                'stream',
                '--build-system',
                'brew',
                'get-nightlies',
                '--allow-pending',
                '--allow-rejected',
                '--allow-inconsistency',
            ],
            stderr=None,
            env=ANY,
        )

        cmd_gather_async.reset_mock()
        pipeline.arches = ("x86_64", "aarch64")
        pipeline.custom = True
        pipeline.nightlies = ("n1", "n2")
        actual = asyncio.run(pipeline._get_nightlies())
        self.assertEqual(actual, ["a", "b", "c"])
        cmd_gather_async.assert_awaited_once_with(
            [
                'doozer',
                '--group',
                'openshift-4.12',
                '--assembly',
                'stream',
                '--build-system',
                'brew',
                '--arches',
                'x86_64,aarch64',
                'get-nightlies',
                '--allow-pending',
                '--allow-rejected',
                '--allow-inconsistency',
                '--matching=n1',
                '--matching=n2',
            ],
            stderr=None,
            env=ANY,
        )

    @patch("artcommonlib.exectools.cmd_gather_async", autospec=True)
    def test_gen_assembly_from_releases(self, cmd_gather_async: AsyncMock):
        runtime = MagicMock()
        pipeline = GenAssemblyPipeline(
            runtime,
            "openshift-4.12",
            "4.12.99",
            "brew",
            "https://example.com/ocp-build-data.git",
            nightlies=(),
            allow_pending=False,
            allow_rejected=False,
            allow_inconsistency=False,
            custom=False,
            arches=(),
            in_flight="4.11.88",
            previous_list=(),
            auto_previous=True,
            auto_trigger_build_sync=False,
            skip_get_nightlies=False,
        )
        out = """
releases:
  4.12.99:
    assembly:
      basis:
        brew_event: 123456
        reference_releases:
          aarch64: nightly1
          ppc64le: nightly2
          s390x: nightly3
          x86_64: nightly4
      group:
        advisories:
          extras: -1
          image: -1
          metadata: -1
          rpm: -1
        release_jira: ART-0
        upgrades: 4.11.1,4.11.2,4.11.3,4.11.88
      members:
        images: []
        rpms: []
      rhcos:
        machine-os-content:
          images:
            aarch64: registry.example.com/rhcos@sha256:606487eb3c86e011412820dd52db558e68ac09106e209953d596619c6f6b0287
            ppc64le: registry.example.com/rhcos@sha256:e8a5656fbedd12c1a9e2c8c182c87e0f35546ef6828204f5b00dd7d3859c6c88
            s390x: registry.example.com/rhcos@sha256:16b6c9da7d8b23b57d6378a9de36fd21455147f3e27d5fe5ee8864852b31065a
            x86_64: registry.example.com/rhcos@sha256:b2e3b0ef40b7ad82b7e4107c1283baca71397b757d3e429ceefc6b1514e19848
      type: standard
        """.strip()
        cmd_gather_async.return_value = (0, out, "")
        candidate_nightlies = ["nightly1", "nightly2", "nightly3", "nightly4"]
        actual = asyncio.run(pipeline._gen_assembly_from_releases(candidate_nightlies))
        self.assertIn("4.12.99", actual["releases"])

    @patch("pathlib.Path.exists", autospec=True, return_value=True)
    @patch("pyartcd.pipelines.gen_assembly.get_github_client_for_org")
    @patch("pyartcd.pipelines.gen_assembly.yaml")
    @patch("pyartcd.pipelines.gen_assembly.GitRepository", autospec=True)
    def test_create_or_update_pull_request(self, git_repo: MagicMock, yaml: MagicMock, mock_get_client: MagicMock, *_):
        runtime = MagicMock(
            dry_run=False,
            config={
                "build_config": {
                    "ocp_build_data_repo_push_url": "git@github.com:someone/ocp-build-data.git",
                }
            },
        )
        pipeline = GenAssemblyPipeline(
            runtime,
            "openshift-4.12",
            "4.12.99",
            "brew",
            "https://example.com/ocp-build-data.git",
            nightlies=(),
            allow_pending=False,
            allow_rejected=False,
            allow_inconsistency=False,
            custom=False,
            arches=(),
            in_flight="4.11.88",
            previous_list=(),
            auto_previous=True,
            auto_trigger_build_sync=False,
            skip_get_nightlies=False,
        )
        pipeline._working_dir = Path("/path/to/working")
        yaml.load.return_value = OrderedDict(
            [
                (
                    "releases",
                    OrderedDict(
                        [
                            ("4.12.98", OrderedDict()),
                            ("4.12.97", OrderedDict()),
                        ]
                    ),
                ),
            ]
        )
        fn = MagicMock(
            return_value=OrderedDict(
                [
                    (
                        "releases",
                        OrderedDict(
                            [
                                ("4.12.99", OrderedDict()),
                                ("4.12.98", OrderedDict()),
                                ("4.12.97", OrderedDict()),
                            ]
                        ),
                    ),
                ]
            )
        )
        git_repo.return_value.commit_push.return_value = True
        gh_repo = mock_get_client.return_value.get_repo.return_value
        gh_repo.get_pulls.return_value = []
        gh_repo.create_pull.return_value = MagicMock(
            html_url="https://github.example.com/foo/bar/pull/1234", number=1234
        )
        actual = asyncio.run(pipeline._create_or_update_pull_request(fn))
        self.assertEqual(actual.number, 1234)
        git_repo.return_value.setup.assert_awaited_once_with("git@github.com:someone/ocp-build-data.git")
        git_repo.return_value.fetch_switch_branch.assert_awaited_once_with(
            'auto-gen-assembly-openshift-4.12-4.12.99', 'openshift-4.12'
        )
        yaml.load.assert_called_once_with(pipeline._working_dir / 'ocp-build-data-push/releases.yml')
        git_repo.return_value.commit_push.assert_awaited_once_with(ANY)
        gh_repo.create_pull.assert_called_once_with(
            head='someone:auto-gen-assembly-openshift-4.12-4.12.99',
            base='openshift-4.12',
            title='Add assembly 4.12.99',
            body=ANY,
            maintainer_can_modify=True,
        )

    @patch.dict("os.environ", {"QUAY_AUTH_FILE": "/tmp/auth.json"})
    @patch("pyartcd.pipelines.gen_assembly.RegistryConfig")
    @patch("pyartcd.pipelines.gen_assembly.GenAssemblyPipeline._get_latest_accepted_nightly", return_value="nightly2")
    @patch(
        "pyartcd.pipelines.gen_assembly.GenAssemblyPipeline._create_or_update_pull_request",
        autospec=True,
        return_value=MagicMock(html_url="https://github.example.com/foo/bar/pull/1234", number=1234),
    )
    @patch("pyartcd.pipelines.gen_assembly.GenAssemblyPipeline._gen_assembly_from_releases", autospec=True)
    @patch("pyartcd.pipelines.gen_assembly.GenAssemblyPipeline._get_nightlies", autospec=True)
    async def test_run(
        self,
        get_nightlies: AsyncMock,
        _gen_assembly_from_releases: AsyncMock,
        _create_or_update_pull_request: AsyncMock,
        _get_latest_accepted_nightly: AsyncMock,
        mock_registry_config: MagicMock,
    ):
        # Mock RegistryConfig context manager
        mock_registry_config.return_value.__enter__.return_value = "/tmp/global_auth.json"
        mock_registry_config.return_value.__exit__.return_value = None

        runtime = MagicMock(
            dry_run=False,
            config={
                "build_config": {
                    "ocp_build_data_repo_push_url": "git@github.com:someone/ocp-build-data.git",
                }
            },
        )
        runtime.new_slack_client.return_value = AsyncMock()
        runtime.new_slack_client.return_value.say.return_value = {'message': {'ts': ''}}
        runtime.new_slack_client.return_value.bind_channel = MagicMock()

        pipeline = GenAssemblyPipeline(
            runtime,
            "openshift-4.12",
            "4.12.99",
            "brew",
            "https://example.com/ocp-build-data.git",
            nightlies=(),
            allow_pending=False,
            allow_rejected=False,
            allow_inconsistency=False,
            custom=False,
            arches=(),
            in_flight="4.11.88",
            previous_list=(),
            auto_previous=True,
            auto_trigger_build_sync=False,
            skip_get_nightlies=False,
        )
        pipeline._working_dir = Path("/path/to/working")
        get_nightlies.return_value = ["nightly1", "nightly2", "nightly3", "nightly4"]
        _gen_assembly_from_releases.return_value = OrderedDict(
            [
                ("releases", OrderedDict([("4.12.99", OrderedDict())])),
            ]
        )
        await pipeline.run()
        get_nightlies.assert_awaited_once_with(pipeline)
        _gen_assembly_from_releases.assert_awaited_once_with(pipeline, ['nightly1', 'nightly2', 'nightly3', 'nightly4'])
        _create_or_update_pull_request.assert_awaited_once_with(pipeline, ANY)


class TestValidateReleaseDate(TestCase):
    """
    Test the validate_release_date callback function.
    """

    def test_validate_release_date_elliott_format(self):
        """
        Test that dates in elliott format (YYYY-Mon-DD) are accepted as-is.
        """
        result = validate_release_date(None, None, "2026-Mar-31")
        self.assertEqual(result, "2026-Mar-31")

        result = validate_release_date(None, None, "2020-Nov-25")
        self.assertEqual(result, "2020-Nov-25")

    def test_validate_release_date_numeric_format(self):
        """
        Test that dates in numeric format (YYYY-MM-DD) are converted to elliott format.
        """
        result = validate_release_date(None, None, "2026-03-31")
        self.assertEqual(result, "2026-Mar-31")

        result = validate_release_date(None, None, "2020-11-25")
        self.assertEqual(result, "2020-Nov-25")

    def test_validate_release_date_none(self):
        """
        Test that None is returned when no date is provided.
        """
        result = validate_release_date(None, None, None)
        self.assertIsNone(result)

    def test_validate_release_date_invalid_format(self):
        """
        Test that invalid date formats raise BadParameter.
        """
        with self.assertRaises(click.BadParameter) as context:
            validate_release_date(None, None, "2026/03/31")

        self.assertIn("must be in YYYY-Mon-DD format", str(context.exception))

        with self.assertRaises(click.BadParameter) as context:
            validate_release_date(None, None, "invalid-date")

        self.assertIn("must be in YYYY-Mon-DD format", str(context.exception))
