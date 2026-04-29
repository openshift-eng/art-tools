import shlex
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from click.testing import CliRunner
from doozerlib import Runtime
from doozerlib.cli.__main__ import beta_reposync


class TestBetaRepoSyncCli(unittest.TestCase):
    def setUp(self):
        self.runner = CliRunner()

    def test_rebuilds_local_metadata_after_sync(self):
        runtime = Runtime()
        runtime.initialize = MagicMock(return_value=None)
        runtime.working_dir = "workdir"
        runtime._logger = MagicMock()

        repo = MagicMock()
        repo.name = "rhel-9-baseos-rpms"
        repo.cs_optional = False
        repo.is_reposync_enabled.return_value = True
        repo.is_reposync_latest_only.return_value = True

        repos = MagicMock()
        repos.items.return_value = [(repo.name, repo)]
        repos.values.return_value = [repo]
        repos.repo_file.return_value = "[rhel-9-baseos-rpms]\nbaseurl = https://example.com/\n"
        runtime.repos = repos

        def fake_cmd_assert(cmd):
            parts = shlex.split(cmd)
            if parts[:2] == ["mkdir", "-p"]:
                Path(parts[2]).mkdir(parents=True, exist_ok=True)

        def fake_cmd_gather(cmd, realtime=True):
            Path("out").joinpath(repo.name).mkdir(parents=True, exist_ok=True)
            return 0, "", ""

        with self.runner.isolated_filesystem():
            with patch("doozerlib.cli.__main__.exectools.cmd_assert", side_effect=fake_cmd_assert) as cmd_assert:
                with patch("doozerlib.cli.__main__.exectools.cmd_gather", side_effect=fake_cmd_gather) as cmd_gather:
                    result = self.runner.invoke(
                        beta_reposync,
                        [
                            "-o",
                            "out",
                            "-c",
                            "cache",
                        ],
                        obj=runtime,
                    )

        self.assertEqual(result.exit_code, 0, result.output)
        synced_command = cmd_gather.call_args.args[0]
        self.assertIn("--refresh", synced_command)
        self.assertIn("--newest-only", synced_command)
        cmd_assert.assert_any_call("createrepo_c --update --keep-all-metadata out/rhel-9-baseos-rpms")
