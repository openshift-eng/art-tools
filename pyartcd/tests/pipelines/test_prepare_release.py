import tempfile
from pathlib import Path
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch

from pyartcd.pipelines.prepare_release import PrepareReleasePipeline
from pyartcd.runtime import Runtime
from pyartcd.slack import SlackClient


class TestPrepareReleasePipeline(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = Mock(spec=Runtime)
        self.runtime.working_dir = Path("/tmp/working-dir")
        self.runtime.dry_run = False
        self.runtime.config = {
            "build_config": {
                "ocp_build_data_url": "https://build.url/repo",
                "ocp_build_data_repo_push_url": "https://build.push.url/repo",
            },
            "advisory": {"package_owner": "test@example.com"},
            "jira": {"url": "https://jira.example.com"},
        }
        self.mock_slack_client = Mock(spec=SlackClient)
        self.group = "openshift-4.18"
        self.assembly = "test-assembly"
        self.jira_token = "jira_token"

    @patch("pyartcd.pipelines.prepare_release.JIRAClient.from_url")
    @patch("pyartcd.pipelines.prepare_release.aiofiles.open")
    @patch("pyartcd.pipelines.prepare_release.exectools.cmd_assert_async")
    @patch("pyartcd.pipelines.prepare_release.yaml")
    async def test_update_build_data_new_advisories(
        self, mock_yaml, mock_cmd_assert, mock_aiofiles_open, mock_jira_client
    ):
        """Test update_build_data for non-stream assembly scenario where advisories get saved and git push'd"""

        # Mock JIRA client to prevent HTTP requests
        mock_jira_client.return_value = Mock()

        # Create pipeline instance for non-stream assembly
        pipeline = PrepareReleasePipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            data_path=None,
            data_gitref=None,
            name=None,
            date=None,
            nightlies=[],
            package_owner=None,
            jira_token=self.jira_token,
        )

        # Mock advisories and jira issue key
        advisories = {"rpm": 12345, "image": 67890}
        jira_issue_key = "ART-1234"

        # Create a temporary directory to simulate the build repo
        with tempfile.TemporaryDirectory() as temp_dir:
            build_repo_dir = Path(temp_dir) / "ocp-build-data-push"
            build_repo_dir.mkdir(parents=True)
            pipeline._build_repo_dir = build_repo_dir

            # Create mock releases.yml content - initially NO advisories exist
            original_releases_config = {
                "releases": {"test-assembly": {"assembly": {"group": {"advisories": {}, "release_jira": "OLD-123"}}}}
            }

            # Mock file operations
            mock_file_handle = AsyncMock()
            mock_file_handle.__aenter__.return_value = mock_file_handle
            mock_file_handle.__aexit__.return_value = None
            mock_aiofiles_open.return_value = mock_file_handle

            # Mock reading the original file
            mock_file_handle.read.return_value = "original releases.yml content"

            # Mock YAML operations
            mock_yaml.load.return_value = original_releases_config
            mock_yaml.dump.return_value = None

            # Mock git commands - simulate changes detected
            mock_cmd_assert.side_effect = [
                None,  # git diff (shows changes)
                None,  # git add .
                1,  # git diff-index --quiet HEAD (returns 1 = changes detected)
                None,  # git commit
                None,  # git push origin
            ]

            with patch.object(pipeline, "clone_build_data"):
                result = await pipeline.update_build_data(advisories, jira_issue_key)

                self.assertTrue(result)

                mock_yaml.dump.assert_called_once()
                updated_config = mock_yaml.dump.call_args[0][0]

                self.assertEqual(
                    updated_config["releases"]["test-assembly"]["assembly"]["group"]["advisories"], advisories
                )
                self.assertEqual(
                    updated_config["releases"]["test-assembly"]["assembly"]["group"]["release_jira"], jira_issue_key
                )

                git_commands = [call[0][0] for call in mock_cmd_assert.call_args_list]

                commit_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "commit" in cmd]
                self.assertEqual(len(commit_commands), 1)
                self.assertIn("Prepare release", commit_commands[0][-1])

                push_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "push" in cmd]
                self.assertEqual(len(push_commands), 1)
                self.assertEqual(push_commands[0][-1], self.group)

    @patch("pyartcd.pipelines.prepare_release.JIRAClient.from_url")
    @patch("pyartcd.pipelines.prepare_release.aiofiles.open")
    @patch("pyartcd.pipelines.prepare_release.exectools.cmd_assert_async")
    @patch("pyartcd.pipelines.prepare_release.yaml")
    async def test_update_build_data_blank_advisories(
        self, mock_yaml, mock_cmd_assert, mock_aiofiles_open, mock_jira_client
    ):
        """Test update_build_data when advisories exist (template values) and get updated"""

        # Mock JIRA client to prevent HTTP requests
        mock_jira_client.return_value = Mock()

        # Create pipeline instance for non-stream assembly
        pipeline = PrepareReleasePipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            data_path=None,
            data_gitref=None,
            name=None,
            date=None,
            nightlies=[],
            package_owner=None,
            jira_token=self.jira_token,
        )

        # New advisories to update
        new_advisories = {"rpm": 99999, "image": 88888}
        jira_issue_key = "ART-5678"

        # Create a temporary directory to simulate the build repo
        with tempfile.TemporaryDirectory() as temp_dir:
            build_repo_dir = Path(temp_dir) / "ocp-build-data-push"
            build_repo_dir.mkdir(parents=True)
            pipeline._build_repo_dir = build_repo_dir

            # Create mock releases.yml content - advisories already exist
            original_releases_config = {
                "releases": {
                    "test-assembly": {
                        "assembly": {"group": {"advisories": {"rpm": -1, "image": -1}, "release_jira": "ART-5678"}}
                    }
                }
            }

            # Mock file operations
            mock_file_handle = AsyncMock()
            mock_file_handle.__aenter__.return_value = mock_file_handle
            mock_file_handle.__aexit__.return_value = None
            mock_aiofiles_open.return_value = mock_file_handle

            # Mock reading the original file
            mock_file_handle.read.return_value = "original releases.yml content"

            # Mock YAML operations
            mock_yaml.load.return_value = original_releases_config
            mock_yaml.dump.return_value = None

            # Mock git commands - simulate changes detected
            mock_cmd_assert.side_effect = [
                None,  # git diff
                None,  # git add
                1,  # git diff-index (changes detected)
                None,  # git commit
                None,  # git push
            ]

            with patch.object(pipeline, "clone_build_data"):
                result = await pipeline.update_build_data(new_advisories, jira_issue_key)

                self.assertTrue(result)

                mock_yaml.dump.assert_called_once()
                updated_config = mock_yaml.dump.call_args[0][0]

                self.assertEqual(
                    updated_config["releases"]["test-assembly"]["assembly"]["group"]["advisories"], new_advisories
                )
                self.assertEqual(
                    updated_config["releases"]["test-assembly"]["assembly"]["group"]["release_jira"], jira_issue_key
                )

                git_commands = [call[0][0] for call in mock_cmd_assert.call_args_list]

                commit_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "commit" in cmd]
                self.assertEqual(len(commit_commands), 1)

                push_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "push" in cmd]
                self.assertEqual(len(push_commands), 1)

                # Should contain a push command
                push_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "push" in cmd]
                self.assertEqual(len(push_commands), 1)

    @patch("pyartcd.pipelines.prepare_release.JIRAClient.from_url")
    @patch("pyartcd.pipelines.prepare_release.aiofiles.open")
    @patch("pyartcd.pipelines.prepare_release.exectools.cmd_assert_async")
    @patch("pyartcd.pipelines.prepare_release.yaml")
    async def test_update_build_data_no_changes(self, mock_yaml, mock_cmd_assert, mock_aiofiles_open, mock_jira_client):
        """Test update_build_data when no changes are detected (git diff-index returns 0)"""

        # Mock JIRA client to prevent HTTP requests
        mock_jira_client.return_value = Mock()

        # Create pipeline instance for non-stream assembly
        pipeline = PrepareReleasePipeline(
            slack_client=self.mock_slack_client,
            runtime=self.runtime,
            group=self.group,
            assembly=self.assembly,
            data_path=None,
            data_gitref=None,
            name=None,
            date=None,
            nightlies=[],
            package_owner=None,
            jira_token=self.jira_token,
        )

        # Mock advisories and jira issue key
        advisories = {"rpm": 12345, "image": 67890}
        jira_issue_key = "ART-1234"

        # Create a temporary directory to simulate the build repo
        with tempfile.TemporaryDirectory() as temp_dir:
            build_repo_dir = Path(temp_dir) / "ocp-build-data-push"
            build_repo_dir.mkdir(parents=True)
            pipeline._build_repo_dir = build_repo_dir

            # Create mock releases.yml content
            original_releases_config = {
                "releases": {
                    "test-assembly": {
                        "assembly": {
                            "group": {"advisories": {"rpm": 12345, "image": 67890}, "release_jira": "ART-1234"}
                        }
                    }
                }
            }

            # Mock file operations
            mock_file_handle = AsyncMock()
            mock_file_handle.__aenter__.return_value = mock_file_handle
            mock_file_handle.__aexit__.return_value = None
            mock_aiofiles_open.return_value = mock_file_handle

            # Mock reading the original file
            mock_file_handle.read.return_value = "original releases.yml content"

            # Mock YAML operations
            mock_yaml.load.return_value = original_releases_config
            mock_yaml.dump.return_value = None

            # Mock git commands - simulate NO changes detected
            mock_cmd_assert.side_effect = [
                None,  # git diff
                None,  # git add
                0,  # git diff-index (no changes)
            ]

            with patch.object(pipeline, "clone_build_data"):
                result = await pipeline.update_build_data(advisories, jira_issue_key)
                self.assertFalse(result)

                # should not contain any git commit or git push invocations
                git_commands = [call[0][0] for call in mock_cmd_assert.call_args_list]

                commit_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "commit" in cmd]
                self.assertEqual(len(commit_commands), 0)

                push_commands = [cmd for cmd in git_commands if cmd[0] == "git" and "push" in cmd]
                self.assertEqual(len(push_commands), 0)
