import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, patch

from pyartcd.plashets import CLEANUP_SCRIPT, build_plashet_from_tags, build_plashets, copy_to_remote, run_cleanup_script

BASE = '/mnt/data/pub/RHOCP/plashets/4.20/stream/el9'


class TestBuildPlashetsRepoArches(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.directory = self.enterContext(TemporaryDirectory())
        self.arches = ['s390x', 'aarch64', 'ppc64le', 'x86_64']
        self.group_config = {
            'arches': self.arches,
            'assemblies': {'enabled': True},
            'vars': {'MAJOR': 5, 'MINOR': 1},
            'all_repos': [],
            'plashet': {
                'base_dir': '$slug',
                'plashet_dir': '$revision',
                'create_symlinks': True,
                'create_repo_subdirs': True,
                'repo_subdir': 'os',
            },
        }
        self.enterContext(patch('pyartcd.plashets.working_dir', self.directory))
        self.enterContext(patch('pyartcd.plashets.PLASHET_REMOTES', [{'host': 'example.com'}]))
        self.enterContext(patch('pyartcd.plashets.util.load_group_config', AsyncMock(return_value=self.group_config)))
        self.enterContext(patch('pyartcd.plashets.util.get_signing_mode', AsyncMock(return_value='unsigned')))
        self.compose = self.enterContext(
            patch('pyartcd.plashets.build_plashet_from_tags', wraps=build_plashet_from_tags)
        )
        self.command = self.enterContext(patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock))
        self.copy = self.enterContext(patch('pyartcd.plashets.copy_to_remote', new_callable=AsyncMock))
        self.symlink = self.enterContext(patch('pyartcd.plashets.create_latest_symlink'))

    def add_repo(self, name, arches=None):
        self.group_config['all_repos'].append(
            {
                'name': name,
                'type': 'plashet',
                'plashet': {
                    'arches': arches,
                    'source': {
                        'type': 'brew',
                        'from_tags': [{'name': 'test-candidate', 'product_version': 'TEST-5.1'}],
                    },
                },
            }
        )

    async def build(self):
        return await build_plashets(group='openshift-5.1', release='20261028080000')

    async def test_four_arch_group_two_arch_repo_and_compose_command(self):
        """Exercise the real command builder with external compose effects mocked."""
        self.add_repo('restricted', ['x86_64', 'aarch64'])

        result = await self.build()

        self.assertEqual(self.compose.await_args.kwargs['arches'], ['aarch64', 'x86_64'])
        command = self.command.await_args.args[0]
        arch_arguments = [command[index + 1 : index + 3] for index, value in enumerate(command) if value == '--arch']
        self.assertEqual(arch_arguments, [['aarch64', 'unsigned'], ['x86_64', 'unsigned']])
        self.assertEqual(
            command[command.index('from-tags') :],
            ['from-tags', '--inherit', '--brew-tag', 'test-candidate', 'TEST-5.1'],
        )
        self.assertEqual(command[command.index('--repo-subdir') + 1], 'os')
        self.assertEqual(list(result), ['restricted'])
        self.copy.assert_awaited_once()
        self.symlink.assert_called_once()

    async def test_no_repo_override_preserves_effective_arches(self):
        for override in ['omitted', None, []]:
            with self.subTest(override=override):
                self.group_config['all_repos'] = []
                self.add_repo('unrestricted', None if override == 'omitted' else override)
                if override == 'omitted':
                    del self.group_config['all_repos'][0]['plashet']['arches']

                await self.build()

                self.assertEqual(self.compose.await_args.kwargs['arches'], self.arches)

    async def test_narrower_global_request_is_not_expanded(self):
        self.group_config['plashet']['arches'] = ['x86_64']
        self.add_repo('restricted', ['aarch64', 'x86_64'])

        await self.build()

        self.assertEqual(self.compose.await_args.kwargs['arches'], ['x86_64'])
        self.assertEqual(self.group_config['plashet']['arches'], ['x86_64'])

    async def test_no_repo_override_preserves_global_request(self):
        self.group_config['plashet']['arches'] = ['ppc64le', 'aarch64']
        for override in ['omitted', None, []]:
            with self.subTest(override=override):
                self.group_config['all_repos'] = []
                self.add_repo('unrestricted', None if override == 'omitted' else override)
                if override == 'omitted':
                    del self.group_config['all_repos'][0]['plashet']['arches']

                await self.build()

                self.assertEqual(self.compose.await_args.kwargs['arches'], ['ppc64le', 'aarch64'])

    async def test_multiple_repos_are_filtered_independently(self):
        self.add_repo('restricted', ['x86_64', 'aarch64'])
        self.add_repo('other', ['ppc64le', 's390x'])
        self.add_repo('unrestricted')

        result = await self.build()

        self.assertEqual(
            [call.kwargs['arches'] for call in self.compose.await_args_list],
            [['aarch64', 'x86_64'], ['s390x', 'ppc64le'], self.arches],
        )
        self.assertEqual(self.arches, ['s390x', 'aarch64', 'ppc64le', 'x86_64'])
        self.assertEqual(list(result), ['restricted', 'other', 'unrestricted'])

    async def test_disjoint_arches_skip_all_repo_side_effects(self):
        """An empty intersection must not compose, copy or update a symlink."""
        self.group_config['plashet']['arches'] = ['ppc64le']
        self.add_repo('restricted', ['x86_64', 'aarch64'])

        with self.assertLogs('pyartcd', level='INFO') as logs:
            result = await self.build()

        self.assertTrue(
            any('Skipping plashet repo restricted: no applicable architectures' in line for line in logs.output)
        )
        self.assertEqual(result, {})
        self.compose.assert_not_awaited()
        self.command.assert_not_awaited()
        self.copy.assert_not_awaited()
        self.symlink.assert_not_called()

    async def test_disjoint_repo_does_not_skip_other_repos(self):
        self.group_config['plashet']['arches'] = ['ppc64le']
        self.add_repo('restricted', ['x86_64', 'aarch64'])
        self.add_repo('unrestricted')

        result = await self.build()

        self.assertEqual(list(result), ['unrestricted'])
        self.compose.assert_awaited_once()
        self.assertEqual(self.compose.await_args.kwargs['arches'], ['ppc64le'])
        self.copy.assert_awaited_once()
        self.symlink.assert_called_once()

    async def test_empty_global_and_repo_arches_fall_back_to_group(self):
        self.group_config['plashet']['arches'] = []
        self.add_repo('unrestricted', [])

        result = await self.build()

        self.assertEqual(self.compose.await_args.kwargs['arches'], self.arches)
        self.assertEqual(list(result), ['unrestricted'])
        self.copy.assert_awaited_once()
        self.symlink.assert_called_once()


class TestCleanupScript(unittest.TestCase):
    def test_script_exists(self):
        assert CLEANUP_SCRIPT.exists(), f"Expected script at {CLEANUP_SCRIPT}"

    def test_script_has_shebang(self):
        content = CLEANUP_SCRIPT.read_text()
        assert content.startswith('#!/bin/bash')


class TestRunCleanupScript(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.plashets.asyncio.subprocess.create_subprocess_exec', new_callable=AsyncMock)
    async def test_passes_args_via_stdin(self, mock_exec):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b'', b''))
        mock_proc.returncode = 0
        mock_exec.return_value = mock_proc

        await run_cleanup_script('remote-host', Path(BASE), keep=3, dry_run=False)

        cmd = mock_exec.call_args[0]
        assert cmd[:4] == ('ssh', 'remote-host', '--', 'bash')
        assert '-s' in cmd
        assert BASE in cmd
        assert '3' in cmd
        assert '--dry-run' not in cmd

        # Script content was piped via stdin
        script_input = mock_proc.communicate.call_args[1]['input']
        assert b'#!/bin/bash' in script_input

    @patch('pyartcd.plashets.asyncio.subprocess.create_subprocess_exec', new_callable=AsyncMock)
    async def test_dry_run_passes_flag(self, mock_exec):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b'', b''))
        mock_proc.returncode = 0
        mock_exec.return_value = mock_proc

        await run_cleanup_script('remote-host', Path(BASE), keep=5, dry_run=True)

        cmd = mock_exec.call_args[0]
        assert '5' in cmd
        assert '--dry-run' in cmd

    @patch('pyartcd.plashets.asyncio.subprocess.create_subprocess_exec', new_callable=AsyncMock)
    async def test_logs_warning_on_failure(self, mock_exec):
        mock_proc = MagicMock()
        mock_proc.communicate = AsyncMock(return_value=(b'', b''))
        mock_proc.returncode = 1
        mock_exec.return_value = mock_proc

        with self.assertLogs('pyartcd', level='WARNING') as cm:
            await run_cleanup_script('remote-host', Path(BASE), keep=3, dry_run=False)
        assert any('Cleanup script failed' in msg for msg in cm.output)


class TestCopyToRemoteCleanup(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.plashets.run_cleanup_script', new_callable=AsyncMock)
    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_cleanup_runs_after_rsync(self, mock_cmd, mock_cleanup):
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path(BASE),
            dry_run=False,
        )
        # mkdir + rsync
        assert mock_cmd.await_count == 2
        # cleanup called with correct args
        mock_cleanup.assert_awaited_once_with('remote-host', Path(BASE), keep=3, dry_run=False)

    @patch('pyartcd.plashets.run_cleanup_script', new_callable=AsyncMock)
    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_dry_run_passes_through(self, mock_cmd, mock_cleanup):
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path(BASE),
            dry_run=True,
        )
        # mkdir + rsync skipped in dry_run
        mock_cmd.assert_not_awaited()
        # cleanup still called with dry_run=True
        mock_cleanup.assert_awaited_once_with('remote-host', Path(BASE), keep=3, dry_run=True)
