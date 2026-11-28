import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from pyartcd.plashets import (
    CLEANUP_SCRIPT,
    build_plashet_from_tags,
    build_plashets,
    copy_to_remote,
    create_plashet_symlink,
    public_sibling_symlink_safe,
    read_detected_as_embargoed,
    run_cleanup_script,
)

BASE = '/mnt/data/pub/RHOCP/plashets/4.20/stream/el9'


def _plashet_source():
    return {
        "type": "brew",
        "from_tags": [
            {
                "name": "rhaos-4.19-rhel-9-candidate",
                "product_version": "OSE-4.19-RHEL-9",
                "release_tag": "rhaos-4.19-rhel-9",
            }
        ],
        "embargoed_tags": ["rhaos-4.19-rhel-9-embargoed"],
    }


def _public_repo():
    return {
        "name": "rhel-9-server-ose-rpms",
        "type": "plashet",
        "plashet": {
            "slug": "el9",
            "include_embargoed": False,
            "include_previous_packages": ["openshift"],
            "source": _plashet_source(),
        },
    }


def _embargoed_repo():
    return {
        "name": "rhel-9-server-ose-rpms-embargoed",
        "type": "plashet",
        "plashet": {
            "slug": "el9-embargoed",
            "include_embargoed": True,
            "include_previous_packages": ["openshift"],
            "source": _plashet_source(),
            "embargoed_sibling": "rhel-9-server-ose-rpms",
            "symlink_when_unembargoed": True,
        },
    }


def _group_config():
    return {
        "assemblies": {"enabled": True},
        "arches": ["x86_64"],
        "vars": {"MAJOR": "4", "MINOR": "19"},
        "signing_advisory": "0",
        "all_repos": [_public_repo(), _embargoed_repo()],
        "plashet": {
            "base_dir": "$MAJOR.$MINOR/$runtime_assembly/$slug",
            "plashet_dir": "$yyyy-$MM/$revision",
            "create_symlinks": True,
            "symlink_name": "latest",
            "create_repo_subdirs": True,
            "repo_subdir": "os",
        },
    }


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


class TestCopyToRemoteRsyncFlags(unittest.IsolatedAsyncioTestCase):
    @patch('pyartcd.plashets.run_cleanup_script', new_callable=AsyncMock)
    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_links_branch_uses_force(self, mock_cmd, _mock_cleanup):
        # Finding (A): rsync cannot replace a non-empty remote directory with a symlink on a retry
        # unless --force is present. The --links branch must therefore include --force.
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path(BASE),
            dry_run=False,
            copy_links=False,
        )
        # Second cmd_assert_async call is the rsync invocation (the first is mkdir).
        rsync_cmd = mock_cmd.await_args_list[1][0][0]
        self.assertIn('--links', rsync_cmd)
        self.assertIn('--force', rsync_cmd)
        self.assertNotIn('--copy-links', rsync_cmd)

    @patch('pyartcd.plashets.run_cleanup_script', new_callable=AsyncMock)
    @patch('pyartcd.plashets.exectools.cmd_assert_async', new_callable=AsyncMock)
    async def test_copy_links_branch_does_not_force(self, mock_cmd, _mock_cleanup):
        # The --copy-links branch dereferences symlinks into real content and must stay unchanged:
        # it must NOT gain --force (and must not use --links).
        await copy_to_remote(
            plashet_remote_host='remote-host',
            local_base_dir=Path('/tmp/plashet-working/4.20/stream/el9'),
            remote_base_dir=Path(BASE),
            dry_run=False,
            copy_links=True,
        )
        rsync_cmd = mock_cmd.await_args_list[1][0][0]
        self.assertIn('--copy-links', rsync_cmd)
        self.assertNotIn('--force', rsync_cmd)
        self.assertNotIn('--links', rsync_cmd)


class TestReadDetectedAsEmbargoed(unittest.TestCase):
    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(read_detected_as_embargoed(d))

    def test_empty_list_when_no_embargo(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'plashet.yml'), 'w') as f:
                yaml.dump({'extra': {'embargo_info': {'detected_as_embargoed': []}}}, f)
            self.assertEqual(read_detected_as_embargoed(d), [])

    def test_returns_nvres_when_embargo_present(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, 'plashet.yml'), 'w') as f:
                yaml.dump({'extra': {'embargo_info': {'detected_as_embargoed': ['foo-1-1.el9']}}}, f)
            self.assertEqual(read_detected_as_embargoed(d), ['foo-1-1.el9'])


class TestCreatePlashetSymlink(unittest.TestCase):
    def test_creates_relative_symlink(self):
        with tempfile.TemporaryDirectory() as d:
            # Public target: <d>/el9/2025-11/<rev>
            public = Path(d, 'el9', '2025-11', 'rev')
            public.mkdir(parents=True)
            (public / 'marker.txt').write_text('hello')

            # Embargoed flavor base dir: <d>/el9-embargoed
            embargoed_base = Path(d, 'el9-embargoed')
            link_path = create_plashet_symlink(embargoed_base, '2025-11/rev', public)

            self.assertTrue(os.path.islink(link_path))
            target = os.readlink(link_path)
            # Must be relative (not absolute) so it survives rsync and resolves on the remote.
            self.assertFalse(os.path.isabs(target))
            self.assertEqual(target, os.path.join('..', '..', 'el9', '2025-11', 'rev'))
            # Resolves to the real public directory contents.
            self.assertTrue((Path(link_path) / 'marker.txt').is_file())

    def test_relative_link_correct_under_symlinked_ancestor(self):
        # Finding (2): rel_target must be built from the RESOLVED locations, not lexical abspaths.
        # When an ancestor directory of the destination is itself a symlink INSIDE base_dir (which
        # still passes the realpath guards), a lexically-computed relative link resolves to the wrong
        # place. Here the destination's parent '2025-11' is a symlink to a deeper real dir under the
        # embargoed base, so the link is physically created one level deeper than its lexical path.
        with tempfile.TemporaryDirectory() as d:
            # Public target: <d>/el9/2025-11/<rev>
            public = Path(d, 'el9', '2025-11', 'rev')
            public.mkdir(parents=True)
            (public / 'marker.txt').write_text('hello')

            # Embargoed flavor base dir with a symlinked ancestor for the destination parent:
            #   <base>/2025-11 -> <base>/sub/real-2025-11  (a real dir one level deeper, in-base).
            embargoed_base = Path(d, 'el9-embargoed')
            (embargoed_base / 'sub' / 'real-2025-11').mkdir(parents=True)
            (embargoed_base / '2025-11').symlink_to(os.path.join('sub', 'real-2025-11'), target_is_directory=True)

            link_path = create_plashet_symlink(embargoed_base, '2025-11/rev', public)

            self.assertTrue(os.path.islink(link_path))
            target = os.readlink(link_path)
            # Still a relative link (not switched to absolute).
            self.assertFalse(os.path.isabs(target))
            # Built from the resolved parent (<base>/sub/real-2025-11), so three levels up, not two.
            self.assertEqual(target, os.path.join('..', '..', '..', 'el9', '2025-11', 'rev'))
            # Critically: the relative link resolves to the intended public target despite the
            # symlinked ancestor. A lexical relpath (../../el9/...) would instead point inside the
            # embargoed base and fail to resolve.
            self.assertEqual(os.path.realpath(link_path), os.path.realpath(public))
            self.assertTrue((Path(link_path) / 'marker.txt').is_file())


class TestBuildPlashetsSymlinkBranch(unittest.IsolatedAsyncioTestCase):
    def _patches(self):
        return (
            patch('pyartcd.plashets.util.load_group_config', new_callable=AsyncMock, return_value=_group_config()),
            patch('pyartcd.plashets.util.get_signing_mode', new_callable=AsyncMock, return_value='signed'),
            patch(
                'pyartcd.plashets.build_plashet_from_tags',
                new_callable=AsyncMock,
                side_effect=lambda **kw: f"/working/{kw['base_dir']}/{kw['name']}",
            ),
            patch('pyartcd.plashets.create_latest_symlink', new=MagicMock(return_value='/working/latest')),
            patch(
                'pyartcd.plashets.create_plashet_symlink', new=MagicMock(return_value='/working/el9-embargoed/symlink')
            ),
            patch('pyartcd.plashets.copy_to_remote', new_callable=AsyncMock),
            patch('pyartcd.plashets.PLASHET_REMOTES', new=[{'host': 'remote-host'}]),
        )

    async def test_symlink_created_when_no_embargo(self):
        patches = self._patches()
        with (
            patches[0],
            patches[1] as _signing,
            patches[2] as build_from_tags,
            patches[3],
            patches[4] as plashet_symlink,
            patches[5],
            patches[6],
            patch('pyartcd.plashets.public_sibling_symlink_safe', new=MagicMock(return_value=True)),
        ):
            result = await build_plashets(group='openshift-4.19', release='202511010101.p?', assembly='stream')

        # Public built once via from-tags; embargoed flavor via symlink (no second full build).
        self.assertEqual(build_from_tags.await_count, 1)
        plashet_symlink.assert_called_once()
        # Both repos recorded as built.
        self.assertIn('rhel-9-server-ose-rpms', result)
        self.assertIn('rhel-9-server-ose-rpms-embargoed', result)

    async def test_full_build_when_embargo_present(self):
        patches = self._patches()
        with (
            patches[0],
            patches[1],
            patches[2] as build_from_tags,
            patches[3],
            patches[4] as plashet_symlink,
            patches[5],
            patches[6],
            patch('pyartcd.plashets.public_sibling_symlink_safe', new=MagicMock(return_value=False)),
        ):
            result = await build_plashets(group='openshift-4.19', release='202511010101.p?', assembly='stream')

        # Embargo signal present -> both flavors are built fully; no symlink.
        self.assertEqual(build_from_tags.await_count, 2)
        plashet_symlink.assert_not_called()
        self.assertIn('rhel-9-server-ose-rpms-embargoed', result)

    async def test_full_build_when_sibling_plashet_yml_unreadable(self):
        # plashet.yml missing/unreadable -> conservative fall back to a full embargoed build.
        patches = self._patches()
        with (
            patches[0],
            patches[1],
            patches[2] as build_from_tags,
            patches[3],
            patches[4] as plashet_symlink,
            patches[5],
            patches[6],
            patch('pyartcd.plashets.public_sibling_symlink_safe', new=MagicMock(return_value=None)),
        ):
            result = await build_plashets(group='openshift-4.19', release='202511010101.p?', assembly='stream')

        self.assertEqual(build_from_tags.await_count, 2)
        plashet_symlink.assert_not_called()
        self.assertIn('rhel-9-server-ose-rpms-embargoed', result)

    async def test_embargoed_only_invocation_falls_back_to_full_build(self):
        # Finding #1: when a `repos` filter selects ONLY the embargoed flavor, its public
        # sibling is not built this run. We must NOT raise; fall back to a full embargoed build.
        patches = self._patches()
        with (
            patches[0],
            patches[1],
            patches[2] as build_from_tags,
            patches[3],
            patches[4] as plashet_symlink,
            patches[5],
            patches[6],
            # Guard: this must not even be consulted when the sibling was not built.
            patch('pyartcd.plashets.public_sibling_symlink_safe', new=MagicMock(return_value=True)) as safe_check,
        ):
            result = await build_plashets(
                group='openshift-4.19',
                release='202511010101.p?',
                assembly='stream',
                repos=['rhel-9-server-ose-rpms-embargoed'],
            )

        # Only the embargoed repo is built, via a full from-tags build (no symlink, no raise).
        self.assertEqual(build_from_tags.await_count, 1)
        plashet_symlink.assert_not_called()
        safe_check.assert_not_called()
        self.assertIn('rhel-9-server-ose-rpms-embargoed', result)
        self.assertNotIn('rhel-9-server-ose-rpms', result)


class TestPublicSiblingSymlinkSafe(unittest.TestCase):
    @staticmethod
    def _write(d, data):
        with open(os.path.join(d, 'plashet.yml'), 'w') as f:
            yaml.dump(data, f)

    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(public_sibling_symlink_safe(d))

    def test_safe_when_empty_and_no_concern(self):
        with tempfile.TemporaryDirectory() as d:
            self._write(
                d,
                {
                    'assemble': {'concerns': []},
                    'extra': {'embargo_info': {'embargoed_permitted': False, 'detected_as_embargoed': []}},
                },
            )
            self.assertIs(public_sibling_symlink_safe(d), True)

    def test_unsafe_when_detected_nonempty(self):
        with tempfile.TemporaryDirectory() as d:
            self._write(
                d,
                {
                    'assemble': {'concerns': []},
                    'extra': {'embargo_info': {'detected_as_embargoed': ['foo-1-1.el9']}},
                },
            )
            self.assertIs(public_sibling_symlink_safe(d), False)

    def test_unsafe_when_concern_mentions_embargo_though_detected_empty(self):
        # Finding #2: include_previous_packages can omit an embargoed historical build and record
        # a concern WITHOUT populating detected_as_embargoed, so an empty list is not sufficient.
        with tempfile.TemporaryDirectory() as d:
            self._write(
                d,
                {
                    'assemble': {
                        'concerns': [
                            'Unable to include previous for openshift because history '
                            'openshift-1-1.el9 is under embargo'
                        ]
                    },
                    'extra': {'embargo_info': {'embargoed_permitted': False, 'detected_as_embargoed': []}},
                },
            )
            self.assertIs(public_sibling_symlink_safe(d), False)

    def test_structural_keys_do_not_trigger_false_match(self):
        # "embargo" in structural KEYS (embargo_info / embargoed_permitted) must not trip the scan;
        # only string VALUES are scanned, otherwise the optimization would never apply.
        with tempfile.TemporaryDirectory() as d:
            self._write(
                d,
                {
                    'assemble': {'concerns': ['some unrelated concern about disk space']},
                    'extra': {'embargo_info': {'embargoed_permitted': False, 'detected_as_embargoed': []}},
                },
            )
            self.assertIs(public_sibling_symlink_safe(d), True)


class TestCreatePlashetSymlinkGuards(unittest.TestCase):
    def test_rejects_destination_equal_to_target(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d, 'el9')
            target = Path(base, '2025-11', 'rev')
            target.mkdir(parents=True)
            with self.assertRaises(ValueError):
                create_plashet_symlink(base, '2025-11/rev', target)
            # The target must be untouched by the rejected call.
            self.assertTrue(target.is_dir())

    def test_rejects_destination_containing_target(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d, 'el9-embargoed')
            dest = Path(base, '2025-11', 'rev')
            target = Path(dest, 'inner')
            target.mkdir(parents=True)
            with self.assertRaises(ValueError):
                create_plashet_symlink(base, '2025-11/rev', target)
            self.assertTrue(target.is_dir())

    def test_rejects_parent_traversal_escape(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d, 'el9-embargoed')
            base.mkdir(parents=True)
            target = Path(d, 'el9', 'rev')
            target.mkdir(parents=True)
            with self.assertRaises(ValueError):
                create_plashet_symlink(base, '../escape/rev', target)

    def test_rejects_absolute_plashet_name(self):
        with tempfile.TemporaryDirectory() as d:
            base = Path(d, 'el9-embargoed')
            base.mkdir(parents=True)
            target = Path(d, 'el9', 'rev')
            target.mkdir(parents=True)
            with self.assertRaises(ValueError):
                create_plashet_symlink(base, '/etc/passwd', target)

    def test_rejects_when_symlinked_ancestor_makes_dest_equal_target(self):
        # Finding (C): a symlinked ANCESTOR of the destination can make the destination and the
        # target resolve to the SAME real directory while their lexical paths differ. The old
        # lexical (os.path.abspath) guard misses this and would delete the public target; the
        # realpath-based guard must reject it and leave the target untouched.
        with tempfile.TemporaryDirectory() as d:
            base = Path(d, 'base')
            # Real public revision dir lives inside base under a differently-named directory.
            real_rev = base / 'real-2025-11' / 'rev'
            real_rev.mkdir(parents=True)
            (real_rev / 'marker.txt').write_text('public')
            # '2025-11' is a SYMLINK to 'real-2025-11' -> a symlinked ancestor of the destination.
            (base / '2025-11').symlink_to('real-2025-11', target_is_directory=True)

            target = base / 'real-2025-11' / 'rev'
            # Destination base/2025-11/rev resolves (via the symlinked ancestor) to the SAME real
            # directory as the target even though the lexical paths differ.
            with self.assertRaises(ValueError):
                create_plashet_symlink(base, '2025-11/rev', target)
            # The public target must be untouched by the rejected call.
            self.assertTrue(target.is_dir())
            self.assertTrue((target / 'marker.txt').is_file())


class TestBuildPlashetFromTagsSymlinkCleanup(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_unlinks_existing_directory_symlink(self):
        # Finding #5: a retry can find repo_path as a directory symlink left by a prior symlink
        # run. shutil.rmtree rejects a directory symlink on py3.11, so it must be unlinked first.
        with tempfile.TemporaryDirectory() as d:
            base = Path(d, 'el9-embargoed')
            name = '2025-11/rev'
            real = Path(d, 'el9', '2025-11', 'rev')
            real.mkdir(parents=True)
            link = Path(base, name)
            link.parent.mkdir(parents=True)
            link.symlink_to(os.path.relpath(real, link.parent), target_is_directory=True)
            self.assertTrue(link.is_symlink())

            result = await build_plashet_from_tags(
                group_param='openshift-4.19',
                assembly='stream',
                base_dir=base,
                name=name,
                arches=['x86_64'],
                include_embargoed=True,
                signing_mode='signed',
                signing_advisory=0,
                tag_pvs=[('rhaos-4.19-rhel-9-candidate', 'OSE-4.19-RHEL-9')],
                embargoed_tags=[],
                include_previous_packages=[],
                dry_run=True,
            )

            repo_path = Path(base, name)
            # The symlink is gone; a real directory was created in its place (no rmtree error).
            self.assertFalse(repo_path.is_symlink())
            self.assertTrue(repo_path.is_dir())
            self.assertEqual(result, os.path.abspath(repo_path))


class TestCleanupScriptEmbargoProtection(unittest.TestCase):
    def test_protects_revision_referenced_by_sibling_embargoed_latest(self):
        # Finding #3: the cleanup must not prune a public revision that a live sibling embargoed
        # `latest` symlink resolves to, even when that revision falls outside the keep window.
        with tempfile.TemporaryDirectory() as d:
            el9 = Path(d, 'el9')
            revs = {}
            for ym, rev in [
                ('2025-01', '202501010000'),
                ('2025-02', '202502010000'),
                ('2025-03', '202503010000'),
                ('2025-04', '202504010000'),
            ]:
                p = el9 / ym / rev
                p.mkdir(parents=True)
                revs[rev] = p
            # Public latest -> newest revision.
            (el9 / 'latest').symlink_to(os.path.join('2025-04', '202504010000'), target_is_directory=True)
            # Sibling embargoed latest resolves (across dirs) into el9's OLDEST revision.
            emb = Path(d, 'el9-embargoed')
            emb.mkdir(parents=True)
            (emb / 'latest').symlink_to(os.path.join('..', 'el9', '2025-01', '202501010000'), target_is_directory=True)

            proc = subprocess.run(
                ['bash', str(CLEANUP_SCRIPT), str(el9), '2'],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            # Oldest revision is outside keep=2 but protected by the live embargoed latest.
            self.assertTrue(revs['202501010000'].is_dir(), proc.stdout + proc.stderr)
            # Next-oldest is outside keep=2 and unprotected -> removed.
            self.assertFalse(revs['202502010000'].exists(), proc.stdout + proc.stderr)
            # The two most recent revisions are always kept.
            self.assertTrue(revs['202503010000'].is_dir())
            self.assertTrue(revs['202504010000'].is_dir())

    def test_prunes_embargoed_symlinks_then_unreferenced_public_revisions(self):
        # Finding (1): the embargoed repo publishes per-revision directory SYMLINKS. Cleanup must
        # count them (together with real directories) toward the keep window and prune the expired
        # ones by UNLINKING the link only, never deleting the public target. A repeated build then
        # cleans the public repo: a public revision whose only embargoed symlink was just removed
        # becomes unreferenced and prunable, while one still referenced by a RETAINED embargoed
        # symlink stays protected even when it is outside the public keep window.
        with tempfile.TemporaryDirectory() as d:
            el9 = Path(d, 'el9')
            pub = {}
            for ym, rev in [
                ('2025-01', '202501010000'),
                ('2025-02', '202502010000'),
                ('2025-03', '202503010000'),
                ('2025-04', '202504010000'),
                ('2025-05', '202505010000'),
            ]:
                p = el9 / ym / rev
                p.mkdir(parents=True)
                (p / 'marker.txt').write_text(rev)
                pub[rev] = p
            (el9 / 'latest').symlink_to(os.path.join('2025-05', '202505010000'), target_is_directory=True)

            # Embargoed repo: a MIX of entry types -- two per-revision symlinks into el9 (one will
            # expire, one will be retained) and one embargoed-only real revision directory.
            emb = Path(d, 'el9-embargoed')
            (emb / '2025-01').mkdir(parents=True)
            emb_link_01 = emb / '2025-01' / '202501010000'
            emb_link_01.symlink_to(os.path.join('..', '..', 'el9', '2025-01', '202501010000'), target_is_directory=True)
            (emb / '2025-02').mkdir(parents=True)
            emb_link_02 = emb / '2025-02' / '202502010000'
            emb_link_02.symlink_to(os.path.join('..', '..', 'el9', '2025-02', '202502010000'), target_is_directory=True)
            emb_dir_05 = emb / '2025-05' / '202505010000'
            emb_dir_05.mkdir(parents=True)
            (emb_dir_05 / 'marker.txt').write_text('embargoed-only')
            (emb / 'latest').symlink_to(os.path.join('2025-05', '202505010000'), target_is_directory=True)

            # Step 1: clean the EMBARGOED repo (keep=2). Three revision entries (2 symlinks + 1 dir);
            # the oldest (a symlink) is expired and must be unlinked, its public target untouched.
            proc = subprocess.run(
                ['bash', str(CLEANUP_SCRIPT), str(emb), '2'],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            # Expired embargoed symlink removed (link only); its empty parent cleaned up.
            self.assertFalse(os.path.lexists(emb_link_01), proc.stdout + proc.stderr)
            self.assertFalse((emb / '2025-01').exists())
            # The public target it pointed to is UNTOUCHED (unlink removes the link, never the target).
            self.assertTrue(pub['202501010000'].is_dir(), proc.stdout + proc.stderr)
            self.assertTrue((pub['202501010000'] / 'marker.txt').is_file())
            # Retained entries survive: the within-keep symlink and the embargoed-only directory.
            self.assertTrue(os.path.islink(emb_link_02))
            self.assertTrue(emb_dir_05.is_dir())

            # Step 2: clean the PUBLIC repo (keep=2). Oldest three are outside the window.
            proc = subprocess.run(
                ['bash', str(CLEANUP_SCRIPT), str(el9), '2'],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            # 2025-01: its only embargoed symlink was removed in step 1 -> now unreferenced -> pruned.
            self.assertFalse(pub['202501010000'].exists(), proc.stdout + proc.stderr)
            # 2025-02: still referenced by the RETAINED embargoed symlink -> protected despite being
            # outside the public keep window.
            self.assertTrue(pub['202502010000'].is_dir(), proc.stdout + proc.stderr)
            # 2025-03: unreferenced and outside the window -> pruned.
            self.assertFalse(pub['202503010000'].exists(), proc.stdout + proc.stderr)
            # The two most recent public revisions are always kept.
            self.assertTrue(pub['202504010000'].is_dir())
            self.assertTrue(pub['202505010000'].is_dir())
            # The retained embargoed symlink still resolves to its (protected) public target.
            self.assertEqual(os.path.realpath(emb_link_02), os.path.realpath(pub['202502010000']))

    def test_protects_revision_referenced_by_non_latest_sibling_symlink(self):
        # Finding (B): the embargoed flavor publishes per-revision directory symlinks, and its
        # `latest` can advance to a newer revision while an OLDER embargoed revision symlink (still
        # within the embargoed keep window) keeps pointing at a public revision here. The cleanup
        # must protect that public revision even though NO sibling `latest` points at it.
        with tempfile.TemporaryDirectory() as d:
            el9 = Path(d, 'el9')
            revs = {}
            for ym, rev in [
                ('2025-01', '202501010000'),
                ('2025-02', '202502010000'),
                ('2025-03', '202503010000'),
                ('2025-04', '202504010000'),
            ]:
                p = el9 / ym / rev
                p.mkdir(parents=True)
                revs[rev] = p
            # Public latest -> newest revision.
            (el9 / 'latest').symlink_to(os.path.join('2025-04', '202504010000'), target_is_directory=True)

            emb = Path(d, 'el9-embargoed')
            # Embargoed `latest` and its per-revision symlink point at the NEWEST public revision.
            (emb / '2025-04').mkdir(parents=True)
            (emb / '2025-04' / '202504010000').symlink_to(
                os.path.join('..', '..', 'el9', '2025-04', '202504010000'), target_is_directory=True
            )
            (emb / 'latest').symlink_to(os.path.join('2025-04', '202504010000'), target_is_directory=True)
            # An OLDER per-revision embargoed symlink (no `latest` points at it) still references
            # el9's OLDEST revision.
            (emb / '2025-01').mkdir(parents=True)
            (emb / '2025-01' / '202501010000').symlink_to(
                os.path.join('..', '..', 'el9', '2025-01', '202501010000'), target_is_directory=True
            )

            proc = subprocess.run(
                ['bash', str(CLEANUP_SCRIPT), str(el9), '2'],
                capture_output=True,
                text=True,
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            # Oldest revision is outside keep=2 but protected by the NON-latest sibling symlink.
            self.assertTrue(revs['202501010000'].is_dir(), proc.stdout + proc.stderr)
            # Next-oldest is outside keep=2 and unprotected -> removed.
            self.assertFalse(revs['202502010000'].exists(), proc.stdout + proc.stderr)
            # The two most recent revisions are always kept.
            self.assertTrue(revs['202503010000'].is_dir())
            self.assertTrue(revs['202504010000'].is_dir())
