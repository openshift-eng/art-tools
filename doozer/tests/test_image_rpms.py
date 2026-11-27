"""Tests for shared image RPM inventory extraction and caching."""

import asyncio
import hashlib
import io
import os
import tarfile
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, MagicMock, patch

from doozerlib import image_rpms

IMAGE = 'quay.io/test/base@sha256:' + 'a' * 64
RPM_OUTPUT = 'libgcc\t0\t14.3.1\t4.4.el10\ts390x\tgcc-14.3.1-4.4.el10.src.rpm\n'


class TestImageRpmInventory(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        image_rpms._get_image_rpm_inventory.cache_clear()
        self.temporary = TemporaryDirectory()
        self.env = patch.dict(os.environ, {'XDG_CACHE_HOME': self.temporary.name, 'JENKINS_HOME': ''})
        self.env.start()

    async def asyncTearDown(self):
        image_rpms._get_image_rpm_inventory.cache_clear()
        self.env.stop()
        self.temporary.cleanup()

    async def _commands(self, command, **kwargs):
        if command[0] == image_rpms.SYSTEM_PYTHON:
            database = Path(command[3]) / 'var/lib/rpm'
            database.mkdir(parents=True)
            (database / 'rpmdb.sqlite').touch()
            return 0, '', ''
        self.assertEqual(command[0], 'rpm')
        return 0, RPM_OUTPUT, ''

    @patch.object(image_rpms, 'oc_image_info_for_arch_async', new_callable=AsyncMock, return_value={})
    @patch.object(image_rpms, 'cmd_gather_async', new_callable=AsyncMock)
    async def test_concurrent_callers_share_extraction_and_query(self, commands, info):
        commands.side_effect = self._commands
        results = await asyncio.gather(*(image_rpms.get_image_rpm_inventory(IMAGE, 's390x') for _ in range(12)))
        self.assertTrue(all(result is results[0] for result in results))
        self.assertEqual(commands.await_count, 2)
        info.assert_awaited_once()
        self.assertEqual(results[0][0].evr, '0:14.3.1-4.4.el10')
        self.assertEqual(results[0][0].source_rpm, 'gcc-14.3.1-4.4.el10.src.rpm')
        self.assertIs(await image_rpms.get_image_rpm_inventory(IMAGE, 's390x'), results[0])
        self.assertEqual(commands.await_count, 2)

    @patch.object(image_rpms, 'oc_image_info_for_arch_async', new_callable=AsyncMock, return_value={})
    @patch.object(image_rpms, 'cmd_gather_async', new_callable=AsyncMock)
    async def test_architecture_aliases_share_cache_but_digests_and_arches_do_not(self, commands, info):
        commands.side_effect = self._commands
        await image_rpms.get_image_rpm_inventory(IMAGE, 'x86_64')
        await image_rpms.get_image_rpm_inventory(IMAGE, 'amd64')
        await image_rpms.get_image_rpm_inventory(IMAGE, 's390x')
        await image_rpms.get_image_rpm_inventory(IMAGE.replace('a' * 64, 'b' * 64), 's390x')
        self.assertEqual(info.await_count, 3)
        self.assertEqual(commands.await_count, 6)

    @patch.object(image_rpms, 'oc_image_info_for_arch_async', new_callable=AsyncMock, return_value={})
    @patch.object(image_rpms, 'cmd_gather_async', new_callable=AsyncMock)
    async def test_failures_are_retried_and_empty_inventory_is_cached(self, commands, info):
        commands.side_effect = ChildProcessError('unavailable')
        with self.assertRaises(ChildProcessError):
            await image_rpms.get_image_rpm_inventory(IMAGE, 's390x')

        async def empty(command, **kwargs):
            await self._commands(command, **kwargs)
            return 0, '', ''

        commands.side_effect = empty
        self.assertEqual(await image_rpms.get_image_rpm_inventory(IMAGE, 's390x'), ())
        self.assertEqual(await image_rpms.get_image_rpm_inventory(IMAGE, 's390x'), ())
        self.assertEqual(info.await_count, 2)
        self.assertEqual(commands.await_count, 3)

    @patch.object(image_rpms, 'oc_image_info_for_arch_async', new_callable=AsyncMock)
    @patch.object(image_rpms, 'cmd_gather_async', new_callable=AsyncMock)
    async def test_reuses_prototype_rpmdb_without_registry_access(self, commands, info):
        database = (
            Path(self.temporary.name) / image_rpms.RPMDB_CACHE_SUBDIR / 's390x' / IMAGE.split('@')[1] / 'usr/share/rpm'
        )
        database.mkdir(parents=True)
        (database / 'rpmdb.sqlite').touch()
        commands.return_value = 0, RPM_OUTPUT, ''
        result = await image_rpms.get_image_rpm_inventory(IMAGE, 's390x')
        self.assertEqual(result[0].name, 'libgcc')
        info.assert_not_awaited()
        commands.assert_awaited_once()
        self.assertNotEqual(commands.call_args.args[0][2], str(database))

    async def test_mutable_tags_are_not_cached(self):
        with self.assertRaisesRegex(ValueError, 'digest-pinned'):
            await image_rpms.get_image_rpm_inventory('quay.io/test/base:latest', 'x86_64')

    @patch.object(image_rpms, 'cmd_gather_async', new_callable=AsyncMock)
    async def test_query_preserves_multilib_and_epochs_and_rejects_malformed_output(self, commands):
        commands.return_value = (
            0,
            RPM_OUTPUT
            + RPM_OUTPUT.replace('s390x', 'i686').replace('\t0\t', '\t2\t')
            + 'gpg-pubkey\t0\t1\t1\t(none)\t(none)\n',
            '',
        )
        records = await image_rpms._query_inventory(Path('/unused'))
        self.assertEqual(len(records), 2)
        self.assertEqual({r.epoch for r in records}, {0, 2})
        commands.return_value = 0, 'libgcc\n', ''
        with self.assertRaises(ValueError):
            await image_rpms._query_inventory(Path('/unused'))

    @patch.object(image_rpms, '_extract_public_rpm_layer')
    @patch.object(image_rpms, 'oc_image_info_for_arch_async', new_callable=AsyncMock)
    @patch.object(image_rpms, 'cmd_gather_async', new_callable=AsyncMock)
    async def test_fast_path_failure_uses_authenticated_fallback(self, commands, info, extract):
        info.return_value = TestOstreeLayer.info()
        extract.side_effect = requests_error = RuntimeError('private registry')
        commands.side_effect = self._commands
        with patch.dict(os.environ, {'QUAY_AUTH_FILE': '/private/auth.json'}):
            result = await image_rpms.get_image_rpm_inventory(IMAGE, 's390x')
        self.assertEqual(result[0].name, 'libgcc')
        self.assertEqual(commands.call_args_list[0].kwargs['env']['REGISTRY_AUTH_FILE'], '/private/auth.json')
        self.assertIs(extract.side_effect, requests_error)


class TestOstreeLayer(unittest.TestCase):
    @staticmethod
    def info():
        return {
            'config': {
                'history': [
                    {'created': 't0', 'created_by': 'ostree export of commit abc'},
                    {'created': 't0', 'created_by': 'rpm-4.19-1.el10.x86_64'},
                    {'created': 't0', 'created_by': '17 components'},
                    {'created': 't1', 'created_by': '/bin/sh -c #(nop) LABEL version=10.2'},
                ]
            },
            'layers': [{'digest': f'sha256:{i}'} for i in range(4)],
        }

    def test_selects_by_history_and_rejects_modified_or_unknown_layout(self):
        info = self.info()
        self.assertEqual(image_rpms._ostree_rpm_layer(info), 'sha256:1')
        info['config']['history'][-1]['created_by'] = '/bin/sh -c dnf install gcc'
        self.assertIsNone(image_rpms._ostree_rpm_layer(info))
        info['config']['history'][-1]['created'] = 't0'
        self.assertIsNone(image_rpms._ostree_rpm_layer(info))
        self.assertIsNone(image_rpms._ostree_rpm_layer({}))
        info = self.info()
        info['layers'].pop()
        self.assertIsNone(image_rpms._ostree_rpm_layer(info))

    @patch.object(image_rpms.requests, 'Session')
    def test_extracts_hardlinked_database_without_unpacking_tar_paths(self, session):
        content = io.BytesIO()
        with tarfile.open(fileobj=content, mode='w:gz') as archive:
            target = tarfile.TarInfo('sysroot/ostree/repo/objects/ab/database.file')
            target.size = 8
            archive.addfile(target, io.BytesIO(b'database'))
            member = tarfile.TarInfo('usr/share/rpm/rpmdb.sqlite')
            member.type = tarfile.LNKTYPE
            member.linkname = target.name
            archive.addfile(member)
            unrelated = tarfile.TarInfo('../../outside')
            unrelated.size = 4
            archive.addfile(unrelated, io.BytesIO(b'evil'))
        data = content.getvalue()
        response = MagicMock(status_code=200)
        response.iter_content.return_value = [data]
        session.return_value.__enter__.return_value.get.return_value = response
        with TemporaryDirectory() as directory:
            root = Path(directory)
            database = image_rpms._extract_public_rpm_layer(IMAGE, 'sha256:' + hashlib.sha256(data).hexdigest(), root)
            self.assertEqual((database / 'rpmdb.sqlite').read_bytes(), b'database')
            self.assertFalse((root / 'sysroot').exists())
            response.iter_content.return_value = [data]
            with self.assertRaisesRegex(ValueError, 'checksum'):
                image_rpms._extract_public_rpm_layer(IMAGE, 'sha256:' + '0' * 64, root)
