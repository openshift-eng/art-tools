"""Regression coverage for inherited CoreOS RPM constraints."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
from artcommonlib.model import Model
from dockerfile_parse import DockerfileParser
from doozerlib.image_rpms import InstalledRpm
from doozerlib.lockfile_prototype.generator import RpmLockfilePrototypeGenerator
from doozerlib.lockfile_prototype.inherited_rpms import (
    constrain_repo,
    locks_inherited_rpms,
    preserved_versions,
    stage_commands,
)
from doozerlib.lockfile_prototype.models import ArchResult, LockfileData, PackageEntry, RepoEntry
from doozerlib.lockfile_prototype.rebaser_hooks import generate_lockfile
from doozerlib.repodata import Rpm

BASE = 'quay.io/coreos/base@sha256:abc'
OLD = InstalledRpm('libatomic', 0, '14.3.1', '4.4.el10', 'aarch64', 'gcc-14.3.1-4.4.el10.src.rpm')


def rpm(name='libatomic', release='4.5.el10_2', arch='s390x'):
    return Rpm(
        name=name,
        epoch=0,
        version='14.3.1',
        release=release,
        arch=arch,
        checksum='',
        size=0,
        location='',
        sourcerpm='gcc.src.rpm',
    )


class TestInheritedRpms(unittest.TestCase):
    def test_constraints_include_packages_absent_on_other_architecture(self):
        versions = preserved_versions({'aarch64': (OLD,), 's390x': ()})
        original = RepoEntry(repoid='baseos', baseurl='https://example/baseos', options={'exclude': 'unrelated*'})
        constrained = constrain_repo(original, [rpm(), rpm(release='4.4.el10'), rpm(name='new-package')], versions)
        self.assertEqual(constrained.options['excludepkgs'], 'unrelated* libatomic-0:14.3.1-4.5.el10_2.s390x')
        self.assertEqual(original.options, {'exclude': 'unrelated*'})

    def test_inherited_disagreement_fails_instead_of_picking_oldest(self):
        other = InstalledRpm('libatomic', 0, '14.3.1', '4.5.el10_2', 'ppc64le', 'gcc.src.rpm')
        with self.assertRaisesRegex(ValueError, 'libatomic'):
            preserved_versions({'aarch64': (OLD,), 'ppc64le': (other,)})

    def test_detection_follows_copied_script_and_ignores_comments(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'node.sh').write_text("dnf --disablerepo=* versionlock add '*'\ndnf install -y openvswitch3.5\n")
            dockerfile = (
                "FROM base\nCOPY node.sh /tmp/renamed.sh\nRUN bash /tmp/renamed.sh\nFROM base\nRUN echo hello\n"
            )
            (root / 'Dockerfile').write_text(dockerfile)
            commands = stage_commands(root, DockerfileParser(str(root / 'Dockerfile')).structure)
            self.assertTrue(locks_inherited_rpms(commands[0]))
            self.assertFalse(locks_inherited_rpms(commands[1]))
            self.assertFalse(locks_inherited_rpms("# dnf versionlock add '*'\necho \"dnf versionlock add '*'\""))

    def test_absolute_scripts_must_be_copied_or_bind_mounted(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'node.sh').write_text("dnf versionlock add '*'\n")
            (root / 'Dockerfile').write_text(
                'FROM base\nRUN /usr/bin/node.sh\nFROM base\nRUN --mount=type=bind,target=/run/src /run/src/node.sh\n'
            )
            commands = stage_commands(root, DockerfileParser(str(root / 'Dockerfile')).structure)
            self.assertFalse(locks_inherited_rpms(commands[0]))
            self.assertTrue(locks_inherited_rpms(commands[1]))


class TestInheritedGeneration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.repository = MagicMock()
        self.repository.baseurl.side_effect = lambda repotype, arch: f'https://example/{arch}/baseos'
        self.repository.get_repodata = AsyncMock(return_value=SimpleNamespace(primary_rpms=[rpm()]))
        self.generator = RpmLockfilePrototypeGenerator({'baseos': self.repository}, self.root)
        self.generator._build_repo_list = lambda names, arches: [
            RepoEntry(repoid='baseos', baseurl=f'https://example/{arches[0]}/baseos')
        ]
        self.generator._container.resolve_to_digest = AsyncMock(return_value=BASE)

    async def asyncTearDown(self):
        self.temporary.cleanup()

    def entries(self, content):
        (self.root / 'Dockerfile').write_text(content)
        return DockerfileParser(str(self.root / 'Dockerfile')).structure

    @patch('doozerlib.lockfile_prototype.generator.get_image_rpm_inventory', new_callable=AsyncMock)
    async def test_node_preserves_inventory_through_stage_aliases(self, inventory):
        inventory.side_effect = [(OLD,), ()]
        self.generator.downstream_parents = [BASE, 'build', 'build']
        entries = self.entries(
            "FROM base AS build\nRUN dnf versionlock add '*' && dnf install -y openvswitch3.5\nFROM build AS metadata\nRUN echo metadata\nFROM build\nRUN echo final\n"
        )
        await self.generator._prepare_inherited_constraints(
            self.root, entries, ['aarch64', 's390x'], {'baseos'}, 'node'
        )
        self.assertEqual(inventory.await_count, 2)
        self.assertEqual(self.generator.inherited_rpms, {'aarch64': (OLD,), 's390x': ()})
        self.assertIn(
            'libatomic-0:14.3.1-4.5.el10_2.s390x', self.generator._locked_repos[0]['s390x'][0].options['excludepkgs']
        )
        self.assertNotIn(2, self.generator._locked_repos)

    @patch('doozerlib.lockfile_prototype.generator.get_image_rpm_inventory', new_callable=AsyncMock)
    async def test_extensions_use_promised_parent_inventory_without_pull(self, inventory):
        self.generator.downstream_parents = ['registry/node:promised', 'registry/builder:existing']
        self.generator.parent_inherited_rpms = {0: {'aarch64': (OLD,), 's390x': ()}}
        self.generator.parent_source_dirs = {0: self.root / 'node'}
        entries = self.entries(
            "FROM registry/node:promised AS os\nRUN dnf versionlock add '*' && dnf install --downloadonly gcc\nFROM registry/builder:existing\nRUN dnf install createrepo_c\n"
        )
        await self.generator._prepare_inherited_constraints(
            self.root, entries, ['aarch64', 's390x'], {'baseos'}, 'extensions'
        )
        inventory.assert_not_awaited()
        self.generator._container.resolve_to_digest.assert_not_awaited()
        self.assertIn(0, self.generator._locked_repos)
        self.assertNotIn(1, self.generator._locked_repos)
        self.assertEqual(self.generator.inherited_rpms, {})

    @patch('doozerlib.lockfile_prototype.generator.get_image_rpm_inventory', new_callable=AsyncMock)
    async def test_unlocked_and_no_rpms_stages_do_not_inspect_base(self, inventory):
        self.generator.downstream_parents = [BASE]
        for command, ignored in [('dnf install gcc', set()), ("dnf versionlock add '*'", {0})]:
            entries = self.entries(f'FROM base\nRUN {command}\n')
            await self.generator._prepare_inherited_constraints(
                self.root, entries, ['s390x'], {'baseos'}, 'image', ignored
            )
        inventory.assert_not_awaited()
        self.repository.get_repodata.assert_not_awaited()

    @patch('doozerlib.lockfile_prototype.generator.get_image_rpm_inventory', new_callable=AsyncMock)
    async def test_missing_promised_parent_inventory_fails_without_using_stale_image(self, inventory):
        self.generator.downstream_parents = ['registry/node:promised']
        self.generator.parent_source_dirs = {0: self.root / 'node'}
        entries = self.entries("FROM registry/node:promised\nRUN dnf versionlock add '*'\n")
        with self.assertRaisesRegex(RuntimeError, 'cannot determine versionlocked parent RPMs'):
            await self.generator._prepare_inherited_constraints(self.root, entries, ['s390x'], {'baseos'}, 'extensions')
        inventory.assert_not_awaited()
        self.generator._container.resolve_to_digest.assert_not_awaited()

    @patch('doozerlib.lockfile_prototype.generator.get_image_rpm_inventory', new_callable=AsyncMock)
    async def test_existing_node_is_inspected_when_node_is_not_in_run(self, inventory):
        inventory.return_value = (OLD,)
        self.generator.downstream_parents = ['registry/node:existing']
        entries = self.entries("FROM registry/node:existing\nRUN dnf versionlock add '*'\n")
        await self.generator._prepare_inherited_constraints(self.root, entries, ['aarch64'], {'baseos'}, 'extensions')
        inventory.assert_awaited_once_with(BASE, 'aarch64')
        self.assertEqual(self.generator.inherited_rpms, {'aarch64': (OLD,)})

    async def test_unprotected_child_mutation_discards_inherited_inventory(self):
        self.generator.parent_inherited_rpms = {0: {'aarch64': (OLD,)}}
        entries = self.entries('FROM registry/node AS node\nRUN echo parent\nFROM node\nRUN dnf update -y\n')
        await self.generator._prepare_inherited_constraints(self.root, entries, ['aarch64'], {'baseos'}, 'child')
        self.assertEqual(self.generator.inherited_rpms, {})

    async def test_absent_optional_repository_is_omitted(self):
        self.repository.cs_optional = True
        self.repository.get_repodata.side_effect = aiohttp.ClientResponseError(None, (), status=404)
        self.assertEqual(await self.generator._constrain_repos_for_arch({'baseos'}, 's390x', {}), [])
        self.repository.cs_optional = False
        with self.assertRaises(aiohttp.ClientResponseError):
            await self.generator._constrain_repos_for_arch({'baseos'}, 's390x', {})
        self.repository.cs_optional = True
        self.repository.get_repodata.side_effect = aiohttp.ClientResponseError(None, (), status=503)
        with self.assertRaises(aiohttp.ClientResponseError):
            await self.generator._constrain_repos_for_arch({'baseos'}, 's390x', {})

    async def test_constrained_resolution_does_not_silently_drop_missing_packages(self):
        self.generator._locked_repos = {0: {'s390x': []}}
        self.generator._resolver.resolve = AsyncMock(side_effect=RuntimeError('No match for argument: gcc'))
        with self.assertRaisesRegex(RuntimeError, 'inherited RPM constraints failed'):
            await self.generator._resolve_stage_with_retry([], ['s390x'], ['gcc'], None, 'extensions', 0)
        self.generator._resolver.resolve.assert_awaited_once()

    def test_repository_change_is_detected_after_resolution(self):
        self.generator._inherited_versions = {0: {'libatomic': OLD.evr}}
        result = LockfileData(
            arches=[
                ArchResult(
                    arch='s390x',
                    packages=[
                        PackageEntry(
                            name='libatomic',
                            evr='14.3.1-4.5.el10_2',
                            url='https://example/libatomic.rpm',
                            repoid='baseos',
                        )
                    ],
                )
            ]
        )
        with self.assertRaisesRegex(RuntimeError, 'inherited version'):
            self.generator._validate_inherited_versions(result, 0)
        result.arches[0].packages[0].evr = '14.3.1-4.4.el10'
        self.generator._validate_inherited_versions(result, 0)

    @patch('doozerlib.lockfile_prototype.rebaser_hooks.RpmLockfilePrototypeGenerator')
    async def test_parent_mapping_uses_metadata_member_not_pullspec_substring(self, generator_type):
        parent = SimpleNamespace(
            distgit_key='rhcos-node-image-rhel10',
            qualified_key='containers/node',
            lockfile_packages=['libatomic'],
            lockfile_inherited_rpms={'aarch64': (OLD,)},
        )
        (self.root / parent.qualified_key).mkdir(parents=True)
        meta = SimpleNamespace(
            config=Model({'from': {'builder': [{'member': parent.distgit_key}], 'member': 'enterprise-base'}})
        )
        self.entries(
            'FROM registry/rhcos-node-image-container-v5:promised AS os\nRUN echo first\nFROM os AS alias\nRUN echo alias\nFROM registry/enterprise-base:existing\n'
        )
        generator = generator_type.return_value
        generator.generate_lockfile = AsyncMock()
        generator.inherited_rpms = {}
        cache = await generate_lockfile(
            meta,
            self.root,
            {},
            self.root,
            self.root,
            downstream_parents=[
                'registry/rhcos-node-image-container-v5:promised',
                'os',
                'registry/enterprise-base:existing',
            ],
            parent_members=[parent],
        )
        try:
            kwargs = generator.generate_lockfile.call_args.kwargs
            self.assertEqual(kwargs['parent_inherited_rpms'], {0: parent.lockfile_inherited_rpms})
            self.assertEqual(kwargs['parent_source_dirs'], {0: self.root / parent.qualified_key})
        finally:
            cache.cleanup()
