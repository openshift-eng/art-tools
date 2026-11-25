from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.rpm_embargo import find_embargoed_rpms, rpm_nvras_from_lockfile


class TestRpmEmbargo(TestCase):
    def test_reads_pinned_packages_and_sources_across_architectures(self):
        with TemporaryDirectory() as dir_name:
            lockfile = Path(dir_name) / 'rpms.lock.yaml'
            lockfile.write_text(
                """
                arches:
                - arch: x86_64
                  packages:
                  - url: https://repo.example/normal-1.0-1.el9.x86_64.rpm
                - arch: aarch64
                  packages:
                  - url: https://repo.example/embargo-test-1.0-2.p3.el9.aarch64.rpm
                  source:
                  - url: https://repo.example/embargo-test-1.0-2.p3.el9.src.rpm
                """
            )
            self.assertEqual(
                rpm_nvras_from_lockfile(lockfile),
                {
                    'normal-1.0-1.el9.x86_64',
                    'embargo-test-1.0-2.p3.el9.aarch64',
                    'embargo-test-1.0-2.p3.el9.src',
                },
            )

    def test_detects_private_suffixes_without_brew_records(self):
        session = MagicMock()
        multicall = session.multicall.return_value.__enter__.return_value
        multicall.getRPM.return_value = SimpleNamespace(result=None)
        private = {'brew-1.0-2.p1.el9.x86_64', 'konflux-1.0-2.p3.el9.aarch64'}
        self.assertEqual(
            find_embargoed_rpms(session, rpm_nvras=private | {'unsigned-1.0-2.el9.aarch64'}),
            private,
        )

    @patch('doozerlib.rpm_embargo.brew.get_builds_tags')
    def test_applies_source_build_tags_to_binary_rpms(self, get_tags):
        session = MagicMock()
        multicall = session.multicall.return_value.__enter__.return_value
        multicall.getRPM.side_effect = [
            SimpleNamespace(result={'build_id': 10}),
            SimpleNamespace(result={'build_id': 11}),
        ]
        get_tags.return_value = [[{'name': 'example-embargoed'}], [{'name': 'RHBA-2026:1-released'}]]
        binary = 'subpackage-1.0-2.p2.el9.aarch64'
        released = 'subpackage-1.0-3.p3.el9.aarch64'
        self.assertEqual(find_embargoed_rpms(session, rpm_nvras={binary, released}), {binary})
        get_tags.assert_called_once_with([10, 11], session)

    @patch('doozerlib.rpm_embargo.brew.get_builds_tags')
    def test_missing_alternative_arch_preserves_exact_release_evidence(self, get_tags):
        session = MagicMock()
        multicall = session.multicall.return_value.__enter__.return_value
        multicall.getRPM.side_effect = [SimpleNamespace(result={'build_id': 11}), SimpleNamespace(result=None)]
        get_tags.return_value = [[{'name': 'RHBA-2026:1-released'}]]
        binary = 'subpackage-1.0-1.p3.el9'
        self.assertEqual(
            find_embargoed_rpms(session, rpm_nvras={f'{binary}.aarch64', f'{binary}.x86_64'}),
            set(),
        )
