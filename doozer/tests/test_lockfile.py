import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

from doozerlib.lockfile import (
    ArtifactInfo,
    ArtifactLockfileGenerator,
    ModuleInfo,
    RpmInfo,
    RpmInfoCollector,
    RPMLockfileGenerator,
)
from doozerlib.repodata import Rpm, RpmModule
from doozerlib.repos import Repos


class TestRpmInfo(unittest.TestCase):
    def setUp(self):
        self.mock_rpm = MagicMock()
        self.mock_rpm.name = "bash"
        self.mock_rpm.epoch = 0
        self.mock_rpm.version = "5.1.0"
        self.mock_rpm.release = "1.el9"
        self.mock_rpm.checksum = "abc123"
        self.mock_rpm.size = 123456
        self.mock_rpm.location = "/Packages/bash.rpm"
        self.mock_rpm.sourcerpm = "bash-5.1.0-1.el9.src.rpm"

    def test_from_rpm_default_epoch(self):
        rpm_info = RpmInfo.from_rpm(self.mock_rpm, repoid="repo-1", baseurl="http://example.com")
        self.assertEqual(rpm_info.name, "bash")
        self.assertEqual(rpm_info.evr, "0:5.1.0-1.el9")
        self.assertEqual(rpm_info.checksum, "abc123")
        self.assertEqual(rpm_info.repoid, "repo-1")
        self.assertEqual(rpm_info.size, 123456)
        self.assertEqual(rpm_info.sourcerpm, "bash-5.1.0-1.el9.src.rpm")
        self.assertEqual(rpm_info.url, "http://example.com/Packages/bash.rpm")
        self.assertEqual(rpm_info.version, "5.1.0")
        self.assertEqual(rpm_info.epoch, 0)
        self.assertEqual(rpm_info.release, "1.el9")

    def test_from_rpm_nonzero_epoch(self):
        self.mock_rpm.epoch = 2
        rpm_info = RpmInfo.from_rpm(self.mock_rpm, repoid="repo-2", baseurl="https://cdn.redhat.com")
        self.assertEqual(rpm_info.evr, "2:5.1.0-1.el9")

    def test_to_dict(self):
        rpm_info = RpmInfo(
            name="coreutils",
            evr="1:9.0-3.el9",
            checksum="def456",
            repoid="baseos-x86_64",
            size=98765,
            sourcerpm="coreutils-9.0-3.el9.src.rpm",
            url="http://example.com/coreutils.rpm",
            version="9.0",
            epoch=1,
            release="3.el9",
        )
        expected = {
            "name": "coreutils",
            "evr": "1:9.0-3.el9",
            "checksum": "def456",
            "repoid": "baseos-x86_64",
            "size": 98765,
            "sourcerpm": "coreutils-9.0-3.el9.src.rpm",
            "url": "http://example.com/coreutils.rpm",
        }
        self.assertEqual(rpm_info.to_dict(), expected)

    # this method does not test the functionality of the underlying compare_nvr, but rather a simple equality check
    def test_equality_and_ordering(self):
        a = RpmInfo("foo", "1:1.0-1", "x", "r", 0, "s", "url", version="1.0-1", epoch=0, release="1.el9")
        b = RpmInfo("foo", "1:1.0-1", "y", "r", 0, "s", "url", version="1.0-1", epoch=0, release="1.el9")
        c = RpmInfo("foo", "1:1.0-2", "z", "r", 0, "s", "url", version="1.0-2", epoch=0, release="1.el9")

        self.assertEqual(a, b)
        self.assertLess(a, c)
        self.assertTrue(a <= b)
        self.assertTrue(c > a)


class TestModuleInfo(unittest.TestCase):
    def test_from_repository_metadata(self):
        module_info = ModuleInfo.from_repository_metadata(
            repoid="rhel-9-appstream-rpms",
            baseurl="https://example.com/",
            checksum="sha256:abc123",
            size=12345,
            url="https://example.com/repodata/modules.yaml.gz",
        )

        self.assertEqual(module_info.repoid, "rhel-9-appstream-rpms")
        self.assertEqual(module_info.url, "https://example.com/repodata/modules.yaml.gz")
        self.assertEqual(module_info.checksum, "sha256:abc123")
        self.assertEqual(module_info.size, 12345)

    def test_to_dict(self):
        module_info = ModuleInfo(
            url="https://example.com/repodata/modules.yaml.gz",
            repoid="rhel-9-appstream-rpms",
            checksum="sha256:abc123",
            size=12345,
        )

        expected = {
            "url": "https://example.com/repodata/modules.yaml.gz",
            "repoid": "rhel-9-appstream-rpms",
            "checksum": "sha256:abc123",
            "size": 12345,
        }
        self.assertEqual(module_info.to_dict(), expected)


class TestRpmInfoCollectorFetchRpms(unittest.TestCase):
    def setUp(self):
        self.arches = ['x86_64', 'aarch64']

        self.repo_data = {
            "rhel-9-baseos-rpms": {
                "conf": {
                    "extra_options": {"module_hotfixes": 1},
                    "baseurl": {
                        "aarch64": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/aarch64/baseos/os/",
                        "ppc64le": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/ppc64le/baseos/os/",
                        "s390x": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/s390x/baseos/os/",
                        "x86_64": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/x86_64/baseos/os/",
                    },
                    "ci_alignment": {"profiles": ["el9"], "localdev": {"enabled": True}},
                },
                "content_set": {
                    "default": "rhel-9-for-x86_64-baseos-eus-rpms__9_DOT_4",
                    "aarch64": "rhel-9-for-aarch64-baseos-eus-rpms__9_DOT_4",
                    "ppc64le": "rhel-9-for-ppc64le-baseos-eus-rpms__9_DOT_4",
                    "s390x": "rhel-9-for-s390x-baseos-eus-rpms__9_DOT_4",
                },
                "reposync": {"enabled": False},
            },
            "rhel-9-appstream-rpms": {
                "conf": {
                    "extra_options": {"module_hotfixes": 1},
                    "baseurl": {
                        "aarch64": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/aarch64/appstream/os/",
                        "ppc64le": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/ppc64le/appstream/os/",
                        "s390x": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/s390x/appstream/os/",
                        "x86_64": "https://rhsm-pulp.corp.redhat.com/content/eus/rhel9/9.4/x86_64/appstream/os/",
                    },
                    "ci_alignment": {"profiles": ["el9"], "localdev": {"enabled": True}},
                },
                "content_set": {
                    "default": "rhel-9-for-x86_64-appstream-eus-rpms__9_DOT_4",
                    "aarch64": "rhel-9-for-aarch64-appstream-eus-rpms__9_DOT_4",
                    "ppc64le": "rhel-9-for-ppc64le-appstream-eus-rpms__9_DOT_4",
                    "s390x": "rhel-9-for-s390x-appstream-eus-rpms__9_DOT_4",
                },
                "reposync": {"enabled": False},
            },
        }

        self.repo_names = list(self.repo_data.keys())
        self.repos = Repos(self.repo_data, arches=self.arches)
        self.collector = RpmInfoCollector(repos=self.repos)
        self.collector.logger = MagicMock()

        # Setup loaded_repos with keys for each repo + arch
        self.loaded_repos = {}
        for repo_name in self.repo_names:
            for arch in self.arches:
                key = f"{repo_name}-{arch}"
                self.loaded_repos[key] = MagicMock()
        self.collector.loaded_repos = self.loaded_repos

        # Create multiple RPMs per arch with varied realistic data
        self.sample_rpms = {
            'x86_64': [
                Rpm(
                    name="mypkg",
                    epoch=0,
                    version="1.0",
                    release="1.el9",
                    arch="x86_64",
                    checksum="dummychecksum-x86_64-1",
                    size=2345,
                    location="path/to/mypkg-1-x86_64.rpm",
                    sourcerpm="mypkg-src-1.0-1.el9.src.rpm",
                ),
                Rpm(
                    name="otherpkg",
                    epoch=1,
                    version="2.1",
                    release="3.el9",
                    arch="x86_64",
                    checksum="dummychecksum-x86_64-2",
                    size=1500,
                    location="path/to/otherpkg-2-x86_64.rpm",
                    sourcerpm="otherpkg-src-2.1-3.el9.src.rpm",
                ),
                Rpm(
                    name="coolpkg",
                    epoch=0,
                    version="0.9",
                    release="10.el9",
                    arch="x86_64",
                    checksum="dummychecksum-x86_64-3",
                    size=4321,
                    location="path/to/coolpkg-0.9-x86_64.rpm",
                    sourcerpm="coolpkg-src-0.9-10.el9.src.rpm",
                ),
            ],
            'aarch64': [
                Rpm(
                    name="mypkg",
                    epoch=0,
                    version="1.0",
                    release="1.el9",
                    arch="aarch64",
                    checksum="dummychecksum-aarch64-1",
                    size=2300,
                    location="path/to/mypkg-1-aarch64.rpm",
                    sourcerpm="mypkg-src-1.0-1.el9.src.rpm",
                ),
                Rpm(
                    name="otherpkg",
                    epoch=0,
                    version="2.0",
                    release="2.el9",
                    arch="aarch64",
                    checksum="dummychecksum-aarch64-2",
                    size=1400,
                    location="path/to/otherpkg-2-aarch64.rpm",
                    sourcerpm="otherpkg-src-2.0-2.el9.src.rpm",
                ),
            ],
        }

    def test_resolves_rpms_from_loaded_repo_multiple_arches_and_packages(self):
        # Mock get_rpms to return all sample rpms for each arch, empty missing list
        for repo_name in self.repo_names:
            for arch in self.arches:
                repodata = MagicMock()
                repodata.get_rpms.return_value = (self.sample_rpms[arch], [])
                key = f"{repo_name}-{arch}"
                self.collector.loaded_repos[key] = repodata

        for arch in self.arches:
            for repo_name in self.repo_names:
                # Prepare set of rpm names to resolve
                rpm_names = {rpm.name for rpm in self.sample_rpms[arch]}
                result = self.collector._fetch_rpms_info_per_arch(rpm_names, {repo_name}, arch)

                # Check that the number of resolved rpms matches the sample count
                self.assertEqual(len(result), len(self.sample_rpms[arch]))

                # Verify each resolved item is an instance of RpmInfo
                for rpm_info in result:
                    self.assertIsInstance(rpm_info, RpmInfo)

                # Optionally verify the rpm names match the input set
                resolved_names = {ri.name for ri in result}
                self.assertTrue(rpm_names.issubset(resolved_names))

    def test_skips_repo_if_repodata_missing(self):
        repo_name = self.repo_names[0]
        arch = self.arches[0]
        repo_key = f"{repo_name}-{arch}"
        self.collector.loaded_repos.pop(repo_key, None)

        rpm_names = {rpm.name for rpm in self.sample_rpms[arch]}
        result = self.collector._fetch_rpms_info_per_arch(rpm_names, {repo_name}, arch)
        self.assertEqual(result, [])
        self.collector.logger.error.assert_any_call(
            f'repodata {repo_key} not found while fetching rpms, it should be loaded by now'
        )

    def test_logs_warning_if_rpms_missing(self):
        repo_name = self.repo_names[0]
        arch = self.arches[0]

        rpm = self.sample_rpms[arch][0]
        repodata = MagicMock()
        repodata.get_rpms.return_value = ([rpm], ["missingpkg"])
        repo_key = f"{repo_name}-{arch}"
        self.collector.loaded_repos[repo_key] = repodata

        self.collector._fetch_rpms_info_per_arch({rpm.name, "missingpkg"}, {repo_name}, arch)
        self.collector.logger.warning.assert_called_with(f"Could not find missingpkg in {repo_name} for arch {arch}")

    def test_load_repos_skips_already_loaded(self):
        # Simulate that both repos are already loaded
        already_loaded = {f"{name}-x86_64" for name in self.repo_names}
        self.collector.loaded_repos = {k: MagicMock() for k in already_loaded}

        with patch.object(
            self.collector.repos["rhel-9-baseos-rpms"], 'get_repodata', new_callable=AsyncMock
        ) as get_repodata:
            asyncio.run(self.collector._load_repos(set(self.repo_names), 'x86_64'))

            # Should skip calling get_repodata
            get_repodata.assert_not_awaited()
            self.collector.logger.info.assert_any_call(
                "Repos already loaded, skipping: rhel-9-appstream-rpms, rhel-9-baseos-rpms for arch x86_64"
            )

    def test_load_repos_fetches_new_ones(self):
        self.collector.loaded_repos = {}  # Start with nothing loaded

        repodata_mock_1 = MagicMock()
        repodata_mock_1.name = "rhel-9-baseos-rpms-x86_64"

        repodata_mock_2 = MagicMock()
        repodata_mock_2.name = "rhel-9-appstream-rpms-x86_64"

        with (
            patch.object(
                self.collector.repos["rhel-9-baseos-rpms"], 'get_repodata', new=AsyncMock(return_value=repodata_mock_1)
            ) as get1,
            patch.object(
                self.collector.repos["rhel-9-appstream-rpms"],
                'get_repodata',
                new=AsyncMock(return_value=repodata_mock_2),
            ) as get2,
        ):
            asyncio.run(self.collector._load_repos(set(self.repo_names), 'x86_64'))

            self.assertIn("rhel-9-baseos-rpms-x86_64", self.collector.loaded_repos)
            self.assertIn("rhel-9-appstream-rpms-x86_64", self.collector.loaded_repos)

            get1.assert_awaited_once_with('x86_64')
            get2.assert_awaited_once_with('x86_64')

    def test_load_repos_handles_partial_loading(self):
        # Preload one repo
        self.collector.loaded_repos = {"rhel-9-baseos-rpms-x86_64": MagicMock()}

        repodata_mock = MagicMock()
        repodata_mock.name = "rhel-9-appstream-rpms-x86_64"

        with patch.object(
            self.collector.repos["rhel-9-appstream-rpms"], 'get_repodata', new=AsyncMock(return_value=repodata_mock)
        ):
            asyncio.run(self.collector._load_repos(set(self.repo_names), 'x86_64'))

            self.assertIn("rhel-9-appstream-rpms-x86_64", self.collector.loaded_repos)
            self.collector.logger.info.assert_any_call(
                "Repos already loaded, skipping: rhel-9-baseos-rpms for arch x86_64"
            )
            self.collector.logger.info.assert_any_call("Finished loading repos: rhel-9-appstream-rpms for arch x86_64")

    def test_fetch_modules_info_empty_modules(self):
        """Test graceful handling when no modules are requested"""

        async def _test():
            result = await self.collector.fetch_modules_info(['x86_64'], {'repo1'}, set())
            self.assertEqual(result, {'x86_64': []})

        asyncio.run(_test())

    def test_fetch_modules_info_missing_modules(self):
        """Test graceful handling when requested modules are not found"""

        empty_repodata = MagicMock()
        empty_repodata.modules = []
        self.collector.loaded_repos = {"repo1-x86_64": empty_repodata}

        mock_repo = MagicMock()
        mock_repo.content_set.return_value = "repo1-content-set"
        mock_repo.baseurl.return_value = "https://example.com/"
        self.collector.repos._repos = {"repo1": mock_repo}

        async def _test():
            result = await self.collector.fetch_modules_info(['x86_64'], {'repo1'}, {'nonexistent'})
            # Should return empty list for the arch when modules not found
            self.assertEqual(result, {'x86_64': []})

        asyncio.run(_test())

    def test_fetch_modules_info_per_arch_basic(self):
        """Test basic module info fetching for a single architecture"""
        # Create sample modules
        nginx_module = RpmModule("nginx", "1.24", 123456, "abc123", "x86_64", set())
        perl_module = RpmModule("perl", "5.32", 789012, "def456", "x86_64", set())

        appstream_repodata = MagicMock()
        appstream_repodata.modules = [nginx_module, perl_module]
        appstream_repodata.modules_checksum = "sha256:abc123"
        appstream_repodata.modules_size = 12345
        appstream_repodata.modules_url = "https://example.com/repodata/modules.yaml.gz"

        self.collector.loaded_repos = {"rhel-9-appstream-rpms-x86_64": appstream_repodata}

        mock_repo = MagicMock()
        mock_repo.content_set.return_value = "rhel-9-appstream-rpms"
        mock_repo.baseurl.return_value = "https://example.com/"
        self.collector.repos._repos = {"rhel-9-appstream-rpms": mock_repo}

        result = self.collector._fetch_modules_info_per_arch({"nginx", "perl"}, {"rhel-9-appstream-rpms"}, "x86_64")

        # Should find one ModuleInfo entry for the repository
        self.assertEqual(len(result), 1)
        module_info = result[0]
        self.assertEqual(module_info.repoid, "rhel-9-appstream-rpms")
        self.assertEqual(module_info.checksum, "sha256:abc123")
        self.assertEqual(module_info.size, 12345)

    def test_fetch_modules_info_per_arch_deduplication(self):
        """Test that only one ModuleInfo is created per repository"""
        # Multiple modules from same repository
        nginx_module = RpmModule("nginx", "1.24", 123456, "abc123", "x86_64", set())
        nodejs_module = RpmModule("nodejs", "16", 456789, "def456", "x86_64", set())

        appstream_repodata = MagicMock()
        appstream_repodata.modules = [nginx_module, nodejs_module]
        appstream_repodata.modules_checksum = "sha256:abc123"
        appstream_repodata.modules_size = 12345
        appstream_repodata.modules_url = "https://example.com/repodata/modules.yaml.gz"

        self.collector.loaded_repos = {"rhel-9-appstream-rpms-x86_64": appstream_repodata}

        mock_repo = MagicMock()
        mock_repo.content_set.return_value = "rhel-9-appstream-rpms"
        mock_repo.baseurl.return_value = "https://example.com/"
        self.collector.repos._repos = {"rhel-9-appstream-rpms": mock_repo}

        # Request multiple modules from same repo
        result = self.collector._fetch_modules_info_per_arch({"nginx", "nodejs"}, {"rhel-9-appstream-rpms"}, "x86_64")

        # Should only have one ModuleInfo entry despite multiple modules
        self.assertEqual(len(result), 1)

    def test_fetch_modules_info_per_arch_stream_extraction(self):
        """Test that module:stream specifications are matched by name only"""
        # Repository has nginx module with name "nginx"
        nginx_module = RpmModule("nginx", "1.24", 123456, "abc123", "x86_64", set())
        perl_module = RpmModule("perl", "5.32", 789012, "def456", "x86_64", set())

        appstream_repodata = MagicMock()
        appstream_repodata.modules = [nginx_module, perl_module]
        appstream_repodata.modules_checksum = "sha256:abc123"
        appstream_repodata.modules_size = 12345
        appstream_repodata.modules_url = "https://example.com/repodata/modules.yaml.gz"

        self.collector.loaded_repos = {"rhel-9-appstream-rpms-x86_64": appstream_repodata}

        mock_repo = MagicMock()
        mock_repo.content_set.return_value = "rhel-9-appstream-rpms"
        mock_repo.baseurl.return_value = "https://example.com/"
        self.collector.repos._repos = {"rhel-9-appstream-rpms": mock_repo}

        # Request modules with stream specification - should match by name only
        result = self.collector._fetch_modules_info_per_arch(
            {"nginx:1.24", "perl:5.32"}, {"rhel-9-appstream-rpms"}, "x86_64"
        )

        # Should find the modules despite the stream specification in request
        self.assertEqual(len(result), 1)
        module_info = result[0]
        self.assertEqual(module_info.repoid, "rhel-9-appstream-rpms")


class TestRPMLockfileGenerator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.repos = MagicMock()
        self.logger = MagicMock()
        self.generator = RPMLockfileGenerator(self.repos, self.logger)
        self.arches = ['x86_64', 'aarch64']
        self.repos_set = {'repo1', 'repo2'}
        self.rpms = {'mypkg', 'otherpkg'}
        self.path = Path('/tmp')
        self.filename = 'rpms.lock.yml'

    @patch.object(RPMLockfileGenerator, '_write_yaml')
    async def test_generate_lockfile_proceeds_when_conditions_met(self, mock_write_yaml):
        """Test that lockfile generation proceeds when basic conditions are met"""
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}

        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        # Should always generate when conditions are met
        self.generator.builder.fetch_rpms_info.assert_called_once()
        mock_write_yaml.assert_called_once()

    async def test_generate_lockfile_with_modules(self):
        """Test lockfile generation includes module metadata when modules are configured"""
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg', 'evr': '1.0-1'}

        dummy_module_info = MagicMock()
        dummy_module_info.to_dict.return_value = {
            'url': 'https://example.com/modules.yaml',
            'repoid': 'rhel-9-appstream',
            'checksum': 'sha256:abc123',
            'size': 12345,
        }

        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})
        self.generator.builder.fetch_modules_info = AsyncMock(return_value={'x86_64': [dummy_module_info]})

        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.get_lockfile_modules_to_install = MagicMock(return_value={'python36'})
        mock_image_meta.get_arches.return_value = ['x86_64']

        with patch.object(self.generator, '_write_yaml') as mock_write:
            await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        self.generator.builder.fetch_rpms_info.assert_called_once()
        self.generator.builder.fetch_modules_info.assert_called_once()
        mock_write.assert_called_once()

        # Verify the lockfile structure contains module_metadata
        call_args = mock_write.call_args[0]
        lockfile_data = call_args[0]
        self.assertIn('arches', lockfile_data)
        arch_data = lockfile_data['arches'][0]
        self.assertIn('module_metadata', arch_data)
        self.assertEqual(len(arch_data['module_metadata']), 1)
        self.assertEqual(arch_data['module_metadata'][0]['repoid'], 'rhel-9-appstream')

    async def test_generate_lockfile_no_modules_configured(self):
        """Test lockfile generation with empty module_metadata when no modules are configured"""
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}

        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})
        self.generator.builder.fetch_modules_info = AsyncMock(return_value={'x86_64': []})

        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.get_lockfile_modules_to_install = MagicMock(return_value=set())
        mock_image_meta.get_arches.return_value = ['x86_64']

        with patch.object(self.generator, '_write_yaml') as mock_write:
            await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        call_args = mock_write.call_args[0]
        lockfile_data = call_args[0]
        arch_data = lockfile_data['arches'][0]
        self.assertIn('module_metadata', arch_data)
        self.assertEqual(len(arch_data['module_metadata']), 0)

    @patch('builtins.open', new_callable=mock_open)
    @patch('pathlib.Path.exists', return_value=False)
    def create_mock_image_meta(
        self,
        distgit_key: str,
        enabled: bool = True,
        has_repos: bool = True,
        has_rpms: bool = True,
        arches: list = None,
    ):
        """Helper to create mock ImageMetadata with common configuration"""
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = distgit_key
        mock_image_meta.is_lockfile_generation_enabled.return_value = enabled
        mock_image_meta.get_enabled_repos.return_value = {'repo1', 'repo2'} if has_repos else set()
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value={'pkg1', 'pkg2'} if has_rpms else set())
        mock_image_meta.get_arches.return_value = arches or ['x86_64', 'aarch64']
        return mock_image_meta

    @patch('opentelemetry.trace.get_current_span')
    async def test_ensure_repositories_loaded_with_mixed_images(self, mock_span):
        """Test repository loading with mix of images needing and not needing lockfiles"""
        mock_span_obj = MagicMock()
        mock_span.return_value = mock_span_obj

        # Create test images
        image1 = self.create_mock_image_meta('image1')
        image2 = self.create_mock_image_meta('image2')
        image3 = self.create_mock_image_meta('image3')
        image4 = self.create_mock_image_meta('image4')

        # Configure which images need generation
        image1.is_lockfile_generation_enabled.return_value = True
        image1.get_enabled_repos.return_value = {'repo1', 'repo2'}
        image1.get_lockfile_rpms_to_install = AsyncMock(return_value={'pkg1', 'pkg2'})

        image2.is_lockfile_generation_enabled.return_value = False  # Disabled

        image3.is_lockfile_generation_enabled.return_value = True
        image3.get_enabled_repos.return_value = set()  # No repos

        image4.is_lockfile_generation_enabled.return_value = True
        image4.get_enabled_repos.return_value = {'repo1', 'repo2'}
        image4.get_lockfile_rpms_to_install = AsyncMock(return_value={'pkg1', 'pkg2'})

        base_dir = Path('/tmp/base')
        with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
            await self.generator.ensure_repositories_loaded([image1, image2, image3, image4], base_dir)

            # Should load repos for image1 and image4 (both need generation)
            # image2 disabled, image3 has no repos
            self.assertEqual(mock_load.call_count, 2)  # Once per architecture

            # Verify telemetry attributes
            mock_span_obj.set_attribute.assert_any_call("lockfile.total_images", 4)
            mock_span_obj.set_attribute.assert_any_call("lockfile.images_needing_generation", 2)
            mock_span_obj.set_attribute.assert_any_call("lockfile.unique_repo_arch_pairs", 4)  # 2 repos * 2 arches

    @patch('opentelemetry.trace.get_current_span')
    async def test_ensure_repositories_loaded_no_images_need_generation(self, mock_span):
        """Test when no images need lockfile generation due to unchanged digests"""
        mock_span_obj = MagicMock()
        mock_span.return_value = mock_span_obj

        image1 = self.create_mock_image_meta('image1')
        image2 = self.create_mock_image_meta('image2')

        # Mock images that don't need lockfile generation (disabled)
        image1.is_lockfile_generation_enabled.return_value = False
        image2.is_lockfile_generation_enabled.return_value = False

        base_dir = Path('/tmp/base')
        with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
            await self.generator.ensure_repositories_loaded([image1, image2], base_dir)

            # Should not load any repos since images don't need generation
            mock_load.assert_not_called()

        # Verify telemetry
        mock_span_obj.set_attribute.assert_any_call("lockfile.total_images", 2)
        mock_span_obj.set_attribute.assert_any_call("lockfile.images_needing_generation", 0)
        mock_span_obj.set_attribute.assert_any_call("lockfile.unique_repo_arch_pairs", 0)

    @patch('opentelemetry.trace.get_current_span')
    async def test_ensure_repositories_loaded_unique_repo_arch_combinations(self, mock_span):
        """Test repository loading with overlapping repo/arch combinations"""
        mock_span_obj = MagicMock()
        mock_span.return_value = mock_span_obj

        # Different repo sets and architectures
        image1 = self.create_mock_image_meta('image1', arches=['x86_64'])
        image1.get_enabled_repos.return_value = {'repo1', 'repo2'}

        image2 = self.create_mock_image_meta('image2', arches=['aarch64'])
        image2.get_enabled_repos.return_value = {'repo1', 'repo3'}

        image3 = self.create_mock_image_meta('image3', arches=['x86_64', 'aarch64'])
        image3.get_enabled_repos.return_value = {'repo2'}

        # All images need generation
        base_dir = Path('/tmp/base')
        with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
            await self.generator.ensure_repositories_loaded([image1, image2, image3], base_dir)

            # Should call _load_repos for each unique architecture
            # x86_64: repos {repo1, repo2} | aarch64: repos {repo1, repo2, repo3}
            self.assertEqual(mock_load.call_count, 2)

            # Verify the correct repo sets were loaded
            call_args = mock_load.call_args_list
            x86_64_call = next(call for call in call_args if call[0][1] == 'x86_64')
            aarch64_call = next(call for call in call_args if call[0][1] == 'aarch64')

            self.assertEqual(x86_64_call[0][0], {'repo1', 'repo2'})
            self.assertEqual(aarch64_call[0][0], {'repo1', 'repo2', 'repo3'})

            # Total unique pairs: repo1-x86_64, repo2-x86_64, repo1-aarch64, repo2-aarch64, repo3-aarch64 = 5
            mock_span_obj.set_attribute.assert_any_call("lockfile.unique_repo_arch_pairs", 5)

    async def test_ensure_repositories_loaded_logs_image_decisions(self):
        """Test that image decisions are properly logged"""
        base_dir = Path('/tmp/base')

        image1 = self.create_mock_image_meta('needs-generation')
        image2 = self.create_mock_image_meta('skips-generation')

        # Mock one image that needs generation and one that doesn't
        image1.is_lockfile_generation_enabled.return_value = True
        image1.get_enabled_repos.return_value = {'repo1', 'repo2'}
        image1.get_lockfile_rpms_to_install = AsyncMock(return_value={'pkg1', 'pkg2'})

        image2.is_lockfile_generation_enabled.return_value = False  # Skip generation

        with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()):
            await self.generator.ensure_repositories_loaded([image1, image2], base_dir)

            # Verify logging
            self.logger.info.assert_any_call("Image needs-generation needs lockfile generation")
            self.logger.info.assert_any_call("Image skips-generation skipping lockfile generation")


class TestArtifactInfo(unittest.TestCase):
    def test_to_dict(self):
        artifact_info = ArtifactInfo(url="https://example.com/file.pem", checksum="sha256:abc123", filename="file.pem")
        expected = {"download_url": "https://example.com/file.pem", "checksum": "sha256:abc123", "filename": "file.pem"}
        self.assertEqual(artifact_info.to_dict(), expected)

    def test_creation(self):
        artifact_info = ArtifactInfo(
            url="https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem",
            checksum="sha256:def456",
            filename="Current-IT-Root-CAs.pem",
        )
        self.assertEqual(artifact_info.url, "https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem")
        self.assertEqual(artifact_info.checksum, "sha256:def456")
        self.assertEqual(artifact_info.filename, "Current-IT-Root-CAs.pem")


class TestArtifactLockfileGenerator(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.logger = MagicMock()
        self.runtime = MagicMock()
        self.generator = ArtifactLockfileGenerator(logger=self.logger, runtime=self.runtime)

    def test_extract_filename_from_url(self):
        """Test filename extraction from URLs"""
        # Test normal URL with filename
        url = "https://example.com/path/to/file.pem"
        result = self.generator._extract_filename_from_url(url)
        self.assertEqual(result, "file.pem")

        # Test URL ending with slash
        url_with_slash = "https://example.com/path/to/"
        result = self.generator._extract_filename_from_url(url_with_slash)
        self.assertEqual(result, "artifact")

        # Test URL with query parameters
        url_with_params = "https://example.com/cert.pem?version=latest"
        result = self.generator._extract_filename_from_url(url_with_params)
        self.assertEqual(result, "cert.pem?version=latest")

    def test_should_generate_artifact_lockfile(self):
        """Test lockfile generation decision logic"""
        mock_image_meta = MagicMock()
        mock_dest_dir = MagicMock()

        # Test enabled case
        mock_image_meta.is_artifact_lockfile_enabled.return_value = True
        result = self.generator.should_generate_artifact_lockfile(mock_image_meta, mock_dest_dir)
        self.assertTrue(result)

        # Test disabled case
        mock_image_meta.is_artifact_lockfile_enabled.return_value = False
        result = self.generator.should_generate_artifact_lockfile(mock_image_meta, mock_dest_dir)
        self.assertFalse(result)

    @patch.object(ArtifactLockfileGenerator, '_write_yaml')
    @patch('aiohttp.ClientSession')
    async def test_generate_artifact_lockfile_success(self, mock_session_class, mock_write_yaml):
        """Test successful artifact lockfile generation with URL list"""
        # Setup mocks
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.get_required_artifacts.return_value = [
            "https://example.com/cert1.pem",
            "https://example.com/cert2.pem",
        ]

        mock_dest_dir = Path("/tmp/test")

        # Mock session and download behavior
        mock_session = MagicMock()
        mock_session_class.return_value.__aenter__.return_value = mock_session

        mock_artifact_info1 = MagicMock()
        mock_artifact_info1.to_dict.return_value = {
            "download_url": "https://example.com/cert1.pem",
            "checksum": "sha256:abc1",
            "filename": "cert1.pem",
        }
        mock_artifact_info2 = MagicMock()
        mock_artifact_info2.to_dict.return_value = {
            "download_url": "https://example.com/cert2.pem",
            "checksum": "sha256:abc2",
            "filename": "cert2.pem",
        }

        # Mock the download and compute checksum method
        self.generator._download_and_compute_checksum = AsyncMock()
        self.generator._download_and_compute_checksum.side_effect = [mock_artifact_info1, mock_artifact_info2]

        # Mock should_generate_artifact_lockfile to return True
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=True)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify calls
        self.generator._download_and_compute_checksum.assert_any_call(
            mock_session, {'name': 'cert1.pem', 'url': 'https://example.com/cert1.pem'}
        )
        self.generator._download_and_compute_checksum.assert_any_call(
            mock_session, {'name': 'cert2.pem', 'url': 'https://example.com/cert2.pem'}
        )

        # Verify lockfile data structure
        expected_lockfile_data = {
            "metadata": {"version": "1.0"},
            "artifacts": [
                {"download_url": "https://example.com/cert1.pem", "checksum": "sha256:abc1", "filename": "cert1.pem"},
                {"download_url": "https://example.com/cert2.pem", "checksum": "sha256:abc2", "filename": "cert2.pem"},
            ],
        }
        mock_write_yaml.assert_called_once_with(expected_lockfile_data, mock_dest_dir / "artifacts.lock.yaml")

    async def test_generate_artifact_lockfile_disabled(self):
        """Test skipping when artifact lockfile is disabled"""
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_dest_dir = Path("/tmp/test")

        # Mock should_generate_artifact_lockfile to return False
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=False)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify early return
        self.logger.debug.assert_called_with("Skipping artifact lockfile generation for test-image")

    async def test_generate_artifact_lockfile_no_artifacts(self):
        """Test behavior when no artifacts are defined"""
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.get_required_artifacts.return_value = []
        mock_dest_dir = Path("/tmp/test")

        # Mock should_generate_artifact_lockfile to return True
        self.generator.should_generate_artifact_lockfile = MagicMock(return_value=True)

        # Execute
        await self.generator.generate_artifact_lockfile(mock_image_meta, mock_dest_dir)

        # Verify warning and early return
        self.logger.warning.assert_called_with("No artifacts defined for test-image")
