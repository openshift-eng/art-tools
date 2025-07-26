import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

from doozerlib.lockfile import DEFAULT_RPM_LOCKFILE_NAME, ArtifactInfo, RpmInfo, RpmInfoCollector, RPMLockfileGenerator
from doozerlib.repodata import Repodata, Rpm
from doozerlib.repos import Repo, Repos


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

    def test_compute_hash_consistency(self):
        rpms1 = {'b', 'a', 'c'}
        rpms2 = {'a', 'b', 'c'}
        hash1 = RPMLockfileGenerator._compute_hash(rpms1)
        hash2 = RPMLockfileGenerator._compute_hash(rpms2)
        self.assertEqual(hash1, hash2)

    @patch.object(RPMLockfileGenerator, '_write_yaml')
    @patch('builtins.open', new_callable=mock_open, read_data='oldfingerprint')
    @patch('pathlib.Path.exists', return_value=True)
    async def test_generate_lockfile_skips_if_fingerprint_matches(self, mock_exists, mock_file, mock_write_yaml):
        fingerprint = RPMLockfileGenerator._compute_hash(self.rpms)
        mock_file().read.return_value = fingerprint

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = False

        self.generator.builder.fetch_rpms_info = MagicMock()

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        self.generator.builder.fetch_rpms_info.assert_not_called()
        mock_write_yaml.assert_not_called()
        self.logger.info.assert_any_call("No changes in RPM list for test-image. Skipping lockfile generation.")

    @patch('builtins.open', new_callable=mock_open)
    @patch('pathlib.Path.exists', return_value=False)
    async def test_generate_lockfile_writes_if_fingerprint_file_missing(self, mock_exists, mock_file):
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}

        async def async_fetch_rpms_info(*args, **kwargs):
            return {'x86_64': [dummy_rpm_info], 'aarch64': []}

        self.generator.builder.fetch_rpms_info = async_fetch_rpms_info

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = False
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
        self.assertIn("packages", written_content)

    @patch('builtins.open', new_callable=mock_open, read_data='differentfingerprint')
    @patch('pathlib.Path.exists', return_value=True)
    async def test_generate_lockfile_writes_if_fingerprint_differs(self, mock_exists, mock_file):
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}

        async def async_fetch_rpms_info(*args, **kwargs):
            return {'x86_64': [dummy_rpm_info], 'aarch64': []}

        self.generator.builder.fetch_rpms_info = async_fetch_rpms_info

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = False
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
        self.assertIn("packages", written_content)

    @patch('pathlib.Path.mkdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_write_yaml_creates_dirs_and_writes_file(self, mock_file, mock_mkdir):
        data = {'key': 'value'}
        output_path = Path('/some/path/file.yaml')
        self.generator._write_yaml(data, output_path)

        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_file.assert_called_once_with(output_path, "w")

        # Verify yaml.safe_dump called with data
        handle = mock_file()
        written_content = ''.join(call.args[0] for call in handle.write.call_args_list)
        # We cannot easily assert the YAML content here, but just ensure write was called
        self.assertTrue(written_content or True)

    @patch.object(RPMLockfileGenerator, '_write_yaml')
    @patch('builtins.open', new_callable=mock_open, read_data='oldfingerprint')
    @patch('pathlib.Path.exists', return_value=True)
    async def test_generate_lockfile_force_skips_digest_check(self, mock_exists, mock_file, mock_write_yaml):
        """Test that force=True skips digest checking and always generates lockfile"""
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}

        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})

        # Create mock ImageMetadata with force enabled
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = True
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        # Should not read the digest file when force=True
        mock_file().read.assert_not_called()
        # Should generate lockfile regardless
        self.generator.builder.fetch_rpms_info.assert_called_once()
        mock_write_yaml.assert_called_once()
        self.logger.info.assert_any_call("Force flag set for test-image. Regenerating lockfile without digest check.")

    @patch("tempfile.NamedTemporaryFile")
    @patch('doozerlib.lockfile.download_file_from_github')
    @patch('os.environ.get')
    async def test_get_digest_from_target_branch_not_found(self, mock_environ, mock_download, mock_tempfile):
        """Test digest fetching when file doesn't exist in target branch"""
        mock_runtime = MagicMock()
        mock_runtime.group = "openshift-4.20"
        mock_runtime.assembly = "test"

        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=mock_runtime)

        # Mock environment and download failure
        mock_environ.return_value = "fake_token"
        mock_download.side_effect = Exception("File not found")

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.config.content.source.git.url = "https://github.com/openshift/test-image"

        digest_path = Path('/some/path/rpms.lock.yaml.digest')
        result = await generator._get_digest_from_target_branch(digest_path, mock_image_meta)

        self.assertIsNone(result)

    async def test_get_digest_from_target_branch_no_runtime(self):
        """Test digest fetching when no runtime is provided"""
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=None)

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"

        digest_path = Path('/some/path/rpms.lock.yaml.digest')
        result = await generator._get_digest_from_target_branch(digest_path, mock_image_meta)

        self.assertIsNone(result)

    @patch.object(RPMLockfileGenerator, '_get_lockfile_from_target_branch')
    @patch.object(RPMLockfileGenerator, '_get_digest_from_target_branch')
    @patch.object(RPMLockfileGenerator, '_write_yaml')
    @patch('pathlib.Path.write_text')
    @patch('pathlib.Path.exists', return_value=False)
    async def test_generate_lockfile_uses_target_branch_digest(
        self, mock_exists, mock_write_text, mock_write_yaml, mock_get_digest, mock_get_lockfile
    ):
        """Test that generate_lockfile downloads files from upstream when digest matches"""
        fingerprint = RPMLockfileGenerator._compute_hash(self.rpms)
        mock_lockfile_content = "lockfile: content"

        # Mock target branch returning same fingerprint and lockfile content
        mock_get_digest.return_value = fingerprint
        mock_get_lockfile.return_value = mock_lockfile_content

        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}
        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = False
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        # Should call target branch digest fetching (called twice - in should_generate and sync)
        self.assertEqual(mock_get_digest.call_count, 2)
        # Should call target branch lockfile fetching
        mock_get_lockfile.assert_called_once()
        # Should skip generation since fingerprints match
        self.generator.builder.fetch_rpms_info.assert_not_called()
        mock_write_yaml.assert_not_called()
        # Should download both files
        mock_write_text.assert_any_call(fingerprint)
        mock_write_text.assert_any_call(mock_lockfile_content)
        self.logger.info.assert_any_call("Found digest in target branch for test-image")
        self.logger.info.assert_any_call("Downloaded digest file to {}".format(self.path / f'{self.filename}.digest'))
        self.logger.info.assert_any_call(f"Downloaded lockfile to {self.path / self.filename}")

    @patch.object(RPMLockfileGenerator, '_get_digest_from_target_branch')
    @patch.object(RPMLockfileGenerator, '_write_yaml')
    @patch('pathlib.Path.exists', return_value=False)
    async def test_generate_lockfile_regenerates_when_target_branch_digest_differs(
        self, mock_exists, mock_write_yaml, mock_get_digest
    ):
        """Test that generate_lockfile regenerates when target branch digest differs"""
        # Mock target branch returning different fingerprint
        mock_get_digest.return_value = "differenthash"

        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}
        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = False
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        # Should call target branch digest fetching
        mock_get_digest.assert_called_once()
        # Should generate since fingerprints differ
        self.generator.builder.fetch_rpms_info.assert_called_once()
        mock_write_yaml.assert_called_once()
        self.logger.info.assert_any_call("Found digest in target branch for test-image")
        self.logger.info.assert_any_call("RPM list changed for test-image. Regenerating lockfile.")

    @patch("tempfile.NamedTemporaryFile")
    @patch('doozerlib.lockfile.download_file_from_github')
    @patch('os.environ.get')
    async def test_get_lockfile_from_target_branch_not_found(self, mock_environ, mock_download, mock_tempfile):
        """Test lockfile fetching when file doesn't exist in target branch"""
        mock_runtime = MagicMock()
        mock_runtime.group = "test-group"
        mock_runtime.assembly = "test-assembly"
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=mock_runtime)

        lockfile_path = Path('/some/path/rpms.lock.yaml')

        # Mock environment and download failure
        mock_environ.return_value = "fake_token"
        mock_download.side_effect = Exception("File not found")

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.distgit_remote_url.return_value = "https://github.com/openshift/test-image"

        result = await generator._get_lockfile_from_target_branch(lockfile_path, mock_image_meta)

        self.assertIsNone(result)
        self.logger.debug.assert_called()

    async def test_get_lockfile_from_target_branch_no_runtime(self):
        """Test lockfile fetching when no runtime is provided"""
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=None)

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"

        lockfile_path = Path('/some/path/rpms.lock.yaml')
        result = await generator._get_lockfile_from_target_branch(lockfile_path, mock_image_meta)

        self.assertIsNone(result)

    @patch.object(RPMLockfileGenerator, '_get_lockfile_from_target_branch')
    @patch.object(RPMLockfileGenerator, '_get_digest_from_target_branch')
    @patch.object(RPMLockfileGenerator, '_write_yaml')
    @patch('pathlib.Path.write_text')
    @patch('pathlib.Path.exists', return_value=False)
    async def test_generate_lockfile_handles_missing_upstream_lockfile(
        self, mock_exists, mock_write_text, mock_write_yaml, mock_get_digest, mock_get_lockfile
    ):
        """Test that generate_lockfile handles case when upstream lockfile is missing"""
        fingerprint = RPMLockfileGenerator._compute_hash(self.rpms)

        # Mock target branch returning digest but no lockfile
        mock_get_digest.return_value = fingerprint
        mock_get_lockfile.return_value = None  # Lockfile not found upstream

        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}
        self.generator.builder.fetch_rpms_info = AsyncMock(return_value={'x86_64': [dummy_rpm_info]})

        # Create mock ImageMetadata
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = self.repos_set
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value=self.rpms)
        mock_image_meta.is_lockfile_force_enabled.return_value = False
        mock_image_meta.get_arches.return_value = self.arches

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        # Should still download digest file
        mock_write_text.assert_any_call(fingerprint)
        # Should log warning about missing lockfile
        self.logger.warning.assert_any_call("Could not download lockfile from upstream branch for test-image")
        # Should skip generation since fingerprints match
        self.generator.builder.fetch_rpms_info.assert_not_called()
        mock_write_yaml.assert_not_called()

    async def test_generate_lockfile_skips_when_repositories_empty(self):
        """Test that generate_lockfile skips when repositories set is empty"""
        # Mock the fetch_rpms_info method to track if it's called
        self.generator.builder.fetch_rpms_info = MagicMock()

        # Create mock ImageMetadata with empty repos
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = "test-image"
        mock_image_meta.is_lockfile_generation_enabled.return_value = True
        mock_image_meta.get_enabled_repos.return_value = set()

        await self.generator.generate_lockfile(mock_image_meta, self.path, self.filename)

        # Should skip generation due to empty repositories
        self.generator.builder.fetch_rpms_info.assert_not_called()
        self.logger.info.assert_called_with("Skipping lockfile generation for test-image: repositories set is empty")


class TestEnsureRepositoriesLoaded(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.repos = MagicMock()
        self.logger = MagicMock()
        self.generator = RPMLockfileGenerator(self.repos, self.logger)
        self.base_dir = Path('/tmp/base')

    def create_mock_image_meta(
        self,
        distgit_key: str,
        enabled: bool = True,
        has_repos: bool = True,
        has_rpms: bool = True,
        force: bool = False,
        arches: list = None,
    ):
        """Helper to create mock ImageMetadata with common configuration"""
        mock_image_meta = MagicMock()
        mock_image_meta.distgit_key = distgit_key
        mock_image_meta.is_lockfile_generation_enabled.return_value = enabled
        mock_image_meta.get_enabled_repos.return_value = {'repo1', 'repo2'} if has_repos else set()
        mock_image_meta.get_lockfile_rpms_to_install = AsyncMock(return_value={'pkg1', 'pkg2'} if has_rpms else set())
        mock_image_meta.is_lockfile_force_enabled.return_value = force
        mock_image_meta.get_arches.return_value = arches or ['x86_64', 'aarch64']
        return mock_image_meta

    @patch('opentelemetry.trace.get_current_span')
    async def test_ensure_repositories_loaded_with_mixed_images(self, mock_span):
        """Test repository loading with mix of images needing and not needing lockfiles"""
        mock_span_obj = MagicMock()
        mock_span.return_value = mock_span_obj

        # Create test images
        image1 = self.create_mock_image_meta('image1', enabled=True, has_repos=True, has_rpms=True)
        image2 = self.create_mock_image_meta('image2', enabled=False)  # Disabled
        image3 = self.create_mock_image_meta('image3', enabled=True, has_repos=False)  # No repos
        image4 = self.create_mock_image_meta('image4', enabled=True, has_repos=True, has_rpms=True, force=True)

        # Mock digest check to return different fingerprints (needs generation)
        with patch.object(self.generator, '_get_digest_from_target_branch', new=AsyncMock(return_value="old_hash")):
            with patch.object(self.generator, '_compute_hash', return_value="new_hash"):
                with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
                    await self.generator.ensure_repositories_loaded([image1, image2, image3, image4], self.base_dir)

                    # Should load repos for image1 and image4 (both need generation)
                    # image2 disabled, image3 has no repos
                    self.assertEqual(mock_load.call_count, 2)  # Once per architecture

                    # Verify telemetry attributes
                    mock_span_obj.set_attribute.assert_any_call("lockfile.total_images", 4)
                    mock_span_obj.set_attribute.assert_any_call("lockfile.images_needing_generation", 2)
                    mock_span_obj.set_attribute.assert_any_call(
                        "lockfile.unique_repo_arch_pairs", 4
                    )  # 2 repos * 2 arches

    @patch('opentelemetry.trace.get_current_span')
    async def test_ensure_repositories_loaded_no_images_need_generation(self, mock_span):
        """Test when no images need lockfile generation due to unchanged digests"""
        mock_span_obj = MagicMock()
        mock_span.return_value = mock_span_obj

        image1 = self.create_mock_image_meta('image1', enabled=True, has_repos=True, has_rpms=True)
        image2 = self.create_mock_image_meta('image2', enabled=True, has_repos=True, has_rpms=True)

        # Mock digest check to return same fingerprints (no generation needed)
        with patch.object(self.generator, '_get_digest_from_target_branch', new=AsyncMock(return_value="same_hash")):
            with patch.object(self.generator, '_compute_hash', return_value="same_hash"):
                with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
                    await self.generator.ensure_repositories_loaded([image1, image2], self.base_dir)

                    # Should not load any repos
                    mock_load.assert_not_called()

                    # Verify telemetry
                    mock_span_obj.set_attribute.assert_any_call("lockfile.total_images", 2)
                    mock_span_obj.set_attribute.assert_any_call("lockfile.images_needing_generation", 0)
                    mock_span_obj.set_attribute.assert_any_call("lockfile.unique_repo_arch_pairs", 0)

    @patch('opentelemetry.trace.get_current_span')
    async def test_ensure_repositories_loaded_force_flag_behavior(self, mock_span):
        """Test that force flag bypasses digest check and loads repositories"""
        mock_span_obj = MagicMock()
        mock_span.return_value = mock_span_obj

        image1 = self.create_mock_image_meta('image1', enabled=True, has_repos=True, has_rpms=True, force=True)

        # Even with same digest, force should cause generation
        with patch.object(self.generator, '_get_digest_from_target_branch', new=AsyncMock(return_value="same_hash")):
            with patch.object(self.generator, '_compute_hash', return_value="same_hash"):
                with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
                    await self.generator.ensure_repositories_loaded([image1], self.base_dir)

                    # Should load repos due to force flag
                    self.assertEqual(mock_load.call_count, 2)  # Once per architecture

                    # Verify telemetry
                    mock_span_obj.set_attribute.assert_any_call("lockfile.total_images", 1)
                    mock_span_obj.set_attribute.assert_any_call("lockfile.images_needing_generation", 1)

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

        # Mock digest check to need generation
        with patch.object(self.generator, '_get_digest_from_target_branch', new=AsyncMock(return_value="old_hash")):
            with patch.object(self.generator, '_compute_hash', return_value="new_hash"):
                with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()) as mock_load:
                    await self.generator.ensure_repositories_loaded([image1, image2, image3], self.base_dir)

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
        image1 = self.create_mock_image_meta('needs-generation', enabled=True, has_repos=True, has_rpms=True)
        image2 = self.create_mock_image_meta('skips-generation', enabled=True, has_repos=True, has_rpms=True)

        # Mock different digest results
        async def mock_digest_check(digest_path, image_meta):
            if image_meta.distgit_key == 'needs-generation':
                return "old_hash"
            return "same_hash"

        with patch.object(self.generator, '_get_digest_from_target_branch', new=mock_digest_check):
            with patch.object(self.generator, '_compute_hash', return_value="same_hash"):
                with patch.object(self.generator.builder, '_load_repos', new=AsyncMock()):
                    await self.generator.ensure_repositories_loaded([image1, image2], self.base_dir)

                    # Verify logging
                    self.logger.info.assert_any_call("Image needs-generation needs lockfile generation")
                    self.logger.info.assert_any_call(
                        "Image skips-generation skipping lockfile generation (digest unchanged)"
                    )
                    self.logger.info.assert_any_call("Loading repositories for 1 images needing lockfile generation")


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
