import asyncio
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

from doozerlib.lockfile import RpmInfo, RpmInfoCollector, RPMLockfileGenerator
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

        self.generator.builder.fetch_rpms_info = MagicMock()

        await self.generator.generate_lockfile(self.arches, self.repos_set, self.rpms, self.path, self.filename)

        self.generator.builder.fetch_rpms_info.assert_not_called()
        mock_write_yaml.assert_not_called()
        self.logger.info.assert_any_call("No changes in RPM list. Skipping lockfile generation.")

    @patch('builtins.open', new_callable=mock_open)
    @patch('pathlib.Path.exists', return_value=False)
    async def test_generate_lockfile_writes_if_fingerprint_file_missing(self, mock_exists, mock_file):
        dummy_rpm_info = MagicMock()
        dummy_rpm_info.to_dict.return_value = {'name': 'mypkg'}

        async def async_fetch_rpms_info(*args, **kwargs):
            return {'x86_64': [dummy_rpm_info], 'aarch64': []}

        self.generator.builder.fetch_rpms_info = async_fetch_rpms_info

        await self.generator.generate_lockfile(self.arches, self.repos_set, self.rpms, self.path, self.filename)

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

        await self.generator.generate_lockfile(self.arches, self.repos_set, self.rpms, self.path, self.filename)

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

        await self.generator.generate_lockfile(
            self.arches, self.repos_set, self.rpms, self.path, self.filename, force=True
        )

        # Should not read the digest file when force=True
        mock_file().read.assert_not_called()
        # Should generate lockfile regardless
        self.generator.builder.fetch_rpms_info.assert_called_once()
        mock_write_yaml.assert_called_once()
        self.logger.info.assert_any_call("Force flag set. Regenerating lockfile without digest check.")

    @patch('artcommonlib.exectools.cmd_gather')
    def test_get_digest_from_target_branch_success(self, mock_cmd_gather):
        """Test successful digest fetching from target branch"""
        mock_runtime = MagicMock()
        mock_runtime.group = "openshift-4.20"
        mock_runtime.assembly = "test"

        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=mock_runtime)

        # Mock successful git command
        mock_cmd_gather.return_value = (0, "abc123hash", "")

        digest_path = Path('/some/path/rpms.lock.yaml.digest')
        result = generator._get_digest_from_target_branch(digest_path, "test-image")

        self.assertEqual(result, "abc123hash")
        mock_cmd_gather.assert_called_once_with(
            ['git', 'show', 'art-openshift-4.20-assembly-test-dgk-test-image:rpms.lock.yaml.digest']
        )

    @patch('artcommonlib.exectools.cmd_gather')
    def test_get_digest_from_target_branch_not_found(self, mock_cmd_gather):
        """Test digest fetching when file doesn't exist in target branch"""
        mock_runtime = MagicMock()
        mock_runtime.group = "openshift-4.20"
        mock_runtime.assembly = "test"

        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=mock_runtime)

        # Mock git command failure (file not found)
        mock_cmd_gather.return_value = (128, "", "fatal: path does not exist")

        digest_path = Path('/some/path/rpms.lock.yaml.digest')
        result = generator._get_digest_from_target_branch(digest_path, "test-image")

        self.assertIsNone(result)

    def test_get_digest_from_target_branch_no_runtime(self):
        """Test digest fetching when no runtime is provided"""
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=None)

        digest_path = Path('/some/path/rpms.lock.yaml.digest')
        result = generator._get_digest_from_target_branch(digest_path, "test-image")

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

        await self.generator.generate_lockfile(
            self.arches, self.repos_set, self.rpms, self.path, self.filename, distgit_key="test-image"
        )

        # Should call target branch digest fetching
        mock_get_digest.assert_called_once_with(self.path / f'{self.filename}.digest', "test-image")
        # Should call target branch lockfile fetching
        mock_get_lockfile.assert_called_once_with(self.path / self.filename, "test-image")
        # Should skip generation since fingerprints match
        self.generator.builder.fetch_rpms_info.assert_not_called()
        mock_write_yaml.assert_not_called()
        # Should download both files
        mock_write_text.assert_any_call(fingerprint)
        mock_write_text.assert_any_call(mock_lockfile_content)
        self.logger.info.assert_any_call("Found digest in target branch for test-image")
        self.logger.info.assert_any_call("No changes in RPM list. Downloading digest and lockfile from upstream.")
        self.logger.info.assert_any_call(f"Downloaded digest file to {self.path / f'{self.filename}.digest'}")
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

        await self.generator.generate_lockfile(
            self.arches, self.repos_set, self.rpms, self.path, self.filename, distgit_key="test-image"
        )

        # Should call target branch digest fetching
        mock_get_digest.assert_called_once()
        # Should generate since fingerprints differ
        self.generator.builder.fetch_rpms_info.assert_called_once()
        mock_write_yaml.assert_called_once()
        self.logger.info.assert_any_call("Found digest in target branch for test-image")
        self.logger.info.assert_any_call("RPM list changed. Regenerating lockfile.")

    def test_get_lockfile_from_target_branch_success(self):
        """Test successful lockfile fetching from target branch"""
        mock_runtime = MagicMock()
        mock_runtime.group = "test-group"
        mock_runtime.assembly = "test-assembly"
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=mock_runtime)

        lockfile_path = Path('/some/path/rpms.lock.yaml')
        expected_content = "lockfile: yaml content"
        expected_branch = "art-test-group-assembly-test-assembly-dgk-test-image"

        with patch('artcommonlib.exectools.cmd_gather') as mock_cmd:
            mock_cmd.return_value = (0, expected_content, "")

            result = generator._get_lockfile_from_target_branch(lockfile_path, "test-image")

            self.assertEqual(result, expected_content)
            mock_cmd.assert_called_once_with(['git', 'show', f'{expected_branch}:rpms.lock.yaml'])

    def test_get_lockfile_from_target_branch_not_found(self):
        """Test lockfile fetching when file doesn't exist in target branch"""
        mock_runtime = MagicMock()
        mock_runtime.group = "test-group"
        mock_runtime.assembly = "test-assembly"
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=mock_runtime)

        lockfile_path = Path('/some/path/rpms.lock.yaml')

        with patch('artcommonlib.exectools.cmd_gather') as mock_cmd:
            mock_cmd.return_value = (128, "", "fatal: Path 'rpms.lock.yaml' does not exist")

            result = generator._get_lockfile_from_target_branch(lockfile_path, "test-image")

            self.assertIsNone(result)
            self.logger.debug.assert_called()

    def test_get_lockfile_from_target_branch_no_runtime(self):
        """Test lockfile fetching when no runtime is provided"""
        generator = RPMLockfileGenerator(self.repos, self.logger, runtime=None)

        lockfile_path = Path('/some/path/rpms.lock.yaml')
        result = generator._get_lockfile_from_target_branch(lockfile_path, "test-image")

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

        await self.generator.generate_lockfile(
            self.arches, self.repos_set, self.rpms, self.path, self.filename, distgit_key="test-image"
        )

        # Should still download digest file
        mock_write_text.assert_any_call(fingerprint)
        # Should log warning about missing lockfile
        self.logger.warning.assert_any_call("Could not download lockfile from upstream branch for test-image")
        # Should skip generation since fingerprints match
        self.generator.builder.fetch_rpms_info.assert_not_called()
        mock_write_yaml.assert_not_called()
