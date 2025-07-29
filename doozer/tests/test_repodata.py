from io import StringIO
from typing import Optional
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import defusedxml.ElementTree as ET
from doozerlib.repodata import OutdatedRPMFinder, Repodata, RepodataLoader, Rpm, RpmModule
from ruamel.yaml import YAML


class TestRpm(TestCase):
    def test_nevra(self):
        rpm = Rpm(
            name="foo",
            epoch=1,
            version="1.2.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")

    def test_nvr(self):
        rpm = Rpm(
            name="foo",
            epoch=1,
            version="1.2.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        self.assertEqual(rpm.nvr, "foo-1.2.3-1.el9")

    def test_compare(self):
        a = Rpm(
            name="foo",
            epoch=1,
            version="1.2.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        b = Rpm(
            name="foo",
            epoch=1,
            version="1.10.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.10.3-1.el9.src.rpm",
        )
        self.assertTrue(a.compare(b) < 0)
        a = Rpm(
            name="foo",
            epoch=2,
            version="1.2.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        b = Rpm(
            name="foo",
            epoch=1,
            version="1.10.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.10.3-1.el9.src.rpm",
        )
        self.assertTrue(a.compare(b) > 0)
        a = Rpm(
            name="foo",
            epoch=0,
            version="1.2.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        b = Rpm(
            name="foo",
            epoch=0,
            version="1.2.3",
            release="1.el9",
            arch="aarch64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        self.assertTrue(a.compare(b) == 0)

    def test_to_dict(self):
        rpm = Rpm(
            name="foo",
            epoch=1,
            version="1.2.3",
            release="1.el9",
            arch="x86_64",
            checksum="dummy",
            size=123,
            location="foo.rpm",
            sourcerpm="foo-1.2.3-1.el9.src.rpm",
        )
        expected = {
            "name": "foo",
            "epoch": "1",
            "version": "1.2.3",
            "release": "1.el9",
            "arch": "x86_64",
            "checksum": "dummy",
            "size": "123",
            "location": "foo.rpm",
            "sourcerpm": "foo-1.2.3-1.el9.src.rpm",
            "nvr": "foo-1.2.3-1.el9",
            "nevra": "foo-1:1.2.3-1.el9.x86_64",
        }
        self.assertEqual(rpm.to_dict(), expected)

    def test_from_nevra(self):
        rpm = Rpm.from_nevra("foo-1:1.2.3-1.el9.x86_64")
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")

    def test_from_dict(self):
        rpm = Rpm.from_dict(
            {
                "name": "foo",
                "epoch": "1",
                "version": "1.2.3",
                "release": "1.el9",
                "arch": "x86_64",
            }
        )
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")

    def test_from_metadata(self):
        xml = """
        <package type="rpm" xmlns="http://linux.duke.edu/metadata/common" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
            <name>foo</name>
            <checksum pkgid="YES" type="sha256">b67e2b</checksum>
            <arch>x86_64</arch>
            <size archive="67404" installed="60614" package="80204" />
            <location href="Packages/l/foo-2.3.1-4.el9.aarch64.rpm" />
            <version epoch="1" rel="1.el9" ver="1.2.3" />
            <format>
                <rpm:sourcerpm>foo-2.3.1-4.el9.src.rpm</rpm:sourcerpm>
            </format>
        </package>
        """
        metadata = ET.fromstring(xml)
        rpm = Rpm.from_metadata(metadata)
        self.assertEqual(rpm.nevra, "foo-1:1.2.3-1.el9.x86_64")
        self.assertEqual(rpm.size, 80204)
        self.assertEqual(rpm.location, "Packages/l/foo-2.3.1-4.el9.aarch64.rpm")
        self.assertEqual(rpm.version, "1.2.3")
        self.assertEqual(rpm.sourcerpm, "foo-2.3.1-4.el9.src.rpm")


class TestRpmModule(TestCase):
    def test_from_metadata(self):
        yaml_str = """
        document: modulemd
        version: 2
        data:
            name: container-tools
            stream: rhel8
            version: 8030020201124131330
            context: 830d479e
            arch: x86_64
        """
        metadata = YAML(typ="safe").load(StringIO(yaml_str))
        module = RpmModule.from_metadata(metadata)
        self.assertEqual(module.nsvca, "container-tools:rhel8:8030020201124131330:830d479e:x86_64")


class TestRepodata(TestCase):
    def setUp(self):
        # Setup for get_rpms tests
        self.rpms = [
            Rpm(
                name="foo",
                epoch=1,
                version="1.2.3",
                checksum="abc",
                size=123,
                location="foo.rpm",
                sourcerpm="foo-1.2.3-1.el9.src.rpm",
                release="1.el9",
                arch="x86_64",
            ),
            Rpm(
                name="bar",
                epoch=0,
                version="2.0.0",
                checksum="def",
                size=456,
                location="bar.rpm",
                sourcerpm="bar-2.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="x86_64",
            ),
        ]
        self.repodata = Repodata(name="testrepo", primary_rpms=self.rpms, modules=[])

    def test_from_metadatas(self):
        repo_name = "test-x86_64"
        primary_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <metadata packages="2" xmlns="http://linux.duke.edu/metadata/common" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
        <package type="rpm">
            <name>foo</name>
            <checksum pkgid="YES" type="sha256">sfy8dfa</checksum>
            <size archive="3" installed="4" package="6" />
            <location href="Packages/l/foo-1.2.3-1.el9.x86_64.rpm" />
            <arch>x86_64</arch>
            <version epoch="1" rel="1.el9" ver="1.2.3" />
            <format>
                <rpm:sourcerpm>foo-1.2.3-1.el9.src.rpm</rpm:sourcerpm>
            </format>
        </package>
        <package type="rpm">
            <name>bar</name>
            <checksum pkgid="YES" type="sha256">barcsum</checksum>
            <size archive="10" installed="20" package="30" />
            <location href="Packages/l/bar-2.2.3-1.el9.x86_64.rpm" />
            <arch>x86_64</arch>
            <version epoch="1" rel="1.el9" ver="2.2.3" />
            <format>
                <rpm:sourcerpm>bar-2.2.3-1.el9.src.rpm</rpm:sourcerpm>
            </format>
        </package>
    </metadata>
    """
        modules_yaml = """
---
document: modulemd
version: 2
data:
    name: aaa
    stream: rhel8
    version: 1
    context: deadbeef
    arch: x86_64
---
document: modulemd
version: 2
data:
    name: bbb
    stream: rhel9
    version: 2
    context: beefdead
    arch: x86_64
"""
        repodata = Repodata.from_metadatas(
            repo_name, ET.fromstring(primary_xml), YAML(typ="safe").load_all(StringIO(modules_yaml))
        )
        self.assertEqual(repodata.name, repo_name)
        self.assertEqual(
            [rpm.nevra for rpm in repodata.primary_rpms], ["foo-1:1.2.3-1.el9.x86_64", "bar-1:2.2.3-1.el9.x86_64"]
        )
        self.assertEqual(
            [m.nsvca for m in repodata.modules], ['aaa:rhel8:1:deadbeef:x86_64', 'bbb:rhel9:2:beefdead:x86_64']
        )

    def test_get_rpms_by_name_found(self):
        found, not_found = self.repodata.get_rpms("foo", arch="x86_64")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].name, "foo")
        self.assertEqual(not_found, [])

    def test_get_rpms_by_name_not_found(self):
        found, not_found = self.repodata.get_rpms("baz", arch="x86_64")
        self.assertEqual(found, [])
        self.assertEqual(not_found, ["baz"])

    def test_get_rpms_by_nvr_found(self):
        nvr = self.rpms[1].nvr
        found, not_found = self.repodata.get_rpms(nvr, arch="x86_64")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].name, "bar")
        self.assertEqual(not_found, [])

    def test_get_rpms_multiple_items(self):
        items = ["foo", self.rpms[1].nvr, "baz"]
        found, not_found = self.repodata.get_rpms(items, arch="x86_64")
        self.assertEqual({rpm.name for rpm in found}, {"foo", "bar"})
        self.assertEqual(not_found, ["baz"])

    def test_get_rpms_duplicates(self):
        items = ["foo", "foo"]
        found, not_found = self.repodata.get_rpms(items, arch="x86_64")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].name, "foo")
        self.assertEqual(not_found, [])

    def test_get_rpms_with_arch_matching(self):
        # Add RPMs with different architectures for testing
        rpms_with_arch = [
            Rpm(
                name="multiarch",
                epoch=1,
                version="1.0.0",
                checksum="abc1",
                size=100,
                location="multiarch-x86_64.rpm",
                sourcerpm="multiarch-1.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="x86_64",
            ),
            Rpm(
                name="multiarch",
                epoch=1,
                version="1.0.0",
                checksum="abc2",
                size=100,
                location="multiarch-aarch64.rpm",
                sourcerpm="multiarch-1.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="aarch64",
            ),
            Rpm(
                name="multiarch",
                epoch=1,
                version="1.0.0",
                checksum="abc4",
                size=100,
                location="multiarch-noarch.rpm",
                sourcerpm="multiarch-1.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="noarch",
            ),
            Rpm(
                name="noarch-pkg",
                epoch=1,
                version="1.0.0",
                checksum="abc3",
                size=100,
                location="noarch-pkg.rpm",
                sourcerpm="noarch-pkg-1.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="noarch",
            ),
        ]
        repodata_with_arch = Repodata(name="testrepo", primary_rpms=self.rpms + rpms_with_arch, modules=[])

        # Test exact architecture match - should return arch-specific AND noarch
        found, not_found = repodata_with_arch.get_rpms("multiarch", arch="x86_64")
        self.assertEqual(len(found), 2)  # x86_64 + noarch
        found_arches = {rpm.arch for rpm in found}
        self.assertEqual(found_arches, {"x86_64", "noarch"})
        self.assertEqual(not_found, [])

        # Test arch with only noarch available
        found, not_found = repodata_with_arch.get_rpms("noarch-pkg", arch="x86_64")
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].arch, "noarch")
        self.assertEqual(not_found, [])

        # Test another arch - should return arch-specific AND noarch (if they exist)
        found, not_found = repodata_with_arch.get_rpms("multiarch", arch="aarch64")
        self.assertEqual(len(found), 2)  # aarch64 + noarch
        found_arches = {rpm.arch for rpm in found}
        self.assertEqual(found_arches, {"aarch64", "noarch"})
        self.assertEqual(not_found, [])

        # Test arch with no matches - should return only noarch (no fallback to other arches)
        found, not_found = repodata_with_arch.get_rpms("multiarch", arch="s390x")
        self.assertEqual(len(found), 1)  # only noarch
        found_arches = {rpm.arch for rpm in found}
        self.assertEqual(found_arches, {"noarch"})
        self.assertEqual(not_found, [])

    def test_get_rpms_with_arch_no_match(self):
        # Test when no RPM matches the requested architecture and no noarch exists
        found, not_found = self.repodata.get_rpms("foo", arch="s390x")
        self.assertEqual(len(found), 0)  # No arch-specific or noarch match
        self.assertEqual(not_found, ["foo"])  # Should be marked as not found for this arch

    def test_get_rpms_exists_different_arch_only(self):
        # Test when package exists for different arch but not requested arch or noarch
        rpm_different_arch = Rpm(
            name="other-arch-only",
            epoch=1,
            version="1.0.0",
            checksum="xyz",
            size=100,
            location="other-arch-only.rpm",
            sourcerpm="other-arch-only-1.0.0-1.el9.src.rpm",
            release="1.el9",
            arch="ppc64le",
        )
        repodata_other_arch = Repodata(name="testrepo", primary_rpms=[rpm_different_arch], modules=[])

        # Request x86_64 for package that only exists as ppc64le
        found, not_found = repodata_other_arch.get_rpms("other-arch-only", arch="x86_64")
        self.assertEqual(len(found), 0)
        self.assertEqual(not_found, ["other-arch-only"])  # Should be not_found for x86_64

    def test_get_rpms_returns_all_versions_for_arch(self):
        # Test that get_rpms returns all versions for a specific architecture
        rpms_multiple_versions = [
            Rpm(
                name="multi-version",
                epoch=1,
                version="1.0.0",
                checksum="v1",
                size=100,
                location="multi-version-1.0.0.x86_64.rpm",
                sourcerpm="multi-version-1.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="x86_64",
            ),
            Rpm(
                name="multi-version",
                epoch=1,
                version="2.0.0",
                checksum="v2",
                size=200,
                location="multi-version-2.0.0.x86_64.rpm",
                sourcerpm="multi-version-2.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="x86_64",
            ),
            Rpm(
                name="multi-version",
                epoch=1,
                version="1.0.0",
                checksum="v1a",
                size=100,
                location="multi-version-1.0.0.aarch64.rpm",
                sourcerpm="multi-version-1.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="aarch64",
            ),
            Rpm(
                name="multi-version",
                epoch=1,
                version="3.0.0",
                checksum="v3n",
                size=300,
                location="multi-version-3.0.0.noarch.rpm",
                sourcerpm="multi-version-3.0.0-1.el9.src.rpm",
                release="1.el9",
                arch="noarch",
            ),
        ]
        repodata_multi = Repodata(name="testrepo", primary_rpms=rpms_multiple_versions, modules=[])

        # Should return all x86_64 versions plus noarch versions
        found, not_found = repodata_multi.get_rpms("multi-version", arch="x86_64")
        self.assertEqual(len(found), 3)  # 2 x86_64 + 1 noarch

        # Check that we get the right versions/architectures
        versions = {rpm.version for rpm in found}
        arches = {rpm.arch for rpm in found}
        self.assertEqual(versions, {"1.0.0", "2.0.0", "3.0.0"})
        self.assertEqual(arches, {"x86_64", "noarch"})
        self.assertEqual(not_found, [])


class TestRepodataLoader(IsolatedAsyncioTestCase):
    @patch("doozerlib.repodata.RepodataLoader._fetch_remote_compressed", autospec=True)
    @patch("aiohttp.ClientSession", autospec=True)
    async def test_load(self, ClientSession: Mock, _fetch_remote_compressed: AsyncMock):
        repo_name = "test-x86_64"
        repo_url = "https://example.com/repos/test/x86_64/os"
        loader = RepodataLoader()
        session = ClientSession.return_value.__aenter__.return_value = Mock(name="session")
        resp = session.get.return_value = AsyncMock(name="get")
        resp.__aenter__.return_value.text.return_value = """<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
  <revision>1689150070</revision>
  <data type="primary">
    <checksum type="sha256">06ed3172b751202671416050ea432945e54a36ee1ab8ef2cc71307234343f1ef</checksum>
    <open-checksum type="sha256">eae43283481de984a39dd093044889efd72d9ff0501f22f9e0a269e0483b596f</open-checksum>
    <location href="repodata/06ed3172b751202671416050ea432945e54a36ee1ab8ef2cc71307234343f1ef-primary.xml.gz"/>
    <timestamp>1689149943</timestamp>
    <size>11771165</size>
    <open-size>89221590</open-size>
  </data>
  <data type="modules">
    <checksum type="sha256">454ea63462df316e80d93b60ce07e4f523bc06dd1989e878cf2df6ee2a762a34</checksum>
    <open-checksum type="sha256">3fec7e44b17d04eb9cedaeb2ed5292dbdc12e3baf631eb43c1e24ed3a25b1b54</open-checksum>
    <location href="repodata/454ea63462df316e80d93b60ce07e4f523bc06dd1989e878cf2df6ee2a762a34-modules.yaml.gz"/>
    <timestamp>1689150030</timestamp>
    <size>471210</size>
    <open-size>3707575</open-size>
  </data>
</repomd>
"""
        resp.__aenter__.return_value.raise_for_status = Mock()

        def _fake_fetch_remote_compressed(_, url: Optional[str]):
            primary_xml = """<?xml version="1.0" encoding="UTF-8"?>
<metadata packages="2" xmlns="http://linux.duke.edu/metadata/common" xmlns:rpm="http://linux.duke.edu/metadata/rpm">
    <package type="rpm">
        <name>foo</name>
        <checksum pkgid="YES" type="sha256">sfy8dfa</checksum>
        <size archive="3" installed="4" package="6" />
        <location href="Packages/l/foo-1.2.3-1.el9.x86_64.rpm" />
        <arch>x86_64</arch>
        <version epoch="1" rel="1.el9" ver="1.2.3" />
        <format>
            <rpm:sourcerpm>foo-1.2.3-1.el9.src.rpm</rpm:sourcerpm>
        </format>
    </package>
    <package type="rpm">
        <name>bar</name>
        <checksum pkgid="YES" type="sha256">barcsum</checksum>
        <size archive="10" installed="20" package="30" />
        <location href="Packages/l/bar-2.2.3-1.el9.x86_64.rpm" />
        <arch>x86_64</arch>
        <version epoch="1" rel="1.el9" ver="2.2.3" />
        <format>
            <rpm:sourcerpm>bar-2.2.3-1.el9.src.rpm</rpm:sourcerpm>
        </format>
    </package>
</metadata>
"""
            modules_yaml = """
---
document: modulemd
version: 2
data:
    name: aaa
    stream: rhel8
    version: 1
    context: deadbeef
    arch: x86_64
---
document: modulemd
version: 2
data:
    name: bbb
    stream: rhel9
    version: 2
    context: beefdead
    arch: x86_64
"""
            if not url:
                return b''
            if url.endswith("primary.xml.gz"):
                return primary_xml.encode()
            if url.endswith("modules.yaml.gz"):
                return modules_yaml.encode()
            raise ValueError("url")

        _fetch_remote_compressed.side_effect = _fake_fetch_remote_compressed
        repodata = await loader.load(repo_name, repo_url)
        _fetch_remote_compressed.assert_any_await(
            ANY,
            "https://example.com/repos/test/x86_64/os/repodata/06ed3172b751202671416050ea432945e54a36ee1ab8ef2cc71307234343f1ef-primary.xml.gz",
        )
        _fetch_remote_compressed.assert_any_await(
            ANY,
            "https://example.com/repos/test/x86_64/os/repodata/454ea63462df316e80d93b60ce07e4f523bc06dd1989e878cf2df6ee2a762a34-modules.yaml.gz",
        )
        self.assertEqual(repodata.name, repo_name)
        self.assertEqual(
            [rpm.nevra for rpm in repodata.primary_rpms], ["foo-1:1.2.3-1.el9.x86_64", "bar-1:2.2.3-1.el9.x86_64"]
        )
        self.assertEqual(
            [m.nsvca for m in repodata.modules], ['aaa:rhel8:1:deadbeef:x86_64', 'bbb:rhel9:2:beefdead:x86_64']
        )


class TestOutdatedRPMFinder(IsolatedAsyncioTestCase):
    async def test_find_non_latest_rpms_with_no_repos(self):
        installed_rpms = [
            "a-0:1.0.0-el8.x86_64",
            "b-0:1.0.0-el8.x86_64",
            "c-0:1.0.0-el8.x86_64",
            "d-0:1.0.0-el8.x86_64",
            "e-0:1.0.0-el8.x86_64",
            "f-0:1.0.0-el8.x86_64",
        ]
        finder = OutdatedRPMFinder()
        repodatas = []
        logger = MagicMock()
        actual = finder.find_non_latest_rpms(
            [Rpm.from_nevra(nevra).to_dict() for nevra in installed_rpms],
            repodatas,
            logger,
        )
        self.assertEqual(actual, [])

    async def test_find_non_latest_rpms_with_non_modular_repos(self):
        installed_rpms = [
            "a-0:1.0.0-el8.x86_64",
            "b-0:1.0.0-el8.x86_64",
            "c-0:1.0.0-el8.x86_64",
            "d-0:1.0.0-el8.x86_64",
            "e-0:1.0.0-el8.x86_64",
            "f-0:1.0.0-el8.x86_64",
        ]
        finder = OutdatedRPMFinder()
        repodatas = [
            Repodata(
                name="alfa-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("a-0:1.0.0-el8.x86_64"),
                    Rpm.from_nevra("b-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:2.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
            Repodata(
                name="bravo-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("b-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:3.0.0-el8.x86_64"),
                    Rpm.from_nevra("d-0:2.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
        ]
        logger = MagicMock()
        actual = finder.find_non_latest_rpms(
            [Rpm.from_nevra(nevra).to_dict() for nevra in installed_rpms],
            repodatas,
            logger,
        )
        expected = [
            ('b-0:1.0.0-el8.x86_64', 'b-0:2.0.0-el8.x86_64', 'alfa-x86_64'),
            ('c-0:1.0.0-el8.x86_64', 'c-0:3.0.0-el8.x86_64', 'bravo-x86_64'),
            ('d-0:1.0.0-el8.x86_64', 'd-0:2.0.0-el8.x86_64', 'bravo-x86_64'),
        ]
        self.assertEqual(actual, expected)

    async def test_find_non_latest_rpms_with_modular_repos(self):
        installed_rpms = [
            "a-0:1.0.0-el8.x86_64",
            "b-0:1.0.0-el8.x86_64",
            "c-0:1.0.0-el8.x86_64",
            "d-0:1.0.0-el8.x86_64",
            "e-0:1.0.0-el8.x86_64",
            "f-0:1.0.0-el8.x86_64",
        ]
        finder = OutdatedRPMFinder()
        repodatas = [
            Repodata(
                name="alfa-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("a-0:1.0.0-el8.x86_64"),
                    Rpm.from_nevra("b-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:2.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
            Repodata(
                name="bravo-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("b-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("c-0:3.0.0-el8.x86_64"),
                    Rpm.from_nevra("d-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("e-0:999.0.0-el8.x86_64"),
                    Rpm.from_nevra("f-0:999.0.0-el8.x86_64"),
                ],
                modules=[],
            ),
            Repodata(
                name="charlie-x86_64",
                primary_rpms=[
                    Rpm.from_nevra("e-0:1.0.0-el8.x86_64"),
                    Rpm.from_nevra("e-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("e-0:2.0.0-el8.x86_64"),
                    Rpm.from_nevra("f-0:1.1.0-el8.x86_64"),
                    Rpm.from_nevra("f-0:2.0.0-el8.x86_64"),
                ],
                modules=[
                    RpmModule(
                        name="e",
                        stream="1",
                        version=1000,
                        context="whatever",
                        arch="x86_64",
                        rpms={
                            "e-0:1.0.0-el8.x86_64",
                        },
                    ),
                    RpmModule(
                        name="e",
                        stream="1",
                        version=1001,
                        context="whatever",
                        arch="x86_64",
                        rpms={
                            "e-0:1.1.0-el8.x86_64",
                        },
                    ),
                    RpmModule(
                        name="e",
                        stream="1",
                        version=2000,
                        context="whatever2",
                        arch="x86_64",
                        rpms={
                            "e-0:3.0.0-el8.x86_64",
                        },
                    ),
                    RpmModule(
                        name="e",
                        stream="2",
                        version=2000,
                        context="whatever",
                        arch="x86_64",
                        rpms={
                            "e-0:2.0.0-el8.x86_64",
                            "f-0:2.0.0-el8.x86_64",
                        },
                    ),
                ],
            ),
        ]
        logger = MagicMock()
        actual = finder.find_non_latest_rpms(
            [Rpm.from_nevra(nevra).to_dict() for nevra in installed_rpms],
            repodatas,
            logger,
        )
        expected = [
            ('b-0:1.0.0-el8.x86_64', 'b-0:2.0.0-el8.x86_64', 'alfa-x86_64'),
            ('c-0:1.0.0-el8.x86_64', 'c-0:3.0.0-el8.x86_64', 'bravo-x86_64'),
            ('d-0:1.0.0-el8.x86_64', 'd-0:2.0.0-el8.x86_64', 'bravo-x86_64'),
            ('e-0:1.0.0-el8.x86_64', 'e-0:1.1.0-el8.x86_64', 'charlie-x86_64'),
            ('f-0:1.0.0-el8.x86_64', 'f-0:999.0.0-el8.x86_64', 'bravo-x86_64'),
        ]
        self.assertEqual(actual, expected)
