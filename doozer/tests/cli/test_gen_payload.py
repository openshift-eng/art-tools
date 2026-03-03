import io
import os
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import openshift_client as oc
import yaml
from artcommonlib.assembly import AssemblyIssue, AssemblyIssueCode, AssemblyTypes
from artcommonlib.model import Model
from artcommonlib.rhcos import RhcosMissingContainerException
from doozerlib import rhcos
from doozerlib.assembly_inspector import AssemblyInspector
from doozerlib.build_info import BrewBuildRecordInspector
from doozerlib.cli import release_gen_payload as rgp_cli
from doozerlib.exceptions import DoozerFatalError
from doozerlib.image import ImageMetadata
from flexmock import flexmock


async def no_sleep(arg):
    pass


# ART regulates the frequency of multi payloads by extracting a timestamp
# from the name. This function makes it easier to generate these names.
def as_multi_release_name(prefix):
    return f'{prefix}-multi-2024-10-10-061203'


rgp_cli.asyncio.sleep = no_sleep


class TestGenPayloadCli(IsolatedAsyncioTestCase):
    def test_find_rhcos_payload_entries(self):
        rhcos_build = MagicMock()
        assembly_inspector = MagicMock()
        assembly_inspector.get_rhcos_build.return_value = rhcos_build
        rhcos_build.get_container_configs.return_value = [
            Model(dict(name="spam", build_metadata_tag="eggs", primary=True)),
            Model(dict(name="foo", build_metadata_tag="bar")),
        ]

        # test when a primary container is missing from rhcos build
        rhcos_build.get_container_pullspec.side_effect = [
            RhcosMissingContainerException("primary missing"),
            "somereg/somerepo@sha256:somesum",
        ]
        rhcos_entries, issues = rgp_cli.PayloadGenerator._find_rhcos_payload_entries(assembly_inspector, "arch")
        self.assertNotIn("spam", rhcos_entries)
        self.assertIn("foo", rhcos_entries)
        self.assertEqual(issues[0].code, AssemblyIssueCode.IMPERMISSIBLE)

        # test when a non-primary container is missing from rhcos build
        rhcos_build.get_container_pullspec.side_effect = [
            "somereg/somerepo@sha256:somesum",
            RhcosMissingContainerException("non-primary missing"),
        ]
        rhcos_entries, issues = rgp_cli.PayloadGenerator._find_rhcos_payload_entries(assembly_inspector, "arch")
        self.assertIn("spam", rhcos_entries)
        self.assertNotIn("foo", rhcos_entries)
        self.assertEqual(issues[0].code, AssemblyIssueCode.MISSING_RHCOS_CONTAINER)

        # test when no container is missing from rhcos build
        rhcos_build.get_container_pullspec.side_effect = [
            "somereg/somerepo@sha256:somesum",
            "somereg/somerepo@sha256:someothersum",
        ]
        rhcos_entries, issues = rgp_cli.PayloadGenerator._find_rhcos_payload_entries(assembly_inspector, "arch")
        self.assertEqual([], issues)
        self.assertEqual(2, len(rhcos_entries))

    # test parameter validation
    def test_parameter_validation(self):
        # test when assembly is not valid
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly="booyah"))
        with self.assertRaises(DoozerFatalError):
            gpcli.validate_parameters()

        # test when called with prod assembly and nightly imagestream
        gpcli = rgp_cli.GenPayloadCli(
            is_name="art-latest",
            runtime=Mock(
                assembly="prod",
                assembly_type=AssemblyTypes.STREAM,
                releases_config=Mock(releases=dict(prod="assembly definition")),
            ),
        )
        with self.assertRaises(ValueError):
            gpcli.validate_parameters()
        # ... and that a nightly has private mode
        gpcli.base_imagestream = ("ocp", "not-latest")
        gpcli.validate_parameters()
        self.assertIn(True, gpcli.privacy_modes, "stream assemblies should have private mode")

        # and that we're protected against incomplete multi nightlies
        gpcli = rgp_cli.GenPayloadCli(
            apply_multi_arch=True,
            runtime=MagicMock(
                assembly="stream",
                exclude=["some-random-image"],
            ),
        )
        with self.assertRaises(DoozerFatalError):
            gpcli.validate_parameters()

    @patch.object(AssemblyInspector, "__init__", lambda *_: None)
    @patch.object(AssemblyInspector, "get_group_release_images", Mock(return_value={}))
    @patch("doozerlib.cli.release_gen_payload.GenPayloadCli.generate_assembly_issues_report")
    async def test_generate_assembly_report(self, gai_report_mock):
        empty_arr = Mock(return_value=[])
        rt = Mock(
            build_retrying_koji_client=empty_arr,
            get_non_release_image_metas=empty_arr,
            get_for_release_image_metas=empty_arr,
        )
        gpcli = rgp_cli.GenPayloadCli(runtime=rt)
        assembly_inspector = AssemblyInspector(rt, rt.build_retrying_koji_client())

        self.assertFalse(gpcli.payload_permitted, "payload not permitted by default")
        gai_report_mock.return_value = (True, {})
        await gpcli.generate_assembly_report(assembly_inspector)
        self.assertTrue(gpcli.payload_permitted, "payload permitted according to assembly report")

    # this mainly checks that method names are valid and it executes
    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.check_nightlies_consistency")
    async def test_generate_assembly_issues_report(self, cnc_mock):
        gpcli = flexmock(
            rgp_cli.GenPayloadCli(runtime=MagicMock(assembly="stream", build_system="brew")),
            collect_assembly_build_ids=MagicMock(return_value={1, 2, 3}),
            detect_mismatched_siblings=None,
            detect_non_latest_rpms=None,
            detect_inconsistent_images=Mock(),
            detect_installed_rpms_issues=None,
            detect_extend_payload_entry_issues=AsyncMock(),
            summarize_issue_permits=(True, {}),
        )
        cnc_mock.return_value = ["spam"]
        await gpcli.generate_assembly_issues_report(Mock(AssemblyInspector))
        self.assertEqual(gpcli.assembly_issues, ["spam"])

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.find_mismatched_siblings")
    def test_detect_mismatched_siblings(self, fms_mock):
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly="stream"))
        ai = flexmock(Mock(AssemblyInspector), get_group_release_images={})
        bbii = flexmock(Mock(BrewBuildRecordInspector), get_nvr="spam-1.0", get_source_git_commit="spamcommit")
        fms_mock.return_value = [(bbii, bbii)]

        gpcli.detect_mismatched_siblings(ai)
        self.assertIsInstance(gpcli.assembly_issues[-1], AssemblyIssue)

    def test_id_tags_list(self):
        gpcli = rgp_cli.GenPayloadCli()
        bbii = flexmock(Mock(BrewBuildRecordInspector), get_build_id=1)
        ai = flexmock(Mock(AssemblyInspector), get_group_release_images=dict(spam=bbii))

        self.assertEqual(gpcli.generate_id_tags_list(ai)[0][0], 1)

    def test_collect_assembly_build_ids(self):
        rt = MagicMock(
            rpm_metas=lambda: [Mock(determine_rhel_targets=lambda *_: [8, 9])],
            get_default_hotfix_brew_tag=lambda el_target: f"{el_target}-hotfix",
        )
        gpcli = flexmock(
            rgp_cli.GenPayloadCli(runtime=rt, skip_gc_tagging=False),
            generate_id_tags_list=lambda *_: [],
        )

        # mock out AI to return a list of build dicts with incrementing IDs
        build_id: int = 42

        def rpm_build_dict(**_):
            nonlocal build_id
            build_id += 1
            return dict(spam=dict(id=build_id))

        ai = flexmock(Mock(AssemblyInspector), get_group_rpm_build_dicts=MagicMock(side_effect=rpm_build_dict))

        # test when we should be tagging for GC prevention
        rt.assembly_type = AssemblyTypes.STANDARD
        gpcli.should_receive("tag_missing_gc_tags").with_args([(43, '8-hotfix'), (44, '9-hotfix')]).once()
        build_ids = gpcli.collect_assembly_build_ids(ai)
        self.assertEqual(build_ids, {43, 44})

        # and when we should not
        rt.assembly_type = AssemblyTypes.STREAM
        # no call here to "tag_missing_gc_tags", so still only once this test
        build_ids = gpcli.collect_assembly_build_ids(ai)
        self.assertEqual(build_ids, {45, 46})

    async def test_detect_non_latest_rpms(self):
        gpcli = rgp_cli.GenPayloadCli()
        gpcli.runtime = MagicMock(build_system='brew')
        bbii = AsyncMock(BrewBuildRecordInspector)
        bbii.get_image_meta.return_value = Mock(ImageMetadata, is_rpm_exempt=Mock(return_value=(False, None)))
        bbii.find_non_latest_rpms.return_value = {
            "x86_64": [("foo-0:1.2.0-1.el9.x86_64", "foo-0:1.2.1-1.el9.x86_64", "repo_name")],
            "s390x": [],
        }
        ai = flexmock(Mock(AssemblyInspector), get_group_release_images=dict(spam=bbii))
        await gpcli.detect_non_latest_rpms(ai)
        self.assertEqual(gpcli.assembly_issues[0].code, AssemblyIssueCode.OUTDATED_RPMS_IN_STREAM_BUILD)

        gpcli.assembly_issues = []
        bbii.find_non_latest_rpms.return_value = {"x86_64": [], "s390x": []}
        await gpcli.detect_non_latest_rpms(ai)
        self.assertEqual(gpcli.assembly_issues, [])

    def test_detect_inconsistent_images(self):
        gpcli = rgp_cli.GenPayloadCli()
        bbii = flexmock(Mock(BrewBuildRecordInspector))
        ai = MagicMock(AssemblyInspector)
        ai.get_group_release_images = MagicMock(return_value={"spam": bbii})
        ai.check_group_image_consistency = MagicMock(return_value=["stuff"])

        gpcli.detect_inconsistent_images(ai)
        self.assertEqual(gpcli.assembly_issues, ["stuff"])

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.find_payload_entries")
    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.embargo_issues_for_payload")
    async def test_generate_payload_entries(self, pg_eissuesfp_mock, pg_findpe_mock):
        gpcli = rgp_cli.GenPayloadCli(
            exclude_arch=["s390x"],
            runtime=MagicMock(arches=["ppc64le", "s390x"]),
        )
        pg_eissuesfp_mock.return_value = ["embargo_issues"]

        test_payload_entry = rgp_cli.PayloadEntry(image_meta=Mock(distgit_key="spam"), issues=[], dest_pullspec="dummy")
        pg_findpe_mock.return_value = (dict(tag1=test_payload_entry), ["issues"])
        e4a = await gpcli.generate_payload_entries(Mock(AssemblyInspector))
        self.assertEqual(
            e4a, (dict(ppc64le=dict(tag1=test_payload_entry)), dict(ppc64le=dict(tag1=test_payload_entry)))
        )
        self.assertEqual(gpcli.assembly_issues, ["issues", "embargo_issues"])

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.find_payload_entries")
    async def test_generate_payload_entries_rhcos(self, pg_findpe_mock):
        """
        Build inspector does not exist, its RHCOS, add to public entry
        """
        gpcli = rgp_cli.GenPayloadCli(
            exclude_arch=[],
            runtime=MagicMock(arches=["ppc64le"], assembly_type=AssemblyTypes.STREAM),
        )

        test_payload_entry = rgp_cli.PayloadEntry(
            image_meta=Mock(distgit_key="image_1"), issues=[], dest_pullspec="pullspec_1"
        )
        pg_findpe_mock.return_value = (dict(tag1=test_payload_entry), [])
        payload_entries = await gpcli.generate_payload_entries(Mock(AssemblyInspector))
        self.assertEqual(
            payload_entries, (dict(ppc64le=dict(tag1=test_payload_entry)), dict(ppc64le=dict(tag1=test_payload_entry)))
        )

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.find_payload_entries")
    async def test_generate_payload_no_embargo(self, pg_findpe_mock):
        gpcli = rgp_cli.GenPayloadCli(
            exclude_arch=[],
            runtime=MagicMock(arches=["ppc64le"], assembly_type=AssemblyTypes.STREAM),
        )
        bbii = MagicMock(BrewBuildRecordInspector)
        bbii.is_under_embargo.return_value = False

        test_payload_entry = rgp_cli.PayloadEntry(
            image_meta=Mock(distgit_key="image_1"), issues=[], dest_pullspec="pullspec_1", build_record_inspector=bbii
        )
        pg_findpe_mock.return_value = (dict(tag1=test_payload_entry), [])
        payload_entries = await gpcli.generate_payload_entries(Mock(AssemblyInspector))
        self.assertEqual(
            payload_entries, (dict(ppc64le=dict(tag1=test_payload_entry)), dict(ppc64le=dict(tag1=test_payload_entry)))
        )

    async def test_detect_extend_payload_entry_issues(self):
        runtime = MagicMock(group_config=Model())
        gpcli = flexmock(rgp_cli.GenPayloadCli(runtime))
        spamEntry = rgp_cli.PayloadEntry(
            image_meta=Mock(distgit_key="spam"),
            issues=[],
            dest_pullspec="dummy",
        )
        rhcosEntry = rgp_cli.PayloadEntry(rhcos_build="rbi", dest_pullspec="dummy", issues=[])
        gpcli.payload_entries_for_arch = dict(ppc64le={"spam": spamEntry, "machine-os-content": rhcosEntry})
        gpcli.assembly_issues = [Mock(component="spam")]  # should associate with spamEntry

        gpcli.should_receive("detect_rhcos_issues").with_args(rhcosEntry, None).once().and_return(
            AsyncMock(return_value=[])()
        )
        gpcli.should_receive("detect_rhcos_inconsistent_rpms").once().with_args({False: ["rbi"], True: []})
        gpcli.should_receive("detect_rhcos_kernel_inconsistencies").once().with_args({False: ["rbi"], True: []})

        await gpcli.detect_extend_payload_entry_issues(None)
        self.assertEqual(gpcli.assembly_issues, spamEntry.issues)

        bogusEntry = rgp_cli.PayloadEntry(dest_pullspec="dummy", issues=[])
        gpcli.payload_entries_for_arch = dict(ppc64le=dict(bogus=bogusEntry))
        with self.assertRaises(DoozerFatalError):
            await gpcli.detect_extend_payload_entry_issues(None)

    async def test_detect_rhcos_issues(self):
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly_type=AssemblyTypes.STREAM))
        rhcos_issue = Mock(AssemblyIssue, component='rhcos')
        ai = flexmock(Mock(AssemblyInspector), check_rhcos_issues=AsyncMock(return_value=[rhcos_issue]))
        rhcos_build = flexmock(
            Mock(rhcos.RHCOSBuildInspector, brew_arch="x86_64"),
            find_non_latest_rpms=AsyncMock(return_value=[("installed", "newest", "repo_name")]),
        )
        rhcos_entry = rgp_cli.PayloadEntry(rhcos_build=rhcos_build, dest_pullspec="dummy", issues=[])

        await gpcli.detect_rhcos_issues(rhcos_entry, ai)
        self.assertEqual(gpcli.assembly_issues[0], rhcos_entry.issues[0])

        # make the if statement false
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly="standard"))
        await gpcli.detect_rhcos_issues(rhcos_entry, ai)
        self.assertEqual(gpcli.assembly_issues, [rhcos_issue])

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.find_rhcos_build_rpm_inconsistencies")
    def test_detect_rhcos_inconsistent_rpms(self, pg_find_mock):
        gpcli = rgp_cli.GenPayloadCli()
        gpcli.privacy_modes = [True, False]
        pg_find_mock.side_effect = [[], ["dummy1", "dummy2"]]
        gpcli.detect_rhcos_inconsistent_rpms({False: ["rbi"], True: []})
        self.assertEqual(gpcli.assembly_issues[0].code, AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS)

    def test_summarize_issue_permits(self):
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly_type=AssemblyTypes.STREAM))
        gpcli.assembly_issues = [
            Mock(AssemblyIssue, code=AssemblyIssueCode.INCONSISTENT_RHCOS_RPMS, component="spam", msg=""),
            Mock(AssemblyIssue, code=AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED, component="eggs", msg=""),
        ]
        ai = flexmock(Mock(AssemblyInspector), does_permit=lambda x: x.component == "spam")
        permitted, report = gpcli.summarize_issue_permits(ai)
        self.assertFalse(permitted)
        self.assertTrue(report["spam"][0]["permitted"])
        self.assertFalse(report["eggs"][0]["permitted"])

    def test_summarize_issue_permits_embargo(self):
        """
        If embargoed content is present, it should not be permitted
        """
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly_type=AssemblyTypes.STREAM))
        gpcli.assembly_issues = [
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component1", msg=""),
            Mock(AssemblyIssue, code=AssemblyIssueCode.CONFLICTING_GROUP_RPM_INSTALLED, component="component2", msg=""),
        ]
        ai = flexmock(Mock(AssemblyInspector), does_permit=lambda x: x.component == "component2")
        permitted, report = gpcli.summarize_issue_permits(ai)
        self.assertFalse(permitted)
        self.assertTrue(report["component2"][0]["permitted"])

    def test_summarize_issue_permits_embargo_more_than_one(self):
        """
        If more than one embargoed content is present, the payload should not be permitted, even if one of them is permitted
        """
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly_type=AssemblyTypes.STREAM))
        gpcli.assembly_issues = [
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component1", msg=""),
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component2", msg=""),
        ]
        ai = flexmock(Mock(AssemblyInspector), does_permit=lambda x: x.component == "component2")
        permitted, report = gpcli.summarize_issue_permits(ai)
        self.assertFalse(permitted)
        self.assertTrue(report["component2"][0]["permitted"])

    def test_summarize_issue_permits_embargo_unacknowledged(self):
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly_type=AssemblyTypes.STANDARD))
        gpcli.assembly_issues = [
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component1", msg=""),
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component2", msg=""),
        ]
        ai = flexmock(Mock(AssemblyInspector), does_permit=lambda x: x.component == "component2")

        with self.assertRaises(IOError):
            gpcli.summarize_issue_permits(ai)

    def test_summarize_issue_permits_embargo_acknowledged(self):
        gpcli = rgp_cli.GenPayloadCli(runtime=MagicMock(assembly_type=AssemblyTypes.STANDARD), embargo_permit_ack=True)
        gpcli.assembly_issues = [
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component1", msg=""),
            Mock(AssemblyIssue, code=AssemblyIssueCode.EMBARGOED_CONTENT, component="component2", msg=""),
        ]
        ai = flexmock(Mock(AssemblyInspector), does_permit=lambda x: x.component in ["component1", "component2"])
        permitted, report = gpcli.summarize_issue_permits(ai)
        self.assertTrue(permitted)

    def test_assess_assembly_viability(self):
        gpcli = rgp_cli.GenPayloadCli(
            apply=True, apply_multi_arch=True, runtime=MagicMock(assembly_type=AssemblyTypes.STREAM)
        )

        gpcli.payload_permitted, gpcli.emergency_ignore_issues = True, False
        gpcli.assess_assembly_viability()
        self.assertTrue(gpcli.apply)

        gpcli.payload_permitted, gpcli.emergency_ignore_issues = False, True
        gpcli.assess_assembly_viability()
        self.assertTrue(gpcli.payload_permitted)
        self.assertTrue(gpcli.apply)

        gpcli.payload_permitted, gpcli.emergency_ignore_issues = False, False
        gpcli.assess_assembly_viability()
        self.assertFalse(gpcli.payload_permitted)
        self.assertFalse(gpcli.apply)

    async def test_sync_payloads(self):
        runtime = MagicMock(group_config=Model(dict(multi_arch=dict(enabled=True))))
        gpcli = rgp_cli.GenPayloadCli(runtime, apply_multi_arch=True)
        payload_entry_test = rgp_cli.PayloadEntry(
            issues=[],
            dest_pullspec="eggs_pullspec",
            build_record_inspector=Mock(get_build_pullspec=lambda: "eggs_manifest_src"),
        )
        gpcli.payload_entries_for_arch = {"x86_64": {"x86_entries": payload_entry_test}}

        gpcli.mirror_payload_content = AsyncMock()
        gpcli.generate_specific_payload_imagestreams = AsyncMock()
        gpcli.sync_heterogeneous_payloads = AsyncMock()

        await gpcli.sync_payloads()
        self.assertEqual(gpcli.mirror_payload_content.await_count, 1)
        self.assertEqual(gpcli.generate_specific_payload_imagestreams.await_count, 1)
        gpcli.sync_heterogeneous_payloads.assert_awaited_once()

    @patch("aiofiles.open")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_mirror_payload_content(self, exec_mock, open_mock):
        gpcli = rgp_cli.GenPayloadCli(output_dir="/tmp", apply=True, runtime=MagicMock(build_system='brew'))
        payload_entries = dict(
            rhcos=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
            ),
            spam=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="spam_pullspec",
                image_inspector=Mock(get_pullspec=lambda: "spam_src"),
            ),
            eggs=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="eggs_pullspec",
                image_inspector=Mock(get_pullspec=lambda: "eggs_src"),
                dest_manifest_list_pullspec="eggs_manifest_pullspec",
                build_record_inspector=Mock(get_build_pullspec=lambda: "eggs_manifest_src"),
            ),
        )

        buffer = io.StringIO()
        open_mock.return_value.__aenter__.return_value.write = AsyncMock(side_effect=lambda s: buffer.write(s))
        exec_mock.return_value = None  # do not actually run the command

        await gpcli.mirror_payload_content("s390x", payload_entries)

        lines = sorted(buffer.getvalue().splitlines())
        self.assertEqual(
            lines,
            [
                "eggs_manifest_src=eggs_manifest_pullspec",
                "eggs_src=eggs_pullspec",
                "spam_src=spam_pullspec",
            ],
        )  # rhcos notably absent from mirroring

    @patch("aiofiles.open")
    @patch("artcommonlib.exectools.cmd_assert_async")
    async def test_mirror_payload_content_mismatched_siblings(self, exec_mock, open_mock):
        """Test that mismatched siblings are not mirrored"""
        gpcli = rgp_cli.GenPayloadCli(output_dir="/tmp", apply=True, runtime=MagicMock(build_system='brew'))

        # Mark mismatched-image as having mismatched siblings
        gpcli.mismatched_siblings = ['mismatched-image']

        payload_entries = dict(
            good_image=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="good_pullspec",
                image_meta=Mock(distgit_key="good-image"),
                image_inspector=Mock(get_pullspec=lambda: "good_src"),
            ),
            mismatched_image=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="mismatched_pullspec",
                image_meta=Mock(distgit_key="mismatched-image"),
                image_inspector=Mock(get_pullspec=lambda: "mismatched_src"),
            ),
        )

        buffer = io.StringIO()
        open_mock.return_value.__aenter__.return_value.write = AsyncMock(side_effect=lambda s: buffer.write(s))
        exec_mock.return_value = None  # do not actually run the command

        await gpcli.mirror_payload_content("s390x", payload_entries)

        lines = sorted(buffer.getvalue().splitlines())
        self.assertEqual(
            lines,
            ["good_src=good_pullspec"],  # mismatched_image notably absent
        )

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.build_payload_istag")
    async def test_generate_specific_payload_imagestreams(self, build_mock):
        build_mock.side_effect = lambda name, _: name  # just to make the test simpler
        runtime = MagicMock(images=[], exclude=[], assembly_type=AssemblyTypes.STREAM)
        gpcli = flexmock(
            rgp_cli.GenPayloadCli(
                runtime=runtime,
                apply=True,
                is_name="release",
                is_namespace="ocp",
            )
        )
        payload_entries = dict(
            rhcos=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
            ),
            spam=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
                build_record_inspector=Mock(BrewBuildRecordInspector, is_under_embargo=lambda: True),
            ),
            eggs=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
                build_record_inspector=Mock(BrewBuildRecordInspector, is_under_embargo=lambda: False),
            ),
        )

        # these need to be true across two calls to generate_specific_payload_imagestreams()
        gpcli.write_imagestream_artifact_file = AsyncMock()
        gpcli.apply_arch_imagestream = AsyncMock()

        # test when we're building a public payload with an embargoed image
        multi_specs = {True: dict(), False: dict()}
        await gpcli.generate_specific_payload_imagestreams("s390x", False, payload_entries, multi_specs)
        self.maxDiff = None
        self.assertEqual(
            multi_specs,
            {
                True: dict(),
                False: dict(
                    rhcos=dict(s390x=payload_entries["rhcos"]),
                    # spam is embargoed and excluded from public payload (not even an empty dict)
                    eggs=dict(s390x=payload_entries["eggs"]),
                ),
            },
        )
        gpcli.write_imagestream_artifact_file.assert_awaited_once_with(
            "ocp-s390x", "release-s390x", ["rhcos", "eggs"], True
        )
        gpcli.apply_arch_imagestream.assert_awaited_once()

        # and when we're building a private payload too
        gpcli.write_imagestream_artifact_file = AsyncMock()
        runtime.images.append("anything")  # just to exercise the other branch of logic
        gpcli.apply = False  # just to exercise the other branch of logic
        await gpcli.generate_specific_payload_imagestreams("s390x", True, payload_entries, multi_specs)
        self.assertEqual(
            multi_specs,
            {
                True: dict(
                    rhcos=dict(s390x=payload_entries["rhcos"]),
                    spam=dict(s390x=payload_entries["spam"]),
                    eggs=dict(s390x=payload_entries["eggs"]),
                ),
                False: dict(
                    rhcos=dict(s390x=payload_entries["rhcos"]),
                    # spam was skipped in public payload (not even an empty dict)
                    eggs=dict(s390x=payload_entries["eggs"]),
                ),
            },
        )
        gpcli.write_imagestream_artifact_file.assert_awaited_once_with(
            "ocp-s390x-priv", "release-s390x-priv", ["rhcos", "spam", "eggs"], True
        )
        gpcli.apply_arch_imagestream.assert_awaited_once()

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.build_payload_istag")
    async def test_generate_specific_payload_imagestreams_mismatched_siblings(self, build_mock):
        """Test that mismatched siblings are excluded from imagestreams and multi_specs"""
        build_mock.side_effect = lambda name, _: name  # just to make the test simpler
        runtime = MagicMock(images=[], exclude=[], assembly_type=AssemblyTypes.STREAM)
        gpcli = flexmock(
            rgp_cli.GenPayloadCli(
                runtime=runtime,
                apply=True,
                is_name="release",
                is_namespace="ocp",
            )
        )

        # Mark 'mismatched-image' as having mismatched siblings
        gpcli.mismatched_siblings = ['mismatched-image']

        payload_entries = dict(
            rhcos=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
            ),
            good_image=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
                image_meta=Mock(distgit_key="good-image"),
                build_record_inspector=Mock(BrewBuildRecordInspector, is_under_embargo=lambda: False),
            ),
            mismatched_image=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="dummy",
                image_meta=Mock(distgit_key="mismatched-image"),
                build_record_inspector=Mock(BrewBuildRecordInspector, is_under_embargo=lambda: False),
            ),
        )

        gpcli.write_imagestream_artifact_file = AsyncMock()
        gpcli.apply_arch_imagestream = AsyncMock()

        multi_specs = {True: dict(), False: dict()}
        await gpcli.generate_specific_payload_imagestreams("x86_64", False, payload_entries, multi_specs)

        # Verify that mismatched-image was excluded from the imagestream tags
        # It should not appear in multi_specs at all (not even as an empty dict)
        self.assertEqual(
            multi_specs,
            {
                True: dict(),
                False: dict(
                    rhcos=dict(x86_64=payload_entries["rhcos"]),
                    good_image=dict(x86_64=payload_entries["good_image"]),
                    # mismatched_image is completely absent from multi_specs
                ),
            },
        )

        # Verify that only rhcos and good_image were included in the istags (not mismatched_image)
        # Note: x86_64 (amd64) doesn't get an arch suffix, so it's just "ocp" and "release"
        gpcli.write_imagestream_artifact_file.assert_awaited_once_with("ocp", "release", ["rhcos", "good_image"], True)

    @patch("aiofiles.open")
    async def test_write_imagestream_artifact_file(self, open_mock):
        gpcli = rgp_cli.GenPayloadCli(
            output_dir="/tmp",
            runtime=Mock(
                brew_event="999999",
                assembly_type=AssemblyTypes.STREAM,
            ),
        )

        buffer = io.StringIO()
        open_mock.return_value.__aenter__.return_value.write = AsyncMock(side_effect=lambda s: buffer.write(s))

        await gpcli.write_imagestream_artifact_file("ocp-s390x", "release-s390x", ["rhcos", "eggs"], True)
        self.assertEqual(
            buffer.getvalue().strip(),
            f"""
apiVersion: image.openshift.io/v1
kind: ImageStream
metadata:
  annotations:
    release.openshift.io/build-url: {os.getenv('BUILD_URL', "''")}
    release.openshift.io/runtime-brew-event: '999999'
  name: release-s390x
  namespace: ocp-s390x
spec:
  tags:
  - rhcos
  - eggs
        """.strip(),
        )

    @patch("doozerlib.cli.release_gen_payload.oc")
    async def test_apply_arch_imagestream(self, oc_mock):
        # pretty much just ensuring names aren't messed up, not checking logic
        gpcli = flexmock(rgp_cli.GenPayloadCli())
        gpcli.ensure_imagestream_apiobj = Mock()
        gpcli.apply_imagestream_update = AsyncMock()
        gpcli.apply_imagestream_update.return_value = ["prune-me", "add-me"]
        await gpcli.apply_arch_imagestream("ocp-s390x", "release-s390x", ["prune-me", "add-me"], True)
        gpcli.ensure_imagestream_apiobj.assert_called()
        gpcli.apply_imagestream_update.assert_awaited()

    @patch("doozerlib.cli.release_gen_payload.oc")
    def test_ensure_imagestream_apiobj(self, oc_mock):
        # pretty much just ensuring names aren't messed up, not checking logic
        gpcli = rgp_cli.GenPayloadCli()
        gpcli.ensure_imagestream_apiobj("release-s390x")
        oc_mock.selector.return_value = Mock(object=lambda **_: False)  # other branch
        gpcli.ensure_imagestream_apiobj("release-s390x")

    @patch("doozerlib.cli.release_gen_payload.PayloadGenerator.build_inconsistency_annotations")
    @patch("doozerlib.cli.release_gen_payload.modify_and_replace_api_object")
    async def test_apply_imagestream_update(self, mar_mock, binc_mock):
        gpcli = rgp_cli.GenPayloadCli(
            output_dir="/tmp",
            runtime=Mock(
                brew_event="999999",
                assembly_type=AssemblyTypes.STREAM,
            ),
        )

        # make method do basically what it would, without writing all the files
        mar_mock.side_effect = lambda apiobj, func, *_: func(apiobj)
        # return a stub inconsistency annotation
        binc_mock.side_effect = lambda issues: {
            "release.openshift.io/inconsistency": ",".join(str(it) for it in issues),
        }

        istream_apiobj = Mock(
            oc.APIObject,
            model=oc.Model(
                dict(
                    metadata=dict(),
                    spec=dict(
                        tags=[
                            dict(name="spam"),
                        ]
                    ),
                )
            ),
        )
        new_istags = [dict(name="eggs")]

        # test when it's a partial update, should just be additive
        (pruning, adding) = await gpcli.apply_imagestream_update(istream_apiobj, new_istags, True)
        self.assertEqual(pruning, set(), "nothing should be pruned in partial update")
        self.assertEqual(adding, {"eggs"}, "new thing added with update")
        self.assertEqual(
            istream_apiobj.model.spec.tags, [dict(name="eggs"), dict(name="spam")], "tags should be combined"
        )
        self.assertIn("release.openshift.io/inconsistency", istream_apiobj.model.metadata.annotations)

        # test when it's a full update, only the new should remain
        gpcli.assembly_issues = ["issue1", "issue2"]
        (pruning, adding) = await gpcli.apply_imagestream_update(istream_apiobj, new_istags, False)
        self.assertEqual(istream_apiobj.model.spec.tags, [dict(name="eggs")], "should be only new tags")
        self.assertEqual(pruning, {"spam"}, "should be pruned with complete update")
        self.assertEqual(adding, set(), "eggs added in previous update")
        self.assertEqual(
            "issue1,issue2", istream_apiobj.model.metadata.annotations["release.openshift.io/inconsistency"]
        )

    def test_get_multi_release_names(self):
        runtime = MagicMock(
            assembly="stream",
            assembly_type=AssemblyTypes.STREAM,
            get_minor_version=lambda: "4.10",
        )
        gpcli = rgp_cli.GenPayloadCli(runtime)
        expected = r"^4.10.0-0.nightly-multi[-\d]+$"
        self.assertRegex(gpcli.get_multi_release_names(False)[0], expected)

        runtime.assembly = "spam"
        runtime.assembly_type = AssemblyTypes.CUSTOM
        expected = r"^4.10.0-0.art-assembly-spam-multi[-\d]+$"
        self.assertRegex(gpcli.get_multi_release_names(False)[0], expected)

    async def test_build_multi_istag(self):
        gpcli = flexmock(rgp_cli.GenPayloadCli())
        args = dict(
            dest_manifest_list_pullspec="quay.io/org/repo:spam",
            image_inspector=Mock(get_manifest_list_digest=lambda: "sha256:abcdef"),
            issues=[],
            dest_pullspec="quay.io/org/repo:eggs",
        )
        arch_to_payload_entry = dict(  # both same
            s390x=rgp_cli.PayloadEntry(**args),
            ppc64le=rgp_cli.PayloadEntry(**args),
        )

        # test that we get the first flow where it just reuses the pullspec w/ digest
        gpcli.create_multi_manifest_list = AsyncMock(return_value="")
        self.assertEqual(
            await gpcli.build_multi_istag("spam", arch_to_payload_entry, "ocp"),
            {
                "annotations": {},
                "from": dict(kind="DockerImage", name="quay.io/org/repo@sha256:abcdef"),
                "name": "spam",
                "importPolicy": {"importMode": "PreserveOriginal"},
            },
        )
        gpcli.create_multi_manifest_list.assert_not_awaited()

        # test that we get the second flow with mismatched entries where it creates a manifest list
        args["dest_manifest_list_pullspec"] = "quay.io/org/repo:spam-alt"
        arch_to_payload_entry["x86_64"] = rgp_cli.PayloadEntry(**args)
        gpcli.create_multi_manifest_list = AsyncMock(return_value="new-manifest-list-pullspec")
        self.assertEqual(
            await gpcli.build_multi_istag("spam", arch_to_payload_entry, "ocp"),
            {
                "annotations": {},
                "from": dict(kind="DockerImage", name="new-manifest-list-pullspec"),
                "name": "spam",
                "importPolicy": {"importMode": "PreserveOriginal"},
            },
        )
        gpcli.create_multi_manifest_list.assert_awaited_once_with("spam", arch_to_payload_entry, "ocp")

    @patch("doozerlib.cli.release_gen_payload.find_manifest_list_sha")
    @patch("artcommonlib.exectools.cmd_assert_async")
    @patch("aiofiles.open")
    async def test_create_multi_manifest_list(self, open_mock, exec_mock, fmlsha_mock):
        os.environ['XDG_RUNTIME_DIR'] = 'fake'
        runtime = MagicMock(uuid="uuid")
        gpcli = rgp_cli.GenPayloadCli(runtime, output_dir="/tmp", organization="org", repository="repo")

        buffer = io.StringIO()
        open_mock.return_value.__aenter__.return_value.write = AsyncMock(side_effect=lambda s: buffer.write(s))
        exec_mock.return_value = None  # do not actually run the command
        fmlsha_mock.return_value = "sha256:abcdef"

        arch_to_payload_entry = dict(
            s390x=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="quay.io/org/repo:eggs-s390x",
            ),
            ppc64le=rgp_cli.PayloadEntry(
                issues=[],
                dest_pullspec="quay.io/org/repo:eggs-ppc64le",
            ),
        )
        await gpcli.create_multi_manifest_list("spam", arch_to_payload_entry, "ocp-multi")
        self.assertEqual(
            exec_mock.call_args[0][0], "manifest-tool  push from-spec /tmp/ocp-multi.spam.manifest-list.yaml"
        )
        ml = yaml.safe_load(buffer.getvalue())
        self.assertRegex(ml["image"], r"^quay.io/org/repo:sha256-")
        self.assertEqual(len(ml["manifests"]), 2)

    @patch("artcommonlib.exectools.cmd_assert_async")
    @patch("pathlib.Path.open")
    async def test_create_multi_release_images(self, open_mock, exec_mock):
        gpcli = flexmock(rgp_cli.GenPayloadCli(output_dir="/tmp"))

        exec_mock.return_value = None  # do not actually execute command
        cmgr = MagicMock(__enter__=lambda _: io.StringIO())  # mock Path.open()
        open_mock.return_value = cmgr

        gpcli.create_multi_release_manifest_list = AsyncMock(return_value="some_pullspec")

        self.assertEqual(
            await gpcli.create_multi_release_image(
                imagestream_name="isname",
                multi_release_is=dict(example="spam"),
                multi_release_dest="quay.io/org/repo:spam",
                multi_release_name="relname",
                multi_specs={False: {"cluster-version-operator": dict(arch=Mock(dest_pullspec="dest"))}},
                private_mode=False,
            ),
            "some_pullspec",
        )
        gpcli.create_multi_release_manifest_list.assert_awaited_once_with(
            {"arch": "quay.io/org/repo:spam-arch"}, 'isname', 'quay.io/org/repo:spam'
        )

    @patch("doozerlib.cli.release_gen_payload.find_manifest_list_sha")
    @patch("doozerlib.cli.release_gen_payload.GenPayloadCli.mirror_payload_content")
    @patch("artcommonlib.exectools.cmd_assert_async")
    @patch("aiofiles.open")
    async def test_create_multi_release_manifest_list(
        self, open_mock, exec_mock, mirror_payload_content_mock, fmlsha_mock
    ):
        os.environ['XDG_RUNTIME_DIR'] = 'fake'
        gpcli = rgp_cli.GenPayloadCli(output_dir="/tmp")

        exec_mock.return_value = None  # do not actually execute command
        buffer = io.StringIO()
        open_mock.return_value.__aenter__.return_value.write = AsyncMock(side_effect=lambda s: buffer.write(s))
        fmlsha_mock.return_value = "sha256:abcdef"

        pullspec = await gpcli.create_multi_release_manifest_list(
            arch_release_dests=dict(x86_64="pullspec:x86"),
            imagestream_name="isname",
            multi_release_dest="quay.io/org/repo:spam",
        )
        self.assertEqual(pullspec, "quay.io/org/repo@sha256:abcdef")
        self.assertEqual(exec_mock.call_args[0][0], "manifest-tool  push from-spec /tmp/isname.manifest-list.yaml")
        self.assertEqual(
            buffer.getvalue().strip(),
            """
image: quay.io/org/repo:spam
manifests:
- image: pullspec:x86
  platform:
    architecture: amd64
    os: linux
        """.strip(),
        )

    @patch("doozerlib.cli.release_gen_payload.what_is_in_master", return_value="4.19")
    @patch("doozerlib.cli.release_gen_payload.modify_and_replace_api_object")
    async def test_apply_multi_imagestream_update(self, mar_mock, _):
        gpcli = flexmock(
            rgp_cli.GenPayloadCli(output_dir="/tmp", runtime=MagicMock(assembly_type=AssemblyTypes.STREAM))
        )

        # make MAR method do basically what it would, without writing all the files
        mar_mock.side_effect = lambda apiobj, func, *_: func(apiobj)

        # test object to modify - really testing inline function
        istream_apiobj = Mock(
            oc.APIObject,
            model=oc.Model(
                dict(
                    metadata=dict(),
                    spec=dict(
                        tags=[
                            dict(
                                name=as_multi_release_name("spam0-nightly"),
                                annotations={'release.openshift.io/phase': 'Accepted'},
                            ),
                            dict(
                                name=as_multi_release_name("spam1-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam2-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam3-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam4-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam5-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam6-nightly"),
                                annotations={'release.openshift.io/phase': 'Accepted'},
                            ),
                            dict(
                                name="tickle",  # non-nightly tag
                                annotations=None,
                            ),
                        ]
                    ),
                )
            ),
        )
        gpcli.should_receive("ensure_imagestream_apiobj").once().and_return(istream_apiobj)

        def contains(name: str):
            for tag in istream_apiobj.model.spec.tags:
                if tag.name == name:
                    return True
            return False

        await gpcli.apply_multi_imagestream_update("final_pullspec", "is_name", "multi_release_name")
        self.assertFalse(contains(name=as_multi_release_name("spam1-nightly")), "old rejected should have been pruned")
        self.assertTrue(contains(name=as_multi_release_name("spam2-nightly")), "recent rejected not pruned")
        self.assertTrue(contains(name=as_multi_release_name("spam6-nightly")), "new accepted not pruned")
        self.assertTrue(contains(name=as_multi_release_name("spam0-nightly")), "older 2nd accepted not pruned")
        self.assertTrue(contains(name="tickle"), "non-nightly tag should be preserved")

        new_tag_annotations = istream_apiobj.model.spec.tags[-1]['annotations']
        self.assertEqual('false', new_tag_annotations['release.openshift.io/rewrite'])
        self.assertEqual(os.getenv('BUILD_URL', ''), new_tag_annotations['release.openshift.io/build-url'])
        self.assertIn('release.openshift.io/runtime-brew-event', new_tag_annotations)

    @patch("doozerlib.cli.release_gen_payload.what_is_in_master", return_value="4.19")
    @patch("doozerlib.cli.release_gen_payload.modify_and_replace_api_object")
    async def test_apply_multi_imagestream_update_retain_accepted(self, mar_mock, _):
        gpcli = flexmock(
            rgp_cli.GenPayloadCli(output_dir="/tmp", runtime=MagicMock(assembly_type=AssemblyTypes.STREAM))
        )

        # make MAR method do basically what it would, without writing all the files
        mar_mock.side_effect = lambda apiobj, func, *_: func(apiobj)

        # test object to modify - really testing inline function
        istream_apiobj = Mock(
            oc.APIObject,
            model=oc.Model(
                dict(
                    metadata=dict(),
                    spec=dict(
                        tags=[
                            dict(
                                name=as_multi_release_name("spam-1-nightly"),
                                annotations={'release.openshift.io/phase': 'Accepted'},
                            ),
                            dict(
                                name=as_multi_release_name("spam0-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam1-nightly"),
                                annotations={'release.openshift.io/phase': 'Accepted'},
                            ),
                            dict(
                                name=as_multi_release_name("spam2-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam3-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam4-nightly"),
                                annotations={'release.openshift.io/phase': 'Accepted'},
                            ),
                            dict(
                                name=as_multi_release_name("spam5-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam6-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam7-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam8-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                            dict(
                                name=as_multi_release_name("spam9-nightly"),
                                annotations={'release.openshift.io/phase': 'Rejected'},
                            ),
                        ]
                    ),
                )
            ),
        )
        gpcli.should_receive("ensure_imagestream_apiobj").once().and_return(istream_apiobj)

        await gpcli.apply_multi_imagestream_update("final_pullspec", "is_name", "multi_release_name")

        def contains(name: str):
            for tag in istream_apiobj.model.spec.tags:
                if tag.name == name:
                    return True
            return False

        self.assertFalse(
            contains(name=as_multi_release_name("spam-1-nightly")), "oldest accepted release should have been pruned"
        )
        self.assertTrue(
            contains(name=as_multi_release_name("spam1-nightly")), "accepted release should not have been pruned"
        )
        self.assertTrue(
            contains(name=as_multi_release_name("spam4-nightly")), "2nd accepted release should not have been pruned"
        )
        self.assertFalse(
            contains(name=as_multi_release_name("spam0-nightly")), "oldest rejected release should have been pruned"
        )

        new_tag_annotations = istream_apiobj.model.spec.tags[-1]['annotations']
        self.assertEqual('false', new_tag_annotations['release.openshift.io/rewrite'])
        self.assertEqual(os.getenv('BUILD_URL', ''), new_tag_annotations['release.openshift.io/build-url'])
        self.assertIn('release.openshift.io/runtime-brew-event', new_tag_annotations)
