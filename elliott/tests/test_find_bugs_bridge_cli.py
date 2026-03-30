from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock

from artcommonlib.model import Model
from elliottlib.cli.find_bugs_bridge_cli import BRIDGE_LABEL, FindBugsBridgeCli


class TestFindBugsBridgeCli(IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = MagicMock()
        self.runtime.group = "openshift-4.23"
        self.runtime.debug = False
        self.runtime.get_major_minor_fields.return_value = (4, 23)
        self.runtime.group_config = {
            "bridge_release": {
                "basis_group": "openshift-5.0",
                "bug_mirroring": {"enabled": True},
            }
        }
        self.cli = FindBugsBridgeCli(runtime=self.runtime, noop=False)
        self.cli.bridge_config = self.cli._load_bridge_config(self.runtime)
        self.cli.major_minor = "4.23"
        self.cli.target_release = "4.23.z"
        self.cli.target_tracker = MagicMock()
        self.cli.target_tracker.project = "OCPBUGS"
        self.cli.target_tracker.field_target_version = "customfield_12345"
        self.cli.target_tracker._query.return_value = "mirror query"
        self.cli.existing_mirrors_by_source = {}
        self.cli._product_config = Model(
            {
                "bug_mapping": {
                    "components": {
                        "visible-image-container": {"issue_component": "Visible"},
                        "disabled-image-container": {"issue_component": "Disabled"},
                        "non-art-built-container": {"issue_component": "Unmanaged"},
                    }
                }
            }
        )

    @staticmethod
    def _link(link_name, inward_key=None, outward_key=None):
        link = MagicMock()
        link.type.name = link_name
        if inward_key:
            link.inwardIssue.key = inward_key
        if outward_key:
            link.outwardIssue.key = outward_key
        return link

    def test_get_candidate_bugs_filters_trackers_and_disabled_images(self):
        visible_image = MagicMock()
        visible_image.distgit_key = "visible-image"
        visible_image.get_component_name.return_value = "visible-image-container"
        visible_image.config = {
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["openshift4/visible-image-rhel9"]},
        }
        disabled_image = MagicMock()
        disabled_image.distgit_key = "disabled-image"
        disabled_image.get_component_name.return_value = "disabled-image-container"
        disabled_image.config = {
            "for_payload": True,
            "delivery": {"delivery_repo_names": ["openshift4/disabled-image-rhel9"]},
            "bridge_release": {"bug_mirroring": {"enabled": False}},
        }
        self.cli.images_by_component = {
            "visible-image-container": visible_image,
            "disabled-image-container": disabled_image,
        }
        self.cli.images_by_jira_component = {
            "Visible": visible_image,
            "Disabled": disabled_image,
        }

        tracker_bug = MagicMock()
        tracker_bug.id = "OCPBUGS-1"
        tracker_bug.is_tracker_bug.return_value = True

        disabled_bug = MagicMock()
        disabled_bug.id = "OCPBUGS-2"
        disabled_bug.is_tracker_bug.return_value = False
        disabled_bug.component = "Disabled"

        visible_bug = MagicMock()
        visible_bug.id = "OCPBUGS-3"
        visible_bug.is_tracker_bug.return_value = False
        visible_bug.component = "Visible"

        unmanaged_bug = MagicMock()
        unmanaged_bug.id = "OCPBUGS-4"
        unmanaged_bug.is_tracker_bug.return_value = False
        unmanaged_bug.component = "Unmanaged"

        self.cli.source_tracker = MagicMock()
        self.cli.source_tracker._query.return_value = "source query"
        self.cli.source_tracker._search.return_value = [tracker_bug, disabled_bug, visible_bug, unmanaged_bug]

        candidates = self.cli._get_candidate_bugs()
        self.assertEqual([(bug.id, image.distgit_key) for bug, image in candidates], [("OCPBUGS-3", "visible-image")])
        self.cli.source_tracker._query.assert_called_once_with(
            search_filter="default",
            custom_query=' and status != "CLOSED"',
        )
        self.cli.source_tracker._search.assert_called_once_with("source query", verbose=False)

    def test_build_target_image_maps_only_includes_payload_images(self):
        visible_image = MagicMock()
        visible_image.get_component_name.return_value = "visible-image-container"
        visible_image.config = {"for_payload": True}

        non_payload_image = MagicMock()
        non_payload_image.get_component_name.return_value = "non-art-built-container"
        non_payload_image.config = {"for_payload": False}

        self.runtime.image_metas.return_value = [visible_image, non_payload_image]

        self.cli._build_target_image_maps()

        self.assertEqual(self.cli.images_by_component, {"visible-image-container": visible_image})
        self.assertEqual(self.cli.images_by_jira_component, {"Visible": visible_image})

    async def test_sync_mirror_creates_issue_and_link(self):
        source_bug = MagicMock()
        source_bug.id = "OCPBUGS-123"
        source_bug.summary = "Visible bug"
        source_bug.status = "ON_QA"
        source_bug.weburl = "https://issues.example.com/OCPBUGS-123"
        source_bug.component = "visible-image-container"
        source_bug.whiteboard_component = None
        source_bug.bug.fields.description = "Source description"
        source_bug.bug.fields.issuetype.name = "Story"
        source_bug.bug.fields.components = [MagicMock(name="Visible")]
        source_bug.bug.fields.components[0].name = "Visible"
        source_bug.bug.fields.priority = None
        source_bug.bug.fields.security = None

        image_meta = MagicMock()
        image_meta.distgit_key = "visible-image"
        image_meta.get_component_name.return_value = "visible-image-container"

        created_issue = MagicMock()
        created_issue.id = "OCPBUGS-999"
        self.cli.target_tracker.create_issue.return_value = created_issue

        result = await self.cli._sync_mirror(source_bug, image_meta)

        self.assertTrue(result)
        create_call = self.cli.target_tracker.create_issue.call_args.args[0]
        self.assertEqual(create_call["issuetype"], {"name": "Story"})
        self.assertEqual(create_call["components"], [{"name": "Visible"}])
        self.assertIn(BRIDGE_LABEL, create_call["labels"])
        self.cli.target_tracker._client.create_issue_link.assert_any_call("Blocks", "OCPBUGS-123", "OCPBUGS-999")
        self.cli.target_tracker._client.create_issue_link.assert_any_call("Depend", "OCPBUGS-999", "OCPBUGS-123")
        self.cli.target_tracker._client.create_issue_link.assert_any_call("Cloners", "OCPBUGS-999", "OCPBUGS-123")
        self.assertEqual(self.cli.target_tracker._client.create_issue_link.call_count, 3)

    async def test_sync_mirror_skips_existing_wont_fix(self):
        source_bug = MagicMock()
        source_bug.id = "OCPBUGS-123"

        image_meta = MagicMock()

        existing_mirror = MagicMock()
        existing_mirror.status = "Closed"
        existing_mirror.resolution = "Won't Fix"
        self.cli.existing_mirrors_by_source = {"OCPBUGS-123": [existing_mirror]}

        result = await self.cli._sync_mirror(source_bug, image_meta)

        self.assertFalse(result)
        self.cli.target_tracker.create_issue.assert_not_called()

    def test_get_existing_mirrors_by_source_uses_linked_issue(self):
        mirror_for_source = MagicMock()
        mirror_for_source.id = "OCPBUGS-900"
        mirror_for_source.bug.fields.issuelinks = [
            self._link("Blocks", inward_key="OCPBUGS-123"),
            self._link("Depend", outward_key="OCPBUGS-123"),
            self._link("Cloners", outward_key="OCPBUGS-123"),
        ]
        unrelated_mirror = MagicMock()
        unrelated_mirror.id = "OCPBUGS-901"
        unrelated_mirror.bug.fields.issuelinks = [self._link("Blocks", inward_key="OCPBUGS-999")]
        self.cli.target_tracker._search.return_value = [mirror_for_source, unrelated_mirror]

        mirrors_by_source = self.cli._get_existing_mirrors_by_source(["OCPBUGS-123", "OCPBUGS-456"])

        self.assertEqual(mirrors_by_source["OCPBUGS-123"], [mirror_for_source])
        self.assertEqual(mirrors_by_source["OCPBUGS-456"], [])
        self.cli.target_tracker._query.assert_called_once_with(
            include_labels=[BRIDGE_LABEL],
            with_target_release=True,
            custom_query=" order by created DESC",
        )
        self.cli.target_tracker._search.assert_called_once()

    def test_get_existing_mirrors_by_source_complains_for_duplicate_mirrors(self):
        mirror_a = MagicMock()
        mirror_a.id = "OCPBUGS-900"
        mirror_a.bug.fields.issuelinks = [self._link("Blocks", inward_key="OCPBUGS-123")]
        mirror_b = MagicMock()
        mirror_b.id = "OCPBUGS-901"
        mirror_b.bug.fields.issuelinks = [self._link("Depend", outward_key="OCPBUGS-123")]
        self.cli.target_tracker._search.return_value = [mirror_a, mirror_b]

        mirrors_by_source = self.cli._get_existing_mirrors_by_source(["OCPBUGS-123"])

        self.assertEqual(mirrors_by_source["OCPBUGS-123"], [mirror_a, mirror_b])
        self.assertIn("OCPBUGS-123", self.cli.invalid_bugs)
        self.assertIn("Expected exactly one bridge mirror", self.cli.invalid_bugs["OCPBUGS-123"][0])

    async def test_run_reports_invalid_cases_and_fails_at_end(self):
        self.cli.runtime.initialize = MagicMock()
        self.cli.runtime.get_major_minor_fields = MagicMock(return_value=(4, 23))
        self.cli.source_runtime = MagicMock()
        self.cli.source_tracker = MagicMock()
        self.cli.target_tracker = MagicMock()
        self.cli.target_tracker.target_release.return_value = ["4.23.z"]
        self.cli.runtime.get_bug_tracker = MagicMock(return_value=self.cli.target_tracker)
        self.cli.source_runtime.get_bug_tracker = MagicMock(return_value=self.cli.source_tracker)
        self.cli._build_source_runtime = MagicMock(return_value=self.cli.source_runtime)
        self.cli._build_target_image_maps = MagicMock()
        bug = MagicMock()
        bug.id = "OCPBUGS-123"
        image_meta = MagicMock()
        self.cli._get_candidate_bugs = MagicMock(return_value=[(bug, image_meta)])

        def _existing_mirrors(*_args, **_kwargs):
            self.cli.invalid_bugs.setdefault("OCPBUGS-123", []).append("bad links")
            return {}

        self.cli._get_existing_mirrors_by_source = MagicMock(side_effect=_existing_mirrors)
        self.cli._sync_mirror = AsyncMock(return_value=False)
        self.cli._report_invalid_bugs = MagicMock()

        with self.assertRaisesRegex(ValueError, "Found invalid bridge bug cases"):
            await self.cli.run()

        self.cli._report_invalid_bugs.assert_called_once()
