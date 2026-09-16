import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

from artcommonlib.konflux.konflux_build_record import Engine, KonfluxBuildOutcome
from artcommonlib.variants import BuildVariant
from doozerlib import distgit
from doozerlib.cli import olm_bundle as olm_bundle_cli
from doozerlib.cli import rpms as rpms_cli


class TestBrewBuildRecords(unittest.IsolatedAsyncioTestCase):
    async def test_rpm_record_stores_runtime_variant(self):
        runtime = MagicMock()
        runtime.group = "microshift-4.17"
        runtime.assembly = "stream"
        runtime.build_system = "brew"
        runtime.variant = BuildVariant.MICROSHIFT
        runtime.konflux_db = MagicMock()
        runtime.shared_koji_client_session.return_value.__enter__.return_value = MagicMock()

        rpm = MagicMock()
        rpm.rpm_name = "microshift"
        rpm.release = "1.p0"
        rpm.public_upstream_url = "https://example.com/microshift.git"
        rpm.pre_init_sha = "deadbeef"
        rpm.get_arches.return_value = ["x86_64"]

        build = {
            "nvr": "microshift-4.17.1-1",
            "extra": {"source": {"original_url": "https://example.com/microshift.git#deadbeef"}},
            "creation_time": "2026-09-16 10:00:00.000000",
            "completion_time": "2026-09-16 10:05:00.000000",
            "task_id": 123,
            "build_id": 456,
        }

        with patch.object(rpms_cli, "get_build_objects", return_value=[build]):
            await rpms_cli.update_konflux_db(runtime, rpm, {"nvrs": build["nvr"]})

        stored_record = runtime.konflux_db.add_build.call_args.args[0]
        self.assertEqual(stored_record.engine, Engine.BREW)
        self.assertEqual(stored_record.build_variant, BuildVariant.MICROSHIFT)

    def test_image_record_stores_runtime_variant(self):
        image_build = MagicMock(start_time=datetime(2026, 9, 16, 10, 0), end_time=datetime(2026, 9, 16, 10, 5))
        image_distgit = MagicMock()
        image_distgit.runtime.konflux_db = MagicMock()
        image_distgit.runtime.group = "microshift-4.17"
        image_distgit.runtime.assembly = "stream"
        image_distgit.runtime.build_system = "brew"
        image_distgit.runtime.variant = BuildVariant.MICROSHIFT
        image_distgit.dg_path = Path("/tmp/microshift")
        image_distgit.distgit_dir = "/tmp/microshift"
        image_distgit.metadata.distgit_key = "microshift"
        image_distgit.metadata.get_arches.return_value = ["x86_64"]

        dockerfile = MagicMock()
        dockerfile.labels = {
            "io.openshift.build.source-location": "https://example.com/microshift.git",
            "io.openshift.build.commit.id": "deadbeef",
            "com.redhat.component": "microshift",
            "version": "4.17.1",
            "release": "1.p0",
        }

        with (
            patch.object(distgit, "DockerfileParser", return_value=dockerfile),
            patch.object(distgit, "gather_git", return_value=("", "https://example.com/microshift.git", "")),
        ):
            distgit.ImageDistGitRepo.update_konflux_db(
                image_distgit,
                image_build,
                KonfluxBuildOutcome.FAILURE,
            )

        stored_record = image_distgit.runtime.konflux_db.add_build.call_args.args[0]
        self.assertEqual(stored_record.engine, Engine.BREW)
        self.assertEqual(stored_record.build_variant, BuildVariant.MICROSHIFT)

    def test_olm_bundle_record_stores_runtime_variant(self):
        runtime = MagicMock()
        runtime.images = []
        runtime.group = "microshift-4.17"
        runtime.assembly = "stream"
        runtime.variant = BuildVariant.MICROSHIFT
        runtime.konflux_db = MagicMock()
        runtime.record_logger = MagicMock()

        bundle = MagicMock()
        bundle.operator_nvr = "operator-1.0-1"
        bundle.does_bundle_branch_exist.return_value = (True, "")
        bundle.bundle_clone_path = "/tmp/microshift-bundle"
        bundle.bundle_name = "microshift-bundle"
        bundle.operator_dict = {"nvr": "operator-1.0-1"}
        bundle.found_image_references = {"operand": "operand-1.0-1"}
        bundle.build.return_value = (
            123,
            "https://brew.example.com/task/123",
            {
                "nvr": "microshift-bundle-1.0-1",
                "version": "1.0",
                "release": "1",
                "start_time": "2026-09-16 10:00:00.000000",
                "completion_time": "2026-09-16 10:05:00.000000",
                "id": 456,
                "extra": {"image": {"index": {"pull": ["quay.io/example/bundle"], "tags": ["latest"]}}},
            },
        )

        dockerfile = MagicMock()
        dockerfile.labels = {"io.openshift.build.commit.url": "https://example.com/microshift.git/commit/deadbeef"}
        operator_build = {"source": "https://example.com/operator.git"}

        def run_parallel(function, bundles):
            return MagicMock(get=lambda: [function(item, 0) for item in bundles])

        with (
            patch.object(olm_bundle_cli.koji, "ClientSession", return_value=MagicMock()),
            patch.object(olm_bundle_cli.brew, "get_build_objects", return_value=[operator_build]),
            patch.object(olm_bundle_cli, "OLMBundle", return_value=bundle),
            patch.object(olm_bundle_cli.exectools, "parallel_exec", side_effect=run_parallel),
            patch.object(olm_bundle_cli, "DockerfileParser", return_value=dockerfile),
            patch.object(olm_bundle_cli, "gather_git", return_value=("", "https://example.com/microshift.git", "")),
            patch.object(olm_bundle_cli.sys, "exit"),
        ):
            olm_bundle_cli.rebase_and_build_olm_bundle.callback.__wrapped__(
                runtime,
                ("operator-1.0-1",),
                force=True,
                dry_run=False,
            )

        stored_record = runtime.konflux_db.add_build.call_args.args[0]
        self.assertEqual(stored_record.engine, Engine.BREW)
        self.assertEqual(stored_record.build_variant, BuildVariant.MICROSHIFT)
