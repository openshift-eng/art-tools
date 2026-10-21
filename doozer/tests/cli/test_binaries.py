import json
import logging
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from artcommonlib.model import Model
from artcommonlib.variants import BuildVariant
from click.testing import CliRunner
from doozerlib import util
from doozerlib.binary import BinaryMetadata
from doozerlib.cli import cli
from doozerlib.cli.__main__ import main  # noqa: F401
from doozerlib.cli.images_konflux import KonfluxBuildCli, KonfluxRebaseCli
from doozerlib.image import ImageMetadata

GROUP_YML = "vars:\n  MAJOR: 4\n  MINOR: 22\nname: openshift-{MAJOR}.{MINOR}\nbranch: rhaos-{MAJOR}.{MINOR}-rhel-9\narches:\n- x86_64\n"
IMAGE_YML = "name: openshift/ose-installer-rhel9\nowners:\n- a@example.com\n"
BINARY_YML = (
    "name: openshift/openshift-install-binaries-rhel9\ncanonical_builders_from_upstream: false\n"
    "from:\n  builder:\n  - member: ose-installer\n  stream: rhel9\nowners:\n- a@example.com\n"
)


def _binary_meta(**config):
    data_obj = Model(
        {"key": "openshift-install", "data": Model({"name": "openshift/x-rhel9", **config}), "filename": "x"}
    )
    rt = MagicMock(group="openshift-4.22", build_system="konflux", assembly="stream", product="ocp")
    return BinaryMetadata(rt, data_obj)


class TestBinaryMetadata(unittest.TestCase):
    def test_kind_and_names(self):
        meta = _binary_meta()
        self.assertIsInstance(meta, ImageMetadata)
        self.assertEqual((meta.kind, ImageMetadata.kind), ("binary", "image"))
        self.assertEqual(meta.get_component_name(), "openshift-install-binaries-container")
        self.assertEqual(meta.get_konflux_component_name("openshift-4-22"), "ose-4-22-openshift-install-binaries")
        self.assertEqual(util.konflux_binary_component_name("openshift-5-0", "opm"), "ose-5-0-opm-binaries")

    def test_never_payload_release_or_image_tree(self):
        meta = _binary_meta(for_payload=True, for_release=True, **{"from": {"member": "ose-installer"}})
        self.assertFalse(meta.is_payload)
        self.assertFalse(meta.for_release)
        self.assertIsNone(meta.resolve_parent())
        meta.runtime.resolve_image.assert_not_called()

    def test_overrides(self):
        meta = _binary_meta(distgit={"component": "custom-container"}, konflux={"component": {"name_override": "c"}})
        self.assertEqual(meta.get_component_name(), "custom-container")
        self.assertEqual(meta.get_konflux_component_name("openshift-4-22"), "c")

    def test_component_name_length_limit(self):
        with self.assertRaises(ValueError):
            _binary_meta().get_konflux_component_name("openshift-4-22-" + "x" * 50)


class TestBinariesSelection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.obd = Path(self.tmp.name, "obd")
        (self.obd / "images").mkdir(parents=True)
        (self.obd / "binaries" / "openshift-install").mkdir(parents=True)
        (self.obd / "group.yml").write_text(GROUP_YML)
        (self.obd / "images" / "ose-installer.yml").write_text(IMAGE_YML)
        (self.obd / "binaries" / "openshift-install.yml").write_text(BINARY_YML)
        (self.obd / "binaries" / "openshift-install" / "Dockerfile").write_text("FROM scratch\n")
        self._commit()
        self.run_index = 0

    def _commit(self):
        git = ["git", "-C", str(self.obd), "-c", "user.email=a@example.com", "-c", "user.name=a"]
        if not (self.obd / ".git").exists():
            subprocess.run(git + ["init", "-q"], check=True)
            subprocess.run(git + ["remote", "add", "origin", "https://example.com/ocp-build-data.git"], check=True)
        subprocess.run(git + ["add", "-A"], check=True)
        subprocess.run(git + ["-c", "core.hooksPath=/dev/null", "commit", "-q", "-m", "data"], check=True)

    def _doozer(self, *args):
        self.run_index += 1
        base = [
            f"--working-dir={self.tmp.name}/w{self.run_index}",
            f"--data-path={self.obd}",
            "--group=openshift-4.22",
            "--assembly=stream",
            "--build-system=konflux",
            "--brew-event=1",
        ]
        root = logging.getLogger()
        handlers, level = list(root.handlers), root.level
        try:
            return CliRunner().invoke(cli, base + list(args))
        finally:
            for handler in root.handlers:
                if handler not in handlers:
                    handler.close()
            root.handlers[:] = handlers
            root.setLevel(level)

    def _json(self, result):
        self.assertEqual(result.exit_code, 0, result.output)
        return json.loads(result.output.strip().splitlines()[-1])

    def test_binaries_are_their_own_kind(self):
        self.assertEqual(self._json(self._doozer("binaries:list", "--json")), {"binaries": ["openshift-install"]})
        self.assertEqual(self._json(self._doozer("images:list", "--json")), {"images": ["ose-installer"]})
        selected = self._doozer("--binaries=openshift-install", "binaries:list", "--json")
        self.assertEqual(self._json(selected), {"binaries": ["openshift-install"]})

    def test_selection_errors(self):
        for args, message in (
            (["--binaries=nope", "binaries:list"], "binaries were either missing or filtered out: nope"),
            (["--binaries=openshift-install", "images:list"], "binaries were either missing or filtered out"),
            (["--images=openshift-install", "images:list"], "images or rpms were either missing or filtered out"),
        ):
            with self.subTest(args=args):
                result = self._doozer(*args)
                self.assertNotEqual(result.exit_code, 0)
                self.assertIn(message, result.output + str(result.exception))

    def test_disabled_binary_needs_load_disabled(self):
        (self.obd / "binaries" / "openshift-install.yml").write_text("mode: disabled\n" + BINARY_YML)
        self._commit()
        self.assertEqual(self._json(self._doozer("binaries:list", "--json")), {"binaries": []})
        loaded = self._doozer("--load-disabled", "binaries:list", "--json")
        self.assertEqual(self._json(loaded), {"binaries": ["openshift-install"]})

    def test_name_clash_with_image_fails(self):
        (self.obd / "binaries" / "ose-installer.yml").write_text(BINARY_YML)
        self._commit()
        result = self._doozer("binaries:list", "--json")
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("has the same name as images/ose-installer", result.output + str(result.exception))

    def test_group_without_binaries_dir(self):
        (self.obd / "binaries" / "openshift-install.yml").unlink()
        (self.obd / "binaries" / "openshift-install" / "Dockerfile").unlink()
        (self.obd / "binaries" / "openshift-install").rmdir()
        (self.obd / "binaries").rmdir()
        self._commit()
        self.assertEqual(self._json(self._doozer("binaries:list", "--json")), {"binaries": []})


class TestKindAwareClis(unittest.IsolatedAsyncioTestCase):
    def _runtime(self, binary_meta):
        runtime = MagicMock()
        runtime.working_dir = tempfile.gettempdir()
        runtime.upcycle = False
        runtime.state = {}
        runtime.binary_metas.return_value = [binary_meta]
        return runtime

    @patch("doozerlib.cli.images_konflux.trace.get_current_span")
    @patch("doozerlib.cli.images_konflux.KonfluxRebaser")
    async def test_rebase_loads_only_binaries(self, rebaser_cls, _):
        meta = MagicMock(distgit_key="openshift-install", kind="binary")
        meta.get_lockfile_backend.return_value = "art-internal"
        runtime = self._runtime(meta)
        rebaser_cls.return_value.rpm_lockfile_generator.ensure_repositories_loaded = AsyncMock()
        rebaser_cls.return_value.rebase_to = AsyncMock(return_value=("v4.22.0", "1"))
        await KonfluxRebaseCli(
            runtime=runtime,
            version="v4.22.0",
            release="1",
            embargoed=False,
            force_yum_updates=False,
            repo_type="unsigned",
            image_repo="repo",
            message="m",
            push=False,
            kind="binary",
        ).run()
        runtime.initialize.assert_called_once_with(mode="binaries", clone_distgits=False, build_system="konflux")
        runtime.ordered_image_metas.assert_not_called()
        rebaser_cls.return_value.rebase_to.assert_awaited_once()
        self.assertIs(rebaser_cls.return_value.rebase_to.await_args.args[0], meta)
        self.assertEqual(runtime.state["images:konflux:rebase"]["images"], {"openshift-install": {"status": "success"}})

    @patch("doozerlib.cli.images_konflux.trace.get_current_span")
    @patch("doozerlib.cli.images_konflux.KonfluxImageBuilder")
    async def test_build_loads_only_binaries(self, builder_cls, _):
        meta = MagicMock(distgit_key="openshift-install", kind="binary")
        meta.should_trigger_base_image_release.return_value = False
        runtime = self._runtime(meta)
        runtime.group = "openshift-4.22"
        runtime.assembly = "4.22.18"
        runtime.variant = BuildVariant.OCP
        runtime.product = "ocp"
        runtime.group_config = Model({})
        builder = builder_cls.return_value
        builder.build = AsyncMock(return_value=("nvr", "plr", {}))
        builder.validate_custom_integration_test_scenarios = AsyncMock()
        builder._konflux_client.ensure_git_auth_secret = AsyncMock(return_value="s")
        builder._konflux_client.token_refresh_loop = AsyncMock()
        builder._konflux_client.delete_git_auth_secret = AsyncMock()
        builder._konflux_client.cleanup_stale_git_auth_secrets = AsyncMock()
        await KonfluxBuildCli(
            runtime=runtime,
            konflux_kubeconfig=None,
            konflux_context=None,
            konflux_namespace="ocp-art-tenant",
            image_repo="repo",
            registry_auth_file=None,
            skip_checks=False,
            dry_run=True,
            plr_template=None,
            build_priority="auto",
            extra_build_args=("RELEASE_VERSION=4.22.18",),
            kind="binary",
        ).run()
        runtime.initialize.assert_called_once_with(mode="binaries", clone_distgits=False)
        runtime.ordered_image_metas.assert_not_called()
        self.assertIs(builder.build.await_args.args[0], meta)
        self.assertEqual(builder_cls.call_args.kwargs["config"].extra_build_args, ("RELEASE_VERSION=4.22.18",))


class TestBinariesCommands(unittest.TestCase):
    def test_commands_registered(self):
        for name in ("binaries:list", "beta:binaries:konflux:rebase", "beta:binaries:konflux:build"):
            self.assertIn(name, cli.commands)
        build_params = {p.name for p in cli.commands["beta:binaries:konflux:build"].params}
        self.assertIn("build_args", build_params)
        self.assertIn("binaries", {p.name for p in cli.params})


if __name__ == "__main__":
    unittest.main()
