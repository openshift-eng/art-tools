from unittest import TestCase
from unittest.mock import MagicMock, patch

from doozerlib.cli.release_gen_assembly_targeted import AssemblyParams, GenAssemblyTargetedCli, KernelInfo
from pydantic import ValidationError


def _make_runtime():
    runtime = MagicMock()
    runtime.build_system = "konflux"
    runtime.group_config.vars.MAJOR = 4
    runtime.group_config.vars.MINOR = 17
    runtime.group_config.arches = ["x86_64", "aarch64"]
    runtime.group_config.rhcos.get.return_value = False
    return runtime


def _make_params(**overrides) -> AssemblyParams:
    defaults = {
        "assembly_name": "4.17.5",
        "basis_assembly": "4.17.4",
        "kernel_nvrs": ("kernel-5.14.0-284.28.1.el9_2",),
        "bug_ids": ("OCPBUGS-12345",),
        "cve_ids": ("CVE-2025-1234",),
    }
    defaults.update(overrides)
    return AssemblyParams(**defaults)


def _make_kernel_info(**overrides) -> KernelInfo:
    defaults = {
        "nvr": "kernel-5.14.0-284.28.1.el9_2",
        "el_ver": 9,
        "kernel_core_nvr": "kernel-core-5.14.0-284.28.1.el9_2",
        "kernel_devel_nvr": "kernel-devel-5.14.0-284.28.1.el9_2",
    }
    defaults.update(overrides)
    return KernelInfo(**defaults)


def _make_cli(**overrides):
    params_keys = set(AssemblyParams.model_fields)
    params_kw = {k: overrides.pop(k) for k in list(overrides) if k in params_keys}
    params = _make_params(**params_kw)
    runtime = overrides.pop("runtime", _make_runtime())
    return GenAssemblyTargetedCli(runtime=runtime, params=params)


class TestAssemblyParamsValidation(TestCase):
    def test_non_kernel_nvr_rejected(self):
        """
        Pydantic rejects NVR that doesn't start with 'kernel'.
        """
        with self.assertRaises(ValidationError) as ctx:
            _make_params(kernel_nvrs=("httpd-2.4.0-1.el9",))
        self.assertIn("does not look like a kernel package", str(ctx.exception))

    def test_empty_kernel_nvrs_allowed_with_image_nvrs(self):
        """
        Empty kernel NVRs tuple is valid when image NVRs are provided (image-only assembly).
        """
        params = _make_params(kernel_nvrs=(), image_nvrs=("ose-installer-container-v4.17.0-202502110000.p0.el9",))
        self.assertEqual(params.kernel_nvrs, ())

    def test_both_empty_pins_rejected(self):
        """
        Both kernel_nvrs and image_nvrs empty raises ValidationError.
        """
        with self.assertRaises(ValidationError) as ctx:
            _make_params(kernel_nvrs=(), image_nvrs=())
        self.assertIn("At least one --kernel-nvr or --image-nvr must be provided", str(ctx.exception))

    def test_valid_params(self):
        """
        Valid params construct without error.
        """
        params = _make_params()
        self.assertEqual(params.assembly_name, "4.17.5")
        self.assertEqual(params.basis_assembly, "4.17.4")


class TestValidateAndParseKernelNvrs(TestCase):
    @patch("doozerlib.cli.release_gen_assembly_targeted.koji.ClientSession")
    def test_valid_kernel_nvr(self, mock_koji_cls):
        """
        Valid kernel NVR populates kernel_info with derived kernel-core and kernel-devel NVRs.
        """
        mock_session = MagicMock()
        mock_koji_cls.return_value = mock_session
        mock_session.getBuild.return_value = {"build_id": 12345, "nvr": "kernel-5.14.0-284.28.1.el9_2"}

        cli = _make_cli()
        cli._validate_and_parse_kernel_nvrs()

        self.assertEqual(len(cli.kernel_info), 1)
        self.assertEqual(cli.kernel_info[0].nvr, "kernel-5.14.0-284.28.1.el9_2")
        self.assertEqual(cli.kernel_info[0].el_ver, 9)
        self.assertEqual(cli.kernel_info[0].kernel_core_nvr, "kernel-core-5.14.0-284.28.1.el9_2")
        self.assertEqual(cli.kernel_info[0].kernel_devel_nvr, "kernel-devel-5.14.0-284.28.1.el9_2")

    @patch("doozerlib.cli.release_gen_assembly_targeted.koji.ClientSession")
    def test_kernel_nvr_not_in_brew(self, mock_koji_cls):
        """
        Kernel NVR not found in Brew raises ValueError.
        """
        mock_session = MagicMock()
        mock_koji_cls.return_value = mock_session
        mock_session.getBuild.return_value = None

        cli = _make_cli()
        with self.assertRaises(ValueError) as ctx:
            cli._validate_and_parse_kernel_nvrs()
        self.assertIn("not found in Brew", str(ctx.exception))

    @patch("doozerlib.cli.release_gen_assembly_targeted.koji.ClientSession")
    def test_multiple_kernel_nvrs(self, mock_koji_cls):
        """
        Multiple kernel NVRs (different RHEL targets) all get parsed correctly.
        """
        mock_session = MagicMock()
        mock_koji_cls.return_value = mock_session
        mock_session.getBuild.return_value = {"build_id": 1, "nvr": ""}

        cli = _make_cli(
            kernel_nvrs=("kernel-5.14.0-284.28.1.el8_6", "kernel-5.14.0-284.28.1.el9_2"),
        )
        cli._validate_and_parse_kernel_nvrs()

        self.assertEqual(len(cli.kernel_info), 2)
        self.assertEqual(cli.kernel_info[0].el_ver, 8)
        self.assertEqual(cli.kernel_info[1].el_ver, 9)


class TestFindRhcosBuildNonlayered(TestCase):
    def _make_cli_with_rhcos_config(self):
        cli = _make_cli()
        cli.runtime.group_config.urls.rhcos_release_base.get.return_value = None
        cli.runtime.group_config.rhcos.payload_tags = [
            MagicMock(name="machine-os-content", build_metadata_key="oscontainer", primary=True),
        ]
        return cli

    @patch("doozerlib.cli.release_gen_assembly_targeted.get_container_configs")
    @patch("doozerlib.cli.release_gen_assembly_targeted.get_container_pullspec")
    @patch("doozerlib.cli.release_gen_assembly_targeted.RHCOSBuildFinder")
    def test_find_matching_build(self, mock_finder_cls, mock_pullspec, mock_container_configs):
        """
        Non-layered RHCOS build found when commitmeta contains matching kernel-core.
        """
        cli = self._make_cli_with_rhcos_config()
        cli.kernel_info = [_make_kernel_info()]

        container_conf = MagicMock()
        container_conf.name = "machine-os-content"
        mock_container_configs.return_value = [container_conf]

        mock_finder = MagicMock()
        mock_finder_cls.return_value = mock_finder

        commitmeta = {
            "rpmostree.rpmdb.pkglist": [
                ["kernel-core", "0", "5.14.0", "284.28.1.el9_2", "x86_64"],
            ],
        }
        meta = {
            "buildid": "417.92.202401150000-0",
            "oscontainer": {
                "digest": "sha256:abc123",
                "image": "quay.io/openshift-release-dev/ocp-v4.0-art-dev",
            },
        }
        mock_finder.rhcos_build_meta.side_effect = [commitmeta, meta, meta]
        mock_pullspec.return_value = "quay.io/test@sha256:abc123"

        with patch("doozerlib.cli.release_gen_assembly_targeted.requests") as mock_requests:
            builds_resp = MagicMock()
            builds_resp.json.return_value = {"builds": [{"id": "417.92.202401150000-0"}]}
            mock_requests.get.return_value = builds_resp
            cli._find_rhcos_build_nonlayered()

        self.assertIn("machine-os-content", cli.rhcos_config)
        self.assertIn("images", cli.rhcos_config["machine-os-content"])

    @patch("doozerlib.cli.release_gen_assembly_targeted.get_container_configs")
    @patch("doozerlib.cli.release_gen_assembly_targeted.RHCOSBuildFinder")
    def test_no_matching_build(self, mock_finder_cls, mock_container_configs):
        """
        Raises RuntimeError when no RHCOS build contains the target kernel.
        """
        cli = self._make_cli_with_rhcos_config()
        cli.kernel_info = [_make_kernel_info()]
        mock_container_configs.return_value = []

        mock_finder = MagicMock()
        mock_finder_cls.return_value = mock_finder

        commitmeta = {
            "rpmostree.rpmdb.pkglist": [
                ["kernel-core", "0", "5.14.0", "284.27.1.el9_2", "x86_64"],
            ],
        }
        mock_finder.rhcos_build_meta.return_value = commitmeta

        with patch("doozerlib.cli.release_gen_assembly_targeted.requests") as mock_requests:
            builds_resp = MagicMock()
            builds_resp.json.return_value = {"builds": [{"id": "417.92.202401150000-0"}]}
            mock_requests.get.return_value = builds_resp

            with self.assertRaises(RuntimeError) as ctx:
                cli._find_rhcos_build_nonlayered()
            self.assertIn("No complete RHCOS build found", str(ctx.exception))


class TestFindDtkBuildBrew(TestCase):
    @patch("doozerlib.cli.release_gen_assembly_targeted.koji.ClientSession")
    def test_find_matching_dtk(self, mock_koji_cls):
        """
        DTK build found in Brew when it contains matching kernel-devel.
        """
        mock_session = MagicMock()
        mock_koji_cls.return_value = mock_session
        mock_session.listTagged.return_value = [
            {"nvr": "driver-toolkit-container-v4.17.0-202401150000.el9", "build_id": 100},
        ]
        mock_session.listArchives.return_value = [{"id": 200}]
        mock_session.listRPMs.return_value = [
            {"name": "kernel-devel", "version": "5.14.0", "release": "284.28.1.el9_2"},
            {"name": "glibc", "version": "2.34", "release": "1.el9"},
        ]

        cli = _make_cli()
        cli.runtime.build_system = "brew"
        cli.kernel_info = [_make_kernel_info()]
        cli._find_dtk_build_brew()

        self.assertEqual(cli.dtk_nvr, "driver-toolkit-container-v4.17.0-202401150000.el9")

    @patch("doozerlib.cli.release_gen_assembly_targeted.koji.ClientSession")
    def test_dtk_not_found(self, mock_koji_cls):
        """
        Raises RuntimeError when no DTK build contains matching kernel-devel.
        """
        mock_session = MagicMock()
        mock_koji_cls.return_value = mock_session
        mock_session.listTagged.return_value = [
            {"nvr": "driver-toolkit-container-v4.17.0-202401150000.el9", "build_id": 100},
        ]
        mock_session.listArchives.return_value = [{"id": 200}]
        mock_session.listRPMs.return_value = [
            {"name": "kernel-devel", "version": "5.14.0", "release": "284.27.1.el9_2"},
        ]

        cli = _make_cli()
        cli.runtime.build_system = "brew"
        cli.kernel_info = [_make_kernel_info()]
        with self.assertRaises(RuntimeError) as ctx:
            cli._find_dtk_build_brew()
        self.assertIn("No driver-toolkit build found", str(ctx.exception))


class TestBuildAssemblyDefinition(TestCase):
    def test_assembly_definition_structure(self):
        """
        Assembly definition has correct structure with basis, issues, kernel/DTK pins, and RHCOS.
        """
        cli = _make_cli()
        cli.kernel_info = [_make_kernel_info()]
        cli.rhcos_config = {
            "machine-os-content": {
                "images": {
                    "x86_64": "quay.io/test@sha256:abc",
                    "aarch64": "quay.io/test@sha256:def",
                },
            },
        }
        cli.dtk_nvr = "driver-toolkit-container-v4.17.0-202401150000.el9"

        result = cli._build_assembly_definition()

        self.assertIn("releases", result)
        self.assertIn("4.17.5", result["releases"])
        assembly = result["releases"]["4.17.5"]["assembly"]

        self.assertEqual(assembly["type"], "standard")
        self.assertEqual(assembly["basis"]["assembly"], "4.17.4")
        self.assertEqual(assembly["basis"]["reference_releases!"], {})
        self.assertTrue(assembly["issues"]["targeted_fixes_only"])
        self.assertEqual(assembly["issues"]["include"], [{"id": "OCPBUGS-12345"}])

        kernel_pin = assembly["members"]["rpms"][0]
        self.assertEqual(kernel_pin["distgit_key"], "kernel")
        self.assertEqual(kernel_pin["metadata"]["is"]["el9"], "kernel-5.14.0-284.28.1.el9_2")
        self.assertIn("CVE-2025-1234", kernel_pin["why"])

        dtk_pin = assembly["members"]["images"][0]
        self.assertEqual(dtk_pin["distgit_key"], "driver-toolkit")
        self.assertEqual(dtk_pin["metadata"]["is"]["nvr"], cli.dtk_nvr)

        self.assertIn("machine-os-content", assembly["rhcos"])

    def test_multiple_kernel_targets_merged(self):
        """
        Multiple kernel NVRs for different RHEL targets merged into single pin entry.
        """
        cli = _make_cli(
            kernel_nvrs=("kernel-5.14.0-284.28.1.el8_6", "kernel-5.14.0-284.28.1.el9_2"),
            bug_ids=("OCPBUGS-11111", "OCPBUGS-22222"),
            cve_ids=("CVE-2025-1234",),
        )
        cli.kernel_info = [
            _make_kernel_info(
                nvr="kernel-5.14.0-284.28.1.el8_6",
                el_ver=8,
                kernel_core_nvr="kernel-core-5.14.0-284.28.1.el8_6",
                kernel_devel_nvr="kernel-devel-5.14.0-284.28.1.el8_6",
            ),
            _make_kernel_info(nvr="kernel-5.14.0-284.28.1.el9_2", el_ver=9),
        ]
        cli.dtk_nvr = "dtk-nvr"

        result = cli._build_assembly_definition()
        assembly = result["releases"]["4.17.5"]["assembly"]
        kernel_pin = assembly["members"]["rpms"][0]

        self.assertEqual(kernel_pin["metadata"]["is"]["el8"], "kernel-5.14.0-284.28.1.el8_6")
        self.assertEqual(kernel_pin["metadata"]["is"]["el9"], "kernel-5.14.0-284.28.1.el9_2")
        self.assertIn("CVE-2025-1234", kernel_pin["why"])
        self.assertEqual(len(assembly["issues"]["include"]), 2)

    def test_candidate_assembly_type_for_rc(self):
        """
        Assembly name like rc.5 produces candidate type.
        """
        cli = _make_cli(
            assembly_name="rc.5",
            basis_assembly="rc.4",
            kernel_nvrs=(),
            image_nvrs=("ose-installer-container-v4.17.0-202502110000.p0.el9",),
        )
        result = cli._build_assembly_definition()
        assembly = result["releases"]["rc.5"]["assembly"]

        self.assertEqual(assembly["type"], "candidate")

    def test_no_targeted_fixes_only_without_bugs(self):
        """
        targeted_fixes_only not set when no bug IDs provided.
        """
        cli = _make_cli(bug_ids=(), kernel_nvrs=(), image_nvrs=("ose-installer-container-v4.17.0-202502110000.p0.el9",))
        result = cli._build_assembly_definition()
        assembly = result["releases"]["4.17.5"]["assembly"]

        self.assertNotIn("targeted_fixes_only", assembly["issues"])

    def test_image_nvrs_pinned(self):
        """
        Extra image NVRs added to members.images alongside DTK.
        """
        cli = _make_cli(
            image_nvrs=(
                "ose-installer-container-v4.18.0-202502110000.p0.el9",
                "ose-installer-artifacts-container-v4.18.0-202502110000.p0.el9",
            ),
        )
        cli.kernel_info = [_make_kernel_info()]
        cli.dtk_nvr = "dtk-nvr"

        result = cli._build_assembly_definition()
        images = result["releases"]["4.17.5"]["assembly"]["members"]["images"]

        self.assertEqual(len(images), 3)
        self.assertEqual(images[0]["distgit_key"], "driver-toolkit")
        self.assertEqual(images[1]["distgit_key"], "ose-installer")
        self.assertEqual(images[1]["metadata"]["is"]["nvr"], "ose-installer-container-v4.18.0-202502110000.p0.el9")
        self.assertEqual(images[2]["distgit_key"], "ose-installer-artifacts")

    def test_image_only_assembly(self):
        """
        Assembly with only image NVRs (no kernel) has no rpms or RHCOS pins.
        """
        cli = _make_cli(
            kernel_nvrs=(),
            bug_ids=(),
            image_nvrs=("ose-installer-container-v4.22.0-202505200000.p0.el9",),
        )

        result = cli._build_assembly_definition()
        assembly = result["releases"]["4.17.5"]["assembly"]

        self.assertNotIn("rpms", assembly["members"])
        self.assertEqual(len(assembly["members"]["images"]), 1)
        self.assertEqual(assembly["members"]["images"][0]["distgit_key"], "ose-installer")
        self.assertEqual(assembly["rhcos"], {})

    def test_release_date_in_group_config(self):
        """
        Release date included in group config when provided.
        """
        cli = _make_cli(
            release_date="2026-Jun-15",
            kernel_nvrs=(),
            image_nvrs=("ose-installer-container-v4.17.0-202502110000.p0.el9",),
        )
        result = cli._build_assembly_definition()
        assembly = result["releases"]["4.17.5"]["assembly"]

        self.assertEqual(assembly["group"]["release_date"], "2026-Jun-15")
