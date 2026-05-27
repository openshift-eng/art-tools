"""
Tests for doozerlib.lockfile_prototype.dockerfile_parser.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from doozerlib.lockfile_prototype.dockerfile_parser import (
    analyze_dockerfile_stages,
    build_copy_map,
    collect_stage_vars,
    extract_packages_from_file_installs,
    extract_packages_from_scripts,
)


def _make_entry(instruction: str, value: str) -> dict:
    """
    Build a minimal DockerfileParser structure entry for testing.
    """
    return {"instruction": instruction, "value": value, "startline": 0, "endline": 0, "content": ""}


class TestCollectStageVars(unittest.TestCase):
    def test_arg_with_default(self):
        entries = [_make_entry("ARG", "GCC_VERSION=12")]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {"GCC_VERSION": "12"})

    def test_arg_with_quoted_default(self):
        entries = [_make_entry("ARG", 'GCC_VERSION="12"')]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {"GCC_VERSION": "12"})

    def test_arg_without_default(self):
        entries = [_make_entry("ARG", "KERNEL_VERSION")]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {})

    def test_arg_inherits_global(self):
        entries = [_make_entry("ARG", "KERNEL_VERSION")]
        result = collect_stage_vars(entries, inherited_vars={"KERNEL_VERSION": "5.14"})
        self.assertEqual(result, {"KERNEL_VERSION": "5.14"})

    def test_env_with_equals(self):
        entries = [_make_entry("ENV", "LANG=en_US.UTF-8")]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {"LANG": "en_US.UTF-8"})

    def test_env_with_space(self):
        entries = [_make_entry("ENV", "LANG en_US.UTF-8")]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {"LANG": "en_US.UTF-8"})

    def test_env_references_arg(self):
        entries = [
            _make_entry("ARG", "GCC_VERSION=12"),
            _make_entry("ENV", "GCC_VERSION=${GCC_VERSION}"),
        ]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {"GCC_VERSION": "12"})

    def test_mixed_arg_env(self):
        entries = [
            _make_entry("ARG", "BASE_VERSION=9"),
            _make_entry("ENV", "INSTALL_DIR=/opt"),
            _make_entry("RUN", "echo hello"),
        ]
        result = collect_stage_vars(entries)
        self.assertEqual(result, {"BASE_VERSION": "9", "INSTALL_DIR": "/opt"})


class TestExtractPackagesFromFileInstalls(unittest.TestCase):
    def test_xargs_dnf_install_from_file(self):
        """
        xargs dnf install < /tmp/pkgs-list should read packages from the file.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            pkg_file = source_dir / "main-packages-list.txt"
            pkg_file.write_text("httpd\npython3-pip\n# comment\nqemu-img\n")
            copy_map = {"/tmp/main-packages-list.txt": "main-packages-list.txt"}

            run_values = ["xargs -rtd'\\n' dnf install -y < /tmp/main-packages-list.txt"]
            result, arch_result = extract_packages_from_file_installs(run_values, copy_map, source_dir)
            self.assertEqual(result, ["httpd", "python3-pip", "qemu-img"])
            self.assertEqual(arch_result, {})

    def test_file_redirect_with_env_var(self):
        """
        File path containing env vars should be resolved before lookup.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            pkg_file = source_dir / "pkgs.ocp"
            pkg_file.write_text("coreos-installer\nsqlite\n")
            copy_map = {"/tmp/pkgs.ocp": "pkgs.ocp"}

            run_values = ["xargs -rtd'\\n' dnf install -y < /tmp/${PKGS_LIST}"]
            result, _ = extract_packages_from_file_installs(
                run_values,
                copy_map,
                source_dir,
                env_vars={"PKGS_LIST": "pkgs.ocp"},
            )
            self.assertEqual(result, ["coreos-installer", "sqlite"])

    def test_file_not_in_copy_map_skipped(self):
        """
        File paths not found in copy_map should be skipped.
        """
        with TemporaryDirectory() as tmpdir:
            run_values = ["xargs dnf install -y < /unknown/path.txt"]
            result, arch_result = extract_packages_from_file_installs(run_values, {}, Path(tmpdir))
            self.assertEqual(result, [])
            self.assertEqual(arch_result, {})

    def test_version_constraints_in_package_list(self):
        """
        Package list entries with version constraints should extract
        only the package name.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            pkg_file = source_dir / "packages.txt"
            pkg_file.write_text("crudini >= 0.9.3-4.el9\npython3.12-babel >= 2.9.1\nsimple-pkg\n")
            copy_map = {"/tmp/packages.txt": "packages.txt"}

            run_values = ["xargs -rtd'\\n' dnf install -y < /tmp/packages.txt"]
            result, _ = extract_packages_from_file_installs(run_values, copy_map, source_dir)
            self.assertEqual(result, ["crudini", "python3.12-babel", "simple-pkg"])

    def test_pipe_to_xargs_install(self):
        """
        grep ... file | xargs dnf install should read packages from file.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            pkg_file = source_dir / "main-packages-list.ocp"
            pkg_file.write_text("httpd\nqemu-img\n# comment\nsqlite\n")
            copy_map = {"/tmp/main-packages-list.ocp": "main-packages-list.ocp"}

            run_values = ["grep -vE '^(#|$)' /tmp/main-packages-list.ocp | xargs -rtd'\\n' dnf install -y"]
            result, arch_result = extract_packages_from_file_installs(run_values, copy_map, source_dir)
            self.assertEqual(result, ["httpd", "qemu-img", "sqlite"])
            self.assertEqual(arch_result, {})

    def test_pipe_with_env_var_in_path(self):
        """
        Pipe pattern with env var in file path should resolve correctly.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            pkg_file = source_dir / "packages-list.ocp"
            pkg_file.write_text("dosfstools\nipmitool\n")
            copy_map = {"/tmp/packages-list.ocp": "packages-list.ocp"}

            run_values = ["grep -vE '^(#|$)' /tmp/${PKGS_LIST} | xargs -rtd'\\n' dnf install -y"]
            result, _ = extract_packages_from_file_installs(
                run_values,
                copy_map,
                source_dir,
                env_vars={"PKGS_LIST": "packages-list.ocp"},
            )
            self.assertEqual(result, ["dosfstools", "ipmitool"])

    def test_arch_suffix_in_filename(self):
        """
        File path with $(arch) should resolve per-architecture and return
        arch-specific packages.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            x86_file = source_dir / "packages-list.ocp-x86_64"
            x86_file.write_text("biosdevname\n")
            copy_map = {"/tmp/packages-list.ocp": "packages-list.ocp"}

            run_values = ["grep -vE '^(#|$)' /tmp/${PKGS_LIST}-$(arch) | xargs -rtd'\\n' dnf install -y"]
            result, arch_result = extract_packages_from_file_installs(
                run_values,
                copy_map,
                source_dir,
                env_vars={"PKGS_LIST": "packages-list.ocp"},
            )
            self.assertEqual(result, [])
            self.assertEqual(arch_result.get("x86_64"), ["biosdevname"])
            self.assertNotIn("aarch64", arch_result)

    def test_arch_suffix_file_not_found_skipped(self):
        """
        When no arch-specific file exists, no packages are returned.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            copy_map = {"/tmp/pkgs": "pkgs"}

            run_values = ["grep -vE '^(#|$)' /tmp/pkgs-$(arch) | xargs -rtd'\\n' dnf install -y"]
            result, arch_result = extract_packages_from_file_installs(
                run_values,
                copy_map,
                source_dir,
            )
            self.assertEqual(result, [])
            self.assertEqual(arch_result, {})


class TestBuildCopyMap(unittest.TestCase):
    def test_copy_file_to_directory(self):
        entries = [_make_entry("COPY", "hack/foo.sh /tmp")]
        result = build_copy_map(entries)
        self.assertEqual(result["/tmp/foo.sh"], "hack/foo.sh")

    def test_copy_file_to_directory_with_trailing_slash(self):
        entries = [_make_entry("COPY", "hack/foo.sh /tmp/")]
        result = build_copy_map(entries)
        self.assertEqual(result["/tmp/foo.sh"], "hack/foo.sh")

    def test_copy_file_to_exact_path(self):
        entries = [_make_entry("COPY", "hack/foo.sh /opt/renamed.sh")]
        result = build_copy_map(entries)
        self.assertEqual(result["/opt/renamed.sh"], "hack/foo.sh")

    def test_skips_copy_from(self):
        entries = [_make_entry("COPY", "--from=builder /app/bin /usr/local/bin")]
        result = build_copy_map(entries)
        self.assertEqual(result, {})

    def test_multiple_copies(self):
        entries = [
            _make_entry("COPY", "hack/install.sh /tmp"),
            _make_entry("COPY", "scripts/setup.sh /opt/"),
        ]
        result = build_copy_map(entries)
        self.assertEqual(result["/tmp/install.sh"], "hack/install.sh")
        self.assertEqual(result["/opt/setup.sh"], "scripts/setup.sh")

    def test_copy_with_chown_flag(self):
        entries = [_make_entry("COPY", "--chown=root:root hack/foo.sh /tmp/")]
        result = build_copy_map(entries)
        self.assertEqual(result["/tmp/foo.sh"], "hack/foo.sh")

    def test_non_copy_entries_ignored(self):
        entries = [
            _make_entry("RUN", "echo hello"),
            _make_entry("ENV", "FOO=bar"),
        ]
        result = build_copy_map(entries)
        self.assertEqual(result, {})


class TestExtractPackagesFromScripts(unittest.TestCase):
    def test_script_via_bash_interpreter(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "hack" / "install.sh"
            script.parent.mkdir()
            script.write_text('PKGS="nmap-ncat procps-ng"\ndnf install -y ${PKGS}\n')

            copy_map = {"/tmp/install.sh": "hack/install.sh"}
            run_values = ["/bin/bash /tmp/install.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("nmap-ncat", result.packages)
            self.assertIn("procps-ng", result.packages)

    def test_script_via_usr_bin_bash(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "setup.sh"
            script.write_text("dnf install -y wget\n")

            copy_map = {"/opt/setup.sh": "setup.sh"}
            run_values = ["/usr/bin/bash /opt/setup.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("wget", result.packages)

    def test_script_via_sh_interpreter(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "install.sh"
            script.write_text("yum install -y curl\n")

            copy_map = {"/tmp/install.sh": "install.sh"}
            run_values = ["sh /tmp/install.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("curl", result.packages)

    def test_script_after_cachi2_env_sourcing(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "hack" / "install.sh"
            script.parent.mkdir()
            script.write_text("dnf install -y pciutils rsync\n")

            copy_map = {"/tmp/install.sh": "hack/install.sh"}
            run_values = [". /cachi2/cachi2.env && /bin/bash /tmp/install.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("pciutils", result.packages)
            self.assertIn("rsync", result.packages)

    def test_direct_script_invocation_still_works(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "install-deps.sh"
            script.write_text("dnf install -y git\n")

            run_values = ["/src/install-deps.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir)
            self.assertIn("git", result.packages)

    def test_bare_script_name_resolved_via_copy_map(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "prepare-image.sh"
            script.write_text("dnf install -y httpd qemu-img\n")

            copy_map = {"/bin/prepare-image.sh": "prepare-image.sh"}
            run_values = ["set -euxo pipefail && prepare-image.sh && rm -f /bin/prepare-image.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("httpd", result.packages)
            self.assertIn("qemu-img", result.packages)

    def test_script_not_in_copy_map_skipped(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            run_values = ["/bin/bash /opt/unknown.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map={})
            self.assertEqual(result.packages, [])

    def test_cluster_node_tuning_pattern(self):
        """
        Script reassigns INSTALL_PKGS with $INSTALL_PKGS self-reference,
        which requires resolving variables at assignment time.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            hack_dir = source_dir / "hack"
            hack_dir.mkdir()
            script = hack_dir / "dockerfile_install_support.sh"
            script.write_text(
                'INSTALL_PKGS="nmap-ncat procps-ng pciutils rsync"\n'
                'source /etc/os-release\n'
                'if [[ "${ID}" == "centos" ]]; then\n'
                '  echo "centos"\n'
                'else\n'
                '  INSTALL_PKGS=" \\\n'
                '     tuned tuned-profiles-atomic tuned-profiles-cpu-partitioning \\\n'
                '     $INSTALL_PKGS"\n'
                '  dnf install --setopt=tsflags=nodocs -y ${INSTALL_PKGS}\n'
                "fi\n"
            )

            copy_map = {"/tmp/dockerfile_install_support.sh": "hack/dockerfile_install_support.sh"}
            run_values = ["/bin/bash /tmp/dockerfile_install_support.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("nmap-ncat", result.packages)
            self.assertIn("procps-ng", result.packages)
            self.assertIn("pciutils", result.packages)
            self.assertIn("rsync", result.packages)
            self.assertIn("tuned", result.packages)
            self.assertIn("tuned-profiles-atomic", result.packages)
            self.assertIn("tuned-profiles-cpu-partitioning", result.packages)

    def test_arch_specific_packages_from_script(self):
        """
        Arch-conditional packages in scripts should be returned in the
        arch dict, not silently dropped.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "prepare-efi.sh"
            script.write_text(
                'dnf install -y grub2 dosfstools\n'
                'if [[ "$ARCH" == "x86_64" ]]; then\n'
                '    GRUB_PKG=grub2-efi-x64\n'
                '    SHIM_PKG=shim-x64\n'
                'elif [[ "$ARCH" == "aarch64" ]]; then\n'
                '    GRUB_PKG=grub2-efi-aa64\n'
                '    SHIM_PKG=shim-aa64\n'
                'fi\n'
                'dnf install -y "$GRUB_PKG" "$SHIM_PKG"\n'
            )

            copy_map = {"/bin/prepare-efi.sh": "prepare-efi.sh"}
            run_values = ["prepare-efi.sh redhat"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("grub2", result.packages)
            self.assertIn("dosfstools", result.packages)
            self.assertNotIn("grub2-efi-x64", result.packages)
            self.assertNotIn("shim-x64", result.packages)
            self.assertEqual(result.arch_packages.get("x86_64"), ["grub2-efi-x64", "shim-x64"])
            self.assertEqual(result.arch_packages.get("aarch64"), ["grub2-efi-aa64", "shim-aa64"])

    def test_bare_update_in_script_sets_has_update(self):
        """
        A bare 'yum update -y' (no packages) inside an invoked script
        must cause the 4th return value to be True so the caller can
        treat the stage as having an implicit bare update.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "install-python-deps-ocp.sh"
            script.write_text("yum update -y\npip install some-package\n")

            run_values = [". /cachi2/cachi2.env && /src/install-python-deps-ocp.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir)
            self.assertTrue(result.has_update)
            self.assertEqual(result.packages, [])
            self.assertEqual(result.update_targets, [])

    def test_no_bare_update_in_script_has_update_false(self):
        """
        A script with only install commands (no update) must return
        has_update=False.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "install-deps.sh"
            script.write_text("dnf install -y curl wget\n")

            run_values = ["/src/install-deps.sh"]
            result = extract_packages_from_scripts(run_values, source_dir=source_dir)
            self.assertFalse(result.has_update)
            self.assertIn("curl", result.packages)
            self.assertIn("wget", result.packages)


class TestAnalyzeDockerfileStages(unittest.TestCase):
    def _write_dockerfile(self, tmpdir: str, content: str) -> Path:
        path = Path(tmpdir) / "Dockerfile"
        path.write_text(content)
        return path

    def test_per_stage_extraction(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                "FROM builder AS build\n"
                "RUN dnf install -y gcc nmstate-devel git\n"
                "\n"
                "FROM base-rhel9\n"
                "RUN dnf install -y postgresql-server skopeo\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(len(stages), 2)
            self.assertEqual(stages[0].packages, ["gcc", "git", "nmstate-devel"])
            self.assertEqual(stages[1].packages, ["postgresql-server", "skopeo"])
            self.assertEqual([s.has_update for s in stages], [False, False])

    def test_per_stage_single_stage(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN yum -y install nfs-utils jq\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(len(stages), 1)
            self.assertEqual(stages[0].packages, ["jq", "nfs-utils"])
            self.assertEqual([s.has_update for s in stages], [False])

    def test_detect_update(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN yum update -y && yum clean all\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, [])
            self.assertEqual([s.has_update for s in stages], [True])

    def test_detect_update_microdnf(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN microdnf update -y && microdnf clean all\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual([s.has_update for s in stages], [True])

    def test_shell_variable_expansion(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                'FROM base\n'
                'RUN INSTALL_PKGS=" \\\n'
                '      which tar wget hostname" && \\\n'
                '    dnf install -y --nodocs ${INSTALL_PKGS} gpgme && \\\n'
                '    dnf clean all\n'
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, ["gpgme", "hostname", "tar", "wget", "which"])
            self.assertEqual([s.has_update for s in stages], [False])

    def test_arg_env_variable_resolution(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                "ARG GCC_VERSION=12\n"
                "ARG KERNEL_VERSION\n"
                "FROM base\n"
                "ARG GCC_VERSION\n"
                "ARG KERNEL_VERSION\n"
                "RUN dnf install -y \\\n"
                "    gcc-${GCC_VERSION} \\\n"
                "    gcc-c++-${GCC_VERSION} \\\n"
                "    kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION} \\\n"
                "    make\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, ["gcc-12", "gcc-c++-12", "kernel-devel", "make"])

    def test_global_arg_with_default_inherited_by_stage(self):
        with TemporaryDirectory() as tmpdir:
            content = "ARG BASE_VERSION=9\nFROM base\nARG BASE_VERSION\nRUN dnf install -y rhel-${BASE_VERSION}-pkg\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, ["rhel-9-pkg"])

    def test_env_variable_resolution(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nENV MY_PKG=httpd\nRUN dnf install -y ${MY_PKG}\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, ["httpd"])

    def test_no_update_no_install(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN echo hello\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, [])
            self.assertEqual([s.has_update for s in stages], [False])

    def test_arch_conditional_packages(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                "FROM base\n"
                "RUN dnf install -y make && \\\n"
                "    if [ $(arch) = x86_64 ]; then \\\n"
                "    dnf -y install kernel-rt-devel; \\\n"
                "    fi && \\\n"
                "    if [ $(arch) = aarch64 ]; then \\\n"
                "    dnf -y install kernel-64k-devel; \\\n"
                "    fi\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, ["make"])
            self.assertEqual(stages[0].arch_packages, {"aarch64": ["kernel-64k-devel"], "x86_64": ["kernel-rt-devel"]})

    def test_arch_conditional_variable(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                "FROM base\n"
                'RUN if [ $(arch) == "x86_64" ] || [ $(arch) == "aarch64" ]; then \\\n'
                '    ARCH_DEP_PKGS="mokutil"; fi && \\\n'
                "    dnf -y install openssl keyutils $ARCH_DEP_PKGS\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path)
            self.assertEqual(stages[0].packages, ["keyutils", "openssl"])
            self.assertEqual(stages[0].arch_packages, {"aarch64": ["mokutil"], "x86_64": ["mokutil"]})

    def test_copy_then_bash_script(self):
        with TemporaryDirectory() as tmpdir:
            hack_dir = Path(tmpdir) / "hack"
            hack_dir.mkdir()
            (hack_dir / "install.sh").write_text('INSTALL_PKGS="nmap-ncat procps-ng"\ndnf install -y ${INSTALL_PKGS}\n')
            content = "FROM base\nCOPY hack/install.sh /tmp\nRUN /bin/bash /tmp/install.sh\n"
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path, source_dir=Path(tmpdir))
            self.assertIn("nmap-ncat", stages[0].packages)
            self.assertIn("procps-ng", stages[0].packages)

    def test_script_arch_packages_flow_to_stages(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            (source_dir / "prepare-efi.sh").write_text(
                'dnf install -y grub2 dosfstools\n'
                'if [[ "$ARCH" == "x86_64" ]]; then\n'
                '    GRUB_PKG=grub2-efi-x64\n'
                '    SHIM_PKG=shim-x64\n'
                'elif [[ "$ARCH" == "aarch64" ]]; then\n'
                '    GRUB_PKG=grub2-efi-aa64\n'
                '    SHIM_PKG=shim-aa64\n'
                'fi\n'
                'dnf install -y "$GRUB_PKG" "$SHIM_PKG"\n'
            )
            content = (
                "FROM builder AS build\n"
                "COPY prepare-efi.sh /bin/\n"
                "RUN prepare-efi.sh redhat\n"
                "\n"
                "FROM base-rhel9\n"
                "RUN dnf install -y httpd\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path, source_dir=source_dir)
            self.assertIn("grub2", stages[0].packages)
            self.assertIn("dosfstools", stages[0].packages)
            self.assertNotIn("grub2-efi-x64", stages[0].packages)
            self.assertEqual(stages[0].arch_packages.get("x86_64"), ["grub2-efi-x64", "shim-x64"])
            self.assertEqual(stages[0].arch_packages.get("aarch64"), ["grub2-efi-aa64", "shim-aa64"])
            self.assertEqual(stages[1].packages, ["httpd"])

    def test_ironic_agent_pipe_and_arch_file_pattern(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            (source_dir / "packages-list.ocp").write_text("ipmitool\nqemu-img\n")
            (source_dir / "packages-list.ocp-x86_64").write_text("biosdevname\n")
            (source_dir / "prepare-image.sh").write_text(
                "grep -vE '^(#|$)' /tmp/${PKGS_LIST} | xargs -rtd'\\n' dnf install -y\n"
                'if [[ -s /tmp/${PKGS_LIST}-$(arch) ]]; then\n'
                "    grep -vE '^(#|$)' /tmp/${PKGS_LIST}-$(arch) | xargs -rtd'\\n' dnf install -y\n"
                "fi\n"
            )
            content = (
                "FROM base-rhel9\n"
                "ENV PKGS_LIST=packages-list.ocp\n"
                "COPY ${PKGS_LIST}* /tmp/\n"
                "COPY prepare-image.sh /bin/\n"
                "RUN prepare-image.sh\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path, source_dir=source_dir)
            self.assertIn("ipmitool", stages[0].packages)
            self.assertIn("qemu-img", stages[0].packages)
            self.assertNotIn("biosdevname", stages[0].packages)
            self.assertEqual(stages[0].arch_packages.get("x86_64"), ["biosdevname"])
            self.assertNotIn("aarch64", stages[0].arch_packages)

    def test_bare_update_in_script_propagates_to_has_update(self):
        """
        A bare 'yum update -y' inside a script invoked by a Dockerfile RUN
        must cause has_update=True for that stage so _resolve_bare_update_targets
        queries the base image for upgrade targets.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            (source_dir / "install-python-deps-ocp.sh").write_text("yum update -y\npip install requests==2.28.0\n")
            content = (
                "FROM builder AS build\n"
                "RUN dnf install -y gcc\n"
                "\n"
                "FROM base-rhel9\n"
                "COPY install-python-deps-ocp.sh /src/\n"
                "RUN . /cachi2/cachi2.env && /src/install-python-deps-ocp.sh\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            stages, _ = analyze_dockerfile_stages(path, source_dir=source_dir)
            self.assertFalse(stages[0].has_update, "builder stage should have no update")
            self.assertTrue(stages[1].has_update, "final stage script bare update must propagate")
