"""
Tests for the rpm-lockfile-prototype integration.
"""

import asyncio
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import yaml
from doozerlib.lockfile_prototype import (
    RpmLockfilePrototypeGenerator,
    _build_copy_map,
    _collect_stage_vars,
    _extract_packages_from_file_installs,
    _extract_packages_from_run_commands,
    _extract_packages_from_scripts,
    _resolve_bash_expansion,
    analyze_dockerfile_stages,
    build_rpms_in_yaml,
)


def _make_entry(instruction: str, value: str) -> dict:
    """
    Build a minimal DockerfileParser structure entry for testing.
    """
    return {"instruction": instruction, "value": value, "startline": 0, "endline": 0, "content": ""}


class TestResolveBashExpansion(unittest.TestCase):
    def test_simple_braced_var(self):
        result = _resolve_bash_expansion("gcc-${GCC_VERSION}", {"GCC_VERSION": "12"})
        self.assertEqual(result, "gcc-12")

    def test_simple_unbraced_var(self):
        result = _resolve_bash_expansion("$VAR", {"VAR": "hello"})
        self.assertEqual(result, "hello")

    def test_default_value_when_unset(self):
        result = _resolve_bash_expansion("${VAR:-fallback}", {})
        self.assertEqual(result, "fallback")

    def test_default_value_when_set(self):
        result = _resolve_bash_expansion("${VAR:-fallback}", {"VAR": "actual"})
        self.assertEqual(result, "actual")

    def test_conditional_value_when_set(self):
        result = _resolve_bash_expansion("pkg${VAR:+-}${VAR}", {"VAR": "1.0"})
        self.assertEqual(result, "pkg-1.0")

    def test_conditional_value_when_unset(self):
        result = _resolve_bash_expansion("pkg${VAR:+-}${VAR}", {})
        self.assertEqual(result, "pkg")

    def test_unresolved_var_becomes_empty(self):
        result = _resolve_bash_expansion("gcc-${UNKNOWN}", {})
        self.assertEqual(result, "gcc-")

    def test_kernel_devel_pattern_with_version(self):
        result = _resolve_bash_expansion(
            "kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}",
            {"KERNEL_VERSION": "5.14.0-427.el9"},
        )
        self.assertEqual(result, "kernel-devel-5.14.0-427.el9")

    def test_kernel_devel_pattern_without_version(self):
        result = _resolve_bash_expansion(
            "kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}",
            {},
        )
        self.assertEqual(result, "kernel-devel")

    def test_no_variables(self):
        result = _resolve_bash_expansion("plain-package", {})
        self.assertEqual(result, "plain-package")

    def test_multiple_vars_in_one_string(self):
        result = _resolve_bash_expansion(
            "${PREFIX}-${NAME}",
            {"PREFIX": "lib", "NAME": "foo"},
        )
        self.assertEqual(result, "lib-foo")


class TestCollectStageVars(unittest.TestCase):
    def test_arg_with_default(self):
        entries = [_make_entry("ARG", "GCC_VERSION=12")]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"GCC_VERSION": "12"})

    def test_arg_with_quoted_default(self):
        entries = [_make_entry("ARG", 'GCC_VERSION="12"')]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"GCC_VERSION": "12"})

    def test_arg_without_default(self):
        entries = [_make_entry("ARG", "KERNEL_VERSION")]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {})

    def test_arg_inherits_global(self):
        entries = [_make_entry("ARG", "KERNEL_VERSION")]
        result = _collect_stage_vars(entries, inherited_vars={"KERNEL_VERSION": "5.14"})
        self.assertEqual(result, {"KERNEL_VERSION": "5.14"})

    def test_env_with_equals(self):
        entries = [_make_entry("ENV", "LANG=en_US.UTF-8")]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"LANG": "en_US.UTF-8"})

    def test_env_with_space(self):
        entries = [_make_entry("ENV", "LANG en_US.UTF-8")]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"LANG": "en_US.UTF-8"})

    def test_env_references_arg(self):
        entries = [
            _make_entry("ARG", "GCC_VERSION=12"),
            _make_entry("ENV", "GCC_VERSION=${GCC_VERSION}"),
        ]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"GCC_VERSION": "12"})

    def test_mixed_arg_env(self):
        entries = [
            _make_entry("ARG", "BASE_VERSION=9"),
            _make_entry("ENV", "INSTALL_DIR=/opt"),
            _make_entry("RUN", "echo hello"),
        ]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"BASE_VERSION": "9", "INSTALL_DIR": "/opt"})


class TestExtractPackagesFromRunCommands(unittest.TestCase):
    def test_resolves_arg_in_packages(self):
        run_values = ["dnf install -y gcc-${GCC_VERSION} gcc-c++-${GCC_VERSION}"]
        common, arch = _extract_packages_from_run_commands(run_values, env_vars={"GCC_VERSION": "12"})
        self.assertEqual(common, ["gcc-12", "gcc-c++-12"])
        self.assertEqual(arch, {})

    def test_kernel_devel_conditional_with_version(self):
        run_values = [
            "dnf install -y kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}"
            " kernel-headers${KERNEL_VERSION:+-}${KERNEL_VERSION}"
        ]
        common, arch = _extract_packages_from_run_commands(run_values, env_vars={"KERNEL_VERSION": "5.14.0"})
        self.assertEqual(common, ["kernel-devel-5.14.0", "kernel-headers-5.14.0"])
        self.assertEqual(arch, {})

    def test_kernel_devel_conditional_without_version(self):
        run_values = [
            "dnf install -y kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}"
            " kernel-headers${KERNEL_VERSION:+-}${KERNEL_VERSION}"
        ]
        common, arch = _extract_packages_from_run_commands(run_values, env_vars={})
        self.assertEqual(common, ["kernel-devel", "kernel-headers"])
        self.assertEqual(arch, {})

    def test_unresolved_var_skipped(self):
        run_values = ["dnf install -y gcc-${UNKNOWN} make"]
        common, arch = _extract_packages_from_run_commands(run_values, env_vars={})
        self.assertEqual(common, ["make"])
        self.assertEqual(arch, {})

    def test_no_env_vars_backward_compatible(self):
        run_values = ['INSTALL_PKGS="wget tar" && dnf install -y ${INSTALL_PKGS} git']
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["git", "tar", "wget"])
        self.assertEqual(arch, {})

    def test_arch_conditional_x86_64(self):
        run_values = [
            "dnf install -y make && if [ $(arch) = x86_64 ]; then dnf -y install kernel-rt-devel kernel-rt-modules; fi"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["make"])
        self.assertEqual(arch, {"x86_64": ["kernel-rt-devel", "kernel-rt-modules"]})

    def test_fallback_install_after_or(self):
        run_values = ["dnf -y install gcc-${GCC_VERSION} gcc-c++-${GCC_VERSION} || dnf -y install gcc gcc-c++"]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["gcc", "gcc-c++"])
        self.assertEqual(arch, {})

    def test_if_not_yum_install(self):
        run_values = [
            "if ! yum install -y prometheus-promu; then curl -s -L https://example.com/promu.tar.gz | tar -xzvf -; fi"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["prometheus-promu"])
        self.assertEqual(arch, {})

    def test_subshell_arch_conditional_var(self):
        run_values = [
            'ARCH_DEP_PKGS=$(if [ "$(uname -m)" != "s390x" ]; then echo -n mstflint ; fi) && '
            "yum -y install pciutils hwdata kmod $ARCH_DEP_PKGS"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("mstflint", common)
        self.assertIn("pciutils", common)
        self.assertIn("hwdata", common)
        self.assertIn("kmod", common)
        self.assertEqual(arch, {})

    def test_if_else_var_with_nested_resolution(self):
        run_values = [
            'if yum install -y iotop >/dev/null 2>&1; then IOTOP_PKG="iotop"; '
            'else IOTOP_PKG="iotop-c"; fi && '
            'INSTALL_PKGS="bash-completion $IOTOP_PKG wget" && '
            "yum -y install $INSTALL_PKGS"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("iotop", common)
        self.assertIn("iotop-c", common)
        self.assertIn("bash-completion", common)
        self.assertIn("wget", common)
        self.assertNotIn(">/dev/null", common)
        self.assertNotIn("2>&1", common)
        self.assertEqual(arch, {})

    def test_arch_conditional_multiple_arches(self):
        run_values = [
            "if [ $(arch) = x86_64 ]; then dnf -y install kernel-rt-devel; fi && "
            "if [ $(arch) = aarch64 ]; then dnf -y install kernel-64k-devel; fi"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, [])
        self.assertEqual(arch, {"aarch64": ["kernel-64k-devel"], "x86_64": ["kernel-rt-devel"]})

    def test_hosttype_arch_conditional(self):
        run_values = [
            'PACKAGES="git gzip" && '
            "if [ $HOSTTYPE = x86_64 ]; then PACKAGES=\"$PACKAGES realtime-tests\"; fi && "
            "yum install -y $PACKAGES"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("git", common)
        self.assertIn("gzip", common)
        self.assertNotIn("realtime-tests", common)
        self.assertEqual(arch.get("x86_64"), ["realtime-tests"])

    def test_arch_conditional_double_equals(self):
        run_values = [
            'if [ $(arch) == "x86_64" ] || [ $(arch) == "aarch64" ]; then '
            'ARCH_DEP_PKGS="mokutil"; fi && '
            "dnf -y install openssl keyutils $ARCH_DEP_PKGS"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["keyutils", "openssl"])
        self.assertEqual(arch, {"aarch64": ["mokutil"], "x86_64": ["mokutil"]})

    def test_variable_reassignment_with_self_reference(self):
        """
        Variable reassignment like PKGS="new-pkg $PKGS" should resolve
        $PKGS at assignment time, preserving original packages.
        """
        run_values = [
            'INSTALL_PKGS="nmap-ncat procps-ng pciutils" && '
            'INSTALL_PKGS="tuned tuned-profiles-atomic $INSTALL_PKGS" && '
            "dnf install -y ${INSTALL_PKGS}"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("nmap-ncat", common)
        self.assertIn("procps-ng", common)
        self.assertIn("pciutils", common)
        self.assertIn("tuned", common)
        self.assertIn("tuned-profiles-atomic", common)
        self.assertEqual(arch, {})

    def test_unquoted_variable_assignment(self):
        """
        Unquoted variable assignments like VAR=value should be captured.
        """
        run_values = ["CONTAINER_RUNTIME=runc && yum install -y $CONTAINER_RUNTIME"]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["runc"])
        self.assertEqual(arch, {})

    def test_non_arch_conditional_preserves_both_branches(self):
        """
        Non-arch conditionals (e.g. PLATFORM_ID check) should include
        packages from all branches — the lockfile must be a superset.
        Reproduces the openshift-enterprise-builder runc/crun pattern.
        """
        run_values = [
            '. /etc/os-release && '
            "CONTAINER_RUNTIME=runc && "
            'if [ "${PLATFORM_ID}" = "platform:el10" ]; then '
            "CONTAINER_RUNTIME=crun; "
            "fi && "
            'INSTALL_PKGS="bind-utils ${CONTAINER_RUNTIME} wget" && '
            "yum install -y $INSTALL_PKGS"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("runc", common)
        self.assertIn("crun", common)
        self.assertIn("bind-utils", common)
        self.assertIn("wget", common)
        self.assertEqual(arch, {})

    def test_non_arch_conditional_else_branch(self):
        """
        Both if-body and else-body values should be included.
        """
        run_values = [
            "PKG=default && if [ -f /some/file ]; then PKG=branch_a; else PKG=branch_b; fi && yum install -y $PKG"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("default", common)
        self.assertIn("branch_a", common)
        self.assertIn("branch_b", common)
        self.assertEqual(arch, {})

    def test_arch_conditional_inside_unknown_conditional(self):
        """
        Arch conditionals nested inside unknown conditionals should still
        produce arch-specific packages correctly.
        """
        run_values = [
            'if [ "${PLATFORM_ID}" = "platform:el10" ]; then '
            "if [ $(arch) = x86_64 ]; then dnf -y install special-pkg; fi && "
            "dnf -y install cond-pkg; "
            "fi && "
            "dnf install -y common-pkg"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("common-pkg", common)
        self.assertIn("cond-pkg", common)
        self.assertIn("special-pkg", arch.get("x86_64", []))

    def test_version_constraints_stripped(self):
        """
        Version constraint operators (>=, <=, etc.) and version strings
        should be filtered out, keeping only the package name.
        """
        run_values = ["dnf install -y 'python3.12-setuptools >= 70.3.0' python3.12-pip"]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertIn("python3.12-setuptools", common)
        self.assertIn("python3.12-pip", common)
        self.assertNotIn(">=", common)
        self.assertNotIn("70.3.0", common)

    def test_uname_m_arch_conditional(self):
        """
        $(uname -m) should be recognized as an arch conditional,
        same as $(arch) and $HOSTTYPE.
        """
        run_values = [
            "if [ $(uname -m) = x86_64 ]; then dnf install -y syslinux-nonlinux; fi && dnf install -y common-pkg"
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["common-pkg"])
        self.assertEqual(arch, {"x86_64": ["syslinux-nonlinux"]})

    def test_uname_p_arch_conditional(self):
        """
        $(uname -p) should also be recognized as arch conditional.
        """
        run_values = ['if [ "$(uname -p)" == "s390x" ]; then dnf install -y s390utils; fi && dnf install -y base-pkg']
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["base-pkg"])
        self.assertEqual(arch, {"s390x": ["s390utils"]})

    def test_double_bracket_conditional_with_quoted_var_install(self):
        """
        [[ ]] conditionals setting variables used in quoted dnf install
        should include packages from all branches. Reproduces the
        ironic prepare-efi.sh pattern.
        """
        run_values = [
            'if [[ "$ARCH" == "x86_64" ]]; then '
            "GRUB_PKG=grub2-efi-x64; SHIM_PKG=shim-x64; "
            'elif [[ "$ARCH" == "aarch64" ]]; then '
            "GRUB_PKG=grub2-efi-aa64; SHIM_PKG=shim-aa64; "
            "fi && "
            'dnf install -y "$GRUB_PKG" "$SHIM_PKG"'
        ]
        common, arch = _extract_packages_from_run_commands(run_values)
        self.assertEqual(common, [])
        self.assertEqual(arch.get("x86_64"), ["grub2-efi-x64", "shim-x64"])
        self.assertEqual(arch.get("aarch64"), ["grub2-efi-aa64", "shim-aa64"])


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
            result, arch_result = _extract_packages_from_file_installs(run_values, copy_map, source_dir)
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
            result, _ = _extract_packages_from_file_installs(
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
            result, arch_result = _extract_packages_from_file_installs(run_values, {}, Path(tmpdir))
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
            result, _ = _extract_packages_from_file_installs(run_values, copy_map, source_dir)
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
            result, arch_result = _extract_packages_from_file_installs(run_values, copy_map, source_dir)
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
            result, _ = _extract_packages_from_file_installs(
                run_values,
                copy_map,
                source_dir,
                env_vars={"PKGS_LIST": "packages-list.ocp"},
            )
            self.assertEqual(result, ["dosfstools", "ipmitool"])

    def test_arch_suffix_in_filename(self):
        """
        File path with $(arch) should resolve per-architecture and return
        arch-specific packages. Reproduces ironic-agent pattern.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            x86_file = source_dir / "packages-list.ocp-x86_64"
            x86_file.write_text("biosdevname\n")
            copy_map = {"/tmp/packages-list.ocp": "packages-list.ocp"}

            run_values = ["grep -vE '^(#|$)' /tmp/${PKGS_LIST}-$(arch) | xargs -rtd'\\n' dnf install -y"]
            result, arch_result = _extract_packages_from_file_installs(
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
            result, arch_result = _extract_packages_from_file_installs(
                run_values,
                copy_map,
                source_dir,
            )
            self.assertEqual(result, [])
            self.assertEqual(arch_result, {})


class TestBuildCopyMap(unittest.TestCase):
    def test_copy_file_to_directory(self):
        """
        COPY hack/foo.sh /tmp should map /tmp/foo.sh to hack/foo.sh.
        """
        entries = [_make_entry("COPY", "hack/foo.sh /tmp")]
        result = _build_copy_map(entries)
        self.assertEqual(result["/tmp/foo.sh"], "hack/foo.sh")

    def test_copy_file_to_directory_with_trailing_slash(self):
        """
        COPY hack/foo.sh /tmp/ should map /tmp/foo.sh to hack/foo.sh.
        """
        entries = [_make_entry("COPY", "hack/foo.sh /tmp/")]
        result = _build_copy_map(entries)
        self.assertEqual(result["/tmp/foo.sh"], "hack/foo.sh")

    def test_copy_file_to_exact_path(self):
        """
        COPY hack/foo.sh /opt/renamed.sh should map /opt/renamed.sh to hack/foo.sh.
        """
        entries = [_make_entry("COPY", "hack/foo.sh /opt/renamed.sh")]
        result = _build_copy_map(entries)
        self.assertEqual(result["/opt/renamed.sh"], "hack/foo.sh")

    def test_skips_copy_from(self):
        """
        COPY --from=builder should be ignored (inter-stage copies).
        """
        entries = [_make_entry("COPY", "--from=builder /app/bin /usr/local/bin")]
        result = _build_copy_map(entries)
        self.assertEqual(result, {})

    def test_multiple_copies(self):
        entries = [
            _make_entry("COPY", "hack/install.sh /tmp"),
            _make_entry("COPY", "scripts/setup.sh /opt/"),
        ]
        result = _build_copy_map(entries)
        self.assertEqual(result["/tmp/install.sh"], "hack/install.sh")
        self.assertEqual(result["/opt/setup.sh"], "scripts/setup.sh")

    def test_copy_with_chown_flag(self):
        entries = [_make_entry("COPY", "--chown=root:root hack/foo.sh /tmp/")]
        result = _build_copy_map(entries)
        self.assertEqual(result["/tmp/foo.sh"], "hack/foo.sh")

    def test_non_copy_entries_ignored(self):
        entries = [
            _make_entry("RUN", "echo hello"),
            _make_entry("ENV", "FOO=bar"),
        ]
        result = _build_copy_map(entries)
        self.assertEqual(result, {})


class TestExtractPackagesFromScripts(unittest.TestCase):
    def test_script_via_bash_interpreter(self):
        """
        RUN /bin/bash /tmp/install.sh should extract packages from the script.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "hack" / "install.sh"
            script.parent.mkdir()
            script.write_text('PKGS="nmap-ncat procps-ng"\ndnf install -y ${PKGS}\n')

            copy_map = {"/tmp/install.sh": "hack/install.sh"}
            run_values = ["/bin/bash /tmp/install.sh"]
            pkgs, updates, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("nmap-ncat", pkgs)
            self.assertIn("procps-ng", pkgs)

    def test_script_via_usr_bin_bash(self):
        """
        RUN /usr/bin/bash /opt/setup.sh should also work.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "setup.sh"
            script.write_text("dnf install -y wget\n")

            copy_map = {"/opt/setup.sh": "setup.sh"}
            run_values = ["/usr/bin/bash /opt/setup.sh"]
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("wget", pkgs)

    def test_script_via_sh_interpreter(self):
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "install.sh"
            script.write_text("yum install -y curl\n")

            copy_map = {"/tmp/install.sh": "install.sh"}
            run_values = ["sh /tmp/install.sh"]
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("curl", pkgs)

    def test_script_after_cachi2_env_sourcing(self):
        """
        RUN . /cachi2/cachi2.env && /bin/bash /tmp/install.sh
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "hack" / "install.sh"
            script.parent.mkdir()
            script.write_text("dnf install -y pciutils rsync\n")

            copy_map = {"/tmp/install.sh": "hack/install.sh"}
            run_values = [". /cachi2/cachi2.env && /bin/bash /tmp/install.sh"]
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("pciutils", pkgs)
            self.assertIn("rsync", pkgs)

    def test_direct_script_invocation_still_works(self):
        """
        Direct script calls like /src/install-deps.sh should still work.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "install-deps.sh"
            script.write_text("dnf install -y git\n")

            run_values = ["/src/install-deps.sh"]
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir)
            self.assertIn("git", pkgs)

    def test_bare_script_name_resolved_via_copy_map(self):
        """
        Bare script names (no path prefix) should be resolved by matching
        the basename against COPY map entries. Covers the ironic pattern:
        COPY prepare-image.sh /bin/ && RUN prepare-image.sh
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            script = source_dir / "prepare-image.sh"
            script.write_text("dnf install -y httpd qemu-img\n")

            copy_map = {"/bin/prepare-image.sh": "prepare-image.sh"}
            run_values = ["set -euxo pipefail && prepare-image.sh && rm -f /bin/prepare-image.sh"]
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("httpd", pkgs)
            self.assertIn("qemu-img", pkgs)

    def test_script_not_in_copy_map_skipped(self):
        """
        Scripts at unknown container paths without copy_map entry are skipped.
        """
        with TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir)
            run_values = ["/bin/bash /opt/unknown.sh"]
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map={})
            self.assertEqual(pkgs, [])

    def test_cluster_node_tuning_pattern(self):
        """
        Real-world pattern from cluster-node-tuning-operator:
        COPY hack/dockerfile_install_support.sh /tmp
        RUN /bin/bash /tmp/dockerfile_install_support.sh

        The script reassigns INSTALL_PKGS with $INSTALL_PKGS self-reference
        in the RHEL branch, which requires resolving variables at assignment
        time to preserve the original value.
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
            pkgs, _, _ = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("nmap-ncat", pkgs)
            self.assertIn("procps-ng", pkgs)
            self.assertIn("pciutils", pkgs)
            self.assertIn("rsync", pkgs)
            self.assertIn("tuned", pkgs)
            self.assertIn("tuned-profiles-atomic", pkgs)
            self.assertIn("tuned-profiles-cpu-partitioning", pkgs)

    def test_arch_specific_packages_from_script(self):
        """
        Arch-conditional packages in scripts should be returned in the
        arch dict, not silently dropped. Reproduces the ironic
        prepare-efi.sh pattern.
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
            pkgs, _, arch_pkgs = _extract_packages_from_scripts(run_values, source_dir=source_dir, copy_map=copy_map)
            self.assertIn("grub2", pkgs)
            self.assertIn("dosfstools", pkgs)
            self.assertNotIn("grub2-efi-x64", pkgs)
            self.assertNotIn("shim-x64", pkgs)
            self.assertEqual(arch_pkgs.get("x86_64"), ["grub2-efi-x64", "shim-x64"])
            self.assertEqual(arch_pkgs.get("aarch64"), ["grub2-efi-aa64", "shim-aa64"])


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
            packages, updates, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(len(packages), 2)
            self.assertEqual(packages[0], ["gcc", "git", "nmstate-devel"])
            self.assertEqual(packages[1], ["postgresql-server", "skopeo"])
            self.assertEqual(updates, [False, False])

    def test_per_stage_single_stage(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN yum -y install nfs-utils jq\n"
            path = self._write_dockerfile(tmpdir, content)
            packages, updates, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(len(packages), 1)
            self.assertEqual(packages[0], ["jq", "nfs-utils"])
            self.assertEqual(updates, [False])

    def test_detect_update(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN yum update -y && yum clean all\n"
            path = self._write_dockerfile(tmpdir, content)
            packages, updates, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages, [[]])
            self.assertEqual(updates, [True])

    def test_detect_update_microdnf(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN microdnf update -y && microdnf clean all\n"
            path = self._write_dockerfile(tmpdir, content)
            _, updates, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(updates, [True])

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
            packages, updates, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages[0], ["gpgme", "hostname", "tar", "wget", "which"])
            self.assertEqual(updates, [False])

    def test_arg_env_variable_resolution(self):
        """
        ARG/ENV variables should be resolved in install commands.
        This mirrors the driver-toolkit Dockerfile pattern.
        """
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
            packages, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages[0], ["gcc-12", "gcc-c++-12", "kernel-devel", "make"])

    def test_global_arg_with_default_inherited_by_stage(self):
        with TemporaryDirectory() as tmpdir:
            content = "ARG BASE_VERSION=9\nFROM base\nARG BASE_VERSION\nRUN dnf install -y rhel-${BASE_VERSION}-pkg\n"
            path = self._write_dockerfile(tmpdir, content)
            packages, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages[0], ["rhel-9-pkg"])

    def test_env_variable_resolution(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nENV MY_PKG=httpd\nRUN dnf install -y ${MY_PKG}\n"
            path = self._write_dockerfile(tmpdir, content)
            packages, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages[0], ["httpd"])

    def test_no_update_no_install(self):
        with TemporaryDirectory() as tmpdir:
            content = "FROM base\nRUN echo hello\n"
            path = self._write_dockerfile(tmpdir, content)
            packages, updates, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages, [[]])
            self.assertEqual(updates, [False])

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
            packages, _, arch_packages, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages[0], ["make"])
            self.assertEqual(arch_packages[0], {"aarch64": ["kernel-64k-devel"], "x86_64": ["kernel-rt-devel"]})

    def test_arch_conditional_variable(self):
        with TemporaryDirectory() as tmpdir:
            content = (
                "FROM base\n"
                'RUN if [ $(arch) == "x86_64" ] || [ $(arch) == "aarch64" ]; then \\\n'
                '    ARCH_DEP_PKGS="mokutil"; fi && \\\n'
                "    dnf -y install openssl keyutils $ARCH_DEP_PKGS\n"
            )
            path = self._write_dockerfile(tmpdir, content)
            packages, _, arch_packages, *_ = analyze_dockerfile_stages(path)
            self.assertEqual(packages[0], ["keyutils", "openssl"])
            self.assertEqual(arch_packages[0], {"aarch64": ["mokutil"], "x86_64": ["mokutil"]})

    def test_copy_then_bash_script(self):
        """
        COPY hack/install.sh /tmp followed by RUN /bin/bash /tmp/install.sh
        should extract packages from the copied script via copy_map.
        """
        with TemporaryDirectory() as tmpdir:
            hack_dir = Path(tmpdir) / "hack"
            hack_dir.mkdir()
            (hack_dir / "install.sh").write_text('INSTALL_PKGS="nmap-ncat procps-ng"\ndnf install -y ${INSTALL_PKGS}\n')
            content = "FROM base\nCOPY hack/install.sh /tmp\nRUN /bin/bash /tmp/install.sh\n"
            path = self._write_dockerfile(tmpdir, content)
            packages, *_ = analyze_dockerfile_stages(path, source_dir=Path(tmpdir))
            self.assertIn("nmap-ncat", packages[0])
            self.assertIn("procps-ng", packages[0])

    def test_script_arch_packages_flow_to_stages(self):
        """
        Arch-conditional packages found inside a COPY'd script must appear
        in the stage's arch_packages, not in common. Reproduces the ironic
        prepare-efi.sh pattern end-to-end through analyze_dockerfile_stages.
        """
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
            packages, _, arch_packages, *_ = analyze_dockerfile_stages(path, source_dir=source_dir)
            self.assertIn("grub2", packages[0])
            self.assertIn("dosfstools", packages[0])
            self.assertNotIn("grub2-efi-x64", packages[0])
            self.assertNotIn("shim-x64", packages[0])
            self.assertEqual(arch_packages[0].get("x86_64"), ["grub2-efi-x64", "shim-x64"])
            self.assertEqual(arch_packages[0].get("aarch64"), ["grub2-efi-aa64", "shim-aa64"])
            self.assertEqual(packages[1], ["httpd"])

    def test_ironic_agent_pipe_and_arch_file_pattern(self):
        """
        ironic-agent prepare-image.sh uses grep | xargs dnf install for
        common packages and ${PKGS_LIST}-$(arch) for arch-specific ones.
        Both patterns should be extracted end-to-end.
        """
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
            packages, _, arch_packages, *_ = analyze_dockerfile_stages(path, source_dir=source_dir)
            self.assertIn("ipmitool", packages[0])
            self.assertIn("qemu-img", packages[0])
            self.assertNotIn("biosdevname", packages[0])
            self.assertEqual(arch_packages[0].get("x86_64"), ["biosdevname"])
            self.assertNotIn("aarch64", arch_packages[0])


class TestBuildRpmsInYaml(unittest.TestCase):
    def test_basic_structure(self):
        repos = [
            {
                "name": "rhel-9-baseos-rpms",
                "baseurl": "https://example.com/baseos/$basearch/os/",
            },
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64", "ppc64le"],
            packages=["nfs-utils", "jq"],
        )
        self.assertEqual(result["arches"], ["x86_64", "ppc64le"])
        self.assertEqual(len(result["contentOrigin"]["repos"]), 1)
        self.assertEqual(result["contentOrigin"]["repos"][0]["repoid"], "rhel-9-baseos-rpms")
        self.assertEqual(result["packages"], ["nfs-utils", "jq"])

    def test_arch_specific_packages(self):
        repos = [
            {
                "name": "rhel-9-baseos-rpms",
                "baseurl": "https://example.com/baseos/$basearch/os/",
            },
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64", "ppc64le"],
            packages=["nfs-utils"],
            arch_specific_packages={"ppc64le": ["librtas"]},
        )
        self.assertIn("nfs-utils", result["packages"])
        arch_entries = [p for p in result["packages"] if isinstance(p, dict)]
        self.assertEqual(len(arch_entries), 1)
        self.assertEqual(arch_entries[0]["name"], "librtas")
        self.assertEqual(arch_entries[0]["arches"]["only"], "ppc64le")

    def test_multiple_repos(self):
        repos = [
            {
                "name": "rhel-9-baseos-rpms",
                "baseurl": "https://example.com/baseos/$basearch/os/",
            },
            {
                "name": "rhel-9-appstream-rpms",
                "baseurl": "https://example.com/appstream/$basearch/os/",
            },
        ]
        result = build_rpms_in_yaml(
            repos=repos,
            arches=["x86_64"],
            packages=["httpd"],
        )
        self.assertEqual(len(result["contentOrigin"]["repos"]), 2)
        repoids = [r["repoid"] for r in result["contentOrigin"]["repos"]]
        self.assertEqual(repoids, ["rhel-9-baseos-rpms", "rhel-9-appstream-rpms"])


class TestRpmLockfilePrototypeGenerator(unittest.TestCase):
    def _make_mock_repo(self, name: str, baseurl: str, content_set_name: str | None = None) -> MagicMock:
        repo = MagicMock()
        repo.name = name
        repo.baseurl.return_value = baseurl
        repo.content_set.return_value = content_set_name
        return repo

    def _make_mock_repos(self) -> MagicMock:
        repos = MagicMock()
        baseos = self._make_mock_repo(
            "rhel-9-baseos-rpms",
            "https://example.com/baseos/x86_64/os/",
            content_set_name="rhel-9-for-x86_64-baseos-rpms",
        )
        appstream = self._make_mock_repo(
            "rhel-9-appstream-rpms",
            "https://example.com/appstream/x86_64/os/",
            content_set_name="rhel-9-for-x86_64-appstream-rpms",
        )
        repo_map = {
            "rhel-9-baseos-rpms": baseos,
            "rhel-9-appstream-rpms": appstream,
        }
        repos.__getitem__ = lambda self_repos, key: repo_map[key]
        return repos

    def _make_mock_image_meta(self) -> MagicMock:
        meta = MagicMock()
        meta.distgit_key = "csi-driver-nfs"
        meta.get_arches.return_value = ["x86_64", "ppc64le"]
        meta.get_enabled_repos.return_value = {"rhel-9-baseos-rpms", "rhel-9-appstream-rpms"}
        meta.is_lockfile_generation_enabled.return_value = True

        lockfile_config = MagicMock()
        lockfile_config.get.return_value = ["keyutils"]
        meta.config.konflux.cachi2.lockfile = lockfile_config

        return meta

    async def _mock_cmd_gather_async(self, cmd, **kwargs):
        """
        Mock for cmd_gather_async that writes a fake lockfile to whatever
        --outfile path is in the command.
        """
        outfile_idx = cmd.index("--outfile") + 1
        outfile_path = Path(cmd[outfile_idx])
        lockfile = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [
                {
                    "arch": "x86_64",
                    "packages": [
                        {
                            "url": "https://example.com/nfs-utils-2.5.4-1.el9.x86_64.rpm",
                            "repoid": "rhel-9-baseos-rpms",
                            "name": "nfs-utils",
                            "evr": "2.5.4-1.el9",
                        }
                    ],
                    "module_metadata": [],
                }
            ],
        }
        outfile_path.parent.mkdir(parents=True, exist_ok=True)
        with open(outfile_path, "w") as f:
            yaml.safe_dump(lockfile, f)
        return (0, "", "")

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_generate_lockfile_calls_prototype(self, mock_gather):
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils jq\n")
            mock_gather.side_effect = self._mock_cmd_gather_async

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        mock_gather.assert_called_once()
        call_args = mock_gather.call_args[0][0]
        self.assertEqual(call_args[0], "rpm-lockfile-prototype")

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_generate_lockfile_cleans_up_in_file(self, mock_gather):
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils\n")
            mock_gather.side_effect = self._mock_cmd_gather_async

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            in_files = list(dest_dir.glob("*.in.yaml"))
            self.assertEqual(in_files, [])

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_generate_lockfile_fails_on_nonzero_exit(self, mock_gather):
        mock_gather.return_value = (1, "", "Error: missing package foo")

        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils\n")

            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(generator.generate_lockfile(meta, dest_dir))
            self.assertIn("rpm-lockfile-prototype failed", str(ctx.exception))

    def test_generate_lockfile_skips_when_disabled(self):
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()
        meta.is_lockfile_generation_enabled.return_value = False

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            self.assertFalse((dest_dir / "rpms.lock.yaml").exists())

    def test_generate_lockfile_writes_empty_when_no_packages(self):
        """
        When the Dockerfile has no install or update commands, an empty
        lockfile should be written so cachi2 finds the expected file.
        """
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = []

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            dockerfile = dest_dir / "Dockerfile"
            dockerfile.write_text("FROM base\nRUN echo hello\n")
            asyncio.run(generator.generate_lockfile(meta, dest_dir))
            lockfile_path = dest_dir / "rpms.lock.yaml"
            self.assertTrue(lockfile_path.exists())
            with open(lockfile_path) as f:
                data = yaml.safe_load(f)
            self.assertEqual(data["lockfileVersion"], 1)
            self.assertEqual(data["arches"], [])

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_stage_alias_uses_bare_mode(self, mock_gather):
        """
        When downstream_parents contains a stage alias (no "/"),
        the generator should use --bare instead of --image.
        """
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = [
            "quay.io/test/builder@sha256:abc123",
            "build",
        ]

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM quay.io/test/builder AS build\n"
                "RUN dnf install -y gcc\n"
                "\n"
                "FROM build\n"
                "RUN dnf install -y nfs-utils\n"
            )
            mock_gather.side_effect = self._mock_cmd_gather_async

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        self.assertEqual(mock_gather.call_count, 2)
        stage0_cmd = mock_gather.call_args_list[0][0][0]
        stage1_cmd = mock_gather.call_args_list[1][0][0]
        self.assertIn("--image", stage0_cmd)
        self.assertIn("--bare", stage1_cmd)
        self.assertNotIn("--image", stage1_cmd)

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_update_only_queries_base_image(self, mock_gather):
        """
        For update-only stages (yum update, no installs), the generator should
        query the base image for installed packages and pass them to the tool.
        """
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        installed_packages = ["bash", "coreutils", "glibc"]

        async def mock_gather_side_effect(cmd, **kwargs):
            if cmd[0] == "podman":
                return (0, "\n".join(installed_packages) + "\n", "")
            return await self._mock_cmd_gather_async(cmd, **kwargs)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum update -y && yum clean all\n")
            mock_gather.side_effect = mock_gather_side_effect

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        podman_calls = [c for c in mock_gather.call_args_list if c[0][0][0] == "podman"]
        self.assertEqual(len(podman_calls), 1)
        prototype_calls = [c for c in mock_gather.call_args_list if c[0][0][0] == "rpm-lockfile-prototype"]
        self.assertEqual(len(prototype_calls), 1)
        # Update-only stages must use --bare, not --image, so the tool resolves
        # the latest available versions from repos rather than seeing all packages
        # as already installed and returning an empty lockfile
        self.assertIn("--bare", prototype_calls[0][0][0])
        self.assertNotIn("--image", prototype_calls[0][0][0])

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_update_only_no_image_skips(self, mock_gather):
        """
        For update-only stages where no base image is available (bare mode),
        the stage should be skipped with a warning.
        """
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = None

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = []

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum update -y && yum clean all\n")

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        mock_gather.assert_not_called()
        self.assertFalse((dest_dir / "rpms.lock.yaml").exists())

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_resolve_cat_packages_from_base_image(self, mock_gather):
        """
        When a Dockerfile uses $(cat /more-pkgs) in an install command,
        the generator should read the file from the base image and add
        those packages to the lockfile.
        """
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()
        meta.config.konflux.cachi2.lockfile.get.return_value = []

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        captured_in_yaml = {}

        async def mock_gather_side_effect(cmd, **kwargs):
            if cmd[0] == "podman" and "cat" in cmd:
                return (0, '"openvswitch3.5-devel" "openvswitch3.5-ipsec" "ovn25.09-vtep"', "")
            if cmd[0] == "rpm-lockfile-prototype":
                in_file = cmd[-1]
                with open(in_file) as f:
                    captured_in_yaml.update(yaml.safe_load(f))
            return await self._mock_cmd_gather_async(cmd, **kwargs)

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text(
                "FROM base\nRUN dnf install -y openssl && \\\n    eval \"dnf install -y $(cat /more-pkgs)\"\n"
            )
            mock_gather.side_effect = mock_gather_side_effect
            asyncio.run(generator.generate_lockfile(meta, dest_dir))

        podman_cat_calls = [c for c in mock_gather.call_args_list if c[0][0][0] == "podman" and "cat" in c[0][0]]
        self.assertEqual(len(podman_cat_calls), 1)
        self.assertIn("/more-pkgs", podman_cat_calls[0][0][0])

        pkg_names = [p if isinstance(p, str) else p["name"] for p in captured_in_yaml["packages"]]
        self.assertIn("openssl", pkg_names)
        self.assertIn("openvswitch3.5-devel", pkg_names)
        self.assertIn("openvswitch3.5-ipsec", pkg_names)
        self.assertIn("ovn25.09-vtep", pkg_names)

    @patch("doozerlib.lockfile_prototype.cmd_gather_async")
    def test_retry_on_missing_packages(self, mock_gather):
        """
        If rpm-lockfile-prototype fails with 'missing packages: X', the generator
        should remove those packages and retry.
        """
        repos = self._make_mock_repos()
        meta = self._make_mock_image_meta()

        generator = RpmLockfilePrototypeGenerator(
            repos=repos,
            runtime=MagicMock(),
        )
        generator.downstream_parents = ["quay.io/test/base@sha256:abc123"]

        call_count = 0

        async def mock_gather_side_effect(cmd, **kwargs):
            nonlocal call_count
            if cmd[0] == "rpm-lockfile-prototype":
                call_count += 1
                if call_count == 1:
                    return (1, "", "missing packages: dmidecode")
                return await self._mock_cmd_gather_async(cmd, **kwargs)
            return (0, "", "")

        with TemporaryDirectory() as tmpdir:
            dest_dir = Path(tmpdir)
            (dest_dir / "Dockerfile").write_text("FROM base\nRUN yum -y install nfs-utils dmidecode\n")
            mock_gather.side_effect = mock_gather_side_effect

            asyncio.run(generator.generate_lockfile(meta, dest_dir))

            prototype_calls = [c for c in mock_gather.call_args_list if c[0][0][0] == "rpm-lockfile-prototype"]
            self.assertEqual(len(prototype_calls), 2)
            self.assertTrue((dest_dir / "rpms.lock.yaml").exists())

    def test_parse_missing_packages(self):
        """
        _parse_missing_packages should extract package names from error output.
        """
        error = (
            "ERROR:dnf:No match for argument: dmidecode\nERROR:root:Problems in request:\nmissing packages: dmidecode\n"
        )
        missing = RpmLockfilePrototypeGenerator._parse_missing_packages(error)
        self.assertEqual(missing, {"dmidecode"})

    def test_parse_missing_packages_multiple(self):
        """
        _parse_missing_packages should handle comma-separated package names.
        """
        error = "missing packages: dmidecode, microcode_ctl\n"
        missing = RpmLockfilePrototypeGenerator._parse_missing_packages(error)
        self.assertEqual(missing, {"dmidecode", "microcode_ctl"})

    def test_build_repo_list_keeps_literal_url_for_single_arch_repo(self):
        """
        When all arches return the same URL (e.g. rhel-9-rt-rpms configured
        with x86_64 for all arches), _build_repo_list should keep the literal
        URL so rpm-lockfile-prototype can download metadata for every arch.
        """
        rt = MagicMock()
        rt.name = "rhel-9-rt-rpms"
        rt.baseurl.return_value = "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/"
        rt.content_set.return_value = "rhel-9-for-x86_64-rt-rpms"

        repo_map = {"rhel-9-rt-rpms": rt}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, runtime=MagicMock())
        result = generator._build_repo_list(
            enabled_repos={"rhel-9-rt-rpms"},
            arches=["x86_64", "aarch64"],
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "rhel-9-for-$basearch-rt-rpms")
        self.assertEqual(result[0]["baseurl"], "https://example.com/e4s/rhel9/9.8/x86_64/rt/os/")

    def test_templatize_baseurl_replaces_known_arch(self):
        """
        _templatize_baseurl should replace any known arch string found
        in the URL, not just the requesting arch.
        """
        url = "https://rhsm-pulp.corp.stage.redhat.com/content/e4s/rhel9/9.8/x86_64/rt/os/"
        result = RpmLockfilePrototypeGenerator._templatize_baseurl(url)
        self.assertEqual(
            result,
            "https://rhsm-pulp.corp.stage.redhat.com/content/e4s/rhel9/9.8/$basearch/rt/os/",
        )

    def test_templatize_baseurl_no_arch_in_url(self):
        """
        URLs without any known arch string should be returned unchanged.
        """
        url = "https://example.com/content/repo/os/"
        result = RpmLockfilePrototypeGenerator._templatize_baseurl(url)
        self.assertEqual(result, url)

    def test_templatize_baseurl_already_has_basearch(self):
        """
        URLs with $basearch should be returned unchanged.
        """
        url = "https://example.com/baseos/$basearch/os/"
        result = RpmLockfilePrototypeGenerator._templatize_baseurl(url)
        self.assertEqual(result, url)

    def test_build_repo_list_templatizes_multi_arch_url(self):
        """
        Repos with different URLs per arch should be templatized with
        $basearch so rpm-lockfile-prototype resolves each arch correctly.
        """
        baseos = MagicMock()
        baseos.name = "rhel-9-baseos-rpms"
        baseos.baseurl.side_effect = lambda repotype="unsigned", arch="x86_64": (
            f"https://example.com/baseos/{arch}/os/"
        )
        baseos.content_set.return_value = "rhel-9-for-x86_64-baseos-rpms"

        repo_map = {"rhel-9-baseos-rpms": baseos}
        repos = MagicMock()
        repos.__getitem__ = lambda self_repos, key: repo_map[key]

        generator = RpmLockfilePrototypeGenerator(repos=repos, runtime=MagicMock())
        result = generator._build_repo_list(
            enabled_repos={"rhel-9-baseos-rpms"},
            arches=["x86_64", "aarch64"],
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "rhel-9-for-$basearch-baseos-rpms")
        self.assertEqual(result[0]["baseurl"], "https://example.com/baseos/$basearch/os/")

    def test_merge_lockfiles_dedupes_by_url(self):
        """
        Merge should deduplicate by url, not by name, so that
        different versions of the same package are preserved.
        """
        lockfile_a = {
            "arches": [
                {
                    "arch": "x86_64",
                    "packages": [
                        {
                            "url": "https://example.com/foo-1.0-1.el9.x86_64.rpm",
                            "repoid": "baseos",
                            "name": "foo",
                            "evr": "1.0-1.el9",
                        },
                    ],
                    "module_metadata": [],
                }
            ],
        }
        lockfile_b = {
            "arches": [
                {
                    "arch": "x86_64",
                    "packages": [
                        {
                            "url": "https://example.com/foo-2.0-1.el9.x86_64.rpm",
                            "repoid": "baseos",
                            "name": "foo",
                            "evr": "2.0-1.el9",
                        },
                        {
                            "url": "https://example.com/bar-1.0-1.el9.x86_64.rpm",
                            "repoid": "baseos",
                            "name": "bar",
                            "evr": "1.0-1.el9",
                        },
                    ],
                    "module_metadata": [],
                }
            ],
        }
        merged = RpmLockfilePrototypeGenerator._merge_lockfiles([lockfile_a, lockfile_b])
        x86_packages = merged["arches"][0]["packages"]
        urls = [p["url"] for p in x86_packages]
        self.assertEqual(len(urls), 3)
        self.assertIn("https://example.com/foo-1.0-1.el9.x86_64.rpm", urls)
        self.assertIn("https://example.com/foo-2.0-1.el9.x86_64.rpm", urls)
        self.assertIn("https://example.com/bar-1.0-1.el9.x86_64.rpm", urls)

    def test_merge_lockfiles_preserves_module_metadata(self):
        """
        Merge should collect module_metadata from all stage lockfiles.
        """
        lockfile_a = {
            "arches": [
                {
                    "arch": "x86_64",
                    "packages": [],
                    "module_metadata": [{"name": "mod-a", "stream": "1.0"}],
                }
            ],
        }
        lockfile_b = {
            "arches": [
                {
                    "arch": "x86_64",
                    "packages": [],
                    "module_metadata": [{"name": "mod-b", "stream": "2.0"}],
                }
            ],
        }
        merged = RpmLockfilePrototypeGenerator._merge_lockfiles([lockfile_a, lockfile_b])
        modules = merged["arches"][0]["module_metadata"]
        module_names = [m["name"] for m in modules]
        self.assertEqual(len(modules), 2)
        self.assertIn("mod-a", module_names)
        self.assertIn("mod-b", module_names)
