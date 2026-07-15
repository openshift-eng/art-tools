"""
Tests for doozerlib.lockfile_prototype.shell_parser.
"""

import unittest

import pytest
from doozerlib.lockfile_prototype.shell_parser import (
    ARCH_VALUE_RE,
    analyze_run_commands,
    extract_packages_from_run_commands,
    resolve_bash_expansion,
)


class TestResolveBashExpansion(unittest.TestCase):
    def test_simple_braced_var(self):
        result = resolve_bash_expansion("gcc-${GCC_VERSION}", {"GCC_VERSION": "12"})
        self.assertEqual(result, "gcc-12")

    def test_simple_unbraced_var(self):
        result = resolve_bash_expansion("$VAR", {"VAR": "hello"})
        self.assertEqual(result, "hello")

    def test_default_value_when_unset(self):
        result = resolve_bash_expansion("${VAR:-fallback}", {})
        self.assertEqual(result, "fallback")

    def test_default_value_when_set(self):
        result = resolve_bash_expansion("${VAR:-fallback}", {"VAR": "actual"})
        self.assertEqual(result, "actual")

    def test_conditional_value_when_set(self):
        result = resolve_bash_expansion("pkg${VAR:+-}${VAR}", {"VAR": "1.0"})
        self.assertEqual(result, "pkg-1.0")

    def test_conditional_value_when_unset(self):
        result = resolve_bash_expansion("pkg${VAR:+-}${VAR}", {})
        self.assertEqual(result, "pkg")

    def test_unresolved_var_becomes_empty(self):
        result = resolve_bash_expansion("gcc-${UNKNOWN}", {})
        self.assertEqual(result, "gcc-")

    def test_kernel_devel_pattern_with_version(self):
        result = resolve_bash_expansion(
            "kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}",
            {"KERNEL_VERSION": "5.14.0-427.el9"},
        )
        self.assertEqual(result, "kernel-devel-5.14.0-427.el9")

    def test_kernel_devel_pattern_without_version(self):
        result = resolve_bash_expansion(
            "kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}",
            {},
        )
        self.assertEqual(result, "kernel-devel")

    def test_no_variables(self):
        result = resolve_bash_expansion("plain-package", {})
        self.assertEqual(result, "plain-package")

    def test_multiple_vars_in_one_string(self):
        result = resolve_bash_expansion(
            "${PREFIX}-${NAME}",
            {"PREFIX": "lib", "NAME": "foo"},
        )
        self.assertEqual(result, "lib-foo")


class TestExtractPackagesFromRunCommands(unittest.TestCase):
    def test_resolves_arg_in_packages(self):
        run_values = ["dnf install -y gcc-${GCC_VERSION} gcc-c++-${GCC_VERSION}"]
        common, arch = extract_packages_from_run_commands(run_values, env_vars={"GCC_VERSION": "12"})
        self.assertEqual(common, ["gcc-12", "gcc-c++-12"])
        self.assertEqual(arch, {})

    def test_kernel_devel_conditional_with_version(self):
        run_values = [
            "dnf install -y kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}"
            " kernel-headers${KERNEL_VERSION:+-}${KERNEL_VERSION}"
        ]
        common, arch = extract_packages_from_run_commands(run_values, env_vars={"KERNEL_VERSION": "5.14.0"})
        self.assertEqual(common, ["kernel-devel-5.14.0", "kernel-headers-5.14.0"])
        self.assertEqual(arch, {})

    def test_kernel_devel_conditional_without_version(self):
        run_values = [
            "dnf install -y kernel-devel${KERNEL_VERSION:+-}${KERNEL_VERSION}"
            " kernel-headers${KERNEL_VERSION:+-}${KERNEL_VERSION}"
        ]
        common, arch = extract_packages_from_run_commands(run_values, env_vars={})
        self.assertEqual(common, ["kernel-devel", "kernel-headers"])
        self.assertEqual(arch, {})

    def test_unresolved_var_skipped(self):
        run_values = ["dnf install -y gcc-${UNKNOWN} make"]
        common, arch = extract_packages_from_run_commands(run_values, env_vars={})
        self.assertEqual(common, ["make"])
        self.assertEqual(arch, {})

    def test_no_env_vars_backward_compatible(self):
        run_values = ['INSTALL_PKGS="wget tar" && dnf install -y ${INSTALL_PKGS} git']
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["git", "tar", "wget"])
        self.assertEqual(arch, {})

    def test_arch_conditional_x86_64(self):
        run_values = [
            "dnf install -y make && if [ $(arch) = x86_64 ]; then dnf -y install kernel-rt-devel kernel-rt-modules; fi"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["make"])
        self.assertEqual(arch, {"x86_64": ["kernel-rt-devel", "kernel-rt-modules"]})

    def test_reinstall_packages_parsed(self):
        run_values = ["dnf -y reinstall tzdata && dnf clean all"]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("tzdata", common)
        self.assertEqual(arch, {})

    def test_reinstall_multiple_packages_parsed(self):
        run_values = ["microdnf -y reinstall tzdata glibc && microdnf clean all"]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("tzdata", common)
        self.assertIn("glibc", common)
        self.assertEqual(arch, {})

    def test_reinstall_mixed_with_install(self):
        run_values = ["microdnf -y install openssl && microdnf -y reinstall tzdata && microdnf clean all"]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("openssl", common)
        self.assertIn("tzdata", common)
        self.assertEqual(arch, {})

    def test_fallback_install_after_or(self):
        run_values = ["dnf -y install gcc-${GCC_VERSION} gcc-c++-${GCC_VERSION} || dnf -y install gcc gcc-c++"]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["gcc", "gcc-c++"])
        self.assertEqual(arch, {})

    def test_if_not_yum_install(self):
        run_values = [
            "if ! yum install -y prometheus-promu; then curl -s -L https://example.com/promu.tar.gz | tar -xzvf -; fi"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["prometheus-promu"])
        self.assertEqual(arch, {})

    def test_subshell_arch_conditional_var(self):
        run_values = [
            'ARCH_DEP_PKGS=$(if [ "$(uname -m)" != "s390x" ]; then echo -n mstflint ; fi) && '
            "yum -y install pciutils hwdata kmod $ARCH_DEP_PKGS"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertNotIn("mstflint", common)
        self.assertIn("pciutils", common)
        self.assertIn("hwdata", common)
        self.assertIn("kmod", common)
        for a in ("x86_64", "aarch64", "ppc64le"):
            self.assertIn("mstflint", arch.get(a, []))
        self.assertNotIn("s390x", arch)

    def test_subshell_arch_conditional_var_eq(self):
        run_values = [
            'SPECIAL=$(if [ "$(uname -m)" == "x86_64" ]; then echo -n intel-pkg ; fi) && '
            "yum -y install base-pkg $SPECIAL"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertNotIn("intel-pkg", common)
        self.assertIn("base-pkg", common)
        self.assertEqual(arch, {"x86_64": ["intel-pkg"]})

    def test_subshell_no_arch_condition(self):
        run_values = ['EXTRA=$(echo extra-pkg) && yum -y install base-pkg $EXTRA']
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("extra-pkg", common)
        self.assertIn("base-pkg", common)
        self.assertEqual(arch, {})

    def test_subshell_arch_conditional_var_go_arch(self):
        run_values = [
            'SPECIAL=$(if [ "$(go env GOARCH)" == "amd64" ]; then echo -n x86-only ; fi) && '
            "yum -y install common-pkg $SPECIAL"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertNotIn("x86-only", common)
        self.assertIn("common-pkg", common)
        self.assertEqual(arch, {"x86_64": ["x86-only"]})

    def test_if_else_var_with_nested_resolution(self):
        run_values = [
            'if yum install -y iotop >/dev/null 2>&1; then IOTOP_PKG="iotop"; '
            'else IOTOP_PKG="iotop-c"; fi && '
            'INSTALL_PKGS="bash-completion $IOTOP_PKG wget" && '
            "yum -y install $INSTALL_PKGS"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, [])
        self.assertEqual(arch, {"aarch64": ["kernel-64k-devel"], "x86_64": ["kernel-rt-devel"]})

    def test_hosttype_arch_conditional(self):
        run_values = [
            'PACKAGES="git gzip" && '
            "if [ $HOSTTYPE = x86_64 ]; then PACKAGES=\"$PACKAGES realtime-tests\"; fi && "
            "yum install -y $PACKAGES"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["runc"])
        self.assertEqual(arch, {})

    def test_non_arch_conditional_preserves_both_branches(self):
        """
        Non-arch conditionals (e.g. PLATFORM_ID check) should include
        packages from all branches — the lockfile must be a superset.
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
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("common-pkg", common)
        self.assertIn("cond-pkg", common)
        self.assertIn("special-pkg", arch.get("x86_64", []))

    def test_version_constraints_stripped(self):
        """
        Version constraint operators (>=, <=, etc.) and version strings
        should be filtered out, keeping only the package name.
        """
        run_values = ["dnf install -y 'python3.12-setuptools >= 70.3.0' python3.12-pip"]
        common, arch = extract_packages_from_run_commands(run_values)
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
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["common-pkg"])
        self.assertEqual(arch, {"x86_64": ["syslinux-nonlinux"]})

    def test_uname_p_arch_conditional(self):
        """
        $(uname -p) should also be recognized as arch conditional.
        """
        run_values = ['if [ "$(uname -p)" == "s390x" ]; then dnf install -y s390utils; fi && dnf install -y base-pkg']
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, ["base-pkg"])
        self.assertEqual(arch, {"s390x": ["s390utils"]})

    def test_file_path_package_specs_are_included(self):
        """
        File path specs like /usr/lib/udev/scsi_id and /usr/bin/xxd are
        valid dnf install targets (resolved via file provides). They must
        be included in the packages list so the lockfile contains the
        packages that provide those files.
        """
        run_values = [
            "yum install --setopt=tsflags=nodocs -y "
            "e2fsprogs xfsprogs util-linux nvme-cli "
            "/usr/lib/udev/scsi_id /usr/bin/xxd"
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("e2fsprogs", common)
        self.assertIn("xfsprogs", common)
        self.assertIn("util-linux", common)
        self.assertIn("nvme-cli", common)
        self.assertIn("/usr/lib/udev/scsi_id", common)
        self.assertIn("/usr/bin/xxd", common)

    def test_two_consecutive_arch_conditional_appends_to_same_variable(self):
        """
        Two consecutive arch-conditional blocks both appending to the same
        variable must not lose the first assignment. The second block used to
        overwrite arch_shell_vars, silently dropping packages from the first
        (e.g. python3-cinderclient lost when realtime-tests was appended).
        """
        run_values = [
            'PACKAGES="git gzip util-linux" && '
            'if [ $HOSTTYPE = x86_64 ]; then PACKAGES="$PACKAGES python3-cinderclient"; fi && '
            'if [ $HOSTTYPE = x86_64 ]; then PACKAGES="$PACKAGES realtime-tests"; fi && '
            'yum install --setopt=tsflags=nodocs -y $PACKAGES'
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(sorted(common), ["git", "gzip", "util-linux"])
        self.assertIn("python3-cinderclient", arch.get("x86_64", []))
        self.assertIn("realtime-tests", arch.get("x86_64", []))

    def test_glob_package_pattern_with_resolved_var(self):
        """
        Glob patterns like golang-*$VERSION* should be included after
        variable resolution (e.g. golang-*1.26*). DNF supports globs.
        """
        run_values = ['dnf install -y "golang-*$VERSION*"']
        common, arch = extract_packages_from_run_commands(run_values, env_vars={"VERSION": "1.26"})
        self.assertIn("golang-*1.26*", common)
        self.assertEqual(arch, {})

    def test_glob_package_pattern_without_var(self):
        """
        Bare glob patterns like python3* should also be included.
        """
        run_values = ["dnf install -y python3*"]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertIn("python3*", common)
        self.assertEqual(arch, {})

    def test_double_bracket_conditional_with_quoted_var_install(self):
        """
        [[ ]] conditionals setting variables used in quoted dnf install
        should include packages from all branches.
        """
        run_values = [
            'if [[ "$ARCH" == "x86_64" ]]; then '
            "GRUB_PKG=grub2-efi-x64; SHIM_PKG=shim-x64; "
            'elif [[ "$ARCH" == "aarch64" ]]; then '
            "GRUB_PKG=grub2-efi-aa64; SHIM_PKG=shim-aa64; "
            "fi && "
            'dnf install -y "$GRUB_PKG" "$SHIM_PKG"'
        ]
        common, arch = extract_packages_from_run_commands(run_values)
        self.assertEqual(common, [])
        self.assertEqual(arch.get("x86_64"), ["grub2-efi-x64", "shim-x64"])
        self.assertEqual(arch.get("aarch64"), ["grub2-efi-aa64", "shim-aa64"])


@pytest.mark.parametrize(
    "text, expected",
    [
        ("[ $(arch) = x86_64 ]", "x86_64"),
        ("[ $HOSTTYPE = aarch64 ]", "aarch64"),
        ("[ ${HOSTTYPE} == ppc64le ]", "ppc64le"),
        ("[ $(uname -m) = s390x ]", "s390x"),
        ("[ $(uname -p) == x86_64 ]", "x86_64"),
        ("[ $ARCH = aarch64 ]", "aarch64"),
        ('[ ${ARCH} == x86_64 ]', "x86_64"),
        ('[ $(arch) == "x86_64" ]', "x86_64"),
        ("[ $(go env GOARCH) = amd64 ]", "amd64"),
        ("[ $GOARCH = amd64 ]", "amd64"),
        ('[ ${GOARCH} == arm64 ]', "arm64"),
    ],
)
def test_arch_value_regex_matches(text, expected):
    """
    ARCH_VALUE_RE should extract arch name from all keyword forms.
    """
    m = ARCH_VALUE_RE.search(text)
    assert m is not None, f"No match for: {text}"
    assert m.group(1) == expected


def test_arch_value_regex_no_match():
    """
    ARCH_VALUE_RE should not match plain text.
    """
    assert ARCH_VALUE_RE.search("echo hello") is None


class TestListArchConditional(unittest.TestCase):
    """
    Tests for ``[ test ] || cmd`` and ``[ test ] && cmd`` arch-conditional
    patterns in list nodes.
    """

    def test_neq_or_extracts_arch_packages(self):
        """
        ``[ $(arch) != x86_64 ] || dnf install -y pkg`` installs only on x86_64.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[ $(arch) != x86_64 ] || dnf install -y special-pkg'])
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"x86_64": ["special-pkg"]})

    def test_eq_and_extracts_arch_packages(self):
        """
        ``[ $(arch) = x86_64 ] && dnf install -y pkg`` installs only on x86_64.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[ $(arch) = x86_64 ] && dnf install -y special-pkg'])
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"x86_64": ["special-pkg"]})

    def test_double_bracket_neq_or(self):
        """
        ``[[ $(arch) != aarch64 ]] || yum install -y arm-pkg`` with [[ ]] preprocessing.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[[ $(arch) != aarch64 ]] || yum install -y arm-pkg'])
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"aarch64": ["arm-pkg"]})

    def test_eq_or_treated_as_unconditional(self):
        """
        ``[ $(arch) = x86_64 ] || dnf install -y pkg`` means "all except x86_64"
        which needs the full arch set — treated as unconditional.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[ $(arch) = x86_64 ] || dnf install -y pkg'])
        self.assertIn("pkg", pkgs)
        self.assertEqual(arch_pkgs, {})

    def test_neq_and_treated_as_unconditional(self):
        """
        ``[ $(arch) != x86_64 ] && dnf install -y pkg`` means "all except x86_64"
        — treated as unconditional.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[ $(arch) != x86_64 ] && dnf install -y pkg'])
        self.assertIn("pkg", pkgs)
        self.assertEqual(arch_pkgs, {})

    def test_arch_context_only_applies_to_next_command(self):
        """
        In ``[ test ] || cmd1 && cmd2``, only cmd1 gets the arch context.
        bashlex parses this as ``(test || cmd1) && cmd2`` (left-associative),
        so only cmd1 is ``parts[i+2]`` and cmd2 falls into remaining nodes.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(
            ['[ $(arch) != x86_64 ] || dnf install -y special-pkg && dnf install -y common-pkg']
        )
        self.assertIn("common-pkg", pkgs)
        self.assertEqual(arch_pkgs, {"x86_64": ["special-pkg"]})

    def test_regular_or_fallback_unchanged(self):
        """
        Regular ``cmd1 || cmd2`` without [ test ] stays unconditional.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(
            ['dnf install -y preferred-pkg || dnf install -y fallback-pkg']
        )
        self.assertIn("preferred-pkg", pkgs)
        self.assertIn("fallback-pkg", pkgs)
        self.assertEqual(arch_pkgs, {})

    def test_uname_m_neq_or(self):
        """
        ``[ $(uname -m) != s390x ] || dnf install -y s390x-pkg``
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[ $(uname -m) != s390x ] || dnf install -y s390x-pkg'])
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"s390x": ["s390x-pkg"]})

    def test_go_env_goarch_neq_or(self):
        """
        ``[ $(go env GOARCH) != "amd64" ] || yum install -y pkg``
        Go arch name ``amd64`` is normalized to RPM name ``x86_64``.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(
            ['[ $(go env GOARCH) != "amd64" ] || yum install -y special-pkg']
        )
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"x86_64": ["special-pkg"]})

    def test_go_env_goarch_neq_or_subshell(self):
        """
        ``[ $(go env GOARCH) != "amd64" ] || (yum install -y pkg1 pkg2 && other)``
        — subshell grouping after ``||`` should still extract arch packages.
        Go arch name ``amd64`` is normalized to RPM name ``x86_64``.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(
            ['[ $(go env GOARCH) != "amd64" ] || (yum install -y llvm-toolset cmake3 gcc-c++ && tar zfx cross.tar.gz)']
        )
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"x86_64": ["cmake3", "gcc-c++", "llvm-toolset"]})

    def test_goarch_var_neq_or(self):
        """
        ``[ $GOARCH != "arm64" ] || dnf install -y pkg``
        Go arch name ``arm64`` is normalized to RPM name ``aarch64``.
        """
        pkgs, arch_pkgs = extract_packages_from_run_commands(['[ $GOARCH != "arm64" ] || dnf install -y arm-only-pkg'])
        self.assertEqual(pkgs, [])
        self.assertEqual(arch_pkgs, {"aarch64": ["arm-only-pkg"]})


class TestBuilddepParsing(unittest.TestCase):
    def test_simple_builddep(self):
        run_values = ["dnf builddep -y pkcs11-helper*"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["pkcs11-helper*"])

    def test_builddep_with_flags(self):
        run_values = ["dnf builddep -y --skip-broken --nobest openvpn*"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["openvpn*"])

    def test_builddep_multiple_patterns(self):
        run_values = ["dnf builddep -y pkcs11-helper* && dnf builddep -y openvpn*"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["openvpn*", "pkcs11-helper*"])

    def test_builddep_does_not_interfere_with_install(self):
        run_values = ["dnf install -y gcc make && dnf builddep -y pkcs11-helper*"]
        common, arch, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(common, ["gcc", "make"])
        self.assertEqual(arch, {})
        self.assertEqual(builddep, ["pkcs11-helper*"])

    def test_builddep_without_glob(self):
        run_values = ["dnf builddep -y pkcs11-helper"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["pkcs11-helper"])

    def test_yum_builddep(self):
        run_values = ["yum builddep -y mypackage"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["mypackage"])

    def test_builddep_spec_file(self):
        run_values = ["dnf builddep -y mypackage.spec"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["mypackage.spec"])

    def test_build_dep_hyphenated(self):
        run_values = ["dnf build-dep tuned.spec -y"]
        _, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertEqual(builddep, ["tuned.spec"])

    def test_build_dep_hyphenated_with_install(self):
        run_values = ["dnf install -y gcc rpm-build && cd assets/tuned/daemon && dnf build-dep tuned.spec -y"]
        common, _, _, _, builddep, _ = analyze_run_commands(run_values)
        self.assertIn("gcc", common)
        self.assertEqual(builddep, ["tuned.spec"])


class TestModuleParsing(unittest.TestCase):
    def test_module_install(self):
        run_values = ["dnf module install -y nodejs:18/development"]
        _, _, _, _, _, modules = analyze_run_commands(run_values)
        self.assertEqual(modules, ["nodejs:18/development"])

    def test_module_enable(self):
        run_values = ["dnf module enable -y nodejs:18"]
        _, _, _, _, _, modules = analyze_run_commands(run_values)
        self.assertEqual(modules, ["nodejs:18"])

    def test_module_with_install_does_not_interfere(self):
        run_values = ["dnf -y install gcc && dnf module install -y nodejs:18/development"]
        common, _, _, _, _, modules = analyze_run_commands(run_values)
        self.assertEqual(common, ["gcc"])
        self.assertEqual(modules, ["nodejs:18/development"])

    def test_module_multiple(self):
        run_values = ["dnf module enable -y nodejs:18 python36:3.6"]
        _, _, _, _, _, modules = analyze_run_commands(run_values)
        self.assertEqual(modules, ["nodejs:18", "python36:3.6"])

    def test_module_without_stream_ignored(self):
        run_values = ["dnf module enable -y nodejs"]
        _, _, _, _, _, modules = analyze_run_commands(run_values)
        self.assertEqual(modules, [])

    def test_variable_package_manager(self):
        run_values = ["${DNF} install -y openssh-clients"]
        pkgs, _, _, _, _, _ = analyze_run_commands(run_values, env_vars={"DNF": "microdnf"})
        self.assertEqual(pkgs, ["openssh-clients"])

    def test_variable_package_manager_in_conditional(self):
        run_values = [
            "if ! rpm -q openssh-clients; then ${DNF} install -y openssh-clients "
            "&& ${DNF} clean all && rm -rf /var/cache/dnf/*; fi"
        ]
        pkgs, _, _, _, _, _ = analyze_run_commands(run_values, env_vars={"DNF": "microdnf"})
        self.assertEqual(pkgs, ["openssh-clients"])

    def test_variable_package_manager_multiple_commands(self):
        run_values = [
            "if ! rpm -q openssh-clients; then ${DNF} install -y openssh-clients && ${DNF} clean all; fi",
            "if ! rpm -q libvirt-libs; then ${DNF} install -y libvirt-libs && ${DNF} clean all; fi",
            "if ! command -v tar; then ${DNF} install -y tar && ${DNF} clean all; fi",
        ]
        pkgs, _, _, _, _, _ = analyze_run_commands(run_values, env_vars={"DNF": "microdnf"})
        self.assertEqual(pkgs, ["libvirt-libs", "openssh-clients", "tar"])
