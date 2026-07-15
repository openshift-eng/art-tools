import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from doozerlib.lockfile_prototype.dockerfile_transforms import (
    fix_rpm_verify_commands,
    strip_bare_updates,
    strip_bare_updates_from_scripts,
    transform_reinstall_commands,
)


class TestStripBareUpdates(unittest.TestCase):
    def test_strips_bare_yum_update(self):
        content = "RUN . /cachi2/cachi2.env && yum update -y && yum clean all\n"
        result = strip_bare_updates(content)
        self.assertNotIn("yum update -y", result)
        self.assertIn("yum clean all", result)

    def test_strips_bare_dnf_update(self):
        content = "RUN dnf update -y && dnf clean all\n"
        result = strip_bare_updates(content)
        self.assertNotIn("dnf update -y", result)
        self.assertIn("dnf clean all", result)

    def test_strips_bare_microdnf_update(self):
        content = (
            "RUN . /cachi2/cachi2.env &&     "
            "echo 'skip_missing_names_on_install=0' >> /etc/yum.conf  "
            "&& microdnf update -y   "
            "&& microdnf clean all\n"
        )
        result = strip_bare_updates(content)
        self.assertNotIn("microdnf update -y", result)
        self.assertIn("microdnf clean all", result)
        self.assertIn("cachi2.env", result)

    def test_strips_microdnf_flag_before_action(self):
        content = "RUN microdnf -y update && microdnf clean all\n"
        result = strip_bare_updates(content)
        self.assertNotIn("microdnf -y update", result)
        self.assertIn("microdnf clean all", result)

    def test_strips_bare_dnf_upgrade(self):
        content = "RUN dnf upgrade -y && dnf clean all\n"
        result = strip_bare_updates(content)
        self.assertNotIn("dnf upgrade -y", result)

    def test_preserves_named_update(self):
        content = "RUN dnf update -y openssl && dnf clean all\n"
        result = strip_bare_updates(content)
        self.assertIn("dnf update -y openssl", result)

    def test_strips_yum_flag_before_action(self):
        """
        yum -y update (flag before action) must also be stripped.
        """
        content = "RUN yum -y update && yum clean all\n"
        result = strip_bare_updates(content)
        self.assertNotIn("yum -y update", result)
        self.assertIn("yum clean all", result)

    def test_no_update_unchanged(self):
        content = "FROM base\nRUN yum install -y wget\n"
        result = strip_bare_updates(content)
        self.assertEqual(result, content)

    def test_strips_with_line_continuation(self):
        content = "RUN . /cachi2/cachi2.env && \\\n    yum update -y \\\n    && yum clean all\n"
        result = strip_bare_updates(content)
        self.assertNotIn("yum update -y", result)

    def test_real_world_sriov_pattern(self):
        """
        Pattern from sriov-network-config-daemon Dockerfile.
        """
        content = (
            'RUN . /cachi2/cachi2.env &&     '
            'yum -y update && '
            'ARCH_DEP_PKGS=$(if [ "$(uname -m)" != "s390x" ]; then echo -n mstflint ; fi) && '
            'yum -y install pciutils hwdata kmod $ARCH_DEP_PKGS && '
            'yum clean all\n'
        )
        result = strip_bare_updates(content)
        self.assertNotIn("yum -y update", result)
        self.assertIn("yum -y install", result)
        self.assertIn("yum clean all", result)

    def test_real_world_nodejs_pattern(self):
        """
        Pattern from openshift-base-nodejs-rhel9 Dockerfile.
        """
        content = (
            "RUN . /cachi2/cachi2.env &&     "
            "echo 'skip_missing_names_on_install=0' >> /etc/yum.conf  "
            "&& echo 'exclude=nodejs nodejs-docs nodejs-full-i18n npm nodejs-libs' >> /etc/yum.conf  "
            "&& yum update -y   "
            "&& yum clean all\n"
        )
        result = strip_bare_updates(content)
        self.assertNotIn("yum update -y", result)
        self.assertIn("yum clean all", result)
        self.assertIn("cachi2.env", result)


class TestStripBareUpdatesFromScripts(unittest.TestCase):
    def test_strips_updates_from_sh_files(self):
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir)
            script = dest / "install.sh"
            script.write_text("#!/bin/bash\nyum -y update\nyum -y install wget\n")
            strip_bare_updates_from_scripts(dest)
            result = script.read_text()
            self.assertNotIn("yum -y update", result)
            self.assertIn("yum -y install wget", result)

    def test_ignores_non_sh_files(self):
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir)
            txt = dest / "notes.txt"
            txt.write_text("yum update -y\n")
            strip_bare_updates_from_scripts(dest)
            self.assertEqual(txt.read_text(), "yum update -y\n")

    def test_walks_subdirectories(self):
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir)
            subdir = dest / "hack"
            subdir.mkdir()
            script = subdir / "build.sh"
            script.write_text("dnf -y update && dnf install -y openssl\n")
            strip_bare_updates_from_scripts(dest)
            result = script.read_text()
            self.assertNotIn("dnf -y update", result)
            self.assertIn("dnf install -y openssl", result)

    def test_no_change_no_write(self):
        with TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir)
            script = dest / "clean.sh"
            script.write_text("#!/bin/bash\necho hello\n")
            mtime_before = script.stat().st_mtime_ns
            strip_bare_updates_from_scripts(dest)
            mtime_after = script.stat().st_mtime_ns
            self.assertEqual(mtime_before, mtime_after)


class TestTransformReinstallCommands(unittest.TestCase):
    def test_wraps_microdnf_reinstall(self):
        content = "RUN microdnf -y install openssl && microdnf -y reinstall tzdata && microdnf clean all\n"
        result = transform_reinstall_commands(content)
        self.assertIn("(microdnf -y reinstall tzdata || true)", result)
        self.assertIn("microdnf -y install openssl", result)
        self.assertIn("microdnf clean all", result)

    def test_wraps_dnf_reinstall(self):
        content = "RUN dnf -y reinstall tzdata && dnf clean all\n"
        result = transform_reinstall_commands(content)
        self.assertIn("(dnf -y reinstall tzdata || true) && dnf clean all", result)

    def test_wraps_yum_reinstall(self):
        content = "RUN yum reinstall -y glibc && yum clean all\n"
        result = transform_reinstall_commands(content)
        self.assertIn("(yum reinstall -y glibc || true)", result)
        self.assertIn("yum clean all", result)

    def test_wraps_reinstall_multiple_packages(self):
        content = "RUN microdnf -y reinstall tzdata glibc && microdnf clean all\n"
        result = transform_reinstall_commands(content)
        self.assertIn("(microdnf -y reinstall tzdata glibc || true)", result)
        self.assertIn("microdnf clean all", result)

    def test_preserves_install_commands(self):
        content = "RUN microdnf -y install openssl && microdnf clean all\n"
        result = transform_reinstall_commands(content)
        self.assertEqual(result, content)

    def test_no_reinstall_unchanged(self):
        content = "FROM base\nRUN yum install -y wget\n"
        result = transform_reinstall_commands(content)
        self.assertEqual(result, content)

    def test_real_world_oadp_pattern(self):
        """
        Pattern from oadp-operator Dockerfile.
        """
        content = (
            "RUN . /cachi2/cachi2.env && "
            "    microdnf -y install openssl && "
            "microdnf -y reinstall tzdata && "
            "microdnf clean all\n"
        )
        result = transform_reinstall_commands(content)
        self.assertIn("(microdnf -y reinstall tzdata || true)", result)
        self.assertIn("microdnf -y install openssl", result)
        self.assertIn("microdnf clean all", result)

    def test_does_not_cross_newline(self):
        """
        Reinstall at end of RUN must not consume the next Dockerfile
        instruction across an unescaped newline.
        """
        content = "RUN dnf -y reinstall tzdata\nCOPY foo bar\n"
        result = transform_reinstall_commands(content)
        self.assertEqual(
            result,
            "RUN (dnf -y reinstall tzdata || true)\nCOPY foo bar\n",
        )

    def test_multiline_continuation(self):
        """
        Backslash-newline continuation is collapsed into a single line.
        """
        content = "RUN dnf -y reinstall tzdata \\\n    && dnf clean all\n"
        result = transform_reinstall_commands(content)
        self.assertEqual(
            result,
            "RUN (dnf -y reinstall tzdata || true) && dnf clean all\n",
        )

    def test_existing_or_fallback_unchanged(self):
        """
        If the reinstall already has an || fallback, leave it unchanged
        to avoid making the original fallback unreachable.
        """
        content = "RUN dnf -y reinstall tzdata || echo failed && dnf clean all\n"
        result = transform_reinstall_commands(content)
        self.assertEqual(result, content)


class TestFixRpmVerifyCommands(unittest.TestCase):
    def test_transforms_variable_package_list(self):
        """
        rpm -V with a variable package list should be transformed so each
        package name is resolved via --whatprovides before verification.
        """
        content = (
            "RUN INSTALL_PKGS=\"bind-utils wget\" && \\\n"
            "    yum -y install $INSTALL_PKGS && \\\n"
            "    rpm -V --nogroup --nosize --nofiledigest --nomtime --nomode $INSTALL_PKGS && \\\n"
            "    yum clean all\n"
        )
        result = fix_rpm_verify_commands(content)
        self.assertIn("rpm -V --nogroup --nosize --nofiledigest --nomtime --nomode", result)
        self.assertIn("$(for _art_pkg in $INSTALL_PKGS; do", result)
        self.assertIn("--whatprovides", result)
        self.assertIn("_art_name=$_art_pkg;", result)
        self.assertNotIn("rpm -V --nogroup --nosize --nofiledigest --nomtime --nomode $INSTALL_PKGS", result)

    def test_transforms_literal_package_names(self):
        """
        rpm -V with literal package names should also be transformed.
        """
        content = "RUN rpm -V bind-utils wget curl\n"
        result = fix_rpm_verify_commands(content)
        self.assertIn("$(for _art_pkg in bind-utils wget curl; do", result)
        self.assertIn("--whatprovides", result)

    def test_no_flags(self):
        """
        rpm -V with no flags and a single variable.
        """
        content = "RUN rpm -V $PKGS && yum clean all\n"
        result = fix_rpm_verify_commands(content)
        self.assertIn("$(for _art_pkg in $PKGS; do", result)

    def test_multiple_rpm_v_in_one_run(self):
        """
        Multiple rpm -V calls in the same RUN command are both transformed.
        """
        content = "RUN rpm -V pkg1 && rpm -V --nosize pkg2\n"
        result = fix_rpm_verify_commands(content)
        self.assertEqual(result.count("--whatprovides"), 2)

    def test_no_rpm_v_unchanged(self):
        """
        Dockerfile without rpm -V must be returned unchanged.
        """
        content = "FROM base\nRUN yum install -y wget && yum clean all\n"
        result = fix_rpm_verify_commands(content)
        self.assertEqual(result, content)

    def test_rpm_query_not_transformed(self):
        """
        rpm -q (query) and rpm -i (install) must not be transformed.
        """
        content = "RUN rpm -q bind-utils && rpm -i foo.rpm\n"
        result = fix_rpm_verify_commands(content)
        self.assertEqual(result, content)

    def test_does_not_corrupt_subsequent_instructions(self):
        """
        rpm -V transformation must not cross Dockerfile instruction
        boundaries. Regression test: the regex previously used \\s+ for
        the package-args group, which matched newlines and consumed
        COPY/WORKDIR/FROM/etc. instructions that followed the RUN.
        """
        content = (
            "RUN INSTALL_PKGS=\"gcc-c++ git\" && \\\n"
            "    microdnf install -y $INSTALL_PKGS && \\\n"
            "    rpm -V $INSTALL_PKGS\n"
            "\n"
            "COPY . /opt/app-root/src\n"
            "WORKDIR /opt/app-root/src\n"
            "\n"
            "COPY --from=builder /usr/bin/vector /usr/bin/\n"
        )
        result = fix_rpm_verify_commands(content)
        self.assertIn("$(for _art_pkg in $INSTALL_PKGS; do", result)
        self.assertIn("COPY . /opt/app-root/src\n", result)
        self.assertIn("WORKDIR /opt/app-root/src\n", result)
        self.assertIn("COPY --from=builder /usr/bin/vector /usr/bin/\n", result)

    def test_does_not_corrupt_multistage_dockerfile(self):
        """
        Full multi-stage Dockerfile with rpm -V in the builder stage
        must not have the transformation leak into the second stage.
        """
        content = (
            "FROM ubi9 AS builder\n"
            "RUN INSTALL_PKGS=\"gcc-c++ openssl-devel\" && \\\n"
            "    microdnf install -y $INSTALL_PKGS && \\\n"
            "    rpm -V $INSTALL_PKGS\n"
            "COPY . /src\n"
            "RUN make build\n"
            "\n"
            "FROM ubi9\n"
            "COPY --from=builder /usr/bin/app /usr/bin/\n"
            "RUN microdnf install -y systemd\n"
        )
        result = fix_rpm_verify_commands(content)
        self.assertIn("$(for _art_pkg in $INSTALL_PKGS; do", result)
        self.assertIn("COPY . /src\n", result)
        self.assertIn("RUN make build\n", result)
        self.assertIn("FROM ubi9\n", result)
        self.assertIn("COPY --from=builder /usr/bin/app /usr/bin/\n", result)
        self.assertIn("RUN microdnf install -y systemd\n", result)
