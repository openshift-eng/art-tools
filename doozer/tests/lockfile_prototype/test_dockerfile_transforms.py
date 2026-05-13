import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from doozerlib.lockfile_prototype.dockerfile_transforms import (
    fix_rpm_verify_commands,
    strip_bare_updates,
    strip_bare_updates_from_scripts,
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
        self.assertIn('|| echo "$_art_pkg"; done)', result)
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
