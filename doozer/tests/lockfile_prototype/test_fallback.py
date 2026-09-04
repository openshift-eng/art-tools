"""
Tests for doozerlib.lockfile_prototype.fallback.
"""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from doozerlib.lockfile_prototype.fallback import (
    _collect_stage_vars,
    _resolve_bash_expansion,
    extract_generated_file_content,
)


class TestResolveBashExpansion(unittest.TestCase):
    def test_plain_var(self):
        self.assertEqual(_resolve_bash_expansion("hello $NAME", {"NAME": "world"}), "hello world")

    def test_braced_var(self):
        self.assertEqual(_resolve_bash_expansion("v${VER}", {"VER": "3.5"}), "v3.5")

    def test_default_value(self):
        self.assertEqual(_resolve_bash_expansion("${MISSING:-fallback}", {}), "fallback")

    def test_default_not_used_when_set(self):
        self.assertEqual(_resolve_bash_expansion("${V:-fallback}", {"V": "real"}), "real")

    def test_conditional_set(self):
        self.assertEqual(_resolve_bash_expansion("${V:+alt}", {"V": "yes"}), "alt")

    def test_conditional_set_empty(self):
        self.assertEqual(_resolve_bash_expansion("${V:+alt}", {}), "")

    def test_unresolved_becomes_empty(self):
        self.assertEqual(_resolve_bash_expansion("pkg-$UNDEFINED", {}), "pkg-")

    def test_nested_resolution(self):
        self.assertEqual(_resolve_bash_expansion("$A", {"A": "$B", "B": "final"}), "final")


class TestCollectStageVars(unittest.TestCase):
    def test_arg_with_default(self):
        entries = [{"instruction": "ARG", "value": "version=3.5"}]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"version": "3.5"})

    def test_arg_without_default_ignored(self):
        entries = [{"instruction": "ARG", "value": "version"}]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {})

    def test_env_collected(self):
        entries = [{"instruction": "ENV", "value": "FOO=bar"}]
        result = _collect_stage_vars(entries)
        self.assertEqual(result, {"FOO": "bar"})

    def test_env_references_arg(self):
        entries = [
            {"instruction": "ARG", "value": "ver=3.5"},
            {"instruction": "ENV", "value": "PKG=openvswitch$ver"},
        ]
        result = _collect_stage_vars(entries)
        self.assertEqual(result["PKG"], "openvswitch3.5")

    def test_inherited_vars(self):
        entries = [{"instruction": "ARG", "value": "local=val"}]
        result = _collect_stage_vars(entries, inherited_vars={"global": "gval"})
        self.assertEqual(result["global"], "gval")
        self.assertEqual(result["local"], "val")


class TestExtractGeneratedFileContent(unittest.TestCase):
    def test_echo_redirect(self):
        with TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            (parent_dir / "Dockerfile").write_text("FROM ubi9\nRUN echo \"pkg1 pkg2\" > /more-pkgs\n")

            result = extract_generated_file_content(parent_dir, "/more-pkgs")
            self.assertEqual(result, "pkg1 pkg2")

    def test_sed_heredoc_with_args(self):
        with TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            (parent_dir / "Dockerfile.base").write_text(
                "FROM ubi9\n"
                "ARG ovsver=3.5\n"
                "ARG ovnver=25.09\n"
                'RUN sed \'s/%/"/g\' <<<"%openvswitch$ovsver-devel% %ovn$ovnver-vtep%" > /more-pkgs\n'
            )

            result = extract_generated_file_content(parent_dir, "/more-pkgs")
            self.assertIn("openvswitch3.5-devel", result)
            self.assertIn("ovn25.09-vtep", result)

    def test_sed_multichar_substitution(self):
        with TemporaryDirectory() as tmpdir:
            parent_dir = Path(tmpdir)
            (parent_dir / "Dockerfile").write_text("FROM ubi9\nRUN sed 's/XX/YY/g' <<<\"aXXb\" > /more-pkgs\n")
            result = extract_generated_file_content(parent_dir, "/more-pkgs")
            self.assertEqual(result, "aYYb")

    def test_returns_empty_when_not_found(self):
        with TemporaryDirectory() as tmpdir:
            result = extract_generated_file_content(Path(tmpdir), "/nonexistent")
            self.assertEqual(result, "")
