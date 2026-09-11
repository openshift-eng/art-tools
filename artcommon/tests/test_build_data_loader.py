from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase

from artcommonlib.config import BuildDataLoader
from ruamel.yaml import YAML


class _StubGitData:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = str(data_dir)

    def load_data(self, key, replace_vars=None):
        path = Path(self.data_dir) / f"{key}.yml"
        content = path.read_text()
        if replace_vars:
            content = content.format(**replace_vars)
        f = StringIO(content)
        f.name = str(path)
        return SimpleNamespace(data=YAML(typ='safe').load(f))


class TestBuildDataLoader(TestCase):
    def test_load_group_config_with_includes(self):
        with self.subTest("group.ext !include uses a fresh parser"):
            from tempfile import TemporaryDirectory

            with TemporaryDirectory() as temp_dir:
                data_dir = Path(temp_dir)
                (data_dir / "group.yml").write_text(
                    "\n".join(
                        [
                            "vars:",
                            "  MAJOR: 4",
                            "  MINOR: 21",
                            "name: openshift-{MAJOR}.{MINOR}",
                            "",
                        ]
                    )
                )
                (data_dir / "group.ext.yml").write_text("plashet: !include plashet.yml\n")
                (data_dir / "plashet.yml").write_text(
                    "\n".join(
                        [
                            'base_dir: "{MAJOR}.{MINOR}/$runtime_assembly/$slug"',
                            'plashet_dir: "$yyyy-$MM/$revision"',
                            "create_symlinks: true",
                            "",
                        ]
                    )
                )

                loader = BuildDataLoader(
                    data_path=str(data_dir),
                    clone_dir=str(data_dir),
                    commitish="openshift-4.21",
                    build_system="brew",
                    gitdata=_StubGitData(data_dir),
                )

                group_config = loader.load_group_config(assembly=None, releases_config=None)

                self.assertEqual(group_config["name"], "openshift-4.21")
                self.assertEqual(group_config["plashet"]["base_dir"], "4.21/$runtime_assembly/$slug")
                self.assertEqual(group_config["plashet"]["plashet_dir"], "$yyyy-$MM/$revision")
                self.assertTrue(group_config["plashet"]["create_symlinks"])

    def test_load_group_config_exposes_effective_vars(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            (data_dir / "group.yml").write_text(
                "\n".join(
                    [
                        "name: golang",
                        "vars:",
                        "  MAJOR: major",
                        "  MINOR: minor",
                        "branch: rhaos-{MAJOR}.{MINOR}-rhel-9",
                        "",
                    ]
                )
            )

            loader = BuildDataLoader(
                data_path=str(data_dir),
                clone_dir=str(data_dir),
                commitish="golang",
                build_system="brew",
                gitdata=_StubGitData(data_dir),
            )

            group_config = loader.load_group_config(
                assembly=None,
                releases_config=None,
                additional_vars={"MAJOR": 5, "MINOR": 0},
            )

            self.assertEqual(group_config["branch"], "rhaos-5.0-rhel-9")
            self.assertEqual(group_config["vars"]["MAJOR"], 5)
            self.assertEqual(group_config["vars"]["MINOR"], 0)

    def test_load_group_config_vars_precedence_assembly_overrides_defaults(self):
        """Assembly group.vars overrides (resolved_vars) beat computed defaults (additional_vars).

        Scenario: an assembly sets runtime_assembly='stream' in releases.yml.
        The computed default additional_vars carries runtime_assembly='art23398'.
        After the final merge, resolved_vars must win → runtime_assembly == 'stream'.
        """
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            (data_dir / "group.yml").write_text(
                "\n".join(
                    [
                        "name: openshift-5.0",
                        "vars:",
                        "  MAJOR: 5",
                        "  MINOR: 0",
                        "  runtime_assembly: '{runtime_assembly}'",
                        "",
                    ]
                )
            )
            # releases.yml: assembly 'art23398' overrides group.vars.runtime_assembly to 'stream'
            (data_dir / "releases.yml").write_text(
                "\n".join(
                    [
                        "releases:",
                        "  art23398:",
                        "    assembly:",
                        "      group:",
                        "        vars:",
                        "          runtime_assembly: stream",
                        "",
                    ]
                )
            )
            loader = BuildDataLoader(
                data_path=str(data_dir),
                clone_dir=str(data_dir),
                commitish="openshift-5.0",
                build_system="brew",
                gitdata=_StubGitData(data_dir),
            )
            releases_config = loader.load_releases_config()
            # additional_vars simulates the computed default from get_replace_vars(None)
            group_config = loader.load_group_config(
                assembly="art23398",
                releases_config=releases_config,
                additional_vars={"runtime_assembly": "art23398", "MAJOR": 5, "MINOR": 0},
            )
            # The assembly override (stream) must survive, not be clobbered by the default (art23398)
            self.assertEqual(group_config["vars"]["runtime_assembly"], "stream")

    def test_load_group_config_vars_precedence_no_assembly_override_fallback(self):
        """Without an assembly override, runtime_assembly falls back to the computed default."""
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            (data_dir / "group.yml").write_text(
                "\n".join(
                    [
                        "name: openshift-5.0",
                        "vars:",
                        "  MAJOR: 5",
                        "  MINOR: 0",
                        "",
                    ]
                )
            )
            loader = BuildDataLoader(
                data_path=str(data_dir),
                clone_dir=str(data_dir),
                commitish="openshift-5.0",
                build_system="brew",
                gitdata=_StubGitData(data_dir),
            )
            group_config = loader.load_group_config(
                assembly=None,
                releases_config=None,
                additional_vars={"runtime_assembly": "4.17.3", "MAJOR": 5, "MINOR": 0},
            )
            # No assembly override → additional_vars default wins
            self.assertEqual(group_config["vars"]["runtime_assembly"], "4.17.3")

    def test_load_group_config_vars_precedence_extra_vars_wins(self):
        """CLI --var (extra_vars) wins over both assembly overrides and computed defaults."""
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)
            (data_dir / "group.yml").write_text(
                "\n".join(
                    [
                        "name: openshift-5.0",
                        "vars:",
                        "  MAJOR: 5",
                        "  MINOR: 0",
                        "  runtime_assembly: '{runtime_assembly}'",
                        "",
                    ]
                )
            )
            (data_dir / "releases.yml").write_text(
                "\n".join(
                    [
                        "releases:",
                        "  art23398:",
                        "    assembly:",
                        "      group:",
                        "        vars:",
                        "          runtime_assembly: stream",
                        "",
                    ]
                )
            )
            loader = BuildDataLoader(
                data_path=str(data_dir),
                clone_dir=str(data_dir),
                commitish="openshift-5.0",
                build_system="brew",
                gitdata=_StubGitData(data_dir),
            )
            releases_config = loader.load_releases_config()
            group_config = loader.load_group_config(
                assembly="art23398",
                releases_config=releases_config,
                additional_vars={"runtime_assembly": "art23398", "MAJOR": 5, "MINOR": 0},
                extra_vars={"runtime_assembly": "cli-override"},
            )
            # CLI --var must win over everything
            self.assertEqual(group_config["vars"]["runtime_assembly"], "cli-override")
