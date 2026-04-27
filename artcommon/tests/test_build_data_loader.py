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
