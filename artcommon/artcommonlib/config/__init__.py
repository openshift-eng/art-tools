from io import StringIO
from logging import Logger, getLogger
from pathlib import Path
from typing import Any, Literal

from artcommonlib.assembly import assembly_field
from artcommonlib.gitdata import GitData
from artcommonlib.util import deep_merge
from ruamel.yaml import YAML

yaml = YAML(typ='safe')


class _IncludeConstructor:
    """Custom ruamel.yaml constructor for processing !include tag in YAML files."""

    def __init__(self, base_dir: Path | None, replace_vars: dict | None = None) -> None:
        self.base_dir = base_dir
        self.replace_vars = replace_vars

    def __call__(self, loader, node) -> Any:
        if node.id == "scalar":  # for `!include "file.yml"`
            patterns = (loader.construct_scalar(node),)
        # for `!include ["file1.yml", "file2.yml"]`
        elif node.id == "sequence":
            patterns = loader.construct_sequence(node)
        else:
            raise ValueError(f"Invalid node type `{node.id}` in `!include` statement")
        current_dir = Path(node.start_mark.name).parent.resolve()
        merged_doc = None
        for pattern in patterns:
            files = sorted(current_dir.glob(pattern))
            if not files:
                raise FileNotFoundError(f"Included file not found from directory {current_dir}: {pattern}")
            for file in files:
                file = file.resolve()
                if self.base_dir and not file.is_relative_to(self.base_dir):
                    raise IOError(f"Included path {file} is outside of base dir {self.base_dir}")
                with open(file, 'r') as f:
                    content = f.read()
                if self.replace_vars:
                    content = content.format(**self.replace_vars)
                f = StringIO(content)
                f.name = str(file)
                doc = yaml.load(f)
                if isinstance(merged_doc, dict):
                    merged_doc = {**merged_doc, **doc}
                elif isinstance(merged_doc, list):
                    merged_doc.extend(doc)
                else:
                    merged_doc = doc
        return merged_doc


class BuildDataLoader:
    """Helper class to load build data from various sources.

    Example usage:
    .. code-block:: python
        loader = BuildDataLoader(
            data_path="https://github.com/openshift-eng/ocp-build-data.git",
            clone_dir="./working-dir",
            commitish="openshift-4.21",
            build_system="konflux",  # If konflux, konflux-specific overrides will be applied
        )
        # Load the group configuration
        group_config = loader.load_group_config(assembly=None, releases_config=None)
        # Apply assembly-specific overrides from releases_config
        releases_config = loader.load_releases_config()
        group_config_with_assembly = loader.load_group_config(assembly="ec.1", releases_config=releases_config)
    """

    BuildSystem = Literal['brew', 'konflux']

    def __init__(
        self,
        data_path: str,
        clone_dir: str,
        commitish: str,
        build_system: BuildSystem,
        upcycle: bool = False,
        gitdata: GitData | None = None,
        logger: Logger | None = None,
    ) -> None:
        self.data_path = data_path
        self.clone_dir = clone_dir
        self.commitish = commitish
        self.build_system = build_system
        self.upcycle = upcycle
        self._logger = logger or getLogger(__name__)
        self._gitdata = gitdata  # will be loaded on demand if None

    def _ensure_gitdata(self):
        if self._gitdata is not None:
            return
        self._gitdata = GitData(
            data_path=self.data_path,
            clone_dir=self.clone_dir,
            commitish=self.commitish,
            reclone=self.upcycle,
            logger=self._logger,
        )

    @property
    def data_dir(self) -> str:
        self._ensure_gitdata()
        assert self._gitdata is not None
        return self._gitdata.data_dir

    def _include_constructor(self, loader, node):
        """This is a ruamel.yaml constructor for processing !include tag in yaml files"""
        if node.id == "scalar":  # for `!include "file.yml"`
            val = (loader.construct_scalar(node),)
        # for `!include ["file1.yml", "file2.yml"]`
        elif node.id == "sequence":
            val = loader.construct_sequence(node)
        else:
            raise ValueError("Invalid node type in !include statement")
        base_dir = self.data_path
        merged_doc = None
        for it in val:
            for path in sorted(Path(base_dir).glob(it)):
                path = path.resolve()
                if not path.is_relative_to(base_dir):
                    raise IOError(f"Included path {path} is outside of base dir {base_dir}")
                with open(path, 'r') as f:
                    doc = yaml.load(f)
                if isinstance(merged_doc, dict):
                    merged_doc = {**merged_doc, **doc}
                elif isinstance(merged_doc, list):
                    merged_doc.extend(doc)
                else:
                    merged_doc = doc
        return merged_doc

    def load_group_config(
        self,
        assembly: str | None,
        releases_config: dict | None,
        additional_vars: dict[str, str] | None = None,
        allow_includes: bool = True,
    ):
        """Loads group configuration.

        :param assembly: The assembly name. If specified, assembly-specific overrides will be applied to the group configuration.
        :param releases_config: The releases configuration. If None, the releases configuration from build data will be used.
        :param additional_vars: Additional variables to substitute in the configuration.
        :param allow_includes: Whether to allow !include directives in the YAML files.
        :return: The loaded group configuration.
        """
        self._ensure_gitdata()
        # group.yml can contain a `vars` section which should be a
        # single level dict containing keys to str.format(**dict) replace
        # into the YAML content. If `vars` found, the format will be
        # preformed and the YAML model will reloaded from that result
        group_config = self.load_config("group")
        variables = group_config.get('vars')
        if variables and not isinstance(variables, dict):
            raise TypeError("The 'vars' field in group configuration must be a dictionary if present.")
        if additional_vars is not None:
            if not isinstance(additional_vars, dict):
                raise TypeError("additional_vars must be a dictionary or None")
            variables = variables.copy() if variables else {}
            variables.update(additional_vars)
        if variables:
            # Perform variable substitution and reload
            group_config = self.load_config("group", replace_vars=variables)

        # If group.ext.yml exists, load it
        data_dir = Path(self.data_dir)
        group_ext_file = data_dir / "group.ext.yml"
        if group_ext_file.exists():
            if allow_includes:
                yaml.constructor.add_constructor(
                    '!include', _IncludeConstructor(base_dir=data_dir, replace_vars=variables)
                )
            with group_ext_file.open('r') as f:
                content = f.read()
                if variables:
                    content = content.format(**variables)
                f = StringIO(content)
                f.name = str(group_ext_file)
                group_ext = yaml.load(f)
            group_config = deep_merge(group_config, group_ext)

        # If build system is konflux, merge konflux-specific overrides
        if self.build_system == 'konflux':
            konflux_overrides = group_config.get('konflux')
            if konflux_overrides:
                group_config = deep_merge(group_config, konflux_overrides)

        # If assembly is specified, apply assembly-specific overrides
        if assembly:
            if releases_config is None:
                releases_config = self.load_releases_config(config_file=None)
            group_config = assembly_field(releases_config, assembly, "group", group_config)
        return group_config

    def load_releases_config(self, config_file: str | None = None) -> dict:
        """Loads releases config from build data.

        :param config_file: If specified, this file will be used.
        """
        # We never support variable substitution in releases config
        return self.load_config(key='releases', config_file=config_file, default={}, replace_vars=None)

    def load_config(
        self, key: str, default: Any = None, config_file: str | None = None, replace_vars: dict[str, str] | None = None
    ):
        """Loads additional configuration from build data with variable substitution.

        :param key: The key for the additional configuration.
        :param default: The default value to return if the configuration is not found.
        :param config_file: If specified, this file will be used instead of the one in the build data.
        :param replace_vars: Variables to use for variable substitution.
        """
        if config_file:  # override filename specified on command line.
            acp = Path(config_file)
            data = yaml.load(acp)
        else:
            self._ensure_gitdata()
            assert self._gitdata is not None
            data_obj = self._gitdata.load_data(key=key, replace_vars=replace_vars)
            data = data_obj.data if data_obj else None
        return data or default
