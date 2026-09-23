"""
Resolve package lists selected by dynamic package scripts.

Dynamic package scripts choose a manifest at build time using values from
``/etc/os-release``. This module resolves the same manifest without executing
the script so its packages can be included in the RPM lockfile.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import yaml

from doozerlib.lockfile_prototype.container_utils import ContainerImageHelper
from doozerlib.lockfile_prototype.fallback import extract_generated_file_content
from doozerlib.lockfile_prototype.models import RepoEntry


@dataclass
class DynamicPackageSet:
    """
    Packages selected by a dynamic package script.
    """

    common: list[str]
    arch_specific: dict[str, list[str]]


def _find_script_paths(entries: list[dict]) -> set[str]:
    """
    Find shell-script paths referenced by Dockerfile RUN instructions.

    Arg(s):
        entries (list[dict]): DockerfileParser structure entries.
    Return Value(s):
        set[str]: Script paths relative to the build context.
    """
    script_re = re.compile(r"(?<![\w./-])(?:\./)?([\w][\w./-]*\.sh)(?![\w./-])")
    return {
        match.group(1).lstrip("./")
        for entry in entries
        if entry["instruction"] == "RUN"
        for match in script_re.finditer(entry["value"])
    }


class DynamicPackageResolver:
    """
    Resolve dynamic package manifests for Dockerfile stages.
    """

    def __init__(
        self,
        container: ContainerImageHelper,
        downstream_parents: list[str],
        parent_source_dirs: dict[int, Path],
        logger: logging.Logger,
    ):
        self._container = container
        self._downstream_parents = downstream_parents
        self._parent_source_dirs = parent_source_dirs
        self._logger = logger

    async def resolve(
        self,
        script_path: str,
        source_dir: Path,
        arches: list[str],
        entries: list[dict],
        repo_list: list[RepoEntry],
    ) -> dict[int, DynamicPackageSet]:
        """
        Resolve packages selected by a configured dynamic package script.

        The configured script is expected to select a YAML package manifest
        using ``ID`` and ``VERSION_ID`` values from ``/etc/os-release``. The
        manifest follows the extension format used by ``extensions/build.sh``.

        Arg(s):
            script_path (str): Script path relative to the build context.
            source_dir (Path): Build context containing the script and manifest.
            arches (list[str]): Target architectures.
            entries (list[dict]): Parsed Dockerfile instructions.
            repo_list (list[RepoEntry]): Repositories used to infer the
                manifest version when the base image is unavailable.
        Return Value(s):
            dict[int, DynamicPackageSet]: Packages keyed by Dockerfile stage.
        """
        script_file = self._resolve_source_path(source_dir, script_path)
        if not script_file.is_file():
            raise FileNotFoundError(f"Dynamic package script not found: {script_path}")

        stage_numbers = self._find_script_stages(entries, script_path)
        if not stage_numbers:
            raise ValueError(f"Dynamic package script is not invoked by the Dockerfile: {script_path}")

        script_content = script_file.read_text()
        manifest_template = self._find_manifest_template(script_content)
        if manifest_template is None:
            raise ValueError(f"Dynamic package script does not reference a YAML manifest: {script_path}")

        dynamic_packages: dict[int, DynamicPackageSet] = {}
        for stage_num in stage_numbers:
            image_pullspec = self._downstream_parents[stage_num] if stage_num < len(self._downstream_parents) else None
            os_release = ""
            if image_pullspec and "/" in image_pullspec:
                os_release = await self._container.read_file_from_image(image_pullspec, "/etc/os-release")
                if not os_release and stage_num in self._parent_source_dirs:
                    os_release = self._read_file_from_parent_source(
                        self._parent_source_dirs[stage_num], "/etc/os-release"
                    )
            else:
                self._logger.warning(
                    f"Cannot read /etc/os-release for dynamic package script {script_path} "
                    f"in stage {stage_num}: base image is unavailable; using source manifest fallback"
                )

            os_release_values = self._parse_os_release(os_release) if os_release else {}
            manifest_file = self._resolve_manifest_file(
                source_dir,
                manifest_template,
                os_release_values,
                repo_list,
                script_path,
            )

            package_set = self._read_package_manifest(manifest_file, arches)
            package_count = len(package_set.common) + sum(
                len(packages) for packages in package_set.arch_specific.values()
            )
            if package_count == 0:
                raise ValueError(f"Dynamic package script produced no packages: {script_path}")
            dynamic_packages[stage_num] = package_set

        return dynamic_packages

    def _resolve_manifest_file(
        self,
        source_dir: Path,
        manifest_template: str,
        os_release_values: dict[str, str],
        repo_list: list[RepoEntry],
        script_path: str,
    ) -> Path:
        """
        Resolve a dynamic package manifest from OS metadata or source files.

        The source-file fallback is used when the parent image cannot be
        extracted. It is safe when the template resolves to one manifest, or
        when repository metadata identifies one version-specific manifest.

        Arg(s):
            source_dir (Path): Build context containing the manifests.
            manifest_template (str): YAML path template from the script.
            os_release_values (dict[str, str]): Parsed OS metadata.
            repo_list (list[RepoEntry]): Repositories used for version hints.
            script_path (str): Script path for error messages.
        Return Value(s):
            Path: Resolved manifest path.
        """
        required_values = {"ID", "VERSION_ID"}
        if required_values.issubset(os_release_values):
            manifest_path = self._expand_manifest_path(manifest_template, os_release_values)
            manifest_file = self._resolve_source_path(source_dir, manifest_path)
            if not manifest_file.is_file():
                raise FileNotFoundError(f"Dynamic package manifest not found for {script_path}: {manifest_path}")
            return manifest_file

        wildcard_template = manifest_template.replace("${ID}", "*").replace("${VERSION_ID}", "*")
        wildcard_template = wildcard_template.replace("$ID", "*").replace("$VERSION_ID", "*")
        resolved_source_dir = source_dir.resolve()
        wildcard_path = self._resolve_source_path(source_dir, wildcard_template)
        wildcard_pattern = str(wildcard_path.relative_to(resolved_source_dir))
        candidates = sorted(path for path in resolved_source_dir.glob(wildcard_pattern) if path.is_file())

        major_version = self._extract_rhel_version_from_repos(repo_list)
        if major_version is not None:
            major_candidates = [
                path
                for path in candidates
                if self._manifest_version(path) and self._manifest_version(path)[0] == major_version
            ]
            if major_candidates:
                candidates = major_candidates

        version_hints = self._extract_repo_version_hints(repo_list)
        if version_hints:
            hinted_candidates = [
                path
                for path in candidates
                if self._manifest_version(path) and self._manifest_version(path)[1] in version_hints
            ]
            if hinted_candidates:
                candidates = hinted_candidates

        if len(candidates) != 1:
            candidate_names = ", ".join(str(path.relative_to(resolved_source_dir)) for path in candidates) or "none"
            raise RuntimeError(
                f"Cannot resolve dynamic package manifest for {script_path} without /etc/os-release; "
                f"candidates: {candidate_names}"
            )
        self._logger.warning(
            f"Using source manifest {candidates[0].relative_to(resolved_source_dir)} for dynamic package script "
            f"{script_path} because /etc/os-release was unavailable"
        )
        return candidates[0]

    @staticmethod
    def _manifest_version(manifest_file: Path) -> tuple[int, str] | None:
        """
        Extract the RHEL major and full version from a manifest filename.

        Arg(s):
            manifest_file (Path): Manifest path, such as ``rhel-9.6.yaml``.
        Return Value(s):
            tuple[int, str] | None: Major and full version, if recognized.
        """
        match = re.search(r"-(\d+(?:\.\d+)*)\.ya?ml$", manifest_file.name)
        if not match:
            return None
        version = match.group(1)
        return int(version.split(".", 1)[0]), version

    @staticmethod
    def _extract_rhel_version_from_repos(repo_list: list[RepoEntry]) -> int | None:
        """
        Extract RHEL major version from repository content set IDs.

        Arg(s):
            repo_list (list[RepoEntry]): Repository entries with repoid fields.
        Return Value(s):
            int | None: RHEL major version, or None if not detectable.
        """
        for repo in repo_list:
            match = re.search(r"rhel-(\d+)", repo.repoid)
            if match:
                return int(match.group(1))
        return None

    @staticmethod
    def _extract_repo_version_hints(repo_list: list[RepoEntry]) -> set[str]:
        """
        Extract version hints from repository IDs and URLs.

        Arg(s):
            repo_list (list[RepoEntry]): Repository entries.
        Return Value(s):
            set[str]: Candidate full RHEL versions such as ``9.6``.
        """
        hints: set[str] = set()
        for repo in repo_list:
            repo_text = f"{repo.repoid} {repo.baseurl}"
            hints.update(f"{major}.{minor}" for major, minor in re.findall(r"__(\d+)_DOT_(\d+)", repo_text))
            hints.update(re.findall(r"(?<![\d.])(\d+\.\d+)(?![\d.])", repo_text))
        return hints

    @staticmethod
    def _resolve_source_path(source_dir: Path, relative_path: str) -> Path:
        """
        Resolve a configured build-context path without allowing traversal.

        Arg(s):
            source_dir (Path): Build context root.
            relative_path (str): Path relative to the build context.
        Return Value(s):
            Path: Resolved path within the build context.
        """
        candidate = (source_dir / relative_path).resolve()
        if not candidate.is_relative_to(source_dir.resolve()):
            raise ValueError(f"Path is outside the build context: {relative_path}")
        return candidate

    @staticmethod
    def _find_script_stages(entries: list[dict], script_path: str) -> list[int]:
        """
        Find Dockerfile stages that invoke the configured dynamic script.

        Arg(s):
            entries (list[dict]): DockerfileParser structure entries.
            script_path (str): Script path relative to the build context.
        Return Value(s):
            list[int]: Matching zero-based Dockerfile stage numbers.
        """
        normalized_path = script_path.lstrip("./")
        script_re = re.compile(rf"(?<![\w./-])(?:\./)?{re.escape(normalized_path)}(?![\w./-])")
        stage_numbers: list[int] = []
        stage_num = -1
        for entry in entries:
            if entry["instruction"] == "FROM":
                stage_num += 1
            elif entry["instruction"] == "RUN" and stage_num >= 0 and script_re.search(entry["value"]):
                if stage_num not in stage_numbers:
                    stage_numbers.append(stage_num)
        return stage_numbers

    @staticmethod
    def _find_manifest_template(script_content: str) -> str | None:
        """
        Find the first YAML path referenced by a dynamic package script.

        Arg(s):
            script_content (str): Dynamic package script content.
        Return Value(s):
            str | None: Manifest path template, if present.
        """
        candidates = re.findall(r"['\"]([^'\"]+\.ya?ml)['\"]", script_content)
        if not candidates:
            return None
        variable_candidates = [
            candidate
            for candidate in candidates
            if "${ID}" in candidate or "$ID" in candidate or "${VERSION_ID}" in candidate or "$VERSION_ID" in candidate
        ]
        return variable_candidates[0] if variable_candidates else candidates[0]

    @staticmethod
    def _parse_os_release(content: str) -> dict[str, str]:
        """
        Parse shell-style values from an ``/etc/os-release`` file.

        Arg(s):
            content (str): File content.
        Return Value(s):
            dict[str, str]: Parsed key/value pairs.
        """
        values: dict[str, str] = {}
        for line in content.splitlines():
            match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=(?:\"([^\"]*)\"|'([^']*)'|(.*))$", line)
            if match:
                values[match.group(1)] = next(value for value in match.groups()[1:] if value is not None).strip()
        return values

    @staticmethod
    def _expand_manifest_path(template: str, values: dict[str, str]) -> str:
        """
        Substitute ``os-release`` variables in a manifest path template.

        Arg(s):
            template (str): Manifest path containing shell variables.
            values (dict[str, str]): Parsed ``os-release`` values.
        Return Value(s):
            str: Expanded manifest path.
        """
        expanded = template
        for name, value in values.items():
            expanded = expanded.replace(f"${{{name}}}", value).replace(f"${name}", value)
        if "$" in expanded:
            raise ValueError(f"Unable to expand dynamic package manifest path: {template}")
        return expanded

    @staticmethod
    def _read_package_manifest(manifest_file: Path, arches: list[str]) -> DynamicPackageSet:
        """
        Read extension packages and retain architecture restrictions.

        Arg(s):
            manifest_file (Path): YAML extension manifest.
            arches (list[str]): Target architectures.
        Return Value(s):
            DynamicPackageSet: Common and architecture-specific packages.
        """
        manifest = yaml.safe_load(manifest_file.read_text()) or {}
        extensions = manifest.get("extensions")
        if not isinstance(extensions, dict):
            raise ValueError(f"Dynamic package manifest has no extensions mapping: {manifest_file}")

        common_packages: set[str] = set()
        arch_specific_packages: dict[str, set[str]] = {}
        for extension, extension_data in extensions.items():
            if not isinstance(extension_data, dict):
                raise ValueError(f"Invalid extension definition {extension} in {manifest_file}")
            packages = extension_data.get("packages")
            if not isinstance(packages, list) or not packages:
                raise ValueError(f"No packages defined for extension {extension} in {manifest_file}")
            extension_arches = extension_data.get("architectures") or arches
            applicable_arches = [arch for arch in arches if arch in extension_arches]
            if applicable_arches == arches:
                common_packages.update(str(package) for package in packages)
            else:
                for arch in applicable_arches:
                    arch_specific_packages.setdefault(arch, set()).update(str(package) for package in packages)

        return DynamicPackageSet(
            common=sorted(common_packages),
            arch_specific={arch: sorted(packages) for arch, packages in sorted(arch_specific_packages.items())},
        )

    def _read_file_from_parent_source(self, parent_dir: Path, container_path: str) -> str:
        """
        Try to read a container file from a parent's build directory.

        Arg(s):
            parent_dir (Path): Parent image build directory.
            container_path (str): Absolute path inside the container.
        Return Value(s):
            str: File content, or an empty string if not found.
        """
        local_file = parent_dir / container_path.lstrip("/")
        if local_file.is_file():
            self._logger.info(f"Resolved {container_path} from parent source dir: {local_file}")
            return local_file.read_text()

        content = extract_generated_file_content(parent_dir, container_path)
        if content:
            self._logger.info(f"Resolved {container_path} from parent Dockerfile RUN command")
        return content


def discover_dynamic_package_script(source_dir: Path, entries: list[dict]) -> str | None:
    """
    Discover the dynamic package script invoked by a Dockerfile.

    A script is considered dynamic when it is invoked by a RUN instruction,
    exists in the build context, and references a YAML manifest containing
    ``ID`` or ``VERSION_ID`` from ``/etc/os-release``. Ambiguous builds must
    configure separate handling rather than silently choosing a script.

    Arg(s):
        source_dir (Path): Build context containing the Dockerfile and scripts.
        entries (list[dict]): DockerfileParser structure entries.
    Return Value(s):
        str | None: Detected script path relative to the build context.
    """
    dynamic_scripts: list[str] = []
    for script_path in sorted(_find_script_paths(entries)):
        try:
            script_file = DynamicPackageResolver._resolve_source_path(source_dir, script_path)
        except ValueError:
            continue
        if not script_file.is_file():
            continue
        manifest_template = DynamicPackageResolver._find_manifest_template(script_file.read_text())
        if manifest_template and any(
            variable in manifest_template for variable in ("${ID}", "$ID", "${VERSION_ID}", "$VERSION_ID")
        ):
            dynamic_scripts.append(script_path)

    if len(dynamic_scripts) > 1:
        raise ValueError(f"Multiple dynamic package scripts found: {', '.join(dynamic_scripts)}")
    return dynamic_scripts[0] if dynamic_scripts else None


def transform_dynamic_package_script(script_content: str) -> str:
    """
    Make the optional reinstall pass in a dynamic package script non-fatal.

    The extension build script first downloads packages with ``install`` and
    then repeats the operation with ``reinstall`` to fetch RPMs that are
    already installed in the base image. In a hermetic build, some packages
    are not installed in that image, so DNF can fail the second pass even
    though the required packages were downloaded by the first pass.

    Arg(s):
        script_content (str): Dynamic package script content.
    Return Value(s):
        str: Script content with the reinstall pass allowed to fail.
    """
    loop_re = re.compile(
        r"(?ms)(?P<header>for\s+subcommand\s+in\s+['\"]install['\"]\s+['\"]reinstall['\"];\s+do\n)"
        r"(?P<body>.*?)"
        r"(?P<footer>^[ \t]*done\b)"
    )
    packages_re = re.compile(r'(?m)^(?P<indent>[ \t]*)"\$\{all_packages\[@\]\}"(?P<spaces>[ \t]*)$')

    def _transform_loop(match: re.Match) -> str:
        body = match.group("body")
        if not re.search(r"\bdnf\b.*['\"]\$\{subcommand\}['\"]", body, re.DOTALL):
            return match.group(0)
        if not packages_re.search(body):
            return match.group(0)
        body = packages_re.sub(
            r'\g<indent>"${all_packages[@]}" || [ "${subcommand}" = "reinstall" ]\g<spaces>',
            body,
            count=1,
        )
        return f"{match.group('header')}{body}{match.group('footer')}"

    return loop_re.sub(_transform_loop, script_content, count=1)
