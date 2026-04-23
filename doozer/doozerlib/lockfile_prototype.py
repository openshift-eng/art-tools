"""
Alternative lockfile generator using rpm-lockfile-prototype for DNF resolution.

Uses the rpm-lockfile-prototype CLI tool to resolve RPM dependencies from
top-level Dockerfile packages, producing a lockfile without needing a
previous build's SBOM.
"""

import json
import logging
import os
import re
import shlex
from enum import Enum
from pathlib import Path

import yaml
from artcommonlib import logutil
from artcommonlib.exectools import cmd_gather_async

from doozerlib.image import ImageMetadata
from doozerlib.repos import Repos

DEFAULT_RPM_LOCKFILE_NAME = "rpms.lock.yaml"


class LockfileBackend(str, Enum):
    ART_INTERNAL = "art-internal"
    RPM_LOCKFILE_PROTOTYPE = "rpm-lockfile-prototype"


def _extract_packages_from_lines(lines: list[str]) -> list[str]:
    """
    Extract package names from yum/dnf install commands in a list of lines.

    Arg(s):
        lines (list[str]): Lines to parse (already backslash-joined).
    Return Value(s):
        list[str]: Sorted unique list of package names.
    """
    packages: set[str] = set()

    for line in lines:
        run_match = re.match(r"^\s*RUN\s+(.*)", line, re.IGNORECASE)
        if not run_match:
            continue

        run_body = run_match.group(1)
        commands = re.split(r"&&|;", run_body)

        for cmd in commands:
            cmd = cmd.strip()
            install_match = re.match(r"(yum|dnf)\s+.*?\binstall\b(.*)$", cmd, re.IGNORECASE)
            if not install_match:
                continue

            args_str = install_match.group(2).strip()
            tokens = shlex.split(args_str)

            for token in tokens:
                if token.startswith("-"):
                    continue
                if token in ("&&", "||", ";", "|"):
                    break
                packages.add(token)

    return sorted(packages)


def extract_dockerfile_packages_per_stage(dockerfile_path: Path) -> list[list[str]]:
    """
    Extract package names from yum/dnf install commands, grouped by
    Dockerfile stage. Each element in the returned list corresponds to
    one FROM stage (0-indexed).

    Arg(s):
        dockerfile_path (Path): Path to the Dockerfile.
    Return Value(s):
        list[list[str]]: Per-stage sorted unique package lists.
    """
    content = dockerfile_path.read_text()
    content = re.sub(r"\\\n\s*", " ", content)

    stages: list[list[str]] = []
    current_stage_lines: list[str] = []
    seen_from = False

    for line in content.splitlines():
        if re.match(r"^\s*FROM\s+", line, re.IGNORECASE):
            if seen_from and current_stage_lines:
                stages.append(_extract_packages_from_lines(current_stage_lines))
            seen_from = True
            current_stage_lines = [line]
        elif seen_from:
            current_stage_lines.append(line)

    if current_stage_lines:
        stages.append(_extract_packages_from_lines(current_stage_lines))

    return stages


def build_rpms_in_yaml(
    repos: list[dict],
    arches: list[str],
    packages: list[str],
    arch_specific_packages: dict[str, list[str]] | None = None,
) -> dict:
    """
    Build the rpms.in.yaml structure for rpm-lockfile-prototype.

    Arg(s):
        repos (list[dict]): Repo dicts with "name" and "baseurl" keys.
        arches (list[str]): Target architectures.
        packages (list[str]): Common package names for all arches.
        arch_specific_packages (dict[str, list[str]] | None): Per-arch packages.
    Return Value(s):
        dict: rpms.in.yaml structure ready for YAML serialization.
    """
    repo_entries = [{"repoid": repo["name"], "baseurl": repo["baseurl"]} for repo in repos]

    package_entries: list[str | dict] = list(packages)
    if arch_specific_packages:
        for arch, arch_pkgs in arch_specific_packages.items():
            for pkg in arch_pkgs:
                if pkg not in packages:
                    package_entries.append(
                        {
                            "name": pkg,
                            "arches": {"only": arch},
                        }
                    )

    return {
        "arches": arches,
        "contentOrigin": {"repos": repo_entries},
        "packages": package_entries,
    }


class RpmLockfilePrototypeGenerator:
    """
    Generates RPM lockfiles using rpm-lockfile-prototype as the DNF
    resolution engine. Produces an ephemeral rpms.in.yaml from image
    config and Dockerfile, delegates resolution to the external tool.
    """

    def __init__(
        self,
        repos: Repos,
        runtime=None,
        logger: logging.Logger | None = None,
    ):
        self.repos = repos
        self.runtime = runtime
        self.downstream_parents: list[str] = []
        self.logger = logger or logutil.get_logger(__name__)

    def _build_env(self) -> dict:
        """
        Build environment for subprocess with registry auth if available.
        """
        env = os.environ.copy()
        registry_auth = os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE")
        if registry_auth:
            env["REGISTRY_AUTH_FILE"] = registry_auth
        return env

    async def _resolve_to_digest(self, pullspec: str) -> str:
        """
        If pullspec uses a tag, resolve it to a digest via skopeo inspect.
        Returns the pullspec with @sha256:... instead of :tag.
        If already a digest, returns as-is.
        """
        if "@sha256:" in pullspec:
            return pullspec

        env = self._build_env()
        cmd = ["skopeo", "inspect", "--no-tags", f"docker://{pullspec}"]
        self.logger.info(f"Resolving tag to digest: {pullspec}")

        rc, stdout, stderr = await cmd_gather_async(cmd, check=False, env=env)

        if rc != 0:
            self.logger.warning(f"Failed to resolve digest for {pullspec}, using tag: {stderr[:200]}")
            return pullspec

        digest = json.loads(stdout).get("Digest", "")
        if not digest:
            self.logger.warning(f"No digest found for {pullspec}, using tag")
            return pullspec

        repo = pullspec.rsplit(":", 1)[0]
        resolved = f"{repo}@{digest}"
        self.logger.info(f"Resolved to: {resolved}")
        return resolved

    def _build_repo_list(self, enabled_repos: set[str], arches: list[str]) -> list[dict]:
        """
        Build repo list from Repos object for rpms.in.yaml.
        """
        repo_list = []
        for repo_name in sorted(enabled_repos):
            try:
                repo = self.repos[repo_name]
            except ValueError:
                continue
            first_arch = arches[0]
            baseurl = repo.baseurl(repotype="unsigned", arch=first_arch)
            baseurl_template = baseurl.replace(f"/{first_arch}/", "/$basearch/")
            repo_list.append({"name": repo.name, "baseurl": baseurl_template})
        return repo_list

    async def _run_prototype(
        self,
        in_file_path: Path,
        out_file_path: Path,
        image_pullspec: str | None = None,
        distgit_key: str = "",
    ) -> None:
        """
        Run rpm-lockfile-prototype for a single invocation.
        """
        cmd = ["rpm-lockfile-prototype"]
        if image_pullspec:
            cmd.extend(["--image", image_pullspec])
        else:
            cmd.append("--bare")
        cmd.extend(["--outfile", str(out_file_path), str(in_file_path)])

        env = self._build_env()
        self.logger.info(f"{distgit_key}: running {' '.join(cmd)}")

        rc, _, stderr = await cmd_gather_async(cmd, check=False, env=env)

        if rc != 0:
            raise RuntimeError(f"rpm-lockfile-prototype failed for {distgit_key} (exit code {rc}): {stderr}")

    async def generate_lockfile(
        self,
        image_meta: ImageMetadata,
        dest_dir: Path,
        filename: str = DEFAULT_RPM_LOCKFILE_NAME,
    ) -> None:
        """
        Generate an RPM lockfile using rpm-lockfile-prototype.

        For multi-stage Dockerfiles, runs the tool once per stage with the
        correct stageNum so each stage resolves against its own base image
        rpmdb. Results are merged into a single lockfile.

        Arg(s):
            image_meta (ImageMetadata): Image metadata with repo/arch config.
            dest_dir (Path): Directory containing the Dockerfile and where
                the lockfile will be written.
            filename (str): Output lockfile name.
        """
        if not image_meta.is_lockfile_generation_enabled():
            self.logger.debug(f"Lockfile generation disabled for {image_meta.distgit_key}")
            return

        enabled_repos = image_meta.get_enabled_repos()
        if not enabled_repos:
            self.logger.info(f"No enabled repos for {image_meta.distgit_key}, skipping")
            return

        arches = image_meta.get_arches()
        repo_list = self._build_repo_list(enabled_repos, arches)
        distgit_key = image_meta.distgit_key

        # Get config RPMs
        config_rpms = []
        lockfile_rpms = image_meta.config.konflux.cachi2.lockfile.get("rpms", [])
        if lockfile_rpms is not None:
            try:
                config_rpms = list(lockfile_rpms)
            except TypeError:
                config_rpms = []

        # Parse Dockerfile for per-stage packages
        dockerfile_path = dest_dir / "Dockerfile"
        if not dockerfile_path.exists():
            all_packages = sorted(set(config_rpms))
            if not all_packages:
                self.logger.warning(f"{distgit_key}: no Dockerfile and no lockfile config rpms, skipping")
                return
            # No Dockerfile — single run with --bare
            in_yaml = build_rpms_in_yaml(repo_list, arches, all_packages)
            in_file_path = dest_dir / "rpms.in.yaml"
            out_file_path = dest_dir / filename
            try:
                with open(in_file_path, "w") as f:
                    yaml.safe_dump(in_yaml, f, sort_keys=False)
                await self._run_prototype(in_file_path, out_file_path, distgit_key=distgit_key)
                self.logger.info(f"{distgit_key}: lockfile written to {out_file_path}")
            finally:
                if in_file_path.exists():
                    in_file_path.unlink()
            return

        stages_packages = extract_dockerfile_packages_per_stage(dockerfile_path)
        # Add config rpms to the last stage
        if config_rpms and stages_packages:
            stages_packages[-1] = sorted(set(stages_packages[-1] + config_rpms))

        # Filter to stages that actually install packages
        stages_with_packages = [(stage_num, pkgs) for stage_num, pkgs in enumerate(stages_packages) if pkgs]

        if not stages_with_packages:
            self.logger.warning(f"{distgit_key}: no packages found in any Dockerfile stage or config, skipping")
            return

        self.logger.info(
            f"{distgit_key}: found {len(stages_with_packages)} stage(s) with packages: "
            + ", ".join(f"stage {n}: {pkgs}" for n, pkgs in stages_with_packages)
        )

        out_file_path = dest_dir / filename
        stage_lockfiles: list[dict] = []

        for stage_num, packages in stages_with_packages:
            image_pullspec = self.downstream_parents[stage_num] if stage_num < len(self.downstream_parents) else None
            if image_pullspec:
                image_pullspec = await self._resolve_to_digest(image_pullspec)
            self.logger.info(
                f"{distgit_key}: resolving stage {stage_num} "
                f"({len(packages)} packages, image={image_pullspec or 'bare'})"
            )

            in_yaml = build_rpms_in_yaml(repo_list, arches, packages)
            in_file_path = dest_dir / f"rpms.stage{stage_num}.in.yaml"
            stage_out_path = dest_dir / f"rpms.stage{stage_num}.lock.yaml"

            try:
                with open(in_file_path, "w") as f:
                    yaml.safe_dump(in_yaml, f, sort_keys=False)

                await self._run_prototype(
                    in_file_path, stage_out_path, image_pullspec=image_pullspec, distgit_key=distgit_key
                )

                with open(stage_out_path) as f:
                    stage_lockfiles.append(yaml.safe_load(f))

            finally:
                if in_file_path.exists():
                    in_file_path.unlink()
                if stage_out_path.exists():
                    stage_out_path.unlink()

        if len(stage_lockfiles) == 1:
            with open(out_file_path, "w") as f:
                yaml.safe_dump(stage_lockfiles[0], f, sort_keys=False)
        else:
            merged = self._merge_lockfiles(stage_lockfiles)
            with open(out_file_path, "w") as f:
                yaml.safe_dump(merged, f, sort_keys=False)

        self.logger.info(f"{distgit_key}: lockfile written to {out_file_path}")

    @staticmethod
    def _merge_lockfiles(lockfiles: list[dict]) -> dict:
        """
        Merge multiple stage lockfiles into one, deduplicating packages.
        """
        if len(lockfiles) == 1:
            return lockfiles[0]

        merged = {
            "lockfileVersion": 1,
            "lockfileVendor": "redhat",
            "arches": [],
        }

        # Collect all arches across all lockfiles
        all_arches: dict[str, dict[str, dict]] = {}
        for lockfile in lockfiles:
            for arch_entry in lockfile.get("arches", []):
                arch = arch_entry["arch"]
                if arch not in all_arches:
                    all_arches[arch] = {}
                for pkg in arch_entry.get("packages", []):
                    key = pkg["name"]
                    if key not in all_arches[arch]:
                        all_arches[arch][key] = pkg

        for arch in sorted(all_arches.keys()):
            packages = sorted(all_arches[arch].values(), key=lambda p: p["name"])
            merged["arches"].append(
                {
                    "arch": arch,
                    "packages": packages,
                    "module_metadata": [],
                }
            )

        return merged
