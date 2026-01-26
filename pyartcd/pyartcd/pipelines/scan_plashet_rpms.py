"""
This pipeline scans configured Brew tags for RPM changes and triggers plashet builds when changes are detected.
"""

import asyncio
import string
from datetime import datetime
from typing import Optional

import click
import httpx
import koji
import yaml
from artcommonlib.config.plashet import PlashetConfig
from artcommonlib.config.repo import Repo, RepoList
from artcommonlib.constants import BREW_HUB

from pyartcd import constants, jenkins, locks, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime


class ScanPlashetRpmsPipeline:
    def __init__(
        self,
        runtime: Runtime,
        group: str,
        data_path: str = constants.OCP_BUILD_DATA_URL,
        data_gitref: str = "",
        assembly: str = "stream",
        repos: Optional[list[str]] = None,
    ):
        self.runtime = runtime
        self.group = group
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.assembly = assembly
        self.repos = repos
        self.logger = runtime.logger

        self.repos_to_rebuild = []
        self.rebuild_reasons = {}

    async def run(self):
        """
        Main pipeline execution:
        1. Load group config and get plashet repo configurations
        2. For each plashet repo, check for changes
        3. Trigger plashet build if changes detected
        """
        # Load group configuration
        self.logger.info(f"Loading group configuration for {self.group}")
        group_config = await util.load_group_config(
            group=self.group,
            assembly=self.assembly,
            doozer_data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )

        # Get PlashetConfig first to fail quickly if not configured
        plashet_config_dict = group_config.get("plashet", {})
        if not plashet_config_dict:
            self.logger.error("No plashet configuration found in group config")
            return
        plashet_config = PlashetConfig.model_validate(plashet_config_dict)

        # Get plashet repos (new-style only)
        plashet_repos = self._get_plashet_repos(group_config)
        if not plashet_repos:
            self.logger.info("No plashet repos configured for this group")
            return

        # Get group arches
        group_arches = group_config.get("arches", [])

        # Initialize Koji client
        koji_api = koji.ClientSession(BREW_HUB)
        koji_api.gssapi_login()

        # Scan each plashet repo concurrently
        self.logger.info(f"Scanning {len(plashet_repos)} plashet repos for changes...")
        scan_tasks = [
            self._scan_repo(
                repo=repo,
                plashet_config=plashet_config,
                group_arches=group_arches,
                koji_api=koji_api,
            )
            for repo in plashet_repos
        ]
        await asyncio.gather(*scan_tasks)

        # Trigger build if needed
        if self.repos_to_rebuild:
            self.logger.info(f"Found {len(self.repos_to_rebuild)} repos that need rebuilding:")
            for repo_name in self.repos_to_rebuild:
                self.logger.info(f"  - {repo_name}: {self.rebuild_reasons[repo_name]}")

            await self._trigger_plashet_build()
        else:
            self.logger.info("No changes detected in any plashet repos")

    def _get_plashet_repos(self, group_config: dict) -> list[Repo]:
        """
        Extract plashet repo configurations from group config (new-style only).
        Returns a list of Repo objects with type=='plashet'.
        If self.repos is set, only returns repos with names in that list.
        """
        if "all_repos" not in group_config:
            self.logger.warning("Group config does not have all_repos (new-style config required)")
            return []

        self.logger.info("Using new-style plashet configs")
        all_repos = group_config["all_repos"]
        repos = [
            repo for repo in RepoList.model_validate(all_repos).root if not repo.disabled and repo.type == "plashet"
        ]

        # Filter by repo names if specified
        if self.repos:
            repos = [repo for repo in repos if repo.name in self.repos]
            self.logger.info(f"Filtering to repos: {', '.join(self.repos)}")

        self.logger.info(f"Found {len(repos)} plashet repos")
        for repo in repos:
            self.logger.debug(f"  {repo.name}")

        return repos

    def _get_configured_arches(self, repo: Repo, plashet_config: PlashetConfig, group_arches: list) -> list[str]:
        """
        Get configured arches for a plashet repo using fallback chain:
        repo.plashet.arches -> PlashetConfig.arches -> group_config.arches
        """
        if repo.plashet and repo.plashet.arches:
            return repo.plashet.arches
        if plashet_config.arches:
            return plashet_config.arches
        return group_arches

    async def _scan_repo(
        self,
        repo: Repo,
        plashet_config: PlashetConfig,
        group_arches: list,
        koji_api,
    ):
        """
        Scan a single plashet repo for changes.
        Marks repo for rebuild if needed.
        """
        repo_name = repo.name
        self.logger.info(f"Scanning repo: {repo_name}")

        # Get configured arches for this repo
        configured_arches = self._get_configured_arches(repo, plashet_config, group_arches)
        self.logger.debug(f"  Configured arches: {configured_arches}")

        # Get repo slug
        slug = repo.plashet.slug or repo_name

        # Fetch plashet.yml
        try:
            plashet_data = await self._fetch_plashet_yml(plashet_config, slug, timeout=10)
        except httpx.HTTPError as e:
            # Network/connection errors and HTTP errors - don't rebuild
            self.logger.warning(f"  Error fetching plashet.yml, skipping rebuild: {e}")
            return

        if plashet_data is None:
            # First time - plashet doesn't exist yet
            self.logger.info("  → First build (plashet.yml not found)")
            self.repos_to_rebuild.append(repo_name)
            self.rebuild_reasons[repo_name] = "first build (plashet.yml not found)"
            return

        # Extract data from plashet.yml
        assemble = plashet_data.get("assemble", {})
        plashet_arches = assemble.get("arches")

        # Check 1: Arches changed? (only if arches field exists in plashet.yml)
        if plashet_arches is not None:
            if set(plashet_arches) != set(configured_arches):
                self.logger.info(f"  → Arches changed: {plashet_arches} → {configured_arches}")
                self.repos_to_rebuild.append(repo_name)
                self.rebuild_reasons[repo_name] = f"arches changed: {plashet_arches} → {configured_arches}"
                return
        else:
            self.logger.debug("  No arches field in plashet.yml, skipping arches check")

        # Check 2: Tag changes?
        plashet_event_id = assemble.get("brew_event", {}).get("id", 0)
        tags_changed = []

        # Collect tag names
        tag_names = [brew_tag.name for brew_tag in repo.plashet.source.from_tags]
        if not tag_names:
            self.logger.debug("  No tags to check")
        else:
            self.logger.debug(f"  Checking {len(tag_names)} tags: {', '.join(tag_names)}")

            # Use multicall to query all tags concurrently
            # Only fetch the most recent event to minimize data transfer
            try:
                with koji_api.multicall(strict=True) as m:
                    tasks = [
                        m.queryHistory(
                            table="tag_listing",
                            tag=tag_name,
                            afterEvent=plashet_event_id,
                            queryOpts={"limit": 1, "order": "-create_event"},
                        )
                        for tag_name in tag_names
                    ]
                histories = [task.result for task in tasks]
            except Exception as e:
                self.logger.error(
                    f"  Error querying tag history for repo {repo_name} (tags: {', '.join(tag_names)}): {e}"
                )
                return

            # Process results
            for tag_name, history in zip(tag_names, histories):
                tag_listing = history.get("tag_listing", [])

                if not tag_listing:
                    self.logger.debug(f"    No tag history found for {tag_name} after event {plashet_event_id}")
                    continue

                # If any entries exist in tag_listing, the tag has changed
                # We only fetch the most recent event, so tag_listing has at most 1 entry
                latest_event_id = tag_listing[0]["create_event"]
                self.logger.info(
                    f"    → Tag {tag_name} has changed (most recent event: {latest_event_id}, after {plashet_event_id})"
                )
                tags_changed.append(tag_name)

        if tags_changed:
            self.logger.info(f"  → Tags changed: {', '.join(tags_changed)}")
            self.repos_to_rebuild.append(repo_name)
            self.rebuild_reasons[repo_name] = f"tags changed: {', '.join(tags_changed)}"
        else:
            self.logger.info("  → No changes detected")

    async def _fetch_plashet_yml(self, plashet_config: PlashetConfig, slug: str, timeout: int = 30) -> Optional[dict]:
        """
        Construct plashet.yml URL and fetch it.
        Returns None if not found (404).

        Args:
            plashet_config: PlashetConfig to construct URL from
            slug: Repo slug for template substitution
            timeout: Timeout in seconds (default: 30)
        """
        # Construct URL to plashet.yml
        # Use base_url if provided, otherwise extract from download_url
        if plashet_config.base_url:
            base_url = plashet_config.base_url.rstrip("/")
        else:
            # Fall back to extracting from download_url
            # Default base URL for ocp-artifacts
            base_url = "https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets"

        # Variables for template substitution (only $-style variables)
        vars_dict = {
            "runtime_assembly": self.assembly,
            "slug": slug,
        }

        # Substitute variables in base_dir
        base_dir_template = string.Template(plashet_config.base_dir)
        base_dir = base_dir_template.substitute(vars_dict)

        # Construct the full URL
        symlink_name = plashet_config.symlink_name
        url = f"{base_url}/{base_dir}/{symlink_name}/plashet.yml"

        self.logger.debug(f"  Fetching plashet.yml from: {url}")

        # Fetch plashet.yml
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url)

            if response.status_code == 404:
                return None

            response.raise_for_status()
            content = response.text
            data = yaml.safe_load(content)
            return data

    async def _trigger_plashet_build(self):
        """
        Trigger a plashet build job for repos that need rebuilding.
        """
        # Generate release timestamp
        release = datetime.now().strftime("%Y%m%d%H%M") + ".p?"

        self.logger.info(f"Triggering build-plashets for {self.group} with release {release}")
        self.logger.info(f"Repos to rebuild: {', '.join(self.repos_to_rebuild)}")

        try:
            jenkins.init_jenkins()
            jenkins.start_build_plashets(
                group=self.group,
                release=release,
                assembly=self.assembly,
                repos=self.repos_to_rebuild,
                data_path=self.data_path if self.data_path != constants.OCP_BUILD_DATA_URL else "",
                data_gitref=self.data_gitref,
                dry_run=self.runtime.dry_run,
            )
            self.logger.info("Successfully triggered build-plashets job")

        except Exception as e:
            self.logger.error(f"Failed to trigger build-plashets: {e}")
            raise


@cli.command("scan-plashet-rpms")
@click.option("--group", required=True, help="OCP group to scan, e.g. openshift-4.17")
@click.option(
    "--data-path",
    required=False,
    default=constants.OCP_BUILD_DATA_URL,
    help="ocp-build-data fork to use",
)
@click.option("--data-gitref", required=False, default="", help="Doozer data path git [branch / tag / sha] to use")
@click.option("--assembly", required=False, default="stream", help="Assembly to scan (default: stream)")
@click.option(
    "--repos",
    multiple=True,
    help="Limit repos to check (can specify multiple times). If not provided, scan all repos",
)
@pass_runtime
@click_coroutine
async def scan_plashet_rpms_cli(
    runtime: Runtime,
    group: str,
    data_path: str,
    data_gitref: str,
    assembly: str,
    repos: tuple[str, ...],
):
    """
    Scan configured Brew tags for RPM changes and trigger plashet builds when changes are detected.

    This pipeline:
    1. Loads the group configuration to identify plashet repos and their Brew tags
    2. Compares current Brew tag state with the last build (from plashet.yml)
    3. Checks if arches configuration has changed
    4. Triggers a build-plashets job if changes are detected

    A plashet repo will be rebuilt if:
    - plashet.yml doesn't exist yet (first build)
    - Arches configuration has changed (if arches field exists in plashet.yml)
    - Any configured Brew tag has new tag events

    Example:
        artcd scan-plashet-rpms --group openshift-4.17
        artcd scan-plashet-rpms --group openshift-4.18 --assembly 4.18.1
        artcd scan-plashet-rpms --group openshift-4.17 --repos el9 --repos rhel-rhcos
    """
    pipeline = ScanPlashetRpmsPipeline(
        runtime=runtime,
        group=group,
        data_path=data_path,
        data_gitref=data_gitref,
        assembly=assembly,
        repos=list(repos) if repos else None,
    )

    # Acquire lock for this group/assembly combination (unless dry run)
    if runtime.dry_run:
        await pipeline.run()
    else:
        lock_name = locks.Lock.SCAN_PLASHET_RPMS.value.format(assembly=assembly, group=group)
        await locks.run_with_lock(
            coro=pipeline.run(),
            lock=locks.Lock.SCAN_PLASHET_RPMS,
            lock_name=lock_name,
        )

    if pipeline.repos_to_rebuild:
        runtime.logger.info(
            f"✓ Detected changes in {len(pipeline.repos_to_rebuild)} plashet repos and triggered rebuild"
        )
    else:
        runtime.logger.info("✓ No changes detected in any plashet repos")
