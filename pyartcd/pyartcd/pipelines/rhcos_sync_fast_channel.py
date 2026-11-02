"""
Fast-channel-driven RHCOS boot image sync for z-stream releases (ART-25059).

Background
----------
For EC/RC/GA(.0) assemblies, the promote pipeline triggers a full RHCOS sync
(``jenkins.start_rhcos_sync(..., sign_only=False)``) which runs a "needsHappening"
check (compare ``rhcos-id.txt`` on mirror.openshift.com against the build id in the
release payload) and only mirrors boot images when they changed.

For z-stream assemblies, promote always calls ``sign_only=True``, which skips
boot-image mirroring (and the needsHappening check) entirely. This assumed boot
images rarely change within a z-stream. In practice they occasionally do
(bootimage bumps), and today publishing them for a z-stream requires a manual Jira
request (recurring template ART-4163).

This pipeline extends the ``rhcos-id.txt``-based auto-discovery ("needsHappening")
check to z-stream releases so that a boot-image bump in a z-stream gets picked up
and published automatically — WITHOUT a manual Jira request.

Design
------
This is intentionally DECOUPLED from promote-time. It is meant to run as an
independent periodic/scheduled pipeline (one invocation per y-stream), NOT inside
any single promote run. Per y-stream, for each architecture it:

  1. Queries the Cincinnati **fast** channel (``fast-{major}.{minor}``) to find the
     release end users are now receiving.
  2. Picks the newest (highest semver) release node in that y-stream and builds the
     stable release tag ``{version}-{brew_arch}`` that ``artcd rhcos-sync`` expects.
  3. Reuses ``RhcosSyncPipeline`` to extract the RHCOS build id referenced by that
     release and compare it against the ``rhcos-id.txt`` currently published on
     mirror.openshift.com (``RhcosSyncPipeline.check_needs_happening()``).
  4. If the id differs, triggers the boot-image sync/publish by reusing the existing
     ``artcd rhcos-sync`` path (``jenkins.start_rhcos_sync(release_tag, dry_run,
     sign_only=False)``). If it matches, it is a no-op.

All relevant arches are triggered concurrently and the pipeline BLOCKS until every
downstream ``rhcos_sync`` build it started has completed, then exits success only if all
of them succeeded. This lets the scheduled dispatcher (which triggers this worker per
y-stream with a waiting ``build()``) serialize naturally: one y-stream's boot-image sync
fully finishes before the next y-stream is evaluated.

NOTE: The Jenkins job definition and its cron schedule live in a SEPARATE repo
(``aos-cd-jobs``), not here. Wiring the scheduled job there is a required follow-up.
"""

import asyncio
from dataclasses import dataclass
from typing import List, Optional

import click
from artcommonlib.arch_util import GO_ARCHES, brew_arch_for_go_arch, go_arch_for_brew_arch
from semver import VersionInfo

from pyartcd import constants, jenkins, util
from pyartcd.cincinnati import CincinnatiAPI
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.pipelines.rhcos_sync import RhcosSyncPipeline, extract_major_minor
from pyartcd.runtime import Runtime

# "multi" is a manifest-list pseudo-arch, not a real RHCOS boot-image arch, so it is
# never iterated by the per-arch fast-channel scan.
DEFAULT_GO_ARCHES = [arch for arch in GO_ARCHES if arch != "multi"]


@dataclass(frozen=True)
class _ArchSyncOutcome:
    """Result of processing a single architecture in :meth:`RhcosSyncFastChannelPipeline.run`.

    Attributes:
        arch: GO arch that was processed (e.g. ``amd64``).
        tag: Release tag that was (or, in dry-run, would be) synced; ``None`` when no sync
            was needed (no fast-channel nodes for the arch, or the mirror is already current).
        triggered: ``True`` only when a real (non-dry-run) downstream ``RHCOS_SYNC`` build
            was actually triggered and awaited to completion.
        result: Downstream Jenkins build result string (e.g. ``'SUCCESS'``/``'FAILURE'``);
            ``None`` in dry-run or when nothing was triggered. This string is safe to log.
        url: Downstream build URL; ``None`` in dry-run or when nothing was triggered.
    """

    arch: str
    tag: Optional[str]
    triggered: bool
    result: Optional[str]
    url: Optional[str]


class RhcosSyncFastChannelPipeline:
    """Per-y-stream periodic scan that auto-publishes RHCOS boot-image bumps.

    For each architecture it queries the Cincinnati fast channel, finds the newest
    z-stream release, and reuses ``RhcosSyncPipeline`` to decide whether the boot
    images on mirror.openshift.com are stale. If so, it triggers the existing
    ``RHCOS_SYNC`` Jenkins job with ``sign_only=False``.
    """

    def __init__(
        self,
        runtime: Runtime,
        version: str,
        arches: Optional[List[str]] = None,
        force: bool = False,
        data_path: str = constants.OCP_BUILD_DATA_URL,
        data_gitref: str = "",
    ):
        """Initialize the scan for a single y-stream.

        Args:
            runtime: The pyartcd runtime (provides the logger and ``dry_run`` flag).
            version: OCP y-stream version ``major.minor`` (e.g. ``4.19``); normalized here.
            arches: Explicit GO arches to scan (an override). When None/empty, the arch set
                is derived per y-stream from ocp-build-data ``group.yml`` at ``run()`` time.
            force: Trigger a sync even if the mirror appears current; also forwarded to the
                downstream ``RHCOS_SYNC`` job's ``FORCE`` parameter.
            data_path: ocp-build-data repository URL used to resolve the group's arches.
            data_gitref: Optional ocp-build-data git ref (branch/tag/sha).
        """
        self.runtime = runtime
        self.version = version  # y-stream "major.minor", e.g. "4.19"
        self.force = force
        self.data_path = data_path
        self.data_gitref = data_gitref
        self.logger = runtime.logger

        # Validate / normalize the y-stream version early.
        self.major, self.minor = extract_major_minor(version)
        # Normalize to exactly "major.minor" so the fast channel name is well-formed.
        self.version = f"{self.major}.{self.minor}"

        # Explicit --arch override (GO arches). None means "derive from group.yml in run()".
        self.arches: Optional[List[str]] = list(arches) if arches else None

        self.channel = f"fast-{self.version}"

    async def _resolve_arches(self) -> List[str]:
        """Resolve the GO arches to scan for this y-stream.

        If an explicit ``--arch`` override was given, it is used verbatim. Otherwise the
        supported arches are read per y-stream from ocp-build-data ``group.yml`` via
        ``util.branch_arches`` (which returns BREW arches); those are translated to GO arches
        for the Cincinnati fast-channel query. The ``multi`` pseudo-arch is never scanned.

        Returns:
            The list of GO arches to scan.
        """
        if self.arches:
            return self.arches

        brew_arches = await util.branch_arches(
            group=f"openshift-{self.version}",
            assembly="stream",
            data_path=self.data_path,
            doozer_data_gitref=self.data_gitref,
        )
        # group.yml lists BREW arches; translate to GO arches for the Cincinnati query and
        # drop the "multi" manifest-list pseudo-arch (not a real boot-image arch).
        go_arches = [go_arch_for_brew_arch(a) for a in brew_arches if a != "multi"]
        self.logger.info("Arches resolved from group.yml for %s: %s", self.version, ", ".join(go_arches))
        return go_arches

    async def run(self):
        """Scan every requested architecture's fast channel and publish any boot-image bumps.

        All arches are processed **concurrently** via :func:`asyncio.gather`. For a real
        (non-dry-run) run, each arch that needs a sync triggers the downstream
        ``RHCOS_SYNC`` Jenkins build AND blocks until that build completes (see
        :meth:`_handle_arch`), so this method does not return until *every* triggered build
        has finished. A failure in one arch (either the scan raising, or the downstream
        build finishing non-``SUCCESS``) does not abort the others: all outcomes are
        collected first, then an aggregate ``RuntimeError`` is raised if any arch failed.

        Dry-run triggers (and therefore waits for) nothing; it only reports what it would do.

        Raises:
            RuntimeError: If one or more architectures failed to process, or a downstream
                build did not succeed.
        """
        arches = await self._resolve_arches()
        self.logger.info(
            "Scanning fast channel %s for RHCOS boot-image bumps (arches=%s, force=%s, dry_run=%s)",
            self.channel,
            ", ".join(arches),
            self.force,
            self.runtime.dry_run,
        )

        # Process every arch concurrently and wait for ALL of them (including each arch's
        # downstream build, which _handle_arch blocks on). return_exceptions keeps one arch's
        # failure from cancelling the siblings mid-flight.
        results = await asyncio.gather(*(self._handle_arch(go_arch) for go_arch in arches), return_exceptions=True)

        outcomes: List[_ArchSyncOutcome] = []
        failures: List[str] = []
        for go_arch, res in zip(arches, results):
            if isinstance(res, Exception):
                # SECURITY: never surface the raw exception text (oc/kubeconfig stderr can
                # carry internal hostnames or credentials) in either the log line OR the
                # aggregate error message. Record only arch, y-stream and the exception type.
                self.logger.error("Failed to process arch %s for %s (%s)", go_arch, self.version, type(res).__name__)
                failures.append(f"{go_arch} ({self.version}): {type(res).__name__}")
                continue

            outcomes.append(res)
            if res.triggered and res.result != "SUCCESS":
                # The Jenkins result string (e.g. 'FAILURE', 'ABORTED') is safe to surface.
                self.logger.error(
                    "Downstream rhcos-sync for arch %s (%s) did not succeed: %s",
                    go_arch,
                    self.version,
                    res.result,
                )
                failures.append(f"{go_arch} ({self.version}): {res.result}")

        triggered_tags = [o.tag for o in outcomes if o.tag]
        if triggered_tags:
            # Dry-run never actually triggers Jenkins, so the summary must say "would".
            if self.runtime.dry_run:
                self.logger.info("[DRY-RUN] Would trigger RHCOS fast-channel sync for: %s", ", ".join(triggered_tags))
            else:
                self.logger.info("RHCOS fast-channel sync triggered for: %s", ", ".join(triggered_tags))
        else:
            self.logger.info("No RHCOS boot-image bumps detected for %s", self.version)

        # Per-arch downstream build summary (dry-run aware).
        for o in outcomes:
            if not o.tag:
                continue
            if self.runtime.dry_run:
                self.logger.info("[DRY-RUN] Would sync arch=%s tag=%s", o.arch, o.tag)
            else:
                self.logger.info(
                    "Downstream rhcos-sync arch=%s tag=%s result=%s url=%s", o.arch, o.tag, o.result, o.url
                )

        if failures:
            raise RuntimeError(f"RHCOS fast-channel scan failed for {len(failures)} arch(es): " + "; ".join(failures))

    async def _handle_arch(self, go_arch: str) -> _ArchSyncOutcome:
        """Process a single architecture.

        Finds the newest fast-channel z-stream release for ``go_arch``, runs the
        needsHappening check, and (outside dry-run) triggers the ``RHCOS_SYNC`` job when
        the boot images on mirror.openshift.com are stale — then **blocks until that
        downstream build completes**, capturing its result and URL.

        Args:
            go_arch: GO architecture to scan (e.g. ``amd64``, ``arm64``).

        Returns:
            An :class:`_ArchSyncOutcome`. ``tag`` is ``None`` when there are no nodes for
            this arch or the mirror is already current (no sync needed). Otherwise ``tag``
            is the release tag; for a real (non-dry-run) run ``triggered`` is ``True`` and
            the downstream build has already finished, so ``result`` (the Jenkins result
            string) and ``url`` are populated. In dry-run, ``tag`` is set but ``triggered``
            is ``False`` and nothing was actually triggered.
        """
        release_version = await self._latest_fast_channel_version(go_arch)
        if not release_version:
            self.logger.info("No %s nodes in channel %s; skipping arch", go_arch, self.channel)
            return _ArchSyncOutcome(arch=go_arch, tag=None, triggered=False, result=None, url=None)

        # ocp-release quay tags use the brew-arch suffix (e.g. 4.19.54-x86_64,
        # 4.19.54-aarch64). parse_release_tag() in rhcos_sync.py parses that brew arch
        # back out of the tag, so build the stable release tag with the brew arch.
        brew_arch = brew_arch_for_go_arch(go_arch)
        release_tag = f"{release_version}-{brew_arch}"
        self.logger.info("Latest %s release in %s: %s (tag=%s)", go_arch, self.channel, release_version, release_tag)

        # Reuse RhcosSyncPipeline's preamble + needsHappening check (no sync/sign here).
        rsp = RhcosSyncPipeline(runtime=self.runtime, release_tag=release_tag, force=self.force)
        needs_happening = await rsp.check_needs_happening()

        if not needs_happening:
            self.logger.info("RHCOS boot images already current on mirror for %s; no-op", release_tag)
            return _ArchSyncOutcome(arch=go_arch, tag=None, triggered=False, result=None, url=None)

        if self.runtime.dry_run:
            self.logger.info("[DRY-RUN] Would trigger rhcos-sync (sign_only=False) for %s", release_tag)
            return _ArchSyncOutcome(arch=go_arch, tag=release_tag, triggered=False, result=None, url=None)

        # Initialize Jenkins lazily: only when a real trigger is about to happen. Doing it
        # up front would require Jenkins service-account creds even for a dry-run scan.
        #
        # start_build / start_rhcos_sync are SYNCHRONOUS (blocking) calls. Run each in a
        # worker thread via asyncio.to_thread so run()'s asyncio.gather can drive all arches
        # concurrently while we still wait for every triggered build to finish.
        #   - block_until_building=True (start_build's default) keeps this child build
        #     backlinked to the parent job.
        #   - block_until_complete=True makes the call wait for the build to finish and
        #     return its result string.
        #   - return_build_url=True also returns the build URL for the summary.
        #   - self.force is forwarded to the downstream RHCOS_SYNC job's FORCE parameter so
        #     the force semantics hold end-to-end (the downstream job runs its own
        #     needsHappening check otherwise).
        #   - propagate_params forwards ART_TOOLS_COMMIT/PLR_TEMPLATE_COMMIT from this
        #     orchestrator so the downstream rhcos_sync build runs the same art-tools / PLR
        #     template revision (important for testing a PR end-to-end).
        self.logger.info(
            "Triggering rhcos-sync (sign_only=False, force=%s) and waiting for %s", self.force, release_tag
        )
        jenkins.init_jenkins()
        result, url = await asyncio.to_thread(
            jenkins.start_rhcos_sync,
            release_tag,
            dry_run=False,
            sign_only=False,
            force=self.force,
            propagate_params=jenkins.get_propagatable_params(),
            block_until_complete=True,
            return_build_url=True,
        )
        # SECURITY: 'result' is a Jenkins result string (e.g. 'SUCCESS'/'FAILURE'), safe to log.
        self.logger.info("Downstream rhcos-sync for %s completed: result=%s url=%s", release_tag, result, url)
        return _ArchSyncOutcome(arch=go_arch, tag=release_tag, triggered=True, result=result, url=url)

    async def _latest_fast_channel_version(self, go_arch: str) -> Optional[str]:
        """Return the newest (highest semver) release version in this y-stream's fast channel.

        Cincinnati graph nodes carry a ``version`` string (e.g. "4.19.54"). The graph
        is requested per-arch using the GO arch (amd64/arm64/s390x/ppc64le).
        """
        graph = await CincinnatiAPI().get_graph(channel=self.channel, arch=go_arch)
        nodes = graph.get("nodes") or []
        if not nodes:
            return None

        best: Optional[VersionInfo] = None
        best_str: Optional[str] = None
        for node in nodes:
            version_str = node.get("version")
            if not version_str:
                continue
            try:
                parsed = VersionInfo.parse(version_str)
            except ValueError:
                self.logger.warning("Skipping unparsable version in %s: %s", self.channel, version_str)
                continue
            # Keep only nodes within this exact y-stream.
            if parsed.major != self.major or parsed.minor != self.minor:
                continue
            if best is None or parsed > best:
                best = parsed
                best_str = version_str

        return best_str


@cli.command("rhcos-sync-fast-channel", help="Auto-detect & publish RHCOS boot-image bumps for a z-stream y-stream")
@click.option("--version", required=True, help="OCP y-stream version to scan (e.g. 4.19)")
@click.option(
    "--arch",
    "arches",
    multiple=True,
    type=click.Choice(DEFAULT_GO_ARCHES),
    help="GO arch to scan (repeatable). Overrides the arches derived from group.yml.",
)
@click.option(
    "--data-path",
    default=constants.OCP_BUILD_DATA_URL,
    show_default=True,
    help="ocp-build-data repository URL used to resolve the group's supported arches.",
)
@click.option("--data-gitref", default="", help="ocp-build-data git ref (branch/tag/sha) to read group.yml from.")
@click.option("--force", is_flag=True, default=False, help="Trigger sync even if mirror already appears current")
@pass_runtime
@click_coroutine
async def rhcos_sync_fast_channel(
    runtime: Runtime, version: str, arches: tuple, data_path: str, data_gitref: str, force: bool
):
    """Periodic, fast-channel-driven RHCOS boot-image sync for z-stream releases.

    When ``--arch`` is not given, the arch set is derived per y-stream from ocp-build-data
    ``group.yml``; pass ``--arch`` (repeatable) to override it explicitly.

    Examples:

      Scan the group.yml arches for 4.19:

        artcd -vv rhcos-sync-fast-channel --version 4.19

      Scan a single arch, dry-run:

        artcd -vv --dry-run rhcos-sync-fast-channel --version 4.19 --arch amd64

      Force a sync regardless of mirror state:

        artcd -vv rhcos-sync-fast-channel --version 4.19 --force
    """
    pipeline = RhcosSyncFastChannelPipeline(
        runtime=runtime,
        version=version,
        arches=list(arches) or None,
        force=force,
        data_path=data_path,
        data_gitref=data_gitref,
    )
    await pipeline.run()
