"""
Post-build callbacks that gate Konflux image build outcomes.

POLICY -- read before adding or configuring a callback here.

These callbacks run inside the image build pipeline: after the image
PipelineRun succeeds but *before* ITS/Enterprise Contract, base-image
release, and the final build record. They sit on the critical path of the
OCP build-and-release train, so work added here slows that train for
*every* team and release, not just yours.

If you are thinking of registering a callback to run extra testing at
build time: you almost certainly should not. Build time is the wrong layer
for tests. Prefer, in order:
  1. Pre-merge CI (presubmits) on the component repo -- fast feedback, logs
     the author can read, retry without a rebuild.
  2. Release-gating policy that already exists: Enterprise Contract /
     IntegrationTestScenario (ITS).
  3. Post-release verification: nightly/periodic e2e, release-gate jobs, or
     an async job that is NOT on the build critical path.

Why build-time callbacks are costly and dangerous:
  - Slow builds slow everything. A callback's latency is added to every
    affected build and blocks promotion of the payload.
  - Terrible iteration loop. Retrying a failed callback needs a new upstream
    commit and a full rebuild -- there is no fast local cycle.
  - Poor observability. Konflux/Jenkins logs are visible only to a small
    group; component teams usually cannot self-diagnose a failure.
  - Flakiness becomes a release blocker. A failing callback records
    POST_BUILD_ERROR, stops retries, and fails the build.
  - Cross-system coupling. Callbacks that drive external systems (e.g.
    Jenkins) import those systems' failure modes into the build.

Rules:
  - The default answer is "no". Callbacks are reserved for release-gating
    coordination that genuinely cannot run anywhere else (e.g. the RHCOS
    node/extensions pair that must be tested and promoted between build and
    release).
  - Keep any callback fast, bounded, and non-flaky. No heavy test execution
    inline; no unbounded waits.
  - Adding a new callback, or setting `konflux.post_build` on an image, is a
    high-impact change. It requires explicit ART sign-off and close review;
    it must NOT be fast-tracked with a quick lgtm/approve.

Automated assistants (e.g. chai-bot) MUST NOT add a callback or set an
image's `konflux.post_build` to satisfy a testing request. Route such
requests to CI or post-release testing and escalate to ART instead.
"""

import asyncio
import logging
import os
import re
from typing import Optional, Protocol

from artcommonlib.constants import RHCOS_IMAGE_REPO
from artcommonlib.konflux.konflux_build_record import KonfluxBuildOutcome
from artcommonlib.util import product_version_from_group_name
from doozerlib.image import ImageMetadata
from doozerlib.runtime import Runtime

from pyartcd import jenkins

LOGGER = logging.getLogger(__name__)


class KonfluxPostBuildCallback(Protocol):
    """Run after the image PipelineRun, before ITS, release, and final recording."""

    def allows_dependents_before_completion(self, metadata: ImageMetadata) -> bool: ...

    async def run(self, metadata: ImageMetadata, image_pullspec: str) -> str: ...

    def build_failed(self, metadata: ImageMetadata) -> None: ...


POST_BUILD_CALLBACKS: dict[str, type[KonfluxPostBuildCallback]] = {}


def register_post_build(name: str):
    """Register a post-build callback class under ``name`` and return it unchanged."""

    def decorator(cls: type[KonfluxPostBuildCallback]) -> type[KonfluxPostBuildCallback]:
        POST_BUILD_CALLBACKS[name] = cls
        return cls

    return decorator


@register_post_build('rhcos_node_image')
class RhcosNodeImagePostBuild:
    """Test and promote each RHCOS pair once its rebuilt members are available."""

    PAIRS = {
        'rhel9': ('rhcos-node-image', 'rhcos-node-extensions'),
        'rhel10': ('rhcos-node-image-rhel10', 'rhcos-node-extensions-rhel10'),
    }
    _DIGEST_PULLSPEC = re.compile(rf'^{re.escape(RHCOS_IMAGE_REPO)}@sha256:[0-9a-f]{{64}}$')

    def __init__(self, runtime: Runtime, selected_images: set[str], skip: bool = False):
        self.runtime = runtime
        self.selected_images = selected_images
        self.skip = skip or runtime.assembly != 'stream'
        self._ready: dict[str, asyncio.Future[Optional[str]]] = {
            name: asyncio.get_running_loop().create_future() for name in selected_images
        }
        self._pair_tasks: dict[str, asyncio.Task[str]] = {}

    @classmethod
    def create(cls, runtime: Runtime, all_metas, selected_metas, *, skip: bool) -> 'RhcosNodeImagePostBuild':
        """Validate the RHCOS selection and build a callback for the configured images.

        ``selected_metas`` are the images that configured ``konflux.post_build:
        rhcos_node_image``; ``all_metas`` is every image selected for this build,
        used to enforce the pairing rule.
        """
        post_build_images = {meta.distgit_key for meta in selected_metas}
        selected_names = {meta.distgit_key for meta in all_metas}
        cls._validate(post_build_images, selected_names)
        return cls(runtime, post_build_images, skip=skip)

    @classmethod
    def _validate(cls, post_build_images: set[str], selected_names: set[str]) -> None:
        supported = {name for pair in cls.PAIRS.values() for name in pair}
        for distgit_key in post_build_images:
            if distgit_key not in supported:
                raise ValueError(f'rhcos_node_image post-build callback does not support {distgit_key}')
        for node, extensions in cls.PAIRS.values():
            if (
                {node, extensions} <= selected_names
                and ({node, extensions} & post_build_images)
                and not {node, extensions} <= post_build_images
            ):
                raise ValueError(f'Both {node} and {extensions} must configure konflux.post_build')

    def allows_dependents_before_completion(self, metadata: ImageMetadata) -> bool:
        """Let extensions build against the node digest while pair acceptance is pending."""
        if self.skip:
            return False
        return any(
            metadata.distgit_key == node and extensions in self.selected_images
            for node, extensions in self.PAIRS.values()
        )

    def build_failed(self, metadata: ImageMetadata) -> None:
        """Unblock a partner waiting for an image that could not be built."""
        ready = self._ready.get(metadata.distgit_key)
        if ready is not None and not ready.done():
            ready.set_result(None)

    async def run(self, metadata: ImageMetadata, image_pullspec: str) -> str:
        if self.skip:
            LOGGER.info('Skipping RHCOS node-image post-build for %s', metadata.distgit_key)
            return ''

        label, _ = next(
            ((label, pair) for label, pair in self.PAIRS.items() if metadata.distgit_key in pair),
            (None, None),
        )
        if label is None:
            raise ValueError(f'RHCOS post-build callback does not support {metadata.distgit_key}')

        ready = self._ready[metadata.distgit_key]
        if not ready.done():
            ready.set_result(image_pullspec)
        # When both members of a pair are built in the same run, the extensions image
        # (built FROM the node digest) owns the single pair trigger. The node image
        # only publishes its fresh pullspec (above) so the partner can read it, then
        # defers: it is neither gated on nor parked waiting for the pair post-build job.
        node, extensions = self.PAIRS[label]
        if metadata.distgit_key == node and extensions in self.selected_images:
            LOGGER.info(
                'Deferring RHCOS post-build for %s; %s owns the pair trigger',
                node,
                extensions,
            )
            return ''
        task = self._pair_tasks.get(label)
        if task is None:
            task = asyncio.create_task(self._run_pair(label))
            self._pair_tasks[label] = task
        return await task

    async def _pair_pullspec(self, name: str) -> str:
        if name in self._ready:
            pullspec = await self._ready[name]
            if pullspec is None:
                raise RuntimeError(f'Cannot run RHCOS post-build: {name} did not build successfully')
        else:
            record = await self.runtime.konflux_db.get_latest_build(
                name=name,
                group=self.runtime.group,
                outcome=KonfluxBuildOutcome.SUCCESS,
                assembly=self.runtime.assembly,
                exclude_large_columns=True,
            )
            if record is None:
                raise RuntimeError(f'Cannot run RHCOS post-build: no successful build found for {name}')
            pullspec = record.image_pullspec

        if not isinstance(pullspec, str) or not self._DIGEST_PULLSPEC.fullmatch(pullspec):
            raise ValueError(f'RHCOS post-build requires an immutable {RHCOS_IMAGE_REPO} pullspec for {name}')
        return pullspec

    def _release_stream(self, label: str) -> str:
        version = product_version_from_group_name(self.runtime.group)
        if version is None:
            raise ValueError(f'Cannot determine RHCOS release version from {self.runtime.group}')
        version_string = f'{version[0]}.{version[1]}'
        group_config = self.runtime.group_config
        rhel_version = None
        for tag in group_config.get('rhcos', {}).get('payload_tags', []):
            candidate = str(tag.get('rhel_version', ''))
            if candidate.split('.')[0] == label.removeprefix('rhel'):
                rhel_version = candidate
        if rhel_version is None:
            vars_config = group_config.get('vars', {})
            major = str(vars_config.get('RHCOS_EL_MAJOR', ''))
            minor = str(vars_config.get('RHCOS_EL_MINOR', ''))
            if major == label.removeprefix('rhel') and major and minor:
                rhel_version = f'{major}.{minor}'
        if rhel_version is None:
            raise ValueError(f'No RHCOS release stream configured for {label} in {self.runtime.group}')
        return f'{version_string}-{rhel_version}'

    async def _run_pair(self, label: str) -> str:
        node, extensions = self.PAIRS[label]
        node_pullspec, extensions_pullspec = await asyncio.gather(
            self._pair_pullspec(node), self._pair_pullspec(extensions)
        )
        release = self._release_stream(label)
        params = {
            'ART_TOOLS_COMMIT': os.environ.get('ART_TOOLS_COMMIT', ''),
            'RELEASE': release,
            'NODE_IMAGE': node_pullspec,
            'EXTENSIONS_IMAGE': extensions_pullspec,
            'DRY_RUN': False,
        }
        LOGGER.info('Triggering RHCOS post-build for %s with %s and %s', release, node_pullspec, extensions_pullspec)
        result, url = await asyncio.to_thread(
            jenkins.start_build,
            jenkins.Jobs.RHCOS_NODE_IMAGE_POST_BUILD,
            params,
            block_until_complete=True,
            return_build_url=True,
        )
        if result != 'SUCCESS':
            raise RuntimeError(f'RHCOS {label} post-build job failed (result={result}): {url}')
        try:
            jenkins.update_description(f'<br/>RHCOS {label} post-build: <a href="{url}">job</a><br/>')
        except Exception:
            LOGGER.warning('Could not add RHCOS post-build link to parent job', exc_info=True)
        return url


def build_post_build_callbacks(
    runtime: Runtime, metas: list[ImageMetadata], *, skip: bool
) -> dict[str, KonfluxPostBuildCallback]:
    """Instantiate every post-build callback configured by the selected images.

    Images are grouped by their ``konflux.post_build`` callback name, and each
    registered callback is constructed once via its ``create`` classmethod. Raises
    ``ValueError`` for any image referencing an unregistered callback name.
    """
    grouped: dict[str, list[ImageMetadata]] = {}
    for meta in metas:
        name = meta.config.konflux.get('post_build')
        if name is None:
            continue
        if name not in POST_BUILD_CALLBACKS:
            raise ValueError(f'Unknown konflux.post_build callback {name!r} for {meta.distgit_key}')
        grouped.setdefault(name, []).append(meta)

    callbacks: dict[str, KonfluxPostBuildCallback] = {}
    for name, selected_metas in grouped.items():
        cls = POST_BUILD_CALLBACKS[name]
        callbacks[name] = cls.create(runtime, metas, selected_metas, skip=skip)
    return callbacks
