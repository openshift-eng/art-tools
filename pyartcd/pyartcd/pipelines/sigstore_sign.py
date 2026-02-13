import os
import sys
from typing import Dict, List, Optional, Set

import click
from artcommonlib import exectools
from semver import VersionInfo

from pyartcd import constants, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import GroupRuntime, Runtime
from pyartcd.signatory import ReleaseImageInfo, SigstoreSignatory

CONCURRENCY_LIMIT = 100  # we run out of processes without a limit


class SigstorePipeline:
    @classmethod
    async def create(cls, *args, **kwargs):
        self = cls(*args, **kwargs)
        doozer_data_path = os.environ.get("DOOZER_DATA_PATH") or constants.OCP_BUILD_DATA_URL
        self.group_runtime = await GroupRuntime.create(
            self.runtime.config,
            self.runtime.working_dir,
            self.group,
            self.assembly,
            doozer_data_path,
        )
        self.releases_config = await util.load_releases_config(
            group=self.group,
            data_path=doozer_data_path,
        )
        return self

    def __init__(
        self,
        runtime: Runtime,
        group: str,
        assembly: str,
        multi: str,
        sign_release: str,
        verify_release: bool,
        pullspecs: Optional[List[str]],
    ) -> None:
        self.runtime = runtime
        self.group = group
        self.assembly = assembly
        self.sign_multi = multi != "no"
        self.sign_arches = multi != "only"
        self.sign_release = sign_release != "no"
        self.sign_components = sign_release != "only"
        self.verify_release = verify_release
        self.pullspecs = pullspecs
        self._logger = self.runtime.logger

        self.signatory = SigstoreSignatory(
            logger=self._logger,
            dry_run=self.runtime.dry_run,
            signing_creds=os.environ.get("KMS_CRED_FILE", "dummy-file"),
            # Allow AWS_KEY_ID to be a comma delimited list
            signing_key_ids=os.environ.get("KMS_KEY_ID", "dummy-key").strip().split(","),
            rekor_url=os.environ.get("REKOR_URL", ""),
            concurrency_limit=CONCURRENCY_LIMIT,
        )

    def check_environment_variables(self):
        required_vars = ["KMS_CRED_FILE", "KMS_KEY_ID"]

        for env_var in required_vars:
            if not os.environ.get(env_var):  # not there, or empty
                msg = f"Environment variable {env_var} is not set."
                if self.runtime.dry_run:
                    self._logger.warning(msg)
                else:
                    raise ValueError(msg)

    async def login_quay(self):
        if all(os.environ.get(var) for var in ["QUAY_USERNAME", "QUAY_PASSWORD"]):
            # the login command has only the variable names in it, so the values can be picked up from
            # the environment rather than included in the command line where they would be logged.
            # better would be to have jenkins write a credentials file (and do the same in `promote`).
            cmd = 'podman login -u "$QUAY_USERNAME" -p "$QUAY_PASSWORD" quay.io'
            await exectools.cmd_assert_async(["bash", "-c", cmd], env=os.environ.copy(), stdout=sys.stderr)
        else:
            self._logger.info("quay login credentials not given in environment; using existing container auth")

    async def run(self):
        logger = self.runtime.logger
        self.check_environment_variables()
        await self.login_quay()

        # Load group config and releases.yml
        logger.info("Loading build metadata...")
        if self.releases_config.get("releases", {}).get(self.assembly) is None:
            raise ValueError(
                f"To sign this release, assembly {self.assembly} must be explicitly defined in releases.yml."
            )

        # Get release name
        release_name = util.get_release_name_for_assembly(self.group, self.releases_config, self.assembly)
        # Ensure release name is valid
        if not VersionInfo.is_valid(release_name):
            raise ValueError(f"Release name `{release_name}` is not a valid semver.")
        logger.info("Release name: %s", release_name)

        if not self.pullspecs:
            # look up release images we expect to exist, since none were specified.
            if not self.verify_release:
                raise ValueError("""
                    For adequate paranoia, either supply pullspecs with shasums (all new releases
                    should do this), or use --verify-release to ensure release images that we look
                    up and retro-sign have legacy signatures.
                """)
            self.pullspecs = self._lookup_release_images(release_name)

        all_errors: Dict[str, Exception] = {}

        # --- Phase 1: Discover release images and their manifests ---
        release_images: List[ReleaseImageInfo] = []

        if self.sign_release:
            for pullspec in self.pullspecs:
                # Extract canonical tag from tag-based pullspecs
                if "@sha256:" in pullspec:
                    logger.warning(
                        "Pullspec %s is digest-based; cannot determine canonical tag. "
                        "For proper canonical tag signing, use tag-based pullspecs.",
                        pullspec,
                    )
                    # Can't do canonical tag signing without knowing the tag
                    continue

                canonical_tag = pullspec.split(":")[-1]
                logger.info("Discovering release image %s with canonical tag: %s", pullspec, canonical_tag)

                release_info, errors = await self.signatory.discover_release_image(
                    pullspec=pullspec,
                    canonical_tag=canonical_tag,
                    release_name=release_name,
                    verify_legacy_sig=self.verify_release,
                )
                release_images.append(release_info)
                all_errors.update(errors)

        # --- Phase 2: Discover component images ---
        component_images: Set[str] = set()

        if self.sign_components and self.pullspecs:
            # Use first release image to get component references
            first_pullspec = self.pullspecs[0]
            logger.info("Discovering component images from %s", first_pullspec)

            components, errors = await self.signatory.discover_component_images(
                release_pullspec=first_pullspec,
                release_name=release_name,
            )
            component_images.update(components)
            all_errors.update(errors)

        if all_errors:
            print("Discovery errors:")
            for ps, err in all_errors.items():
                print(f"  {ps}: {err}")
            exit(1)

        # --- Phase 3: Sign release images (with canonical tags) ---
        if release_images:
            total_manifests = sum(len(ri.manifests_to_sign) for ri in release_images)
            logger.info("Signing %d release images with %d total manifests", len(release_images), total_manifests)
            if errors := await self.signatory.sign_release_images(release_images):
                print(f"Release image signing failed: {errors}")
                exit(1)

        # --- Phase 4: Sign component images (digest only) ---
        if component_images:
            logger.info("Signing %d component images", len(component_images))
            if errors := await self.signatory.sign_component_images(component_images):
                print(f"Component image signing failed: {errors}")
                exit(1)

        logger.info("Signing complete!")

    def _lookup_release_images(self, release_name):
        # NOTE: only do this for testing purposes. for secure signing, always supply an
        # immutable pullspec with a digest (as tags could theoretically be rewritten in between
        # publishing and signing). TODO: enforce this at invocation time
        arches = []
        if self.sign_arches:
            arches += (
                self.releases_config.get("group", {}).get("arches")
                or self.group_runtime.group_config.get("arches")
                or []
            )
        if self.sign_multi:
            arches.append("multi")
        return list(f"{constants.RELEASE_IMAGE_REPO}:{release_name}-{arch}" for arch in arches)


@cli.command("sigstore-sign")
@click.option(
    "-g",
    "--group",
    metavar="NAME",
    required=True,
    help="The group of components on which to operate. e.g. openshift-4.15",
)
@click.option(
    "-a", "--assembly", metavar="ASSEMBLY_NAME", required=True, help="The name of an assembly to be signed. e.g. 4.15.1"
)
@click.option(
    "--multi",
    type=click.Choice(("yes", "no", "only")),
    default="yes",
    help="Whether to sign multi-arch or arch-specific payloads.",
)
@click.option(
    "--sign-release",
    type=click.Choice(("yes", "no", "only")),
    default="yes",
    help="Whether to sign the release image or just component images.",
)
@click.option(
    "--verify-release",
    is_flag=True,
    default=False,
    help="Verify that release images have a legacy signature before re-signing.",
)
@click.argument("pullspecs", nargs=-1, required=False)
@pass_runtime
@click_coroutine
async def sigstore_sign_container(
    runtime: Runtime,
    group: str,
    assembly: str,
    multi: str,
    sign_release: str,
    verify_release: bool,
    pullspecs: Optional[List[str]] = None,
):
    pipeline = await SigstorePipeline.create(
        runtime,
        group,
        assembly,
        multi,
        sign_release,
        verify_release,
        pullspecs,
    )
    await pipeline.run()
