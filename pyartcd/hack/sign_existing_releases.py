#!/usr/bin/env python3
"""
Standalone tool to sign existing release images (and optionally the component
images they reference) with Sigstore/cosign.

This tool can sign release payloads (manifest lists or single manifests) that
already exist in quay.io. It reuses the SigstoreSignatory class for signing logic.

For release images, only TAG-BASED signatures are created by default (digest
signatures are skipped). This is appropriate for retroactive signing where digest
signatures already exist. Use --sign-digest to also create digest signatures.

Use --sign-release to control what gets signed:
    yes  (default) sign the release image(s) and the components they reference
    only          sign only the release image(s)
    no            sign only the referenced component images

Referenced component images are discovered by spidering each release payload with
`oc adm release info -o json` and are always signed with digest identity only.
NOTE: `oc adm release info` on a `-multi` pullspec only returns one arch's
references, so to sign all referenced images across every architecture, pass the
per-arch release pullspecs (e.g. via --file), not just the `-multi` pullspec.

Usage:
    # Sign a single release image and its referenced components (from art-tools directory)
    uv run pyartcd/hack/sign_existing_releases.py --dry-run \
        quay.io/openshift-release-dev/ocp-release:4.16.1-multi

    # Sign only the release images (skip components) from a file
    uv run pyartcd/hack/sign_existing_releases.py --dry-run --sign-release only \
        --file pullspecs.txt

    # Sign only the referenced component images
    uv run pyartcd/hack/sign_existing_releases.py --dry-run --sign-release no \
        quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64

    # Real signing (requires KMS credentials)
    KMS_CRED_FILE=/path/to/creds KMS_KEY_ID=key-id REKOR_URL=https://... \
    uv run pyartcd/hack/sign_existing_releases.py \
        quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64

Environment Variables:
    KMS_CRED_FILE: Path to AWS credentials file for KMS signing
    KMS_KEY_ID: AWS KMS key ID(s) for signing (comma-separated for multiple keys)
    REKOR_URL: Rekor transparency log URL (required for signing)
"""

import asyncio
import logging
import os
import sys
from typing import Dict, List, Optional, Set

import click
from pyartcd.signatory import ReleaseImageInfo, SigstoreSignatory

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

CONCURRENCY_LIMIT = 50  # Limit concurrent operations


def extract_canonical_tag(pullspec: str) -> Optional[str]:
    """
    Extract the canonical tag from a pullspec.

    For tag-based pullspecs like "quay.io/.../ocp-release:4.16.1-multi",
    returns "4.16.1-multi".

    For digest-based pullspecs, returns None (can't determine tag).

    :param pullspec: The pullspec to extract the tag from
    :return: The canonical tag, or None if not determinable
    """
    if "@sha256:" in pullspec:
        return None
    if ":" in pullspec:
        # Split on last colon, but be careful of registry port
        parts = pullspec.rsplit(":", 1)
        if len(parts) == 2 and "/" in parts[0]:
            return parts[1]
    return None


async def main_async(
    pullspecs: List[str],
    dry_run: bool,
    concurrency: int,
    sign_digest: bool = False,
    sign_release: str = "yes",
) -> int:
    """
    Main async entry point for signing release images and/or referenced components.

    Mirrors the phased flow of the `sigstore-sign` pipeline:
        1. Discover release images and their manifests (if signing release images).
        2. Discover referenced component images by spidering each payload (if signing components).
        3. Sign release image manifests (with canonical tags).
        4. Sign component images (digest identity only).

    :param pullspecs: List of release pullspecs to process
    :param dry_run: If True, don't actually sign anything
    :param concurrency: Maximum concurrent operations
    :param sign_digest: If True, also sign release images with digest identity
        (default: False, tag only). Does not affect component images.
    :param sign_release: One of "yes" (release images + components), "only" (release
        images only), or "no" (referenced component images only).
    :return: Exit code (0 for success, 1 for errors)
    """
    # Validate environment
    kms_cred_file = os.environ.get("KMS_CRED_FILE", "")
    kms_key_id = os.environ.get("KMS_KEY_ID", "")
    rekor_url = os.environ.get("REKOR_URL", "")

    if not dry_run:
        if not kms_cred_file:
            logger.error("KMS_CRED_FILE environment variable is required for non-dry-run mode")
            return 1
        if not kms_key_id:
            logger.error("KMS_KEY_ID environment variable is required for non-dry-run mode")
            return 1

    # Create signatory
    signatory = SigstoreSignatory(
        logger=logger,
        dry_run=dry_run,
        signing_creds=kms_cred_file or "dummy-creds",
        signing_key_ids=kms_key_id.split(",") if kms_key_id else ["dummy-key"],
        rekor_url=rekor_url,
        concurrency_limit=concurrency,
    )

    tag_only = not sign_digest
    do_sign_release = sign_release != "no"
    do_sign_components = sign_release != "only"

    # Clean input: drop blanks and comment lines
    cleaned = [ps.strip() for ps in pullspecs if ps.strip() and not ps.strip().startswith("#")]

    logger.info("Starting to process %d release pullspec(s)...", len(cleaned))
    logger.info(
        "Release images: %s | Component images: %s",
        ("TAG ONLY" if tag_only else "digest+tag") if do_sign_release else "SKIP",
        "digest only" if do_sign_components else "SKIP",
    )
    if dry_run:
        logger.info("[DRY RUN MODE] No actual signing will occur")

    all_errors: Dict[str, Exception] = {}

    # --- Phase 1: Discover release images and their manifests ---
    release_images: List[ReleaseImageInfo] = []
    if do_sign_release:
        for pullspec in cleaned:
            canonical_tag = extract_canonical_tag(pullspec)
            if not canonical_tag:
                logger.warning(
                    "Cannot determine canonical tag for %s (digest-based pullspec). "
                    "Skipping release-image signing for it (tag-based pullspecs required).",
                    pullspec,
                )
                continue
            logger.info("Discovering release image %s (canonical tag: %s)", pullspec, canonical_tag)
            release_info, errors = await signatory.discover_release_image(
                pullspec=pullspec,
                canonical_tag=canonical_tag,
                release_name="",  # Skip release name validation
                verify_legacy_sig=False,
            )
            release_images.append(release_info)
            all_errors.update(errors)

    # --- Phase 2: Discover referenced component images from each payload ---
    component_images: Set[str] = set()
    if do_sign_components:
        for pullspec in cleaned:
            canonical_tag = extract_canonical_tag(pullspec)
            if canonical_tag and canonical_tag.endswith("-multi"):
                logger.warning(
                    "%s is a multi payload; `oc adm release info` returns only one arch's "
                    "references. To sign all referenced images across every architecture, pass "
                    "the per-arch release pullspecs instead of (or in addition to) the -multi one.",
                    pullspec,
                )
            logger.info("Discovering component images from %s", pullspec)
            components, errors = await signatory.discover_component_images(
                release_pullspec=pullspec,
                release_name="",  # Not used for component discovery
            )
            component_images.update(components)
            all_errors.update(errors)

    if all_errors:
        for ps, err in all_errors.items():
            logger.error("Discovery error for %s: %s", ps, err)
        return 1

    # --- Phase 3: Sign release images (with canonical tags) ---
    if release_images:
        total_manifests = sum(len(ri.manifests_to_sign) for ri in release_images)
        logger.info(
            "Signing %d release image(s) with %d total manifest(s) [%s]",
            len(release_images),
            total_manifests,
            "TAG ONLY" if tag_only else "digest+tag",
        )
        if errors := await signatory.sign_release_images(release_images, tag_only=tag_only):
            for ps, err in errors.items():
                logger.error("Release image signing error for %s: %s", ps, err)
            return 1

    # --- Phase 4: Sign component images (digest identity only) ---
    if component_images:
        logger.info("Signing %d component image(s) [digest only]", len(component_images))
        if errors := await signatory.sign_component_images(component_images):
            for ps, err in errors.items():
                logger.error("Component image signing error for %s: %s", ps, err)
            return 1

    # Summary
    logger.info("=" * 60)
    logger.info("Signing complete!")
    return 0


@click.command()
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Don't actually sign anything, just show what would be done",
)
@click.option(
    "--file",
    "-f",
    "input_file",
    type=click.Path(exists=True),
    help="File containing pullspecs to sign (one per line)",
)
@click.option(
    "--concurrency",
    "-c",
    type=int,
    default=CONCURRENCY_LIMIT,
    help=f"Maximum concurrent operations (default: {CONCURRENCY_LIMIT})",
)
@click.option(
    "--sign-digest",
    is_flag=True,
    default=False,
    help="Also sign release images with digest identity (default: tag-only for retroactive signing)",
)
@click.option(
    "--sign-release",
    type=click.Choice(("yes", "no", "only")),
    default="yes",
    help=(
        "What to sign: 'yes' = release images + referenced components (default), "
        "'only' = release images only, 'no' = referenced components only."
    ),
)
@click.argument("pullspecs", nargs=-1)
def main(
    dry_run: bool,
    input_file: Optional[str],
    concurrency: int,
    sign_digest: bool,
    sign_release: str,
    pullspecs: tuple,
):
    """
    Sign existing release images (and optionally their referenced components) with Sigstore/cosign.

    PULLSPECS are tag-based release image pullspecs like:
    quay.io/openshift-release-dev/ocp-release:4.16.1-multi

    For manifest lists, all arch-specific manifests will be discovered and signed.

    Release images are signed with TAG-BASED signatures only by default (digest
    signatures are skipped, appropriate for retroactive signing where digest
    signatures already exist). Use --sign-digest to also create digest signatures.

    Referenced component images are discovered by spidering each payload with
    `oc adm release info -o json` and are always signed with digest identity only.
    Use --sign-release to choose whether to sign release images, components, or both.
    NOTE: a `-multi` payload only yields one arch's references, so pass the per-arch
    release pullspecs to cover all referenced images across every architecture.

    Examples:

    \b
    # Dry run: sign a release image and its referenced components
    uv run pyartcd/hack/sign_existing_releases.py --dry-run \\
        quay.io/openshift-release-dev/ocp-release:4.16.1-multi

    \b
    # Sign only the release images from a file
    uv run pyartcd/hack/sign_existing_releases.py --dry-run --sign-release only -f pullspecs.txt

    \b
    # Sign only the referenced component images
    uv run pyartcd/hack/sign_existing_releases.py --dry-run --sign-release no \\
        quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64

    \b
    # Also sign release images with digest identity
    uv run pyartcd/hack/sign_existing_releases.py --dry-run --sign-digest \\
        quay.io/openshift-release-dev/ocp-release:4.16.1-x86_64
    """
    # Collect pullspecs from arguments and file
    all_pullspecs: List[str] = list(pullspecs)

    if input_file:
        with open(input_file, "r") as f:
            file_pullspecs = [line.strip() for line in f if line.strip() and not line.startswith("#")]
            all_pullspecs.extend(file_pullspecs)
            logger.info("Loaded %d pullspecs from %s", len(file_pullspecs), input_file)

    if not all_pullspecs:
        logger.error("No pullspecs provided. Use --file or pass pullspecs as arguments.")
        sys.exit(1)

    # Run async main
    exit_code = asyncio.run(main_async(all_pullspecs, dry_run, concurrency, sign_digest, sign_release))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
