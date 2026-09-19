#!/usr/bin/env python3
"""
Standalone tool to sign existing release images (and optionally the component
images they reference) with Sigstore/cosign.

This tool can sign release payloads (manifest lists or single manifests) that
already exist in quay.io. It reuses the SigstoreSignatory class for signing logic.

By default, only TAG-BASED signatures are created (digest signatures are skipped).
This is appropriate for retroactive signing where digest signatures already exist.

Use --sign-release to control what is signed: "yes" (default) signs the release
image(s) and the component images they reference, "only" signs just the release
image(s), and "no" signs only the referenced component images. Referenced component
images are discovered with `oc adm release info -o json` and are always signed with
digest identity only (multi-arch references are expanded to and signed for every
architecture).

Usage:
    # Sign a single release image and its referenced components (from art-tools directory)
    uv run pyartcd/hack/sign_existing_releases.py --dry-run \
        quay.io/openshift-release-dev/ocp-release:4.16.1-multi

    # Sign multiple release images (only, no components) from a file
    uv run pyartcd/hack/sign_existing_releases.py --dry-run --sign-release only \
        --file pullspecs.txt

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
from typing import List, Optional, Set

import click
from pyartcd.signatory import SigstoreSignatory

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


async def sign_release_pullspec(
    signatory: SigstoreSignatory,
    pullspec: str,
    tag_only: bool = True,
    sign_release: str = "yes",
    signed_components: Optional[Set[str]] = None,
) -> bool:
    """
    Sign a single release pullspec and/or the component images it references.

    For manifest lists, discovers all arch-specific manifests and signs each
    with the manifest list's canonical tag.

    :param signatory: The SigstoreSignatory instance to use
    :param pullspec: The release image pullspec (tag-based preferred)
    :param tag_only: If True, only sign release images with tag identity (skip digest
        identity). Default is True for retroactive signing where digest signatures
        already exist.
    :param sign_release: One of "yes" (release image + referenced components), "only"
        (release image only), or "no" (referenced component images only).
    :param signed_components: Optional shared set of component pullspecs already signed
        in this run, used to avoid re-signing components shared across release images.
    :return: True if successful, False if any errors occurred
    """
    ok = True

    # Sign the release image itself.
    if sign_release != "no":
        canonical_tag = extract_canonical_tag(pullspec)
        if not canonical_tag:
            logger.warning(
                "Cannot determine canonical tag for %s (digest-based pullspec). "
                "Skipping release-image signing - tag-based pullspecs are required.",
                pullspec,
            )
        else:
            logger.info("Processing %s (canonical tag: %s)", pullspec, canonical_tag)
            # We don't have a release_name to validate against, so we'll skip that check
            release_info, errors = await signatory.discover_release_image(
                pullspec=pullspec,
                canonical_tag=canonical_tag,
                release_name="",  # Skip release name validation
                verify_legacy_sig=False,
            )
            if errors:
                for ps, err in errors.items():
                    logger.error("Discovery error for %s: %s", ps, err)
                ok = False
            elif not release_info.manifests_to_sign:
                logger.warning("No manifests found to sign for %s", pullspec)
            else:
                logger.info("Found %d manifest(s) to sign for %s", len(release_info.manifests_to_sign), pullspec)
                errors = await signatory.sign_release_images([release_info], tag_only=tag_only)
                if errors:
                    for ps, err in errors.items():
                        logger.error("Signing error for %s: %s", ps, err)
                    ok = False
                else:
                    logger.info("Successfully signed release image %s", pullspec)

    # Sign the component images referenced by the release (digest identity only).
    if sign_release != "only":
        seen = signed_components if signed_components is not None else set()
        logger.info("Discovering component images referenced by %s", pullspec)
        components, errors = await signatory.discover_component_images(
            release_pullspec=pullspec,
            release_name="",  # Not used for component discovery
        )
        if errors:
            for ps, err in errors.items():
                logger.error("Discovery error for %s: %s", ps, err)
            ok = False
        to_sign = components - seen
        if to_sign:
            logger.info("Signing %d component image(s) referenced by %s [digest only]", len(to_sign), pullspec)
            errors = await signatory.sign_component_images(to_sign)
            if errors:
                for ps, err in errors.items():
                    logger.error("Component signing error for %s: %s", ps, err)
                ok = False
            seen.update(to_sign - set(errors))  # only mark successfully-signed components as done
        else:
            logger.info("No new component images to sign for %s", pullspec)

    return ok


async def main_async(
    pullspecs: List[str],
    dry_run: bool,
    concurrency: int,
    sign_digest: bool = False,
    sign_release: str = "yes",
) -> int:
    """
    Main async entry point for signing release images and/or referenced components.

    :param pullspecs: List of pullspecs to sign
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
    logger.info("Starting to sign %d release pullspec(s)...", len(pullspecs))
    logger.info("Mode: %s", "TAG ONLY (skipping digest signatures)" if tag_only else "BOTH digest and tag signatures")
    if dry_run:
        logger.info("[DRY RUN MODE] No actual signing will occur")

    # Process each pullspec. Track components signed across releases to avoid re-signing
    # images shared between payloads (e.g. the same component referenced by multiple arches).
    success_count = 0
    error_count = 0
    signed_components: Set[str] = set()

    for i, pullspec in enumerate(pullspecs, 1):
        pullspec = pullspec.strip()
        if not pullspec or pullspec.startswith("#"):
            continue  # Skip empty lines and comments

        logger.info("--- [%d/%d] Processing %s ---", i, len(pullspecs), pullspec)

        try:
            success = await sign_release_pullspec(
                signatory,
                pullspec,
                tag_only=tag_only,
                sign_release=sign_release,
                signed_components=signed_components,
            )
            if success:
                success_count += 1
            else:
                error_count += 1
        except Exception as exc:
            logger.exception("Unexpected error processing %s: %s", pullspec, exc)
            error_count += 1

    # Summary
    logger.info("=" * 60)
    logger.info("Signing complete: %d successful, %d errors", success_count, error_count)

    return 0 if error_count == 0 else 1


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
