"""
Pipeline to sign RHCOS container images using Sigstore/cosign.

This pipeline reads RHCOS data (already extracted by the Jenkinsfile)
and signs any container images published to Quay.
"""

import asyncio
import json
import os
from pathlib import Path

import click

from pyartcd.cli import cli, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.signatory import SigstoreSignatory


class SignRhcosContainersPipeline:
    """
    Pipeline to sign RHCOS container images with Sigstore.
    """

    def __init__(
        self,
        runtime: Runtime,
        rhcos_file: str,
        arch: str,
        signing_env: str | None = None,
    ):
        """
        Initialize the pipeline.

        Arg(s):
            runtime (Runtime): The runtime object
            rhcos_file (str): Path to the rhcos json file
            arch (str): Architecture (e.g., x86_64, aarch64)
            signing_env (str | None): Signing environment (stage or prod)
        """
        self.runtime = runtime
        self.rhcos_file = Path(rhcos_file)
        self.arch = arch
        self.signing_env = signing_env or "stage"
        self.logger = self.runtime.logger

        if not self.rhcos_file.exists():
            raise FileNotFoundError(f"RHCOS file not found: {self.rhcos_file}")

        if not self.runtime.dry_run:
            required_vars = ["KMS_CRED_FILE", "KMS_KEY_ID"]
            missing_vars = [var for var in required_vars if not os.environ.get(var)]
            if missing_vars:
                raise ValueError(
                    f"Missing required environment variables for signing: {missing_vars}. "
                    "Please set KMS_CRED_FILE and KMS_KEY_ID."
                )

    def _get_rhcos_container_digests(self) -> dict[str, str]:
        """
        Extract all RHCOS container image digests from the rhcos JSON file.

        Returns container images that are published to Quay (have digest-ref field
        pointing to quay.io).

        Return Value(s):
            dict[str, str]: Dictionary mapping image type to digest-ref pullspec
        """
        self.logger.info(f"Reading RHCOS data from {self.rhcos_file}")
        with open(self.rhcos_file, "r") as f:
            stream_data = json.load(f)

        images = stream_data.get("architectures", {}).get(self.arch, {}).get("images", {})

        # Filter for container images (have digest-ref pointing to quay.io)
        container_images = {}
        for image_type, image_data in images.items():
            if not isinstance(image_data, dict):
                continue

            digest_ref = image_data.get("digest-ref")
            if digest_ref and "quay.io" in digest_ref:
                container_images[image_type] = digest_ref
                self.logger.info(f"Found {image_type} container: {digest_ref}")

        return container_images

    async def _sign_containers(self, container_images: dict[str, str]) -> bool:
        """
        Sign RHCOS container images using SigstoreSignatory.

        Arg(s):
            container_images (dict[str, str]): Dictionary of image type to digest-ref
        Return Value(s):
            bool: True if successful, False otherwise
        """
        if not container_images:
            self.logger.warning(f"No RHCOS container images found for {self.arch}")
            return True

        self.logger.info(f"Signing {len(container_images)} RHCOS container image(s):")
        for image_type, digest in container_images.items():
            self.logger.info(f"  - {image_type}: {digest}")

        kms_cred_file = os.environ.get("KMS_CRED_FILE", "")
        kms_key_ids = os.environ.get("KMS_KEY_ID", "").strip().split(",")
        rekor_url = os.environ.get("REKOR_URL", "")

        signatory = SigstoreSignatory(
            logger=self.logger,
            dry_run=self.runtime.dry_run,
            signing_creds=kms_cred_file,
            signing_key_ids=kms_key_ids,
            rekor_url=rekor_url,
            concurrency_limit=10,
        )

        digest_list = list(container_images.values())
        errors = await signatory.sign_component_images(digest_list)

        if errors:
            self.logger.error(f"Signing failed with errors: {errors}")
            return False

        if not self.runtime.dry_run:
            self.logger.info(f"Successfully signed {len(container_images)} RHCOS container image(s)")

        return True

    def run(self):
        """
        Run the pipeline.
        """
        self.logger.info(f"Signing RHCOS containers from {self.rhcos_file} ({self.arch})")
        self.logger.info(f"Signing environment: {self.signing_env}")
        self.logger.info(f"Dry run: {self.runtime.dry_run}")

        self.logger.info("Extracting RHCOS container digests from stream data...")
        container_images = self._get_rhcos_container_digests()

        if not container_images:
            self.logger.warning(
                f"No RHCOS container images found for {self.arch} in stream file. "
                "This is expected for older releases or architectures without container images."
            )
            return

        success = asyncio.run(self._sign_containers(container_images))

        if not success:
            raise RuntimeError("Failed to sign one or more RHCOS container images")

        self.logger.info("RHCOS container signing complete")


@cli.command("sign-rhcos-containers", help="Sign RHCOS container images with Sigstore/cosign")
@click.option("--rhcos-file", required=True, help="Path to rhcos JSON file")
@click.option("--arch", required=True, help="Architecture (e.g., x86_64, aarch64, s390x, ppc64le)")
@click.option(
    "--signing-env",
    type=click.Choice(["stage", "prod"]),
    default="stage",
    help="Signing environment (determines which KMS key to use)",
)
@pass_runtime
def sign_rhcos_containers(
    runtime: Runtime,
    rhcos_file: str,
    arch: str,
    signing_env: str,
):
    """
    Sign RHCOS container images for a release.

    This command reads RHCOS stream data from a JSON file and signs all container
    images published to Quay. It handles kubevirt containers and any other RHCOS
    images with digest-ref fields.

    The rhcos.json file is extracted by the Jenkins job using:
        cat coreos-bootimages.yaml | yq -r .data.stream > rhcos.json

    Required environment variables:
    - KMS_CRED_FILE: Path to AWS credentials file for KMS signing
    - KMS_KEY_ID: AWS KMS key ID(s) for signing (comma-separated for multiple keys)
    - REKOR_URL: Rekor transparency log URL (optional, defaults to no tlog upload)

    Example:
        artcd -v --dry-run sign-rhcos-containers \\
            --rhcos-file /path/to/rhcos.json \\
            --arch x86_64 \\
            --signing-env prod
    """
    pipeline = SignRhcosContainersPipeline(
        runtime=runtime,
        rhcos_file=rhcos_file,
        arch=arch,
        signing_env=signing_env,
    )
    pipeline.run()
