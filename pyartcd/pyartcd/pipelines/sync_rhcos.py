"""
Pipeline to sync RHCOS boot images to mirror.openshift.com and sign sha256sum.txt.

Migrated from aos-cd-jobs/jobs/build/rhcos_sync/S3-rhcossync.sh.

This pipeline:
1. Downloads RHCOS artifacts listed in a synclist file
2. Renames them with release version, creates unversioned symlinks
3. Creates legacy rhcos-installer-* symlinks from rhcos-live-* files
4. Generates sha256sum.txt and rhcos-id.txt
5. GPG-signs sha256sum.txt via RADAS/UMB
6. Syncs everything (including sha256sum.txt.gpg) to S3 and Cloudflare
7. Updates latest directories as appropriate
"""

import hashlib
import os
import re
from pathlib import Path
from typing import List, Optional

import aiohttp
import click

from pyartcd import constants, util
from pyartcd.cli import cli, click_coroutine, pass_runtime
from pyartcd.runtime import Runtime
from pyartcd.signatory import AsyncSignatory

S3_BUCKET = "s3://art-srv-enterprise"


class SyncRhcosPipeline:
    """Pipeline to sync RHCOS boot images to mirror.openshift.com."""

    def __init__(
        self,
        runtime: Runtime,
        arch: str,
        build_id: str,
        version: str,
        mirror_prefix: str,
        base_dir: str,
        synclist: str,
        no_latest: bool = False,
        signing_env: Optional[str] = None,
    ):
        self.runtime = runtime
        self.arch = arch
        self.build_id = build_id
        self.version = version
        self.mirror_prefix = mirror_prefix
        self.base_dir = base_dir
        self.synclist = synclist
        self.no_latest = no_latest
        self.signing_env = signing_env
        self.logger = runtime.logger

        self.staging_dir = Path(runtime.working_dir) / f"staging-{version}"

    async def run(self):
        self.logger.info(
            "Starting RHCOS sync: version=%s, build_id=%s, arch=%s, prefix=%s",
            self.version,
            self.build_id,
            self.arch,
            self.mirror_prefix,
        )

        self.staging_dir.mkdir(parents=True, exist_ok=True)

        urls = self._parse_synclist()
        await self._download_images(urls)
        self._rename_with_version()
        self._create_installer_symlinks()
        self._generate_rhcos_id_txt()
        self._generate_sha256sum()

        if self.signing_env:
            await self._sign_sha256sum()

        versioned_dest = f"{S3_BUCKET}{self.base_dir}/{self.mirror_prefix}/{self.version}/"
        await self._sync_to_s3(str(self.staging_dir), versioned_dest)

        if not self.no_latest:
            await self._update_latest()

        self.logger.info("RHCOS sync completed successfully")

    def _parse_synclist(self) -> List[str]:
        """Parse the synclist file for URLs to download."""
        synclist_path = Path(self.synclist)
        if not synclist_path.is_file():
            raise FileNotFoundError(f"Synclist file not found: {self.synclist}")
        urls = [line.strip() for line in synclist_path.read_text().splitlines() if line.strip()]
        self.logger.info("Found %d artifacts in synclist", len(urls))
        return urls

    async def _download_images(self, urls: List[str]):
        """Download all images from the synclist URLs."""
        timeout = aiohttp.ClientTimeout(total=30 * 60, sock_read=5 * 60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for url in urls:
                filename = url.rsplit("/", 1)[-1]
                dest = self.staging_dir / filename
                self.logger.info("Downloading %s", url)

                async with session.get(url) as response:
                    if response.status != 200:
                        raise Exception(f"Failed to download {url}: HTTP {response.status}")

                    content_length = response.headers.get("content-length")
                    if content_length:
                        self.logger.info("%s size: %.1f MB", filename, int(content_length) / (1024 * 1024))

                    with open(dest, "wb") as f:
                        downloaded = 0
                        last_logged_mb = 0
                        async for chunk in response.content.iter_chunked(1024 * 1024):
                            f.write(chunk)
                            downloaded += len(chunk)
                            downloaded_mb = downloaded / (1024 * 1024)
                            if downloaded_mb - last_logged_mb >= 200:
                                self.logger.info("%s downloaded: %.1f MB", filename, downloaded_mb)
                                last_logged_mb = downloaded_mb

                self.logger.info("Downloaded %s", filename)

    def _rename_with_version(self):
        """Rename files to include release version and create unversioned symlinks.

        Transforms:  rhcos-{build_id}-qemu... -> rhcos-{release}-qemu...
        Symlink:     rhcos-qemu...            -> rhcos-{release}-qemu...

        The release name includes the arch suffix by convention.
        """
        release = self.version
        if self.arch not in release:
            release = f"{release}-{self.arch}"

        for path in sorted(self.staging_dir.iterdir()):
            if not path.is_file() or path.is_symlink():
                continue
            name = path.name
            if self.build_id not in name:
                continue

            versioned_name = name.replace(self.build_id, release)
            unversioned_name = name.replace(f"-{self.build_id}", "")

            versioned_path = self.staging_dir / versioned_name
            path.rename(versioned_path)
            self.logger.info("Renamed %s -> %s", name, versioned_name)

            symlink_path = self.staging_dir / unversioned_name
            symlink_path.symlink_to(versioned_name)
            self.logger.info("Symlink %s -> %s", unversioned_name, versioned_name)

    def _create_installer_symlinks(self):
        """Create legacy rhcos-installer-* symlinks from rhcos-live-* symlinks.

        Some customer portals reference the deprecated rhcos-installer names.
        """
        for path in sorted(self.staging_dir.iterdir()):
            if not path.is_symlink():
                continue
            if not path.name.startswith("rhcos-live-"):
                continue
            installer_name = path.name.replace("rhcos-live-", "rhcos-installer-")
            installer_path = self.staging_dir / installer_name
            if not installer_path.exists():
                installer_path.symlink_to(os.readlink(path))
                self.logger.info("Legacy symlink %s -> %s", installer_name, os.readlink(path))

    def _generate_rhcos_id_txt(self):
        """Write rhcos-id.txt containing the RHCOS build ID."""
        id_path = self.staging_dir / "rhcos-id.txt"
        id_path.write_text(f"{self.build_id}\n")
        self.logger.info("Generated rhcos-id.txt with build ID %s", self.build_id)

    def _generate_sha256sum(self):
        """Generate sha256sum.txt for all files in the staging directory.

        Excludes rhcos-id.txt, sha256sum.txt itself, and any .gpg files.
        For symlinks, hash the target content (S3 stores symlinks as regular files).
        """
        entries = []
        for path in sorted(self.staging_dir.iterdir()):
            if path.name in ("rhcos-id.txt", "sha256sum.txt", "sha256sum.txt.gpg"):
                continue

            if path.is_symlink():
                target = self.staging_dir / os.readlink(path)
                if target.exists() and target.is_file():
                    sha = self._sha256_file(target)
                    entries.append((sha, path.name))
            elif path.is_file():
                sha = self._sha256_file(path)
                entries.append((sha, path.name))

        sha256sum_path = self.staging_dir / "sha256sum.txt"
        with open(sha256sum_path, "w") as f:
            for sha, name in entries:
                f.write(f"{sha}  {name}\n")
        self.logger.info("Generated sha256sum.txt with %d entries", len(entries))

    @staticmethod
    def _sha256_file(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    async def _sign_sha256sum(self):
        """GPG-sign sha256sum.txt via RADAS/UMB and write sha256sum.txt.gpg."""
        cert_file = os.environ.get("SIGNING_CERT")
        key_file = os.environ.get("SIGNING_KEY")
        if not cert_file or not key_file:
            self.logger.warning("SIGNING_CERT/SIGNING_KEY not set; skipping sha256sum.txt signing")
            return

        uri = constants.UMB_BROKERS[self.signing_env]
        sig_keyname = "redhatrelease2" if self.signing_env == "prod" else "beta2"
        sha256sum_path = self.staging_dir / "sha256sum.txt"
        gpg_path = self.staging_dir / "sha256sum.txt.gpg"

        self.logger.info("Signing sha256sum.txt with key %s via %s", sig_keyname, self.signing_env)

        async with AsyncSignatory(uri, cert_file, key_file, sig_keyname=sig_keyname) as signatory:
            with open(sha256sum_path, "rb") as in_file, open(gpg_path, "wb") as sig_file:
                await signatory.sign_message_digest(
                    product="openshift",
                    release_name=self.version,
                    artifact=in_file,
                    sig_file=sig_file,
                )

        self.logger.info("sha256sum.txt.gpg written to %s", gpg_path)

    async def _sync_to_s3(self, source: str, dest: str):
        """Sync local directory to an S3 destination (and Cloudflare)."""
        self.logger.info("Syncing %s -> %s", source, dest)
        await util.mirror_to_s3(
            source=source,
            dest=dest,
            dry_run=self.runtime.dry_run,
            delete=True,
        )

    async def _update_latest(self):
        """Update the latest directories, mirroring the logic from S3-rhcossync.sh emulateSymlinks."""
        source = str(self.staging_dir)

        if self.mirror_prefix == "pre-release":
            await self._update_latest_pre_release(source)
        else:
            await self._update_latest_stable(source)

    async def _update_latest_pre_release(self, source: str):
        """Update pre-release latest directories."""
        major_minor = self._extract_major_minor()
        next_major_minor = self._next_major_minor(major_minor)

        y_stream_latest = f"{S3_BUCKET}{self.base_dir}/{self.mirror_prefix}/latest-{major_minor}/"
        await self._sync_to_s3(source, y_stream_latest)

        if await self._should_update_global_latest(f"{self.mirror_prefix}/latest-{next_major_minor}"):
            global_latest = f"{S3_BUCKET}{self.base_dir}/{self.mirror_prefix}/latest/"
            await self._sync_to_s3(source, global_latest)
        else:
            self.logger.info("Skipping global latest update (newer Y-stream exists)")

    async def _update_latest_stable(self, source: str):
        """Update stable latest directories."""
        stable_latest = f"{S3_BUCKET}{self.base_dir}/{self.mirror_prefix}/latest/"
        await self._sync_to_s3(source, stable_latest)

        next_major_minor = self._next_major_minor(self._extract_major_minor())
        if await self._should_update_global_latest(next_major_minor):
            global_latest = f"{S3_BUCKET}{self.base_dir}/latest/"
            await self._sync_to_s3(source, global_latest)
        else:
            self.logger.info("Skipping global latest update (newer Y-stream exists)")

    async def _should_update_global_latest(self, check_prefix: str) -> bool:
        """Check if we should update the global 'latest' by seeing if a newer Y-stream exists."""
        from artcommonlib import exectools

        check_path = f"{S3_BUCKET}{self.base_dir}/{check_prefix}/"
        rc, stdout, stderr = await exectools.cmd_gather_async(["aws", "s3", "ls", check_path], check=False)
        if rc == 0:
            return not bool(stdout.strip())
        elif not stderr.strip():
            return True
        else:
            raise ChildProcessError(f"AWS S3 ls failed: {stderr}")

    def _extract_major_minor(self) -> str:
        match = re.match(r"(\d+\.\d+)", self.version)
        if not match:
            raise ValueError(f"Cannot extract major.minor from version: {self.version}")
        return match.group(1)

    @staticmethod
    def _next_major_minor(major_minor: str) -> str:
        major, minor = major_minor.split(".")
        return f"{major}.{int(minor) + 1}"


@cli.command("sync-rhcos", help="Sync RHCOS boot images to mirror.openshift.com and sign sha256sum.txt")
@click.option("--arch", required=True, help="Architecture (e.g., x86_64, aarch64)")
@click.option("--build-id", required=True, help="RHCOS build ID (e.g., 9.6.20250121-0)")
@click.option("--version", required=True, help="OCP release version (e.g., 4.19.0-ec.0)")
@click.option("--mirror-prefix", required=True, help="Mirror prefix directory (e.g., 4.19, pre-release)")
@click.option("--base-dir", required=True, help="Base S3 directory (e.g., /pub/openshift-v4/x86_64/dependencies/rhcos)")
@click.option("--synclist", required=True, help="Path to file listing artifact URLs to download")
@click.option("--no-latest", is_flag=True, default=False, help="Do not update the 'latest' directory")
@click.option(
    "--signing-env",
    type=click.Choice(["prod", "stage"]),
    default=None,
    help="Signing environment for GPG-signing sha256sum.txt (omit to skip signing)",
)
@pass_runtime
@click_coroutine
async def sync_rhcos(
    runtime: Runtime,
    arch: str,
    build_id: str,
    version: str,
    mirror_prefix: str,
    base_dir: str,
    synclist: str,
    no_latest: bool,
    signing_env: Optional[str],
):
    pipeline = SyncRhcosPipeline(
        runtime=runtime,
        arch=arch,
        build_id=build_id,
        version=version,
        mirror_prefix=mirror_prefix,
        base_dir=base_dir,
        synclist=synclist,
        no_latest=no_latest,
        signing_env=signing_env,
    )
    await pipeline.run()
