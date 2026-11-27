"""Read immutable, cached RPM inventories from container images.

The cache is shared by all callers in the process, including concurrent
rebases. Callers must resolve tags before requesting an inventory.
"""

import asyncio
import hashlib
import logging
import os
import re
import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import requests
from artcommonlib.arch_util import brew_arch_for_go_arch, go_arch_for_brew_arch
from artcommonlib.exectools import cmd_gather_async
from artcommonlib.util import oc_image_info_for_arch_async
from async_lru import alru_cache

from doozerlib.constants import BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL
from doozerlib.lockfile_prototype.constants import (
    JENKINS_CACHE_DIR,
    RPM_PSEUDO_PACKAGES,
    RPMDB_CACHE_SUBDIR,
    SYSTEM_PYTHON,
)
from doozerlib.lockfile_prototype.utils import build_env

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class InstalledRpm:
    """An installed RPM, retaining epoch, architecture and source RPM."""

    name: str
    epoch: int
    version: str
    release: str
    arch: str
    source_rpm: str

    @property
    def evr(self) -> str:
        return f"{self.epoch}:{self.version}-{self.release}"


async def get_image_rpm_inventory(image_pullspec: str, arch: str) -> tuple[InstalledRpm, ...]:
    """Return installed RPMs for a digest-pinned image and architecture.

    Results are immutable and shared across image helpers. Concurrent requests
    for the same digest and architecture share one extraction. Failures are
    raised and are not cached; an empty tuple means a successful empty query.
    Architecture aliases and ART's Brew registry proxy are normalized before
    entering the cache. No container execution or foreign-architecture emulator
    is required.
    """
    if "@sha256:" not in image_pullspec:
        raise ValueError("RPM inventory caching requires a digest-pinned image")
    pullspec = image_pullspec.replace(BREW_REGISTRY_BASE_URL, REGISTRY_PROXY_BASE_URL)
    return await _get_image_rpm_inventory(pullspec, brew_arch_for_go_arch(arch))


@alru_cache(maxsize=256)
async def _get_image_rpm_inventory(pullspec: str, arch: str) -> tuple[InstalledRpm, ...]:
    """Extract and query once per immutable image/architecture."""
    temporary_parent = JENKINS_CACHE_DIR if os.environ.get("JENKINS_HOME") else None
    if temporary_parent:
        temporary_parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="image-rpms-", dir=temporary_parent) as temporary_dir:
        root = Path(temporary_dir)
        cache_home = (
            JENKINS_CACHE_DIR
            if os.environ.get("JENKINS_HOME")
            else Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
        )
        cached_root = cache_home / RPMDB_CACHE_SUBDIR / arch / pullspec.split("@", 1)[1]
        cached_database = _find_database(cached_root)
        if cached_database:
            try:
                # Query a copy so RPM never writes into another process's cache.
                database = root / "cached-rpmdb"
                await asyncio.to_thread(shutil.copytree, cached_database, database)
                return await _query_inventory(database)
            except Exception:
                LOGGER.debug("Cached RPMDB unusable for %s [%s]", pullspec, arch, exc_info=True)
        try:
            info = await oc_image_info_for_arch_async(
                pullspec,
                go_arch=go_arch_for_brew_arch(arch),
                registry_config=os.environ.get("QUAY_AUTH_FILE") or os.environ.get("REGISTRY_AUTH_FILE"),
            )
            layer = _ostree_rpm_layer(info)
            if layer:
                database = await asyncio.to_thread(_extract_public_rpm_layer, pullspec, layer, root)
                return await _query_inventory(database)
        except Exception:
            # Public RHCOS exports have a small RPM chunk. Private registries,
            # different layouts and failed queries use the authenticated tool.
            LOGGER.debug("RPM layer shortcut unavailable for %s [%s]", pullspec, arch, exc_info=True)

        fallback = root / "fallback"
        env = build_env()
        env["TMPDIR"] = str(root)
        if os.environ.get("JENKINS_HOME"):
            env["XDG_CACHE_HOME"] = str(JENKINS_CACHE_DIR)
        # Reuse the extractor and persistent RPMDB cache already used by the
        # prototype resolver instead of downloading the base a second time.
        await cmd_gather_async(
            [
                SYSTEM_PYTHON,
                "-c",
                "import sys; from rpm_lockfile.containers import setup_rpmdb; "
                "setup_rpmdb(sys.argv[1], sys.argv[2], sys.argv[3])",
                str(fallback),
                pullspec,
                arch,
            ],
            env=env,
        )
        database = _find_database(fallback)
        if database:
            return await _query_inventory(database)
        raise RuntimeError(f"No RPM database found in {pullspec} [{arch}]")


def _find_database(root: Path) -> Path | None:
    """Locate an existing RPMDB without confusing absence with an empty query."""
    for path in ("usr/share/rpm", "usr/lib/sysimage/rpm", "var/lib/rpm", "files/lib/sysimage/rpm"):
        database = root / path
        if any((database / name).is_file() for name in ("rpmdb.sqlite", "Packages")):
            return database
    return None


def _ostree_rpm_layer(info: dict) -> str | None:
    """Identify the RPM chunk only in an unmodified OSTree export.

    Chunk order varies by architecture; never assume a fixed layer number.
    Filesystem-changing commands after the export require the full extractor.
    """
    config = info.get("config", {})
    history = [entry for entry in config.get("history", []) if not entry.get("empty_layer")]
    layers = info.get("layers", [])
    if not history or len(history) != len(layers):
        return None
    if not history[0].get("created_by", "").startswith("ostree export of commit "):
        return None
    exported_at = history[0].get("created")
    if not exported_at:
        return None
    candidates = []
    nevra = r"[A-Za-z0-9+_.-]+-[^\s]+-[^\s]+\.(?:x86_64|aarch64|ppc64le|s390x|i686|noarch)"
    for index, entry in enumerate(history):
        command = entry.get("created_by", "")
        if index == 0 or command.startswith('/bin/sh -c #(nop) LABEL '):
            continue
        is_chunk = (
            re.fullmatch(rf"{nevra}(?: and {nevra})*", command)
            or re.fullmatch(r"\d+ components", command)
            or command == "Reserved for new packages"
            or re.fullmatch(r"initramfs \(kernel [^ )]+\) and rpmostree-unpackaged-content", command)
        )
        if entry.get("created") == exported_at and is_chunk:
            if re.match(r"^rpm-\d", command):
                candidates.append(layers[index]["digest"])
        else:
            return None
    return candidates[0] if len(candidates) == 1 else None


def _extract_public_rpm_layer(pullspec: str, digest: str, root: Path) -> Path:
    """Read a public OCI blob and its RPMDB, including OSTree hardlinks.

    Only the database's bytes are written; tar paths are never extracted into
    the host filesystem. Private registry authentication is left to the
    existing prototype extractor in the fallback path.
    """
    repository = pullspec.split("@", 1)[0]
    host, repository = repository.split("/", 1)
    # Strip an optional tag without confusing the registry's port number.
    repository = repository.rsplit(":", 1)[0]
    url = f"https://{host}/v2/{repository}/blobs/{digest}"
    archive_path = root / "rpm-layer.tar.gz"
    with requests.Session() as session:
        response = session.get(url, stream=True, timeout=60)
        if response.status_code == 401:
            challenge = response.headers.get("Www-Authenticate", "")
            if not challenge.lower().startswith("bearer "):
                response.raise_for_status()
            fields = requests.utils.parse_dict_header(challenge.split(" ", 1)[1])
            response.close()
            if not fields["realm"].startswith("https://"):
                raise ValueError("Registry authentication requires HTTPS")
            auth = session.get(fields["realm"], params={k: v for k, v in fields.items() if k != "realm"}, timeout=30)
            auth.raise_for_status()
            token = auth.json().get("token") or auth.json()["access_token"]
            response = session.get(url, headers={"Authorization": f"Bearer {token}"}, stream=True, timeout=60)
        with response:
            response.raise_for_status()
            checksum = hashlib.sha256()
            with archive_path.open("wb") as output:
                for block in response.iter_content(1024 * 1024):
                    checksum.update(block)
                    output.write(block)
    if digest != f"sha256:{checksum.hexdigest()}":
        raise ValueError("RPM layer checksum mismatch")
    with tarfile.open(archive_path) as archive:
        members = [m for m in archive.getmembers() if m.name.removeprefix("./") == "usr/share/rpm/rpmdb.sqlite"]
        if len(members) != 1 or not (members[0].isfile() or members[0].islnk()):
            raise ValueError("RPM layer does not contain a unique RPM database")
        source = archive.extractfile(members[0])
        if source is None:
            raise ValueError("Cannot read RPM database")
        database = root / "rpmdb"
        database.mkdir()
        with source, (database / "rpmdb.sqlite").open("wb") as output:
            shutil.copyfileobj(source, output)
    return database


async def _query_inventory(database: Path) -> tuple[InstalledRpm, ...]:
    """Query full RPM identities, excluding RPM's pseudo-packages."""
    _, stdout, _ = await cmd_gather_async(
        [
            "rpm",
            "--dbpath",
            str(database),
            "-qa",
            "--qf",
            r"%{NAME}\t%{EPOCHNUM}\t%{VERSION}\t%{RELEASE}\t%{ARCH}\t%{SOURCERPM}\n",
        ]
    )
    packages = set()
    for line in stdout.splitlines():
        name, epoch, version, release, arch, source_rpm = line.split("\t")
        if name not in RPM_PSEUDO_PACKAGES:
            packages.add(InstalledRpm(name, int(epoch), version, release, arch, source_rpm))
    return tuple(sorted(packages))
