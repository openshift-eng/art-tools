import asyncio
from dataclasses import asdict, dataclass
import logging
from typing import Any, Dict, List, Set
import dnf
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import hawkey
import yaml
from pathlib import Path

def strip_suffix(s: str, suffix: str) -> str:
    """
    Remove the given suffix from a string if it exists.

    Args:
        s: The input string.
        suffix: The suffix to strip from the string.

    Returns:
        The string with the suffix removed, if it was present.
    """
    if s.endswith(suffix):
        return s[: -len(suffix)]
    return s


@dataclass(frozen=True, order=True)
class PackageItem:
    """
    Data class to represent a resolved package and its metadata.
    """
    url: str
    repoid: str
    size: int
    checksum: str
    name: str
    evr: str
    sourcerpm: str

    @classmethod
    def from_dnf(cls, pkg):
        """
        Create a PackageItem instance from a DNF package object.

        Args:
            pkg: The DNF package object.

        Returns:
            A new PackageItem instance.
        """
        return cls(
            pkg.remote_location(),
            pkg.repoid,
            pkg.downloadsize,
            f"{hawkey.chksum_name(pkg.chksum[0])}:{pkg.chksum[1].hex()}",
            pkg.name,
            pkg.evr,
            pkg.sourcerpm,
        )

    def as_dict(self):
        """
        Convert the package item to a dictionary. Exclude 'sourcerpm' if empty.

        Returns:
            A dictionary representation of the package item.
        """
        d = asdict(self)
        if not self.sourcerpm:
            del d["sourcerpm"]
        return d


class RPMLockfileGenerator:
    """
    Class responsible for generating a YAML-based RPM lockfile from package data.
    """
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)

    def generate(self, arches_data: Dict[str, Dict[str, Any]]) -> None:
        """
        Generate an rpm lockfile YAML from installed packages per architecture.

        Args:
            arches_data: Dict keyed by architecture with package and module metadata.
        """
        lockfile = {
            "version": 1,
            "lockfileVendor": "redhat",
            "arches": {},
        }

        for arch, data in arches_data.items():
            lockfile["arches"][arch] = {
                "packages": data.get("packages", []),
                "source": data.get("source", []),
                "module_metadata": data.get("module_metadata", []),
            }

        self._write_yaml(lockfile)

    def _write_yaml(self, data: dict) -> None:
        """
        Write the YAML lockfile to disk.

        Args:
            data: The YAML-serializable data to write.
        """
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "w") as f:
            yaml.dump(data, f, sort_keys=False)


class DnfManager:
    """
    Manages DNF operations across multiple architectures using isolated installroots.
    """
    def __init__(self, repodir: str, installroot_base: str, arches: list[str]):
        """
        Initialize a multi-arch DNF manager.

        Args:
            repodir: Directory containing .repo files.
            installroot_base: Base path for per-arch installroots.
            arches: List of architectures to manage.
        """
        self.repodir = repodir
        self.installroot_base = installroot_base
        self.arches = arches
        self._bases = {}

        # Create DNF base instances in parallel for each architecture
        self._create_bases_parallel()

    def _create_base(self, arch: str) -> dnf.Base:
        """
        Initialize and configure a DNF base for a given architecture.

        Args:
            arch: The architecture for which to create the base.

        Returns:
            A configured dnf.Base instance.
        """
        base = dnf.Base()
        base.conf.installroot = os.path.join(self.installroot_base, arch)
        base.conf.reposdir = [self.repodir]
        base.conf.cachedir = os.path.join(self.installroot_base, f"cache-{arch}")
        base.conf.arch = arch
        base.conf.assumeyes = True
        base.conf.gpgcheck = False

        base.read_all_repos()
        base.fill_sack()

        return base

    def _create_bases_parallel(self):
        """
        Create DNF base instances for all architectures in parallel.
        """
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(self._create_base, arch): arch for arch in self.arches}
            for future in as_completed(futures):
                arch = futures[future]
                try:
                    base = future.result()
                    self._bases[arch] = base
                except Exception as e:
                    print(f"Failed to initialize base for arch {arch}: {e}")

    def get_base(self, arch: str) -> dnf.Base:
        """
        Retrieve the DNF base for the given architecture.

        Args:
            arch: The architecture identifier.

        Returns:
            The corresponding dnf.Base instance.

        Raises:
            KeyError: If the base for the given arch is not initialized.
        """
        if arch not in self._bases:
            raise KeyError(f"No base found for arch {arch}")
        return self._bases[arch]

    async def install_packages_for_arches(
        self,
        install_packages: Dict[str, List[str]],
        module_enable: Dict[str, Set[str]] = {},
        module_disable: Dict[str, Set[str]] = {},
        allow_erasing: bool = False,
        no_sources: bool = False,
    ) -> Dict[str, Dict]:
        """
        Resolve and install packages for multiple architectures concurrently.

        Args:
            install_packages: Dict of architectures and list of package names to install.
            module_enable: Optional dict mapping arch to modules to enable.
            module_disable: Optional dict mapping arch to modules to disable.
            allow_erasing: Whether to allow package erasure when resolving.
            no_sources: If True, skip collecting source RPMs.

        Returns:
            Dict mapping architecture to result with packages, sources, module metadata.
        """

        async def process_single_arch(arch: str) -> tuple[str, dict[str, list[Any]]]:
            """
            Process package resolution and installation for a single architecture.

            Args:
                arch: The architecture identifier.

            Returns:
                A tuple containing the architecture and a dictionary with install data.
            """
            base = self.get_base(arch)
            mb = dnf.module.module_base.ModuleBase(base)

            # Enable and disable modules
            mb.disable(module_disable.get(arch, set()))
            mb.enable(module_enable.get(arch, set()))

            try:
                base.install_specs(install_packages.get(arch, []))
            except dnf.exceptions.MarkingErrors as exc:
                logging.error(exc.value)
                raise RuntimeError(f"[{arch}] DNF install error: {exc}")

            base.resolve(allow_erasing=allow_erasing)

            packages = set()
            sources = set()
            module_metadata = []

            modular_packages = {
                nevra
                for module in mb.get_modules("*")[0]
                for nevra in module.getArtifacts()
            }
            modular_repos = set()

            for pkg in base.transaction.install_set:
                if f"{pkg.name}-{pkg.e}:{pkg.v}-{pkg.r}.{pkg.a}" in modular_packages:
                    modular_repos.add(pkg.repoid)
                packages.add(PackageItem.from_dnf(pkg))

                if not no_sources:
                    n, v, r = strip_suffix(pkg.sourcerpm, ".src.rpm").rsplit("-", 2)
                    results = base.sack.query().filter(name=n, version=v, release=r, arch="src")
                    if results:
                        sources.add(PackageItem.from_dnf(results[0]))
                    else:
                        logging.warning("[%s] No sources found for %s", arch, pkg)

            for repoid in modular_repos:
                repo = base.repos[repoid]
                modulemd_path = repo.get_metadata_path("modules")
                if not modulemd_path:
                    raise RuntimeError(f"[{arch}] Modular package from repo without metadata")
                module_metadata.append({
                    "url": repo.remote_location(f"repodata/{os.path.basename(modulemd_path)}"),
                    "repoid": repo.id,
                    "size": os.stat(modulemd_path).st_size,
                    "checksum": f"sha256:{utils.hash_file(modulemd_path)}",
                })

            return arch, {
                "packages": sorted([p.as_dict() for p in packages], key=lambda x: x["name"]),
                "source": sorted([s.as_dict() for s in sources], key=lambda x: x["name"]),
                "module_metadata": sorted(module_metadata, key=lambda x: x["url"]),
            }

        # Run per-arch resolution in parallel using asyncio
        tasks = [process_single_arch(arch) for arch in install_packages]
        results = await asyncio.gather(*tasks)

        return dict(results)
