"""
Data models for RPM lockfile generation.
"""

from pydantic import BaseModel, Field, model_serializer

from doozerlib.lockfile_prototype.constants import LOCKFILE_VENDOR, LOCKFILE_VERSION

# ── rpms.in.yaml input models ──


class RepoEntry(BaseModel):
    """
    A repository entry for rpms.in.yaml.
    """

    repoid: str
    baseurl: str
    options: dict[str, str | int] = Field(default_factory=dict)

    @model_serializer
    def _serialize(self) -> dict:
        d: dict = {"repoid": self.repoid, "baseurl": self.baseurl}
        d.update(self.options)
        return d


class ArchSpecificPackage(BaseModel):
    """
    A package restricted to a specific architecture.
    """

    name: str
    arches: dict[str, str]


class RpmsInConfig(BaseModel):
    """
    Input configuration for rpm-lockfile-prototype (rpms.in.yaml structure).
    """

    arches: list[str]
    contentOrigin: dict[str, list[RepoEntry]]
    packages: list[str | ArchSpecificPackage] = Field(default_factory=list)
    reinstallPackages: list[str] = Field(default_factory=list)
    upgradePackages: list[str] = Field(default_factory=list)
    moduleEnable: list[str] = Field(default_factory=list)
    excludePackages: list[str] = Field(default_factory=list)
    packagesFromContainerfile: dict | str | None = None


# ── Lockfile output models ──


class PackageEntry(BaseModel):
    """
    A resolved RPM package in the lockfile.
    """

    url: str
    repoid: str
    name: str | None = None
    evr: str | None = None
    size: int | None = None
    checksum: str | None = None
    sourcerpm: str | None = None


class ModuleEntry(BaseModel):
    """
    Module metadata in the lockfile.
    """

    url: str | None = None
    repoid: str | None = None
    name: str | None = None
    stream: str | None = None
    version: str | None = None
    size: int | None = None
    checksum: str | None = None


class ArchResult(BaseModel):
    """
    Resolved packages for a single architecture.
    """

    arch: str
    packages: list[PackageEntry] = Field(default_factory=list)
    source: list[PackageEntry] = Field(default_factory=list)
    module_metadata: list[ModuleEntry] = Field(default_factory=list)


class LockfileData(BaseModel):
    """
    Complete RPM lockfile structure.
    """

    lockfileVersion: int = LOCKFILE_VERSION
    lockfileVendor: str = LOCKFILE_VENDOR
    arches: list[ArchResult] = Field(default_factory=list)
