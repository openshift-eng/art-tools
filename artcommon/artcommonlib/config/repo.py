from typing import Literal

from pydantic import BaseModel, RootModel

RepoType = Literal['external', 'plashet']
BrewArch = Literal['x86_64', 's390x', 'ppc64le', 'aarch64']


class PlashetRepo(BaseModel):
    disabled: bool = False
    slug: str | None = None
    assembly_aware: bool = False
    embargo_aware: bool = False
    include_embargoed: bool = False
    include_previous_packages: list[str] = []
    arches: list[BrewArch] | None = None
    source: "BrewSource"


class BrewSource(BaseModel):
    type: Literal['brew']
    from_tags: list["BrewTag"]
    embargoed_tags: list[str] = []


class BrewTag(BaseModel):
    name: str
    product_version: str
    release_tag: str | None = None
    inherit: bool = False


class ContentSet(BaseModel):
    optional: bool = False
    default: str | None = None
    x86_64: str | None = None
    s390x: str | None = None
    ppc64le: str | None = None
    aarch64: str | None = None


class RepoSync(BaseModel):
    enabled: bool = False
    latest_only: bool = True


class Repo(BaseModel):
    name: str
    disabled: bool = False
    type: RepoType
    plashet: PlashetRepo | None = None
    conf: dict | None = None
    content_set: ContentSet | None = None
    reposync: RepoSync = RepoSync()


RepoList = RootModel[list[Repo]]
