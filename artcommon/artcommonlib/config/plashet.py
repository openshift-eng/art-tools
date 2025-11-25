from typing import List

from pydantic import BaseModel, Field


class PlashetConfig(BaseModel):
    base_dir: str
    plashet_dir: str
    create_symlinks: bool = False
    symlink_name: str = 'latest'
    create_repo_subdirs: bool = False
    repo_subdir: str = 'os'
    arches: List[str] | None = None
    base_url: str = 'https://ocp-artifacts.engineering.redhat.com/pub/RHOCP/plashets'
    download_url: str | None = Field(
        default=None, deprecated=True, description='Deprecated: use Repo.construct_download_url() method instead'
    )
