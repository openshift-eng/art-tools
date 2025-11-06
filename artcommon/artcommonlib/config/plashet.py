from typing import List

from pydantic import BaseModel, HttpUrl


class PlashetConfig(BaseModel):
    base_dir: str
    plashet_dir: str
    create_symlinks: bool = False
    symlink_name: str = 'latest'
    create_repo_subdirs: bool = False
    repo_subdir: str = 'os'
    arches: List[str] | None = None
    download_url: str
