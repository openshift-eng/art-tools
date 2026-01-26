from tempfile import TemporaryDirectory

import pygit2

from . import support


def validate(file, data, group_cfg):
    namespace = support.get_namespace(data, file)
    repository = support.get_repository_name(file)
    distgit_host = "pkgs.devel.redhat.com"
    repo_name = f"{namespace}/{repository}"
    repo_url = f"https://{distgit_host}/git/{repo_name}"
    branch = support.get_distgit_branch(data, group_cfg)

    with TemporaryDirectory() as repo_dir:
        try:
            print(f"Cloning {repo_url} into {repo_dir}")
            pygit2.clone_repository(repo_url, repo_dir, checkout_branch=branch)
            return repo_url, None
        except Exception as e:
            return repo_url, f"DistGit repository or branch not exist: {e}"
