import os
import pathlib
from typing import Union
from dockerfile_parse import DockerfileParser
from artcommonlib import assertion, logutil, build_util, exectools
from artcommonlib.pushd import Dir
from doozerlib.distgit import ImageDistGitRepo
from doozerlib.metadata import Metadata


class KonfluxImageDistGitRepo(ImageDistGitRepo):
    """
    It's not technically distgit anymore, but using the same name for simplicity
    """
    def __init__(self, metadata, autoclone=True):
        super(KonfluxImageDistGitRepo, self).__init__(metadata, autoclone=False)

        # Using k_distgits_dir which points to the new konflux dir
        self.distgit_dir = os.path.join(self.runtime.k_distgits_dir, self.metadata.distgit_key)
        self.dg_path = pathlib.Path(self.distgit_dir)

        if autoclone:
            self.clone()

    def clone(self) -> None:
        # Refresh if dir already exists, instead of cloning
        if os.path.isdir(self.distgit_dir):
            with Dir(self.distgit_dir):
                self.logger.info(f"{self.distgit_dir} dir already exists, refreshing git instead of cloning")
                exectools.cmd_assert('git fetch --all', retries=3)
                exectools.cmd_assert('git reset --hard @{upstream}', retries=3)
            return

        branch = self.metadata.config.content.source.git.branch.target
        url = self.metadata.config.content.source.git.url

        git_args = ["--no-single-branch", "--branch", branch]
        self.runtime.git_clone(url, self.distgit_dir, gitargs=git_args)

    def push(self) -> Union[Metadata, bool]:
        """
        Push to the appropriate branch on openshift-priv
        """
        # Figure out which branch to push to
        branch = f"art-<{self.runtime.group}>-assembly-<{self.runtime.assembly}>-dgk-<{self.name}>"
        self.logger.info(f"Setting upstream branch to: {branch}")

        with Dir(self.dg_path):
            self.logger.info("Pushing konflux repository %s", self.name)
            try:
                # When initializing new release branches, a large amount of data needs to
                # be pushed. If every repo within a release is being pushed at the same
                # time, a single push invocation can take hours to complete -- making the
                # timeout value counterproductive. Limit to 5 simultaneous pushes.
                with self.runtime.get_named_semaphore('k_distgit::push', count=5):
                    exectools.cmd_assert(f"git checkout -b {branch}")
                    exectools.cmd_assert(f"git push --set-upstream origin {branch}", retries=3)
            except IOError as e:
                return self.metadata, repr(e)
        return self.metadata, True

    def add_distgits_diff(self, diff):
        return self.runtime.add_distgits_diff(self.metadata.distgit_key, diff, konflux=True)

    def update_distgit_dir(self, version, release, prev_release=None, force_yum_updates=False):
        version, release = super().update_distgit_dir(version="v0.0.0", release=release, prev_release=prev_release, force_yum_updates=force_yum_updates)

        # DNF repo injection steps for Konflux
        dfp = DockerfileParser(path=str(self.dg_path.joinpath('Dockerfile')))
        # Populating the repo file needs to happen after every FROM before the original Dockerfile can invoke yum/dnf.
        dfp.add_lines(
            "# Konflux-specific steps",
            "RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/",
            "COPY .oit/signed.repo /etc/yum.repos.d/",
            "RUN cp /tmp/yum_temp/* /etc/yum.repos.d/",
            "# end Konflux-specific steps",
            at_start=True,
            all_stages=True,
        )

        return version, release
