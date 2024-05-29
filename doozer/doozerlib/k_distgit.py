import os
import pathlib
from dockerfile_parse import DockerfileParser
from artcommonlib import assertion, logutil, build_util, exectools
from artcommonlib.pushd import Dir
from doozerlib.distgit import ImageDistGitRepo
from doozerlib import util


class KonfluxImageDistGitRepo(ImageDistGitRepo):
    """
    It's not technically distgit anymore, but using the same name for simplicity
    """
    def __init__(self, metadata, autoclone=True, dry_run=False):
        super(KonfluxImageDistGitRepo, self).__init__(metadata, autoclone=False)

        # Using k_distgits_dir which points to the new konflux dir
        self.distgit_dir = os.path.join(self.runtime.k_distgits_dir, self.metadata.distgit_key)
        self.dg_path = pathlib.Path(self.distgit_dir)
        self.upstream_branch = ""
        self.is_konflux = True
        self.dry_run = dry_run

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

    def push(self):
        """
        Push to the appropriate branch on openshift-priv
        """
        # Figure out which branch to push to
        self.upstream_branch = f"art-<{self.runtime.group}>-assembly-<{self.runtime.assembly}>-dgk-<{self.name}>"
        self.logger.info(f"Setting upstream branch to: {self.upstream_branch}")

        if self.dry_run:
            self.logger.info(f"Would have pushed to branch {self.upstream_branch} in openshift-priv")
            return
        with Dir(self.dg_path):
            self.logger.info("Pushing konflux repository %s", self.name)
            # When initializing new release branches, a large amount of data needs to
            # be pushed. If every repo within a release is being pushed at the same
            # time, a single push invocation can take hours to complete -- making the
            # timeout value counterproductive. Limit to 5 simultaneous pushes.
            with self.runtime.get_named_semaphore('k_distgit::push', count=5):
                exectools.cmd_assert(f"git checkout -b {self.upstream_branch}")
                exectools.cmd_assert(f"git push --set-upstream origin {self.upstream_branch} -f", retries=3)

        return self.metadata, True

    def add_distgits_diff(self, diff):
        return self.runtime.add_distgits_diff(self.metadata.distgit_key, diff, konflux=True)

    def update_distgit_dir(self, version, release, prev_release=None, force_yum_updates=False):
        version, release = super().update_distgit_dir(version="v0.0.0", release=release, prev_release=prev_release, force_yum_updates=force_yum_updates)

        # DNF repo injection steps for Konflux
        dfp = DockerfileParser(path=str(self.dg_path.joinpath('Dockerfile')))
        # Populating the repo file needs to happen after every FROM before the original Dockerfile can invoke yum/dnf.
        dfp.add_lines(
            "\n# Start Konflux-specific steps",
            "RUN mkdir -p /tmp/yum_temp; mv /etc/yum.repos.d/*.repo /tmp/yum_temp/",
            "COPY .oit/signed.repo /etc/yum.repos.d/",
            "ADD https://certs.corp.redhat.com/certs/Current-IT-Root-CAs.pem /tmp",
            "# End Konflux-specific steps\n\n",
            at_start=True,
            all_stages=True,
        )

        # Put back original yum config
        dfp.add_lines(
            "\n# Start Konflux-specific steps",
            "RUN cp /tmp/yum_temp/* /etc/yum.repos.d/",
            "# End Konflux-specific steps\n\n"
        )
        return version, release

    def wait_for_rebase(self, image_name, terminate_event):
        """ Wait for image_name to be rebased. """
        image = self.runtime.resolve_image(image_name, False)
        if image is None:
            self.logger.info("Skipping image rebase since it is not included: %s" % image_name)
            return
        dgr = image.k_distgit_repo()
        self.logger.info("Waiting for image rebase: %s" % image_name)
        dgr.rebase_event.wait()
        if not dgr.rebase_status:  # failed to rebase
            raise IOError(f"Error rebasing image: {self.metadata.qualified_name} ({image_name} was waiting)")
        self.logger.info("Image rebase for %s completed. Stop waiting." % image_name)
        if terminate_event.is_set():
            raise KeyboardInterrupt()

    def _generate_repo_conf(self):
        """
        Generates a repo file in .oit/repo.conf
        """

        self.logger.debug("Generating repo file for Dockerfile {}".format(self.metadata.distgit_key))

        # Make our metadata directory if it does not exist
        util.mkdirs(self.dg_path.joinpath('.oit'))

        repos = self.runtime.repos
        enabled_repos = self.config.get('enabled_repos', [])
        non_shipping_repos = self.config.get('non_shipping_repos', [])

        for t in repos.repotypes:
            with self.dg_path.joinpath('.oit', f'{t}.repo').open('w', encoding="utf-8") as rc:
                content = repos.repo_file(t, enabled_repos=enabled_repos, konflux=True)
                rc.write(content)

        with self.dg_path.joinpath('content_sets.yml').open('w', encoding="utf-8") as rc:
            rc.write(repos.content_sets(enabled_repos=enabled_repos, non_shipping_repos=non_shipping_repos))
