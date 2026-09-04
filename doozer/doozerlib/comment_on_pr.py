from datetime import datetime, timezone

from artcommonlib import logutil
from artcommonlib.github_auth import get_github_client_for_org
from artcommonlib.pushd import Dir
from dockerfile_parse import DockerfileParser

from doozerlib.constants import BREWWEB_URL

LOGGER = logutil.get_logger(__name__)


class CommentOnPr:
    OLD_PR_AGE_IN_DAYS = 7  # A pull requests closed 7 days ago will be considered "old" and will not receive a comment

    def __init__(self, distgit_dir: str, nvr: str, build_id: str, distgit_name: str):
        self.distgit_dir = distgit_dir
        self.nvr = nvr
        self.build_id = build_id
        self.distgit_name = distgit_name
        self.owner = None
        self.repo = None
        self.commit = None
        self.gh_repo = None  # PyGithub Repository object
        self.pr = None

    def list_comments(self):
        """
        List the comments in a PR
        """
        return list(self.gh_repo.get_issue(self.pr.number).get_comments())

    def check_if_comment_exist(self):
        """
        Check if the same comment already exists in the PR
        """
        issue_comments = self.list_comments()
        for issue_comment in issue_comments:
            if (
                "[ART PR BUILD NOTIFIER]" in issue_comment.body
                and f"Distgit: {self.distgit_name}" in issue_comment.body
            ):
                return True
        return False

    def post_comment(self):
        """
        Post the comment in the PR
        """
        comment = (
            "**[ART PR BUILD NOTIFIER]**\n\n"
            + f"Distgit: {self.distgit_name}\n"
            + "This PR has been included in build "
            + f"[{self.nvr}]({BREWWEB_URL}/buildinfo"
            + f"?buildID={self.build_id}).\n"
            + "All builds following this will include this PR."
        )

        self.gh_repo.get_issue(self.pr.number).create_comment(body=comment)
        LOGGER.info(
            f"[{self.distgit_name}] Successful commented on PR: https://github.com/{self.owner}/{self.repo}/pull/{self.pr.number}"
        )

    def set_pr_from_commit(self):
        """
        Get the PR from the merge commit
        """
        prs = list(self.gh_repo.get_commit(self.commit).get_pulls())
        if len(prs) == 1:
            self.pr = prs[0]
            return
        if not prs:
            raise LookupError(f"No PR found for merge commit {self.commit}")
        raise RuntimeError(f"Multiple PRs found for merge commit {self.commit}")

    def set_repo_details(self):
        """
        Get the owner, commit and repo from the dfp label
        """
        with Dir(self.distgit_dir):
            dfp = DockerfileParser(str(Dir.getpath().joinpath('Dockerfile')))

            # eg: "https://github.com/openshift/origin/commit/660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5"
            source_commit_url = dfp.labels["io.openshift.build.commit.url"]
            url_split = source_commit_url.split("/")
            commit = url_split[-1]  # eg: 660e0c785a2c9b1fd5fad33cbcffd77a6d84ccb5
            repo = url_split[-3]  # eg: origin
            owner = url_split[-4]  # eg: openshift

            self.owner = owner
            self.commit = commit
            self.repo = repo

    def set_github_client(self):
        """
        Set the gh client after the get_source_details function is run
        """
        self.gh_repo = get_github_client_for_org(self.owner).get_repo(f"{self.owner}/{self.repo}")

    def run(self):
        self.set_repo_details()

        if self.repo == "ocp-build-data":
            LOGGER.warning("Skipping commenting on PRs in ocp-build-data")
            return

        self.set_github_client()
        self.set_pr_from_commit()

        # Skip if the PR is too old
        if self.pr and self.pr.closed_at:
            age = datetime.now(timezone.utc) - self.pr.closed_at
            if age.days >= self.OLD_PR_AGE_IN_DAYS:
                LOGGER.warning("Skipped PR build notifier reporting on old PR %s", self.pr.html_url)
                return

        # Check if comment doesn't already exist already. Then post comment
        if not self.check_if_comment_exist():
            self.post_comment()
