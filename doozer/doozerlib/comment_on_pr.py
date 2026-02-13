import os
from datetime import datetime, timezone

from artcommonlib import logutil
from artcommonlib.pushd import Dir
from dockerfile_parse import DockerfileParser
from ghapi.all import GhApi

from doozerlib.constants import BREWWEB_URL, GITHUB_TOKEN

LOGGER = logutil.get_logger(__name__)


class CommentOnPr:
    OLD_PR_AGE_IN_DAYS = 7  # A pull requests closed 7 days ago will be considered "old" and will not receive a comment

    def __init__(self, distgit_dir: str, nvr: str, build_id: str, distgit_name: str):
        self.distgit_dir = distgit_dir
        self.nvr = nvr
        self.build_id = build_id
        self.distgit_name = distgit_name
        self.token = os.getenv(GITHUB_TOKEN)
        self.owner = None
        self.repo = None
        self.commit = None
        self.gh_client = None  # GhApi client
        self.pr = None

    def list_comments(self):
        """
        List the comments in a PR
        """
        # https://docs.github.com/rest/reference/issues#list-issue-comments
        return self.gh_client.issues.list_comments(issue_number=self.pr["number"], per_page=100)

    def check_if_comment_exist(self):
        """
        Check if the same comment already exists in the PR
        """
        issue_comments = self.list_comments()
        for issue_comment in issue_comments:
            if (
                "[ART PR BUILD NOTIFIER]" in issue_comment["body"]
                and f"Distgit: {self.distgit_name}" in issue_comment["body"]
            ):
                return True
        return False

    def post_comment(self):
        """
        Post the comment in the PR
        """
        # https://docs.github.com/rest/reference/issues#create-an-issue-comment

        # Message to be posted to the comment
        comment = (
            "**[ART PR BUILD NOTIFIER]**\n\n"
            + f"Distgit: {self.distgit_name}\n"
            + "This PR has been included in build "
            + f"[{self.nvr}]({BREWWEB_URL}/buildinfo"
            + f"?buildID={self.build_id}).\n"
            + "All builds following this will include this PR."
        )

        self.gh_client.issues.create_comment(issue_number=self.pr["number"], body=comment)
        LOGGER.info(
            f"[{self.distgit_name}] Successful commented on PR: https://github.com/{self.owner}/{self.repo}/pull/{self.pr['number']}"
        )

    def set_pr_from_commit(self):
        """
        Get the PR from the merge commit
        """
        # https://docs.github.com/rest/commits/commits#list-pull-requests-associated-with-a-commit
        prs = self.gh_client.repos.list_pull_requests_associated_with_commit(self.commit)
        if len(prs) == 1:
            # self._logger.info(f"PR from merge commit {sha}: {pull_url}")
            self.pr = prs[0]
            return
        raise Exception(f"Multiple PRs found for merge commit {self.commit}")

    def set_repo_details(self):
        """
        Get the owner, commit and repo from the dfp label
        """
        with Dir(self.distgit_dir):
            dfp = DockerfileParser(str(Dir.getpath().joinpath("Dockerfile")))

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
        self.gh_client = GhApi(owner=self.owner, repo=self.repo, token=self.token)

    def run(self):
        self.set_repo_details()

        if self.repo == "ocp-build-data":
            LOGGER.warning("Skipping commenting on PRs in ocp-build-data")
            return

        self.set_github_client()
        self.set_pr_from_commit()

        # Skip if the PR is too old
        if self.pr and self.pr["closed_at"]:
            closed_at = datetime.fromisoformat(self.pr["closed_at"].replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - closed_at
            if age.days >= self.OLD_PR_AGE_IN_DAYS:
                LOGGER.warning("Skipped PR build notifier reporting on old PR %s", self.pr["html_url"])
                return

        # Check if comment doesn't already exist already. Then post comment
        if not self.check_if_comment_exist():
            self.post_comment()
