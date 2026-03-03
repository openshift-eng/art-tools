"""
GitLab client for ART tools.
"""

from logging import getLogger
from urllib.parse import urlparse

import gitlab

logger = getLogger(__name__)


class GitLabClient:
    """
    GitLab client for ART tools.
    Provides high-level operations for managing merge requests and CI pipelines.
    """

    def __init__(self, gitlab_url: str, gitlab_token: str, dry_run: bool = False):
        """
        Initialize GitLabClient.

        Arg(s):
            gitlab_url (str): GitLab server URL (e.g., "https://gitlab.cee.redhat.com")
            gitlab_token (str): GitLab personal access token
            dry_run (bool): If True, operations will be logged but not executed
        Raises:
            ValueError: If gitlab_token is empty or None
            gitlab.exceptions.GitlabAuthenticationError: If authentication fails
        """
        self.dry_run = dry_run

        if not gitlab_token:
            raise ValueError("GitLab token is required but was not provided")

        try:
            self._client = gitlab.Gitlab(gitlab_url, private_token=gitlab_token)
            self._client.auth()
            logger.info(f"Successfully authenticated to GitLab at {gitlab_url}")
        except gitlab.exceptions.GitlabAuthenticationError as e:
            logger.error(f"Failed to authenticate to GitLab at {gitlab_url}: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to GitLab at {gitlab_url}: {e}")
            raise

    def get_project(self, project_path: str):
        """
        Get a GitLab project by path.

        Arg(s):
            project_path (str): Project path
        Return Value(s):
            GitLab project object
        """
        return self._client.projects.get(project_path)

    def get_mr_from_url(self, mr_url: str):
        """
        Get MR object from URL.

        Arg(s):
            mr_url (str): Full URL to the merge request
        Return Value(s):
            GitLab merge request object, or None if URL is invalid
        """
        if not mr_url:
            return None

        parsed_url = urlparse(mr_url)
        target_project_path = parsed_url.path.strip("/").split("/-/merge_requests")[0]
        mr_id = parsed_url.path.split("/")[-1]

        project = self._client.projects.get(target_project_path)
        return project.mergerequests.get(mr_id)

    async def set_mr_ready(self, mr_url: str):
        """
        Mark a GitLab MR as ready by removing the "Draft: " prefix from the title.
        This function is used at the end of a pipeline to indicate that all work is complete.

        Arg(s):
            mr_url (str): URL of the merge request to mark as ready
        Return Value(s):
            The MR object if successfully updated, None otherwise
        """
        if not mr_url:
            logger.info("No MR URL provided, skipping setting to ready")
            return None

        logger.info(f"Setting MR to ready: {mr_url}")

        mr = self.get_mr_from_url(mr_url)
        if not mr:
            logger.error(f"Could not retrieve MR from URL: {mr_url}")
            return None

        if mr.title.startswith("Draft: "):
            mr.title = mr.title.removeprefix("Draft: ")
            if self.dry_run:
                logger.info(f"[DRY-RUN] Would have set MR to ready with title: {mr.title}")
            else:
                mr.save()
                logger.info(f"MR marked as ready: {mr_url}")
            return mr
        else:
            logger.info("MR is already ready (no draft prefix found)")
            return mr

    async def trigger_ci_pipeline(self, mr) -> str | None:
        """
        Trigger a GitLab Merge Request pipeline using the MR API.

        Arg(s):
            mr: GitLab merge request object
        Return Value(s):
            Pipeline URL if successfully triggered, None otherwise
        """
        logger.info(f"Triggering CI pipeline for MR {mr.iid} on branch {mr.source_branch}")

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would have triggered MR pipeline for MR !{mr.iid} (branch: {mr.source_branch})")
            return None

        pipeline = mr.pipelines.create({"ref": mr.source_branch})

        pipeline_url = pipeline.web_url
        logger.info(f"CI MR pipeline triggered successfully: {pipeline_url}")
        return pipeline_url
