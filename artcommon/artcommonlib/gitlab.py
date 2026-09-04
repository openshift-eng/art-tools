"""
GitLab client for ART tools.
"""

import os
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

    @classmethod
    def from_url(cls, url: str, gitlab_token: str | None = None, dry_run: bool = False) -> "GitLabClient":
        """
        Create a GitLabClient from a full GitLab URL, extracting the server base URL automatically.

        Arg(s):
            url (str): Any GitLab URL (project, MR, etc.)
            gitlab_token (str | None): GitLab token. If None, reads from GITLAB_TOKEN env var.
            dry_run (bool): If True, operations will be logged but not executed
        Return Value(s):
            GitLabClient: Authenticated client instance
        """
        parsed = urlparse(url)
        gitlab_url = f"{parsed.scheme}://{parsed.netloc}"
        if gitlab_token is None:
            gitlab_token = os.getenv("GITLAB_TOKEN", "")
        return cls(gitlab_url, gitlab_token, dry_run=dry_run)

    def get_project(self, project_path: str):
        """
        Get a GitLab project by path.

        Arg(s):
            project_path (str): Project path
        Return Value(s):
            GitLab project object
        """
        return self._client.projects.get(project_path)

    @staticmethod
    def _parse_mr_url(mr_url: str) -> tuple[str, str]:
        """
        Extract the project path and MR IID from a full GitLab MR URL.

        Arg(s):
            mr_url (str): Full URL, e.g. "https://gitlab.example.com/group/project/-/merge_requests/42"
        Return Value(s):
            Tuple of (project_path, mr_iid) e.g. ("group/project", "42")
        Raises:
            ValueError: If the URL cannot be parsed as a GitLab MR URL
        """
        parsed = urlparse(mr_url)
        path = parsed.path.strip("/")
        if "/-/merge_requests/" not in path:
            raise ValueError(f"Not a valid GitLab MR URL: {mr_url}")
        project_path = path.split("/-/merge_requests")[0]
        mr_iid = path.split("/")[-1]
        return project_path, mr_iid

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

        project_path, mr_iid = self._parse_mr_url(mr_url)
        project = self._client.projects.get(project_path)
        return project.mergerequests.get(mr_iid)

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

    def _resolve_user_ids(self, usernames: list[str]) -> list[int]:
        """
        Resolve GitLab usernames to user IDs.

        Arg(s):
            usernames: List of GitLab usernames
        Return Value(s):
            List of resolved user IDs (skips unresolved usernames with a warning)
        """
        user_ids = []
        for username in usernames:
            users = self._client.users.list(username=username)
            if users:
                user_ids.append(users[0].id)
            else:
                logger.warning(f"Could not resolve GitLab username: {username}")
        return user_ids

    async def set_mr_approval_rules(self, mr_url: str, approvers_config: dict[str, list[str]]):
        """
        Configure MR-level approval rules based on group.yml mr_approvers config.
        Keeps the "ART" rule, removes all other inherited rules, and creates new
        rules from approvers_config.

        Arg(s):
            mr_url: Full URL to the merge request
            approvers_config: Dict mapping approval group names to lists of GitLab usernames,
                              e.g. {"QE": ["user1", "user2"]}
        """
        if not mr_url or not approvers_config:
            return

        if self.dry_run:
            for name, usernames in approvers_config.items():
                logger.info(f"[DRY-RUN] Would create approval rule '{name}' with users: {usernames}")
            return

        mr = self.get_mr_from_url(mr_url)
        if not mr:
            logger.error(f"Could not retrieve MR from URL: {mr_url}")
            return

        existing_rules = mr.approval_rules.list()

        for rule in existing_rules:
            if rule.name != "ART":
                logger.info(f"Deleting approval rule '{rule.name}' (id={rule.id})")
                rule.delete()

        for name, usernames in approvers_config.items():
            user_ids = self._resolve_user_ids(usernames)
            if not user_ids:
                logger.warning(f"No valid user IDs resolved for approval rule '{name}', skipping")
                continue
            rule = mr.approval_rules.create(
                {
                    "name": name,
                    "approvals_required": 1,
                    "user_ids": user_ids,
                }
            )
            actual_ids = [u["id"] for u in rule.users]
            dropped = set(user_ids) - set(actual_ids)
            if dropped:
                dropped_names = [u for u, uid in zip(usernames, user_ids) if uid in dropped]
                logger.warning(
                    "Approval rule '%s': GitLab dropped users %s (ids: %s) "
                    "— these users must be members of the ocp-shipment-data project "
                    "(or a group shared with it, e.g. team-ert or layered-products) to be eligible as approvers",
                    name,
                    dropped_names,
                    sorted(dropped),
                )
            logger.info(
                f"Created approval rule '{name}' with users: {usernames} (ids: {user_ids}, actual: {actual_ids})"
            )

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

    def add_mr_dependency(self, mr_url: str, blocking_mr_url: str):
        """
        Set a native GitLab MR dependency so *mr_url* cannot be merged until
        *blocking_mr_url* is merged first.

        Uses the ``POST /projects/:id/merge_requests/:iid/blocks`` API.
        Cross-project dependencies are supported by passing ``blocking_project_id``
        when the two MRs belong to different projects.

        Arg(s):
            mr_url (str): Full URL of the merge request to be blocked
            blocking_mr_url (str): Full URL of the merge request that must merge first
        """
        mr_project_path, mr_iid = self._parse_mr_url(mr_url)
        blocking_project_path, blocking_mr_iid = self._parse_mr_url(blocking_mr_url)

        project = self._client.projects.get(mr_project_path)

        query: dict = {"blocking_merge_request_iid": int(blocking_mr_iid)}
        if mr_project_path != blocking_project_path:
            blocking_project = self._client.projects.get(blocking_project_path)
            query["blocking_project_id"] = blocking_project.id

        if self.dry_run:
            logger.info("[DRY-RUN] Would add MR dependency: %s depends on %s", mr_url, blocking_mr_url)
            return

        self._client.http_post(
            f"/projects/{project.id}/merge_requests/{mr_iid}/blocks",
            query_data=query,
        )
        logger.info("Set MR dependency: %s depends on %s", mr_url, blocking_mr_url)

    def list_merge_requests(self, project_path: str, state: str = "opened", **kwargs):
        """
        List merge requests for a project, filtered by state and optional criteria.

        Arg(s):
            project_path (str): Project path (e.g., "hybrid-platforms/art/ocp-shipment-data")
            state (str): MR state filter ("opened", "closed", "merged", "all")
            **kwargs: Additional filters passed to python-gitlab (source_branch, target_branch, etc.)
        Return Value(s):
            list: List of merge request objects
        """
        project = self.get_project(project_path)
        return project.mergerequests.list(state=state, get_all=True, **kwargs)
